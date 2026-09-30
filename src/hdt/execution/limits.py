"""Execution's own hard limits (phase 09 section 5), independent of Risk.

A valid signature proves an intent came from Risk; it does not prove Risk was right (or not compromised).
Every intent that passed `verify_intent` is checked again here against constants in this module and
`hdt.settings.ceilings`, never against the Risk config:
- symbol allowlist: the point-in-time universe; BTC only for the hedge book (`hedge` leg and the book's
  `sl`); protective legs are always allowed on a symbol the namespace holds (a coin that left the universe
  can still be closed);
- max notional per order (a fraction of equity) and per UTC day per namespace (opening notional);
- leverage <= `LEVERAGE_MAX`;
- an `entry` is sent only once the `sl` leg of the same `event_id` has been received, and at most one
  `entry` / `entry_ioc` per `event_id`;
- `sl` / `tp1` / `trail` / `exit` are reduce-only (or `closePosition` for a stop);
- stop and take-profit triggers sit on the correct side of the entry;
- opening legs are refused while the namespace is paused/killed or re-syncing, and on a coin whose unexpired
  `RiskFlags` veto that side; a `hedge` leg counts as opening when it grows the book or flips it to the
  other side (a flip is valued at the whole new side, `abs(after)`).
"""

from __future__ import annotations

from collections.abc import Collection
from dataclasses import dataclass
from decimal import Decimal
from typing import Final

from hdt.contracts.common import Leg, OrderSide, OrderType
from hdt.contracts.order import OrderIntent
from hdt.settings.ceilings import LEVERAGE_MAX

MAX_ORDER_NOTIONAL_EQUITY_FRAC: Final[Decimal] = Decimal("0.10")  # Risk: margin 3% x leverage 3 = 9%
MAX_HEDGE_ORDER_NOTIONAL_EQUITY_FRAC: Final[Decimal] = Decimal("0.75")
MAX_DAILY_NOTIONAL_EQUITY_MULT: Final[Decimal] = Decimal("3.0")
BTC_SYMBOL: Final[str] = "BTCUSDT"
REDUCING_LEGS: Final[frozenset[Leg]] = frozenset({Leg.SL, Leg.TP1, Leg.TRAIL, Leg.EXIT})
OPENING_LEGS: Final[frozenset[Leg]] = frozenset({Leg.ENTRY, Leg.ENTRY_IOC})


@dataclass(frozen=True)
class LimitContext:
    """What execution knows when an intent arrives."""

    equity: Decimal | None
    daily_notional: Decimal
    allowlist: Collection[str]
    held_symbols: Collection[str]
    sl_received: bool
    entries_for_event: int
    blocked_reason: str | None  # paused / killed / re-syncing
    vetoed_sides: Collection[OrderSide]  # entry sides vetoed by unexpired RiskFlags on this coin
    hedge_book_qty: Decimal  # signed current BTC book (hedge legs only)


@dataclass(frozen=True)
class LimitResult:
    ok: bool
    reason: str | None = None
    notional: Decimal = Decimal(0)

    @classmethod
    def reject(cls, reason: str) -> LimitResult:
        return cls(False, reason)


def is_hedge_event(event_id: str) -> bool:
    return event_id.startswith("hedge:")


def reference_price(intent: OrderIntent, mark: Decimal | None) -> Decimal | None:
    return intent.price if intent.order_type is OrderType.LIMIT else (mark or intent.trigger_price)


def hedge_book_after(intent: OrderIntent, hedge_book_qty: Decimal) -> Decimal:
    """Signed BTC book once the `hedge` leg `intent` filled."""
    delta = intent.qty or Decimal(0)
    return hedge_book_qty + (delta if intent.side is OrderSide.BUY else -delta)


def flips_book(hedge_book_qty: Decimal, after: Decimal) -> bool:
    """The book changes side (short to long or long to short)."""
    return hedge_book_qty != 0 and after != 0 and (after > 0) != (hedge_book_qty > 0)


def increases_exposure(intent: OrderIntent, hedge_book_qty: Decimal) -> bool:
    """Opening legs always; a `hedge` leg that grows the book or flips it to the other side."""
    if intent.leg in OPENING_LEGS:
        return True
    if intent.leg is not Leg.HEDGE or intent.reduce_only:
        return False
    after = hedge_book_after(intent, hedge_book_qty)
    return abs(after) > abs(hedge_book_qty) or flips_book(hedge_book_qty, after)


def opening_qty(intent: OrderIntent, hedge_book_qty: Decimal) -> Decimal | None:
    """Quantity an exposure-increasing intent is valued at against the caps: the order quantity, except a
    hedge that flips the book, which opens the whole new side (`abs(after)`); None when unknown."""
    if intent.qty is None:
        return None
    if intent.leg is Leg.HEDGE:
        after = hedge_book_after(intent, hedge_book_qty)
        if flips_book(hedge_book_qty, after):
            return abs(after)
    return intent.qty


def opening_notional(intent: OrderIntent, hedge_book_qty: Decimal, mark: Decimal | None) -> Decimal | None:
    """Notional counted against the per-order and daily caps (None when it cannot be valued)."""
    price = reference_price(intent, mark)
    qty = opening_qty(intent, hedge_book_qty)
    return None if price is None or qty is None else qty * price


def check_intent(intent: OrderIntent, ctx: LimitContext, mark: Decimal | None) -> LimitResult:
    """Execution's verdict on a verified intent (the first failing rule wins)."""
    hedge_event = is_hedge_event(intent.event_id)
    if intent.leverage is not None and not 1 <= intent.leverage <= LEVERAGE_MAX:
        return LimitResult.reject(f"leverage {intent.leverage} above {LEVERAGE_MAX}")
    if intent.leg in REDUCING_LEGS and not (intent.reduce_only or intent.close_position):
        return LimitResult.reject(f"leg {intent.leg.value} must be reduce-only")
    if intent.symbol == BTC_SYMBOL:
        if not (intent.leg is Leg.HEDGE or (hedge_event and intent.leg is Leg.SL)):
            return LimitResult.reject("BTC is reserved for the hedge book")
    elif intent.leg is Leg.HEDGE or hedge_event:
        return LimitResult.reject(f"hedge legs trade {BTC_SYMBOL} only")
    elif intent.symbol not in ctx.allowlist and not (
        intent.leg in REDUCING_LEGS and intent.symbol in ctx.held_symbols
    ):
        return LimitResult.reject(f"{intent.symbol} is not in the tradable universe")

    opening = increases_exposure(intent, ctx.hedge_book_qty)
    if not opening:
        return LimitResult(True)
    if ctx.blocked_reason is not None:
        return LimitResult.reject(ctx.blocked_reason)
    if intent.leg in OPENING_LEGS:
        if not ctx.sl_received:
            return LimitResult.reject("entry before the sl leg of its event")
        if ctx.entries_for_event > 0:
            return LimitResult.reject(f"a {intent.leg.value} leg was already accepted for this event")
        if intent.side in ctx.vetoed_sides:
            return LimitResult.reject(f"{intent.symbol} is vetoed for {intent.side.value} entries")
    if ctx.equity is None or ctx.equity <= 0:
        return LimitResult.reject("equity unknown: no exposure-increasing order")
    notional = opening_notional(intent, ctx.hedge_book_qty, mark)
    if notional is None:
        return LimitResult.reject("cannot value the order notional")
    cap_frac = (
        MAX_HEDGE_ORDER_NOTIONAL_EQUITY_FRAC if intent.leg is Leg.HEDGE else MAX_ORDER_NOTIONAL_EQUITY_FRAC
    )
    cap = ctx.equity * cap_frac
    if notional > cap:
        return LimitResult.reject(f"order notional {notional:.2f} above the execution cap {cap:.2f}")
    daily_cap = ctx.equity * MAX_DAILY_NOTIONAL_EQUITY_MULT
    if ctx.daily_notional + notional > daily_cap:
        return LimitResult.reject(
            f"daily opening notional {ctx.daily_notional + notional:.2f} "
            f"above the execution cap {daily_cap:.2f}"
        )
    return LimitResult(True, notional=notional)


def check_protective_geometry(entry: OrderIntent, protective: OrderIntent) -> str | None:
    """A stop below a long entry (above a short one), a take-profit on the other side (None when fine)."""
    if entry.price is None or protective.trigger_price is None:
        return None
    long_entry = entry.side is OrderSide.BUY
    if protective.side is entry.side:
        return f"{protective.leg.value} is on the entry side"
    if protective.leg is Leg.SL:
        wrong = (
            protective.trigger_price >= entry.price if long_entry else protective.trigger_price <= entry.price
        )
        return f"stop {protective.trigger_price} on the wrong side of entry {entry.price}" if wrong else None
    if protective.leg is Leg.TP1:
        wrong = (
            protective.trigger_price <= entry.price if long_entry else protective.trigger_price >= entry.price
        )
        return (
            f"take-profit {protective.trigger_price} on the wrong side of entry {entry.price}"
            if wrong
            else None
        )
    return None
