"""Pooled BTC hedge book (phase 09 section 7). Pure functions; the Risk consumer supplies the book.

BTC is not a council coin. Each namespace holds one net BTC position that hedges the beta of every open
alt position:

    target_notional = - sum(beta_i x notional_i)       (notional signed: + LONG, - SHORT)
    target_qty      = target_notional / btc_mark       (rounded toward zero to the lot step)

A rebalance is signed when `|target - current|` exceeds both `rebalance_threshold_frac x |target|` and
`rebalance_min_notional_mult x minNotional` (in BTC), or when the target is flat and a book remains. The
whole book has one STOP at `stop_atr_mult x` BTC 1 h ATR from the mark (placed `closePosition`: the book is
the only BTC position of the namespace), replaced when the book changes size. The book's distance to that
stop counts toward the same-direction risk ceiling: a growing book is capped (`cap_growth`) so the book's
side (book risk + that side's alt exposures) stays within `same_direction_risk_max x equity`.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal
from typing import Final

from hdt.contracts.common import Account, Side
from hdt.core.config import HedgeConfig
from hdt.execution.filters import SymbolFilters

BTC_SYMBOL: Final[str] = "BTCUSDT"
DEFAULT_BETA: Final[float] = 1.0  # used when an event carries no beta: hedge the full notional
HEDGE_SETTLE_S: Final[float] = 60.0  # a rebalance younger than this is still in flight


@dataclass(frozen=True)
class AltLeg:
    """One open council position as the hedge book sees it."""

    symbol: str
    notional: Decimal  # signed: qty x mark, positive = long
    beta: float


@dataclass(frozen=True)
class Rebalance:
    target_qty: Decimal
    current_qty: Decimal
    delta: Decimal  # signed BTC quantity to trade (0 = nothing to do)
    reason: str

    @property
    def needed(self) -> bool:
        return self.delta != 0

    @property
    def reduces(self) -> bool:
        """The trade only shrinks the book toward zero (reduce-only)."""
        return (
            self.current_qty != 0
            and (self.delta > 0) != (self.current_qty > 0)
            and abs(self.delta) <= abs(self.current_qty)
        )


def hedge_event_id(account: Account, rebalance_seq: int) -> str:
    return f"hedge:{account.value}:{rebalance_seq}"


def target_notional(legs: Iterable[AltLeg]) -> Decimal:
    return -sum((Decimal(repr(leg.beta)) * leg.notional for leg in legs), Decimal(0))


def target_qty(legs: Iterable[AltLeg], btc_mark: Decimal, filters: SymbolFilters) -> Decimal:
    if btc_mark <= 0:
        raise ValueError("BTC mark must be positive")
    raw = target_notional(legs) / btc_mark
    steps = (abs(raw) / filters.step_size).to_integral_value(rounding=ROUND_DOWN)
    qty = steps * filters.step_size
    return qty if raw >= 0 else -qty


def plan_rebalance(
    target: Decimal, current: Decimal, btc_mark: Decimal, filters: SymbolFilters, cfg: HedgeConfig
) -> Rebalance:
    """Signed trade that moves the book to `target` when the drift is large enough."""
    diff = target - current
    if diff == 0:
        return Rebalance(target, current, Decimal(0), "book on target")
    if target == 0:
        return Rebalance(target, current, diff, "no alt exposure left: close the book")
    min_qty = Decimal(repr(cfg.rebalance_min_notional_mult)) * filters.min_notional / btc_mark
    threshold = max(Decimal(repr(cfg.rebalance_threshold_frac)) * abs(target), min_qty, filters.min_qty)
    if abs(diff) <= threshold:
        return Rebalance(target, current, Decimal(0), f"drift {abs(diff)} within threshold {threshold}")
    steps = (abs(diff) / filters.step_size).to_integral_value(rounding=ROUND_DOWN)
    delta = steps * filters.step_size
    if delta == 0:
        return Rebalance(target, current, Decimal(0), "drift below one lot step")
    return Rebalance(target, current, delta if diff > 0 else -delta, f"drift {abs(diff)} above {threshold}")


def cap_growth(
    plan: Rebalance, room: Decimal, stop_distance: Decimal, btc_mark: Decimal, filters: SymbolFilters
) -> Rebalance:
    """`plan` with the book after it capped at the size whose risk to the book stop fits in `room` (the
    same-direction budget left on the book's side once that side's alt exposures are counted).

    The cap never shrinks a same-side book that already exceeds it (the trade is dropped instead), and a
    flip with no room on the new side is reduced to closing the old book."""
    after = plan.current_qty + plan.delta
    if after == 0 or stop_distance <= 0:
        return plan
    cap = filters.floor_qty(max(room, Decimal(0)) / stop_distance)
    if abs(after) <= cap:
        return plan
    capped = cap if after > 0 else -cap
    delta = capped - plan.current_qty
    note = f"book capped at {cap} BTC by the same-direction ceiling"
    if delta == 0 or (delta > 0) != (plan.delta > 0):
        return Rebalance(plan.target_qty, plan.current_qty, Decimal(0), f"growth skipped: {note}")
    if capped != 0 and filters.qty_problem(abs(delta), btc_mark) is not None:
        return Rebalance(
            plan.target_qty, plan.current_qty, Decimal(0), f"growth skipped: {note}, rest below the minimum"
        )
    return Rebalance(plan.target_qty, plan.current_qty, delta, f"{plan.reason}; {note}")


def book_side(qty: Decimal) -> Side | None:
    if qty == 0:
        return None
    return Side.LONG if qty > 0 else Side.SHORT


def book_stop(
    qty: Decimal, btc_mark: Decimal, btc_atr_1h: Decimal, cfg: HedgeConfig, filters: SymbolFilters
) -> Decimal:
    """Stop trigger of a book of signed size `qty`: `stop_atr_mult x ATR` beyond the mark, tick-aligned."""
    if qty == 0:
        raise ValueError("an empty book has no stop")
    if btc_atr_1h <= 0:
        raise ValueError("BTC ATR must be positive")
    distance = Decimal(repr(cfg.stop_atr_mult)) * btc_atr_1h
    if qty > 0:
        return filters.floor_price(btc_mark - distance)
    return filters.ceil_price(btc_mark + distance)


def book_risk(qty: Decimal, stop: Decimal | None, mark: Decimal, leverage: int = 1) -> Decimal:
    """Loss if the book is stopped now (margin at risk when no stop is known)."""
    if qty == 0:
        return Decimal(0)
    if stop is None:
        return abs(qty) * mark / Decimal(max(1, leverage))
    adverse = (mark - stop) if qty > 0 else (stop - mark)
    return abs(qty) * max(Decimal(0), adverse)
