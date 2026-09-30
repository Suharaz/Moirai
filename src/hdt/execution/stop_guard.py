"""STOP invariant (phase 09 section 5): every non-zero position has a reduce-only STOP on the correct side
with qty >= |position| in `openAlgoOrders`.

Checked after every fill (ledger view, `protect`) and on every reconcile cycle and re-sync (exchange view,
`check_exchange`), independently of mismatch handling. A missing STOP is re-placed at once at the signed
trigger (or the tighter trailing level the position already reached) with a fresh idempotent
`clientAlgoId`; after `stop_place_max_attempts` failed placements (a rejection, 429, 418 or 503 alike; or a
trigger already crossed, -2021) the position is closed MARKET reduce-only. A 429 is waited out (bounded by
`RATE_LIMIT_WAIT_MAX_S`, the namespace lock is held) before the next attempt, since the client refuses every
request until its window ends. A close that cannot be sent either never raises: it is a Critical alert, and
the position is protected again by the first order manager tick after a 429 window (`retry_protection`) or
by the next reconcile cycle, whichever comes first. A `PENDING` stop row (its request never
confirmed) never counts as covering. A stop is never "moved": the new stop is placed first and the
superseded one cancelled after, so the position is never without a stop.

When the position shrank below a covering stop (TP1 took its share, "Sweep round 2"), the stop is re-placed
for exactly the remaining quantity at the same level; tightening (trailing, liquidation distance) places the
better level the same way. A failed re-placement or tightening keeps the old, still covering stop; only a
tightened level the market already crossed closes the position reduce-only (that stop would have fired).

The hedge book has one stop for the whole BTC position, placed `closePosition=true` (the book is the only
BTC position of the namespace), at the trigger Risk signed with the latest hedge event.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Final

from hdt.contracts.common import Leg, OrderType
from hdt.db.models.ledger import AlgoOrderRow, PositionRow
from hdt.execution.actions import (
    cancel_algo,
    close_market,
    close_sent,
    closing_side,
    next_client_id,
    place_algo,
    place_order,
)
from hdt.execution.adapter_base import (
    CODE_WOULD_TRIGGER_IMMEDIATELY,
    AlgoRequest,
    AlgoSnapshot,
    ExchangeError,
    ExchangePosition,
    OrderRequest,
    RateLimitedError,
)
from hdt.execution.ledger import signed_qty
from hdt.execution.runtime import Namespace

log = logging.getLogger(__name__)

STOP_LEGS: Final[tuple[str, ...]] = (Leg.SL.value, Leg.TRAIL.value)
ORPHAN_EVENT_PREFIX: Final[str] = "orphan:"
RATE_LIMIT_WAIT_MAX_S: Final[float] = 2.0
"""Longest wait for a 429 window before the next STOP attempt (the STOP is due within 2 s of the fill)."""
RATE_LIMIT_WAIT_MARGIN_S: Final[float] = 0.1
"""Added to `retry_after_s`: the wait must end after the client's window, never a timer tick before it."""


@dataclass(frozen=True)
class StopIssue:
    symbol: str
    detail: str
    resolved: bool


def covers(
    algo_side: str, order_type: str, qty: Decimal | None, close_position: bool, position: Decimal
) -> bool:
    """A working conditional order that satisfies the STOP invariant for signed `position`."""
    return (
        order_type == OrderType.STOP_MARKET.value
        and algo_side == closing_side(position).value
        and (close_position or (qty is not None and qty >= abs(position)))
    )


class StopGuard:
    def __init__(self, ns: Namespace) -> None:
        self.ns = ns
        self._protect_at: dict[str, datetime] = {}  # symbol -> end of the 429 window that left it unprotected

    async def retry_protection(self, now: datetime) -> None:
        """Protect again every position a 429 left without a STOP (and not closed) once the window is over,
        instead of waiting for the next reconcile cycle (review cycle 3, m-a). A 429 again reschedules."""
        for symbol, due in list(self._protect_at.items()):
            if now >= due:
                del self._protect_at[symbol]
                await self.protect(symbol)

    # ------------------------------------------------------------------ trigger of a position
    def trigger_for(self, pos: PositionRow | None) -> Decimal | None:
        """The stop level of a position: its current (possibly trailed) stop, else the signed `sl`."""
        if pos is None:
            return None
        if pos.stop_price is not None:
            return pos.stop_price
        ledger = self.ns.ledger
        if pos.is_hedge_book:
            book = ledger.latest_hedge_intent(Leg.SL)
            return book.trigger_price if book is not None else None
        if pos.event_id is None:
            return None
        sl = ledger.event_leg(pos.event_id, Leg.SL)
        return sl.trigger_price if sl is not None else None

    def _working_stops(self, symbol: str) -> list[AlgoOrderRow]:
        return [
            a
            for a in self.ns.ledger.working_algos(symbol)
            if a.order_type == OrderType.STOP_MARKET.value and a.leg in STOP_LEGS
        ]

    # ------------------------------------------------------------------ after fills (ledger view)
    async def protect(self, symbol: str) -> bool:
        """STOP (and the TP1 share) for the current ledger position of `symbol`."""
        pos = self.ns.ledger.position(symbol)
        if pos is None or pos.qty == 0:
            return True
        signed = signed_qty(pos)
        covering = [
            a
            for a in self._working_stops(symbol)
            if a.status != "PENDING" and covers(a.side, a.order_type, a.qty, a.close_position, signed)
        ]
        if not covering:
            if not await self.place_stop(pos, signed, self.trigger_for(pos)):
                return False
        elif not pos.exit_in_progress and not any(a.close_position or a.qty == abs(signed) for a in covering):
            await self._resize(pos, signed, covering[-1])
        if not pos.is_hedge_book:
            await self._ensure_tp(pos)
        return True

    async def _resize(self, pos: PositionRow, signed: Decimal, current: AlgoOrderRow) -> None:
        """Re-place an oversized covering stop for exactly the remaining position, at its own level."""
        trigger = current.trigger_price if current.trigger_price is not None else self.trigger_for(pos)
        if trigger is None:
            return
        error = await self._place_first(pos, signed, trigger, Leg(current.leg), attempts=1)
        if error is not None:
            log.warning(
                "stop resize failed, the previous stop still covers",
                extra={"account": self.ns.name, "symbol": pos.symbol, "error": str(error)},
            )

    async def place_stop(
        self, pos: PositionRow, signed: Decimal, trigger: Decimal | None, *, leg: Leg = Leg.SL
    ) -> bool:
        """Place a STOP covering `signed`, then cancel the stops it supersedes; close if impossible."""
        ns = self.ns
        symbol = pos.symbol
        event_id = pos.event_id or f"{ORPHAN_EVENT_PREFIX}{symbol}"
        if trigger is None:
            await self._close_unprotected(symbol, signed, event_id, "position without a known stop level")
            return False
        attempts = max(1, ns.risk().stop_place_max_attempts)
        error = await self._place_first(pos, signed, trigger, leg, attempts=attempts)
        if error is None:
            return True
        why = f"stop {trigger} already crossed" if crossed(error) else str(error)
        await self._close_unprotected(symbol, signed, event_id, f"STOP could not be placed ({why})")
        return False

    async def _close_unprotected(self, symbol: str, signed: Decimal, event_id: str, why: str) -> None:
        """Reduce-only MARKET close of a position no STOP covers, with one Critical alert saying whether the
        close went out. A close that cannot be sent (the 429 latch, a ban, a 5xx) never raises: the position
        stays without a STOP until the 429 window ends (`retry_protection`) or the next reconcile cycle runs
        the invariant check again."""
        ns = self.ns
        outcome = "reduce-only close outcome unknown"
        refusal: ExchangeError | None = None
        try:
            placed = await close_market(ns, symbol, signed, event_id=event_id)
            if close_sent(placed):
                outcome = "closing reduce-only"
            else:
                refusal = placed.error or ExchangeError("the close ended without executing")
        except ExchangeError as exc:
            refusal = exc
        finally:
            if refusal is not None:
                until = "the next reconcile"
                if isinstance(refusal, RateLimitedError):
                    wait = timedelta(seconds=refusal.retry_after_s + RATE_LIMIT_WAIT_MARGIN_S)
                    self._protect_at[symbol] = ns.clock() + wait
                    until = "the 429 window ends"
                outcome = f"reduce-only close not sent ({refusal}): UNPROTECTED until {until}"
            ns.alert("stop_invariant", "critical", f"{ns.name} {symbol}: {why}, {outcome}")

    async def _place_first(
        self, pos: PositionRow, signed: Decimal, trigger: Decimal, leg: Leg, *, attempts: int
    ) -> ExchangeError | None:
        """New STOP covering `signed` first (up to `attempts` placements), then every stop it supersedes
        cancelled. None once placed, else the last error (a crossed trigger is never retried; a 429 is
        waited out before the next attempt)."""
        ns = self.ns
        symbol = pos.symbol
        event_id = pos.event_id or f"{ORPHAN_EVENT_PREFIX}{symbol}"
        superseded = self._working_stops(symbol)
        close_position = pos.is_hedge_book
        error = ExchangeError("STOP not attempted")
        for attempt in range(attempts):
            if attempt and isinstance(error, RateLimitedError):
                # The client refuses every request until the 429 window ends: attempting at once would
                # fail locally and burn the attempt.
                await asyncio.sleep(
                    min(error.retry_after_s + RATE_LIMIT_WAIT_MARGIN_S, RATE_LIMIT_WAIT_MAX_S)
                )
            request = AlgoRequest(
                symbol=symbol,
                side=closing_side(signed),
                order_type=OrderType.STOP_MARKET,
                trigger_price=trigger,
                client_algo_id=next_client_id(ns, event_id, leg),
                qty=None if close_position else abs(signed),
                close_position=close_position,
                reduce_only=not close_position,
            )
            placed = await place_algo(ns, request, event_id=event_id, leg=leg, intent_id=None)
            if placed.ok:
                await self.cancel_superseded(superseded, keep=request.client_algo_id)
                if pos.stop_price != trigger:
                    ns.ledger.update_position(pos.position_id, stop_price=trigger)
                return None
            error = placed.error or ExchangeError("STOP not placed")
            if crossed(error):
                return error
        return error

    async def cancel_superseded(self, stops: Iterable[AlgoOrderRow], *, keep: str) -> None:
        """Cancel the stops a newer, working one replaced. A failed cancel is only logged: the old stop only
        ever reduces risk, and the next replacement or the position close cancels it."""
        for old in stops:
            if old.client_algo_id == keep:
                continue
            try:
                await cancel_algo(self.ns, old.client_algo_id)
            except ExchangeError as exc:
                log.warning(
                    "superseded stop not cancelled, retried with the next replacement",
                    extra={"account": self.ns.name, "client_algo_id": old.client_algo_id, "error": str(exc)},
                )

    async def _ensure_tp(self, pos: PositionRow) -> None:
        """Take-profit for `tp1_fraction` of the current position while TP1 has not filled."""
        ns = self.ns
        symbol = pos.symbol
        kill = ns.ledger.kill_state()
        if kill is not None and kill.state == "killed":
            return  # the kill removed the TPs; they stay off until resume
        tps = [a for a in ns.ledger.working_algos(symbol) if a.leg == Leg.TP1.value]
        if pos.tp1_done or pos.tp1_price is None or pos.exit_in_progress or pos.event_id is None:
            return
        filters = await ns.symbol_filters(symbol)
        if filters is None:
            return
        desired = filters.floor_qty(pos.qty * Decimal(repr(ns.risk().tp1_fraction)))
        if desired < filters.min_qty:
            desired = Decimal(0)
        keep = next((a for a in tps if desired > 0 and a.qty == desired), None)
        if keep is None and desired > 0:
            signed = signed_qty(pos)
            request = AlgoRequest(
                symbol=symbol,
                side=closing_side(signed),
                order_type=OrderType.TAKE_PROFIT_MARKET,
                trigger_price=pos.tp1_price,
                client_algo_id=next_client_id(ns, pos.event_id, Leg.TP1),
                qty=desired,
                reduce_only=True,
            )
            placed = await place_algo(ns, request, event_id=pos.event_id, leg=Leg.TP1, intent_id=None)
            if placed.ok:
                keep_id = request.client_algo_id
            elif placed.error is not None and placed.error.code == CODE_WOULD_TRIGGER_IMMEDIATELY:
                # Price is already past TP1: take the share now (reduce-only, risk-reducing).
                market = OrderRequest(
                    symbol=symbol,
                    side=closing_side(signed),
                    order_type=OrderType.MARKET,
                    qty=desired,
                    client_id=next_client_id(ns, pos.event_id, Leg.TP1),
                    reduce_only=True,
                )
                try:
                    await place_order(ns, market, event_id=pos.event_id, leg=Leg.TP1, intent_id=None)
                except ExchangeError as exc:  # the STOP still covers the whole position
                    log.warning(
                        "TP1 market share not sent",
                        extra={"account": ns.name, "symbol": symbol, "error": str(exc)},
                    )
                keep_id = ""
            else:
                return
        else:
            keep_id = keep.client_algo_id if keep is not None else ""
        for old in tps:
            if old.client_algo_id != keep_id:
                await cancel_algo(ns, old.client_algo_id)

    async def tighten(self, pos: PositionRow, trigger: Decimal) -> bool:
        """Trailing or liquidation distance: move the stop in the favourable direction only (new stop first,
        old one cancelled). A failed placement keeps the old stop, which still covers the position; only a
        level the market already crossed (-2021) closes it reduce-only, since that stop would have fired."""
        ns = self.ns
        signed = signed_qty(pos)
        current = self.trigger_for(pos)
        if current is not None:
            better = trigger > current if signed > 0 else trigger < current
            if not better:
                return False
        error = await self._place_first(pos, signed, trigger, Leg.TRAIL, attempts=1)
        if error is None:
            return True
        extra = {"account": ns.name, "symbol": pos.symbol, "trigger": str(trigger), "error": str(error)}
        if not crossed(error):
            log.warning("stop not tightened, the previous stop still covers", extra=extra)
            return False
        log.warning("tightened stop level already crossed, closing reduce-only", extra=extra)
        try:
            await close_market(
                ns, pos.symbol, signed, event_id=pos.event_id or f"{ORPHAN_EVENT_PREFIX}{pos.symbol}"
            )
        except ExchangeError as exc:  # the previous stop still covers; the next tick or cycle tries again
            log.warning("reduce-only close not sent", extra={**extra, "error": str(exc)})
        return False

    # ------------------------------------------------------------------ reconcile / re-sync (exchange view)
    async def check_exchange(
        self, positions: Sequence[ExchangePosition], algos: Sequence[AlgoSnapshot]
    ) -> list[StopIssue]:
        """Enforce the invariant on the exchange's own view; returns what had to be fixed."""
        issues: list[StopIssue] = []
        for p in positions:
            if p.qty == 0:
                continue
            ok = any(
                a.symbol == p.symbol
                and a.is_working
                and covers(a.side.value, a.order_type, a.qty, a.close_position, p.qty)
                and (a.close_position or a.reduce_only)
                for a in algos
            )
            if ok:
                continue
            pos = self.ns.ledger.position(p.symbol)
            if pos is None:
                resolved = await self._protect_orphan(p)
            else:
                resolved = await self.place_stop(pos, p.qty, self.trigger_for(pos))
            issues.append(StopIssue(p.symbol, f"no covering STOP for {p.qty}", resolved))
            log.warning(
                "stop invariant repaired"
                if resolved
                else "stop invariant: no STOP placed, reduce-only close",
                extra={"account": self.ns.name, "symbol": p.symbol},
            )
        return issues

    async def _protect_orphan(self, p: ExchangePosition) -> bool:
        """A position the ledger does not know: no signed level exists, so it is closed reduce-only."""
        await self._close_unprotected(
            p.symbol,
            p.qty,
            f"{ORPHAN_EVENT_PREFIX}{p.symbol}",
            f"exchange position {p.qty} unknown to the ledger",
        )
        return False


def crossed(error: ExchangeError) -> bool:
    """The STOP trigger is already crossed (-2021): placing it again at that level can never succeed."""
    return error.code == CODE_WOULD_TRIGGER_IMMEDIATELY
