"""Order state machine of one namespace (phase 09 section 5 and "Sweep round 2").

Signed plan of an OPEN event (published by Risk in this order): `sl`, `tp1`, `trail`, `entry_ioc`, `entry`.
- `entry`: LIMIT GTX at the candidate price, sent once the `sl` leg is held. After `post_only_wait_s`
  without a full fill (or a GTX rejection -5022 "would take") and while the intent has not expired:
  **cancel, confirm the cancel** (response or `GET` by client id), read the exchange's real `executedQty`
  and send `entry_ioc` for `signed_qty - executedQty` (capped by the signed IOC qty) at the signed IOC
  limit. Past `expires_at` (set only by the testnet driver: a council decision has no time limit) the entry
  is only cancelled.
- Price guard (owner decision 2026-09-28): right before the entry or its IOC goes out, the current mark
  (`adapter.mark_prices`, the Binance mark or the paper venue's) is re-checked against the signed stop, TP1
  and `max_entry_distance` with the rule Risk applied (`hdt.core.entry_guard`). A broken rule, or no mark
  (fail closed), sends nothing: the intents are voided with the reason, alert `entry_refused` and
  `hdt_entries_refused_total{reason}`. An entry that waited in `orders` while execution was down is
  therefore sent only while its levels still hold.
- Every entry fill (partial included): STOP reduce-only for exactly the filled position at the signed
  invalidation plus TAKE_PROFIT for `tp1_fraction` of it (`StopGuard.protect`), via `algoOrder`.
- Emulated OCO: on a STOP/trail trigger the TP is cancelled by `clientAlgoId`; on a TP1 fill the STOP is
  re-placed for the remaining quantity and trailing starts (1x ATR, tighten only, new stop first).
- A position closed (net 0) cancels every remaining conditional order of the symbol and marks the event's
  intents done.
- `exit`: cancel resting entries (confirmed), then MARKET reduce-only for the position.
- `hedge`: MARKET for the signed BTC delta; the hedge event's `sl` (closePosition) is the new book stop. A
  book stop for the other side (the rebalance flips the book) waits for the flip fill: until then the
  current book keeps its own stop. Only the newest book `sl` is ever placed: a re-delivered older one is
  marked done without touching the book, and a placed one is marked done.
- A re-driven `sl`, `tp1` or `trail` only fills in plan fields the position still lacks: it never moves a
  trailed or tightened stop back to the signed level.
- Time stop at the horizon; a hard veto against a held position closes it without waiting for Risk. A close
  that could not be placed (or ended without executing) clears `exit_in_progress`, so trailing and the
  other exits keep managing the position, and is retried after `EXIT_RETRY_S`. An exit still in progress
  `EXIT_RETRY_S` after the tick first saw it (a triggered stop whose order expired while the algo reports
  FINISHED, a close that expired) pulls the missed fills, then closes a position still open again.

Actions that only reduce risk (tighten a stop, re-place a missing stop, cancel an entry/TP, reduce-only
close) need no signature; everything that increases exposure comes from a verified signed intent and is
refused while the namespace is re-syncing, paused or killed. Execution's own limits are checked again right
before every such order goes out, a re-sent `PENDING` row (crash, lost response) included.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Final

from hdt.contracts.common import Leg, OrderSide, OrderType, TimeInForce
from hdt.contracts.order import OrderIntent
from hdt.core.entry_guard import EntryRefusal, entry_refusal
from hdt.db.models.ledger import OrderRow, PositionRow
from hdt.execution.actions import (
    Placed,
    cancel_algo,
    cancel_order_confirmed,
    close_market,
    close_sent,
    closing_side,
    ioc_request,
    place_algo,
    place_order,
)
from hdt.execution.adapter_base import (
    CODE_GTX_WOULD_TAKE,
    AlgoEvent,
    AlgoRequest,
    ExchangeError,
    OrderEvent,
    OrderRequest,
    OrderSnapshot,
    TradeFill,
)
from hdt.execution.ledger import FillApplied, signed_qty
from hdt.execution.limits import (
    BTC_SYMBOL,
    LimitContext,
    LimitResult,
    check_intent,
    opening_notional,
)
from hdt.execution.runtime import Namespace
from hdt.execution.stop_guard import StopGuard
from hdt.ops.metrics import ENTRIES_REFUSED

log = logging.getLogger(__name__)

TRIGGERED: Final[frozenset[str]] = frozenset({"TRIGGERING", "TRIGGERED", "FINISHED"})
LOST: Final[frozenset[str]] = frozenset({"CANCELED", "EXPIRED", "REJECTED"})
ENTRY_LEGS: Final[tuple[str, ...]] = (Leg.ENTRY.value, Leg.ENTRY_IOC.value)
STOP_EXIT_REASONS: Final[frozenset[str]] = frozenset({"stop", "trail"})
EXIT_RETRY_S: Final[float] = 10.0


def limit_context(
    ns: Namespace,
    intent: OrderIntent,
    now: datetime,
    *,
    entries_for_event: int,
    released: Decimal = Decimal(0),
) -> LimitContext:
    """What execution's own limits see for `intent` now, from the ledger (`released`: opening notional
    already counted today for an order the one being checked replaces)."""
    ledger = ns.ledger
    book = ledger.position(BTC_SYMBOL)
    held = ns.held_reason() if intent.leg in (Leg.ENTRY, Leg.ENTRY_IOC) else None
    return LimitContext(
        equity=ns.equity(),
        daily_notional=max(Decimal(0), ledger.daily_notional(now.date()) - released),
        allowlist=ns.allowlist(),
        held_symbols={p.symbol for p in ledger.open_positions()},
        sl_received=ledger.event_leg(intent.event_id, Leg.SL) is not None,
        entries_for_event=entries_for_event,
        blocked_reason=ns.blocked_reason() or held,
        vetoed_sides=ns.vetoed_sides(intent.symbol, now),
        hedge_book_qty=signed_qty(book) if book is not None else Decimal(0),
    )


class OrderManager:
    """Acts on accepted intents and applies exchange events (the namespace's `ExchangeEventSink`)."""

    def __init__(self, ns: Namespace, guard: StopGuard) -> None:
        self.ns = ns
        self.guard = guard
        self._exit_retry: dict[str, tuple[str, datetime]] = {}  # symbol -> (reason, retry not before)
        self._exit_seen: dict[str, datetime] = {}  # symbol -> when the tick first saw its exit in progress

    # ================================================================== exchange events (sink)
    async def on_order_event(self, event: OrderEvent) -> None:
        async with self.ns.lock:
            await self.apply_order_event(event)

    async def on_algo_event(self, event: AlgoEvent) -> None:
        async with self.ns.lock:
            await self.apply_algo_event(event)

    async def on_account_update(self, wallet_balance: Decimal | None) -> None:
        self.ns.notify()

    async def apply_order_event(self, event: OrderEvent) -> None:
        """Lock held. Idempotent: replayed events (re-sync, duplicate pushes) change nothing."""
        self.ns.ledger.update_order(event.order, fill_model=event.fill_model)
        if event.fill is not None:
            await self.apply_fill(event.fill, event.fill_model)
        self.ns.notify()

    async def apply_fill(
        self, fill: TradeFill, fill_model: str | None = None, *, protect: bool = True
    ) -> FillApplied:
        model = fill_model if fill_model in ("queue", "degraded") else None
        applied = self.ns.ledger.apply_fill(fill, fill_model=model)  # type: ignore[arg-type]
        if applied.inserted:
            await self._after_fill(fill.symbol, applied, protect=protect)
        return applied

    async def apply_algo_event(self, event: AlgoEvent) -> None:
        ns = self.ns
        snap = event.algo
        row = ns.ledger.update_algo(snap)
        if row is None:
            return
        symbol = row.symbol
        pos = ns.ledger.position(symbol)
        if row.status in TRIGGERED and row.leg in (Leg.SL.value, Leg.TRAIL.value, Leg.TP1.value):
            if pos is not None and row.leg != Leg.TP1.value:
                ns.ledger.update_position(
                    pos.position_id,
                    exit_in_progress=True,
                    exit_reason="stop" if row.leg == Leg.SL.value else "trail",
                )
                for tp in ns.ledger.working_algos(symbol):
                    if tp.leg == Leg.TP1.value:
                        await cancel_algo(ns, tp.client_algo_id)
            await self.sync_symbol(symbol)
        elif row.status in LOST and pos is not None and row.leg in (Leg.SL.value, Leg.TRAIL.value):
            # A stop disappeared without us cancelling it (or its triggered order never executed): restore
            # the invariant right away. The exit this stop started when it triggered is over: the position
            # is managed again and the close it did not make is retried.
            reason = pos.exit_reason
            if (
                await self.guard.protect(symbol)
                and row.triggered_at is not None
                and pos.exit_in_progress
                and reason is not None
                and reason in STOP_EXIT_REASONS
                and ns.ledger.position(symbol) is not None
            ):
                ns.ledger.update_position(pos.position_id, exit_in_progress=False, exit_reason=None)
                self._exit_retry[symbol] = (reason, ns.clock() + timedelta(seconds=EXIT_RETRY_S))
        ns.notify()

    async def sync_symbol(self, symbol: str) -> None:
        """Pull fills the push stream may not have delivered yet, then re-check protection once.

        The pulled fills are applied as one batch: protecting between them would see a triggered STOP's
        order half-applied (the position still open, the STOP no longer working) and place a second STOP
        or a reduce-only close for a quantity the exchange has already closed.
        """
        ns = self.ns
        last = ns.ledger.last_trade_id(symbol)
        known = ns.ledger.known_order_ids(symbol)
        fills = await ns.adapter.user_trades(symbol, last + 1 if last is not None else None, None, known)
        for fill in fills:
            await self.apply_fill(fill, protect=False)
        if ns.ledger.position(symbol) is not None:
            await self.guard.protect(symbol)

    async def _after_fill(self, symbol: str, applied: FillApplied, *, protect: bool = True) -> None:
        ns = self.ns
        pos = ns.ledger.position(symbol)
        if applied.opened and pos is not None:
            self._init_plan(pos)
            pos = ns.ledger.position(symbol)
        if pos is None:
            await self._cleanup_closed(symbol, applied.event_id)
            return
        if applied.leg == Leg.TP1.value and not pos.tp1_done:
            ns.ledger.update_position(
                pos.position_id, tp1_done=True, best_price=pos.mark_price or pos.best_price
            )
        if protect:
            await self.guard.protect(symbol)

    def _init_plan(self, pos: PositionRow) -> None:
        """Copy the signed protective plan of the position's event onto the position row, only into the
        fields still unset: a re-driven intent never moves a trailed or tightened stop back to the signed
        level (the hedge book's stop moves only with a newer book `sl`, in `_book_stop`)."""
        ledger = self.ns.ledger
        plan: dict[str, object] = {"margin_type": "ISOLATED"}
        if pos.is_hedge_book:
            book = ledger.latest_hedge_intent(Leg.SL)
            if book is not None:
                plan.update(stop_price=book.trigger_price, initial_stop_price=book.trigger_price)
        elif pos.event_id is not None:
            legs = {i.leg: i for i in ledger.event_intents(pos.event_id)}
            sl, tp1, trail, entry = (
                legs.get(Leg.SL),
                legs.get(Leg.TP1),
                legs.get(Leg.TRAIL),
                legs.get(Leg.ENTRY),
            )
            if sl is not None:
                plan.update(
                    stop_price=sl.trigger_price,
                    initial_stop_price=sl.trigger_price,
                    time_stop_at=sl.expires_at,
                )
            if tp1 is not None:
                plan["tp1_price"] = tp1.trigger_price
            if trail is not None:
                plan["trail_distance"] = trail.price
            if entry is not None:
                plan["leverage"] = entry.leverage
        values = {k: v for k, v in plan.items() if v is not None and getattr(pos, k) is None}
        if values:
            ledger.update_position(pos.position_id, **values)

    async def _cleanup_closed(self, symbol: str, event_id: str | None) -> None:
        """Position flat: cancel its remaining conditional orders and resting entries."""
        ns = self.ns
        for algo in ns.ledger.working_algos(symbol):
            await cancel_algo(ns, algo.client_algo_id)
        for order in ns.ledger.open_orders(symbol):
            if order.leg in ENTRY_LEGS:
                await cancel_order_confirmed(ns, symbol, order.client_id)
        if not ns.ledger.working_algos(symbol):
            try:
                await ns.adapter.cancel_all_algos(symbol)  # sweeps leftovers; only after the close
            except Exception:
                log.warning("cancel_all_algos failed", extra={"account": ns.name, "symbol": symbol})
        if event_id is not None:
            for intent in ns.ledger.event_intents(event_id, statuses=("accepted",)):
                ns.ledger.set_intent_status(intent.intent_id, "done")
        self._exit_retry.pop(symbol, None)
        self._exit_seen.pop(symbol, None)

    # ================================================================== signed intents
    async def act(self, intent: OrderIntent) -> None:
        """Lock held. `intent` passed verification and execution's own limits."""
        if intent.leg is Leg.ENTRY:
            await self._send_entry(intent)
        elif intent.leg is Leg.EXIT:
            await self.exit_position(intent.symbol, "exit", intent)
        elif intent.leg is Leg.HEDGE:
            await self._send_hedge(intent)
        elif intent.leg is Leg.SL and intent.close_position:
            await self._book_stop(intent)
        elif intent.leg in (Leg.SL, Leg.TP1, Leg.TRAIL):
            pos = self.ns.ledger.position(intent.symbol)
            if pos is not None and pos.event_id == intent.event_id:
                self._init_plan(pos)
                await self.guard.protect(intent.symbol)
        # entry_ioc is the signed fallback plan of the entry: used by the entry watchdog.

    def _deferred_limits(
        self, intent: OrderIntent, now: datetime, mark: Decimal | None = None, released: Decimal = Decimal(0)
    ) -> LimitResult:
        """Execution's own limits again, on the ledger as it is now, right before an exposure-increasing
        order goes out (IOC fallback, entry or hedge, a re-sent `PENDING` row included): the daily cap, the
        per-order cap, blocks and vetoes may have changed since the intent was checked. A refusal voids the
        intent and raises an alert (not for a namespace block: the kill or re-sync alerted already);
        nothing is sent and no notional is counted."""
        ns = self.ns
        ctx = limit_context(ns, intent, now, entries_for_event=0, released=released)
        result = check_intent(intent, ctx, mark)
        if not result.ok:
            ns.ledger.set_intent_status(intent.intent_id, "void", result.reason)
            if result.reason == ctx.blocked_reason:
                log.info(
                    "order not sent, namespace blocked",
                    extra={"account": ns.name, "leg": intent.leg.value, "reason": result.reason},
                )
            else:
                ns.alert(
                    "intent_signature",
                    "warning",
                    f"{ns.name}: {intent.leg.value} {intent.symbol} not sent, refused by execution limits",
                    result.reason,
                    episode=intent.intent_id,
                )
        return result

    def _counted_today(
        self, intent: OrderIntent, row: OrderRow | None, now: datetime, mark: Decimal | None
    ) -> Decimal:
        """Opening notional of `intent`'s order already counted today: a `PENDING` row first sent earlier
        (crash, lost response) is re-checked with it released, so the daily cap never counts it twice."""
        if row is None or row.created_at.date() != now.date():
            return Decimal(0)
        book = self.ns.ledger.position(BTC_SYMBOL) if intent.leg is Leg.HEDGE else None
        notional = opening_notional(intent, signed_qty(book) if book is not None else Decimal(0), mark)
        return notional if notional is not None else Decimal(0)

    async def _send_entry(self, intent: OrderIntent) -> None:
        ns = self.ns
        assert intent.qty is not None
        assert intent.price is not None
        now = ns.clock()
        if intent.expires_at is not None and now > intent.expires_at:
            ns.ledger.set_intent_status(intent.intent_id, "void", "expired before it was sent")
            return
        row = ns.ledger.order(intent.client_id)
        if row is None and await self._entry_refused(
            intent, (intent, ns.ledger.event_leg(intent.event_id, Leg.ENTRY_IOC))
        ):
            return  # checked on the first send only: a PENDING row already passed it and may be live
        notional = Decimal(0)
        if row is None or row.status == "PENDING":  # this call sends the order: limits again, every time
            limits = self._deferred_limits(intent, now, released=self._counted_today(intent, row, now, None))
            if not limits.ok:
                return
            if row is None:
                notional = limits.notional  # a re-sent PENDING row was counted when first sent
        await ns.adapter.prepare_symbol(intent.symbol, intent.leverage or 1)
        request = OrderRequest(
            symbol=intent.symbol,
            side=intent.side,
            order_type=OrderType.LIMIT,
            qty=intent.qty,
            client_id=intent.client_id,
            price=intent.price,
            tif=intent.tif or TimeInForce.GTX,
        )
        if notional > 0:
            ns.ledger.add_daily_notional(now.date(), notional)
        placed = await place_order(
            ns,
            request,
            event_id=intent.event_id,
            leg=Leg.ENTRY,
            intent_id=intent.intent_id,
            expires_at=intent.expires_at,
        )
        if placed.ok:
            return
        if placed.error is not None and placed.error.code == CODE_GTX_WOULD_TAKE:
            # Post-only would take: nothing rests, nothing executed -> straight to the IOC fallback.
            await self._fallback_ioc(intent, Decimal(0))
            return
        ns.ledger.set_intent_status(intent.intent_id, "rejected", str(placed.error))

    async def _entry_refused(self, entry: OrderIntent, void: tuple[OrderIntent | None, ...]) -> bool:
        """The price guard right before an entry order goes out (see the module docstring): True when it
        refused, after voiding `void` with the reason, alerting and counting."""
        ns = self.ns
        stop = ns.ledger.event_leg(entry.event_id, Leg.SL)
        tp1 = ns.ledger.event_leg(entry.event_id, Leg.TP1)
        try:
            mark = (await ns.adapter.mark_prices([entry.symbol])).get(entry.symbol)
        except ExchangeError as exc:
            log.warning("mark unavailable before an entry", extra={"account": ns.name, "error": str(exc)})
            mark = None
        refused: EntryRefusal | None
        if entry.price is None or stop is None or stop.trigger_price is None:
            refused = EntryRefusal("no_stop_plan", "The entry has no price or no signed stop to check")
        elif mark is None:
            refused = EntryRefusal("no_mark", f"No current mark for {entry.symbol}: nothing is sent blind")
        else:
            refused = entry_refusal(
                long=entry.side is OrderSide.BUY,
                mark=mark,
                entry=entry.price,
                stop=stop.trigger_price,
                tp1=tp1.trigger_price if tp1 is not None else None,
                max_distance=entry.max_entry_distance,
            )
        if refused is None:
            return False
        for intent in void:
            if intent is not None:
                ns.ledger.set_intent_status(intent.intent_id, "void", f"{refused.reason}: {refused.text}")
        ENTRIES_REFUSED.labels(account=ns.name, reason=refused.reason).inc()
        ns.alert(
            "entry_refused",
            "warning",
            f"{ns.name}: {entry.symbol} entry not sent ({refused.reason})",
            refused.text,
            episode=entry.intent_id,
        )
        return True

    async def _fallback_ioc(self, entry: OrderIntent, executed: Decimal) -> None:
        ns = self.ns
        ioc = ns.ledger.event_leg(entry.event_id, Leg.ENTRY_IOC)
        now = ns.clock()
        if ioc is None or ioc.qty is None or ioc.price is None or entry.qty is None:
            return
        if entry.expires_at is not None and now > entry.expires_at:
            return
        blocked = ns.blocked_reason()
        if blocked is not None:
            log.info("IOC fallback refused", extra={"account": ns.name, "reason": blocked})
            ns.ledger.set_intent_status(ioc.intent_id, "void", blocked)
            return
        if ns.ledger.event_orders(entry.event_id, (Leg.ENTRY_IOC,)):
            return  # the IOC of this event was already sent (one per event)
        if await self._entry_refused(entry, (ioc,)):
            return
        filters = await ns.symbol_filters(entry.symbol)
        remaining = min(entry.qty - executed, ioc.qty)
        qty = filters.floor_qty(remaining) if filters is not None else remaining
        if qty <= 0 or (filters is not None and filters.qty_problem(qty, ioc.price) is not None):
            ns.ledger.set_intent_status(ioc.intent_id, "done", f"shortfall {remaining} below the lot minimum")
            return
        # The entry's unexecuted part was counted when it was sent today; the IOC replaces it.
        entry_row = ns.ledger.order(entry.client_id)
        released = Decimal(0)
        if entry_row is not None and entry.price is not None and entry_row.created_at.date() == now.date():
            released = (entry.qty - executed) * entry.price
        if not self._deferred_limits(ioc.model_copy(update={"qty": qty}), now, released=released).ok:
            return
        ns.ledger.add_daily_notional(now.date(), qty * ioc.price - released)
        placed = await place_order(
            ns,
            ioc_request(entry.symbol, entry.side, qty, ioc.price, ioc.client_id),
            event_id=entry.event_id,
            leg=Leg.ENTRY_IOC,
            intent_id=ioc.intent_id,
            expires_at=ioc.expires_at,
        )
        ns.ledger.set_intent_status(ioc.intent_id, "done", None if placed.ok else str(placed.error))

    async def _send_hedge(self, intent: OrderIntent) -> None:
        ns = self.ns
        assert intent.qty is not None
        now = ns.clock()
        row = ns.ledger.order(intent.client_id)
        notional = Decimal(0)
        if (row is None or row.status == "PENDING") and not intent.reduce_only:
            # This call sends the order: limits again, every time (growth or a flip of the book is opening).
            mark = (await ns.adapter.mark_prices([intent.symbol])).get(intent.symbol)
            released = self._counted_today(intent, row, now, mark)
            limits = self._deferred_limits(intent, now, mark, released=released)
            if not limits.ok:
                return
            if row is None:
                notional = limits.notional
        await ns.adapter.prepare_symbol(intent.symbol, intent.leverage or 1)
        request = OrderRequest(
            symbol=intent.symbol,
            side=intent.side,
            order_type=OrderType.MARKET,
            qty=intent.qty,
            client_id=intent.client_id,
            reduce_only=intent.reduce_only,
        )
        if notional > 0:
            ns.ledger.add_daily_notional(now.date(), notional)
        placed = await place_order(
            ns, request, event_id=intent.event_id, leg=Leg.HEDGE, intent_id=intent.intent_id
        )
        ns.ledger.set_intent_status(
            intent.intent_id, "done" if placed.ok else "rejected", str(placed.error or "")
        )

    async def _book_stop(self, intent: OrderIntent) -> None:
        """New hedge-book stop: placed first, the previous book stop cancelled after; the intent is done once
        its stop works.

        Only the newest book `sl` (highest hedge seq, `Ledger.latest_hedge_intent`) acts: an older one (a
        re-delivered, replayed or late reclaimed message) is marked done and never touches the book, so it
        can neither cancel the current stop nor move the level back. A stop on the other side (the rebalance
        flips the book) is not placed yet: the current book keeps its own valid stop until the flip fill,
        then `_init_plan` takes the new trigger from this intent and `protect` places it (new first, the old
        side's stop cancelled after). A refused or delayed flip leg therefore never leaves the book without
        a stop."""
        ns = self.ns
        assert intent.trigger_price is not None
        latest = ns.ledger.latest_hedge_intent(Leg.SL)
        if latest is None or latest.intent_id != intent.intent_id:
            log.info(
                "book stop superseded by a newer hedge event, not placed",
                extra={"account": ns.name, "event_id": intent.event_id},
            )
            ns.ledger.set_intent_status(intent.intent_id, "done", "superseded by a newer book stop")
            return
        pos = ns.ledger.position(intent.symbol)
        if pos is not None and intent.side is not closing_side(signed_qty(pos)):
            log.info(
                "book stop waits for the flip fill",
                extra={"account": ns.name, "event_id": intent.event_id},
            )
            return
        if pos is not None:
            ns.ledger.update_position(pos.position_id, stop_price=intent.trigger_price)
        previous = [
            a for a in ns.ledger.working_algos(intent.symbol) if a.order_type == OrderType.STOP_MARKET.value
        ]
        if any(
            a.status != "PENDING"
            and a.close_position
            and a.side == intent.side.value
            and a.trigger_price == intent.trigger_price
            for a in previous
        ):
            ns.ledger.set_intent_status(intent.intent_id, "done")  # this level already works
            return
        request = AlgoRequest(
            symbol=intent.symbol,
            side=intent.side,
            order_type=OrderType.STOP_MARKET,
            trigger_price=intent.trigger_price,
            client_algo_id=intent.client_id,
            close_position=True,
            reduce_only=False,
        )
        placed = await place_algo(
            ns, request, event_id=intent.event_id, leg=Leg.SL, intent_id=intent.intent_id
        )
        if placed.ok:
            ns.ledger.set_intent_status(intent.intent_id, "done")
            await self.guard.cancel_superseded(previous, keep=request.client_algo_id)
        elif pos is not None:
            await self.guard.protect(intent.symbol)

    async def exit_position(self, symbol: str, reason: str, intent: OrderIntent | None = None) -> None:
        """Close all legs of `symbol`: resting entries cancelled (confirmed), then MARKET reduce-only.

        When no close could be sent (rejected, not executed, or ended without executing) the position is
        managed again (`exit_in_progress` cleared) and the close is retried after `EXIT_RETRY_S`."""
        ns = self.ns
        for order in ns.ledger.open_orders(symbol):
            if order.leg in ENTRY_LEGS:
                await cancel_order_confirmed(ns, symbol, order.client_id)
        pos = ns.ledger.position(symbol)
        if pos is None:
            self._exit_retry.pop(symbol, None)
            if intent is not None:
                ns.ledger.set_intent_status(intent.intent_id, "done", "no position")
            return
        if pos.exit_in_progress and intent is None:
            return
        ns.ledger.update_position(pos.position_id, exit_in_progress=True, exit_reason=reason)
        signed = signed_qty(pos)
        event_id = pos.event_id or f"exit:{symbol}"
        if intent is not None and intent.qty is not None:
            request = OrderRequest(
                symbol=symbol,
                side=closing_side(signed),
                order_type=OrderType.MARKET,
                qty=min(intent.qty, abs(signed)),
                client_id=intent.client_id,
                reduce_only=True,
            )
            try:
                placed = await place_order(
                    ns, request, event_id=intent.event_id, leg=Leg.EXIT, intent_id=intent.intent_id
                )
            except ExchangeError as exc:  # 429, 418, 5xx: nothing to adopt; the full close below takes over
                placed = Placed(None, exc)
            ns.ledger.set_intent_status(
                intent.intent_id, "done" if placed.ok else "rejected", str(placed.error or "")
            )
            if close_sent(placed):
                if request.qty >= abs(signed):
                    self._exit_retry.pop(symbol, None)
                    return
                # The signed quantity was smaller than the position: the rest is closed too.
                signed -= request.qty if signed > 0 else -request.qty
        try:
            placed = await close_market(ns, symbol, signed, event_id=event_id)
        except ExchangeError as exc:  # not sent or state unknown: a reduce-only close is safe to send again
            placed = Placed(None, exc)
        if close_sent(placed):
            self._exit_retry.pop(symbol, None)
            return
        # Nothing closes the position: keep managing it (trailing, stop) and retry the close later.
        ns.ledger.update_position(pos.position_id, exit_in_progress=False, exit_reason=None)
        self._exit_retry[symbol] = (reason, ns.clock() + timedelta(seconds=EXIT_RETRY_S))
        log.warning(
            "close not placed, retrying later",
            extra={"account": ns.name, "symbol": symbol, "reason": reason, "error": str(placed.error)},
        )

    # ================================================================== timers (lock held)
    async def tick(self, now: datetime) -> None:
        """Entry watchdog, STOPs a 429 window held back, trailing stops, time stops, hard vetoes of held
        coins, close retries and exits that never completed."""
        await self._watch_entries(now)
        await self.guard.retry_protection(now)
        positions = self.ns.ledger.open_positions()
        if not positions:
            return
        marks = await self.ns.adapter.mark_prices([p.symbol for p in positions])
        for pos in positions:
            mark = marks.get(pos.symbol)
            if mark is not None:
                self.ns.ledger.update_position(pos.position_id, mark_price=mark)
            if pos.exit_in_progress:
                await self._watch_exit(pos, now)
                continue
            self._exit_seen.pop(pos.symbol, None)
            reason = self._exit_due(pos, now)
            if reason is not None:
                await self.exit_position(pos.symbol, reason)
                continue
            if mark is not None:
                await self._trail(pos, mark)

    async def _watch_exit(self, pos: PositionRow, now: datetime) -> None:
        """An exit still in progress `EXIT_RETRY_S` after the tick first saw it has not closed the position
        (a triggered stop whose order expired while the algo reports FINISHED, a close MARKET that expired,
        a flag left over from before a restart): pull the fills the stream may have missed, then hand a
        position still open back and close it again (reduce-only)."""
        seen = self._exit_seen.setdefault(pos.symbol, now)
        if (now - seen).total_seconds() < EXIT_RETRY_S:
            return
        self._exit_seen.pop(pos.symbol, None)
        await self.sync_symbol(pos.symbol)
        fresh = self.ns.ledger.position(pos.symbol)
        if fresh is None or not fresh.exit_in_progress:
            return
        reason = fresh.exit_reason or "exit"
        log.warning(
            "exit did not close the position, closing again",
            extra={"account": self.ns.name, "symbol": fresh.symbol, "reason": reason},
        )
        self.ns.ledger.update_position(fresh.position_id, exit_in_progress=False, exit_reason=None)
        await self.exit_position(fresh.symbol, reason)

    def _exit_due(self, pos: PositionRow, now: datetime) -> str | None:
        """Why `pos` is closed now: a failed close whose retry is due (None while it waits), the time stop,
        or a hard veto against its side."""
        retry = self._exit_retry.get(pos.symbol)
        if retry is not None:
            reason, not_before = retry
            return reason if now >= not_before else None
        if pos.time_stop_at is not None and now >= pos.time_stop_at:
            return "time"
        if not pos.is_hedge_book and self._vetoed(pos, now):
            return "veto"
        return None

    def _vetoed(self, pos: PositionRow, now: datetime) -> bool:
        flags = self.ns.flags_for(pos.symbol)
        if flags is None or now > flags.expires_at:
            return False
        return flags.veto_long if pos.side == "LONG" else flags.veto_short

    async def _trail(self, pos: PositionRow, mark: Decimal) -> None:
        if pos.trail_distance is None or pos.is_hedge_book:
            return
        long = pos.side == "LONG"
        activated = pos.tp1_done or (
            pos.tp1_price is not None and (mark >= pos.tp1_price if long else mark <= pos.tp1_price)
        )
        if not activated:
            return
        best = pos.best_price or mark
        best = max(best, mark) if long else min(best, mark)
        if best != pos.best_price:
            self.ns.ledger.update_position(pos.position_id, best_price=best)
        filters = await self.ns.symbol_filters(pos.symbol)
        raw = best - pos.trail_distance if long else best + pos.trail_distance
        level = (
            (filters.floor_price(raw) if long else filters.ceil_price(raw)) if filters is not None else raw
        )
        current = self.guard.trigger_for(pos)
        tick = filters.tick_size if filters is not None else Decimal(0)
        improves = current is None or (level > current + tick if long else level < current - tick)
        beyond_mark = level >= mark if long else level <= mark
        if improves and not beyond_mark:
            fresh = self.ns.ledger.position(pos.symbol)
            if fresh is not None:
                await self.guard.tighten(fresh, level)

    async def _watch_entries(self, now: datetime) -> None:
        ns = self.ns
        wait_s = ns.risk().post_only_wait_s
        for order in ns.ledger.open_orders():
            if order.leg != Leg.ENTRY.value or order.status == "PENDING":
                continue
            expired = order.expires_at is not None and now > order.expires_at
            waited = (now - order.created_at).total_seconds() >= wait_s
            if expired or waited:
                await self._retire_entry(order, try_ioc=not expired)

    async def _retire_entry(self, order: OrderRow, *, try_ioc: bool) -> None:
        """Cancel, confirm, then (not expired) IOC for the real shortfall `signed_qty - executedQty`.

        An unconfirmed cancel raises its own warning (episode: the symbol, never the reconciler's mismatch
        key, so the reconciler's Critical still pages), resolved once a cancel of that symbol confirms."""
        ns = self.ns
        done = await cancel_order_confirmed(ns, order.symbol, order.client_id)
        if done is None:
            ns.alert(
                "reconcile_mismatch",
                "warning",
                f"{ns.name} {order.symbol}: entry cancel not confirmed, IOC withheld",
                episode=order.symbol,
            )
            return
        ns.resolve("reconcile_mismatch", episode=order.symbol)
        if done.snapshot is not None and done.executed_qty > 0:
            await self._fill_gap(done.snapshot)
        entry = ns.ledger.event_leg(order.event_id, Leg.ENTRY)
        if entry is None:
            return
        ns.ledger.set_intent_status(entry.intent_id, "done")
        if try_ioc and entry.qty is not None and done.executed_qty < entry.qty:
            await self._fallback_ioc(entry, done.executed_qty)

    async def _fill_gap(self, snap: OrderSnapshot) -> None:
        """The exchange reports more executed than the ledger's fills: fetch the missing fills now."""
        if self.ns.ledger.filled_qty(snap.client_id) < snap.executed_qty:
            await self.sync_symbol(snap.symbol)

    # ================================================================== kill-switch helpers
    async def cancel_entries(self, symbol: str | None = None) -> int:
        """Cancel every resting entry / entry_ioc (confirmed) - never a conditional order."""
        cancelled = 0
        for order in self.ns.ledger.open_orders(symbol):
            if order.leg in ENTRY_LEGS:
                await cancel_order_confirmed(self.ns, order.symbol, order.client_id)
                cancelled += 1
        return cancelled
