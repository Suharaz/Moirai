"""Exchange action primitives with ledger bookkeeping (callers hold the namespace lock).

Every order is recorded `PENDING` in the ledger **before** the request is sent, so a crash between the two
leaves a row the reconciler resolves by client id. An `UnknownOrderStateError` (503 "Unknown error",
timeout) is never retried blindly: the order is looked up by client id first and only a confirmed absence
allows a new attempt (with the next `seq`, so the id never collides with a request that may still land).

A rejection (`OrderRejectedError`) comes back as a failed `Placed`. Any other failure of a regular order
raises: the intent's stream message is not acked and its re-delivery re-sends the same client id. The
request of a conditional order (a STOP or TP, placed by the stop guard that must count its failures) never
raises an `ExchangeError`: a 429, 418 or 503 "not processed" created nothing (the row is closed `REJECTED`),
a failed lookup after an unknown state leaves the row `PENDING` for the reconciler; both are a failed
`Placed`. A conditional order is placed only once per `clientAlgoId`: asking again for an id whose order is
no longer working (cancelled, triggered, rejected) never reports a new placement.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Final

from hdt.contracts.common import Leg, OrderSide, OrderType, TimeInForce
from hdt.execution.adapter_base import (
    CODE_DUPLICATE_CLIENT_ID,
    FINAL_ORDER_STATUSES,
    AlgoRequest,
    AlgoSnapshot,
    ExchangeError,
    OrderRejectedError,
    OrderRequest,
    OrderSnapshot,
    UnknownOrderStateError,
)
from hdt.execution.client_ids import client_id, next_seq
from hdt.execution.runtime import Namespace

log = logging.getLogger(__name__)

CANCEL_CONFIRM_ATTEMPTS: Final[int] = 3
CANCEL_CONFIRM_BACKOFF_S: Final[float] = 0.5
UNKNOWN_VERIFY_DELAY_S: Final[float] = 0.5


@dataclass(frozen=True)
class Placed:
    """Outcome of one placement: a snapshot, or the reason nothing exists on the exchange."""

    snapshot: OrderSnapshot | AlgoSnapshot | None
    error: ExchangeError | None = None

    @property
    def ok(self) -> bool:
        return self.snapshot is not None


def close_sent(placed: Placed) -> bool:
    """A close order that exists and is filled or can still execute (a final state other than FILLED,
    for example an EXPIRED MARKET, closed nothing more)."""
    snap = placed.snapshot
    if not isinstance(snap, OrderSnapshot):
        return False
    return snap.status == "FILLED" or snap.status not in FINAL_ORDER_STATUSES


@dataclass(frozen=True)
class Cancelled:
    """A confirmed final state of a regular order; `executed_qty` is the exchange's real `executedQty`."""

    executed_qty: Decimal
    snapshot: OrderSnapshot | None  # None when the order never existed on the exchange


def closing_side(signed_qty: Decimal) -> OrderSide:
    return OrderSide.SELL if signed_qty > 0 else OrderSide.BUY


def next_client_id(ns: Namespace, event_id: str, leg: Leg) -> str:
    return client_id(event_id, leg, next_seq(ns.ledger.client_ids_for_event(event_id), event_id, leg))


async def place_order(
    ns: Namespace,
    request: OrderRequest,
    *,
    event_id: str,
    leg: Leg,
    intent_id: str | None,
    expires_at: datetime | None = None,
) -> Placed:
    """Record, send, and resolve a regular order (never sends twice for one client id)."""
    existing = ns.ledger.order(request.client_id)
    if existing is not None and existing.status != "PENDING":
        snap = await ns.adapter.query_order(request.symbol, request.client_id)
        if snap is not None:
            ns.ledger.update_order(snap)
        return Placed(snap)
    if existing is None:
        ns.ledger.record_order_pending(
            request,
            event_id=event_id,
            leg=leg,
            intent_id=intent_id,
            expires_at=expires_at,
            fill_model=ns.adapter.fill_model if ns.adapter.fill_model != "exchange" else None,
        )
    try:
        snap = await ns.adapter.place_order(request)
    except OrderRejectedError as exc:
        if exc.code == CODE_DUPLICATE_CLIENT_ID:  # an earlier attempt landed: adopt it
            found = await ns.adapter.query_order(request.symbol, request.client_id)
            if found is not None:
                ns.ledger.update_order(found)
                return Placed(found)
        ns.ledger.mark_order(request.client_id, "REJECTED", f"{exc.code}: {exc}")
        return Placed(None, exc)
    except UnknownOrderStateError as exc:
        await asyncio.sleep(UNKNOWN_VERIFY_DELAY_S)
        found = await ns.adapter.query_order(request.symbol, request.client_id)
        if found is None:
            ns.ledger.mark_order(request.client_id, "REJECTED", f"unknown state, absent on verify: {exc}")
            return Placed(None, exc)
        ns.ledger.update_order(found)
        return Placed(found)
    ns.ledger.update_order(snap)
    return Placed(snap)


async def place_algo(
    ns: Namespace,
    request: AlgoRequest,
    *,
    event_id: str,
    leg: Leg,
    intent_id: str | None,
    link_id: str | None = None,
) -> Placed:
    existing = ns.ledger.algo(request.client_algo_id)
    if existing is not None and existing.status != "PENDING":
        # Already sent once: only a still-working order counts as placed (a re-driven intent whose stop
        # was cancelled or replaced since must not look like a fresh placement).
        snap = await ns.adapter.query_algo(request.client_algo_id)
        if snap is not None:
            ns.ledger.update_algo(snap)
            if snap.is_working:
                return Placed(snap)
        status = snap.status if snap is not None else existing.status
        return Placed(
            None,
            OrderRejectedError(
                f"clientAlgoId {request.client_algo_id} already used ({status}), not placed again",
                code=CODE_DUPLICATE_CLIENT_ID,
            ),
        )
    if existing is None:
        ns.ledger.record_algo_pending(
            request,
            event_id=event_id,
            leg=leg,
            intent_id=intent_id,
            fill_model=ns.adapter.fill_model if ns.adapter.fill_model != "exchange" else None,
            link_id=link_id,
        )
    try:
        snap = await ns.adapter.place_algo(request)
    except OrderRejectedError as exc:
        if exc.code == CODE_DUPLICATE_CLIENT_ID:
            found = await ns.adapter.query_algo(request.client_algo_id)
            if found is not None:
                ns.ledger.update_algo(found)
                return Placed(found)
        ns.ledger.mark_algo(request.client_algo_id, "REJECTED", f"{exc.code}: {exc}")
        return Placed(None, exc)
    except UnknownOrderStateError as exc:
        await asyncio.sleep(UNKNOWN_VERIFY_DELAY_S)
        try:
            found = await ns.adapter.query_algo(request.client_algo_id)
        except ExchangeError as verify_exc:
            log.warning(
                "algo state unknown, verify failed", extra={"account": ns.name, "error": str(verify_exc)}
            )
            return Placed(None, exc)  # stays PENDING (never counted as a covering stop)
        if found is None:
            ns.ledger.mark_algo(request.client_algo_id, "REJECTED", f"unknown state, absent on verify: {exc}")
            return Placed(None, exc)
        ns.ledger.update_algo(found)
        return Placed(found)
    except ExchangeError as exc:  # 429, 418, 503 "not processed": the request was not executed
        ns.ledger.mark_algo(request.client_algo_id, "REJECTED", f"not executed: {exc}")
        return Placed(None, exc)
    ns.ledger.update_algo(snap)
    return Placed(snap)


async def cancel_order_confirmed(ns: Namespace, symbol: str, cid: str) -> Cancelled | None:
    """Cancel and confirm a final state (cancel response or `GET` by client id); None = not confirmed.

    The result carries the exchange's real `executedQty`: the IOC fallback sizes from it, never from the
    ledger (fill events of a lost user stream may still be missing there).
    """
    for attempt in range(CANCEL_CONFIRM_ATTEMPTS):
        snap: OrderSnapshot | None = None
        try:
            snap = await ns.adapter.cancel_order(symbol, cid)
        except UnknownOrderStateError:
            snap = None
        except OrderRejectedError as exc:
            log.info("cancel rejected, verifying", extra={"account": ns.name, "code": exc.code})
        if snap is None or snap.status not in FINAL_ORDER_STATUSES:
            snap = await ns.adapter.query_order(symbol, cid)
        if snap is not None and snap.status in FINAL_ORDER_STATUSES:
            ns.ledger.update_order(snap)
            return Cancelled(snap.executed_qty, snap)
        if snap is None:
            row = ns.ledger.order(cid)
            if row is None or row.status in ("PENDING", "REJECTED"):
                # Never reached the exchange: nothing to cancel, nothing executed.
                if row is not None:
                    ns.ledger.mark_order(cid, "CANCELED", "absent on the exchange")
                return Cancelled(Decimal(0), None)
        await asyncio.sleep(CANCEL_CONFIRM_BACKOFF_S * (attempt + 1))
    return None


async def cancel_algo(ns: Namespace, client_algo_id: str) -> bool:
    """Cancel one conditional order by `clientAlgoId`; True when it is no longer working."""
    try:
        snap = await ns.adapter.cancel_algo(client_algo_id)
    except UnknownOrderStateError:
        snap = await ns.adapter.query_algo(client_algo_id)
    except OrderRejectedError:
        snap = await ns.adapter.query_algo(client_algo_id)
    if snap is None:
        ns.ledger.mark_algo(client_algo_id, "CANCELED", "absent on the exchange")
        return True
    if snap.is_working:
        snap = await ns.adapter.query_algo(client_algo_id) or snap
    ns.ledger.update_algo(snap)
    return not snap.is_working


async def close_market(
    ns: Namespace, symbol: str, signed_qty: Decimal, *, event_id: str, leg: Leg = Leg.EXIT
) -> Placed:
    """Reduce-only MARKET close of `signed_qty` (the position; no signature needed: it only reduces risk)."""
    filters = await ns.symbol_filters(symbol)
    qty = abs(signed_qty)
    if filters is not None:
        floored = filters.floor_qty(qty, market=True)
        qty = floored if floored > 0 else qty
    request = OrderRequest(
        symbol=symbol,
        side=closing_side(signed_qty),
        order_type=OrderType.MARKET,
        qty=qty,
        client_id=next_client_id(ns, event_id, leg),
        reduce_only=True,
    )
    return await place_order(ns, request, event_id=event_id, leg=leg, intent_id=None)


def ioc_request(symbol: str, side: OrderSide, qty: Decimal, price: Decimal, cid: str) -> OrderRequest:
    return OrderRequest(
        symbol=symbol,
        side=side,
        order_type=OrderType.LIMIT,
        qty=qty,
        client_id=cid,
        price=price,
        tif=TimeInForce.IOC,
    )
