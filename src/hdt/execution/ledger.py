"""Postgres execution ledger of one namespace (tables of `hdt.db.models.ledger`, written only by execution).

Postgres is the source of truth for what execution did (intents, orders, fills, positions); the exchange is
the source of truth for what happened and is reconciled every cycle. Every write is its own short
transaction and idempotent on a natural key, so replaying an exchange event or a REST re-sync never double
counts:
- `processed_intents` by `(account, intent_id)` (replay protection of signature-verified intents),
- `orders` / `algo_orders` by client id (recorded PENDING before the request is sent),
- `fills` by `(account, symbol, trade_id)`; a fill is applied to the position in the same transaction,
- `cash_flows` by `flow_id`.

Positions are net per symbol (One-way mode): BUY fills add, SELL fills subtract. A position row lives from
the first fill until the net quantity is zero; a fill crossing zero closes the row and opens a new one with
the remainder. `realized_pnl` on the row is net of fees (USDT commissions) and funding.

Calls are synchronous (one short transaction each); callers run them on the event loop thread and hold the
namespace lock, so ledger writes are ordered exactly like the exchange events that caused them.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Final

import sqlalchemy as sa
from sqlalchemy.orm import Session, sessionmaker

from hdt.contracts.common import Account, Leg
from hdt.contracts.order import OrderIntent
from hdt.core.clock import utcnow
from hdt.db.dedupe import insert_once
from hdt.db.models.ledger import (
    AlgoOrderRow,
    CashFlowRow,
    EquitySnapshotRow,
    ExecAccountRow,
    FillRow,
    KillStateRow,
    OrderRow,
    PositionRow,
    ProcessedIntentRow,
    ReconcileRunRow,
)
from hdt.db.session import transaction
from hdt.execution.adapter_base import (
    AlgoRequest,
    AlgoSnapshot,
    CashFlow,
    FillModel,
    OrderRequest,
    OrderSnapshot,
    TradeFill,
)

ZERO: Final[Decimal] = Decimal(0)
FINAL_ORDER_STATES: Final[tuple[str, ...]] = ("FILLED", "CANCELED", "EXPIRED", "REJECTED")
MARGIN_ASSET: Final[str] = "USDT"
OPEN_ORDER_STATES: Final[tuple[str, ...]] = ("PENDING", "NEW", "PARTIALLY_FILLED", "UNKNOWN")
WORKING_ALGO_STATES: Final[tuple[str, ...]] = ("PENDING", "NEW", "TRIGGERING", "UNKNOWN")
HEDGE_EVENT_PREFIX: Final[str] = "hedge:"
HEDGE_SEQ_PATTERN: Final[str] = ":([0-9]{1,18})$"
"""POSIX regex of the rebalance seq ending a hedge event id (`hedge:{account}:{seq}`; fits a bigint)."""
KILL_CAUSE_RANK: Final[dict[str, int]] = {"manual": 0, "rate_limit": 1, "reconcile": 2, "daily_loss": 3}
"""Strictness of the kill causes: while killed, a stricter cause is never replaced by a weaker one."""
SINCE_RULED_CAUSES: Final[frozenset[str]] = frozenset({"daily_loss", "reconcile"})
"""Kill causes whose resume rule reads `since` (same UTC day; a clean reconcile after the kill)."""
EXIT_REASON_BY_LEG: Final[dict[str, str]] = {
    Leg.SL.value: "stop",
    Leg.TP1.value: "tp1",
    Leg.TRAIL.value: "trail",
    Leg.EXIT.value: "exit",
    Leg.HEDGE.value: "hedge",
}
_ALGO_STATUS: Final[dict[str, str]] = {
    "NEW": "NEW",
    "WORKING": "NEW",
    "TRIGGERING": "TRIGGERING",
    "TRIGGERED": "TRIGGERED",
    "FINISHED": "FINISHED",
    "CANCELED": "CANCELED",
    "CANCELLED": "CANCELED",
    "REJECTED": "REJECTED",
    "EXPIRED": "EXPIRED",
}
_ORDER_STATUS: Final[dict[str, str]] = {
    "NEW": "NEW",
    "PARTIALLY_FILLED": "PARTIALLY_FILLED",
    "FILLED": "FILLED",
    "CANCELED": "CANCELED",
    "EXPIRED": "EXPIRED",
    "EXPIRED_IN_MATCH": "EXPIRED",
    "REJECTED": "REJECTED",
}


def is_hedge_event(event_id: str | None) -> bool:
    return event_id is not None and event_id.startswith(HEDGE_EVENT_PREFIX)


def signed_qty(position: PositionRow) -> Decimal:
    return position.qty if position.side == "LONG" else -position.qty


def kill_cause_rank(cause: str | None) -> int:
    """Strictness of a kill cause (a missing cause counts as `manual`, like the kill switch treats it)."""
    return KILL_CAUSE_RANK.get(cause or "manual", 0)


@dataclass(frozen=True)
class FillApplied:
    inserted: bool
    leg: str | None
    event_id: str | None
    position_id: int | None
    opened: bool = False
    closed: bool = False
    net_after: Decimal = ZERO


class Ledger:
    """Ledger rows of one namespace."""

    def __init__(self, sessions: sessionmaker[Session], account: Account) -> None:
        self.sessions = sessions
        self.account = account
        self._a = account.value

    # ------------------------------------------------------------------ intents
    def record_intent(self, intent: OrderIntent, *, status: str, reason: str | None = None) -> bool:
        """Insert a signature-verified intent once (by `(account, intent_id)`); False when it was already
        received (replay). Only verified intents may occupy the replay key."""
        if intent.account is not self.account:
            raise ValueError(f"intent of {intent.account.value} recorded in the {self._a} ledger")
        with transaction(self.sessions) as s:
            return insert_once(
                s.connection(),
                ProcessedIntentRow.__table__,  # type: ignore[arg-type]
                {
                    "account": self._a,
                    "intent_id": intent.intent_id,
                    "event_id": intent.event_id,
                    "leg": intent.leg.value,
                    "seq": intent.seq,
                    "client_id": intent.client_id,
                    "symbol": intent.symbol,
                    "key_id": intent.key_id,
                    "status": status,
                    "reason": reason,
                    "payload": intent.model_dump(mode="json"),
                    "received_at": utcnow(),
                },
            )

    def intent_row(self, intent_id: str) -> ProcessedIntentRow | None:
        """This namespace's recorded intent `intent_id` (its stored, verified payload and status)."""
        with self.sessions() as s:
            return s.get(ProcessedIntentRow, (self._a, intent_id))

    def set_intent_status(self, intent_id: str, status: str, reason: str | None = None) -> None:
        with transaction(self.sessions) as s:
            s.execute(
                sa.update(ProcessedIntentRow)
                .where(ProcessedIntentRow.account == self._a, ProcessedIntentRow.intent_id == intent_id)
                .values(status=status, reason=reason)
            )

    def event_intents(
        self, event_id: str, *, statuses: Sequence[str] = ("accepted", "done")
    ) -> list[OrderIntent]:
        """Trusted intents of `event_id` (every recorded intent is signature verified), oldest first."""
        with self.sessions() as s:
            rows = s.scalars(
                sa.select(ProcessedIntentRow)
                .where(
                    ProcessedIntentRow.account == self._a,
                    ProcessedIntentRow.event_id == event_id,
                    ProcessedIntentRow.status.in_(statuses),
                )
                .order_by(ProcessedIntentRow.received_at, ProcessedIntentRow.intent_id)
            ).all()
            return [OrderIntent.model_validate(r.payload) for r in rows]

    def event_leg(self, event_id: str, leg: Leg) -> OrderIntent | None:
        """Newest trusted intent of `leg` for `event_id`."""
        found = [i for i in self.event_intents(event_id) if i.leg is leg]
        return found[-1] if found else None

    def count_event_leg(self, event_id: str, leg: Leg) -> int:
        with self.sessions() as s:
            return int(
                s.scalar(
                    sa.select(sa.func.count())
                    .select_from(ProcessedIntentRow)
                    .where(
                        ProcessedIntentRow.account == self._a,
                        ProcessedIntentRow.event_id == event_id,
                        ProcessedIntentRow.leg == leg.value,
                        ProcessedIntentRow.status.in_(("accepted", "done")),
                    )
                )
                or 0
            )

    def accepted_intents(self, legs: Iterable[Leg]) -> list[OrderIntent]:
        """Trusted intents still waiting to be acted upon (status `accepted`)."""
        with self.sessions() as s:
            rows = s.scalars(
                sa.select(ProcessedIntentRow)
                .where(
                    ProcessedIntentRow.account == self._a,
                    ProcessedIntentRow.status == "accepted",
                    ProcessedIntentRow.leg.in_([leg.value for leg in legs]),
                )
                .order_by(ProcessedIntentRow.received_at)
            ).all()
            return [OrderIntent.model_validate(r.payload) for r in rows]

    def latest_hedge_intent(self, leg: Leg) -> OrderIntent | None:
        """Trusted intent of `leg` from the newest hedge-book event (the book's current stop plan).

        Newest by the hedge seq Risk signed into the event id (`hedge:{account}:{seq}`, allocated in order
        from the `hedge_book` rows), never by arrival: a message left pending is reclaimed after newer
        rebalances, and its older stop must not replace theirs."""
        seq = sa.cast(sa.func.substring(ProcessedIntentRow.event_id, HEDGE_SEQ_PATTERN), sa.BigInteger)
        with self.sessions() as s:
            row = s.scalars(
                sa.select(ProcessedIntentRow)
                .where(
                    ProcessedIntentRow.account == self._a,
                    ProcessedIntentRow.event_id.startswith(HEDGE_EVENT_PREFIX),
                    ProcessedIntentRow.leg == leg.value,
                    ProcessedIntentRow.status.in_(("accepted", "done")),
                )
                .order_by(seq.desc().nulls_last(), ProcessedIntentRow.received_at.desc())
                .limit(1)
            ).first()
            return OrderIntent.model_validate(row.payload) if row is not None else None

    # ------------------------------------------------------------------ client ids
    def client_ids_for_event(self, event_id: str) -> list[str]:
        """Every client id (regular and algo) already used by `event_id` (for `next_seq`)."""
        with self.sessions() as s:
            regular = s.scalars(
                sa.select(OrderRow.client_id).where(
                    OrderRow.account == self._a, OrderRow.event_id == event_id
                )
            ).all()
            algo = s.scalars(
                sa.select(AlgoOrderRow.client_algo_id).where(
                    AlgoOrderRow.account == self._a, AlgoOrderRow.event_id == event_id
                )
            ).all()
            return [*regular, *algo]

    # ------------------------------------------------------------------ regular orders
    def record_order_pending(
        self,
        request: OrderRequest,
        *,
        event_id: str,
        leg: Leg,
        intent_id: str | None,
        expires_at: datetime | None,
        fill_model: FillModel | None,
    ) -> bool:
        """Record the order before the request is sent (False if the client id already exists)."""
        now = utcnow()
        with transaction(self.sessions) as s:
            return insert_once(
                s.connection(),
                OrderRow.__table__,  # type: ignore[arg-type]
                {
                    "account": self._a,
                    "client_id": request.client_id,
                    "event_id": event_id,
                    "intent_id": intent_id,
                    "symbol": request.symbol,
                    "leg": leg.value,
                    "side": request.side.value,
                    "order_type": request.order_type.value,
                    "tif": request.tif.value if request.tif else None,
                    "price": request.price,
                    "qty": request.qty,
                    "executed_qty": ZERO,
                    "avg_price": None,
                    "reduce_only": request.reduce_only,
                    "status": "PENDING",
                    "exchange_order_id": None,
                    "fill_model": fill_model,
                    "created_at": now,
                    "updated_at": now,
                    "expires_at": expires_at,
                    "last_error": None,
                },
            )

    def update_order(self, snap: OrderSnapshot, *, fill_model: FillModel | None = None) -> OrderRow | None:
        """Apply an exchange snapshot; statuses never move backwards from a final state."""
        with transaction(self.sessions) as s:
            row = s.get(OrderRow, (self._a, snap.client_id), with_for_update=True)
            if row is None:
                return None
            status = _ORDER_STATUS.get(snap.status, "UNKNOWN")
            if snap.executed_qty > row.executed_qty:
                row.executed_qty = snap.executed_qty
                row.avg_price = snap.avg_price or row.avg_price
            if row.status not in FINAL_ORDER_STATES:
                row.status = status
            row.exchange_order_id = snap.order_id or row.exchange_order_id
            if fill_model is not None and fill_model != "exchange":
                row.fill_model = fill_model
            row.updated_at = utcnow()
            return row

    def mark_order(self, client_id: str, status: str, error: str | None = None) -> None:
        with transaction(self.sessions) as s:
            s.execute(
                sa.update(OrderRow)
                .where(OrderRow.account == self._a, OrderRow.client_id == client_id)
                .values(status=status, last_error=error, updated_at=utcnow())
            )

    def order(self, client_id: str) -> OrderRow | None:
        with self.sessions() as s:
            return s.get(OrderRow, (self._a, client_id))

    def order_by_exchange_id(self, symbol: str, order_id: int) -> OrderRow | None:
        with self.sessions() as s:
            return s.scalars(
                sa.select(OrderRow).where(
                    OrderRow.account == self._a,
                    OrderRow.symbol == symbol,
                    OrderRow.exchange_order_id == order_id,
                )
            ).first()

    def open_orders(self, symbol: str | None = None) -> list[OrderRow]:
        with self.sessions() as s:
            q = sa.select(OrderRow).where(OrderRow.account == self._a, OrderRow.status.in_(OPEN_ORDER_STATES))
            if symbol is not None:
                q = q.where(OrderRow.symbol == symbol)
            return list(s.scalars(q.order_by(OrderRow.created_at)).all())

    def event_orders(self, event_id: str, legs: Iterable[Leg] | None = None) -> list[OrderRow]:
        with self.sessions() as s:
            q = sa.select(OrderRow).where(OrderRow.account == self._a, OrderRow.event_id == event_id)
            if legs is not None:
                q = q.where(OrderRow.leg.in_([leg.value for leg in legs]))
            return list(s.scalars(q.order_by(OrderRow.created_at)).all())

    # ------------------------------------------------------------------ algo orders
    def record_algo_pending(
        self,
        request: AlgoRequest,
        *,
        event_id: str,
        leg: Leg,
        intent_id: str | None,
        fill_model: FillModel | None,
        link_id: str | None = None,
    ) -> bool:
        now = utcnow()
        with transaction(self.sessions) as s:
            return insert_once(
                s.connection(),
                AlgoOrderRow.__table__,  # type: ignore[arg-type]
                {
                    "account": self._a,
                    "client_algo_id": request.client_algo_id,
                    "event_id": event_id,
                    "intent_id": intent_id,
                    "symbol": request.symbol,
                    "leg": leg.value,
                    "side": request.side.value,
                    "order_type": request.order_type.value,
                    "trigger_price": request.trigger_price,
                    "qty": request.qty,
                    "close_position": request.close_position,
                    "reduce_only": request.reduce_only,
                    "working_type": request.working_type,
                    "link_id": link_id,
                    "status": "PENDING",
                    "algo_id": None,
                    "triggered_order_id": None,
                    "fill_model": fill_model,
                    "created_at": now,
                    "updated_at": now,
                    "triggered_at": None,
                    "last_error": None,
                },
            )

    def update_algo(self, snap: AlgoSnapshot) -> AlgoOrderRow | None:
        with transaction(self.sessions) as s:
            row = s.get(AlgoOrderRow, (self._a, snap.client_algo_id), with_for_update=True)
            if row is None:
                return None
            status = _ALGO_STATUS.get(snap.status.upper(), "UNKNOWN")
            if row.status in ("TRIGGERED", "FINISHED", "CANCELED", "REJECTED", "EXPIRED") and status in (
                "NEW",
                "UNKNOWN",
            ):
                return row
            row.status = status
            row.algo_id = snap.algo_id or row.algo_id
            if snap.triggered_order_id is not None:
                row.triggered_order_id = snap.triggered_order_id
            if status in ("TRIGGERING", "TRIGGERED", "FINISHED") and row.triggered_at is None:
                row.triggered_at = utcnow()
            row.updated_at = utcnow()
            return row

    def mark_algo(self, client_algo_id: str, status: str, error: str | None = None) -> None:
        with transaction(self.sessions) as s:
            s.execute(
                sa.update(AlgoOrderRow)
                .where(AlgoOrderRow.account == self._a, AlgoOrderRow.client_algo_id == client_algo_id)
                .values(status=status, last_error=error, updated_at=utcnow())
            )

    def algo(self, client_algo_id: str) -> AlgoOrderRow | None:
        with self.sessions() as s:
            return s.get(AlgoOrderRow, (self._a, client_algo_id))

    def algo_by_triggered_order(self, order_id: int) -> AlgoOrderRow | None:
        with self.sessions() as s:
            return s.scalars(
                sa.select(AlgoOrderRow).where(
                    AlgoOrderRow.account == self._a, AlgoOrderRow.triggered_order_id == order_id
                )
            ).first()

    def working_algos(self, symbol: str | None = None) -> list[AlgoOrderRow]:
        with self.sessions() as s:
            q = sa.select(AlgoOrderRow).where(
                AlgoOrderRow.account == self._a, AlgoOrderRow.status.in_(WORKING_ALGO_STATES)
            )
            if symbol is not None:
                q = q.where(AlgoOrderRow.symbol == symbol)
            return list(s.scalars(q.order_by(AlgoOrderRow.created_at)).all())

    # ------------------------------------------------------------------ fills and positions
    def leg_of(self, client_id: str, symbol: str, order_id: int | None) -> tuple[str | None, str | None]:
        """(leg, event_id) of an exchange order: by client id, then by the algo order that triggered it."""
        with self.sessions() as s:
            if client_id:
                row = s.get(OrderRow, (self._a, client_id))
                if row is not None:
                    return row.leg, row.event_id
                algo = s.get(AlgoOrderRow, (self._a, client_id))
                if algo is not None:
                    return algo.leg, algo.event_id
            if order_id is not None:
                regular = s.scalars(
                    sa.select(OrderRow).where(
                        OrderRow.account == self._a,
                        OrderRow.symbol == symbol,
                        OrderRow.exchange_order_id == order_id,
                    )
                ).first()
                if regular is not None:
                    return regular.leg, regular.event_id
                triggered = s.scalars(
                    sa.select(AlgoOrderRow).where(
                        AlgoOrderRow.account == self._a, AlgoOrderRow.triggered_order_id == order_id
                    )
                ).first()
                if triggered is not None:
                    return triggered.leg, triggered.event_id
        return None, None

    def apply_fill(self, fill: TradeFill, *, fill_model: FillModel | None = None) -> FillApplied:
        """Insert the fill once and apply it to the net position of its symbol (same transaction)."""
        leg, event_id = self.leg_of(fill.client_id, fill.symbol, fill.order_id)
        fee_usd = fill.fee if fill.fee_asset == MARGIN_ASSET else None
        with transaction(self.sessions) as s:
            inserted = insert_once(
                s.connection(),
                FillRow.__table__,  # type: ignore[arg-type]
                {
                    "account": self._a,
                    "symbol": fill.symbol,
                    "trade_id": fill.trade_id,
                    "client_id": fill.client_id,
                    "exchange_order_id": fill.order_id,
                    "event_id": event_id,
                    "leg": leg,
                    "side": fill.side.value,
                    "price": fill.price,
                    "qty": fill.qty,
                    "fee_usd": fee_usd,
                    "fee_asset": fill.fee_asset,
                    "maker": fill.maker,
                    "realized_pnl": fill.realized_pnl,
                    "fill_model": fill_model if fill_model != "exchange" else None,
                    "position_id": None,
                    "filled_at": fill.time,
                },
            )
            if not inserted:
                return FillApplied(False, leg, event_id, None)
            result = _apply_to_position(s, self._a, fill, leg, event_id, fee_usd or ZERO)
            s.execute(
                sa.update(FillRow)
                .where(
                    FillRow.account == self._a,
                    FillRow.symbol == fill.symbol,
                    FillRow.trade_id == fill.trade_id,
                )
                .values(position_id=result.position_id)
            )
            return result

    def open_positions(self) -> list[PositionRow]:
        with self.sessions() as s:
            return list(
                s.scalars(
                    sa.select(PositionRow)
                    .where(PositionRow.account == self._a, PositionRow.closed_at.is_(None))
                    .order_by(PositionRow.opened_at)
                ).all()
            )

    def position(self, symbol: str) -> PositionRow | None:
        with self.sessions() as s:
            return _open_position(s, self._a, symbol, lock=False)

    def update_position(self, position_id: int, **values: Any) -> None:
        values["updated_at"] = utcnow()
        with transaction(self.sessions) as s:
            s.execute(sa.update(PositionRow).where(PositionRow.position_id == position_id).values(**values))

    def filled_qty(self, client_id: str) -> Decimal:
        """Sum of the ledger's fills of one order (compared with the exchange's `executedQty`)."""
        with self.sessions() as s:
            value = s.scalar(
                sa.select(sa.func.coalesce(sa.func.sum(FillRow.qty), 0)).where(
                    FillRow.account == self._a, FillRow.client_id == client_id
                )
            )
            return Decimal(value or 0)

    def known_order_ids(self, symbol: str) -> dict[int, str]:
        """Exchange order id -> client id for every order of `symbol` the ledger knows (trade attribution)."""
        with self.sessions() as s:
            regular = s.execute(
                sa.select(OrderRow.exchange_order_id, OrderRow.client_id).where(
                    OrderRow.account == self._a,
                    OrderRow.symbol == symbol,
                    OrderRow.exchange_order_id.is_not(None),
                )
            ).all()
            triggered = s.execute(
                sa.select(AlgoOrderRow.triggered_order_id, AlgoOrderRow.client_algo_id).where(
                    AlgoOrderRow.account == self._a,
                    AlgoOrderRow.symbol == symbol,
                    AlgoOrderRow.triggered_order_id.is_not(None),
                )
            ).all()
            return {int(oid): cid for oid, cid in [*regular, *triggered] if oid is not None}

    def last_trade_id(self, symbol: str) -> int | None:
        with self.sessions() as s:
            value = s.scalar(
                sa.select(sa.func.max(FillRow.trade_id)).where(
                    FillRow.account == self._a, FillRow.symbol == symbol
                )
            )
            return int(value) if value is not None else None

    def traded_symbols(self, since: datetime) -> list[str]:
        with self.sessions() as s:
            fills = s.scalars(
                sa.select(FillRow.symbol)
                .where(FillRow.account == self._a, FillRow.filled_at >= since)
                .distinct()
            ).all()
            orders = s.scalars(
                sa.select(OrderRow.symbol)
                .where(OrderRow.account == self._a, OrderRow.updated_at >= since)
                .distinct()
            ).all()
            return sorted(set(fills) | set(orders))

    # ------------------------------------------------------------------ cash flows and wallet
    def record_cash_flow(self, flow: CashFlow) -> bool:
        """Insert a funding/transfer flow once; funding is also booked on the open position of its symbol."""
        with transaction(self.sessions) as s:
            position = _open_position(s, self._a, flow.symbol, lock=True) if flow.symbol else None
            inserted = insert_once(
                s.connection(),
                CashFlowRow.__table__,  # type: ignore[arg-type]
                {
                    "account": self._a,
                    "flow_id": flow.flow_id,
                    "kind": flow.kind,
                    "symbol": flow.symbol,
                    "amount_usd": flow.amount if flow.asset == MARGIN_ASSET else ZERO,
                    "asset": flow.asset,
                    "rate": None,
                    "occurred_at": flow.time,
                    "position_id": position.position_id if position is not None else None,
                },
            )
            if (
                inserted
                and position is not None
                and flow.kind == "FUNDING_FEE"
                and flow.asset == MARGIN_ASSET
            ):
                position.realized_pnl += flow.amount
                position.fees_funding_usd -= flow.amount
                position.updated_at = utcnow()
            return inserted

    def last_cash_flow_at(self) -> datetime | None:
        """Newest `occurred_at` of the recorded cash flows (the income fetch cursor)."""
        with self.sessions() as s:
            return s.scalar(
                sa.select(sa.func.max(CashFlowRow.occurred_at)).where(CashFlowRow.account == self._a)
            )

    def expected_wallet(self) -> Decimal | None:
        """Wallet implied by the ledger: anchor + realized PnL - USDT fees + cash flows since the anchor."""
        with self.sessions() as s:
            acct = s.get(ExecAccountRow, self._a)
            if acct is None or acct.wallet_anchor is None or acct.anchor_at is None:
                return None
            pnl = s.scalar(
                sa.select(sa.func.coalesce(sa.func.sum(FillRow.realized_pnl), 0)).where(
                    FillRow.account == self._a, FillRow.filled_at > acct.anchor_at
                )
            )
            fees = s.scalar(
                sa.select(sa.func.coalesce(sa.func.sum(FillRow.fee_usd), 0)).where(
                    FillRow.account == self._a, FillRow.filled_at > acct.anchor_at
                )
            )
            flows = s.scalar(
                sa.select(sa.func.coalesce(sa.func.sum(CashFlowRow.amount_usd), 0)).where(
                    CashFlowRow.account == self._a, CashFlowRow.occurred_at > acct.anchor_at
                )
            )
            return acct.wallet_anchor + Decimal(pnl or 0) - Decimal(fees or 0) + Decimal(flows or 0)

    # ------------------------------------------------------------------ namespace state
    def _ensure_exec_account(self, s: Session) -> None:
        """Create this namespace's `exec_accounts` row if missing (ON CONFLICT DO NOTHING: another process
        such as `hdt-kill` may create it concurrently)."""
        insert_once(
            s.connection(),
            ExecAccountRow.__table__,  # type: ignore[arg-type]
            {
                "account": self._a,
                "sync_state": "resyncing",
                "day": None,
                "day_start_equity": None,
                "wallet_anchor": None,
                "anchor_at": None,
                "daily_notional_day": None,
                "daily_notional_usd": ZERO,
                "banned_until": None,
                "updated_at": utcnow(),
            },
        )

    def exec_account(self) -> ExecAccountRow:
        with transaction(self.sessions) as s:
            self._ensure_exec_account(s)
            row = s.get(ExecAccountRow, self._a)
            assert row is not None
            return row

    def set_sync_state(self, state: str) -> None:
        with transaction(self.sessions) as s:
            self._ensure_exec_account(s)
            s.execute(
                sa.update(ExecAccountRow)
                .where(ExecAccountRow.account == self._a)
                .values(sync_state=state, updated_at=utcnow())
            )

    def set_wallet_anchor(self, wallet: Decimal, at: datetime) -> None:
        with transaction(self.sessions) as s:
            self._ensure_exec_account(s)
            s.execute(
                sa.update(ExecAccountRow)
                .where(ExecAccountRow.account == self._a)
                .values(wallet_anchor=wallet, anchor_at=at, updated_at=utcnow())
            )

    def day_start_equity(self, day: date, equity_now: Decimal) -> Decimal:
        """Equity at the start of UTC `day` (the first equity seen that day becomes the anchor)."""
        with transaction(self.sessions) as s:
            self._ensure_exec_account(s)
            row = s.get(ExecAccountRow, self._a, with_for_update=True)
            assert row is not None
            if row.day != day or row.day_start_equity is None:
                row.day = day
                row.day_start_equity = equity_now
                row.updated_at = utcnow()
            return row.day_start_equity

    def daily_notional(self, day: date) -> Decimal:
        row = self.exec_account()
        return row.daily_notional_usd if row.daily_notional_day == day else ZERO

    def add_daily_notional(self, day: date, notional: Decimal) -> Decimal:
        with transaction(self.sessions) as s:
            self._ensure_exec_account(s)
            row = s.get(ExecAccountRow, self._a, with_for_update=True)
            assert row is not None
            if row.daily_notional_day != day:
                row.daily_notional_day = day
                row.daily_notional_usd = ZERO
            row.daily_notional_usd += notional
            row.updated_at = utcnow()
            return row.daily_notional_usd

    def banned_until(self) -> datetime | None:
        """End of the exchange IP ban (HTTP 418) recorded for this namespace; None when not banned now."""
        with self.sessions() as s:
            value = s.scalar(sa.select(ExecAccountRow.banned_until).where(ExecAccountRow.account == self._a))
        return value if value is not None and value > utcnow() else None

    def record_ban(self, until: datetime) -> None:
        """Record an IP ban end for every process calling this namespace's exchange (only ever extended)."""
        with transaction(self.sessions) as s:
            self._ensure_exec_account(s)
            s.execute(
                sa.update(ExecAccountRow)
                .where(ExecAccountRow.account == self._a)
                .values(
                    banned_until=sa.func.greatest(ExecAccountRow.banned_until, until),
                    updated_at=utcnow(),
                )
            )

    def kill_state(self) -> KillStateRow | None:
        with self.sessions() as s:
            return s.get(KillStateRow, self._a)

    def set_kill_state(
        self,
        state: str,
        *,
        cause: str | None,
        reason: str | None,
        command_id: str | None = None,
        restart_since: bool = True,
    ) -> KillStateRow:
        """Upsert the run state. While the row is already `killed`, a new kill keeps the strictest cause
        (`daily_loss` > `reconcile` > `rate_limit` > `manual`) with its reason and `since`, so a later
        manual kill never lifts a daily-loss or reconcile resume block. `since` restarts only on a new
        detection: a stricter cause, or a repeat of a cause whose resume rule depends on `since`
        (`daily_loss`, `reconcile`) when `restart_since` is set. Re-engaging the stored cause (`enforce`
        after a restart, a retry, a CLI kill on top) passes `restart_since=False`, so it never pushes the
        daily-loss resume to a later UTC day. The command id is always updated."""
        now = utcnow()
        with transaction(self.sessions) as s:
            insert_once(
                s.connection(),
                KillStateRow.__table__,  # type: ignore[arg-type]
                {
                    "account": self._a,
                    "state": state,
                    "cause": cause,
                    "reason": reason,
                    "since": now,
                    "updated_at": now,
                    "command_id": command_id,
                },
            )
            row = s.get(KillStateRow, self._a, with_for_update=True)
            assert row is not None
            if state == "killed" and row.state == "killed":
                rank = kill_cause_rank(cause)
                current = kill_cause_rank(row.cause)
                if (
                    rank > current
                    or row.since is None
                    or (restart_since and rank == current and cause in SINCE_RULED_CAUSES)
                ):
                    row.since = now
                if rank >= current:
                    row.cause = cause
                    row.reason = reason
            else:
                if row.state != state or row.since is None:
                    row.since = now
                row.cause = cause
                row.reason = reason
            row.state = state
            row.command_id = command_id
            row.updated_at = now
            return row

    def insert_equity_snapshot(
        self,
        *,
        ts: datetime,
        equity: Decimal,
        available: Decimal,
        day_start_equity: Decimal,
        wallet_balance: Decimal,
        unrealized_pnl: Decimal,
        open_positions: int,
    ) -> None:
        with transaction(self.sessions) as s:
            insert_once(
                s.connection(),
                EquitySnapshotRow.__table__,  # type: ignore[arg-type]
                {
                    "account": self._a,
                    "ts": ts,
                    "equity": equity,
                    "available": available,
                    "day_start_equity": day_start_equity,
                    "wallet_balance": wallet_balance,
                    "unrealized_pnl": unrealized_pnl,
                    "open_positions": open_positions,
                },
            )

    def insert_reconcile_run(
        self, *, clean: bool, stop_invariant_ok: bool, detail: str | None, mismatches: list[dict[str, Any]]
    ) -> None:
        with transaction(self.sessions) as s:
            s.add(
                ReconcileRunRow(
                    account=self._a,
                    ran_at=utcnow(),
                    clean=clean,
                    stop_invariant_ok=stop_invariant_ok,
                    detail=detail,
                    mismatches=mismatches,
                )
            )

    def last_reconcile(self) -> ReconcileRunRow | None:
        with self.sessions() as s:
            return s.scalars(
                sa.select(ReconcileRunRow)
                .where(ReconcileRunRow.account == self._a)
                .order_by(ReconcileRunRow.ran_at.desc(), ReconcileRunRow.id.desc())
                .limit(1)
            ).first()


# ---------------------------------------------------------------------- position arithmetic


def _open_position(s: Session, account: str, symbol: str | None, *, lock: bool) -> PositionRow | None:
    if symbol is None:
        return None
    q = sa.select(PositionRow).where(
        PositionRow.account == account, PositionRow.symbol == symbol, PositionRow.closed_at.is_(None)
    )
    if lock:
        q = q.with_for_update()
    return s.scalars(q).first()


def _new_position(
    s: Session, account: str, fill: TradeFill, qty: Decimal, side: str, event_id: str | None
) -> PositionRow:
    now = utcnow()
    row = PositionRow(
        account=account,
        event_id=event_id,
        symbol=fill.symbol,
        side=side,
        qty=qty,
        max_qty=qty,
        entry_price=fill.price,
        mark_price=fill.price,
        unrealized_pnl=ZERO,
        leverage=None,
        margin_type=None,
        liquidation_price=None,
        stop_price=None,
        initial_stop_price=None,
        tp1_price=None,
        trail_distance=None,
        best_price=fill.price,
        is_hedge_book=is_hedge_event(event_id),
        opened_at=fill.time,
        time_stop_at=None,
        closed_at=None,
        exit_price=None,
        exit_reason=None,
        exit_notional=ZERO,
        exit_qty=ZERO,
        realized_pnl=ZERO,
        fees_funding_usd=ZERO,
        r_multiple=None,
        exit_in_progress=False,
        tp1_done=False,
        updated_at=now,
    )
    s.add(row)
    s.flush()
    return row


def _close(row: PositionRow, fill: TradeFill, leg: str | None) -> None:
    row.closed_at = fill.time
    row.qty = ZERO
    row.exit_price = row.exit_notional / row.exit_qty if row.exit_qty > 0 else fill.price
    if row.exit_reason is None:
        row.exit_reason = EXIT_REASON_BY_LEG.get(leg or "", "unknown")
    risk = (
        abs(row.entry_price - row.initial_stop_price) * row.max_qty
        if row.initial_stop_price is not None
        else ZERO
    )
    row.r_multiple = float(row.realized_pnl / risk) if risk > 0 else None
    row.exit_in_progress = False
    row.unrealized_pnl = ZERO


def _apply_to_position(
    s: Session, account: str, fill: TradeFill, leg: str | None, event_id: str | None, fee: Decimal
) -> FillApplied:
    delta = fill.qty if fill.side.value == "BUY" else -fill.qty
    row = _open_position(s, account, fill.symbol, lock=True)
    if row is None:
        created = _new_position(s, account, fill, abs(delta), "LONG" if delta > 0 else "SHORT", event_id)
        created.realized_pnl = fill.realized_pnl - fee
        created.fees_funding_usd = fee
        return FillApplied(True, leg, event_id, created.position_id, opened=True, net_after=delta)
    current = signed_qty(row)
    after = current + delta
    row.updated_at = utcnow()
    if (current > 0) == (delta > 0):  # adding in the same direction
        total = abs(after)
        row.entry_price = (row.entry_price * row.qty + fill.price * fill.qty) / total
        row.qty = total
        row.max_qty = max(row.max_qty, total)
        row.realized_pnl += fill.realized_pnl - fee
        row.fees_funding_usd += fee
        return FillApplied(True, leg, event_id, row.position_id, net_after=after)
    closed_qty = min(abs(delta), abs(current))
    row.exit_notional += fill.price * closed_qty
    row.exit_qty += closed_qty
    row.realized_pnl += fill.realized_pnl - fee
    row.fees_funding_usd += fee
    if after == 0:
        _close(row, fill, leg)
        return FillApplied(True, leg, event_id, row.position_id, closed=True, net_after=ZERO)
    if (after > 0) == (current > 0):  # partial reduction
        row.qty = abs(after)
        return FillApplied(True, leg, event_id, row.position_id, net_after=after)
    # Crossed zero: close this row, open the remainder as a new position.
    _close(row, fill, leg)
    created = _new_position(s, account, fill, abs(after), "LONG" if after > 0 else "SHORT", event_id)
    return FillApplied(True, leg, event_id, created.position_id, opened=True, closed=True, net_after=after)
