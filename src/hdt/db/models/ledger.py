"""Phase 09 execution ledger, one namespace per `account` (paper | testnet | live).

Written only by `hdt_execution` (Postgres is the source of truth for the ledger; the exchange is the
source of truth for positions and fills and is reconciled every cycle). Status values:
- `orders.status`: PENDING (recorded before the request), NEW, PARTIALLY_FILLED, FILLED, CANCELED,
  EXPIRED, REJECTED, UNKNOWN (HTTP 503 / lost response, verified by client id before any retry);
- `algo_orders.status`: PENDING, NEW, TRIGGERING, TRIGGERED, FINISHED, CANCELED, REJECTED, EXPIRED,
  UNKNOWN;
- `kill_state.state`: running | paused | killed, `cause`: manual | daily_loss | reconcile | rate_limit.

`positions.realized_pnl` is net of fees and funding; `fees_funding_usd` is the part of it paid as fees
and funding (positive = cost). Quantities are absolute with an explicit `side`.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import BigInteger, Boolean, CheckConstraint, Date, Float, Identity, Index, Integer, Text, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from hdt.db.base import Base

ACCOUNTS = ("paper", "testnet", "live")
ORDER_STATUSES = (
    "PENDING",
    "NEW",
    "PARTIALLY_FILLED",
    "FILLED",
    "CANCELED",
    "EXPIRED",
    "REJECTED",
    "UNKNOWN",
)
ALGO_STATUSES = (
    "PENDING",
    "NEW",
    "TRIGGERING",
    "TRIGGERED",
    "FINISHED",
    "CANCELED",
    "REJECTED",
    "EXPIRED",
    "UNKNOWN",
)
KILL_STATES = ("running", "paused", "killed")
KILL_CAUSES = ("manual", "daily_loss", "reconcile", "rate_limit")
INTENT_STATUSES = ("accepted", "rejected", "done", "void")
SYNC_STATES = ("synced", "resyncing")
LEGS = ("entry", "entry_ioc", "sl", "tp1", "trail", "exit", "hedge")


def _in(column: str, values: tuple[str, ...]) -> str:
    return f"{column} IN ({', '.join(repr(v) for v in values)})"


class ProcessedIntentRow(Base):
    """Every signature-verified `OrderIntent` execution received (replay protection per namespace + the
    signed plan of an event). A payload that fails parsing or verification never gets a row here (it would
    occupy the replay key of a real intent): it is written to `audit_log` instead."""

    __tablename__ = "processed_intents"
    __table_args__ = (
        CheckConstraint(_in("account", ACCOUNTS), name="account"),
        CheckConstraint(_in("status", INTENT_STATUSES), name="status"),
        Index("ix_processed_intents_account_event_id", "account", "event_id"),
    )

    account: Mapped[str] = mapped_column(Text, primary_key=True)
    intent_id: Mapped[str] = mapped_column(Text, primary_key=True)
    event_id: Mapped[str] = mapped_column(Text)
    leg: Mapped[str] = mapped_column(Text)
    seq: Mapped[int] = mapped_column(Integer)
    client_id: Mapped[str] = mapped_column(Text)
    symbol: Mapped[str] = mapped_column(Text)
    key_id: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text)
    reason: Mapped[str | None] = mapped_column(Text)
    payload: Mapped[dict[str, Any]]
    received_at: Mapped[datetime]


class OrderRow(Base):
    """Regular orders (`newClientOrderId`)."""

    __tablename__ = "orders"
    __table_args__ = (
        CheckConstraint(_in("account", ACCOUNTS), name="account"),
        CheckConstraint(_in("status", ORDER_STATUSES), name="status"),
        CheckConstraint(_in("leg", LEGS), name="leg"),
        Index("ix_orders_account_status", "account", "status"),
        Index("ix_orders_account_event_id", "account", "event_id"),
    )

    account: Mapped[str] = mapped_column(Text, primary_key=True)
    client_id: Mapped[str] = mapped_column(Text, primary_key=True)
    event_id: Mapped[str] = mapped_column(Text)
    intent_id: Mapped[str | None] = mapped_column(Text)
    symbol: Mapped[str] = mapped_column(Text)
    leg: Mapped[str] = mapped_column(Text)
    side: Mapped[str] = mapped_column(Text)
    order_type: Mapped[str] = mapped_column(Text)
    tif: Mapped[str | None] = mapped_column(Text)
    price: Mapped[Decimal | None]
    qty: Mapped[Decimal]
    executed_qty: Mapped[Decimal]
    avg_price: Mapped[Decimal | None]
    reduce_only: Mapped[bool] = mapped_column(Boolean)
    status: Mapped[str] = mapped_column(Text)
    exchange_order_id: Mapped[int | None] = mapped_column(BigInteger)
    fill_model: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime]
    updated_at: Mapped[datetime]
    expires_at: Mapped[datetime | None]
    last_error: Mapped[str | None] = mapped_column(Text)


class AlgoOrderRow(Base):
    """Conditional STOP_MARKET / TAKE_PROFIT_MARKET orders (`clientAlgoId`, `POST /fapi/v1/algoOrder`)."""

    __tablename__ = "algo_orders"
    __table_args__ = (
        CheckConstraint(_in("account", ACCOUNTS), name="account"),
        CheckConstraint(_in("status", ALGO_STATUSES), name="status"),
        CheckConstraint(_in("leg", LEGS), name="leg"),
        Index("ix_algo_orders_account_status", "account", "status"),
        Index("ix_algo_orders_account_symbol", "account", "symbol"),
    )

    account: Mapped[str] = mapped_column(Text, primary_key=True)
    client_algo_id: Mapped[str] = mapped_column(Text, primary_key=True)
    event_id: Mapped[str] = mapped_column(Text)
    intent_id: Mapped[str | None] = mapped_column(Text)
    symbol: Mapped[str] = mapped_column(Text)
    leg: Mapped[str] = mapped_column(Text)
    side: Mapped[str] = mapped_column(Text)
    order_type: Mapped[str] = mapped_column(Text)
    trigger_price: Mapped[Decimal]
    qty: Mapped[Decimal | None]
    close_position: Mapped[bool] = mapped_column(Boolean)
    reduce_only: Mapped[bool] = mapped_column(Boolean)
    working_type: Mapped[str] = mapped_column(Text)
    link_id: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text)
    algo_id: Mapped[int | None] = mapped_column(BigInteger)
    triggered_order_id: Mapped[int | None] = mapped_column(BigInteger)
    fill_model: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime]
    updated_at: Mapped[datetime]
    triggered_at: Mapped[datetime | None]
    last_error: Mapped[str | None] = mapped_column(Text)


class FillRow(Base):
    """Executions (one row per exchange trade id; Binance trade ids are unique per symbol)."""

    __tablename__ = "fills"
    __table_args__ = (
        CheckConstraint(_in("account", ACCOUNTS), name="account"),
        Index("ix_fills_account_filled_at", "account", "filled_at"),
        Index("ix_fills_account_client_id", "account", "client_id"),
    )

    account: Mapped[str] = mapped_column(Text, primary_key=True)
    symbol: Mapped[str] = mapped_column(Text, primary_key=True)
    trade_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    client_id: Mapped[str] = mapped_column(Text)
    exchange_order_id: Mapped[int | None] = mapped_column(BigInteger)
    event_id: Mapped[str | None] = mapped_column(Text)
    leg: Mapped[str | None] = mapped_column(Text)
    side: Mapped[str] = mapped_column(Text)
    price: Mapped[Decimal]
    qty: Mapped[Decimal]
    fee_usd: Mapped[Decimal | None]
    fee_asset: Mapped[str | None] = mapped_column(Text)
    maker: Mapped[bool | None] = mapped_column(Boolean)
    realized_pnl: Mapped[Decimal]
    fill_model: Mapped[str | None] = mapped_column(Text)
    position_id: Mapped[int | None] = mapped_column(BigInteger)
    filled_at: Mapped[datetime]


class PositionRow(Base):
    """Position ledger: one row per position lifecycle, at most one open row per (account, symbol)."""

    __tablename__ = "positions"
    __table_args__ = (
        CheckConstraint(_in("account", ACCOUNTS), name="account"),
        CheckConstraint("side IN ('LONG', 'SHORT')", name="side"),
        Index(
            "uq_positions_account_symbol_open",
            "account",
            "symbol",
            unique=True,
            postgresql_where=text("closed_at IS NULL"),
        ),
        Index("ix_positions_account_closed_at", "account", "closed_at"),
    )

    position_id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    account: Mapped[str] = mapped_column(Text)
    event_id: Mapped[str | None] = mapped_column(Text)
    symbol: Mapped[str] = mapped_column(Text)
    side: Mapped[str] = mapped_column(Text)
    qty: Mapped[Decimal]
    max_qty: Mapped[Decimal]
    entry_price: Mapped[Decimal]
    mark_price: Mapped[Decimal | None]
    unrealized_pnl: Mapped[Decimal]
    leverage: Mapped[int | None] = mapped_column(Integer)
    margin_type: Mapped[str | None] = mapped_column(Text)
    liquidation_price: Mapped[Decimal | None]
    stop_price: Mapped[Decimal | None]
    initial_stop_price: Mapped[Decimal | None]
    tp1_price: Mapped[Decimal | None]
    trail_distance: Mapped[Decimal | None]
    best_price: Mapped[Decimal | None]
    is_hedge_book: Mapped[bool] = mapped_column(Boolean)
    opened_at: Mapped[datetime]
    time_stop_at: Mapped[datetime | None]
    closed_at: Mapped[datetime | None]
    exit_price: Mapped[Decimal | None]
    exit_reason: Mapped[str | None] = mapped_column(Text)
    exit_notional: Mapped[Decimal]
    exit_qty: Mapped[Decimal]
    realized_pnl: Mapped[Decimal]
    fees_funding_usd: Mapped[Decimal]
    r_multiple: Mapped[float | None] = mapped_column(Float)
    exit_in_progress: Mapped[bool] = mapped_column(Boolean)
    tp1_done: Mapped[bool] = mapped_column(Boolean)
    updated_at: Mapped[datetime]


class CashFlowRow(Base):
    """Non-trade wallet changes: funding (`FUNDING_FEE`), transfers, insurance clear, etc.

    `amount_usd` > 0 credits the wallet. Realized PnL and commissions are on `fills`, never here.
    """

    __tablename__ = "cash_flows"
    __table_args__ = (
        CheckConstraint(_in("account", ACCOUNTS), name="account"),
        Index("ix_cash_flows_account_occurred_at", "account", "occurred_at"),
    )

    account: Mapped[str] = mapped_column(Text, primary_key=True)
    flow_id: Mapped[str] = mapped_column(Text, primary_key=True)
    kind: Mapped[str] = mapped_column(Text)
    symbol: Mapped[str | None] = mapped_column(Text)
    amount_usd: Mapped[Decimal]
    asset: Mapped[str] = mapped_column(Text)
    rate: Mapped[Decimal | None]
    occurred_at: Mapped[datetime]
    position_id: Mapped[int | None] = mapped_column(BigInteger)


class EquitySnapshotRow(Base):
    """`AccountState` equity history (one row per snapshot interval)."""

    __tablename__ = "equity_snapshots"
    __table_args__ = (CheckConstraint(_in("account", ACCOUNTS), name="account"),)

    account: Mapped[str] = mapped_column(Text, primary_key=True)
    ts: Mapped[datetime] = mapped_column(primary_key=True)
    equity: Mapped[Decimal]
    available: Mapped[Decimal]
    day_start_equity: Mapped[Decimal]
    wallet_balance: Mapped[Decimal]
    unrealized_pnl: Mapped[Decimal]
    open_positions: Mapped[int] = mapped_column(Integer)


class KillStateRow(Base):
    """Run state per account (one row, upserted). Resume rules live in `hdt.execution.kill_switch`."""

    __tablename__ = "kill_state"
    __table_args__ = (
        CheckConstraint(_in("account", ACCOUNTS), name="account"),
        CheckConstraint(_in("state", KILL_STATES), name="state"),
        CheckConstraint(f"cause IS NULL OR {_in('cause', KILL_CAUSES)}", name="cause"),
    )

    account: Mapped[str] = mapped_column(Text, primary_key=True)
    state: Mapped[str] = mapped_column(Text)
    cause: Mapped[str | None] = mapped_column(Text)
    reason: Mapped[str | None] = mapped_column(Text)
    since: Mapped[datetime | None]
    updated_at: Mapped[datetime]
    command_id: Mapped[str | None] = mapped_column(Text)


class ReconcileRunRow(Base):
    """One reconciler cycle: exchange vs ledger (net per symbol) and the STOP invariant."""

    __tablename__ = "reconcile_runs"
    __table_args__ = (
        CheckConstraint(_in("account", ACCOUNTS), name="account"),
        Index("ix_reconcile_runs_account_ran_at", "account", "ran_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    account: Mapped[str] = mapped_column(Text)
    ran_at: Mapped[datetime]
    clean: Mapped[bool] = mapped_column(Boolean)
    stop_invariant_ok: Mapped[bool] = mapped_column(Boolean)
    detail: Mapped[str | None] = mapped_column(Text)
    mismatches: Mapped[list[dict[str, Any]]] = mapped_column(JSONB)


class ExecAccountRow(Base):
    """Per-namespace execution state: sync state, UTC day start equity, wallet anchor, daily notional and
    the end of an exchange IP ban (HTTP 418) that every process calling the exchange must honour."""

    __tablename__ = "exec_accounts"
    __table_args__ = (
        CheckConstraint(_in("account", ACCOUNTS), name="account"),
        CheckConstraint(_in("sync_state", SYNC_STATES), name="sync_state"),
    )

    account: Mapped[str] = mapped_column(Text, primary_key=True)
    sync_state: Mapped[str] = mapped_column(Text)
    day: Mapped[date | None] = mapped_column(Date)
    day_start_equity: Mapped[Decimal | None]
    wallet_anchor: Mapped[Decimal | None]
    anchor_at: Mapped[datetime | None]
    daily_notional_day: Mapped[date | None] = mapped_column(Date)
    daily_notional_usd: Mapped[Decimal]
    banned_until: Mapped[datetime | None]
    updated_at: Mapped[datetime]


class PaperStateRow(Base):
    """The paper exchange (simulated venue) persisted as one document per paper account.

    `state` holds the simulated wallet, positions, resting orders with their estimated queue ahead,
    working conditional orders, recent fills and funding bookkeeping; `market_cursor` is the lake time the
    engine has replayed up to. It is the paper venue's own truth, independent of the execution ledger, so
    the reconciler compares two separate books exactly as it does for Binance.
    """

    __tablename__ = "paper_state"
    __table_args__ = (CheckConstraint("account = 'paper'", name="account"),)

    account: Mapped[str] = mapped_column(Text, primary_key=True)
    state: Mapped[dict[str, Any]]
    market_cursor: Mapped[datetime | None]
    updated_at: Mapped[datetime]
