"""Risk and execution tables: decision dedupe, verdicts, signed-intent outbox, hedge book, order ledger.

Revision ID: 0005_ledger_risk
Revises: 0004_memory
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0005_ledger_risk"
down_revision: str | None = "0004_memory"
branch_labels = None
depends_on = None

TS = sa.DateTime(timezone=True)
JSONB = postgresql.JSONB(astext_type=sa.Text())
ACCOUNT_CHECK = "account IN ('paper', 'testnet', 'live')"
LEG_CHECK = "leg IN ('entry', 'entry_ioc', 'sl', 'tp1', 'trail', 'exit', 'hedge')"

RISK_TABLES = ("processed_decisions", "risk_verdicts", "risk_intents", "hedge_book")
LEDGER_TABLES = (
    "processed_intents",
    "orders",
    "algo_orders",
    "fills",
    "positions",
    "cash_flows",
    "equity_snapshots",
    "kill_state",
    "reconcile_runs",
    "exec_accounts",
    "paper_state",
)


def _account(table: str) -> sa.CheckConstraint:
    return sa.CheckConstraint(ACCOUNT_CHECK, name=op.f(f"ck_{table}_account"))


def _risk_tables() -> None:
    op.create_table(
        "processed_decisions",
        sa.Column("account", sa.Text(), nullable=False),
        sa.Column("event_id", sa.Text(), nullable=False),
        sa.Column("processed_at", TS, nullable=False),
        _account("processed_decisions"),
        sa.PrimaryKeyConstraint("account", "event_id", name=op.f("pk_processed_decisions")),
    )
    op.create_table(
        "risk_verdicts",
        sa.Column("account", sa.Text(), nullable=False),
        sa.Column("event_id", sa.Text(), nullable=False),
        sa.Column("msg_id", sa.Text(), nullable=True),
        sa.Column("coin_id", sa.Integer(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=True),
        sa.Column("decision_intent", sa.Text(), nullable=False),
        sa.Column("effective_intent", sa.Text(), nullable=True),
        sa.Column("verdict", sa.Text(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("intent_id", sa.Text(), nullable=True),
        sa.Column("signature_ok", sa.Boolean(), nullable=True),
        sa.Column("entry_client_id", sa.Text(), nullable=True),
        sa.Column("stop_client_algo_id", sa.Text(), nullable=True),
        sa.Column("sizing", JSONB, nullable=True),
        sa.Column("checks", JSONB, nullable=False),
        sa.Column("hedge_beta", sa.Float(), nullable=True),
        sa.Column("config_version_ids", JSONB, nullable=False),
        sa.Column("decided_at", TS, nullable=False),
        _account("risk_verdicts"),
        sa.CheckConstraint(
            "verdict IN ('approved', 'rejected', 'hold', 'ignored')", name=op.f("ck_risk_verdicts_verdict")
        ),
        sa.CheckConstraint(
            "decision_intent IN ('OPEN', 'HOLD', 'EXIT')", name=op.f("ck_risk_verdicts_decision_intent")
        ),
        sa.PrimaryKeyConstraint("account", "event_id", name=op.f("pk_risk_verdicts")),
    )
    op.create_index("ix_risk_verdicts_account_decided_at", "risk_verdicts", ["account", "decided_at"])
    op.create_table(
        "risk_intents",
        sa.Column("intent_id", sa.Text(), nullable=False),
        sa.Column("account", sa.Text(), nullable=False),
        sa.Column("event_id", sa.Text(), nullable=False),
        sa.Column("leg", sa.Text(), nullable=False),
        sa.Column("seq", sa.Integer(), nullable=False),
        sa.Column("publish_order", sa.Integer(), nullable=False),
        sa.Column("payload", JSONB, nullable=False),
        sa.Column("created_at", TS, nullable=False),
        sa.Column("published_at", TS, nullable=True),
        sa.Column("stream_id", sa.Text(), nullable=True),
        _account("risk_intents"),
        sa.PrimaryKeyConstraint("intent_id", name=op.f("pk_risk_intents")),
    )
    op.create_index("ix_risk_intents_account_event_id", "risk_intents", ["account", "event_id"])
    op.create_index(
        "ix_risk_intents_unpublished",
        "risk_intents",
        ["created_at"],
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.create_table(
        "hedge_book",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("account", sa.Text(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("target_qty", sa.Numeric(), nullable=False),
        sa.Column("actual_qty", sa.Numeric(), nullable=False),
        sa.Column("target_notional", sa.Numeric(), nullable=False),
        sa.Column("beta", sa.Float(), nullable=True),
        sa.Column("btc_mark", sa.Numeric(), nullable=False),
        sa.Column("btc_atr_1h", sa.Numeric(), nullable=True),
        sa.Column("stop_price", sa.Numeric(), nullable=True),
        sa.Column("rebalance_seq", sa.Integer(), nullable=False),
        sa.Column("event_id", sa.Text(), nullable=True),
        sa.Column("updated_at", TS, nullable=False),
        _account("hedge_book"),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_hedge_book")),
    )
    op.create_index("ix_hedge_book_account_updated_at", "hedge_book", ["account", "updated_at"])


def _order_tables() -> None:
    op.create_table(
        "processed_intents",
        sa.Column("account", sa.Text(), nullable=False),
        sa.Column("intent_id", sa.Text(), nullable=False),
        sa.Column("event_id", sa.Text(), nullable=False),
        sa.Column("leg", sa.Text(), nullable=False),
        sa.Column("seq", sa.Integer(), nullable=False),
        sa.Column("client_id", sa.Text(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("key_id", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("payload", JSONB, nullable=False),
        sa.Column("received_at", TS, nullable=False),
        _account("processed_intents"),
        sa.CheckConstraint(
            "status IN ('accepted', 'rejected', 'done', 'void')", name=op.f("ck_processed_intents_status")
        ),
        sa.PrimaryKeyConstraint("account", "intent_id", name=op.f("pk_processed_intents")),
    )
    op.create_index("ix_processed_intents_account_event_id", "processed_intents", ["account", "event_id"])
    op.create_table(
        "orders",
        sa.Column("account", sa.Text(), nullable=False),
        sa.Column("client_id", sa.Text(), nullable=False),
        sa.Column("event_id", sa.Text(), nullable=False),
        sa.Column("intent_id", sa.Text(), nullable=True),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("leg", sa.Text(), nullable=False),
        sa.Column("side", sa.Text(), nullable=False),
        sa.Column("order_type", sa.Text(), nullable=False),
        sa.Column("tif", sa.Text(), nullable=True),
        sa.Column("price", sa.Numeric(), nullable=True),
        sa.Column("qty", sa.Numeric(), nullable=False),
        sa.Column("executed_qty", sa.Numeric(), nullable=False),
        sa.Column("avg_price", sa.Numeric(), nullable=True),
        sa.Column("reduce_only", sa.Boolean(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("exchange_order_id", sa.BigInteger(), nullable=True),
        sa.Column("fill_model", sa.Text(), nullable=True),
        sa.Column("created_at", TS, nullable=False),
        sa.Column("updated_at", TS, nullable=False),
        sa.Column("expires_at", TS, nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        _account("orders"),
        sa.CheckConstraint(
            "status IN ('PENDING', 'NEW', 'PARTIALLY_FILLED', 'FILLED', 'CANCELED', 'EXPIRED', "
            "'REJECTED', 'UNKNOWN')",
            name=op.f("ck_orders_status"),
        ),
        sa.CheckConstraint(LEG_CHECK, name=op.f("ck_orders_leg")),
        sa.PrimaryKeyConstraint("account", "client_id", name=op.f("pk_orders")),
    )
    op.create_index("ix_orders_account_status", "orders", ["account", "status"])
    op.create_index("ix_orders_account_event_id", "orders", ["account", "event_id"])
    op.create_table(
        "algo_orders",
        sa.Column("account", sa.Text(), nullable=False),
        sa.Column("client_algo_id", sa.Text(), nullable=False),
        sa.Column("event_id", sa.Text(), nullable=False),
        sa.Column("intent_id", sa.Text(), nullable=True),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("leg", sa.Text(), nullable=False),
        sa.Column("side", sa.Text(), nullable=False),
        sa.Column("order_type", sa.Text(), nullable=False),
        sa.Column("trigger_price", sa.Numeric(), nullable=False),
        sa.Column("qty", sa.Numeric(), nullable=True),
        sa.Column("close_position", sa.Boolean(), nullable=False),
        sa.Column("reduce_only", sa.Boolean(), nullable=False),
        sa.Column("working_type", sa.Text(), nullable=False),
        sa.Column("link_id", sa.Text(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("algo_id", sa.BigInteger(), nullable=True),
        sa.Column("triggered_order_id", sa.BigInteger(), nullable=True),
        sa.Column("fill_model", sa.Text(), nullable=True),
        sa.Column("created_at", TS, nullable=False),
        sa.Column("updated_at", TS, nullable=False),
        sa.Column("triggered_at", TS, nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        _account("algo_orders"),
        sa.CheckConstraint(
            "status IN ('PENDING', 'NEW', 'TRIGGERING', 'TRIGGERED', 'FINISHED', 'CANCELED', "
            "'REJECTED', 'EXPIRED', 'UNKNOWN')",
            name=op.f("ck_algo_orders_status"),
        ),
        sa.CheckConstraint(LEG_CHECK, name=op.f("ck_algo_orders_leg")),
        sa.PrimaryKeyConstraint("account", "client_algo_id", name=op.f("pk_algo_orders")),
    )
    op.create_index("ix_algo_orders_account_status", "algo_orders", ["account", "status"])
    op.create_index("ix_algo_orders_account_symbol", "algo_orders", ["account", "symbol"])
    op.create_table(
        "fills",
        sa.Column("account", sa.Text(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("trade_id", sa.BigInteger(), nullable=False),
        sa.Column("client_id", sa.Text(), nullable=False),
        sa.Column("exchange_order_id", sa.BigInteger(), nullable=True),
        sa.Column("event_id", sa.Text(), nullable=True),
        sa.Column("leg", sa.Text(), nullable=True),
        sa.Column("side", sa.Text(), nullable=False),
        sa.Column("price", sa.Numeric(), nullable=False),
        sa.Column("qty", sa.Numeric(), nullable=False),
        sa.Column("fee_usd", sa.Numeric(), nullable=True),
        sa.Column("fee_asset", sa.Text(), nullable=True),
        sa.Column("maker", sa.Boolean(), nullable=True),
        sa.Column("realized_pnl", sa.Numeric(), nullable=False),
        sa.Column("fill_model", sa.Text(), nullable=True),
        sa.Column("position_id", sa.BigInteger(), nullable=True),
        sa.Column("filled_at", TS, nullable=False),
        _account("fills"),
        sa.PrimaryKeyConstraint("account", "symbol", "trade_id", name=op.f("pk_fills")),
    )
    op.create_index("ix_fills_account_filled_at", "fills", ["account", "filled_at"])
    op.create_index("ix_fills_account_client_id", "fills", ["account", "client_id"])


def _position_tables() -> None:
    op.create_table(
        "positions",
        sa.Column("position_id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("account", sa.Text(), nullable=False),
        sa.Column("event_id", sa.Text(), nullable=True),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("side", sa.Text(), nullable=False),
        sa.Column("qty", sa.Numeric(), nullable=False),
        sa.Column("max_qty", sa.Numeric(), nullable=False),
        sa.Column("entry_price", sa.Numeric(), nullable=False),
        sa.Column("mark_price", sa.Numeric(), nullable=True),
        sa.Column("unrealized_pnl", sa.Numeric(), nullable=False),
        sa.Column("leverage", sa.Integer(), nullable=True),
        sa.Column("margin_type", sa.Text(), nullable=True),
        sa.Column("liquidation_price", sa.Numeric(), nullable=True),
        sa.Column("stop_price", sa.Numeric(), nullable=True),
        sa.Column("initial_stop_price", sa.Numeric(), nullable=True),
        sa.Column("tp1_price", sa.Numeric(), nullable=True),
        sa.Column("trail_distance", sa.Numeric(), nullable=True),
        sa.Column("best_price", sa.Numeric(), nullable=True),
        sa.Column("is_hedge_book", sa.Boolean(), nullable=False),
        sa.Column("opened_at", TS, nullable=False),
        sa.Column("time_stop_at", TS, nullable=True),
        sa.Column("closed_at", TS, nullable=True),
        sa.Column("exit_price", sa.Numeric(), nullable=True),
        sa.Column("exit_reason", sa.Text(), nullable=True),
        sa.Column("exit_notional", sa.Numeric(), nullable=False),
        sa.Column("exit_qty", sa.Numeric(), nullable=False),
        sa.Column("realized_pnl", sa.Numeric(), nullable=False),
        sa.Column("fees_funding_usd", sa.Numeric(), nullable=False),
        sa.Column("r_multiple", sa.Float(), nullable=True),
        sa.Column("exit_in_progress", sa.Boolean(), nullable=False),
        sa.Column("tp1_done", sa.Boolean(), nullable=False),
        sa.Column("updated_at", TS, nullable=False),
        _account("positions"),
        sa.CheckConstraint("side IN ('LONG', 'SHORT')", name=op.f("ck_positions_side")),
        sa.PrimaryKeyConstraint("position_id", name=op.f("pk_positions")),
    )
    op.create_index(
        "uq_positions_account_symbol_open",
        "positions",
        ["account", "symbol"],
        unique=True,
        postgresql_where=sa.text("closed_at IS NULL"),
    )
    op.create_index("ix_positions_account_closed_at", "positions", ["account", "closed_at"])
    op.create_table(
        "cash_flows",
        sa.Column("account", sa.Text(), nullable=False),
        sa.Column("flow_id", sa.Text(), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=True),
        sa.Column("amount_usd", sa.Numeric(), nullable=False),
        sa.Column("asset", sa.Text(), nullable=False),
        sa.Column("rate", sa.Numeric(), nullable=True),
        sa.Column("occurred_at", TS, nullable=False),
        sa.Column("position_id", sa.BigInteger(), nullable=True),
        _account("cash_flows"),
        sa.PrimaryKeyConstraint("account", "flow_id", name=op.f("pk_cash_flows")),
    )
    op.create_index("ix_cash_flows_account_occurred_at", "cash_flows", ["account", "occurred_at"])
    op.create_table(
        "equity_snapshots",
        sa.Column("account", sa.Text(), nullable=False),
        sa.Column("ts", TS, nullable=False),
        sa.Column("equity", sa.Numeric(), nullable=False),
        sa.Column("available", sa.Numeric(), nullable=False),
        sa.Column("day_start_equity", sa.Numeric(), nullable=False),
        sa.Column("wallet_balance", sa.Numeric(), nullable=False),
        sa.Column("unrealized_pnl", sa.Numeric(), nullable=False),
        sa.Column("open_positions", sa.Integer(), nullable=False),
        _account("equity_snapshots"),
        sa.PrimaryKeyConstraint("account", "ts", name=op.f("pk_equity_snapshots")),
    )


def _state_tables() -> None:
    op.create_table(
        "kill_state",
        sa.Column("account", sa.Text(), nullable=False),
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column("cause", sa.Text(), nullable=True),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("since", TS, nullable=True),
        sa.Column("updated_at", TS, nullable=False),
        sa.Column("command_id", sa.Text(), nullable=True),
        _account("kill_state"),
        sa.CheckConstraint("state IN ('running', 'paused', 'killed')", name=op.f("ck_kill_state_state")),
        sa.CheckConstraint(
            "cause IS NULL OR cause IN ('manual', 'daily_loss', 'reconcile', 'rate_limit')",
            name=op.f("ck_kill_state_cause"),
        ),
        sa.PrimaryKeyConstraint("account", name=op.f("pk_kill_state")),
    )
    op.create_table(
        "reconcile_runs",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("account", sa.Text(), nullable=False),
        sa.Column("ran_at", TS, nullable=False),
        sa.Column("clean", sa.Boolean(), nullable=False),
        sa.Column("stop_invariant_ok", sa.Boolean(), nullable=False),
        sa.Column("detail", sa.Text(), nullable=True),
        sa.Column("mismatches", JSONB, nullable=False),
        _account("reconcile_runs"),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_reconcile_runs")),
    )
    op.create_index("ix_reconcile_runs_account_ran_at", "reconcile_runs", ["account", "ran_at"])
    op.create_table(
        "exec_accounts",
        sa.Column("account", sa.Text(), nullable=False),
        sa.Column("sync_state", sa.Text(), nullable=False),
        sa.Column("day", sa.Date(), nullable=True),
        sa.Column("day_start_equity", sa.Numeric(), nullable=True),
        sa.Column("wallet_anchor", sa.Numeric(), nullable=True),
        sa.Column("anchor_at", TS, nullable=True),
        sa.Column("daily_notional_day", sa.Date(), nullable=True),
        sa.Column("daily_notional_usd", sa.Numeric(), nullable=False),
        sa.Column("banned_until", TS, nullable=True),
        sa.Column("updated_at", TS, nullable=False),
        _account("exec_accounts"),
        sa.CheckConstraint("sync_state IN ('synced', 'resyncing')", name=op.f("ck_exec_accounts_sync_state")),
        sa.PrimaryKeyConstraint("account", name=op.f("pk_exec_accounts")),
    )
    op.create_table(
        "paper_state",
        sa.Column("account", sa.Text(), nullable=False),
        sa.Column("state", JSONB, nullable=False),
        sa.Column("market_cursor", TS, nullable=True),
        sa.Column("updated_at", TS, nullable=False),
        sa.CheckConstraint("account = 'paper'", name=op.f("ck_paper_state_account")),
        sa.PrimaryKeyConstraint("account", name=op.f("pk_paper_state")),
    )


def _grants() -> None:
    risk = ", ".join(RISK_TABLES)
    ledger = ", ".join(LEDGER_TABLES)
    # Risk owns its tables; the console reads everything.
    op.execute(f"GRANT SELECT, INSERT ON {risk} TO hdt_risk")
    op.execute("GRANT UPDATE (published_at, stream_id) ON risk_intents TO hdt_risk")
    op.execute("GRANT USAGE ON SEQUENCE hedge_book_id_seq TO hdt_risk")
    op.execute(f"GRANT SELECT ON {risk}, {ledger} TO hdt_console_ro")
    # Only execution writes the ledger (no DELETE: rows move through statuses).
    op.execute(f"GRANT SELECT, INSERT, UPDATE ON {ledger} TO hdt_execution")
    # `write_audit` inserts with RETURNING id (hdt-kill CLI runs as hdt_execution).
    op.execute("GRANT SELECT (id) ON audit_log TO hdt_execution")
    op.execute("GRANT USAGE ON SEQUENCE positions_position_id_seq, reconcile_runs_id_seq TO hdt_execution")
    # Risk reads the namespace ledger (intent by position, kill state veto, hedge betas, and the exposure
    # that keeps a namespace attached after a mode switch); config-api reads that exposure for the mode
    # policy.
    op.execute(
        "GRANT SELECT ON kill_state, positions, processed_intents, equity_snapshots, reconcile_runs, "
        "orders, algo_orders TO hdt_risk"
    )
    op.execute("GRANT SELECT ON positions, orders, algo_orders TO hdt_configapi")
    # Scoring reads fills from Postgres; the public publisher and the Telegram bot read a small set.
    op.execute("GRANT SELECT ON fills, positions, cash_flows TO hdt_scorer")
    op.execute("GRANT SELECT ON equity_snapshots, positions TO hdt_publisher_ro")
    op.execute(
        "GRANT SELECT ON kill_state, equity_snapshots, positions, fills, reconcile_runs TO hdt_telegram"
    )


def upgrade() -> None:
    _risk_tables()
    _order_tables()
    _position_tables()
    _state_tables()
    _grants()


def downgrade() -> None:
    for table in reversed((*RISK_TABLES, *LEDGER_TABLES)):
        op.drop_table(table)
