"""Recorder tables: CMC credit metering, governor state, route/WS health, lake statistics.

Revision ID: 0002_ingest
Revises: 0001_settings_vault
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "0002_ingest"
down_revision: str | None = "0001_settings_vault"
branch_labels = None
depends_on = None

TS = sa.DateTime(timezone=True)
TABLES = ("cmc_credit_usage", "cmc_key_info", "cmc_governor_days", "route_health", "ws_health", "lake_stats")


def upgrade() -> None:
    op.create_table(
        "cmc_credit_usage",
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("route", sa.Text(), nullable=False),
        sa.Column("credits", sa.BigInteger(), nullable=False),
        sa.Column("calls", sa.BigInteger(), nullable=False),
        sa.PrimaryKeyConstraint("date", "route", name=op.f("pk_cmc_credit_usage")),
    )
    op.create_table(
        "cmc_key_info",
        sa.Column("checked_at", TS, nullable=False),
        sa.Column("credits_used_cycle", sa.BigInteger(), nullable=False),
        sa.Column("credit_limit_cycle", sa.BigInteger(), nullable=False),
        sa.Column("cycle_start", TS, nullable=False),
        sa.Column("cycle_end", TS, nullable=False),
        sa.Column("credits_used_today", sa.BigInteger(), nullable=False),
        sa.Column("governor_daily_budget", sa.BigInteger(), nullable=False),
        sa.Column("rate_limit_minute", sa.Integer(), nullable=True),
        sa.Column("halt_state", sa.Text(), nullable=True),
        sa.Column("degraded", sa.Boolean(), nullable=False),
        sa.CheckConstraint(
            "halt_state IS NULL OR halt_state IN ('daily_cap', 'monthly_cap', 'ip_limit')",
            name=op.f("ck_cmc_key_info_halt"),
        ),
        sa.PrimaryKeyConstraint("checked_at", name=op.f("pk_cmc_key_info")),
    )
    op.create_table(
        "cmc_governor_days",
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("budget", sa.BigInteger(), nullable=False),
        sa.PrimaryKeyConstraint("date", name=op.f("pk_cmc_governor_days")),
    )
    op.create_table(
        "route_health",
        sa.Column("route_key", sa.Text(), nullable=False),
        sa.Column("sort_order", sa.Integer(), nullable=False),
        sa.Column("route", sa.Text(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("cadence_label", sa.Text(), nullable=False),
        sa.Column("last_success_at", TS, nullable=True),
        sa.Column("last_attempt_at", TS, nullable=True),
        sa.Column("cycles_late", sa.Integer(), nullable=False),
        sa.Column("consecutive_failures", sa.Integer(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("credits_per_day", sa.BigInteger(), nullable=True),
        sa.Column("consumers", sa.Text(), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.CheckConstraint(
            "status IN ('ok', 'late', 'failing', 'halted', 'shed', 'disabled', 'pending')",
            name=op.f("ck_route_health_status"),
        ),
        sa.PrimaryKeyConstraint("route_key", name=op.f("pk_route_health")),
    )
    op.create_table(
        "ws_health",
        sa.Column("connection", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("gaps_24h", sa.Integer(), nullable=False),
        sa.Column("connected_since", TS, nullable=True),
        sa.Column("last_message_at", TS, nullable=True),
        sa.CheckConstraint(
            "status IN ('connected', 'reconnecting', 'down')", name=op.f("ck_ws_health_status")
        ),
        sa.PrimaryKeyConstraint("connection", name=op.f("pk_ws_health")),
    )
    op.create_table(
        "lake_stats",
        sa.Column("as_of", TS, nullable=False),
        sa.Column("lake_bytes", sa.BigInteger(), nullable=False),
        sa.Column("disk_used_fraction", sa.Float(), nullable=True),
        sa.Column("retention_days", sa.Integer(), nullable=True),
        sa.Column("merkle_date", sa.Date(), nullable=True),
        sa.Column("merkle_root", sa.Text(), nullable=True),
        sa.Column("object_locked", sa.Boolean(), nullable=False),
        sa.PrimaryKeyConstraint("as_of", name=op.f("pk_lake_stats")),
    )
    tables = ", ".join(TABLES)
    op.execute(f"GRANT SELECT, INSERT, UPDATE ON {tables} TO hdt_recorder")
    op.execute(f"GRANT SELECT ON {tables} TO hdt_console_ro")
    # Risk and the council read the CMC-degraded / halt state from the latest key info snapshot.
    op.execute("GRANT SELECT ON cmc_key_info, route_health TO hdt_risk, hdt_council")


def downgrade() -> None:
    for table in reversed(TABLES):
        op.drop_table(table)
