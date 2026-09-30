"""Phase 06 council tables: events, round commits, decision cards, forecasts, claims, outbox, checkpoints.

Revision ID: 0009_council
Revises: 0008_news

The `checkpoint_*` tables are the LangGraph PostgresSaver schema of langgraph-checkpoint-postgres 3.1.x
(its migrations 0-9, recorded in `checkpoint_migrations` so `setup()` has nothing left to do): the council
role holds no DDL rights. `decision_commits` is insert-only; the blind round-1 hash is also written to the
append-only `audit_log`.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0009_council"
down_revision: str | None = "0008_news"
branch_labels = None
depends_on = None

TS = sa.DateTime(timezone=True)
JSONB = postgresql.JSONB(astext_type=sa.Text())
SOURCE_CHECK = "source IN ('LTX', 'MIGRATION', 'HOLLOW_HYPE', 'HELD')"
CHECKPOINT_MIGRATIONS = 10
"""len(BasePostgresSaver.MIGRATIONS) in langgraph-checkpoint-postgres 3.1.x."""

COUNCIL_TABLES = (
    "council_events",
    "decision_commits",
    "decision_cards",
    "decision_forecasts",
    "decision_claims",
    "decision_outbox",
)
CHECKPOINT_TABLES = ("checkpoint_migrations", "checkpoints", "checkpoint_blobs", "checkpoint_writes")
READER_TABLES = ("decision_cards", "decision_forecasts", "decision_claims")


def _council_tables() -> None:
    op.create_table(
        "council_events",
        sa.Column("event_id", sa.Text(), nullable=False),
        sa.Column("coin_id", sa.Integer(), nullable=False),
        sa.Column("as_of", TS, nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("candidate", JSONB, nullable=False),
        sa.Column("config_version_ids", JSONB, nullable=False),
        sa.Column("unscored", sa.Boolean(), nullable=False),
        sa.Column("shadow_only", sa.Boolean(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("skip_reason", sa.Text(), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("msg_id", sa.Text(), nullable=True),
        sa.Column("created_at", TS, nullable=False),
        sa.Column("updated_at", TS, nullable=False),
        sa.Column("checkpoint_pruned_at", TS, nullable=True),
        sa.CheckConstraint(
            "status IN ('pending', 'running', 'done', 'failed', 'skipped')",
            name=op.f("ck_council_events_status"),
        ),
        sa.CheckConstraint(SOURCE_CHECK, name=op.f("ck_council_events_source")),
        sa.PrimaryKeyConstraint("event_id", name=op.f("pk_council_events")),
        sa.UniqueConstraint("coin_id", "as_of", "source", name="uq_council_events_coin_id_as_of_source"),
    )
    op.create_index("ix_council_events_coin_id_as_of", "council_events", ["coin_id", "as_of"])
    op.create_index(
        "ix_council_events_open",
        "council_events",
        ["created_at"],
        postgresql_where=sa.text("status IN ('pending', 'running')"),
    )
    op.create_index(
        "ix_council_events_unpruned",
        "council_events",
        ["updated_at"],
        postgresql_where=sa.text("status IN ('done', 'failed') AND checkpoint_pruned_at IS NULL"),
    )
    op.create_table(
        "decision_commits",
        sa.Column("event_id", sa.Text(), nullable=False),
        sa.Column("round", sa.Integer(), nullable=False),
        sa.Column("agent", sa.Text(), nullable=False),
        sa.Column("forecast_sha256", sa.Text(), nullable=False),
        sa.Column("round_sha256", sa.Text(), nullable=False),
        sa.Column("committed_at", TS, nullable=False),
        sa.PrimaryKeyConstraint("event_id", "round", "agent", name=op.f("pk_decision_commits")),
    )
    op.create_table(
        "decision_cards",
        sa.Column("event_id", sa.Text(), nullable=False),
        sa.Column("coin_id", sa.Integer(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("as_of", TS, nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("outcome", sa.Text(), nullable=False),
        sa.Column("side", sa.Text(), nullable=True),
        sa.Column("manager_size", sa.Float(), nullable=False),
        sa.Column("p_pooled", sa.Float(), nullable=True),
        sa.Column("disagreement", sa.Float(), nullable=True),
        sa.Column("rounds", sa.Integer(), nullable=False),
        sa.Column("stop_reason", sa.Text(), nullable=False),
        sa.Column("candidate_id", sa.Text(), nullable=True),
        sa.Column("candidate_set_sha256", sa.Text(), nullable=True),
        sa.Column("packet_sha256", sa.Text(), nullable=True),
        sa.Column("target_type", sa.Text(), nullable=False),
        sa.Column("label_spec_version", sa.Text(), nullable=False),
        sa.Column("config_version_ids", JSONB, nullable=False),
        sa.Column("universe_date", sa.Date(), nullable=False),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("consensus", JSONB, nullable=False),
        sa.Column("manager_rule", JSONB, nullable=False),
        sa.Column("timeline", JSONB, nullable=False),
        sa.Column("unscored", sa.Boolean(), nullable=False),
        sa.Column("shadow_only", sa.Boolean(), nullable=False),
        sa.Column("held_side", sa.Text(), nullable=True),
        sa.Column("intent", sa.Text(), nullable=True),
        sa.Column("round1_sha256", sa.Text(), nullable=False),
        sa.Column("params", JSONB, nullable=False),
        sa.Column("agents", JSONB, nullable=False),
        sa.Column("hard_evidence_version", sa.Text(), nullable=False),
        sa.Column("card_sha256", sa.Text(), nullable=False),
        sa.Column("created_at", TS, nullable=False),
        sa.CheckConstraint(
            "outcome IN ('LONG', 'SHORT', 'NO_TRADE', 'HOLD', 'EXIT')", name=op.f("ck_decision_cards_outcome")
        ),
        sa.CheckConstraint(SOURCE_CHECK, name=op.f("ck_decision_cards_source")),
        sa.CheckConstraint("side IS NULL OR side IN ('LONG', 'SHORT')", name=op.f("ck_decision_cards_side")),
        sa.CheckConstraint("rounds BETWEEN 0 AND 3", name=op.f("ck_decision_cards_rounds")),
        sa.PrimaryKeyConstraint("event_id", name=op.f("pk_decision_cards")),
    )
    op.create_index("ix_decision_cards_as_of", "decision_cards", ["as_of"])
    op.create_index("ix_decision_cards_coin_id_as_of", "decision_cards", ["coin_id", "as_of"])
    op.create_table(
        "decision_forecasts",
        sa.Column("event_id", sa.Text(), nullable=False),
        sa.Column("agent", sa.Text(), nullable=False),
        sa.Column("round", sa.Integer(), nullable=False),
        sa.Column("forecast", JSONB, nullable=False),
        sa.Column("submitted", JSONB, nullable=False),
        sa.Column("revision", JSONB, nullable=True),
        sa.Column("stance", sa.Text(), nullable=False),
        sa.Column("weight_norm", sa.Float(), nullable=True),
        sa.Column("commit_sha256", sa.Text(), nullable=False),
        sa.CheckConstraint(
            "stance IN ('LONG', 'SHORT', 'NEUTRAL', 'ABSTAIN')", name=op.f("ck_decision_forecasts_stance")
        ),
        sa.PrimaryKeyConstraint("event_id", "agent", "round", name=op.f("pk_decision_forecasts")),
    )
    op.create_table(
        "decision_claims",
        sa.Column("event_id", sa.Text(), nullable=False),
        sa.Column("round", sa.Integer(), nullable=False),
        sa.Column("shared_id", sa.Text(), nullable=False),
        sa.Column("source_agent", sa.Text(), nullable=False),
        sa.Column("original_claim_id", sa.Text(), nullable=False),
        sa.Column("claim_sha256", sa.Text(), nullable=False),
        sa.Column("claim", JSONB, nullable=False),
        sa.Column("reject_reason", sa.Text(), nullable=True),
        sa.Column("penalty", sa.Boolean(), nullable=False),
        sa.Column("shared", sa.Boolean(), nullable=False),
        sa.PrimaryKeyConstraint("event_id", "round", "shared_id", name=op.f("pk_decision_claims")),
    )
    op.create_index("ix_decision_claims_source_agent", "decision_claims", ["source_agent"])
    op.create_table(
        "decision_outbox",
        sa.Column("event_id", sa.Text(), nullable=False),
        sa.Column("payload", JSONB, nullable=False),
        sa.Column("as_of", TS, nullable=False),
        sa.Column("expires_at", TS, nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("created_at", TS, nullable=False),
        sa.Column("published_at", TS, nullable=True),
        sa.Column("stream_id", sa.Text(), nullable=True),
        sa.CheckConstraint(
            "status IN ('pending', 'published', 'expired')", name=op.f("ck_decision_outbox_status")
        ),
        sa.PrimaryKeyConstraint("event_id", name=op.f("pk_decision_outbox")),
    )
    op.create_index(
        "ix_decision_outbox_pending",
        "decision_outbox",
        ["created_at"],
        postgresql_where=sa.text("status = 'pending'"),
    )


def _checkpoint_tables() -> None:
    op.create_table(
        "checkpoint_migrations",
        sa.Column("v", sa.Integer(), autoincrement=False, nullable=False),
        sa.PrimaryKeyConstraint("v", name=op.f("pk_checkpoint_migrations")),
    )
    op.create_table(
        "checkpoints",
        sa.Column("thread_id", sa.Text(), nullable=False),
        sa.Column("checkpoint_ns", sa.Text(), server_default=sa.text("''"), nullable=False),
        sa.Column("checkpoint_id", sa.Text(), nullable=False),
        sa.Column("parent_checkpoint_id", sa.Text(), nullable=True),
        sa.Column("type", sa.Text(), nullable=True),
        sa.Column("checkpoint", JSONB, nullable=False),
        sa.Column("metadata", JSONB, server_default=sa.text("'{}'::jsonb"), nullable=False),
        sa.PrimaryKeyConstraint("thread_id", "checkpoint_ns", "checkpoint_id", name=op.f("pk_checkpoints")),
    )
    op.create_index("checkpoints_thread_id_idx", "checkpoints", ["thread_id"])
    op.create_table(
        "checkpoint_blobs",
        sa.Column("thread_id", sa.Text(), nullable=False),
        sa.Column("checkpoint_ns", sa.Text(), server_default=sa.text("''"), nullable=False),
        sa.Column("channel", sa.Text(), nullable=False),
        sa.Column("version", sa.Text(), nullable=False),
        sa.Column("type", sa.Text(), nullable=False),
        sa.Column("blob", sa.LargeBinary(), nullable=True),
        sa.PrimaryKeyConstraint(
            "thread_id", "checkpoint_ns", "channel", "version", name=op.f("pk_checkpoint_blobs")
        ),
    )
    op.create_index("checkpoint_blobs_thread_id_idx", "checkpoint_blobs", ["thread_id"])
    op.create_table(
        "checkpoint_writes",
        sa.Column("thread_id", sa.Text(), nullable=False),
        sa.Column("checkpoint_ns", sa.Text(), server_default=sa.text("''"), nullable=False),
        sa.Column("checkpoint_id", sa.Text(), nullable=False),
        sa.Column("task_id", sa.Text(), nullable=False),
        sa.Column("idx", sa.Integer(), autoincrement=False, nullable=False),
        sa.Column("channel", sa.Text(), nullable=False),
        sa.Column("type", sa.Text(), nullable=True),
        sa.Column("blob", sa.LargeBinary(), nullable=False),
        sa.Column("task_path", sa.Text(), server_default=sa.text("''"), nullable=False),
        sa.PrimaryKeyConstraint(
            "thread_id", "checkpoint_ns", "checkpoint_id", "task_id", "idx", name=op.f("pk_checkpoint_writes")
        ),
    )
    op.create_index("checkpoint_writes_thread_id_idx", "checkpoint_writes", ["thread_id"])
    op.execute(
        f"INSERT INTO checkpoint_migrations (v) SELECT generate_series(0, {CHECKPOINT_MIGRATIONS - 1})"
    )


def _grants() -> None:
    readers = ", ".join(READER_TABLES)
    # The council writes its own tables; commits are insert-only, events and the outbox move by status.
    op.execute(
        "GRANT SELECT, INSERT ON council_events, decision_commits, decision_cards, decision_forecasts, "
        "decision_claims, decision_outbox TO hdt_council"
    )
    op.execute(
        "GRANT UPDATE (status, skip_reason, attempts, error, updated_at, checkpoint_pruned_at) "
        "ON council_events TO hdt_council"
    )
    op.execute("GRANT UPDATE (status, published_at, stream_id) ON decision_outbox TO hdt_council")
    # The round-1 hash goes to the append-only audit log too (INSERT only, never UPDATE or DELETE).
    op.execute("GRANT INSERT ON audit_log TO hdt_council")
    # LangGraph checkpoints of in-flight meetings (resume by thread_id = event_id).
    op.execute("GRANT SELECT ON checkpoint_migrations TO hdt_council")
    op.execute(
        "GRANT SELECT, INSERT, UPDATE, DELETE ON checkpoints, checkpoint_blobs, checkpoint_writes "
        "TO hdt_council"
    )
    # Readers: scoring, the admin console, the public publisher (table level: it selects d.*), Telegram.
    op.execute(f"GRANT SELECT ON {readers} TO hdt_scorer, hdt_console_ro, hdt_publisher_ro")
    op.execute("GRANT SELECT ON council_events, decision_outbox TO hdt_console_ro, hdt_scorer")
    op.execute("GRANT SELECT ON decision_cards TO hdt_telegram")


def upgrade() -> None:
    _council_tables()
    _checkpoint_tables()
    _grants()


def downgrade() -> None:
    readers = ", ".join(READER_TABLES)
    op.execute("REVOKE SELECT ON decision_cards FROM hdt_telegram")
    op.execute(f"REVOKE SELECT ON {readers} FROM hdt_scorer, hdt_console_ro, hdt_publisher_ro")  # noqa: S608
    op.execute("REVOKE INSERT ON audit_log FROM hdt_council")
    for table in reversed((*COUNCIL_TABLES, *CHECKPOINT_TABLES)):
        op.drop_table(table)
