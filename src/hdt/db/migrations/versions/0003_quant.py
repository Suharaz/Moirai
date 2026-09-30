"""Quant tables: stored packets, shared candidate sets, scanner superset log (insert-only).

Revision ID: 0003_quant
Revises: 0002_ingest
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0003_quant"
down_revision: str | None = "0002_ingest"
branch_labels = None
depends_on = None

TS = sa.DateTime(timezone=True)
TABLES = ("candidate_sets", "quant_packets", "scanner_log", "scanner_published")


def upgrade() -> None:
    op.create_table(
        "candidate_sets",
        sa.Column("candidate_set_sha256", sa.Text(), nullable=False),
        sa.Column("coin_id", sa.Integer(), nullable=False),
        sa.Column("as_of", TS, nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", TS, nullable=False),
        sa.PrimaryKeyConstraint("candidate_set_sha256", name=op.f("pk_candidate_sets")),
    )
    op.create_index("ix_candidate_sets_coin_id_as_of", "candidate_sets", ["coin_id", "as_of"])
    op.create_table(
        "quant_packets",
        sa.Column("packet_sha256", sa.Text(), nullable=False),
        sa.Column("agent", sa.Text(), nullable=False),
        sa.Column("coin_id", sa.Integer(), nullable=False),
        sa.Column("as_of", TS, nullable=False),
        sa.Column("candidate_set_sha256", sa.Text(), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", TS, nullable=False),
        sa.PrimaryKeyConstraint("packet_sha256", name=op.f("pk_quant_packets")),
    )
    op.create_index("ix_quant_packets_coin_id_as_of", "quant_packets", ["coin_id", "as_of"])
    op.create_index("ix_quant_packets_candidate_set_sha256", "quant_packets", ["candidate_set_sha256"])
    op.create_table(
        "scanner_log",
        sa.Column("coin_id", sa.Integer(), nullable=False),
        sa.Column("as_of", TS, nullable=False),
        sa.Column("rule", sa.Text(), nullable=False),
        sa.Column("rule_version", sa.Text(), nullable=False),
        sa.Column("side", sa.Text(), nullable=True),
        sa.Column("conditions", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("strict_pass", sa.Boolean(), nullable=False),
        sa.Column("loose_pass", sa.Boolean(), nullable=False),
        sa.Column("contagion_blocked", sa.Boolean(), nullable=True),
        sa.Column("emitted", sa.Boolean(), nullable=False),
        sa.Column("dropped_budget", sa.Boolean(), nullable=False),
        sa.Column("score", sa.Float(), nullable=True),
        sa.Column("target_type", sa.Text(), nullable=False),
        sa.Column("label_spec_version", sa.Text(), nullable=False),
        sa.Column("universe_date", sa.Date(), nullable=False),
        sa.Column("feature_ver", sa.Text(), nullable=False),
        sa.Column("created_at", TS, nullable=False),
        sa.CheckConstraint("rule IN ('LTX', 'MIGRATION', 'HELD')", name=op.f("ck_scanner_log_rule")),
        sa.CheckConstraint("side IS NULL OR side IN ('LONG', 'SHORT')", name=op.f("ck_scanner_log_side")),
        sa.CheckConstraint(
            "NOT (emitted AND dropped_budget)", name=op.f("ck_scanner_log_emitted_or_dropped")
        ),
        sa.PrimaryKeyConstraint("coin_id", "as_of", "rule", "rule_version", name=op.f("pk_scanner_log")),
    )
    op.create_index("ix_scanner_log_as_of", "scanner_log", ["as_of"])
    # Outbox mark of scanner_log: a row per emitted candidate whose XADD succeeded.
    op.create_table(
        "scanner_published",
        sa.Column("coin_id", sa.Integer(), nullable=False),
        sa.Column("as_of", TS, nullable=False),
        sa.Column("rule", sa.Text(), nullable=False),
        sa.Column("rule_version", sa.Text(), nullable=False),
        sa.Column("published_at", TS, nullable=False),
        sa.ForeignKeyConstraint(
            ["coin_id", "as_of", "rule", "rule_version"],
            ["scanner_log.coin_id", "scanner_log.as_of", "scanner_log.rule", "scanner_log.rule_version"],
            name=op.f("fk_scanner_published_coin_id_scanner_log"),
        ),
        sa.PrimaryKeyConstraint(
            "coin_id", "as_of", "rule", "rule_version", name=op.f("pk_scanner_published")
        ),
    )
    tables = ", ".join(TABLES)
    # Insert-only: the council (quant_core + scanner) is the only writer and gets no UPDATE / DELETE.
    op.execute(f"GRANT SELECT, INSERT ON {tables} TO hdt_council")
    # Risk re-verifies packet_sha256 and reads the candidate set; the scorer and the console read all three.
    op.execute(f"GRANT SELECT ON {tables} TO hdt_risk, hdt_scorer, hdt_console_ro")


def downgrade() -> None:
    for table in reversed(TABLES):
        op.drop_table(table)
