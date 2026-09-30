"""Phase 05 LLM tables (llm_calls, llm_cache).

Grants (least privilege, both tables insert-only):
- every service holding the `llm` vault scope calls OpenRouter through `hdt.agents.llm_router.LlmRouter`
  (council agents, news extractor and judges, veto scan, scorer reflection): SELECT and INSERT on
  `llm_calls` (one row per billed generation) and `llm_cache` (the reply cache replay reads). No UPDATE or
  DELETE: a recorded generation or cached reply never changes;
- the console reads `llm_calls` for the cost pages, the public publisher only its cost columns
  (`PUBLISHER_COLUMNS`); nobody outside the LLM callers reads `llm_cache` (it holds full model replies).

Revision ID: 0007_llm
Revises: 0006_ops
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0007_llm"
down_revision: str | None = "0006_ops"
branch_labels = None
depends_on = None

TS = sa.DateTime(timezone=True)
JSONB = postgresql.JSONB(astext_type=sa.Text())
TABLES = ("llm_calls", "llm_cache")
LLM_CALLERS = ("hdt_council", "hdt_news", "hdt_veto_scan", "hdt_scorer")
CONSOLE_READERS = ("hdt_console_ro",)
PUBLISHER_COLUMNS = (
    "called_at",
    "pipeline",
    "role",
    "event_id",
    "model_slug",
    "prompt_tokens",
    "completion_tokens",
    "cost_usd",
)
"""What the public cost pages read (`hdt.public.publisher` sources): column grants, as 0006 does for every
other publisher source; generation ids, provider, hashes and returned models stay private."""


def upgrade() -> None:
    op.create_table(
        "llm_calls",
        sa.Column("generation_id", sa.Text(), nullable=False),
        sa.Column("called_at", TS, nullable=False),
        sa.Column("pipeline", sa.Text(), nullable=False),
        sa.Column("role", sa.Text(), nullable=False),
        sa.Column("event_id", sa.Text(), nullable=True),
        sa.Column("model_slug", sa.Text(), nullable=False),
        sa.Column("model_returned", sa.Text(), nullable=True),
        sa.Column("provider", sa.Text(), nullable=True),
        sa.Column("prompt_tokens", sa.Integer(), nullable=False),
        sa.Column("completion_tokens", sa.Integer(), nullable=False),
        sa.Column("cost_usd", sa.Float(), nullable=True),
        sa.Column("prompt_hash", sa.Text(), nullable=False),
        sa.Column("params_hash", sa.Text(), nullable=False),
        sa.Column("latency_ms", sa.Integer(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.CheckConstraint(
            "pipeline IN ('council', 'news', 'reflection')", name=op.f("ck_llm_calls_pipeline")
        ),
        sa.CheckConstraint("status IN ('ok', 'invalid_output')", name=op.f("ck_llm_calls_status")),
        sa.CheckConstraint("prompt_tokens >= 0 AND completion_tokens >= 0", name=op.f("ck_llm_calls_tokens")),
        sa.CheckConstraint("cost_usd IS NULL OR cost_usd >= 0", name=op.f("ck_llm_calls_cost")),
        sa.CheckConstraint("latency_ms >= 0", name=op.f("ck_llm_calls_latency")),
        sa.PrimaryKeyConstraint("generation_id", name=op.f("pk_llm_calls")),
    )
    op.create_index("ix_llm_calls_called_at", "llm_calls", ["called_at"])
    op.create_index("ix_llm_calls_event_id", "llm_calls", ["event_id"])
    op.create_table(
        "llm_cache",
        sa.Column("prompt_hash", sa.Text(), nullable=False),
        sa.Column("model_slug", sa.Text(), nullable=False),
        sa.Column("provider", sa.Text(), nullable=False),
        sa.Column("params_hash", sa.Text(), nullable=False),
        sa.Column("role", sa.Text(), nullable=False),
        sa.Column("generation_id", sa.Text(), nullable=False),
        sa.Column("response", JSONB, nullable=False),
        sa.Column("created_at", TS, nullable=False),
        sa.PrimaryKeyConstraint(
            "prompt_hash", "model_slug", "provider", "params_hash", name=op.f("pk_llm_cache")
        ),
    )
    tables = ", ".join(TABLES)
    op.execute(f"GRANT SELECT, INSERT ON {tables} TO {', '.join(LLM_CALLERS)}")
    op.execute(f"GRANT SELECT ON llm_calls TO {', '.join(CONSOLE_READERS)}")
    op.execute(f"GRANT SELECT ({', '.join(PUBLISHER_COLUMNS)}) ON llm_calls TO hdt_publisher_ro")


def downgrade() -> None:
    for table in reversed(TABLES):
        op.drop_table(table)
