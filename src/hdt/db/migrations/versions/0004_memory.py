"""Memory store: the LangGraph `store` table, append-only, with row-level security per writer.

The table and its indexes match `langgraph.store.postgres.PostgresStore` migrations 0-3, so services use
PostgresStore without ever calling `setup()`. On top of that:
- memory never expires (`ck_store_no_ttl`);
- a trigger refuses DELETE, TRUNCATE and any UPDATE that changes prefix, key, value or created_at
  (PostgresStore writes with INSERT ... ON CONFLICT DO UPDATE, so writers hold UPDATE on the columns it
  sets, and a conflicting write with a different value fails);
- row-level security limits each writer role to what it owns: `hdt_council` decision episodes (phase 06
  `emit_decision`), `hdt_scorer` outcomes and Reflection lesson events (phase 08), `hdt_configapi` human
  lesson reviews, `hdt_news` known news events of the News namespace (phase 07).

Revision ID: 0004_memory
Revises: 0002_ingest
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0004_memory"
down_revision: str | None = "0003_quant"
branch_labels = None
depends_on = None

TS = sa.DateTime(timezone=True)
AGENTS = "(crowding|technical|micro|fundamental|news|macro)"
EPISODIC = f"prefix ~ '^{AGENTS}\\.episodic$'"
LESSONS = f"prefix ~ '^{AGENTS}\\.lessons$'"
RECORD = "value->>'record_type'"
LESSON_EVENT = f"{LESSONS} AND {RECORD} = 'lesson_event'"
WRITERS: dict[str, str] = {
    "hdt_council": f"{EPISODIC} AND {RECORD} = 'decision'",
    "hdt_scorer": (
        f"({EPISODIC} AND {RECORD} = 'outcome') OR "
        f"({LESSON_EVENT} AND value->>'action' IN ('proposed', 'ab_completed', 'retired'))"
    ),
    "hdt_configapi": f"{LESSON_EVENT} AND value->>'action' IN ('approved', 'rejected', 'retired')",
    "hdt_news": f"prefix = 'news.episodic' AND {RECORD} = 'known_event'",
}
READERS = (*WRITERS, "hdt_console_ro")


def upgrade() -> None:
    op.create_table(
        "store",
        sa.Column("prefix", sa.Text(), nullable=False),
        sa.Column("key", sa.Text(), nullable=False),
        sa.Column("value", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", TS, server_default=sa.text("CURRENT_TIMESTAMP"), nullable=True),
        sa.Column("updated_at", TS, server_default=sa.text("CURRENT_TIMESTAMP"), nullable=True),
        sa.Column("expires_at", TS, nullable=True),
        sa.Column("ttl_minutes", sa.Integer(), nullable=True),
        sa.CheckConstraint("expires_at IS NULL AND ttl_minutes IS NULL", name=op.f("ck_store_no_ttl")),
        sa.PrimaryKeyConstraint("prefix", "key", name=op.f("pk_store")),
    )
    op.create_index(
        "store_prefix_idx", "store", ["prefix"], unique=False, postgresql_ops={"prefix": "text_pattern_ops"}
    )
    op.create_index(
        "idx_store_expires_at",
        "store",
        ["expires_at"],
        unique=False,
        postgresql_where=sa.text("expires_at IS NOT NULL"),
    )
    op.execute(
        """
        CREATE FUNCTION store_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF TG_OP IN ('DELETE', 'TRUNCATE') THEN
                RAISE EXCEPTION 'memory store is append-only: % refused', TG_OP
                    USING ERRCODE = 'insufficient_privilege';
            END IF;
            IF NEW.prefix IS DISTINCT FROM OLD.prefix OR NEW.key IS DISTINCT FROM OLD.key
               OR NEW.value IS DISTINCT FROM OLD.value OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
                RAISE EXCEPTION 'memory store is append-only: record %.% cannot change', OLD.prefix, OLD.key
                    USING ERRCODE = 'insufficient_privilege';
            END IF;
            RETURN NEW;
        END
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER store_append_only_row BEFORE UPDATE OR DELETE ON store "
        "FOR EACH ROW EXECUTE FUNCTION store_append_only()"
    )
    op.execute(
        "CREATE TRIGGER store_append_only_truncate BEFORE TRUNCATE ON store "
        "FOR EACH STATEMENT EXECUTE FUNCTION store_append_only()"
    )
    op.execute("ALTER TABLE store ENABLE ROW LEVEL SECURITY")
    readers = ", ".join(READERS)
    op.execute(f"GRANT SELECT ON store TO {readers}")
    op.execute(f"CREATE POLICY store_read ON store FOR SELECT TO {readers} USING (true)")
    for role, owned in WRITERS.items():
        name = role.removeprefix("hdt_")
        op.execute(f"GRANT INSERT, UPDATE (value, updated_at, expires_at, ttl_minutes) ON store TO {role}")
        op.execute(f"CREATE POLICY store_insert_{name} ON store FOR INSERT TO {role} WITH CHECK ({owned})")
        op.execute(
            f"CREATE POLICY store_update_{name} ON store FOR UPDATE TO {role} "
            f"USING ({owned}) WITH CHECK ({owned})"
        )


def downgrade() -> None:
    op.drop_table("store")
    op.execute("DROP FUNCTION store_append_only()")
