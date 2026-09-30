"""Phase 07 news evidence tables (models: `hdt.db.models.news`).

Grants (least privilege):
- `hdt_news` (ingestion worker) writes items, announcements, unlock versions, verdicts, attention and
  HOLLOW_HYPE candidates (insert-only; only `news_candidates.published_at` is updated after the XADD),
  keeps `news_source_state` (upsert) and holds the label set of the labeling app;
- `hdt_veto_scan` reads everything it evaluates, judges pending bad-catalyst items itself (INSERT on
  `news_verdicts`, idempotent per item and rule version) and logs every published `RiskFlags`;
- `hdt_council` reads items, announcements, unlocks, verdicts and attention (the News agent's tools,
  `PgNewsAssessor`, `PgSourceLookup`);
- the console reads every table.

Revision ID: 0008_news
Revises: 0007_llm
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0008_news"
down_revision: str | None = "0007_llm"
branch_labels = None
depends_on = None

TS = sa.DateTime(timezone=True)
JSONB = postgresql.JSONB(astext_type=sa.Text())
TABLES = (
    "news_items",
    "news_announcements",
    "news_unlocks",
    "news_verdicts",
    "news_attention",
    "news_candidates",
    "news_source_state",
    "news_veto_scans",
    "news_labels",
)
TIERS = "'T0', 'T1', 'T2', 'T3'"
DIRECTIONS = "'up', 'down', 'none'"
EVENT_CLASSES = (
    "'LISTING', 'DELIST', 'EXPLOIT', 'UNLOCK', 'PARTNERSHIP', 'PRODUCT', 'REGULATORY', 'OTHER_FACT', "
    "'RUMOR', 'KOL_MEME', 'CIRCULAR', 'DENIAL', 'NO_EVENT'"
)
COUNCIL_READS = ("news_items", "news_announcements", "news_unlocks", "news_verdicts", "news_attention")
VETO_READS = ("news_items", "news_announcements", "news_unlocks", "news_verdicts", "news_source_state")


def upgrade() -> None:
    op.create_table(
        "news_items",
        sa.Column("item_id", sa.Text(), nullable=False),
        sa.Column("source_key", sa.Text(), nullable=False),
        sa.Column("source_kind", sa.Text(), nullable=False),
        sa.Column("source_name", sa.Text(), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("url_key", sa.Text(), nullable=False),
        sa.Column("domain", sa.Text(), nullable=False),
        sa.Column("tier", sa.Text(), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("summary", sa.Text(), nullable=True),
        sa.Column("content", sa.Text(), nullable=True),
        sa.Column("published_at", TS, nullable=True),
        sa.Column("ingested_at", TS, nullable=False),
        sa.Column("coin_ids", postgresql.ARRAY(sa.Integer()), nullable=False),
        sa.Column("title_fingerprint", sa.Text(), nullable=False),
        sa.Column("duplicate_of", sa.Text(), nullable=True),
        sa.Column("duplicate_kind", sa.Text(), nullable=True),
        sa.Column("similarity", sa.Float(), nullable=True),
        sa.Column("raw_sha256", sa.Text(), nullable=False),
        sa.CheckConstraint(
            "source_kind IN ('rss', 'api', 'binance')", name=op.f("ck_news_items_source_kind")
        ),
        sa.CheckConstraint(f"tier IN ({TIERS})", name=op.f("ck_news_items_tier")),
        sa.CheckConstraint(
            "duplicate_kind IS NULL OR duplicate_kind IN ('exact', 'near')",
            name=op.f("ck_news_items_duplicate_kind"),
        ),
        sa.CheckConstraint("cardinality(coin_ids) >= 1", name=op.f("ck_news_items_coin_ids")),
        sa.PrimaryKeyConstraint("item_id", name=op.f("pk_news_items")),
    )
    op.create_index("ix_news_items_ingested_at", "news_items", ["ingested_at"])
    op.create_index("uq_news_items_url_key", "news_items", ["url_key"], unique=True)
    op.create_table(
        "news_announcements",
        sa.Column("exchange", sa.Text(), nullable=False),
        sa.Column("announcement_id", sa.Text(), nullable=False),
        sa.Column("catalog", sa.Text(), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("published_at", TS, nullable=False),
        sa.Column("recorded_at", TS, nullable=False),
        sa.PrimaryKeyConstraint("exchange", "announcement_id", name=op.f("pk_news_announcements")),
    )
    op.create_index("ix_news_announcements_exchange", "news_announcements", ["exchange", "published_at"])
    op.create_table(
        "news_unlocks",
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("coin_id", sa.Integer(), nullable=False),
        sa.Column("unlock_at", TS, nullable=False),
        sa.Column("category", sa.Text(), nullable=False),
        sa.Column("recorded_at", TS, nullable=False),
        sa.Column("amount_tokens", sa.Float(), nullable=True),
        sa.Column("pct_of_circulating", sa.Float(), nullable=True),
        sa.Column("tier", sa.Text(), nullable=False),
        sa.Column("removed", sa.Boolean(), nullable=False),
        sa.Column("content_sha256", sa.Text(), nullable=False),
        sa.CheckConstraint(f"tier IN ({TIERS})", name=op.f("ck_news_unlocks_tier")),
        sa.PrimaryKeyConstraint(
            "source", "coin_id", "unlock_at", "category", "recorded_at", name=op.f("pk_news_unlocks")
        ),
    )
    op.create_index("ix_news_unlocks_coin_id", "news_unlocks", ["coin_id", "unlock_at"])
    op.create_table(
        "news_verdicts",
        sa.Column("item_id", sa.Text(), nullable=False),
        sa.Column("rule_version", sa.Text(), nullable=False),
        sa.Column("processed_at", TS, nullable=False),
        sa.Column("processor", sa.Text(), nullable=False),
        sa.Column("coin_ids", postgresql.ARRAY(sa.Integer()), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("event_key", sa.Text(), nullable=True),
        sa.Column("event_class", sa.Text(), nullable=True),
        sa.Column("direction", sa.Text(), nullable=True),
        sa.Column("hardness", sa.Float(), nullable=True),
        sa.Column("novelty", sa.Float(), nullable=True),
        sa.Column("confidence", sa.Float(), nullable=True),
        sa.Column("event_time", TS, nullable=True),
        sa.Column("quote", sa.Text(), nullable=True),
        sa.Column("quote_ok", sa.Boolean(), nullable=False),
        sa.Column("domain", sa.Text(), nullable=False),
        sa.Column("tier", sa.Text(), nullable=False),
        sa.Column("official", sa.Boolean(), nullable=False),
        sa.Column("official_ref", sa.Text(), nullable=True),
        sa.Column("class_a", sa.Text(), nullable=True),
        sa.Column("class_b", sa.Text(), nullable=True),
        sa.Column("direction_a", sa.Text(), nullable=True),
        sa.Column("direction_b", sa.Text(), nullable=True),
        sa.Column("known_event", sa.Text(), nullable=True),
        sa.Column("detail", JSONB, nullable=False),
        sa.CheckConstraint(
            "status IN ('agreed', 'disagree', 'no_event', 'quote_failed', 'llm_failed')",
            name=op.f("ck_news_verdicts_status"),
        ),
        sa.CheckConstraint("processor IN ('news', 'veto_scan')", name=op.f("ck_news_verdicts_processor")),
        sa.CheckConstraint(f"tier IN ({TIERS})", name=op.f("ck_news_verdicts_tier")),
        sa.CheckConstraint(
            f"direction IS NULL OR direction IN ({DIRECTIONS})", name=op.f("ck_news_verdicts_direction")
        ),
        sa.CheckConstraint(
            f"event_class IS NULL OR event_class IN ({EVENT_CLASSES})",
            name=op.f("ck_news_verdicts_event_class"),
        ),
        sa.CheckConstraint(
            "hardness IS NULL OR (hardness >= 0 AND hardness <= 1)", name=op.f("ck_news_verdicts_hardness")
        ),
        sa.CheckConstraint(
            "novelty IS NULL OR (novelty >= 0 AND novelty <= 1)", name=op.f("ck_news_verdicts_novelty")
        ),
        sa.CheckConstraint(
            "confidence IS NULL OR (confidence >= 0 AND confidence <= 1)",
            name=op.f("ck_news_verdicts_confidence"),
        ),
        sa.CheckConstraint(
            "known_event IS NULL OR known_event IN ('new', 'exact', 'near')",
            name=op.f("ck_news_verdicts_known_event"),
        ),
        sa.CheckConstraint("status <> 'agreed' OR quote_ok", name=op.f("ck_news_verdicts_agreed_quote")),
        sa.PrimaryKeyConstraint("item_id", "rule_version", name=op.f("pk_news_verdicts")),
    )
    op.create_index("ix_news_verdicts_processed_at", "news_verdicts", ["processed_at"])
    op.create_table(
        "news_attention",
        sa.Column("coin_id", sa.Integer(), nullable=False),
        sa.Column("as_of", TS, nullable=False),
        sa.Column("trending_rank", sa.Integer(), nullable=True),
        sa.Column("prior_rank", sa.Integer(), nullable=True),
        sa.Column("new_i", sa.Boolean(), nullable=False),
        sa.Column("roll_i", sa.Float(), nullable=True),
        sa.Column("gainer_rank", sa.Integer(), nullable=True),
        sa.Column("new_listing", sa.Boolean(), nullable=False),
        sa.Column("news_count", sa.Integer(), nullable=False),
        sa.Column("news_baseline", sa.Float(), nullable=False),
        sa.Column("news_z", sa.Float(), nullable=False),
        sa.Column("attention", sa.Boolean(), nullable=False),
        sa.Column("rule_version", sa.Text(), nullable=False),
        sa.Column("computed_at", TS, nullable=False),
        sa.PrimaryKeyConstraint("coin_id", "as_of", name=op.f("pk_news_attention")),
    )
    op.create_index("ix_news_attention_as_of", "news_attention", ["as_of"])
    op.create_table(
        "news_candidates",
        sa.Column("coin_id", sa.Integer(), nullable=False),
        sa.Column("as_of", TS, nullable=False),
        sa.Column("mode", sa.Text(), nullable=False),
        sa.Column("score", sa.Float(), nullable=False),
        sa.Column("rule_version", sa.Text(), nullable=False),
        sa.Column("target_type", sa.Text(), nullable=False),
        sa.Column("label_spec_version", sa.Text(), nullable=False),
        sa.Column("evidence", JSONB, nullable=False),
        sa.Column("published_at", TS, nullable=True),
        sa.CheckConstraint(
            "mode IN ('fade_hype', 'fade_panic', 'ride')", name=op.f("ck_news_candidates_mode")
        ),
        sa.CheckConstraint(
            "target_type IN ('RAW_12H', 'RESID_12H')", name=op.f("ck_news_candidates_target_type")
        ),
        sa.PrimaryKeyConstraint("coin_id", "as_of", name=op.f("pk_news_candidates")),
    )
    op.create_table(
        "news_source_state",
        sa.Column("source_key", sa.Text(), nullable=False),
        sa.Column("last_attempt_at", TS, nullable=False),
        sa.Column("last_success_at", TS, nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("last_count", sa.Integer(), nullable=False),
        sa.Column("consecutive_failures", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("source_key", name=op.f("pk_news_source_state")),
    )
    op.create_table(
        "news_veto_scans",
        sa.Column("coin_id", sa.Integer(), nullable=False),
        sa.Column("as_of", TS, nullable=False),
        sa.Column("veto_long", sa.Boolean(), nullable=False),
        sa.Column("veto_short", sa.Boolean(), nullable=False),
        sa.Column("size_mult", sa.Float(), nullable=False),
        sa.Column("evidence_ref", sa.Text(), nullable=True),
        sa.Column("scan_fresh_at", TS, nullable=False),
        sa.Column("expires_at", TS, nullable=False),
        sa.Column("reasons", JSONB, nullable=False),
        sa.Column("rule_version", sa.Text(), nullable=False),
        sa.CheckConstraint("size_mult >= 0 AND size_mult <= 1", name=op.f("ck_news_veto_scans_size_mult")),
        sa.CheckConstraint("expires_at > as_of", name=op.f("ck_news_veto_scans_expires")),
        sa.PrimaryKeyConstraint("coin_id", "as_of", name=op.f("pk_news_veto_scans")),
    )
    op.create_index("ix_news_veto_scans_as_of", "news_veto_scans", ["as_of"])
    op.create_table(
        "news_labels",
        sa.Column("item_id", sa.Text(), nullable=False),
        sa.Column("labeler", sa.Text(), nullable=False),
        sa.Column("labeled_at", TS, nullable=False),
        sa.Column("event_class", sa.Text(), nullable=False),
        sa.Column("direction", sa.Text(), nullable=False),
        sa.Column("hardness", sa.Text(), nullable=False),
        sa.Column("duplicate", sa.Boolean(), nullable=False),
        sa.Column("note", sa.Text(), nullable=True),
        sa.CheckConstraint(f"direction IN ({DIRECTIONS})", name=op.f("ck_news_labels_direction")),
        sa.CheckConstraint(f"event_class IN ({EVENT_CLASSES})", name=op.f("ck_news_labels_event_class")),
        sa.CheckConstraint("hardness IN ('hard', 'soft')", name=op.f("ck_news_labels_hardness")),
        sa.PrimaryKeyConstraint("item_id", "labeler", name=op.f("pk_news_labels")),
    )

    insert_only = ", ".join(
        (
            "news_items",
            "news_announcements",
            "news_unlocks",
            "news_verdicts",
            "news_attention",
            "news_candidates",
        )
    )
    op.execute(f"GRANT SELECT, INSERT ON {insert_only} TO hdt_news")
    op.execute("GRANT UPDATE (published_at) ON news_candidates TO hdt_news")
    op.execute("GRANT SELECT, INSERT, UPDATE ON news_source_state, news_labels TO hdt_news")
    op.execute(f"GRANT SELECT ON {', '.join(VETO_READS)} TO hdt_veto_scan")
    op.execute("GRANT SELECT, INSERT ON news_verdicts, news_veto_scans TO hdt_veto_scan")
    op.execute(f"GRANT SELECT ON {', '.join(COUNCIL_READS)} TO hdt_council")
    op.execute(f"GRANT SELECT ON {', '.join(TABLES)} TO hdt_console_ro")


def downgrade() -> None:
    for table in reversed(TABLES):
        op.drop_table(table)
