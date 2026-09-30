"""Phase 07 news evidence tables (migration `0008_news`).

- `news_items`: ingested feed / API / announcement items mapped to coins, with ingest-time duplicate marks
  (exact normalized title or MinHash near duplicate of an earlier item of the same coins). Insert-only.
- `news_announcements`: recorded exchange announcements (Binance: the only T0 listing source).
- `news_unlocks`: versioned unlock-calendar entries; a changed entry is a new row, a vanished one a
  `removed` row, so the schedule known at any `as_of` is rebuilt point in time.
- `news_verdicts`: extractor + quote check + two judges per item and `rule_version`, with the code-computed
  tier and `check_official` verdict. `processed_at` is when the verdict became known.
- `news_attention`: CMC attention (New_i, Roll_i) and NewsZ_i per coin and attention slot.
- `news_candidates`: HOLLOW_HYPE candidates published on `candidates`, with the target type and label spec
  fixed at insert (a republish rebuilds the `Candidate` from the row alone).
- `news_source_state`: per-source poll health (feeds the veto scan's `scan_fresh_at`).
- `news_veto_scans`: every `RiskFlags` the veto scan published.
- `news_labels`: the human label set (labeling app) used to measure precision and recall.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import ARRAY, Boolean, CheckConstraint, Float, Index, Integer, Text
from sqlalchemy.orm import Mapped, mapped_column

from hdt.db.base import Base
from hdt.news.classes import EVENT_CLASSES

SOURCE_KINDS = ("rss", "api", "binance")
VERDICT_STATUSES = ("agreed", "disagree", "no_event", "quote_failed", "llm_failed")
TIERS = ("T0", "T1", "T2", "T3")
DIRECTIONS = ("up", "down", "none")
NEWS_MODES = ("fade_hype", "fade_panic", "ride")
TARGET_TYPES = ("RAW_12H", "RESID_12H")


def _in(column: str, values: tuple[str, ...], *, nullable: bool = False) -> str:
    listed = ", ".join(repr(v) for v in values)
    return f"{column} IS NULL OR {column} IN ({listed})" if nullable else f"{column} IN ({listed})"


class NewsItemRow(Base):
    __tablename__ = "news_items"
    __table_args__ = (
        CheckConstraint(_in("source_kind", SOURCE_KINDS), name="source_kind"),
        CheckConstraint(_in("tier", TIERS), name="tier"),
        CheckConstraint(_in("duplicate_kind", ("exact", "near"), nullable=True), name="duplicate_kind"),
        CheckConstraint("cardinality(coin_ids) >= 1", name="coin_ids"),
        Index("ix_news_items_ingested_at", "ingested_at"),
        Index("uq_news_items_url_key", "url_key", unique=True),
    )

    item_id: Mapped[str] = mapped_column(Text, primary_key=True)
    source_key: Mapped[str] = mapped_column(Text)
    source_kind: Mapped[str] = mapped_column(Text)
    source_name: Mapped[str] = mapped_column(Text)
    url: Mapped[str] = mapped_column(Text)
    url_key: Mapped[str] = mapped_column(Text)
    domain: Mapped[str] = mapped_column(Text)
    tier: Mapped[str] = mapped_column(Text)
    title: Mapped[str] = mapped_column(Text)
    summary: Mapped[str | None] = mapped_column(Text)
    content: Mapped[str | None] = mapped_column(Text)
    published_at: Mapped[datetime | None]
    ingested_at: Mapped[datetime]
    coin_ids: Mapped[list[int]] = mapped_column(ARRAY(Integer))
    title_fingerprint: Mapped[str] = mapped_column(Text)
    duplicate_of: Mapped[str | None] = mapped_column(Text)
    duplicate_kind: Mapped[str | None] = mapped_column(Text)
    similarity: Mapped[float | None] = mapped_column(Float)
    raw_sha256: Mapped[str] = mapped_column(Text)


class NewsAnnouncementRow(Base):
    __tablename__ = "news_announcements"
    __table_args__ = (Index("ix_news_announcements_exchange", "exchange", "published_at"),)

    exchange: Mapped[str] = mapped_column(Text, primary_key=True)
    announcement_id: Mapped[str] = mapped_column(Text, primary_key=True)
    catalog: Mapped[str] = mapped_column(Text)
    title: Mapped[str] = mapped_column(Text)
    url: Mapped[str] = mapped_column(Text)
    published_at: Mapped[datetime]
    recorded_at: Mapped[datetime]


class NewsUnlockRow(Base):
    __tablename__ = "news_unlocks"
    __table_args__ = (
        CheckConstraint(_in("tier", TIERS), name="tier"),
        Index("ix_news_unlocks_coin_id", "coin_id", "unlock_at"),
    )

    source: Mapped[str] = mapped_column(Text, primary_key=True)
    coin_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    unlock_at: Mapped[datetime] = mapped_column(primary_key=True)
    category: Mapped[str] = mapped_column(Text, primary_key=True)
    recorded_at: Mapped[datetime] = mapped_column(primary_key=True)
    amount_tokens: Mapped[float | None] = mapped_column(Float)
    pct_of_circulating: Mapped[float | None] = mapped_column(Float)
    tier: Mapped[str] = mapped_column(Text)
    removed: Mapped[bool] = mapped_column(Boolean)
    content_sha256: Mapped[str] = mapped_column(Text)


class NewsVerdictRow(Base):
    __tablename__ = "news_verdicts"
    __table_args__ = (
        CheckConstraint(_in("status", VERDICT_STATUSES), name="status"),
        CheckConstraint(_in("processor", ("news", "veto_scan")), name="processor"),
        CheckConstraint(_in("tier", TIERS), name="tier"),
        CheckConstraint(_in("direction", DIRECTIONS, nullable=True), name="direction"),
        CheckConstraint(_in("event_class", EVENT_CLASSES, nullable=True), name="event_class"),
        CheckConstraint("hardness IS NULL OR (hardness >= 0 AND hardness <= 1)", name="hardness"),
        CheckConstraint("novelty IS NULL OR (novelty >= 0 AND novelty <= 1)", name="novelty"),
        CheckConstraint("confidence IS NULL OR (confidence >= 0 AND confidence <= 1)", name="confidence"),
        CheckConstraint(_in("known_event", ("new", "exact", "near"), nullable=True), name="known_event"),
        CheckConstraint("status <> 'agreed' OR quote_ok", name="agreed_quote"),
        Index("ix_news_verdicts_processed_at", "processed_at"),
    )

    item_id: Mapped[str] = mapped_column(Text, primary_key=True)
    rule_version: Mapped[str] = mapped_column(Text, primary_key=True)
    processed_at: Mapped[datetime]
    processor: Mapped[str] = mapped_column(Text)
    coin_ids: Mapped[list[int]] = mapped_column(ARRAY(Integer))
    status: Mapped[str] = mapped_column(Text)
    event_key: Mapped[str | None] = mapped_column(Text)
    event_class: Mapped[str | None] = mapped_column(Text)
    direction: Mapped[str | None] = mapped_column(Text)
    hardness: Mapped[float | None] = mapped_column(Float)
    novelty: Mapped[float | None] = mapped_column(Float)
    confidence: Mapped[float | None] = mapped_column(Float)
    event_time: Mapped[datetime | None]
    quote: Mapped[str | None] = mapped_column(Text)
    quote_ok: Mapped[bool] = mapped_column(Boolean)
    domain: Mapped[str] = mapped_column(Text)
    tier: Mapped[str] = mapped_column(Text)
    official: Mapped[bool] = mapped_column(Boolean)
    official_ref: Mapped[str | None] = mapped_column(Text)
    class_a: Mapped[str | None] = mapped_column(Text)
    class_b: Mapped[str | None] = mapped_column(Text)
    direction_a: Mapped[str | None] = mapped_column(Text)
    direction_b: Mapped[str | None] = mapped_column(Text)
    known_event: Mapped[str | None] = mapped_column(Text)
    detail: Mapped[dict[str, Any]]


class NewsAttentionRow(Base):
    __tablename__ = "news_attention"
    __table_args__ = (Index("ix_news_attention_as_of", "as_of"),)

    coin_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    as_of: Mapped[datetime] = mapped_column(primary_key=True)
    trending_rank: Mapped[int | None] = mapped_column(Integer)
    prior_rank: Mapped[int | None] = mapped_column(Integer)
    new_i: Mapped[bool] = mapped_column(Boolean)
    roll_i: Mapped[float | None] = mapped_column(Float)
    gainer_rank: Mapped[int | None] = mapped_column(Integer)
    new_listing: Mapped[bool] = mapped_column(Boolean)
    news_count: Mapped[int] = mapped_column(Integer)
    news_baseline: Mapped[float] = mapped_column(Float)
    news_z: Mapped[float] = mapped_column(Float)
    attention: Mapped[bool] = mapped_column(Boolean)
    rule_version: Mapped[str] = mapped_column(Text)
    computed_at: Mapped[datetime]


class NewsCandidateRow(Base):
    __tablename__ = "news_candidates"
    __table_args__ = (
        CheckConstraint(_in("mode", NEWS_MODES), name="mode"),
        CheckConstraint(_in("target_type", TARGET_TYPES), name="target_type"),
    )

    coin_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    as_of: Mapped[datetime] = mapped_column(primary_key=True)
    mode: Mapped[str] = mapped_column(Text)
    score: Mapped[float] = mapped_column(Float)
    rule_version: Mapped[str] = mapped_column(Text)
    target_type: Mapped[str] = mapped_column(Text)
    """`TargetType` of the published `Candidate`, fixed at insert so a republish never reads the config."""
    label_spec_version: Mapped[str] = mapped_column(Text)
    """Label spec of the published `Candidate`, fixed at insert like `target_type`."""
    evidence: Mapped[dict[str, Any]]
    published_at: Mapped[datetime | None]


class NewsSourceStateRow(Base):
    __tablename__ = "news_source_state"

    source_key: Mapped[str] = mapped_column(Text, primary_key=True)
    last_attempt_at: Mapped[datetime]
    last_success_at: Mapped[datetime | None]
    last_error: Mapped[str | None] = mapped_column(Text)
    last_count: Mapped[int] = mapped_column(Integer)
    consecutive_failures: Mapped[int] = mapped_column(Integer)


class NewsVetoScanRow(Base):
    __tablename__ = "news_veto_scans"
    __table_args__ = (
        CheckConstraint("size_mult >= 0 AND size_mult <= 1", name="size_mult"),
        CheckConstraint("expires_at > as_of", name="expires"),
        Index("ix_news_veto_scans_as_of", "as_of"),
    )

    coin_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    as_of: Mapped[datetime] = mapped_column(primary_key=True)
    veto_long: Mapped[bool] = mapped_column(Boolean)
    veto_short: Mapped[bool] = mapped_column(Boolean)
    size_mult: Mapped[float] = mapped_column(Float)
    evidence_ref: Mapped[str | None] = mapped_column(Text)
    scan_fresh_at: Mapped[datetime]
    expires_at: Mapped[datetime]
    reasons: Mapped[dict[str, Any]]
    rule_version: Mapped[str] = mapped_column(Text)


class NewsLabelRow(Base):
    __tablename__ = "news_labels"
    __table_args__ = (
        CheckConstraint(_in("direction", DIRECTIONS), name="direction"),
        CheckConstraint(_in("event_class", EVENT_CLASSES), name="event_class"),
        CheckConstraint(_in("hardness", ("hard", "soft")), name="hardness"),
    )

    item_id: Mapped[str] = mapped_column(Text, primary_key=True)
    labeler: Mapped[str] = mapped_column(Text, primary_key=True)
    labeled_at: Mapped[datetime]
    event_class: Mapped[str] = mapped_column(Text)
    direction: Mapped[str] = mapped_column(Text)
    hardness: Mapped[str] = mapped_column(Text)
    duplicate: Mapped[bool] = mapped_column(Boolean)
    note: Mapped[str | None] = mapped_column(Text)
