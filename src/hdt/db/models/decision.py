"""Phase 06 council tables: events, round commits, decision cards, the decision outbox, LangGraph checkpoints.

Written only by `hdt_council` (migration `0009_council`):
- `council_events`: one row per convened (coin, as_of, source) candidate; the trigger inserts it (natural-key
  dedupe) and the meeting worker moves it through pending -> running -> done | failed, or skipped;
- `decision_commits`: the blind round-1 commit (per-agent forecast sha256 and the round sha256), insert-only;
- `decision_cards`, `decision_forecasts`, `decision_claims` and `decision_outbox`: written in ONE transaction
  by `emit_decision`; the console, the public publisher, the Telegram bot and the scorer read them (column
  names are their read contract, `research/p05-08-integration-map.md` section 2);
- `checkpoint_*`: the LangGraph PostgresSaver schema (langgraph-checkpoint-postgres 3.1.x migrations 0-9),
  created here so the council never needs DDL rights or `PostgresSaver.setup()`.
`forecast` / `claim` hold canonical JSON of `AgentForecast` / `Claim`; `consensus` and `manager_rule` are
arrays of `{passed, text}`; `timeline` is an array of `DecisionTimelineEntry`.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Date,
    Float,
    Index,
    Integer,
    LargeBinary,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from hdt.db.base import Base

EVENT_STATUSES = ("pending", "running", "done", "failed", "skipped")
OUTCOMES = ("LONG", "SHORT", "NO_TRADE", "HOLD", "EXIT")
SOURCES = ("LTX", "MIGRATION", "HOLLOW_HYPE", "HELD")
OUTBOX_STATUSES = ("pending", "published")
STANCES = ("LONG", "SHORT", "NEUTRAL", "ABSTAIN")


def _in(column: str, values: tuple[str, ...]) -> str:
    return f"{column} IN ({', '.join(repr(v) for v in values)})"


class CouncilEventRow(Base):
    __tablename__ = "council_events"
    __table_args__ = (
        CheckConstraint(_in("status", EVENT_STATUSES), name="status"),
        CheckConstraint(_in("source", SOURCES), name="source"),
        UniqueConstraint("coin_id", "as_of", "source", name="uq_council_events_coin_id_as_of_source"),
        Index("ix_council_events_coin_id_as_of", "coin_id", "as_of"),
        Index(
            "ix_council_events_open", "created_at", postgresql_where=text("status IN ('pending', 'running')")
        ),
        Index(
            "ix_council_events_unpruned",
            "updated_at",
            postgresql_where=text("status IN ('done', 'failed') AND checkpoint_pruned_at IS NULL"),
        ),
    )

    event_id: Mapped[str] = mapped_column(Text, primary_key=True)
    coin_id: Mapped[int] = mapped_column(Integer)
    as_of: Mapped[datetime]
    source: Mapped[str] = mapped_column(Text)
    candidate: Mapped[dict[str, Any]]
    config_version_ids: Mapped[dict[str, Any]]
    """Config versions pinned when the event was admitted (the meeting runs with exactly these)."""
    unscored: Mapped[bool] = mapped_column(Boolean)
    shadow_only: Mapped[bool] = mapped_column(Boolean)
    status: Mapped[str] = mapped_column(Text)
    skip_reason: Mapped[str | None] = mapped_column(Text)
    attempts: Mapped[int] = mapped_column(Integer)
    error: Mapped[str | None] = mapped_column(Text)
    msg_id: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime]
    updated_at: Mapped[datetime]
    checkpoint_pruned_at: Mapped[datetime | None]
    """When the meeting's LangGraph checkpoint thread was deleted (None: a done / failed event keeps it)."""


class DecisionCommitRow(Base):
    """Blind round-1 commit, written before any claim is shared (insert-only)."""

    __tablename__ = "decision_commits"

    event_id: Mapped[str] = mapped_column(Text, primary_key=True)
    round: Mapped[int] = mapped_column(Integer, primary_key=True)
    agent: Mapped[str] = mapped_column(Text, primary_key=True)
    forecast_sha256: Mapped[str] = mapped_column(Text)
    round_sha256: Mapped[str] = mapped_column(Text)
    committed_at: Mapped[datetime]


class DecisionCardRow(Base):
    __tablename__ = "decision_cards"
    __table_args__ = (
        CheckConstraint(_in("outcome", OUTCOMES), name="outcome"),
        CheckConstraint(_in("source", SOURCES), name="source"),
        CheckConstraint("side IS NULL OR side IN ('LONG', 'SHORT')", name="side"),
        CheckConstraint("rounds BETWEEN 0 AND 3", name="rounds"),
        Index("ix_decision_cards_as_of", "as_of"),
        Index("ix_decision_cards_coin_id_as_of", "coin_id", "as_of"),
    )

    event_id: Mapped[str] = mapped_column(Text, primary_key=True)
    coin_id: Mapped[int] = mapped_column(Integer)
    symbol: Mapped[str] = mapped_column(Text)
    as_of: Mapped[datetime]
    source: Mapped[str] = mapped_column(Text)
    outcome: Mapped[str] = mapped_column(Text)
    side: Mapped[str | None] = mapped_column(Text)
    manager_size: Mapped[float] = mapped_column(Float)
    p_pooled: Mapped[float | None] = mapped_column(Float)
    disagreement: Mapped[float | None] = mapped_column(Float)
    rounds: Mapped[int] = mapped_column(Integer)
    stop_reason: Mapped[str] = mapped_column(Text)
    candidate_id: Mapped[str | None] = mapped_column(Text)
    candidate_set_sha256: Mapped[str | None] = mapped_column(Text)
    packet_sha256: Mapped[str | None] = mapped_column(Text)
    target_type: Mapped[str] = mapped_column(Text)
    label_spec_version: Mapped[str] = mapped_column(Text)
    config_version_ids: Mapped[dict[str, Any]]
    universe_date: Mapped[date] = mapped_column(Date)
    summary: Mapped[str] = mapped_column(Text)
    consensus: Mapped[list[dict[str, Any]]] = mapped_column(JSONB)
    manager_rule: Mapped[list[dict[str, Any]]] = mapped_column(JSONB)
    timeline: Mapped[list[dict[str, Any]]] = mapped_column(JSONB)
    unscored: Mapped[bool] = mapped_column(Boolean)
    shadow_only: Mapped[bool] = mapped_column(Boolean)
    held_side: Mapped[str | None] = mapped_column(Text)
    intent: Mapped[str | None] = mapped_column(Text)
    """DecisionMsg intent sent to Risk (None: nothing sent)."""
    round1_sha256: Mapped[str] = mapped_column(Text)
    params: Mapped[dict[str, Any]]
    """w / a / r / b in force at decision time, params_version, regime and pool method."""
    agents: Mapped[dict[str, Any]]
    """Per agent: packet_sha256, model_slug, provider, agent_version, prompt_hash, skill_commit."""
    hard_evidence_version: Mapped[str] = mapped_column(Text)
    card_sha256: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime]


class DecisionForecastRow(Base):
    __tablename__ = "decision_forecasts"
    __table_args__ = (CheckConstraint(_in("stance", STANCES), name="stance"),)

    event_id: Mapped[str] = mapped_column(Text, primary_key=True)
    agent: Mapped[str] = mapped_column(Text, primary_key=True)
    round: Mapped[int] = mapped_column(Integer, primary_key=True)
    forecast: Mapped[dict[str, Any]]
    """The effective forecast (after the council's bounds); the one that counts."""
    submitted: Mapped[dict[str, Any]]
    """The forecast as the runner returned it."""
    revision: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    stance: Mapped[str] = mapped_column(Text)
    weight_norm: Mapped[float | None] = mapped_column(Float)
    commit_sha256: Mapped[str] = mapped_column(Text)


class DecisionClaimRow(Base):
    __tablename__ = "decision_claims"
    __table_args__ = (Index("ix_decision_claims_source_agent", "source_agent"),)

    event_id: Mapped[str] = mapped_column(Text, primary_key=True)
    round: Mapped[int] = mapped_column(Integer, primary_key=True)
    shared_id: Mapped[str] = mapped_column(Text, primary_key=True)
    source_agent: Mapped[str] = mapped_column(Text)
    original_claim_id: Mapped[str] = mapped_column(Text)
    claim_sha256: Mapped[str] = mapped_column(Text)
    claim: Mapped[dict[str, Any]]
    reject_reason: Mapped[str | None] = mapped_column(Text)
    penalty: Mapped[bool] = mapped_column(Boolean)
    shared: Mapped[bool] = mapped_column(Boolean)


class DecisionOutboxRow(Base):
    """`DecisionMsg` outbox, written with the card; the relay XADDs it to `decisions` once."""

    __tablename__ = "decision_outbox"
    __table_args__ = (
        CheckConstraint(_in("status", OUTBOX_STATUSES), name="status"),
        Index("ix_decision_outbox_pending", "created_at", postgresql_where=text("status = 'pending'")),
    )

    event_id: Mapped[str] = mapped_column(Text, primary_key=True)
    payload: Mapped[dict[str, Any]]
    as_of: Mapped[datetime]
    status: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime]
    published_at: Mapped[datetime | None]
    stream_id: Mapped[str | None] = mapped_column(Text)


# ------------------------------------------------------------------ LangGraph PostgresSaver schema


class CheckpointMigrationRow(Base):
    __tablename__ = "checkpoint_migrations"

    v: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)


class CheckpointRow(Base):
    __tablename__ = "checkpoints"
    __table_args__ = (Index("checkpoints_thread_id_idx", "thread_id"),)

    thread_id: Mapped[str] = mapped_column(Text, primary_key=True)
    checkpoint_ns: Mapped[str] = mapped_column(Text, primary_key=True, server_default=text("''"))
    checkpoint_id: Mapped[str] = mapped_column(Text, primary_key=True)
    parent_checkpoint_id: Mapped[str | None] = mapped_column(Text)
    type: Mapped[str | None] = mapped_column(Text)
    checkpoint: Mapped[dict[str, Any]]
    metadata_: Mapped[dict[str, Any]] = mapped_column("metadata", JSONB, server_default=text("'{}'::jsonb"))


class CheckpointBlobRow(Base):
    __tablename__ = "checkpoint_blobs"
    __table_args__ = (Index("checkpoint_blobs_thread_id_idx", "thread_id"),)

    thread_id: Mapped[str] = mapped_column(Text, primary_key=True)
    checkpoint_ns: Mapped[str] = mapped_column(Text, primary_key=True, server_default=text("''"))
    channel: Mapped[str] = mapped_column(Text, primary_key=True)
    version: Mapped[str] = mapped_column(Text, primary_key=True)
    type: Mapped[str] = mapped_column(Text)
    blob: Mapped[bytes | None] = mapped_column(LargeBinary)


class CheckpointWriteRow(Base):
    __tablename__ = "checkpoint_writes"
    __table_args__ = (Index("checkpoint_writes_thread_id_idx", "thread_id"),)

    thread_id: Mapped[str] = mapped_column(Text, primary_key=True)
    checkpoint_ns: Mapped[str] = mapped_column(Text, primary_key=True, server_default=text("''"))
    checkpoint_id: Mapped[str] = mapped_column(Text, primary_key=True)
    task_id: Mapped[str] = mapped_column(Text, primary_key=True)
    idx: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    channel: Mapped[str] = mapped_column(Text)
    type: Mapped[str | None] = mapped_column(Text)
    blob: Mapped[bytes] = mapped_column(LargeBinary)
    task_path: Mapped[str] = mapped_column(Text, server_default=text("''"))
