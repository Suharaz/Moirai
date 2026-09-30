"""Phase 08 tables: labels, scored forecasts, learned weights and calibration, lesson A/B, gate progress.

Everything learned is split by `(target_type, label_spec_version)` and recomputed by the scorer from the
full labeled history in canonical `(as_of, event_id)` order, so every write is an idempotent upsert keyed
by that history (rescoring twice writes identical rows). Writers and readers (migration `0010_scoring`):
- `hdt_scorer` writes every table here except `lesson_shadow_forecasts` (written by `hdt_council` inside
  the decision transaction, `hdt.scoring.shadow.PgShadowForecastSink`);
- `hdt_council` reads `scoring_params`, `stacking_models` and `claims_audit_flags`
  (`hdt.scoring.params.PgParamsSource`);
- the console reads everything; the public publisher reads `scored_forecasts`, `weight_history`,
  `calibration_bins`, `calibration_stats`, `gate_progress`; the Telegram bot reads `weight_history` and
  `scored_forecasts`.
- `lessons` is a SQL view over the append-only lesson events in `store` (migration only, no model), so a
  config-api approval is visible at once, without a scorer run.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Float,
    Index,
    Integer,
    PrimaryKeyConstraint,
    SmallInteger,
    Text,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from hdt.db.base import Base

TARGET_TYPES = ("RAW_12H", "RESID_12H")
AGENTS = ("crowding", "technical", "micro", "fundamental", "news", "macro")
POOLED = "pooled"
LABEL_STATUSES = ("resolved", "missing", "error")
REFLECTION_STATUSES = (
    "proposed",
    "skipped_no_wrong_agent",
    "skipped_shadow_busy",
    "skipped_duplicate",
    "invalid_output",
    "invalid_template",
)


def _in(column: str, values: tuple[str, ...]) -> str:
    return f"{column} IN ({', '.join(repr(v) for v in values)})"


class ScoringLabelRow(Base):
    """The 12 h label of one scored event (its own `target_type` only); `missing` is recorded, never
    dropped, and `error` records a card the resolver could not process (`error` holds why). Both are
    re-resolved for a bounded window (`resolver.missing_retry_h`); a `resolved` label never changes."""

    __tablename__ = "scoring_labels"
    __table_args__ = (
        CheckConstraint(_in("target_type", TARGET_TYPES), name="target_type"),
        CheckConstraint(_in("status", LABEL_STATUSES), name="status"),
        CheckConstraint("(status = 'resolved') = (y IS NOT NULL)", name="resolved_has_y"),
        CheckConstraint("(status = 'error') = (error IS NOT NULL)", name="error_has_reason"),
        CheckConstraint("y IS NULL OR y IN (0, 1)", name="y"),
        CheckConstraint("barrier IS NULL OR barrier IN (-1, 0, 1)", name="barrier"),
        CheckConstraint("barrier_y IS NULL OR barrier_y IN (0, 1)", name="barrier_y"),
        Index("ix_scoring_labels_target", "target_type", "label_spec_version", "as_of"),
    )

    event_id: Mapped[str] = mapped_column(Text, primary_key=True)
    coin_id: Mapped[int] = mapped_column(Integer)
    symbol: Mapped[str] = mapped_column(Text)
    as_of: Mapped[datetime]
    target_type: Mapped[str] = mapped_column(Text)
    label_spec_version: Mapped[str] = mapped_column(Text)
    horizon_h: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(Text)
    y: Mapped[int | None] = mapped_column(SmallInteger)
    label_value: Mapped[float | None] = mapped_column(Float)
    coin_return: Mapped[float | None] = mapped_column(Float)
    btc_return: Mapped[float | None] = mapped_column(Float)
    beta_btc: Mapped[float | None] = mapped_column(Float)
    atr: Mapped[float | None] = mapped_column(Float)
    """ATR(1h) as a fraction of the mark at as_of (packet `atr_1h / mark_price`)."""
    regime: Mapped[str | None] = mapped_column(Text)
    barrier: Mapped[int | None] = mapped_column(SmallInteger)
    barrier_y: Mapped[int | None] = mapped_column(SmallInteger)
    resolved_at: Mapped[datetime]
    """When the label row was last written (the latest attempt for `missing` and `error`)."""
    error: Mapped[str | None] = mapped_column(Text)
    outcomes_recorded_at: Mapped[datetime | None]
    """When the per-agent `OutcomeRecord`s went to episodic memory (None: not yet)."""


class ScoredForecastRow(Base):
    """Round-1 forecast of one agent (non-abstaining only) or the pooled `p` of the card, scored."""

    __tablename__ = "scored_forecasts"
    __table_args__ = (
        CheckConstraint(_in("target_type", TARGET_TYPES), name="target_type"),
        CheckConstraint(_in("agent", (*AGENTS, POOLED)), name="agent"),
        CheckConstraint(_in("label", ("up", "down")), name="label"),
        CheckConstraint("y IN (0, 1)", name="y"),
        CheckConstraint("p > 0 AND p < 1", name="p"),
        CheckConstraint("log_loss >= 0", name="log_loss"),
        Index("ix_scored_forecasts_target_scored_at", "target_type", "scored_at"),
    )

    event_id: Mapped[str] = mapped_column(Text, primary_key=True)
    agent: Mapped[str] = mapped_column(Text, primary_key=True)
    target_type: Mapped[str] = mapped_column(Text)
    label_spec_version: Mapped[str] = mapped_column(Text)
    coin_id: Mapped[int] = mapped_column(Integer)
    as_of: Mapped[datetime]
    agent_version: Mapped[int | None] = mapped_column(Integer)
    p: Mapped[float] = mapped_column(Float)
    p_model: Mapped[float | None] = mapped_column(Float)
    label: Mapped[str] = mapped_column(Text)
    y: Mapped[int] = mapped_column(SmallInteger)
    hit: Mapped[bool] = mapped_column(Boolean)
    log_loss: Mapped[float] = mapped_column(Float)
    regime: Mapped[str | None] = mapped_column(Text)
    scored_at: Mapped[datetime]
    """as_of + horizon: when the outcome became known (deterministic, so rescoring is idempotent)."""


class WeightHistoryRow(Base):
    """w, a, r, coverage per agent after every as_of group of scored events (known at as_of + horizon)."""

    __tablename__ = "weight_history"
    __table_args__ = (
        PrimaryKeyConstraint("target_type", "label_spec_version", "agent", "as_of"),
        CheckConstraint(_in("target_type", TARGET_TYPES), name="target_type"),
        CheckConstraint(_in("agent", AGENTS), name="agent"),
        CheckConstraint("w >= 0 AND w <= 1 AND w_capped >= 0 AND w_capped <= 1", name="w"),
        CheckConstraint("a >= 0 AND a <= 1 AND r >= 0 AND r <= 1", name="trust"),
        CheckConstraint("coverage >= 0 AND coverage <= 1", name="coverage"),
        Index("ix_weight_history_target_as_of", "target_type", "as_of"),
    )

    target_type: Mapped[str] = mapped_column(Text)
    label_spec_version: Mapped[str] = mapped_column(Text)
    agent: Mapped[str] = mapped_column(Text)
    as_of: Mapped[datetime]
    agent_version: Mapped[int | None] = mapped_column(Integer)
    w: Mapped[float] = mapped_column(Float)
    w_capped: Mapped[float] = mapped_column(Float)
    a: Mapped[float] = mapped_column(Float)
    r: Mapped[float] = mapped_column(Float)
    coverage: Mapped[float] = mapped_column(Float)
    forecasts: Mapped[int] = mapped_column(Integer)
    """Opinionated scored forecasts of the current `agent_version` (all target types: the 50 threshold)."""
    events: Mapped[int] = mapped_column(Integer)


class CalibrationBinRow(Base):
    """Reliability chart data (raw probabilities vs observed rate) per agent and for the pooled `p`."""

    __tablename__ = "calibration_bins"
    __table_args__ = (
        PrimaryKeyConstraint("target_type", "label_spec_version", "agent", "computed_at", "bin_index"),
        CheckConstraint(_in("target_type", TARGET_TYPES), name="target_type"),
        CheckConstraint(_in("agent", (*AGENTS, POOLED)), name="agent"),
        CheckConstraint("n > 0", name="n"),
    )

    target_type: Mapped[str] = mapped_column(Text)
    label_spec_version: Mapped[str] = mapped_column(Text)
    agent: Mapped[str] = mapped_column(Text)
    computed_at: Mapped[datetime]
    bin_index: Mapped[int] = mapped_column(Integer)
    bin_lo: Mapped[float] = mapped_column(Float)
    bin_hi: Mapped[float] = mapped_column(Float)
    n: Mapped[int] = mapped_column(Integer)
    mean_p: Mapped[float] = mapped_column(Float)
    observed_rate: Mapped[float] = mapped_column(Float)


class CalibrationStatRow(Base):
    __tablename__ = "calibration_stats"
    __table_args__ = (
        PrimaryKeyConstraint("target_type", "label_spec_version", "agent", "computed_at"),
        CheckConstraint(_in("target_type", TARGET_TYPES), name="target_type"),
        CheckConstraint(_in("agent", (*AGENTS, POOLED)), name="agent"),
    )

    target_type: Mapped[str] = mapped_column(Text)
    label_spec_version: Mapped[str] = mapped_column(Text)
    agent: Mapped[str] = mapped_column(Text)
    computed_at: Mapped[datetime]
    spiegelhalter_z: Mapped[float | None] = mapped_column(Float)
    ece: Mapped[float] = mapped_column(Float)
    n: Mapped[int] = mapped_column(Integer)


class ScoringParamsRow(Base):
    """One served parameter snapshot (w tables, caps, a, r, b, calibration maps, stacker reference)."""

    __tablename__ = "scoring_params"
    __table_args__ = (
        PrimaryKeyConstraint("target_type", "label_spec_version", "params_version"),
        CheckConstraint(_in("target_type", TARGET_TYPES), name="target_type"),
        CheckConstraint("params_version >= 1", name="params_version"),
    )

    target_type: Mapped[str] = mapped_column(Text)
    label_spec_version: Mapped[str] = mapped_column(Text)
    params_version: Mapped[int] = mapped_column(Integer)
    as_of: Mapped[datetime]
    """Knowledge time of the newest scored event in the snapshot."""
    payload: Mapped[dict[str, Any]]
    payload_sha256: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime]


class StackingModelRow(Base):
    """A LightGBM stacker fit (target = triple-barrier label) with its purged walk-forward comparison."""

    __tablename__ = "stacking_models"
    __table_args__ = (
        PrimaryKeyConstraint("target_type", "label_spec_version", "trained_through"),
        CheckConstraint(_in("target_type", TARGET_TYPES), name="target_type"),
        CheckConstraint("NOT enabled OR model IS NOT NULL", name="enabled_has_model"),
    )

    target_type: Mapped[str] = mapped_column(Text)
    label_spec_version: Mapped[str] = mapped_column(Text)
    trained_through: Mapped[datetime]
    n: Mapped[int] = mapped_column(Integer)
    oos_n: Mapped[int] = mapped_column(Integer)
    oos_logloss_stack: Mapped[float] = mapped_column(Float)
    oos_logloss_pool: Mapped[float] = mapped_column(Float)
    enabled: Mapped[bool] = mapped_column(Boolean)
    features: Mapped[dict[str, Any]]
    model: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime]


class ClaimsAuditFlagRow(Base):
    """An agent whose rejected-claim rate exceeded the limit; its weight ceiling drops until reviewed."""

    __tablename__ = "claims_audit_flags"
    __table_args__ = (
        PrimaryKeyConstraint("agent", "raised_at"),
        CheckConstraint(_in("agent", AGENTS), name="agent"),
        CheckConstraint("rejected >= 0 AND claims >= rejected", name="counts"),
        CheckConstraint("(reviewed_at IS NULL) = (reviewed_by IS NULL)", name="review"),
        Index(
            "uq_claims_audit_flags_open", "agent", unique=True, postgresql_where=text("reviewed_at IS NULL")
        ),
    )

    agent: Mapped[str] = mapped_column(Text)
    raised_at: Mapped[datetime]
    window_start: Mapped[datetime]
    window_end: Mapped[datetime]
    claims: Mapped[int] = mapped_column(Integer)
    rejected: Mapped[int] = mapped_column(Integer)
    rate: Mapped[float] = mapped_column(Float)
    reviewed_at: Mapped[datetime | None]
    reviewed_by: Mapped[str | None] = mapped_column(Text)
    review_note: Mapped[str | None] = mapped_column(Text)


class GateProgressRow(Base):
    """Gate checks (G1 / G4) with their measured value and target; the G4 writer arrives in phase 11."""

    __tablename__ = "gate_progress"
    __table_args__ = (PrimaryKeyConstraint("gate", "check_key"),)

    gate: Mapped[str] = mapped_column(Text)
    check_key: Mapped[str] = mapped_column(Text)
    label: Mapped[str] = mapped_column(Text)
    value: Mapped[float | None] = mapped_column(Float)
    target: Mapped[float | None] = mapped_column(Float)
    met: Mapped[bool | None] = mapped_column(Boolean)
    updated_at: Mapped[datetime]


class LessonShadowForecastRow(Base):
    """Round-1 forecast an agent made with its shadow lesson (never pooled or shared), for the A/B."""

    __tablename__ = "lesson_shadow_forecasts"
    __table_args__ = (
        PrimaryKeyConstraint("event_id", "agent", "lesson_id"),
        CheckConstraint(_in("agent", AGENTS), name="agent"),
        Index("ix_lesson_shadow_forecasts_lesson_id", "lesson_id"),
    )

    event_id: Mapped[str] = mapped_column(Text)
    agent: Mapped[str] = mapped_column(Text)
    lesson_id: Mapped[str] = mapped_column(Text)
    forecast: Mapped[dict[str, Any]]
    recorded_at: Mapped[datetime] = mapped_column(server_default=text("CURRENT_TIMESTAMP"))


class ReflectionRunRow(Base):
    """One Reflection pass over a closed trade's event (at most one per event)."""

    __tablename__ = "reflection_runs"
    __table_args__ = (
        CheckConstraint(_in("status", REFLECTION_STATUSES), name="status"),
        CheckConstraint(f"agent IS NULL OR {_in('agent', AGENTS)}", name="agent"),
        CheckConstraint("(status = 'proposed') = (lesson_id IS NOT NULL)", name="lesson"),
    )

    event_id: Mapped[str] = mapped_column(Text, primary_key=True)
    agent: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text)
    lesson_id: Mapped[str | None] = mapped_column(Text)
    detail: Mapped[str | None] = mapped_column(Text)
    ran_at: Mapped[datetime]
