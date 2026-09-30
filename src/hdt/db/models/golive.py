"""Phase 11 go-live tables: the calibration / final evaluation split of the shadow run and the one-shot final
G4 evaluation.

`golive_eval_splits` holds, per `target_type` and `generation`, the instant from which events belong to the
final evaluation set (earlier events are the calibration set). It is written by
`python -m hdt.golive.split set` (role `hdt_scorer`: SELECT and a column-level INSERT only, never UPDATE or
DELETE): `created_at` is the server clock at the insert (`clock_timestamp()`) and `eval_start >= created_at`
is a CHECK, so a split is always chosen before any evaluation data exists. A database trigger assigns the
next generation and refuses it while the previous split has no final evaluation: a split is never moved,
and a new generation (after a recorded final) is the only way to evaluate a changed configuration.

`golive_g4_finals` holds the final G4 evaluation of each split (`python -m hdt.golive.final run`), one row per
(target type, generation), inserted with its `gate_flags` row in one transaction; a second final evaluation
of the same split fails on the primary key, and the composite foreign key binds the row to that exact split.
`pins` records what the window ran with (`hdt.golive.pins`): the config-api live gate reads the newest final
evaluation and refuses `live` once today's configuration differs from those pins. A trigger on `gate_flags`
accepts a `G4` flag only when it repeats the newest final's outcome. Weekly runs never write here or to
`gate_flags`. The console reads both.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    FetchedValue,
    ForeignKeyConstraint,
    Integer,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from hdt.db.base import Base

TARGET_TYPES = ("RAW_12H", "RESID_12H")
_TARGETS = f"target_type IN ({', '.join(repr(t) for t in TARGET_TYPES)})"


class GoLiveEvalSplitRow(Base):
    __tablename__ = "golive_eval_splits"
    __table_args__ = (
        CheckConstraint(_TARGETS, name="target_type"),
        CheckConstraint("generation >= 1", name="generation"),
        CheckConstraint("eval_start >= created_at", name="eval_start_not_past"),
        UniqueConstraint("target_type", "generation", "eval_start"),
    )

    target_type: Mapped[str] = mapped_column(Text, primary_key=True)
    generation: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=False, server_default=FetchedValue()
    )
    """1 for the first split of a target type; assigned by the database trigger `golive_split_generation`."""
    eval_start: Mapped[datetime]
    """Events with `as_of >= eval_start` form the final evaluation set."""
    decided_by: Mapped[str] = mapped_column(Text)
    reason: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(server_default=text("clock_timestamp()"))


class GoLiveG4FinalRow(Base):
    __tablename__ = "golive_g4_finals"
    __table_args__ = (
        CheckConstraint(_TARGETS, name="target_type"),
        CheckConstraint("eval_end > eval_start", name="window"),
        CheckConstraint("passed OR NOT gate_passed", name="gate_needs_target"),
        ForeignKeyConstraint(
            ["target_type", "generation", "eval_start"],
            [
                "golive_eval_splits.target_type",
                "golive_eval_splits.generation",
                "golive_eval_splits.eval_start",
            ],
        ),
    )

    target_type: Mapped[str] = mapped_column(Text, primary_key=True)
    generation: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    eval_start: Mapped[datetime]
    eval_end: Mapped[datetime]
    """Exclusive: the UTC midnight after the day the evaluation set reached its entry count (sealed)."""
    passed: Mapped[bool] = mapped_column(Boolean)
    """This target type passed every deciding check."""
    gate_passed: Mapped[bool] = mapped_column(Boolean)
    """The whole gate passed in this final evaluation (every required target type and the account)."""
    evidence_ref: Mapped[str] = mapped_column(Text)
    result: Mapped[dict[str, Any]]
    pins: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True))
    """`{"models": id, "council": id, "agents": {agent: {model_slug, agent_version}}, "static": {file: sha}}`
    of the window (an id is None when no console version was active); NULL when the window was not pinned
    (the gate did not pass)."""
    evaluated_at: Mapped[datetime] = mapped_column(server_default=text("CURRENT_TIMESTAMP"))
