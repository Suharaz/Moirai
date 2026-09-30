"""`Candidate` (scanner/news -> council trigger), `LevelCandidate` and the shared candidate set."""

from __future__ import annotations

from decimal import Decimal
from typing import Literal, Self

from pydantic import Field, PositiveInt, model_validator

from hdt.contracts.common import (
    CandidateSource,
    ContractModel,
    Side,
    TargetType,
    UtcDatetime,
)
from hdt.core.ids import canonical_sha256
from hdt.settings.ceilings import MIN_RR

CANDIDATE_ID_PATTERN = r"^lc_[A-Z2-7]{16}$"


class Candidate(ContractModel):
    """A coin worth convening the council on. Natural key: (coin_id, as_of, source)."""

    schema_version: Literal[1] = 1
    coin_id: PositiveInt
    as_of: UtcDatetime
    source: CandidateSource
    score: float
    rule_version: str = Field(min_length=1)
    target_type: TargetType
    label_spec_version: str = Field(min_length=1)

    @property
    def natural_key(self) -> tuple[int, str, str]:
        return (self.coin_id, self.as_of.isoformat(), self.source.value)


def _aligned(value: Decimal, tick: Decimal) -> bool:
    return (value / tick) == (value / tick).to_integral_value()


class LevelCandidate(ContractModel):
    """One price-level option: entry, invalidation (stop), first take-profit, all tick-aligned."""

    candidate_id: str = Field(pattern=CANDIDATE_ID_PATTERN)
    side: Side
    entry: Decimal = Field(gt=0)
    invalidation: Decimal = Field(gt=0)
    tp1: Decimal = Field(gt=0)
    rr: float = Field(ge=MIN_RR, description="reward:risk to tp1; the design floor is 1.5")
    tick: Decimal = Field(gt=0)

    @model_validator(mode="after")
    def _geometry(self) -> Self:
        if self.side is Side.LONG and not (self.invalidation < self.entry < self.tp1):
            raise ValueError("LONG candidate needs invalidation < entry < tp1")
        if self.side is Side.SHORT and not (self.tp1 < self.entry < self.invalidation):
            raise ValueError("SHORT candidate needs tp1 < entry < invalidation")
        for name in ("entry", "invalidation", "tp1"):
            if not _aligned(getattr(self, name), self.tick):
                raise ValueError(f"{name} is not aligned to tick {self.tick}")
        reward = abs(self.tp1 - self.entry)
        risk = abs(self.entry - self.invalidation)
        expected = float(reward / risk)
        if abs(expected - self.rr) > 1e-6 * max(1.0, expected):
            raise ValueError(f"rr {self.rr} does not match levels ({expected})")
        return self

    @property
    def stop_distance(self) -> Decimal:
        return abs(self.entry - self.invalidation)


class CandidateSet(ContractModel):
    """The single candidate set shared by every agent for one (coin, as_of)."""

    coin_id: PositiveInt
    as_of: UtcDatetime
    levels_ver: str = Field(min_length=1)
    candidates: tuple[LevelCandidate, ...]

    @model_validator(mode="after")
    def _unique_sorted(self) -> Self:
        ids = [c.candidate_id for c in self.candidates]
        if len(ids) != len(set(ids)):
            raise ValueError("candidate_id must be unique within a set")
        if ids != sorted(ids):
            raise ValueError("candidates must be sorted by candidate_id")
        return self

    @property
    def candidate_set_sha256(self) -> str:
        return canonical_sha256(self)

    def get(self, candidate_id: str) -> LevelCandidate | None:
        for candidate in self.candidates:
            if candidate.candidate_id == candidate_id:
                return candidate
        return None
