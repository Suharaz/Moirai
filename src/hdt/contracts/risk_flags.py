"""`RiskFlags` (independent news veto scan -> stream `risk_flags` -> risk, execution)."""

from __future__ import annotations

from typing import Literal, Self

from pydantic import Field, PositiveInt, model_validator

from hdt.contracts.common import InboundContractModel, UtcDatetime


class RiskFlags(InboundContractModel):
    """Latest veto state for a coin. Past `expires_at` it counts as absent (fail closed in Risk)."""

    schema_version: Literal[1] = 1
    coin_id: PositiveInt
    as_of: UtcDatetime
    veto_long: bool
    veto_short: bool
    size_mult: float = Field(ge=0, le=1)
    evidence_ref: str | None = None
    expires_at: UtcDatetime
    scan_fresh_at: UtcDatetime

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.expires_at <= self.as_of:
            raise ValueError("expires_at must be after as_of")
        return self
