"""`DecisionMsg` (council outbox -> stream `decisions` -> risk).

The message never carries prices, sizes or leverage: Risk takes every price from the packet stored by
`packet_sha256` and the candidate chosen by `candidate_id`. Unknown fields are ignored. Semantic checks
(`p_side` sign, candidate side) are Risk verdicts, not parse errors.

A decision has no time limit (owner decision 2026-09-28: the council waits for the debate result). The v1
field `expires_at` stays in the schema for compatibility: optional, always null from the council, and never
read by Risk. An OPEN is instead refused by Risk when the current mark has already invalidated its levels.
"""

from __future__ import annotations

from typing import Literal, Self

from pydantic import Field, PositiveInt, model_validator

from hdt.contracts.candidate import CANDIDATE_ID_PATTERN
from hdt.contracts.common import InboundContractModel, Intent, Side, TargetType, UtcDatetime
from hdt.contracts.packet import SHA256_PATTERN


class DecisionMsg(InboundContractModel):
    schema_version: Literal[1] = 1
    event_id: str = Field(min_length=1)
    coin_id: PositiveInt
    as_of: UtcDatetime
    intent: Intent
    side: Side = Field(description="direction for OPEN; the held side for HOLD/EXIT")
    p: float = Field(gt=0, lt=1)
    p_side: float = Field(gt=0, lt=1)
    manager_size: float = Field(ge=0, le=1)
    candidate_id: str | None = Field(default=None, pattern=CANDIDATE_ID_PATTERN)
    packet_sha256: str | None = Field(default=None, pattern=SHA256_PATTERN)
    config_version_ids: dict[str, int]
    target_type: TargetType
    label_spec_version: str = Field(min_length=1)
    expires_at: UtcDatetime | None = Field(default=None, description="v1 compatibility only; unused")

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.intent is Intent.OPEN:
            if not self.candidate_id or not self.packet_sha256:
                raise ValueError("OPEN needs candidate_id and packet_sha256")
            if self.manager_size <= 0:
                raise ValueError("OPEN needs manager_size > 0")
        elif self.candidate_id:
            raise ValueError("HOLD/EXIT carry no candidate_id")
        if self.expires_at is not None and self.expires_at <= self.as_of:
            raise ValueError("expires_at must be after as_of")
        return self
