"""`QuantPacket`: every number an agent may use, stored in Postgres by `packet_sha256`.

The packet is deeply immutable (mappings are read-only views) and `model_copy` re-validates, so the hash
can never go stale inside a process. Consumers that load a packet from storage always parse it with
`QuantPacket.model_validate`, which recomputes and checks the hash.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from types import MappingProxyType
from typing import Any, Literal, Self

from pydantic import Field, PositiveInt, field_serializer, field_validator, model_validator

from hdt.contracts.common import AgentName, ContractModel, DataQualityFlag, TargetType, UtcDatetime
from hdt.core.ids import canonical_sha256

FeatureValue = float | int | str | bool | None
SHA256_PATTERN = r"^[0-9a-f]{64}$"


class QuantPacketBody(ContractModel):
    """All packet fields covered by `packet_sha256`."""

    schema_version: Literal[1] = 1
    agent: AgentName
    coin_id: PositiveInt
    as_of: UtcDatetime
    features: Mapping[str, FeatureValue]
    p_model: float = Field(gt=0, lt=1)
    candidate_set_sha256: str = Field(pattern=SHA256_PATTERN)
    data_quality: Mapping[DataQualityFlag, tuple[str, ...]] = Field(
        description="flag -> affected feature names (empty tuple = whole packet)"
    )
    universe_date: date
    config_version_ids: Mapping[str, int]
    feature_ver: str = Field(min_length=1)
    p_model_ver: str = Field(min_length=1)
    target_type: TargetType
    label_spec_version: str = Field(min_length=1)

    @field_validator("features", "data_quality", "config_version_ids", mode="after")
    @classmethod
    def _freeze(cls, value: Mapping[Any, Any]) -> Mapping[Any, Any]:
        return MappingProxyType(dict(value))

    @field_serializer("features", "data_quality", "config_version_ids")
    def _thaw(self, value: Mapping[Any, Any]) -> dict[Any, Any]:
        return dict(value)


class QuantPacket(QuantPacketBody):
    """Computed by quant_core for exactly one agent; parsing fails if the hash does not match."""

    packet_sha256: str = Field(pattern=SHA256_PATTERN)

    @classmethod
    def build(cls, **fields: Any) -> QuantPacket:
        body = QuantPacketBody.model_validate(fields)
        return cls(**dict(body), packet_sha256=canonical_sha256(body))

    def body(self) -> QuantPacketBody:
        return QuantPacketBody(**{k: v for k, v in dict(self).items() if k != "packet_sha256"})

    @model_validator(mode="after")
    def _check_hash(self) -> Self:
        if canonical_sha256(self.body()) != self.packet_sha256:
            raise ValueError("packet_sha256 does not match the packet content")
        return self

    def model_copy(self, *, update: Mapping[str, Any] | None = None, deep: bool = False) -> Self:
        """Re-validate on copy: changing content without a matching hash raises."""
        return type(self).model_validate({**dict(self), **dict(update or {})})

    def flags_touching(self, feature_names: set[str]) -> set[DataQualityFlag]:
        """Flags whose affected fields intersect `feature_names` (packet-wide flags always count)."""
        return {
            flag
            for flag, fields in self.data_quality.items()
            if not fields or feature_names.intersection(fields)
        }
