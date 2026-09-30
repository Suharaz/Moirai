"""`Claim` and `AgentForecast` (agents -> council verifier/share -> scoring).

LLM output is parsed ONLY with the draft models (`ClaimDraft`, `AgentForecastDraft`), which contain the
fields an LLM may write. Everything else (`p_model`, `p_used`, provenance, `verified`, `hard`, `tier`,
`domain_url`, claim ids) is assigned by code when building `Claim` / `AgentForecast`; any self-declared
value in LLM output is ignored.

`AgentForecast` is the per-agent part of the decision card, stored verbatim as JSON (one
`decision_forecasts.forecast` value per agent and round), so its fields need no DB column of their own.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, Self

from pydantic import ConfigDict, Field, NonNegativeInt, PositiveInt, model_validator

from hdt.contracts.candidate import CANDIDATE_ID_PATTERN
from hdt.contracts.common import (
    AgentName,
    ClaimKind,
    ContractModel,
    DirectionHint,
    TargetType,
    Tier,
    UtcDatetime,
)
from hdt.contracts.packet import SHA256_PATTERN
from hdt.core.ids import canonical_json

if TYPE_CHECKING:
    from hdt.lake.schemas import RawRecord

CODE_ASSIGNED_CLAIM_FIELDS = frozenset({"verified", "hard", "tier", "domain_url"})
Probability = float


class ClaimDraft(ContractModel):
    """A claim exactly as an LLM may write it (self-declared verification fields are dropped)."""

    model_config = ConfigDict(frozen=True, extra="ignore", allow_inf_nan=False)

    claim_id: str = Field(min_length=1, max_length=64, description="local id, cited via cited_claim_ids")
    kind: ClaimKind
    ref: str = Field(
        min_length=1, max_length=512, description="packet field, tool result id, or news item_id"
    )
    statement: str = Field(min_length=1, max_length=2000)
    value: float | str | None = None
    quote: str | None = Field(default=None, max_length=8000)
    direction_hint: DirectionHint = DirectionHint.NONE


class Claim(ContractModel):
    """A piece of evidence. `verified`, `hard`, `tier`, `domain_url` are assigned by code only."""

    claim_id: str = Field(min_length=1, max_length=64)
    kind: ClaimKind
    ref: str = Field(min_length=1, max_length=512)
    statement: str = Field(min_length=1, max_length=2000)
    value: float | str | None = None
    quote: str | None = Field(default=None, max_length=8000)
    direction_hint: DirectionHint = DirectionHint.NONE
    verified: bool = False
    hard: bool = False
    tier: Tier | None = None
    domain_url: str | None = None

    @classmethod
    def from_draft(cls, draft: ClaimDraft) -> Claim:
        """Code-side construction: unverified until the council verifier assigns the code fields."""
        return cls(**draft.model_dump())

    @classmethod
    def from_llm(cls, data: dict[str, Any]) -> Claim:
        return cls.from_draft(ClaimDraft.model_validate(data))


class AgentForecastDraft(ContractModel):
    """The structured output requested from the LLM (strict json_schema is generated from this model)."""

    model_config = ConfigDict(frozen=True, extra="ignore", allow_inf_nan=False)

    p_llm: float | None = Field(default=None, gt=0, lt=1)
    abstain: bool
    abstain_reason: str | None = Field(default=None, max_length=500)
    candidate_id: str | None = Field(default=None, pattern=CANDIDATE_ID_PATTERN)
    claims: tuple[ClaimDraft, ...] = Field(default=(), max_length=12)
    cited_claim_ids: tuple[str, ...] = Field(default=(), max_length=24)
    reason: str = Field(default="", max_length=2000, description="stored in the decision card, never shared")

    @model_validator(mode="after")
    def _check(self) -> Self:
        if not self.abstain and self.p_llm is None:
            raise ValueError("a non-abstaining forecast needs p_llm")
        return self


class LlmUsage(ContractModel):
    prompt_tokens: NonNegativeInt
    completion_tokens: NonNegativeInt
    cost_usd: float | None = Field(default=None, ge=0)


class ToolCallRecord(ContractModel):
    tool: str
    input_sha256: str = Field(pattern=SHA256_PATTERN)
    output_sha256: str | None = Field(default=None, pattern=SHA256_PATTERN)
    status: Literal["ok", "not_available", "error", "budget_exceeded"]
    latency_ms: NonNegativeInt


class LakeRef(ContractModel):
    """Provenance of one lake (raw store) record: tool results cite it, capture manifests pin it."""

    source: str
    route: str
    key: str
    fetched_at: UtcDatetime
    http_status: int
    body_sha256: str = Field(pattern=SHA256_PATTERN)

    @classmethod
    def of(cls, record: RawRecord) -> LakeRef:
        return cls(
            source=record.source,
            route=record.route,
            key=record.key,
            fetched_at=record.fetched_at,
            http_status=record.http_status,
            body_sha256=record.body_sha256,
        )


class AgentForecast(ContractModel):
    """One agent's forecast for one round of one council event (horizon 12 h), assembled by code.

    `tool_calls` and `capture_manifest` come from the agent's `ToolSession` (`records` and `captures`).
    `capture_manifest` is the immutable list of lake records the live meeting itself created after `as_of`
    (`fetch_source` copies). Replay builds `ToolContext.pinned` only from this stored field
    (`hdt.tools.replay.replay_context`), so a capture missing here is `not_available` on replay. The
    council (phase 06) MUST copy `ToolSession.captures` into this field before the forecast is committed
    (round commit hash and decision card); it is part of `commit_bytes`.
    """

    schema_version: Literal[1] = 1
    agent: AgentName
    agent_version: str = Field(min_length=1)
    event_id: str = Field(min_length=1)
    round: int = Field(ge=1, le=3)
    coin_id: PositiveInt
    as_of: UtcDatetime
    target_type: TargetType
    label_spec_version: str = Field(min_length=1)
    p_model: Probability | None = Field(default=None, gt=0, lt=1)
    p_llm: Probability | None = Field(default=None, gt=0, lt=1)
    p_used: Probability | None = Field(default=None, gt=0, lt=1)
    abstain: bool
    abstain_reason: str | None = None
    candidate_id: str | None = Field(default=None, pattern=CANDIDATE_ID_PATTERN)
    claims: tuple[Claim, ...] = ()
    cited_claim_ids: tuple[str, ...] = ()
    reason: str = Field(default="", max_length=2000)
    packet_sha256: str | None = Field(default=None, pattern=SHA256_PATTERN)
    prompt_hash: str | None = Field(default=None, pattern=SHA256_PATTERN)
    model_slug: str | None = Field(default=None, description="model requested from OpenRouter")
    model_returned: str | None = Field(default=None, description="model reported by OpenRouter")
    provider: str | None = Field(default=None, description="provider that actually served the call")
    generation_id: str | None = None
    usage: LlmUsage | None = None
    skill_commit: str | None = None
    tool_calls: tuple[ToolCallRecord, ...] = ()
    capture_manifest: tuple[LakeRef, ...] = Field(
        default=(), description="lake records this agent's live meeting created after as_of, in call order"
    )

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.abstain:
            if not self.abstain_reason:
                raise ValueError("an abstaining forecast needs abstain_reason")
            if self.p_used is not None or self.candidate_id is not None:
                raise ValueError("an abstaining forecast carries no p_used or candidate_id")
        else:
            if self.abstain_reason is not None:
                raise ValueError("abstain_reason is only allowed when abstaining")
            if self.p_model is None or self.p_llm is None or self.p_used is None:
                raise ValueError("a non-abstaining forecast needs p_model, p_llm and p_used")
        known = {c.claim_id for c in self.claims}
        if len(known) != len(self.claims):
            raise ValueError("claim_id must be unique within a forecast")
        if len(set(self.capture_manifest)) != len(self.capture_manifest):
            raise ValueError("capture_manifest entries must be unique")
        if any(ref.fetched_at <= self.as_of for ref in self.capture_manifest):
            raise ValueError("capture_manifest lists only records fetched after as_of")
        return self

    def commit_bytes(self) -> bytes:
        """Canonical bytes for the round commit hash: excludes run-time telemetry (tool latency) so the
        same forecast replayed from cache commits to the same hash."""
        body = self.model_dump(mode="python")
        body["tool_calls"] = [
            {k: v for k, v in call.items() if k != "latency_ms"} for call in body["tool_calls"]
        ]
        return canonical_json(body)
