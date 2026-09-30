"""Interfaces the council consumes; other phases implement them and `hdt.council.main` wires them.

- `AgentRunner` (phase 05): one agent, one round of one event. Round 1 when `request.round == 1`,
  a revision otherwise. The runner never raises for LLM or tool failures (it returns an abstaining
  forecast with a reason); in replay mode an LLM cache miss is a hard error that it raises (never
  caught here), so a replayed meeting either reproduces every call from the cache or fails. The runner
  computes `p_used` in `validate`: round 1
  `z_used = z_model + a_i * clip(z_llm - z_model, +-llm_logit_margin)`, revisions
  `z_used = z_model + a_i * clip(z_llm - z_model, +-logit_bound_total)`; the council recomputes both and
  enforces the per-round bound, new citations and the flip rule itself.
- `PacketStore` (phase 03 `quant_packets`): the committed packet by `packet_sha256`, hash re-verified.
- `CandidateSetSource` (phase 03 `QuantCore.candidate_set`): the shared candidate set of (coin, as_of).
- `SourceLookup` (phase 07): the stored copy of a news item for `url` claims, with the code-computed
  domain, tier, registered event class and `check_official` verdict.
- `ParamsSource` / `PoolParams` (phase 08 `hdt.scoring.params`): learned w, a, r, b, calibration.
- `ShadowLessons` (phase 04 `hdt.memory.lessons.LessonLog`): the agent's shadow lesson at `as_of`.
- `ShadowForecastSink` (phase 08 `lesson_shadow_forecasts`): round-1 forecasts made with a shadow lesson.
- `ScannerLog` (phase 03 `scanner_log`): whether a candidate belongs only to the shadow superset.
- `HeldPositions` (phase 09 `AccountState`): the held side of a coin in the running account namespace.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Protocol

import sqlalchemy as sa
from pydantic import Field

from hdt.contracts.candidate import Candidate, CandidateSet
from hdt.contracts.common import AgentName, CandidateSource, ContractModel, Side, TargetType, Tier
from hdt.contracts.forecast import AgentForecast, Claim, LakeRef
from hdt.contracts.packet import SHA256_PATTERN, QuantPacket
from hdt.memory.lessons import Lesson
from hdt.tools.base import ToolMode, ToolResult


@dataclass(frozen=True)
class AgentRequest:
    """Everything one agent receives for one round; built by the council, never by the LLM."""

    event_id: str
    agent: AgentName
    round: int
    coin_id: int
    as_of: datetime
    source: CandidateSource
    target_type: TargetType
    label_spec_version: str
    config_version_ids: Mapping[str, int]
    universe_date: date
    candidate_set: CandidateSet | None
    """None for a held-coin re-evaluation (HOLD / EXIT only): the forecast then carries no candidate_id."""
    a_i: float
    """LLM trust of this agent (phase 08); used by `validate`, never shown to the LLM."""
    mode: ToolMode
    previous: AgentForecast | None = None
    """Round >= 2: this agent's effective forecast of the previous round."""
    shared_claims: tuple[Claim, ...] = ()
    """Round >= 2: anonymized verified claims; `claim_id` is the shared id to cite. The News agent gets only
    the news-safe subset (`hdt.council.claims.shown_to`)."""
    pinned: tuple[LakeRef, ...] = ()
    """Replay: the union of this agent's recorded `capture_manifest` of rounds 1..`round` (becomes
    `ToolContext.pinned` for this round); live: empty."""
    shadow_lesson: Lesson | None = None
    """Round 1 only: the phase 08 A/B run with this shadow lesson; its output is never pooled or shared."""
    earlier: tuple[AgentResult, ...] = ()
    """Round >= 2: this agent's own submitted results of rounds 1..`round - 1` of this event, in round order,
    rebuilt by the council from its checkpoint (`forecast` with its `tool_calls` and `capture_manifest`, the
    `ok` tool results it was returned, `captures` = its `capture_manifest`). A runner in a new process
    rebuilds the meeting's tool session from it (budget used, citable result ids, captures), so a resumed
    meeting sees exactly what an uninterrupted one would."""


@dataclass(frozen=True)
class AgentResult:
    forecast: AgentForecast
    captures: tuple[LakeRef, ...] = ()
    """Live: the lake records this agent's tool session created after `as_of` (fetch_source copies)."""
    tool_results: tuple[ToolResult, ...] = ()
    """The `ok` tool results returned to the agent in this call; `tool` claims cite their `result_id`."""
    tool_log: tuple[ToolResult, ...] = ()
    """The agent session's full ordered non-packet tool results of all rounds so far, every status (what the
    prompt shows under "Tool results so far"); a resumed round rebuilds its session from the latest one."""


class AgentRunner(Protocol):
    async def forecast(self, request: AgentRequest) -> AgentResult: ...


class PacketStore(Protocol):
    def packet(self, packet_sha256: str) -> QuantPacket | None:
        """The stored packet (parsed with `QuantPacket.model_validate`, which re-checks the hash)."""
        ...


@dataclass(frozen=True)
class EventPins:
    """What the council pins for one event (the phase 03 `QuantPins` fields)."""

    config_version_ids: Mapping[str, int]
    universe_date: date
    target_type: TargetType
    label_spec_version: str


class CandidateSetSource(Protocol):
    def candidate_set(self, coin_id: int, as_of: datetime, pins: EventPins) -> CandidateSet:
        """The shared candidate set of (coin, as_of), stored before it is returned."""
        ...


class StoredSource(ContractModel):
    """The stored copy of a news item as the verifier sees it; every field is computed by code."""

    item_id: str = Field(min_length=1, max_length=64)
    text: str = Field(description="text of the stored copy (the article as fetched, or the recorded item)")
    final_url: str
    domain: str = Field(min_length=1, description="registrable domain after redirects")
    tier: Tier = Field(description="from news_sources.yaml + check_official, never from an LLM")
    event_class: str | None = Field(
        default=None, description="registered event class (listing, delist, exploit, unlock) when classified"
    )
    official: bool = Field(description="check_official confirmed the event on the official source")
    event_coin_ids: tuple[int, ...] = Field(
        default=(),
        description="coins the item's verdict names as the subject of its event (empty: none named); a url "
        "claim confirms an on-chain event only for one of these coins",
    )
    body_sha256: str = Field(pattern=SHA256_PATTERN)


class SourceLookup(Protocol):
    def stored_source(
        self, item_id: str, *, as_of: datetime, pinned: tuple[LakeRef, ...], coin_id: int | None = None
    ) -> StoredSource | None:
        """The item's stored copy known at `as_of` (or pinned by this event's live meeting); with `coin_id`,
        `official` holds only when `check_official` confirmed the event for that coin."""
        ...


class Stacker(Protocol):
    def predict(self, p_by_agent: Mapping[str, float], regime: str | None) -> float: ...


class PoolParams(Protocol):
    """Learned pooling parameters for one (target_type, label_spec_version)."""

    @property
    def params_version(self) -> int: ...

    @property
    def intercept_b(self) -> float: ...

    @property
    def stacker(self) -> Stacker | None: ...

    @property
    def stacker_trained_through(self) -> datetime | None:
        """Identity of the stacking model in use (None: no stacker); recorded on the decision card."""
        ...

    @property
    def pins(self) -> dict[str, Any]:
        """JSON-safe identity of what this view resolved beyond `params_version` (stacking model,
        agent-version overlays, claims-audit flags); `ParamsSource.load(pins=...)` reproduces it exactly."""
        ...

    def a(self, agent: str) -> float: ...

    def r(self, agent: str) -> float: ...

    def calibrate(self, agent: str, p: float) -> float: ...

    def weights(self, regime: str | None, present: Iterable[str]) -> dict[str, float]:
        """Normalized weights over the `present` (non-abstaining) agents, every ceiling applied."""
        ...


class ParamsSource(Protocol):
    def load(
        self,
        target_type: TargetType,
        label_spec_version: str,
        *,
        at: datetime | None = None,
        params_version: int | None = None,
        pins: Mapping[str, Any] | None = None,
    ) -> PoolParams:
        """The parameters known at `at` (None: now): the newest `scoring_params` created at or before `at`,
        or exactly `params_version` when given (LookupError when it does not exist), with the stacking
        model and overlays resolved at `at`, or taken verbatim from `pins` (a view's `pins`) when given.
        The council pins one version and its `pins` per event (`at` = the event's `as_of`) for live,
        resume and replay."""
        ...

    def regime(self, packets: Mapping[str, QuantPacket]) -> str | None:
        """The regime cell of the event from its committed packets (None when unknown)."""
        ...


class ShadowLessons(Protocol):
    def shadow(self, agent: AgentName, as_of: datetime) -> Lesson | None: ...


class ShadowForecastSink(Protocol):
    def record(
        self, conn: sa.Connection, *, event_id: str, agent: AgentName, lesson_id: str, forecast: AgentForecast
    ) -> None:
        """Insert inside the emit_decision transaction (idempotent on (event_id, agent, lesson_id))."""
        ...


class ScannerLog(Protocol):
    def shadow_only(self, candidate: Candidate) -> bool:
        """True when the candidate passed only the loose (superset) thresholds."""
        ...


class HeldPositions(Protocol):
    async def held_side(self, coin_id: int, as_of: datetime) -> Side | None: ...


@dataclass(frozen=True)
class EventContext:
    """Everything `load_context` pins for one event (recorded on the decision card)."""

    event_id: str
    candidate: Candidate
    symbol: str
    universe_date: date
    config_version_ids: Mapping[str, int]
    candidate_set: CandidateSet | None
    held_side: Side | None
    shadow_only: bool
    unscored: bool
    extra: Mapping[str, str] = field(default_factory=dict)
