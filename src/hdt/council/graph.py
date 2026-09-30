"""The council meeting as a LangGraph graph (Design Contract section 3, phase 06 Red Team Delta).

```text
load_context -> Send(agent x6) -> collect -> commit_round1 -> check_consensus
  check_consensus -> [agreed | last round | debate disabled] -> aggregate
                  -> [else] share_claims -> [no new verified claims] -> aggregate
                                         -> [else] Send(agent, round + 1) -> collect -> apply_revisions
                                            -> check_consensus
aggregate -> manager -> emit_decision
```

Every node is deterministic given its inputs (runner outputs, stores, pinned config) and every value in
the graph state is plain JSON, so the PostgresSaver checkpoint (`thread_id = event_id`,
`durability="sync"`) resumes a meeting where it stopped: the finished agent calls of an interrupted round
are not repeated. All math lives in the pure modules (`consensus`, `aggregate`, `manager`, `verifier`,
`revision`, `commit`); this module moves data between them and the ports.

Side effects: `commit_round1` writes the blind round-1 commit before any claim is shared, and
`emit_decision` writes the whole decision in one transaction. Both are idempotent (a resumed node that
already wrote re-checks the stored hash instead of writing twice).
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import date, datetime
from decimal import Decimal
from typing import Annotated, Any, Final, Protocol, TypedDict, cast

from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Send

from hdt.contracts.candidate import Candidate, CandidateSet
from hdt.contracts.common import AgentName, CandidateSource, Intent, Side
from hdt.contracts.decision import DecisionMsg
from hdt.contracts.forecast import AgentForecast, Claim, LakeRef
from hdt.contracts.packet import QuantPacket
from hdt.contracts.timeline import DecisionTimelineEntry, TimelineStage, TimelineTone
from hdt.core.clock import utcnow
from hdt.core.config import CouncilFile
from hdt.core.ids import canonical_sha256, to_canonical
from hdt.council import hard_evidence
from hdt.council.aggregate import aggregate as pool
from hdt.council.claims import SharedClaim, share, shared_claim_id, shown_to, visible
from hdt.council.commit import CommitMismatchError, RoundCommit, forecast_sha256, verify_commit
from hdt.council.consensus import Check, ConsensusResult, consensus, stance_of
from hdt.council.manager import Outcome, manage, position_intent, select_candidate
from hdt.council.ports import (
    AgentRequest,
    AgentResult,
    AgentRunner,
    CandidateSetSource,
    EventPins,
    HeldPositions,
    PacketStore,
    ParamsSource,
    PoolParams,
    ShadowLessons,
    SourceLookup,
)
from hdt.council.revision import enforce_round1, revise, side_of
from hdt.council.verifier import AgentEvidence, VerifiedClaim, verify_round
from hdt.memory.lessons import Lesson
from hdt.tools.base import ToolMode, ToolResult, result_id

log = logging.getLogger(__name__)

MISMATCH_REASON: Final[str] = "council: forecast does not match the request"
SUMMARY_MAX: Final[int] = 4000
CHECKS_MAX: Final[int] = 20
DEBATE_OVER: Final[frozenset[str]] = frozenset({"max_rounds", "no_new_claims", "debate_disabled"})


# ---------------------------------------------------------------------------------------------- ports


@dataclass(frozen=True)
class EventSettings:
    """Settings pinned by an event's config versions."""

    council: CouncilFile


@dataclass(frozen=True)
class UniverseEntry:
    symbol: str
    universe_date: date


class UniverseLookup(Protocol):
    def entry(self, coin_id: int, as_of: datetime) -> UniverseEntry | None:
        """The coin's Binance symbol and the point-in-time universe date at `as_of` (None: not tradable)."""
        ...


@dataclass(frozen=True)
class DecisionRecord:
    """Everything `emit_decision` writes in one transaction."""

    card: dict[str, Any]
    """`decision_cards` row (JSON values; `as_of` / `universe_date` as ISO strings)."""
    forecasts: list[dict[str, Any]]
    claims: list[dict[str, Any]]
    decision: DecisionMsg | None
    """What is sent to Risk (None: nothing)."""
    shadow: list[tuple[AgentName, str, AgentForecast]]
    """(agent, lesson_id, round-1 forecast made with the shadow lesson): never pooled or shared."""
    final: dict[str, AgentForecast]
    """Each agent's effective forecast of the last round (episodic memory)."""

    @property
    def event_id(self) -> str:
        return str(self.card["event_id"])


class DecisionStore(Protocol):
    """Durable side effects of a meeting."""

    def commit_round(self, commit: RoundCommit, at: datetime) -> None:
        """Write the blind commit (idempotent); raise `CommitMismatchError` when a different one is stored."""
        ...

    def emit(self, record: DecisionRecord) -> bool:
        """Write the whole decision in one transaction; False when this event already has a card."""
        ...


@dataclass(frozen=True)
class CouncilServices:
    runner: AgentRunner
    packets: PacketStore
    candidate_sets: CandidateSetSource
    sources: SourceLookup
    params: ParamsSource
    held: HeldPositions
    universe: UniverseLookup
    store: DecisionStore
    settings: Callable[[Mapping[str, int]], EventSettings]
    mode: ToolMode = "live"
    lessons: ShadowLessons | None = None
    pinned: Callable[[str, AgentName, int], tuple[LakeRef, ...]] = field(default=lambda _e, _a, _r: ())
    """Replay: the captures recorded for (event_id, agent) in rounds 1..round, in round order."""
    bind_pins: Callable[[int, datetime, EventPins], None] | None = None
    """Called before an agent runs, so the live `quant_core` tool computes packets with the event's pins."""


# ---------------------------------------------------------------------------------------------- state


def _merge(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    return {**left, **right}


class CouncilState(TypedDict, total=False):
    event: dict[str, Any]
    """Input: event_id, candidate, config_version_ids, unscored, shadow_only; completed by load_context."""
    round: int
    results: Annotated[dict[str, Any], _merge]
    """`"{round}:{agent}"` / `"shadow:{agent}"` -> {forecast, tool_results[, lesson_id]} from the tasks."""
    sent: list[str]
    effective: dict[str, Any]
    """agent -> effective forecast of the current round."""
    history: dict[str, Any]
    """`"{round}:{agent}"` -> {forecast (effective), revision}."""
    commit: dict[str, Any]
    weights: dict[str, float]
    regime: str | None
    claims: list[dict[str, Any]]
    shared: list[dict[str, Any]]
    consensus: dict[str, Any]
    round1_consensus: bool
    stop_reason: str
    timeline: list[dict[str, Any]]
    pool: dict[str, Any] | None
    decision: dict[str, Any]


# ---------------------------------------------------------------------------------------------- helpers


def _json(value: Any) -> Any:
    return to_canonical(value)


def _forecast(data: Mapping[str, Any]) -> AgentForecast:
    return AgentForecast.model_validate(data)


def _entry(stage: TimelineStage, text: str, tone: TimelineTone = "") -> dict[str, Any]:
    entry = DecisionTimelineEntry(at=utcnow(), stage=stage, text=text[:4000] or "-", tone=tone)
    return dict(_json(entry))


def agent_names(council: CouncilFile) -> tuple[AgentName, ...]:
    return tuple(AgentName(a) for a in council.agents)


def stance_label(forecast: AgentForecast, council: CouncilFile) -> str:
    if forecast.abstain or forecast.p_used is None:
        return "ABSTAIN"
    return stance_of(
        forecast.p_used, stance_long=council.stance_long, stance_short=council.stance_short
    ).value


def _abstaining(request: AgentRequest, like: AgentForecast, reason: str) -> AgentForecast:
    return AgentForecast(
        agent=request.agent,
        agent_version=like.agent_version,
        event_id=request.event_id,
        round=request.round,
        coin_id=request.coin_id,
        as_of=request.as_of,
        target_type=request.target_type,
        label_spec_version=request.label_spec_version,
        abstain=True,
        abstain_reason=reason,
    )


def _matches(forecast: AgentForecast, request: AgentRequest) -> bool:
    return (
        forecast.agent is request.agent
        and forecast.event_id == request.event_id
        and forecast.round == request.round
        and forecast.coin_id == request.coin_id
        and forecast.as_of == request.as_of
        and forecast.target_type is request.target_type
        and forecast.label_spec_version == request.label_spec_version
    )


def _result_json(result: AgentResult, request: AgentRequest) -> dict[str, Any]:
    forecast = result.forecast
    if not _matches(forecast, request):
        log.warning(
            "agent forecast does not match its request; treated as abstaining",
            extra={"event_id": request.event_id, "agent": request.agent.value, "round": request.round},
        )
        forecast = _abstaining(request, forecast, MISMATCH_REASON)
    elif not forecast.capture_manifest and result.captures:
        captures = tuple(r for r in dict.fromkeys(result.captures) if r.fetched_at > request.as_of)
        forecast = forecast.model_copy(update={"capture_manifest": captures})
    tools = {
        r.result_id: {"tool": r.tool, "data": r.data or {}}
        for r in result.tool_results
        if r.status == "ok" and r.result_id is not None
    }
    log_ = [r.model_dump(mode="json", exclude_none=True) for r in result.tool_log]
    return {"forecast": _json(forecast), "tool_results": _json(tools), "tool_log": _json(log_)}


def _earlier_result(data: Mapping[str, Any]) -> AgentResult:
    """An agent's submitted result of an earlier round, rebuilt from the checkpoint: the `ok` tool results
    in the order the agent's session made the calls (any the records do not name follow by result id)."""
    forecast = _forecast(data["forecast"])
    stored = {str(k): v for k, v in dict(data.get("tool_results") or {}).items()}
    ordered: list[ToolResult] = []

    def take(rid: str) -> None:
        item = stored.pop(rid, None)
        if item is not None:
            ordered.append(
                ToolResult(tool=str(item["tool"]), status="ok", result_id=rid, data=dict(item["data"]))
            )

    for call in forecast.tool_calls:
        if call.status == "ok" and call.output_sha256 is not None:
            take(result_id(call.tool, call.output_sha256))
    for rid in sorted(stored):
        take(rid)
    tool_log = tuple(ToolResult.model_validate(r) for r in data.get("tool_log") or ())
    return AgentResult(
        forecast=forecast, captures=forecast.capture_manifest, tool_results=tuple(ordered), tool_log=tool_log
    )


def _side_checked(
    forecast: AgentForecast, candidate_set: CandidateSet | None, council: CouncilFile
) -> AgentForecast:
    """The forecast with its `candidate_id` dropped unless it names a candidate of the event's set on the side
    of the council's own stance for it (the runner checked against its own `p_used`); News never picks one."""
    if forecast.candidate_id is None:
        return forecast
    stance = stance_label(forecast, council)
    chosen = (
        next((c for c in candidate_set.candidates if c.candidate_id == forecast.candidate_id), None)
        if candidate_set is not None and forecast.agent is not AgentName.NEWS
        else None
    )
    if chosen is not None and chosen.side.value == stance:
        return forecast
    return forecast.model_copy(update={"candidate_id": None})


def _news_secrets(state: Mapping[str, Any]) -> tuple[float, ...]:
    """What the News agent must never be shown (Design Contract section 10): every price level of the
    event's candidate set and every other agent's `p_model` / `p_llm` / `p_used` of the rounds played before
    the current one (the same set when the round is sent and when its revisions are checked)."""
    values: set[float] = set()
    raw_set = state["event"].get("candidate_set")
    if raw_set is not None:
        for c in CandidateSet.model_validate(raw_set).candidates:
            values.update((float(c.entry), float(c.invalidation), float(c.tp1)))
    current = int(state.get("round", 1))
    forecasts: list[Mapping[str, Any]] = [
        r["forecast"]
        for key, r in state.get("results", {}).items()
        if key.startswith("shadow:") or int(key.split(":", 1)[0]) < current
    ]
    forecasts.extend(h["forecast"] for h in state.get("history", {}).values())
    forecasts.extend(state.get("effective", {}).values())
    for f in forecasts:
        if f.get("agent") == AgentName.NEWS.value:
            continue
        for key in ("p_model", "p_llm", "p_used"):
            value = f.get(key)
            if isinstance(value, int | float) and not isinstance(value, bool):
                values.add(float(value))
    return tuple(sorted(values))


def _last_side(history: Mapping[str, Any], agent: str, round_: int) -> int:
    """The side of the agent's latest effective forecast before `round_` not exactly on 0.5 (0: none)."""
    for r in range(round_ - 1, 0, -1):
        h = history.get(f"{r}:{agent}")
        if h is None:
            continue
        p = h["forecast"].get("p_used")
        if h["forecast"].get("abstain") or p is None:
            continue
        side = side_of(float(p))
        if side != 0:
            return side
    return 0


def _decimal(value: Any) -> Decimal | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    return Decimal(repr(float(value)))


@dataclass(frozen=True)
class Event:
    """Typed view of `state["event"]` after `load_context`."""

    data: Mapping[str, Any]

    @property
    def event_id(self) -> str:
        return str(self.data["event_id"])

    @property
    def candidate(self) -> Candidate:
        return Candidate.model_validate(self.data["candidate"])

    @property
    def council(self) -> CouncilFile:
        return CouncilFile.model_validate(self.data["council"])

    @property
    def candidate_set(self) -> CandidateSet | None:
        raw = self.data.get("candidate_set")
        return CandidateSet.model_validate(raw) if raw is not None else None

    @property
    def held_side(self) -> Side | None:
        raw = self.data.get("held_side")
        return Side(raw) if raw else None

    @property
    def pins(self) -> EventPins:
        candidate = self.candidate
        return EventPins(
            config_version_ids={str(k): int(v) for k, v in self.data["config_version_ids"].items()},
            universe_date=date.fromisoformat(self.data["universe_date"]),
            target_type=candidate.target_type,
            label_spec_version=candidate.label_spec_version,
        )

    def a(self, agent: str) -> float:
        return float(self.data["params"]["a"][agent])

    def r(self, agent: str) -> float:
        return float(self.data["params"]["r"][agent])

    def lesson(self, agent: str) -> Lesson | None:
        raw = self.data.get("lessons", {}).get(agent)
        return Lesson.model_validate(raw) if raw is not None else None

    def agree(self, effective: Mapping[str, Any], weights: Mapping[str, float]) -> ConsensusResult:
        council = self.council
        forecasts = {a: _forecast(f) for a, f in effective.items()}
        return consensus(
            {a: (None if f.abstain else f.p_used) for a, f in forecasts.items()},
            weights,
            stance_long=council.stance_long,
            stance_short=council.stance_short,
            quorum=council.quorum,
            supermajority=council.supermajority,
            groups={k: tuple(str(a) for a in v) for k, v in council.correlation_groups.items()},
        )


def _shared_obj(data: Mapping[str, Any]) -> SharedClaim:
    return SharedClaim(
        shared_id=data["shared_id"],
        source_agent=data["source_agent"],
        claim_sha256=data["claim_sha256"],
        claim=Claim.model_validate(data["claim"]),
    )


def _shared_json(shared: SharedClaim) -> dict[str, Any]:
    return {
        "shared_id": shared.shared_id,
        "source_agent": shared.source_agent,
        "claim_sha256": shared.claim_sha256,
        "claim": _json(shared.claim),
    }


def _claim_records(
    event_id: str, verified: Sequence[VerifiedClaim], shared: Sequence[SharedClaim]
) -> list[dict[str, Any]]:
    """`decision_claims` rows: shared claims under their shared id, the others under a derived id."""
    ids = {(s.source_agent, s.claim_sha256): s for s in shared}
    out = []
    for v in verified:
        s = ids.get((v.source_agent, v.claim_sha256)) if v.accepted else None
        sid = (
            s.shared_id
            if s is not None
            else shared_claim_id(event_id, v.round, f"{v.source_agent}|{v.original_id}|{v.claim_sha256}")
        )
        out.append(
            {
                "round": v.round,
                "shared_id": sid,
                "source_agent": v.source_agent,
                "original_claim_id": v.original_id,
                "claim_sha256": v.claim_sha256,
                "claim": _json(s.claim if s is not None else v.claim),
                "reject_reason": v.reject_reason,
                "penalty": v.penalty,
                "shared": s is not None,
            }
        )
    return out


# ---------------------------------------------------------------------------------------------- graph


class CouncilGraph:
    """Builds the meeting graph over `CouncilServices`."""

    def __init__(self, services: CouncilServices) -> None:
        self.s = services

    def compile(self, checkpointer: BaseCheckpointSaver[Any] | None) -> Any:
        g: StateGraph[Any] = StateGraph(CouncilState)
        g.add_node("load_context", self.load_context)
        # Send payloads (one agent turn) are not the graph state, which the node typing assumes.
        g.add_node("agent", cast(Any, self.agent))
        g.add_node("collect", self.collect)
        g.add_node("commit_round1", self.commit_round1)
        g.add_node("apply_revisions", self.apply_revisions)
        g.add_node("check_consensus", self.check_consensus)
        g.add_node("share_claims", self.share_claims)
        g.add_node("aggregate", self.aggregate)
        g.add_node("manager", self.manager)
        g.add_node("emit_decision", self.emit_decision)
        g.add_edge(START, "load_context")
        g.add_conditional_edges("load_context", self.fan_round1, ["agent"])
        g.add_edge("agent", "collect")
        g.add_conditional_edges("collect", self.after_collect, ["commit_round1", "apply_revisions"])
        g.add_edge("commit_round1", "check_consensus")
        g.add_edge("apply_revisions", "check_consensus")
        g.add_conditional_edges("check_consensus", self.after_consensus, ["share_claims", "aggregate"])
        g.add_conditional_edges("share_claims", self.fan_revisions, ["agent", "aggregate"])
        g.add_edge("aggregate", "manager")
        g.add_edge("manager", "emit_decision")
        g.add_edge("emit_decision", END)
        return g.compile(checkpointer=checkpointer)

    # ------------------------------------------------------------------ context and agent calls

    async def load_context(self, state: CouncilState) -> dict[str, Any]:
        """Pin everything the event runs with: config versions, universe date, candidate set, held side,
        the learned parameters and the shadow lessons."""
        event = dict(state["event"])
        candidate = Candidate.model_validate(event["candidate"])
        version_ids = {str(k): int(v) for k, v in event["config_version_ids"].items()}
        settings = self.s.settings(version_ids)
        entry = self.s.universe.entry(candidate.coin_id, candidate.as_of)
        if entry is None:
            raise LookupError(
                f"coin {candidate.coin_id} is not in the universe at {candidate.as_of.isoformat()}"
            )
        held = await self.s.held.held_side(candidate.coin_id, candidate.as_of)
        pins = EventPins(
            version_ids, entry.universe_date, candidate.target_type, candidate.label_spec_version
        )
        candidate_set = None
        if candidate.source is not CandidateSource.HELD:
            candidate_set = self.s.candidate_sets.candidate_set(candidate.coin_id, candidate.as_of, pins)
        params = self.s.params.load(candidate.target_type, candidate.label_spec_version, at=candidate.as_of)
        stacker_at = params.stacker_trained_through
        agents = agent_names(settings.council)
        lessons: dict[str, Any] = {}
        if self.s.lessons is not None:
            for agent in agents:
                lesson = self.s.lessons.shadow(agent, candidate.as_of)
                if lesson is not None:
                    lessons[agent.value] = _json(lesson)
        event.update(
            symbol=entry.symbol,
            universe_date=entry.universe_date.isoformat(),
            config_version_ids=version_ids,
            candidate_set=_json(candidate_set) if candidate_set is not None else None,
            held_side=held.value if held is not None else None,
            council=_json(settings.council),
            params={
                "params_version": params.params_version,
                "stacker_trained_through": _json(stacker_at) if stacker_at is not None else None,
                "b": params.intercept_b,
                "a": {a.value: params.a(a.value) for a in agents},
                "r": {a.value: params.r(a.value) for a in agents},
                "pins": _json(params.pins),
            },
            lessons=lessons,
        )
        held_text = f"; held {held.value}" if held is not None else ""
        text = (
            f"{candidate.source.value} candidate on {entry.symbol} (score {candidate.score:.3f}, "
            f"rule {candidate.rule_version}){held_text}"
        )
        return {
            "event": event,
            "round": 1,
            "results": {},
            "history": {},
            "claims": [],
            "shared": [],
            "timeline": [_entry("scanner", text)],
        }

    def _params(self, ev: Event) -> PoolParams:
        """The parameters pinned by `load_context`: the same version, stacking model, agent-version overlays
        and claims-audit flags (by identity, not by time: a scorer commit that spans `as_of` cannot change
        them between the live run, a resume and a replay)."""
        candidate = ev.candidate
        pinned = ev.data["params"]
        return self.s.params.load(
            candidate.target_type,
            candidate.label_spec_version,
            at=candidate.as_of,
            params_version=int(pinned["params_version"]),
            pins=pinned.get("pins"),
        )

    def _visible(self, state: Mapping[str, Any], agent: str) -> list[SharedClaim]:
        """The shared claims `agent` may see this round (the News agent: only the news-safe ones)."""
        shared = [_shared_obj(s) for s in state["shared"]]
        if agent != AgentName.NEWS.value:
            return visible(agent, shared, news=False)
        return visible(agent, shared, news=True, secrets=_news_secrets(state))

    def _payload(self, state: CouncilState, agent: str, round_: int) -> dict[str, Any]:
        payload: dict[str, Any] = {"event": state["event"], "agent": agent, "round": round_}
        if round_ > 1:
            payload["previous"] = state["effective"][agent]
            seen = self._visible(state, agent)
            payload["shared_claims"] = [_json(c) for c in shown_to(agent, seen)]
            payload["earlier"] = [
                state["results"][key] for r in range(1, round_) if (key := f"{r}:{agent}") in state["results"]
            ]
        return payload

    def fan_round1(self, state: CouncilState) -> list[Send]:
        agents = agent_names(Event(state["event"]).council)
        return [Send("agent", self._payload(state, a.value, 1)) for a in agents]

    async def agent(self, payload: dict[str, Any]) -> dict[str, Any]:
        """One agent, one round (a `Send` task; its write is checkpointed as soon as it finishes)."""
        ev = Event(payload["event"])
        agent = AgentName(payload["agent"])
        round_ = int(payload["round"])
        candidate = ev.candidate
        pins = ev.pins
        if self.s.bind_pins is not None:
            self.s.bind_pins(candidate.coin_id, candidate.as_of, pins)
        request = AgentRequest(
            event_id=ev.event_id,
            agent=agent,
            round=round_,
            coin_id=candidate.coin_id,
            as_of=candidate.as_of,
            source=candidate.source,
            target_type=candidate.target_type,
            label_spec_version=candidate.label_spec_version,
            config_version_ids=pins.config_version_ids,
            universe_date=pins.universe_date,
            candidate_set=ev.candidate_set,
            a_i=ev.a(agent.value),
            mode=self.s.mode,
            previous=_forecast(payload["previous"]) if "previous" in payload else None,
            shared_claims=tuple(Claim.model_validate(c) for c in payload.get("shared_claims", ())),
            pinned=self.s.pinned(ev.event_id, agent, round_),
            earlier=tuple(_earlier_result(r) for r in payload.get("earlier", ())),
        )
        out = {f"{round_}:{agent.value}": _result_json(await self.s.runner.forecast(request), request)}
        lesson = ev.lesson(agent.value) if round_ == 1 else None
        if lesson is not None:
            shadow_request = replace(request, shadow_lesson=lesson)
            shadow = _result_json(await self.s.runner.forecast(shadow_request), shadow_request)
            out[f"shadow:{agent.value}"] = {"lesson_id": lesson.lesson_id, **shadow}
        return {"results": out}

    async def collect(self, state: CouncilState) -> dict[str, Any]:
        """Join point after the parallel agent calls of a round."""
        return {}

    def after_collect(self, state: CouncilState) -> str:
        return "commit_round1" if state["round"] == 1 else "apply_revisions"

    # ------------------------------------------------------------------ rounds

    def _packets(self, forecasts: Iterable[AgentForecast], coin_id: int) -> dict[str, QuantPacket]:
        out: dict[str, QuantPacket] = {}
        for f in forecasts:
            if f.packet_sha256 is None:
                continue
            packet = self.s.packets.packet(f.packet_sha256)
            if packet is not None and packet.agent is f.agent and packet.coin_id == coin_id:
                out[f.agent.value] = packet
        return out

    async def commit_round1(self, state: CouncilState) -> dict[str, Any]:
        """Re-enforce the round-1 bound and commit the blind round before anything is shared."""
        ev = Event(state["event"])
        council = ev.council
        forecasts = {
            a.value: _side_checked(
                enforce_round1(
                    _forecast(state["results"][f"1:{a.value}"]["forecast"]),
                    a_i=ev.a(a.value),
                    margin=council.llm_logit_margin,
                ),
                ev.candidate_set,
                council,
            )
            for a in agent_names(council)
        }
        commit = RoundCommit.of(ev.event_id, 1, forecasts)
        self.s.store.commit_round(commit, utcnow())
        present = sorted(a for a, f in forecasts.items() if not f.abstain)
        regime = self.s.params.regime(self._packets(forecasts.values(), ev.candidate.coin_id))
        params = self._params(ev)
        weights = params.weights(regime, present) if present else {}
        effective = {a: _json(f) for a, f in forecasts.items()}
        text = (
            f"round 1 committed blind: {len(present)} of {len(forecasts)} agents with an opinion "
            f"(sha256 {commit.round_sha256[:16]})"
        )
        return {
            "effective": effective,
            "history": {f"1:{a}": {"forecast": f, "revision": None} for a, f in effective.items()},
            "commit": {"round_sha256": commit.round_sha256, "forecasts": dict(commit.forecasts)},
            "weights": {a: float(w) for a, w in sorted(weights.items())},
            "regime": regime,
            "timeline": [*state["timeline"], _entry("council", text)],
        }

    async def check_consensus(self, state: CouncilState) -> dict[str, Any]:
        ev = Event(state["event"])
        council = ev.council
        result = ev.agree(state["effective"], state["weights"])
        round_ = state["round"]
        out: dict[str, Any] = {
            "consensus": {
                "reached": result.reached,
                "side": result.side.value if result.side is not None else None,
                "side_weight": result.side_weight,
                "checks": [c.to_json() for c in result.checks],
            }
        }
        if round_ == 1:
            out["round1_consensus"] = result.reached
        side = result.side.value if result.side is not None else "no side"
        verdict = "reached" if result.reached else "not reached"
        text = f"round {round_}: consensus {verdict} ({side}, weight {result.side_weight:.2f})"
        out["timeline"] = [*state["timeline"], _entry("council", text, "pos" if result.reached else "")]
        if result.reached:
            out["stop_reason"] = f"consensus_round_{round_}"
        elif not council.debate_enabled:
            out["stop_reason"] = "debate_disabled"
        elif round_ >= council.max_rounds:
            out["stop_reason"] = "max_rounds"
        return out

    def after_consensus(self, state: CouncilState) -> str:
        return "aggregate" if state.get("stop_reason") else "share_claims"

    def _verify(self, state: CouncilState, round_: int) -> list[VerifiedClaim]:
        """Verify the claims the agents submitted in `round_`."""
        ev = Event(state["event"])
        coin_id = ev.candidate.coin_id
        evidence: list[AgentEvidence] = []
        manifest: list[LakeRef] = []
        for agent in agent_names(ev.council):
            result = state["results"].get(f"{round_}:{agent.value}")
            if result is None:
                continue
            forecast = _forecast(result["forecast"])
            manifest.extend(forecast.capture_manifest)
            packet = self._packets([forecast], coin_id).get(agent.value)
            evidence.append(AgentEvidence(agent.value, forecast, packet, result["tool_results"]))
        return verify_round(
            evidence,
            round_=round_,
            sources=self.s.sources,
            as_of=ev.candidate.as_of,
            pinned=tuple(dict.fromkeys(manifest)),
            already_shared=[c["claim_sha256"] for c in state["claims"] if c["shared"]],
        )

    async def share_claims(self, state: CouncilState) -> dict[str, Any]:
        """Verify this round's claims and share the verified ones, anonymized, for the next round."""
        ev = Event(state["event"])
        council = ev.council
        round_ = state["round"]
        verified = self._verify(state, round_)
        shared = share(
            [(v.source_agent, v.claim_sha256, v.claim) for v in verified if v.accepted],
            event_id=ev.event_id,
            round_=round_,
            max_chars=council.claim_max_chars,
        )
        rejected = sum(1 for v in verified if not v.accepted)
        text = f"round {round_}: {len(shared)} verified claims shared, {rejected} rejected"
        out: dict[str, Any] = {
            "claims": [*state["claims"], *_claim_records(ev.event_id, verified, shared)],
            "timeline": [*state["timeline"], _entry("council", text, "warn" if rejected else "")],
        }
        shared_json = [_shared_json(s) for s in shared]
        next_round = {**state, "round": round_ + 1, "shared": shared_json}
        targets = [
            a.value
            for a in agent_names(council)
            if not _forecast(state["effective"][a.value]).abstain and self._visible(next_round, a.value)
        ]
        if not targets:
            out["stop_reason"] = "no_new_claims"
            return out
        out.update(round=round_ + 1, shared=shared_json, sent=targets)
        return out

    def fan_revisions(self, state: CouncilState) -> list[Send] | str:
        if state.get("stop_reason"):
            return "aggregate"
        return [Send("agent", self._payload(state, a, state["round"])) for a in state["sent"]]

    async def apply_revisions(self, state: CouncilState) -> dict[str, Any]:
        """Re-enforce the per-round and total logit bounds, the new-citation rule and the flip rule."""
        ev = Event(state["event"])
        council = ev.council
        round_ = state["round"]
        effective = dict(state["effective"])
        history = dict(state["history"])
        accepted = 0
        for agent in state["sent"]:
            cited_before = {
                cid
                for r in range(2, round_)
                if (h := history.get(f"{r}:{agent}")) is not None and h["revision"] is not None
                for cid in h["revision"]["new_citations"]
            }
            revision = revise(
                _forecast(effective[agent]),
                _forecast(state["results"][f"{round_}:{agent}"]["forecast"]),
                shown={s.shared_id: s.claim for s in self._visible(state, agent)},
                cited_before=cited_before,
                a_i=ev.a(agent),
                margin=council.llm_logit_margin,
                bound_round=council.logit_bound_round,
                bound_total=council.logit_bound_total,
                last_side=_last_side(history, agent, round_),
            )
            effective[agent] = _json(_side_checked(revision.effective, ev.candidate_set, council))
            history[f"{round_}:{agent}"] = {"forecast": effective[agent], "revision": revision.to_json()}
            accepted += int(revision.accepted)
        text = f"round {round_}: {accepted} of {len(state['sent'])} revisions within the rules"
        return {
            "effective": effective,
            "history": history,
            "timeline": [*state["timeline"], _entry("council", text)],
        }

    # ------------------------------------------------------------------ decision

    async def aggregate(self, state: CouncilState) -> dict[str, Any]:
        """Record the last round's claims (verified, never shared), then pool round 1 with the final round."""
        ev = Event(state["event"])
        council = ev.council
        round_ = state["round"]
        claims = list(state["claims"])
        if not any(c["round"] == round_ for c in claims):
            claims.extend(_claim_records(ev.event_id, self._verify(state, round_), []))
        round1 = {
            k.split(":", 1)[1]: _forecast(h["forecast"]) for k, h in state["history"].items() if k[0] == "1"
        }
        final = {a: _forecast(f) for a, f in state["effective"].items()}
        p1 = {a: f.p_used for a, f in round1.items() if not f.abstain and f.p_used is not None}
        pf = {a: f.p_used for a, f in final.items() if not f.abstain and f.p_used is not None}
        params = self._params(ev)
        result = pool(
            p1,
            pf,
            weights=state["weights"],
            r=ev.r,
            calibrate=params.calibrate,
            intercept_b=float(ev.data["params"]["b"]),
            p_clip=council.p_clip,
            stacker=params.stacker,
            regime=state.get("regime"),
        )
        if result is None:
            return {"claims": claims, "pool": None}
        return {
            "claims": claims,
            "pool": {
                "p": result.p,
                "p_unclipped": result.p_unclipped,
                "disagreement": result.disagreement,
                "weights": dict(result.weights),
                "method": result.method,
            },
        }

    def _decision_packet(
        self, final: Mapping[str, AgentForecast], side: Side, weights: Mapping[str, float], wanted: str
    ) -> QuantPacket | None:
        """A committed non-news packet on the event's candidate set (Risk reads its features): winning-side
        agents by weight first, then the others."""

        def order(f: AgentForecast) -> tuple[int, float, str]:
            on_side = f.p_used is not None and (f.p_used > 0.5) == (side is Side.LONG)
            return (0 if on_side else 1, -weights.get(f.agent.value, 0.0), f.agent.value)

        ranked = sorted((f for f in final.values() if f.agent is not AgentName.NEWS), key=order)
        for f in ranked:
            if f.packet_sha256 is None:
                continue
            packet = self.s.packets.packet(f.packet_sha256)
            if packet is not None and packet.agent is f.agent and packet.candidate_set_sha256 == wanted:
                return packet
        return None

    async def manager(self, state: CouncilState) -> dict[str, Any]:
        ev = Event(state["event"])
        council = ev.council
        m = council.manager
        pooled = state.get("pool")
        p = pooled["p"] if pooled else None
        agreed = state["consensus"]["side"] if state["consensus"]["reached"] else None
        decision = manage(
            Side(agreed) if agreed else None,
            p,
            pooled["disagreement"] if pooled else None,
            debate_over=state["stop_reason"] in DEBATE_OVER,
            p_long=m.p_long,
            p_short=m.p_short,
            d_threshold=m.d_threshold,
            size_consensus=m.size_consensus,
            size_fallback=m.size_fallback,
        )
        checks: list[Check] = list(decision.checks)
        outcome, size = decision.outcome, decision.size
        candidate_id: str | None = None
        packet_sha256: str | None = None
        side = decision.side
        held = ev.held_side
        final = {a: _forecast(f) for a, f in state["effective"].items()}
        if side is not None and held is None:
            candidate_set = ev.candidate_set
            packet = (
                self._decision_packet(final, side, state["weights"], candidate_set.candidate_set_sha256)
                if candidate_set is not None
                else None
            )
            if candidate_set is None or packet is None:
                checks.append(Check(False, "a committed quant packet on the event's candidate set"))
            else:
                winners = {
                    a: f.candidate_id for a, f in final.items() if stance_label(f, council) == side.value
                }
                chosen = select_candidate(
                    side,
                    winners,
                    state["weights"],
                    candidate_set,
                    _decimal(packet.features.get("mark_price")),
                )
                if chosen is None:
                    checks.append(Check(False, f"a {side.value} level candidate chosen by the winning side"))
                else:
                    checks.append(Check(True, f"level candidate {chosen.candidate_id} (R:R {chosen.rr:.2f})"))
                    candidate_id, packet_sha256 = chosen.candidate_id, packet.packet_sha256
            if candidate_id is None:
                outcome, size = Outcome.NO_TRADE, 0.0
        intent = position_intent(outcome, held, p, reevaluation=ev.candidate.source is CandidateSource.HELD)
        if intent is Intent.OPEN and ev.data.get("shadow_only"):
            checks.append(Check(False, "shadow-only candidate (loose scanner thresholds): not traded"))
            intent = None
        tone: TimelineTone = "pos" if intent is Intent.OPEN else "warn" if intent is Intent.EXIT else ""
        p_text = f"pooled p {p:.3f}" if p is not None else "no pooled p"
        text = f"{outcome.value} size {size:.2f} by the {decision.rule} rule ({p_text}); " + (
            f"intent {intent.value}" if intent is not None else "nothing sent to Risk"
        )
        return {
            "decision": {
                "outcome": outcome.value,
                "size": size,
                "rule": decision.rule,
                "checks": [c.to_json() for c in checks],
                "candidate_id": candidate_id if intent is Intent.OPEN or intent is None else None,
                "packet_sha256": packet_sha256,
                "intent": intent.value if intent is not None else None,
            },
            "timeline": [*state["timeline"], _entry("manager", text, tone)],
        }

    async def emit_decision(self, state: CouncilState) -> dict[str, Any]:
        """Re-check the card's round-1 forecasts against the blind commit (in the state and in the durable
        store) and write the decision; a mismatch fails the meeting instead of emitting a tampered card."""
        ev = Event(state["event"])
        round1 = {
            key.split(":", 1)[1]: _forecast(h["forecast"])
            for key, h in state["history"].items()
            if key.split(":", 1)[0] == "1"
        }
        committed = RoundCommit(
            ev.event_id, 1, dict(state["commit"]["forecasts"]), str(state["commit"]["round_sha256"])
        )
        problems = verify_commit(committed, round1)
        if problems:
            raise CommitMismatchError(
                f"event {ev.event_id}: round-1 forecasts differ from the blind commit ({', '.join(problems)})"
            )
        self.s.store.commit_round(RoundCommit.of(ev.event_id, 1, round1), utcnow())
        self.s.store.emit(build_record(state))
        return {}


# ---------------------------------------------------------------------------------------------- record


def card_hash(
    card: Mapping[str, Any], forecasts: Sequence[Mapping[str, Any]], claims: Sequence[Mapping[str, Any]]
) -> str:
    """sha256 of the decision content: the card without its hash and wall-clock times (timeline `at`),
    plus every forecast row and claim row. A replay of the same event reproduces it exactly."""
    body = {k: v for k, v in card.items() if k not in ("card_sha256", "timeline")}
    body["timeline"] = [{k: v for k, v in e.items() if k != "at"} for e in card["timeline"]]
    return canonical_sha256({"card": body, "forecasts": list(forecasts), "claims": list(claims)})


def _summary(outcome: str, size: float, rule: str, p: float | None, stop: str, rounds: int) -> str:
    p_text = f"pooled p {p:.3f}" if p is not None else "no pooled p"
    return f"{outcome} (size {size:.2f}, {rule} rule), {p_text}, {rounds} round(s), stopped: {stop}"


def build_record(state: CouncilState) -> DecisionRecord:
    """Assemble the decision card, its rows and the DecisionMsg from the final state (pure)."""
    ev = Event(state["event"])
    council = ev.council
    candidate = ev.candidate
    decision = state["decision"]
    pooled = state.get("pool")
    p = pooled["p"] if pooled else None
    outcome = Outcome(decision["outcome"])
    intent = Intent(decision["intent"]) if decision["intent"] else None
    held = ev.held_side
    if intent in (Intent.HOLD, Intent.EXIT):
        card_outcome = intent.value
        side = held.value if held is not None else None
        manager_size = 0.0
    else:
        card_outcome = outcome.value
        side = None if outcome is Outcome.NO_TRADE else outcome.value
        manager_size = float(decision["size"])
    weights = state["weights"]
    rounds = int(state["round"])
    forecasts = []
    for key, h in sorted(state["history"].items(), key=lambda kv: (int(kv[0].split(":")[0]), kv[0])):
        round_s, agent = key.split(":", 1)
        effective = _forecast(h["forecast"])
        forecasts.append(
            {
                "agent": agent,
                "round": int(round_s),
                "forecast": h["forecast"],
                "submitted": state["results"][key]["forecast"],
                "revision": h["revision"],
                "stance": stance_label(effective, council),
                "weight_norm": weights.get(agent),
                "commit_sha256": forecast_sha256(effective),
            }
        )
    claims = sorted(state["claims"], key=lambda c: (c["round"], c["shared_id"]))
    final = {a: _forecast(f) for a, f in sorted(state["effective"].items())}
    candidate_set = ev.candidate_set
    params = ev.data["params"]
    card: dict[str, Any] = {
        "event_id": ev.event_id,
        "coin_id": candidate.coin_id,
        "symbol": ev.data["symbol"],
        "as_of": _json(candidate.as_of),
        "source": candidate.source.value,
        "outcome": card_outcome,
        "side": side,
        "manager_size": manager_size,
        "p_pooled": p,
        "disagreement": pooled["disagreement"] if pooled else None,
        "rounds": rounds,
        "stop_reason": state["stop_reason"],
        "candidate_id": decision["candidate_id"],
        "candidate_set_sha256": candidate_set.candidate_set_sha256 if candidate_set is not None else None,
        "packet_sha256": decision["packet_sha256"],
        "target_type": candidate.target_type.value,
        "label_spec_version": candidate.label_spec_version,
        "config_version_ids": dict(ev.data["config_version_ids"]),
        "universe_date": ev.data["universe_date"],
        "summary": _summary(card_outcome, manager_size, decision["rule"], p, state["stop_reason"], rounds)[
            :SUMMARY_MAX
        ],
        "consensus": list(state["consensus"]["checks"])[:CHECKS_MAX],
        "manager_rule": list(decision["checks"])[:CHECKS_MAX],
        "timeline": list(state["timeline"]),
        "unscored": bool(ev.data.get("unscored", False)),
        "shadow_only": bool(ev.data.get("shadow_only", False)),
        "held_side": held.value if held is not None else None,
        "intent": intent.value if intent is not None else None,
        "round1_sha256": state["commit"]["round_sha256"],
        "params": {
            "params_version": params["params_version"],
            "stacker_trained_through": params.get("stacker_trained_through"),
            "pins": params.get("pins"),
            "b": params["b"],
            "a": params["a"],
            "r": params["r"],
            "w": dict(weights),
            "regime": state.get("regime"),
            "method": pooled["method"] if pooled else None,
            "round1_consensus": bool(state.get("round1_consensus", False)),
        },
        "agents": {
            a: {
                "agent_version": f.agent_version,
                "packet_sha256": f.packet_sha256,
                "model_slug": f.model_slug,
                "model_returned": f.model_returned,
                "provider": f.provider,
                "prompt_hash": f.prompt_hash,
                "skill_commit": f.skill_commit,
                "abstain": f.abstain,
            }
            for a, f in final.items()
        },
        "hard_evidence_version": hard_evidence.POLICY_VERSION,
    }
    card["card_sha256"] = card_hash(card, forecasts, claims)
    msg = None
    if intent is not None:
        assert side is not None
        msg_side = Side(side)
        p_msg = min(max(p if p is not None else 0.5, 1e-6), 1 - 1e-6)
        msg = DecisionMsg(
            event_id=ev.event_id,
            coin_id=candidate.coin_id,
            as_of=candidate.as_of,
            intent=intent,
            side=msg_side,
            p=p_msg,
            p_side=p_msg if msg_side is Side.LONG else 1.0 - p_msg,
            manager_size=manager_size if intent is Intent.OPEN else 0.0,
            candidate_id=decision["candidate_id"] if intent is Intent.OPEN else None,
            packet_sha256=decision["packet_sha256"],
            config_version_ids=dict(ev.data["config_version_ids"]),
            target_type=candidate.target_type,
            label_spec_version=candidate.label_spec_version,
        )
    shadow = [
        (AgentName(key.split(":", 1)[1]), str(value["lesson_id"]), _forecast(value["forecast"]))
        for key, value in sorted(state["results"].items())
        if key.startswith("shadow:")
    ]
    return DecisionRecord(card, forecasts, claims, msg, shadow, final)
