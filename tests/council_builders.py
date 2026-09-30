"""Builders and in-memory ports for council tests: scripted agents, packets, candidate sets, sources.

`ScriptedRunner` plays each agent from a script keyed by (agent, round); the forecast it returns is built
like the phase 05 runner builds one (`p_used` from `p_model`, `p_llm` and `a_i`), so the council's own
re-enforcement of the bounds is what the tests observe.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

from hdt.contracts.candidate import Candidate, CandidateSet, LevelCandidate
from hdt.contracts.common import (
    AgentName,
    CandidateSource,
    ClaimKind,
    DirectionHint,
    Side,
    TargetType,
    Tier,
)
from hdt.contracts.forecast import AgentForecast, Claim, LakeRef
from hdt.contracts.packet import QuantPacket
from hdt.core.config import static_config
from hdt.core.ids import sha256_hex
from hdt.council.commit import CommitMismatchError, RoundCommit
from hdt.council.graph import (
    CouncilGraph,
    CouncilServices,
    DecisionRecord,
    EventSettings,
    UniverseEntry,
)
from hdt.council.ports import AgentRequest, AgentResult, EventPins, Stacker, StoredSource
from hdt.tools.base import ToolResult

AS_OF = datetime(2026, 3, 2, 12, 0, tzinfo=UTC)
COIN = 5426
SYMBOL = "SOLUSDT"
UNIVERSE_DATE = date(2026, 3, 2)
VERSION_IDS = {"council": 1, "risk": 2}
LABEL_SPEC = "label-v1"
AGENTS = tuple(AgentName)
MODEL_AGENTS = tuple(a for a in AGENTS if a is not AgentName.NEWS)

LONG_A = "lc_AAAAAAAAAAAAAAAA"
LONG_B = "lc_BBBBBBBBBBBBBBBB"
SHORT_A = "lc_CCCCCCCCCCCCCCCC"


def logit(p: float) -> float:
    return math.log(p / (1 - p))


def sigmoid(z: float) -> float:
    return 1 / (1 + math.exp(-z))


def candidate(
    source: CandidateSource = CandidateSource.LTX, as_of: datetime = AS_OF, coin_id: int = COIN
) -> Candidate:
    return Candidate(
        coin_id=coin_id,
        as_of=as_of,
        source=source,
        score=2.5,
        rule_version="ltx-v1",
        target_type=TargetType.RESID_12H,
        label_spec_version=LABEL_SPEC,
    )


def candidate_set(as_of: datetime = AS_OF) -> CandidateSet:
    tick = Decimal("0.01")
    return CandidateSet(
        coin_id=COIN,
        as_of=as_of,
        levels_ver="levels-v1",
        candidates=(
            LevelCandidate(
                candidate_id=LONG_A,
                side=Side.LONG,
                entry=Decimal("100.00"),
                invalidation=Decimal("98.00"),
                tp1=Decimal("104.00"),
                rr=2.0,
                tick=tick,
            ),
            LevelCandidate(
                candidate_id=LONG_B,
                side=Side.LONG,
                entry=Decimal("99.00"),
                invalidation=Decimal("97.00"),
                tp1=Decimal("105.00"),
                rr=3.0,
                tick=tick,
            ),
            LevelCandidate(
                candidate_id=SHORT_A,
                side=Side.SHORT,
                entry=Decimal("101.00"),
                invalidation=Decimal("103.00"),
                tp1=Decimal("97.00"),
                rr=2.0,
                tick=tick,
            ),
        ),
    )


def packet(agent: AgentName, p_model: float, features: Mapping[str, Any] | None = None) -> QuantPacket:
    base: dict[str, Any] = {"mark_price": 100.2, "atr_1h": 1.5, "rsi_14": 61.0, "funding_z": -1.8}
    base.update(features or {})
    return QuantPacket.build(
        agent=agent,
        coin_id=COIN,
        as_of=AS_OF,
        features=base,
        p_model=p_model,
        candidate_set_sha256=candidate_set().candidate_set_sha256,
        data_quality={},
        universe_date=UNIVERSE_DATE,
        config_version_ids=VERSION_IDS,
        feature_ver="features-v1",
        p_model_ver="pm-v1",
        target_type=TargetType.RESID_12H,
        label_spec_version=LABEL_SPEC,
    )


def claim(
    claim_id: str,
    kind: ClaimKind,
    ref: str,
    statement: str,
    *,
    value: float | str | None = None,
    quote: str | None = None,
    hint: DirectionHint = DirectionHint.NONE,
) -> Claim:
    return Claim(
        claim_id=claim_id,
        kind=kind,
        ref=ref,
        statement=statement,
        value=value,
        quote=quote,
        direction_hint=hint,
    )


@dataclass(frozen=True)
class Turn:
    """One agent's move in one round (None p_llm: abstain)."""

    p_llm: float | None
    claims: tuple[Claim, ...] = ()
    cited: tuple[str, ...] = ()
    """Round >= 2: indexes into the claims shown ("#0", "#1") or literal shared ids."""
    candidate_id: str | None = None
    cite_where: tuple[tuple[str, str], ...] = ()
    """(field, substring) pairs: cite every shown claim whose `field` contains the substring."""
    tool_results: tuple[ToolResult, ...] = ()
    tool_log: tuple[ToolResult, ...] = ()


@dataclass
class ScriptedRunner:
    """Fake `AgentRunner`: round-1 `p_model` per agent and a `Turn` per (agent, round)."""

    p_model: Mapping[AgentName, float]
    turns: Mapping[tuple[AgentName, int], Turn]
    margin: float = 0.5
    bound_total: float = 1.0
    calls: list[tuple[str, AgentName, int]] = field(default_factory=list)
    shown: dict[tuple[AgentName, int], tuple[Claim, ...]] = field(default_factory=dict)
    fail_on: set[tuple[AgentName, int]] = field(default_factory=set)
    features: Mapping[AgentName, Mapping[str, Any]] = field(default_factory=dict)
    """Extra packet features per agent (the packet store holds the same packets)."""
    requests: list[AgentRequest] = field(default_factory=list)

    def packet(self, agent: AgentName) -> QuantPacket:
        return packet(agent, self.p_model[agent], self.features.get(agent))

    async def forecast(self, request: AgentRequest) -> AgentResult:
        key = (request.agent, request.round)
        if key in self.fail_on:
            self.fail_on.discard(key)
            raise RuntimeError(f"process killed during {request.agent.value} round {request.round}")
        self.calls.append((request.event_id, request.agent, request.round))
        self.requests.append(request)
        self.shown[key] = request.shared_claims
        turn = self.turns.get(key, Turn(p_llm=None))
        return AgentResult(
            forecast=self.build(request, turn), tool_results=turn.tool_results, tool_log=turn.tool_log
        )

    def build(self, request: AgentRequest, turn: Turn) -> AgentForecast:
        pm = self.p_model[request.agent]
        packet_sha = self.packet(request.agent).packet_sha256 if request.agent is not AgentName.NEWS else None
        common: dict[str, Any] = {
            "agent": request.agent,
            "agent_version": "1",
            "event_id": request.event_id,
            "round": request.round,
            "coin_id": request.coin_id,
            "as_of": request.as_of,
            "target_type": request.target_type,
            "label_spec_version": request.label_spec_version,
            "claims": turn.claims,
            "packet_sha256": packet_sha,
            "model_slug": "fake/model",
            "provider": "fake",
            "reason": f"{request.agent.value} round {request.round}",
        }
        cited: list[str] = []
        for ref in turn.cited:
            if ref.startswith("#"):
                cited.append(request.shared_claims[int(ref[1:])].claim_id)
            else:
                cited.append(ref)
        for attr, needle in turn.cite_where:
            cited.extend(c.claim_id for c in request.shared_claims if needle in str(getattr(c, attr)))
        common["cited_claim_ids"] = tuple(dict.fromkeys(cited))
        if turn.p_llm is None:
            return AgentForecast(**common, abstain=True, abstain_reason="scripted abstain")
        z_model = logit(pm)
        bound = self.margin if request.round == 1 else self.bound_total
        z_used = z_model + request.a_i * max(-bound, min(bound, logit(turn.p_llm) - z_model))
        return AgentForecast(
            **common,
            abstain=False,
            p_model=pm,
            p_llm=turn.p_llm,
            p_used=sigmoid(z_used),
            candidate_id=turn.candidate_id,
        )


class Packets:
    def __init__(self, packets: Iterable[QuantPacket]) -> None:
        self.by_sha = {p.packet_sha256: p for p in packets}

    def packet(self, packet_sha256: str) -> QuantPacket | None:
        return self.by_sha.get(packet_sha256)


class CandidateSets:
    def candidate_set(self, coin_id: int, as_of: datetime, pins: EventPins) -> CandidateSet:
        return candidate_set(as_of)


class Sources:
    def __init__(self, sources: Iterable[StoredSource] = ()) -> None:
        self.by_id = {s.item_id: s for s in sources}

    def stored_source(
        self, item_id: str, *, as_of: datetime, pinned: tuple[LakeRef, ...], coin_id: int | None = None
    ) -> StoredSource | None:
        return self.by_id.get(item_id)


def stored_source(
    item_id: str, text: str, *, tier: Tier, official: bool, event_class: str | None
) -> StoredSource:
    return StoredSource(
        item_id=item_id,
        text=text,
        final_url=f"https://example.org/{item_id}",
        domain="example.org",
        tier=tier,
        event_class=event_class,
        official=official,
        body_sha256=sha256_hex(text.encode()),
    )


@dataclass
class UniformParams:
    """PoolParams before any scored history: uniform w, a = 1, r = 0.5, b = 0, identity calibration."""

    params_version: int = 0
    intercept_b: float = 0.0
    stacker: Stacker | None = None
    stacker_trained_through: datetime | None = None
    pins: dict[str, Any] = field(default_factory=dict)

    def a(self, agent: str) -> float:
        return 1.0

    def r(self, agent: str) -> float:
        return 0.5

    def calibrate(self, agent: str, p: float) -> float:
        return p

    def weights(self, regime: str | None, present: Iterable[str]) -> dict[str, float]:
        agents = sorted(present)
        return {a: 1 / len(agents) for a in agents}


class Params:
    def __init__(self, params: UniformParams | None = None) -> None:
        self.params = params or UniformParams()
        self.calls: list[tuple[datetime | None, int | None]] = []
        self.pins_seen: list[dict[str, Any] | None] = []

    def load(
        self,
        target_type: TargetType,
        label_spec_version: str,
        *,
        at: datetime | None = None,
        params_version: int | None = None,
        pins: Mapping[str, Any] | None = None,
    ) -> UniformParams:
        self.calls.append((at, params_version))
        self.pins_seen.append(dict(pins) if pins is not None else None)
        return self.params

    def regime(self, packets: Mapping[str, QuantPacket]) -> str | None:
        return None


class Held:
    def __init__(self, side: Side | None = None) -> None:
        self.side = side

    async def held_side(self, coin_id: int, as_of: datetime) -> Side | None:
        return self.side


class Universe:
    def entry(self, coin_id: int, as_of: datetime) -> UniverseEntry | None:
        return UniverseEntry(SYMBOL, UNIVERSE_DATE)


class MemoryDecisionStore:
    def __init__(self) -> None:
        self.commits: dict[tuple[str, int], RoundCommit] = {}
        self.records: dict[str, DecisionRecord] = {}
        self.emits = 0

    def commit_round(self, commit: RoundCommit, at: datetime) -> None:
        stored = self.commits.get((commit.event_id, commit.round))
        if stored is not None and stored != commit:
            raise CommitMismatchError("stored commit differs")
        self.commits[(commit.event_id, commit.round)] = commit

    def emit(self, record: DecisionRecord) -> bool:
        self.emits += 1
        if record.event_id in self.records:
            return False
        self.records[record.event_id] = record
        return True


def settings(**council_updates: Any) -> EventSettings:
    council = static_config().council.model_copy(update=council_updates)
    return EventSettings(council=council)


def services(
    runner: ScriptedRunner,
    *,
    store: Any | None = None,
    sources: Sources | None = None,
    held: Side | None = None,
    council_updates: Mapping[str, Any] | None = None,
    params: Any | None = None,
    **extra: Any,
) -> CouncilServices:
    packets = [runner.packet(a) for a in MODEL_AGENTS]
    event_settings = settings(**(council_updates or {}))
    return CouncilServices(
        runner=runner,
        packets=Packets(packets),
        candidate_sets=CandidateSets(),
        sources=sources or Sources(),
        params=params if params is not None else Params(),
        held=Held(held),
        universe=Universe(),
        store=store if store is not None else MemoryDecisionStore(),
        settings=lambda _ids: event_settings,
        **extra,
    )


def event_input(
    cand: Candidate | None = None,
    *,
    event_id: str = "ev_test_1",
    unscored: bool = False,
    shadow_only: bool = False,
) -> dict[str, Any]:
    cand = cand or candidate()
    return {
        "event": {
            "event_id": event_id,
            "candidate": cand.model_dump(mode="json"),
            "config_version_ids": dict(VERSION_IDS),
            "unscored": unscored,
            "shadow_only": shadow_only,
        }
    }


async def run_meeting(
    services_: CouncilServices, *, event_id: str = "ev_test_1", **kwargs: Any
) -> DecisionRecord:
    from langgraph.checkpoint.memory import InMemorySaver

    app = CouncilGraph(services_).compile(InMemorySaver())
    config = {"configurable": {"thread_id": event_id}}
    await app.ainvoke(event_input(event_id=event_id, **kwargs), config, durability="sync")
    store = services_.store
    assert isinstance(store, MemoryDecisionStore)
    return store.records[event_id]
