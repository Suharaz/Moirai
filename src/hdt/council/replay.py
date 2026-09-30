"""Council replay: re-run recorded meetings offline and compare them with their decision cards.

    python -m hdt.council.replay event <event_id> [--out FILE]
    python -m hdt.council.replay range (--day YYYY-MM-DD | --start ISO --end ISO) [--out FILE]

One recorded event (`council_events` plus its `decision_cards`, `decision_forecasts`, `decision_claims` and
`lesson_shadow_forecasts` rows) is rebuilt from what the meeting pinned and recorded:
- the graph input of the original run (candidate, pinned config versions, unscored, shadow only);
- the universe entry (symbol, universe date) and the held side recorded on the card, the candidate set
  stored under the card's `candidate_set_sha256`, and the learned parameters known at the event's `as_of`;
- tools from the offline registry (`hdt.tools.replay`): `quant_core` serves the packet the agent's recorded
  forecast names (`quant_packets`, never recomputed or stored), the other tools read the lake and the stores
  point in time, and `fetch_source` opens only the captures recorded for rounds 1..N (`replay_context`);
- LLM calls from `llm_cache` only (`LlmRouter` mode `replay`): a miss raises `LlmCacheMissError`, reported as
  `cache_miss`, never a live call;
- the agent versions recorded on the forecasts (checked against `agent_versions`);
- the clock (`use_clock`) fixed at the recorded meeting start (the card's first timeline entry), so the lake
  view and every timestamp of the meeting see the time the original meeting saw.

A recorded abstain whose cause the recorded inputs cannot reproduce (no usable model reply, two replies
failing the schema, an internal error, the wall-clock tool budget, a role closed by its self-test, a
`quant_core` backend error) is returned verbatim; the agent still runs first (a cache miss is expected there),
so its tool session advances as the live one did. Every other forecast is the replayed one. Tool latency is
wall clock, not content: a replayed call identical to the recorded one at the same position (tool, input,
output, status) keeps the recorded latency.

The decision goes to an in-memory `ReplayDecisionStore`: nothing is written to Postgres (every session runs
with `default_transaction_read_only=on`), nothing is published to `decisions`, the checkpoint stays in memory.
The replayed card hash is compared with the recorded one and every differing card field, forecast row, claim
row and shadow forecast is reported.

`range` replays every done event whose card `as_of` lies in [start, end) (one UTC day with `--day`) and prints
a JSON report (also written to `--out`): per agent the run count, the parse errors (`llm_output_invalid`) and
their rate, the abstain reasons, mean |p_llm - p_model| and mean |z_llm - z_model|; the identical cards, the
differing events, the cache misses and the errors. Exit 0 when every card is identical, nothing missed the
cache or failed and every agent's parse error rate is below 1 %; 1 otherwise; 2 when nothing was replayed.

Database: `HDT_PG_DSN` of role `hdt_council`, the one role that reads the council tables and `llm_cache` as
well as the config versions, packets, candidate sets, news, scoring parameters and memory the meeting read;
the replay forces it read only per session. Lake: the configured staging and lake roots, read only.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from collections import Counter
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Final, Literal
from urllib.parse import quote

import sqlalchemy as sa
from langgraph.checkpoint.memory import InMemorySaver
from sqlalchemy.orm import Session, sessionmaker

from hdt.agents.llm_router import LlmRouter
from hdt.agents.llm_types import LlmCacheMissError
from hdt.agents.model_test import CLOSED_REASON
from hdt.agents.runner import (
    REASON_LLM_OUTPUT,
    REASON_LLM_UNAVAILABLE,
    REASON_PACKET,
    LlmAgentRunner,
    PgPins,
)
from hdt.agents.validate import logit
from hdt.agents.versions import AgentVersionBook, VersionKey
from hdt.contracts.candidate import CandidateSet
from hdt.contracts.common import AgentName, Side, TargetType
from hdt.contracts.forecast import AgentForecast, LakeRef, ToolCallRecord
from hdt.contracts.packet import QuantPacket
from hdt.core.clock import ManualClock, ensure_utc, use_clock, utcnow
from hdt.core.config import StaticConfig, require_env_value, static_config
from hdt.core.ids import canonical_json, canonical_sha256, to_canonical
from hdt.core.logging import configure_logging
from hdt.council.adapters import PgPacketStore, PinnedSettings
from hdt.council.commit import CommitMismatchError, RoundCommit
from hdt.council.graph import (
    CouncilGraph,
    CouncilServices,
    DecisionRecord,
    EventSettings,
    UniverseEntry,
    card_hash,
)
from hdt.council.ports import (
    AgentRequest,
    AgentResult,
    AgentRunner,
    CandidateSetSource,
    EventPins,
    PacketStore,
    ParamsSource,
    PoolParams,
    ShadowLessons,
    SourceLookup,
)
from hdt.db.models.decision import (
    CouncilEventRow,
    DecisionCardRow,
    DecisionClaimRow,
    DecisionForecastRow,
)
from hdt.db.models.scoring import LessonShadowForecastRow
from hdt.db.models.settings import AgentVersionRow
from hdt.db.session import make_engine, make_session_factory, normalize_dsn
from hdt.lake.pit_query import PitQuery
from hdt.memory.store import open_postgres_store
from hdt.news.assess import PgNewsAssessor
from hdt.news.market import LakeMarketView
from hdt.news.official import PgOfficialSource
from hdt.news.source_lookup import PgSourceLookup
from hdt.news.store import PgNewsIndex
from hdt.news.unlocks import PgUnlockCalendar
from hdt.quant.packet import load_candidate_set, load_packet
from hdt.scoring.params import PgParamsSource
from hdt.scoring.shadow import MemoryShadowLessons
from hdt.tools.budget import ABSTAIN_REASON as BUDGET_ABSTAIN_REASON
from hdt.tools.replay import build_replay_registry, replay_context

log = logging.getLogger(__name__)

SERVICE: Final[str] = "council-replay"
PARSE_ERROR_REASON: Final[str] = REASON_LLM_OUTPUT
"""The runner's abstain after two model replies failed the forecast schema."""
PARSE_ERROR_RATE_MAX: Final[float] = 0.01
"""Phase 05 success criterion: every agent's parse error rate stays below 1 %."""
TRANSIENT_ABSTAINS: Final[frozenset[str]] = frozenset(
    {
        REASON_LLM_UNAVAILABLE,
        REASON_LLM_OUTPUT,
        BUDGET_ABSTAIN_REASON,
        CLOSED_REASON,
        f"{REASON_PACKET}: error",
        f"{REASON_PACKET}: budget_exceeded",
    }
)
"""Abstain reasons the recorded inputs cannot reproduce (the model, the wall clock or a backend failed during
the live meeting): a recorded abstain with one of them is returned verbatim when its replay misses the cache
(or reproduces the same reason); a replay that gets further is a difference. `internal_error` is a code path,
deterministic under the recorded inputs, so it is replayed like any other forecast."""
READ_ONLY_OPTION: Final[str] = "-c default_transaction_read_only=on"
VALUE_CHARS_MAX: Final[int] = 200
"""A differing value longer than this (canonical JSON) is reported by its hash."""
ERROR_CHARS_MAX: Final[int] = 1000

Status = Literal["identical", "different", "cache_miss", "error"]


# ------------------------------------------------------------------------------------------- recorded event


def _started_at(card: Mapping[str, Any], fallback: datetime | None) -> datetime:
    """The recorded meeting start: the card's first timeline entry (written by `load_context`)."""
    timeline = card.get("timeline") or []
    if timeline:
        return ensure_utc(datetime.fromisoformat(str(timeline[0]["at"])))
    if fallback is None:
        raise ValueError(f"event {card.get('event_id')}: the card has no timeline and no start time")
    return ensure_utc(fallback)


@dataclass(frozen=True)
class RecordedEvent:
    """One recorded meeting: the graph input and the rows its `emit_decision` wrote (canonical JSON)."""

    event: Mapping[str, Any]
    """The graph input `state["event"]` of the original run."""
    card: Mapping[str, Any]
    """The stored `decision_cards` row (without `created_at`), `card_sha256` included."""
    forecasts: tuple[Mapping[str, Any], ...]
    """`decision_forecasts` rows (without `event_id`) in card order."""
    claims: tuple[Mapping[str, Any], ...]
    """`decision_claims` rows (without `event_id`) in card order."""
    shadow: Mapping[tuple[str, str], Mapping[str, Any]]
    """(agent, lesson_id) -> round-1 forecast made with a shadow lesson (`lesson_shadow_forecasts`)."""
    started_at: datetime
    """The clock of the replay: the recorded meeting start."""

    def __post_init__(self) -> None:
        if str(self.event.get("event_id")) != str(self.card.get("event_id")):
            raise ValueError("the recorded graph input and card belong to different events")

    @classmethod
    def from_record(cls, event: Mapping[str, Any], record: DecisionRecord) -> RecordedEvent:
        """The recorded event of a meeting that emitted `record` from the graph input `event`."""
        card = to_canonical(record.card)
        if not isinstance(card, dict):
            raise TypeError("a decision card is a JSON object")
        return cls(
            event=dict(event),
            card=card,
            forecasts=tuple(_mapping(to_canonical(row)) for row in record.forecasts),
            claims=tuple(_mapping(to_canonical(row)) for row in record.claims),
            shadow={
                (agent.value, lesson_id): _mapping(to_canonical(forecast))
                for agent, lesson_id, forecast in record.shadow
            },
            started_at=_started_at(card, None),
        )

    @property
    def event_id(self) -> str:
        return str(self.card["event_id"])

    @property
    def card_sha256(self) -> str:
        return str(self.card["card_sha256"])

    def submitted(self, agent: AgentName, round_: int) -> AgentForecast | None:
        """The forecast the agent's runner returned in `round_` (None: the agent did not play that round)."""
        for row in self.forecasts:
            if row["agent"] == AgentName(agent).value and int(row["round"]) == round_:
                return AgentForecast.model_validate(row["submitted"])
        return None

    def shadow_forecast(self, agent: AgentName, lesson_id: str) -> AgentForecast | None:
        found = self.shadow.get((AgentName(agent).value, lesson_id))
        return AgentForecast.model_validate(found) if found is not None else None

    def _own(self, agent: AgentName) -> list[AgentForecast]:
        rows = sorted(
            (r for r in self.forecasts if r["agent"] == AgentName(agent).value), key=lambda r: r["round"]
        )
        return [AgentForecast.model_validate(r["submitted"]) for r in rows]

    def pinned(self, event_id: str, agent: AgentName, round_: int) -> tuple[LakeRef, ...]:
        """`CouncilServices.pinned`: the agent's recorded captures of rounds 1..`round_`, plus those of its
        recorded round-1 A/B runs with a shadow lesson (the shadow run reuses the round-1 request, pins
        included); a round the original meeting never played raises `LookupError` (the replay diverged)."""
        if event_id != self.event_id:
            raise LookupError(f"replay of event {self.event_id} asked for the pins of event {event_id}")
        try:
            main = replay_context(AgentName(agent), self._own(agent), round_=round_).pinned
        except ValueError as exc:
            raise LookupError(f"event {event_id}: {exc}") from exc
        name = AgentName(agent).value
        shadow = (
            ref
            for (who, _lesson), row in sorted(self.shadow.items())
            if who == name
            for ref in AgentForecast.model_validate(row).capture_manifest
        )
        return tuple(dict.fromkeys((*main, *shadow)))

    def packet_sha256(self, agent: AgentName) -> str | None:
        """The quant packet the agent's recorded forecasts name (None: it had none)."""
        return next((f.packet_sha256 for f in self._own(agent) if f.packet_sha256 is not None), None)

    def agent_version(self, agent: AgentName) -> int | None:
        """The registered agent version the agent ran with (None: none recorded or unregistered)."""
        for forecast in self._own(agent):
            if forecast.agent_version.isdecimal() and int(forecast.agent_version) > 0:
                return int(forecast.agent_version)
        return None


def _mapping(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise TypeError("a recorded row is a JSON object")
    return value


# ------------------------------------------------------------------------------------------- recorded ports


class RecordedHeld:
    """`HeldPositions` of a replay: the held side recorded on the card (the live one read Redis)."""

    def __init__(self, recorded: RecordedEvent) -> None:
        self._coin_id = int(recorded.card["coin_id"])
        self._side = recorded.card.get("held_side")

    async def held_side(self, coin_id: int, as_of: datetime) -> Side | None:
        if coin_id != self._coin_id or not self._side:
            return None
        return Side(self._side)


class RecordedUniverse:
    """`UniverseLookup` of a replay: the symbol and universe date recorded on the card."""

    def __init__(self, recorded: RecordedEvent) -> None:
        self._coin_id = int(recorded.card["coin_id"])
        self._entry = UniverseEntry(
            symbol=str(recorded.card["symbol"]),
            universe_date=date.fromisoformat(str(recorded.card["universe_date"])),
        )

    def entry(self, coin_id: int, as_of: datetime) -> UniverseEntry | None:
        return self._entry if coin_id == self._coin_id else None


class RecordedParams:
    """`ParamsSource` of a replay: the parameter version and pins (stacking model, agent-version overlays,
    claims-audit flags) recorded on the card, so `load_context` resolves what the live meeting resolved
    instead of what is newest at `as_of` now. A card without them (older records) loads as asked."""

    def __init__(self, inner: ParamsSource, recorded: RecordedEvent) -> None:
        self._inner = inner
        params = recorded.card.get("params")
        params = params if isinstance(params, Mapping) else {}
        version = params.get("params_version")
        self._version = int(version) if isinstance(version, int) and not isinstance(version, bool) else None
        pins = params.get("pins")
        self._pins: Mapping[str, Any] | None = pins if isinstance(pins, Mapping) else None

    def load(
        self,
        target_type: TargetType,
        label_spec_version: str,
        *,
        at: datetime | None = None,
        params_version: int | None = None,
        pins: Mapping[str, Any] | None = None,
    ) -> PoolParams:
        return self._inner.load(
            target_type,
            label_spec_version,
            at=at,
            params_version=params_version if params_version is not None else self._version,
            pins=pins if pins is not None else self._pins,
        )

    def regime(self, packets: Mapping[str, QuantPacket]) -> str | None:
        return self._inner.regime(packets)


class ReplayDecisionStore:
    """`DecisionStore` of a replay: the blind commit and the decision stay in memory. It never writes
    Postgres, the `decision_outbox` or episodic memory, and nothing is published."""

    def __init__(self) -> None:
        self.commit: RoundCommit | None = None
        self.record: DecisionRecord | None = None

    def commit_round(self, commit: RoundCommit, at: datetime) -> None:
        if self.commit is not None and self.commit != commit:
            raise CommitMismatchError(f"event {commit.event_id}: the replay committed two different round-1s")
        self.commit = commit

    def emit(self, record: DecisionRecord) -> bool:
        if self.record is not None:
            return False
        self.record = record
        return True


@dataclass(frozen=True)
class AgentRun:
    """One agent call of a replay."""

    agent: str
    round: int
    lesson_id: str | None
    """Set for a round-1 A/B run with a shadow lesson (never pooled; left out of the agent statistics)."""
    forecast: AgentForecast
    verbatim: bool
    """A recorded transient abstain returned as recorded."""
    replayed: str | None
    """Verbatim runs: what the replay itself produced (`cache_miss`, its abstain reason, or `forecast`)."""

    def to_json(self) -> dict[str, Any]:
        f = self.forecast
        return {
            "agent": self.agent,
            "round": self.round,
            "lesson_id": self.lesson_id,
            "abstain": f.abstain,
            "abstain_reason": f.abstain_reason,
            "p_model": f.p_model,
            "p_llm": f.p_llm,
            "p_used": f.p_used,
            "verbatim": self.verbatim,
            "replayed": self.replayed,
        }

    def divergence(self) -> Difference | None:
        """A verbatim run whose replay did not stop where the live one did (it forecast, or abstained for
        another reason): the recorded abstain does not stand for what the recorded inputs produce."""
        if not self.verbatim or self.replayed in ("cache_miss", self.forecast.abstain_reason):
            return None
        key = (
            f"forecast.{self.agent}.{self.round}.replayed"
            if self.lesson_id is None
            else f"shadow.{self.agent}.{self.lesson_id}.replayed"
        )
        return Difference(key, self.forecast.abstain_reason, self.replayed)


def _call_identity(call: ToolCallRecord) -> tuple[str, str, str | None, str]:
    return (call.tool, call.input_sha256, call.output_sha256, call.status)


def with_recorded_latency(forecast: AgentForecast, recorded: AgentForecast | None) -> AgentForecast:
    """`forecast` with the recorded tool latencies when its calls are the recorded calls, in order."""
    if recorded is None or len(forecast.tool_calls) != len(recorded.tool_calls):
        return forecast
    same = all(
        _call_identity(a) == _call_identity(b)
        for a, b in zip(forecast.tool_calls, recorded.tool_calls, strict=True)
    )
    if not same or forecast.tool_calls == recorded.tool_calls:
        return forecast
    return forecast.model_copy(update={"tool_calls": recorded.tool_calls})


class ReplayAgentRunner:
    """Wraps the replay-mode runner: returns recorded transient abstains verbatim, keeps recorded tool latency
    for identical calls and records every call. A cache miss anywhere else propagates (a hard error)."""

    def __init__(self, inner: AgentRunner, recorded: RecordedEvent) -> None:
        self.inner = inner
        self.recorded = recorded
        self.runs: list[AgentRun] = []

    async def forecast(self, request: AgentRequest) -> AgentResult:
        agent = AgentName(request.agent)
        lesson_id = request.shadow_lesson.lesson_id if request.shadow_lesson is not None else None
        recorded = (
            self.recorded.submitted(agent, request.round)
            if lesson_id is None
            else self.recorded.shadow_forecast(agent, lesson_id)
        )
        if recorded is not None and recorded.abstain and recorded.abstain_reason in TRANSIENT_ABSTAINS:
            try:
                replayed = (await self.inner.forecast(request)).forecast
                outcome = replayed.abstain_reason if replayed.abstain else "forecast"
            except LlmCacheMissError:
                outcome = "cache_miss"
            self.runs.append(AgentRun(agent.value, request.round, lesson_id, recorded, True, outcome))
            return AgentResult(forecast=recorded, captures=recorded.capture_manifest)
        result = await self.inner.forecast(request)
        forecast = with_recorded_latency(result.forecast, recorded)
        self.runs.append(AgentRun(agent.value, request.round, lesson_id, forecast, False, None))
        return replace(result, forecast=forecast) if forecast is not result.forecast else result


# ---------------------------------------------------------------------------------------------- comparison


@dataclass(frozen=True)
class Difference:
    key: str
    """`card.<field>`, `forecast.<agent>.<round>[.<column>[.<field>]]`, `claim.<round>.<shared_id>[...]`,
    `shadow.<agent>.<lesson_id>[.<field>]`, or `recorded.card_sha256` (the stored rows do not match their
    stored hash)."""
    recorded: Any
    replayed: Any

    def to_json(self) -> dict[str, Any]:
        return {"key": self.key, "recorded": self.recorded, "replayed": self.replayed}


def _short(value: Any) -> Any:
    """A differing value as reported: canonical JSON, or its hash when long."""
    canonical = to_canonical(value)
    if len(canonical_json(canonical)) > VALUE_CHARS_MAX:
        return "sha256:" + canonical_sha256(canonical)[:16]
    return canonical


def _diff_mapping(prefix: str, recorded: Any, replayed: Any, out: list[Difference], *, depth: int) -> None:
    a, b = to_canonical(recorded), to_canonical(replayed)
    if a == b:
        return
    if depth > 0 and isinstance(a, dict) and isinstance(b, dict):
        for key in sorted(set(a) | set(b)):
            _diff_mapping(f"{prefix}.{key}", a.get(key), b.get(key), out, depth=depth - 1)
        return
    out.append(Difference(prefix, _short(a), _short(b)))


def _without_at(timeline: Any) -> list[Any]:
    return [{k: v for k, v in e.items() if k != "at"} for e in (timeline or [])]


def _rows_by(rows: Sequence[Mapping[str, Any]], *keys: str) -> dict[str, Mapping[str, Any]]:
    return {".".join(str(row[k]) for k in keys): row for row in rows}


def compare(recorded: RecordedEvent, record: DecisionRecord) -> list[Difference]:
    """Every difference between the recorded meeting and the replayed decision (empty: identical)."""
    out: list[Difference] = []
    rows_hash = card_hash(recorded.card, recorded.forecasts, recorded.claims)
    if rows_hash != recorded.card_sha256:
        out.append(Difference("recorded.card_sha256", recorded.card_sha256, rows_hash))
    replayed_card = to_canonical(record.card)
    if not isinstance(replayed_card, dict):
        raise TypeError("a decision card is a JSON object")
    if replayed_card["card_sha256"] != recorded.card_sha256:
        out.append(Difference("card.card_sha256", recorded.card_sha256, replayed_card["card_sha256"]))
    for key in sorted((set(recorded.card) | set(replayed_card)) - {"card_sha256", "timeline"}):
        _diff_mapping(f"card.{key}", recorded.card.get(key), replayed_card.get(key), out, depth=0)
    _diff_mapping(
        "card.timeline",
        _without_at(recorded.card.get("timeline")),
        _without_at(replayed_card.get("timeline")),
        out,
        depth=0,
    )
    tables: tuple[
        tuple[str, Sequence[Mapping[str, Any]], Sequence[Mapping[str, Any]], tuple[str, ...]], ...
    ] = (
        ("forecast", recorded.forecasts, record.forecasts, ("agent", "round")),
        ("claim", recorded.claims, record.claims, ("round", "shared_id")),
    )
    for name, before, after, keys in tables:
        old, new = _rows_by(before, *keys), _rows_by(after, *keys)
        for key in sorted(set(old) | set(new)):
            _diff_mapping(f"{name}.{key}", old.get(key), new.get(key), out, depth=2)
    shadow = {f"{a.value}.{lesson}": to_canonical(f) for a, lesson, f in record.shadow}
    recorded_shadow = {f"{a}.{lesson}": f for (a, lesson), f in recorded.shadow.items()}
    for key in sorted(set(recorded_shadow) | set(shadow)):
        _diff_mapping(f"shadow.{key}", recorded_shadow.get(key), shadow.get(key), out, depth=1)
    return out


# ---------------------------------------------------------------------------------------------- one event


@dataclass(frozen=True)
class ReplayServices:
    """The ports a replay runs with (held side, universe, pins and decision store come from the record)."""

    runner: AgentRunner
    """A replay-mode runner: tools from the offline registry, LLM replies from the cache only."""
    packets: PacketStore
    candidate_sets: CandidateSetSource
    sources: SourceLookup
    params: ParamsSource
    settings: Callable[[Mapping[str, int]], EventSettings]
    lessons: ShadowLessons | None = None
    bind: Callable[[RecordedEvent], None] | None = None
    """Called before each event (the recorded packets, candidate set and agent versions it serves)."""
    close_event: Callable[[str], None] | None = None
    """Called before and after each event: the runner forgets the event's tool sessions."""


@dataclass(frozen=True)
class EventReplay:
    event_id: str
    status: Status
    recorded_card_sha256: str
    replayed_card_sha256: str | None
    differences: tuple[Difference, ...]
    runs: tuple[AgentRun, ...]
    error: str | None = None
    record: DecisionRecord | None = field(default=None, compare=False, repr=False)
    """The replayed decision (kept in memory only)."""

    def to_json(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "status": self.status,
            "recorded_card_sha256": self.recorded_card_sha256,
            "replayed_card_sha256": self.replayed_card_sha256,
            "differences": [d.to_json() for d in self.differences],
            "error": self.error,
            "runs": [r.to_json() for r in self.runs],
        }


async def replay_event(recorded: RecordedEvent, services: ReplayServices) -> EventReplay:
    """Re-run one recorded meeting offline and compare it with its record."""
    store = ReplayDecisionStore()
    runner = ReplayAgentRunner(services.runner, recorded)
    council = CouncilServices(
        runner=runner,
        packets=services.packets,
        candidate_sets=services.candidate_sets,
        sources=services.sources,
        params=RecordedParams(services.params, recorded),
        held=RecordedHeld(recorded),
        universe=RecordedUniverse(recorded),
        store=store,
        settings=services.settings,
        mode="replay",
        lessons=services.lessons,
        pinned=recorded.pinned,
    )
    app = CouncilGraph(council).compile(InMemorySaver())
    config = {"configurable": {"thread_id": f"replay:{recorded.event_id}"}}
    status: Status | None = None
    error: str | None = None
    if services.bind is not None:
        services.bind(recorded)
    if services.close_event is not None:
        services.close_event(recorded.event_id)
    try:
        with use_clock(ManualClock(recorded.started_at)):
            await app.ainvoke({"event": dict(recorded.event)}, config, durability="sync")
    except LlmCacheMissError as exc:
        status, error = "cache_miss", str(exc)[:ERROR_CHARS_MAX]
    except Exception as exc:
        log.exception("replay failed", extra={"event_id": recorded.event_id})
        status, error = "error", f"{type(exc).__name__}: {exc}"[:ERROR_CHARS_MAX]
    finally:
        if services.close_event is not None:
            services.close_event(recorded.event_id)
    record = store.record
    if status is None and record is None:
        status, error = "error", "the replayed meeting emitted no decision"
    differences: list[Difference] = []
    if status is None and record is not None:
        differences = compare(recorded, record)
        differences.extend(d for run in runner.runs if (d := run.divergence()) is not None)
        status = "different" if differences else "identical"
    assert status is not None
    return EventReplay(
        event_id=recorded.event_id,
        status=status,
        recorded_card_sha256=recorded.card_sha256,
        replayed_card_sha256=str(record.card["card_sha256"]) if record is not None else None,
        differences=tuple(differences),
        runs=tuple(runner.runs),
        error=error,
        record=record,
    )


# ---------------------------------------------------------------------------------------------- report


@dataclass
class AgentStats:
    """One agent over the replayed events (main runs; A/B runs with a shadow lesson are left out)."""

    runs: int = 0
    parse_errors: int = 0
    verbatim: int = 0
    abstains: Counter[str] = field(default_factory=Counter)
    abs_dp: list[float] = field(default_factory=list)
    abs_dz: list[float] = field(default_factory=list)

    def add(self, run: AgentRun) -> None:
        f = run.forecast
        self.runs += 1
        self.verbatim += int(run.verbatim)
        if f.abstain:
            reason = f.abstain_reason or "unspecified"
            self.abstains[reason] += 1
            self.parse_errors += int(reason == PARSE_ERROR_REASON)
            return
        if f.p_llm is not None and f.p_model is not None:
            self.abs_dp.append(abs(f.p_llm - f.p_model))
            self.abs_dz.append(abs(logit(f.p_llm) - logit(f.p_model)))

    @property
    def parse_error_rate(self) -> float:
        return self.parse_errors / self.runs if self.runs else 0.0

    def to_json(self) -> dict[str, Any]:
        return {
            "runs": self.runs,
            "parse_errors": self.parse_errors,
            "parse_error_rate": self.parse_error_rate,
            "abstain_reasons": dict(sorted(self.abstains.items())),
            "verbatim_abstains": self.verbatim,
            "llm_forecasts": len(self.abs_dp),
            "mean_abs_p_llm_minus_p_model": _mean(self.abs_dp),
            "mean_abs_z_llm_minus_z_model": _mean(self.abs_dz),
        }


def _mean(values: Sequence[float]) -> float | None:
    return sum(values) / len(values) if values else None


@dataclass(frozen=True)
class ReplayReport:
    events: tuple[EventReplay, ...]
    start: datetime | None = None
    end: datetime | None = None

    def agents(self) -> dict[str, AgentStats]:
        stats: dict[str, AgentStats] = {}
        for event in self.events:
            for run in event.runs:
                if run.lesson_id is None:
                    stats.setdefault(run.agent, AgentStats()).add(run)
        return dict(sorted(stats.items()))

    def _with(self, status: Status) -> list[EventReplay]:
        return [e for e in self.events if e.status == status]

    def parse_error_rate_exceeded(self) -> list[str]:
        return [a for a, s in self.agents().items() if s.parse_error_rate >= PARSE_ERROR_RATE_MAX]

    @property
    def passed(self) -> bool:
        return (
            bool(self.events)
            and all(e.status == "identical" for e in self.events)
            and not self.parse_error_rate_exceeded()
        )

    def exit_code(self) -> int:
        if not self.events:
            return 2
        return 0 if self.passed else 1

    def to_json(self) -> dict[str, Any]:
        return {
            "start": to_canonical(self.start),
            "end": to_canonical(self.end),
            "events": len(self.events),
            "identical": len(self._with("identical")),
            "different": [e.event_id for e in self._with("different")],
            "cache_misses": [{"event_id": e.event_id, "error": e.error} for e in self._with("cache_miss")],
            "errors": [{"event_id": e.event_id, "error": e.error} for e in self._with("error")],
            "parse_error_rate_max": PARSE_ERROR_RATE_MAX,
            "parse_error_rate_exceeded": self.parse_error_rate_exceeded(),
            "agents": {a: s.to_json() for a, s in self.agents().items()},
            "passed": self.passed,
            "replays": [e.to_json() for e in self.events],
        }


# ---------------------------------------------------------------------------------------------- Postgres


def read_only_dsn(dsn: str) -> str:
    """`dsn` with every session read only (libpq `options`, percent-encoded so that both SQLAlchemy and libpq
    read it)."""
    url = sa.engine.make_url(normalize_dsn(dsn))
    if "options" in url.query:
        raise ValueError("the replay DSN must not carry its own libpq options")
    base = url.render_as_string(hide_password=False)
    return f"{base}{'&' if '?' in base else '?'}options={quote(READ_ONLY_OPTION, safe='')}"


def _row_dict(row: Any, *, drop: tuple[str, ...]) -> dict[str, Any]:
    return _mapping(to_canonical({k: v for k, v in row._mapping.items() if k not in drop}))


class NothingToReplayError(LookupError):
    """The recorded event, or its decision card, does not exist (the CLI's exit 2)."""


def load_recorded(conn: sa.Connection, event_id: str) -> RecordedEvent:
    """The recorded meeting of `event_id`; `NothingToReplayError` when the event or its card is missing."""
    e = CouncilEventRow
    event = conn.execute(
        sa.select(
            e.event_id, e.candidate, e.config_version_ids, e.unscored, e.shadow_only, e.created_at
        ).where(e.event_id == event_id)
    ).one_or_none()
    if event is None:
        raise NothingToReplayError(f"no council event {event_id}")
    card_row = conn.execute(
        sa.select(DecisionCardRow.__table__).where(DecisionCardRow.event_id == event_id)
    ).one_or_none()
    if card_row is None:
        raise NothingToReplayError(f"council event {event_id} has no decision card")
    card = _row_dict(card_row, drop=("created_at",))
    forecasts = [
        _row_dict(row, drop=("event_id",))
        for row in conn.execute(
            sa.select(DecisionForecastRow.__table__).where(DecisionForecastRow.event_id == event_id)
        )
    ]
    forecasts.sort(key=lambda r: (int(r["round"]), f"{r['round']}:{r['agent']}"))
    claims = [
        _row_dict(row, drop=("event_id",))
        for row in conn.execute(
            sa.select(DecisionClaimRow.__table__).where(DecisionClaimRow.event_id == event_id)
        )
    ]
    claims.sort(key=lambda r: (int(r["round"]), str(r["shared_id"])))
    s = LessonShadowForecastRow
    shadow = {
        (str(row.agent), str(row.lesson_id)): _mapping(row.forecast)
        for row in conn.execute(sa.select(s.agent, s.lesson_id, s.forecast).where(s.event_id == event_id))
    }
    return RecordedEvent(
        event={
            "event_id": event.event_id,
            "candidate": dict(event.candidate),
            "config_version_ids": {str(k): int(v) for k, v in event.config_version_ids.items()},
            "unscored": bool(event.unscored),
            "shadow_only": bool(event.shadow_only),
        },
        card=card,
        forecasts=tuple(forecasts),
        claims=tuple(claims),
        shadow=shadow,
        started_at=_started_at(card, event.created_at),
    )


def recorded_event_ids(conn: sa.Connection, start: datetime, end: datetime) -> list[str]:
    """The done events with a decision card whose `as_of` lies in [start, end), oldest first."""
    e, c = CouncilEventRow, DecisionCardRow
    rows = conn.execute(
        sa.select(e.event_id)
        .join(c, c.event_id == e.event_id)
        .where(e.status == "done", c.as_of >= ensure_utc(start), c.as_of < ensure_utc(end))
        .order_by(c.as_of, e.event_id)
    )
    return [str(r.event_id) for r in rows]


class ReplayBinding:
    """The recorded event being replayed; the replay runs one event at a time."""

    def __init__(self) -> None:
        self._event: RecordedEvent | None = None

    def bind(self, recorded: RecordedEvent) -> None:
        self._event = recorded

    @property
    def event(self) -> RecordedEvent:
        if self._event is None:
            raise LookupError("no recorded event is bound to the replay")
        return self._event


class RecordedPackets:
    """`QuantCoreFn` of a replay: the stored packet the agent's recorded forecast names (never recomputed)."""

    def __init__(self, engine: sa.Engine, binding: ReplayBinding) -> None:
        self.engine = engine
        self.binding = binding

    def __call__(self, agent: AgentName, coin_id: int, as_of: datetime) -> QuantPacket:
        event = self.binding.event
        candidate = event.event["candidate"]
        if coin_id != int(candidate["coin_id"]) or ensure_utc(as_of) != datetime.fromisoformat(
            str(candidate["as_of"])
        ):
            raise LookupError(f"the replay of event {event.event_id} has no packet for coin {coin_id}")
        sha = event.packet_sha256(agent)
        if sha is None:
            raise LookupError(
                f"event {event.event_id}: no quant packet was recorded for {AgentName(agent).value}"
            )
        with self.engine.connect() as conn:
            packet = load_packet(conn, sha)
        if packet is None:
            raise LookupError(f"event {event.event_id}: recorded packet {sha[:16]} is not stored")
        return packet


class RecordedCandidateSets:
    """`CandidateSetSource` of a replay: the candidate set stored under the card's `candidate_set_sha256`."""

    def __init__(self, engine: sa.Engine, binding: ReplayBinding) -> None:
        self.engine = engine
        self.binding = binding

    def candidate_set(self, coin_id: int, as_of: datetime, pins: EventPins) -> CandidateSet:
        event = self.binding.event
        sha = event.card.get("candidate_set_sha256")
        if not sha:
            raise LookupError(f"event {event.event_id}: the card records no candidate set")
        with self.engine.connect() as conn:
            found = load_candidate_set(conn, str(sha))
        if found is None or found.coin_id != coin_id or found.as_of != ensure_utc(as_of):
            raise LookupError(
                f"event {event.event_id}: candidate set {str(sha)[:16]} is not stored for the event"
            )
        return found


class RecordedVersions:
    """`VersionSource` of a replay: the agent version the meeting recorded when its `agent_versions` row
    matches the replayed configuration, otherwise the newest matching version (never written)."""

    def __init__(self, session_factory: sessionmaker[Session], binding: ReplayBinding) -> None:
        self.session_factory = session_factory
        self.binding = binding
        self.book = AgentVersionBook(session_factory, write=False)

    def version(self, key: VersionKey, *, config_version_id: int | None) -> int:
        recorded = self.binding.event.agent_version(key.agent)
        if recorded is not None:
            with self.session_factory() as session:
                row = session.get(AgentVersionRow, (key.agent.value, recorded))
            if row is not None and key.matches(row):
                return recorded
        return self.book.version(key, config_version_id=config_version_id)


@dataclass(frozen=True)
class PgReplay:
    """Replays recorded events of a Postgres database (read only) and the configured lake."""

    engine: sa.Engine
    services: ReplayServices

    def load(self, event_id: str) -> RecordedEvent:
        with self.engine.connect() as conn:
            return load_recorded(conn, event_id)

    async def event(self, event_id: str) -> EventReplay:
        return await replay_event(await asyncio.to_thread(self.load, event_id), self.services)

    async def range(self, start: datetime, end: datetime) -> ReplayReport:
        """Every recorded event of [start, end); an event that cannot even be loaded is reported as an
        `error` of its own, the other events are still replayed."""
        with self.engine.connect() as conn:
            event_ids = recorded_event_ids(conn, start, end)
        replays: list[EventReplay] = []
        for event_id in event_ids:
            try:
                replays.append(await self.event(event_id))
            except Exception as exc:
                log.exception("replay of one event failed", extra={"event_id": event_id})
                replays.append(_failed_replay(event_id, exc))
        return ReplayReport(tuple(replays), start=ensure_utc(start), end=ensure_utc(end))


def _failed_replay(event_id: str, exc: Exception) -> EventReplay:
    return EventReplay(
        event_id=event_id,
        status="error",
        recorded_card_sha256="",
        replayed_card_sha256=None,
        differences=(),
        runs=(),
        error=f"{type(exc).__name__}: {exc}"[:ERROR_CHARS_MAX],
    )


@contextmanager
def open_pg_replay(
    dsn: str, *, static: StaticConfig | None = None, pit: PitQuery | None = None
) -> Iterator[PgReplay]:
    """The production replay wiring over `dsn` (forced read only) and the lake (`pit`, default: the
    configured roots)."""
    static = static or static_config()
    data = static.settings.data
    pit = pit or PitQuery(Path(data.staging_root), Path(data.lake_root))
    ro_dsn = read_only_dsn(dsn)
    engine = make_engine(ro_dsn)
    sessions = make_session_factory(engine)
    binding = ReplayBinding()
    try:
        with open_postgres_store(ro_dsn) as memory:
            registry = build_replay_registry(
                pit=pit,
                stale_s=data.stale_s,
                news_sources=static.news_sources,
                quant_core=RecordedPackets(engine, binding),
                news=PgNewsIndex(engine),
                official=PgOfficialSource(engine),
                unlocks=PgUnlockCalendar(engine),
                memory=memory,
            )
            runner = LlmAgentRunner(
                llm=LlmRouter(mode="replay", session_factory=sessions, api_key=None),
                registry=registry,
                pins=PgPins(sessions),
                memory=memory,
                versions=RecordedVersions(sessions, binding),
                health=None,
                news=PgNewsAssessor(engine, LakeMarketView(pit, static=static)),
                static=static,
            )
            yield PgReplay(
                engine=engine,
                services=ReplayServices(
                    runner=runner,
                    packets=PgPacketStore(engine),
                    candidate_sets=RecordedCandidateSets(engine, binding),
                    sources=PgSourceLookup(engine, pit, static.news_sources),
                    params=PgParamsSource(engine, council=static.council),
                    settings=PinnedSettings(sessions, static),
                    lessons=MemoryShadowLessons(memory),
                    bind=binding.bind,
                    close_event=runner.close_event,
                ),
            )
    finally:
        engine.dispose()


# ---------------------------------------------------------------------------------------------- CLI


def _utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError(f"{value!r} has no UTC offset")
    return ensure_utc(parsed)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m hdt.council.replay",
        description="Replay recorded council meetings offline (read only) and compare their decision cards.",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    one = sub.add_parser("event", help="replay one recorded event")
    one.add_argument("event_id")
    one.add_argument("--out", type=Path, help="also write the JSON report to this file")
    many = sub.add_parser("range", help="replay every recorded event of a UTC day or [start, end)")
    when = many.add_mutually_exclusive_group(required=True)
    when.add_argument("--day", type=date.fromisoformat, help="UTC day, YYYY-MM-DD")
    when.add_argument("--start", type=_utc, help="ISO timestamp with offset (needs --end)")
    many.add_argument("--end", type=_utc, help="ISO timestamp with offset, exclusive")
    many.add_argument("--out", type=Path, help="also write the JSON report to this file")
    return parser


def _window(parser: argparse.ArgumentParser, args: argparse.Namespace) -> tuple[datetime, datetime]:
    if args.day is not None:
        if args.end is not None:
            parser.error("--end goes with --start, not with --day")
        start = datetime(args.day.year, args.day.month, args.day.day, tzinfo=UTC)
        return start, start + timedelta(days=1)
    if args.end is None:
        parser.error("--start needs --end")
    if args.end <= args.start:
        parser.error("--end must be after --start")
    return args.start, args.end


async def _run(args: argparse.Namespace, window: tuple[datetime, datetime] | None) -> ReplayReport:
    with open_pg_replay(require_env_value("HDT_PG_DSN")) as replay:
        if window is None:
            return ReplayReport((await replay.event(args.event_id),))
        return await replay.range(*window)


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    window = _window(parser, args) if args.command == "range" else None
    configure_logging(SERVICE, stream=sys.stderr)
    try:
        report = asyncio.run(_run(args, window))
    except NothingToReplayError as exc:
        print(f"nothing to replay: {exc}", file=sys.stderr)
        return 2
    payload = json.dumps(
        {"generated_at": to_canonical(utcnow()), **report.to_json()}, indent=2, sort_keys=True
    )
    print(payload)
    if args.out is not None:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(payload + "\n", encoding="utf-8")
    return report.exit_code()


if __name__ == "__main__":
    sys.exit(main())
