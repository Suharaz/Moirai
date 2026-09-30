"""Episodic memory: decision episodes, their outcomes, and known news events (namespace `(agent, episodic)`).

Writers (enforced again by row-level security on the `store` table):
- `record_decision`: the phase 06 `emit_decision` node (role `hdt_council`), one episode per agent per
  event, `known_at` = when the decision was emitted (never before the event `as_of`);
- `record_outcome`: the phase 08 resolver (role `hdt_scorer`), a separate record whose
  `known_at = as_of + horizon`, so an outcome is invisible to any event before it resolved;
- `record_known_event`: the phase 07 news worker (role `hdt_news`), News namespace only, deduplicated by
  exact `event_key` and by MinHash similarity of the title among known events of the same coins.

Readers are bound to one agent and one `as_of`: they return only records with `known_at < as_of`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Final, Literal, Self

from pydantic import Field, PositiveInt, model_validator

from hdt.contracts.common import (
    AgentName,
    CandidateSource,
    ContractModel,
    Intent,
    Side,
    TargetType,
    Tier,
    UtcDatetime,
)
from hdt.contracts.forecast import ToolCallRecord
from hdt.core.clock import ensure_utc
from hdt.memory.dedupe import DuplicateIndex
from hdt.memory.store import MemoryRecord, MemoryStore, memory_namespace
from hdt.tools.base import ShortText, Text

EVENT_KEY_PATTERN: Final[str] = r"^[a-z0-9_]{1,48}:[A-Za-z0-9_.:-]{1,80}$"
KNOWN_EVENT_DEDUPE_WINDOW: Final[timedelta] = timedelta(days=7)
DEFAULT_EPISODE_LOOKBACK: Final[timedelta] = timedelta(days=30)
_SHA256: Final[str] = r"^[0-9a-f]{64}$"


class DecisionEpisode(MemoryRecord):
    """One agent's part in one council decision, as emitted."""

    record_type: Literal["decision"] = "decision"
    event_id: str = Field(min_length=1, max_length=128)
    agent: AgentName
    agent_version: str = Field(min_length=1, max_length=64)
    coin_id: PositiveInt
    as_of: UtcDatetime
    source: CandidateSource
    target_type: TargetType
    council_intent: Intent | None = Field(description="None when the council decided not to trade")
    council_side: Side | None = None
    abstain: bool
    p_used: float | None = Field(default=None, gt=0, lt=1)
    candidate_id: str | None = Field(default=None, max_length=64)
    summary: Text = Field(default="", description="the agent's own reason, sanitized")
    packet_sha256: str | None = Field(default=None, pattern=_SHA256)
    skill_commit: str | None = Field(default=None, pattern=r"^[0-9a-f]{40}$")
    tool_calls: tuple[ToolCallRecord, ...] = Field(default=(), description="the agent's tool calls, in order")

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.known_at < self.as_of:
            raise ValueError("a decision cannot be known before its as_of")
        if self.abstain == (self.p_used is not None):
            raise ValueError("p_used is required unless the agent abstained")
        if self.council_intent is None and self.council_side is not None:
            raise ValueError("council_side requires a council intent")
        if self.council_intent == Intent.OPEN and self.council_side is None:
            raise ValueError("an OPEN decision needs council_side")
        return self


class OutcomeRecord(MemoryRecord):
    """The resolved outcome of an episode; known exactly `horizon_h` after the decision's `as_of`."""

    record_type: Literal["outcome"] = "outcome"
    event_id: str = Field(min_length=1, max_length=128)
    agent: AgentName
    coin_id: PositiveInt
    as_of: UtcDatetime
    horizon_h: PositiveInt
    target_type: TargetType
    label: Literal[0, 1] | None = Field(description="1 = up over the horizon, None = not resolvable")
    realized_return: float | None = None
    hit: bool | None = Field(default=None, description="the agent's stance matched the label")
    log_loss: float | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.known_at != self.as_of + timedelta(hours=self.horizon_h):
            raise ValueError("an outcome is known exactly at as_of + horizon")
        return self

    @classmethod
    def resolved(cls, *, as_of: datetime, horizon_h: int, **fields: object) -> OutcomeRecord:
        as_of = ensure_utc(as_of)
        return cls.model_validate(
            {**fields, "as_of": as_of, "horizon_h": horizon_h, "known_at": as_of + timedelta(hours=horizon_h)}
        )


class KnownEvent(MemoryRecord):
    """A news event the News agent already knows about, identified by `event_key`."""

    record_type: Literal["known_event"] = "known_event"
    event_key: str = Field(pattern=EVENT_KEY_PATTERN)
    coin_ids: tuple[PositiveInt, ...] = Field(min_length=1, max_length=20)
    title: ShortText = Field(min_length=1)
    tier: Tier
    item_ids: tuple[str, ...] = Field(default=(), max_length=20)


class Episode(ContractModel):
    decision: DecisionEpisode
    outcome: OutcomeRecord | None
    """None while the outcome was not yet known at the reader's `as_of`."""


@dataclass(frozen=True)
class KnownEventWrite:
    status: Literal["new", "exact", "near"]
    key: str
    """Key of the written record, or of the known event this one duplicates."""
    similarity: float = 1.0


def decision_key(event_id: str) -> str:
    return f"decision:{event_id}"


def outcome_key(event_id: str) -> str:
    return f"outcome:{event_id}"


def known_event_key(event_key: str) -> str:
    return f"known_event:{event_key}"


class EpisodicWriter:
    def __init__(self, memory: MemoryStore) -> None:
        self._memory = memory

    def record_decision(self, episode: DecisionEpisode) -> bool:
        ns = memory_namespace(episode.agent, "episodic")
        return self._memory.append(ns, decision_key(episode.event_id), episode)

    def record_outcome(self, outcome: OutcomeRecord) -> bool:
        ns = memory_namespace(outcome.agent, "episodic")
        return self._memory.append(ns, outcome_key(outcome.event_id), outcome)

    def record_known_event(
        self, event: KnownEvent, *, window: timedelta = KNOWN_EVENT_DEDUPE_WINDOW
    ) -> KnownEventWrite:
        """Write `event` unless its key, or a near-identical title of the same coins, is already known."""
        ns = memory_namespace(AgentName.NEWS, "episodic")
        key = known_event_key(event.event_key)
        if self._memory.exists(ns, key):
            return KnownEventWrite("exact", key)
        index = DuplicateIndex()
        coins = set(event.coin_ids)
        for stored in self._memory.query(
            ns, as_of=event.known_at, record_type="known_event", since=event.known_at - window
        ):
            known = KnownEvent.model_validate(stored.value)
            if coins.intersection(known.coin_ids):
                index.add(stored.key, known.title)
        match = index.first_match(event.title)
        if match is not None:
            return KnownEventWrite(match.kind, match.key, match.similarity)
        if not self._memory.append(ns, key, event):
            return KnownEventWrite("exact", key)
        return KnownEventWrite("new", key)


class EpisodicReader:
    """One agent's episodic memory as known strictly before `as_of`."""

    def __init__(self, memory: MemoryStore, agent: AgentName, as_of: datetime) -> None:
        self._memory = memory
        self.agent = AgentName(agent)
        self.as_of = ensure_utc(as_of)
        self._ns = memory_namespace(self.agent, "episodic")

    def recent(
        self,
        *,
        coin_id: int | None = None,
        limit: int = 5,
        lookback: timedelta = DEFAULT_EPISODE_LOOKBACK,
    ) -> tuple[Episode, ...]:
        """Newest episodes first (by decision `as_of`), each with its outcome if already known."""
        if limit < 1:
            raise ValueError("limit must be >= 1")
        equals = {"coin_id": coin_id} if coin_id is not None else None
        decisions = [
            DecisionEpisode.model_validate(stored.value)
            for stored in self._memory.query(
                self._ns,
                as_of=self.as_of,
                record_type="decision",
                since=self.as_of - lookback,
                equals=equals,
            )
        ]
        decisions.sort(key=lambda d: (d.as_of, d.event_id), reverse=True)
        episodes = []
        for decision in decisions[:limit]:
            stored = self._memory.get(self._ns, outcome_key(decision.event_id), as_of=self.as_of)
            outcome = OutcomeRecord.model_validate(stored.value) if stored is not None else None
            episodes.append(Episode(decision=decision, outcome=outcome))
        return tuple(episodes)

    def known_events(self, *, coin_id: int, since: datetime) -> tuple[KnownEvent, ...]:
        """Known events of `coin_id` first seen in `[since, as_of)`, newest first."""
        found = [
            KnownEvent.model_validate(stored.value)
            for stored in self._memory.query(
                self._ns, as_of=self.as_of, record_type="known_event", since=since
            )
        ]
        found = [event for event in found if coin_id in event.coin_ids]
        found.sort(key=lambda e: (e.known_at, e.event_key), reverse=True)
        return tuple(found)
