"""What one agent may remember in one council event: its own active lessons and recent episodes.

`AgentMemory` is bound to one agent and the event's `as_of` at construction, so it cannot read another
agent's namespace or anything known at or after `as_of`. `MemoryRecall.prompt_text()` is canonical JSON
without any wall-clock value: the same event recalled on any later day yields the same bytes.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import Field

from hdt.contracts.common import AgentName, CandidateSource, ContractModel, Intent, Side, UtcDatetime
from hdt.core.clock import ensure_utc
from hdt.core.ids import canonical_json
from hdt.memory.episodic import DEFAULT_EPISODE_LOOKBACK, Episode, EpisodicReader
from hdt.memory.lessons import LessonLog
from hdt.memory.store import MemoryStore


class LessonText(ContractModel):
    lesson_id: str
    title: str
    when_text: str
    observation: str
    adjustment: str


class EpisodeSummary(ContractModel):
    event_id: str
    coin_id: int
    as_of: UtcDatetime
    source: CandidateSource
    abstained: bool
    p_used: float | None
    council_intent: Intent | None
    council_side: Side | None
    summary: str
    outcome: str = Field(description="'pending' until the outcome is known, else up/down/unresolved")
    hit: bool | None = None
    realized_return: float | None = None

    @classmethod
    def of(cls, episode: Episode) -> EpisodeSummary:
        decision, outcome = episode.decision, episode.outcome
        label = "pending" if outcome is None else {1: "up", 0: "down", None: "unresolved"}[outcome.label]
        return cls(
            event_id=decision.event_id,
            coin_id=decision.coin_id,
            as_of=decision.as_of,
            source=decision.source,
            abstained=decision.abstain,
            p_used=decision.p_used,
            council_intent=decision.council_intent,
            council_side=decision.council_side,
            summary=decision.summary,
            outcome=label,
            hit=outcome.hit if outcome is not None else None,
            realized_return=outcome.realized_return if outcome is not None else None,
        )


class MemoryRecall(ContractModel):
    agent: AgentName
    as_of: UtcDatetime
    lessons: tuple[LessonText, ...]
    episodes: tuple[EpisodeSummary, ...]

    def prompt_text(self) -> str:
        return canonical_json(self.model_dump(mode="json")).decode("utf-8")


class AgentMemory:
    def __init__(self, memory: MemoryStore, agent: AgentName, as_of: datetime) -> None:
        self.agent = AgentName(agent)
        self.as_of = ensure_utc(as_of)
        self._lessons = LessonLog(memory)
        self._episodes = EpisodicReader(memory, self.agent, self.as_of)

    def lessons(self) -> tuple[LessonText, ...]:
        """This agent's lessons that were `active` at `as_of` (never shadow, never another agent's)."""
        return tuple(
            LessonText(
                lesson_id=lesson.lesson_id,
                title=lesson.template.title,
                when_text=lesson.template.when_text,
                observation=lesson.template.observation,
                adjustment=lesson.template.adjustment,
            )
            for lesson in self._lessons.active(self.agent, self.as_of)
        )

    def episodes(self, *, coin_id: int | None = None, limit: int = 5) -> tuple[Episode, ...]:
        return self._episodes.recent(coin_id=coin_id, limit=limit, lookback=DEFAULT_EPISODE_LOOKBACK)

    def recall(self, *, coin_id: int | None = None, limit: int = 5) -> MemoryRecall:
        return MemoryRecall(
            agent=self.agent,
            as_of=self.as_of,
            lessons=self.lessons(),
            episodes=tuple(EpisodeSummary.of(e) for e in self.episodes(coin_id=coin_id, limit=limit)),
        )
