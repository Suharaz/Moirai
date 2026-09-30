"""Lessons: one append-only event chain per agent (namespace `(agent, lessons)`), state rebuilt at `as_of`.

Lifecycle (phase 08 Reflection proposes and measures, a human decides on the console via config-api):

| action         | writer                      | transition                                             |
|:---------------|:----------------------------|:-------------------------------------------------------|
| `proposed`     | Reflection (`hdt_scorer`)   | -> `shadow`; at most one `shadow` lesson per agent      |
| `ab_completed` | Reflection                  | `shadow` -> `shadow`, A/B result recorded once          |
| `approved`     | human (`hdt_configapi`)     | `shadow`, improving A/B on >= 50 forecasts -> `active`  |
| `rejected`     | human, review note required | `shadow` -> `retired`                                   |
| `retired`      | Reflection or human         | `shadow` or `active` -> `retired`                       |

The state at `as_of` is the fold of the events known strictly before `as_of`, in `(known_at, key)` order;
an event that is not a valid transition from the folded state is ignored. Record keys make the invariants
hold under concurrent writers: each proposal takes the agent's next slot (`<agent>-<slot>:proposed`), and
approval and rejection share one key (`<lesson_id>:review`), so they are mutually exclusive.
Writers stamp `known_at` no earlier than the chain's last event, so clock skew cannot reorder a chain.
Lessons are structured templates: no quotation marks (no raw quotes) and nothing that reads as a role or
system instruction. An agent reads only its own `active` lessons (`hdt.memory.recall`); the `shadow`
lesson is read only by the phase 08 A/B runner (`LessonLog.shadow`).
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Iterable
from datetime import UTC, datetime, timedelta
from typing import Final, Literal, Self

from pydantic import Field, NonNegativeInt, field_validator, model_validator

from hdt.contracts.common import AgentName, ContractModel, UtcDatetime
from hdt.core.clock import ensure_utc, utcnow
from hdt.memory.dedupe import DuplicateIndex
from hdt.memory.store import MemoryRecord, MemoryStore, memory_namespace
from hdt.tools.base import ShortText, Text

log = logging.getLogger(__name__)

MIN_AB_FORECASTS: Final[int] = 50
LESSON_ID_PATTERN: Final[str] = r"^(crowding|technical|micro|fundamental|news|macro)-[0-9]{4}$"
LessonState = Literal["shadow", "active", "retired"]
LessonAction = Literal["proposed", "ab_completed", "approved", "rejected", "retired"]
_TICK: Final[timedelta] = timedelta(microseconds=1)
_END_OF_TIME: Final[datetime] = datetime(9999, 1, 1, tzinfo=UTC)
_QUOTES: Final[frozenset[str]] = frozenset('"`\u201c\u201d\u201e\u00ab\u00bb\u2033')
_INSTRUCTION: Final = re.compile(
    r"(?im)^\s*(system|assistant|user|developer|tool)\s*:"
    r"|\bignore\s+(all\s+|any\s+|the\s+)?(previous|prior|above|earlier)\s+(instructions|rules)"
    r"|<\|[^|]*\|>|\[/?(inst|sys)\]"
)


class LessonTransitionError(ValueError):
    """The requested lesson event is not a valid transition from the current state."""


class LessonTemplate(ContractModel):
    """The structured lesson text an agent may read once the lesson is active."""

    title: ShortText = Field(min_length=3, max_length=120)
    when_text: Text = Field(min_length=3, max_length=400, description="when the lesson applies")
    observation: Text = Field(min_length=3, max_length=400, description="what went wrong")
    adjustment: Text = Field(min_length=3, max_length=400, description="how to adjust the reading")

    @field_validator("title", "when_text", "observation", "adjustment")
    @classmethod
    def _no_quotes_or_instructions(cls, value: str) -> str:
        if any(ch in _QUOTES for ch in value):
            raise ValueError("lessons are templates: quotation marks (raw quotes) are not allowed")
        if _INSTRUCTION.search(value):
            raise ValueError("lessons must not contain role labels or system instructions")
        return value

    def text(self) -> str:
        return "\n".join((self.title, self.when_text, self.observation, self.adjustment))


class AbResult(ContractModel):
    """Paired A/B of the shadow lesson: log-loss with and without it; CI is for (with - without)."""

    n: NonNegativeInt
    logloss_with: float = Field(ge=0)
    logloss_without: float = Field(ge=0)
    ci_low: float
    ci_high: float

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.ci_low > self.ci_high:
            raise ValueError("ci_low must be <= ci_high")
        return self

    @property
    def improved(self) -> bool:
        """Enough independent forecasts and the whole CI of (with - without) below zero."""
        return self.n >= MIN_AB_FORECASTS and self.logloss_with < self.logloss_without and self.ci_high < 0


class LessonEvent(MemoryRecord):
    record_type: Literal["lesson_event"] = "lesson_event"
    lesson_id: str = Field(pattern=LESSON_ID_PATTERN)
    agent: AgentName
    action: LessonAction
    actor: str = Field(min_length=1, max_length=64)
    note: Text | None = Field(default=None, max_length=500)
    slot: int | None = Field(default=None, ge=1, le=9999)
    template: LessonTemplate | None = None
    ab: AbResult | None = None

    @model_validator(mode="after")
    def _check(self) -> Self:
        if not self.lesson_id.startswith(f"{self.agent.value}-"):
            raise ValueError("lesson_id must belong to the agent")
        proposal = self.action == "proposed"
        if proposal != (self.template is not None and self.slot is not None):
            raise ValueError("template and slot are required on (and only on) a proposal")
        if (self.action == "ab_completed") != (self.ab is not None):
            raise ValueError("ab is required on (and only on) ab_completed")
        if self.action == "rejected" and not self.note:
            raise ValueError("a rejection needs a review note")
        return self

    @property
    def record_key(self) -> str:
        suffix = {
            "proposed": "proposed",
            "ab_completed": "ab",
            "approved": "review",
            "rejected": "review",
            "retired": "retired",
        }[self.action]
        return f"{self.lesson_id}:{suffix}"


class Lesson(ContractModel):
    lesson_id: str
    agent: AgentName
    state: LessonState
    template: LessonTemplate
    shadow_since: UtcDatetime
    ab: AbResult | None = None
    ab_completed_at: UtcDatetime | None = None
    decided_by: str | None = None
    decided_at: UtcDatetime | None = None
    review_note: str | None = None
    retired_at: UtcDatetime | None = None

    @property
    def awaiting_review(self) -> bool:
        return self.state == "shadow" and self.ab is not None and self.ab.improved


def fold_lessons(events: Iterable[tuple[str, LessonEvent]]) -> dict[str, Lesson]:
    """Rebuild lesson states from `(key, event)` pairs; invalid transitions are ignored."""
    lessons: dict[str, Lesson] = {}
    for key, event in sorted(events, key=lambda pair: (pair[1].known_at, pair[0])):
        applied = _apply(lessons, event)
        if applied is None:
            log.warning(
                "ignored invalid lesson event",
                extra={"lesson_id": event.lesson_id, "action": event.action, "key": key},
            )
            continue
        lessons[event.lesson_id] = applied
    return lessons


def _apply(lessons: dict[str, Lesson], event: LessonEvent) -> Lesson | None:
    current = lessons.get(event.lesson_id)
    if event.action == "proposed":
        if current is not None or any(lesson.state == "shadow" for lesson in lessons.values()):
            return None
        assert event.template is not None
        return Lesson(
            lesson_id=event.lesson_id,
            agent=event.agent,
            state="shadow",
            template=event.template,
            shadow_since=event.known_at,
        )
    if current is None:
        return None
    if event.action == "ab_completed":
        if current.state != "shadow" or current.ab is not None:
            return None
        return current.model_copy(update={"ab": event.ab, "ab_completed_at": event.known_at})
    if event.action == "approved":
        if not current.awaiting_review:
            return None
        return current.model_copy(
            update={
                "state": "active",
                "decided_by": event.actor,
                "decided_at": event.known_at,
                "review_note": event.note,
            }
        )
    if event.action == "rejected":
        if current.state != "shadow":
            return None
        return current.model_copy(
            update={
                "state": "retired",
                "decided_by": event.actor,
                "decided_at": event.known_at,
                "review_note": event.note,
                "retired_at": event.known_at,
            }
        )
    if current.state == "retired":
        return None
    return current.model_copy(
        update={
            "state": "retired",
            "retired_at": event.known_at,
            "review_note": event.note or current.review_note,
        }
    )


class LessonLog:
    """Read the lesson chains at any `as_of`; append events (Reflection and human review only)."""

    def __init__(self, memory: MemoryStore, clock: Callable[[], datetime] = utcnow) -> None:
        self._memory = memory
        self._clock = clock

    # ------------------------------------------------------------------ reads

    def lessons_at(self, agent: AgentName, as_of: datetime) -> tuple[Lesson, ...]:
        """Every lesson of `agent` in its state at `as_of`, ordered by lesson id."""
        folded = fold_lessons(self._events(agent, as_of))
        return tuple(folded[lesson_id] for lesson_id in sorted(folded))

    def active(self, agent: AgentName, as_of: datetime) -> tuple[Lesson, ...]:
        return tuple(lesson for lesson in self.lessons_at(agent, as_of) if lesson.state == "active")

    def shadow(self, agent: AgentName, as_of: datetime) -> Lesson | None:
        """The agent's shadow lesson (A/B runner only; agents never see it)."""
        return next((x for x in self.lessons_at(agent, as_of) if x.state == "shadow"), None)

    # ------------------------------------------------------------------ writes

    def propose(self, agent: AgentName, template: LessonTemplate, *, actor: str) -> Lesson:
        agent = AgentName(agent)
        events, now = self._current(agent)
        lessons = fold_lessons(events)
        if any(lesson.state == "shadow" for lesson in lessons.values()):
            raise LessonTransitionError(f"{agent.value} already has a shadow lesson")
        index = DuplicateIndex()
        for lesson in lessons.values():
            index.add(lesson.lesson_id, lesson.template.text())
        duplicate = index.first_match(template.text())
        if duplicate is not None:
            raise LessonTransitionError(f"{duplicate.kind} duplicate of lesson {duplicate.key}")
        slot = 1 + max((event.slot or 0 for _, event in events), default=0)
        event = LessonEvent(
            lesson_id=f"{agent.value}-{slot:04d}",
            agent=agent,
            action="proposed",
            actor=actor,
            slot=slot,
            template=template,
            known_at=now,
        )
        return self._append(agent, events, event)

    def record_ab(self, agent: AgentName, lesson_id: str, ab: AbResult, *, actor: str) -> Lesson:
        return self._transition(agent, lesson_id, "ab_completed", actor=actor, ab=ab)

    def approve(self, agent: AgentName, lesson_id: str, *, actor: str, note: str | None = None) -> Lesson:
        return self._transition(agent, lesson_id, "approved", actor=actor, note=note)

    def reject(self, agent: AgentName, lesson_id: str, *, actor: str, note: str) -> Lesson:
        return self._transition(agent, lesson_id, "rejected", actor=actor, note=note)

    def retire(self, agent: AgentName, lesson_id: str, *, actor: str, note: str | None = None) -> Lesson:
        return self._transition(agent, lesson_id, "retired", actor=actor, note=note)

    # ------------------------------------------------------------------ internals

    def _events(self, agent: AgentName, as_of: datetime) -> list[tuple[str, LessonEvent]]:
        ns = memory_namespace(agent, "lessons")
        return [
            (stored.key, LessonEvent.model_validate(stored.value))
            for stored in self._memory.query(ns, as_of=as_of, record_type="lesson_event")
        ]

    def _current(self, agent: AgentName) -> tuple[list[tuple[str, LessonEvent]], datetime]:
        """Every event of the chain (even ones stamped ahead of this clock) and the `known_at` for a new
        one: never before the chain's last event, so a skewed writer clock cannot reorder the chain."""
        events = self._events(agent, _END_OF_TIME)
        now = ensure_utc(self._clock())
        last = max((event.known_at for _, event in events), default=None)
        if last is not None and last >= now:
            now = last + _TICK
        return events, now

    def _transition(
        self,
        agent: AgentName,
        lesson_id: str,
        action: LessonAction,
        *,
        actor: str,
        note: str | None = None,
        ab: AbResult | None = None,
    ) -> Lesson:
        agent = AgentName(agent)
        events, now = self._current(agent)
        event = LessonEvent(
            lesson_id=lesson_id, agent=agent, action=action, actor=actor, note=note, ab=ab, known_at=now
        )
        return self._append(agent, events, event)

    def _append(self, agent: AgentName, events: list[tuple[str, LessonEvent]], event: LessonEvent) -> Lesson:
        folded = fold_lessons(events)
        applied = _apply(folded, event)
        if applied is None:
            current = folded.get(event.lesson_id)
            state = current.state if current is not None else "unknown"
            raise LessonTransitionError(f"{event.action} is not valid for lesson {event.lesson_id} ({state})")
        self._memory.append(memory_namespace(agent, "lessons"), event.record_key, event)
        return applied
