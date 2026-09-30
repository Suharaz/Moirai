"""Shadow-lesson ports for the council (phase 08 lesson A/B).

- `MemoryShadowLessons` (council `ShadowLessons`): the agent's shadow lesson at `as_of` from the append-only
  lesson chain (`hdt.memory.lessons.LessonLog`), only while its A/B is still running: once the A/B result is
  recorded the lesson waits for the human review and no more shadow forecasts are needed.
- `PgShadowForecastSink` (council `ShadowForecastSink`): inserts the round-1 forecast made with the shadow
  lesson into `lesson_shadow_forecasts` inside the council's decision transaction (role `hdt_council`),
  idempotent on `(event_id, agent, lesson_id)`.
"""

from __future__ import annotations

from datetime import datetime

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert

from hdt.contracts.common import AgentName
from hdt.contracts.forecast import AgentForecast
from hdt.db.models.scoring import LessonShadowForecastRow
from hdt.memory.lessons import Lesson, LessonLog
from hdt.memory.store import MemoryStore


class MemoryShadowLessons:
    def __init__(self, memory: MemoryStore) -> None:
        self._lessons = LessonLog(memory)

    def shadow(self, agent: AgentName, as_of: datetime) -> Lesson | None:
        lesson = self._lessons.shadow(AgentName(agent), as_of)
        if lesson is None or lesson.ab is not None:
            return None
        return lesson


class PgShadowForecastSink:
    def record(
        self, conn: sa.Connection, *, event_id: str, agent: AgentName, lesson_id: str, forecast: AgentForecast
    ) -> None:
        agent = AgentName(agent)
        if forecast.event_id != event_id or forecast.agent != agent or forecast.round != 1:
            raise ValueError("a shadow forecast must be the same event's round-1 forecast of the same agent")
        if not lesson_id.startswith(f"{agent.value}-"):
            raise ValueError("the shadow lesson must belong to the forecasting agent")
        conn.execute(
            insert(LessonShadowForecastRow)
            .values(
                event_id=event_id,
                agent=agent.value,
                lesson_id=lesson_id,
                forecast=forecast.model_dump(mode="json"),
            )
            .on_conflict_do_nothing(index_elements=["event_id", "agent", "lesson_id"])
        )
