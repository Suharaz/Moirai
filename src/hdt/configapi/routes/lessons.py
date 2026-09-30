"""`/lessons`: human review of Reflection lessons (phase 08).

- `GET /lessons?limit=` lists the `lessons` read model (a view over the append-only lesson events in
  `store`), newest first.
- `POST /lessons/{id}/approve {note}` turns a shadow lesson with an improving A/B (n >= 50, CI below 0)
  into `active`.
- `POST /lessons/{id}/retire {note}` retires a lesson: a shadow lesson is rejected (the note is the review
  note), an active one is retired.
Both write one lesson event and its audit row in one `hdt_configapi` transaction: an advisory transaction
lock serializes the reviews of one agent's lesson chain, the chain is read under the lock and the
transition is checked by `hdt.memory.lessons.LessonLog`, and the event is inserted into `store` with
`ON CONFLICT DO NOTHING` (the row-level security policy allows actions approved / rejected / retired
only). A concurrent review that lost the race, or any invalid transition, answers 409; an unknown lesson
404. The body may carry `expected_state`, the state the reviewer saw: when the lesson is no longer in it
(for example an approve won the race against a retire, which would otherwise retire the fresh approval)
the review answers 409. When the audit write fails nothing is written.
"""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from typing import Any, Final, Literal

from fastapi import APIRouter, Depends, Query, Request
from langgraph.store.memory import InMemoryStore
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator
from sqlalchemy import text
from sqlalchemy.orm import Session

from hdt.configapi.audit import write_audit
from hdt.configapi.auth import AuthContext, require_session
from hdt.configapi.context import get_state
from hdt.configapi.errors import ApiError, error_list
from hdt.contracts.common import AgentName
from hdt.memory.lessons import LESSON_ID_PATTERN, Lesson, LessonLog, LessonState, LessonTransitionError
from hdt.memory.store import MemoryConflictError, MemoryStore, memory_namespace

router = APIRouter(prefix="/lessons", tags=["lessons"])
AUDIT_SECTION: Final[str] = "lessons"
_ID = re.compile(LESSON_ID_PATTERN)
LATEST: Final[datetime] = datetime(9999, 1, 1, tzinfo=UTC)
"""Read the whole chain before writing (the transition itself is re-checked by `LessonLog`)."""
_LOCK = text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))")
_CHAIN = text("SELECT key, value FROM store WHERE prefix = :prefix")
_INSERT = text(
    "INSERT INTO store (prefix, key, value) VALUES (:prefix, :key, CAST(:value AS jsonb)) "
    "ON CONFLICT (prefix, key) DO NOTHING"
)
_KEY_SUFFIX: Final[dict[str, str]] = {"approved": "review", "rejected": "review", "retired": "retired"}


class ReviewRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    note: str = Field(min_length=1, max_length=500)
    expected_state: LessonState | None = None
    """The state the reviewer saw; a different current state answers 409 (a concurrent review won)."""

    @field_validator("note")
    @classmethod
    def _not_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("note must not be blank")
        return value


def _row(r: Any) -> dict[str, Any]:
    return dict(r._mapping)


@router.get("")
async def list_lessons(
    request: Request,
    limit: int = Query(200, ge=1, le=1000),
    _ctx: AuthContext = Depends(require_session),
) -> dict[str, Any]:
    def read(session: Session) -> list[dict[str, Any]]:
        rows = session.execute(
            text("SELECT * FROM lessons ORDER BY created_at DESC, lesson_id LIMIT :limit"), {"limit": limit}
        )
        return [_row(r) for r in rows]

    return {"items": await get_state(request).db(read)}


def _lesson_body(lesson: Lesson) -> dict[str, Any]:
    return {
        "lesson_id": lesson.lesson_id,
        "agent": lesson.agent.value,
        "state": lesson.state,
        "decided_by": lesson.decided_by,
        "decided_at": lesson.decided_at,
        "review_note": lesson.review_note,
    }


def review_lesson(
    session: Session,
    *,
    agent: AgentName,
    lesson_id: str,
    action: Literal["approve", "retire"],
    actor: str,
    ip: str | None,
    note: str,
    expected_state: LessonState | None = None,
) -> Lesson:
    """Append one review event and its audit row inside the caller's transaction (see the module doc)."""
    namespace = memory_namespace(agent, "lessons")
    prefix = ".".join(namespace)
    session.execute(_LOCK, {"key": f"hdt.lessons.{agent.value}"})
    chain = InMemoryStore()
    for row in session.execute(_CHAIN, {"prefix": prefix}):
        chain.put(namespace, str(row.key), dict(row.value), index=False)
    log = LessonLog(MemoryStore(chain))
    current = next((x for x in log.lessons_at(agent, LATEST) if x.lesson_id == lesson_id), None)
    if current is None:
        raise ApiError(404, "unknown_lesson", f"unknown lesson {lesson_id}")
    if expected_state is not None and current.state != expected_state:
        raise ApiError(
            409,
            "invalid_transition",
            f"lesson {lesson_id} is {current.state}, not {expected_state}: it was reviewed concurrently",
            state=current.state,
        )
    try:
        if action == "approve":
            lesson, event = log.approve(agent, lesson_id, actor=actor, note=note), "approved"
        elif current.state == "shadow":
            lesson, event = log.reject(agent, lesson_id, actor=actor, note=note), "rejected"
        else:
            lesson, event = log.retire(agent, lesson_id, actor=actor, note=note), "retired"
    except (LessonTransitionError, MemoryConflictError) as exc:
        raise ApiError(409, "invalid_transition", str(exc), state=current.state) from None
    except ValidationError as exc:
        raise ApiError(422, "invalid_note", "the review note is not valid", errors=error_list(exc)) from None
    key = f"{lesson_id}:{_KEY_SUFFIX[event]}"
    stored = chain.get(namespace, key)
    if stored is None:
        raise RuntimeError(f"lesson event {key} was not staged")
    inserted = session.execute(
        _INSERT, {"prefix": prefix, "key": key, "value": json.dumps(stored.value, sort_keys=True)}
    )
    if not inserted.rowcount:  # type: ignore[attr-defined]
        raise ApiError(
            409, "invalid_transition", f"lesson {lesson_id} was reviewed concurrently", state=current.state
        )
    write_audit(
        session,
        user=actor,
        ip=ip,
        action=f"lesson.{event}",
        section=AUDIT_SECTION,
        diff={"lesson_id": lesson_id, "agent": agent.value, "state": lesson.state, "note": note},
    )
    return lesson


async def _review(
    request: Request,
    ctx: AuthContext,
    lesson_id: str,
    body: ReviewRequest,
    action: Literal["approve", "retire"],
) -> dict[str, Any]:
    if not _ID.fullmatch(lesson_id):
        raise ApiError(404, "unknown_lesson", f"unknown lesson {lesson_id}")
    agent = AgentName(lesson_id.split("-", 1)[0])
    lesson = await get_state(request).db(
        lambda s: review_lesson(
            s,
            agent=agent,
            lesson_id=lesson_id,
            action=action,
            actor=ctx.username,
            ip=ctx.ip,
            note=body.note,
            expected_state=body.expected_state,
        )
    )
    return _lesson_body(lesson)


@router.post("/{lesson_id}/approve")
async def approve_lesson(
    lesson_id: str, body: ReviewRequest, request: Request, ctx: AuthContext = Depends(require_session)
) -> dict[str, Any]:
    return await _review(request, ctx, lesson_id, body, "approve")


@router.post("/{lesson_id}/retire")
async def retire_lesson(
    lesson_id: str, body: ReviewRequest, request: Request, ctx: AuthContext = Depends(require_session)
) -> dict[str, Any]:
    return await _review(request, ctx, lesson_id, body, "retire")
