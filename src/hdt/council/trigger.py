"""Event admission from the `candidates` stream and the meeting worker.

Admission (`CouncilTrigger.admit`, one transaction serialized per coin by an advisory lock):
- the natural key (coin_id, as_of, source) is admitted once; a redelivered candidate changes nothing;
- a candidate is never skipped for its age: the meeting takes the time the debate needs, and Risk refuses
  an OPEN whose levels the market has already invalidated (owner decision 2026-09-28);
- a coin gets at most one meeting per `min_event_spacing_min` (15 min): a candidate closer than that to an
  admitted event of the same coin is recorded `skipped` (reason `spacing`);
- one forecast is scored per (coin, `scoring_window_h` window): a scored event within the window before
  or after the candidate's `as_of` (candidates can arrive out of order) makes it `unscored`; a HELD
  re-evaluation and a shadow-only candidate are always `unscored`, and only scored events that did not
  fail open a window;
- the config versions active at admission are pinned on the event, and the candidate is flagged
  `shadow_only` when the scanner emitted it only from its loose superset thresholds.

`MeetingWorker` runs admitted events oldest first. The LangGraph checkpoint (`thread_id = event_id`) makes
a restart resume an interrupted meeting instead of starting it over; a finished graph whose status was not
yet marked is only marked. A failed attempt is retried after an exponential backoff; an event whose
attempts are used up (a crash mid-meeting counts too) is marked `failed`, counted and alerted.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Final, Literal, Protocol

import sqlalchemy as sa
from langchain_core.runnables import RunnableConfig
from sqlalchemy.orm import Session

from hdt.contracts.candidate import Candidate
from hdt.contracts.common import CandidateSource
from hdt.core.alerts import alert
from hdt.core.clock import utcnow
from hdt.core.config import CouncilFile
from hdt.core.ids import b32_digest, to_canonical
from hdt.council.ports import ScannerLog
from hdt.db.models.decision import CouncilEventRow
from hdt.ops.metrics import COUNCIL_EVENTS_FAILED

log = logging.getLogger(__name__)

SKIP_SPACING: Final[str] = "spacing"
ERROR_MAX: Final[int] = 2000
RETRY_BASE_S: Final[float] = 10.0
RETRY_MAX_S: Final[float] = 300.0
ALERT_SERVICE: Final[str] = "council"
PRUNE_BATCH: Final[int] = 50
"""Finished events whose checkpoint threads one `prune_checkpoints` pass deletes at most."""
Admission = Literal["admitted", "duplicate", "skipped"]


def event_id_for(candidate: Candidate) -> str:
    """Deterministic event id of a candidate's natural key (<= 80 chars)."""
    coin_id, as_of, source = candidate.natural_key
    return "ev_" + b32_digest(f"{coin_id}|{as_of}|{source}", 26).lower()


@dataclass(frozen=True)
class AdmissionResult:
    status: Admission
    event_id: str
    unscored: bool = False
    shadow_only: bool = False


class CouncilTrigger:
    def __init__(
        self,
        engine: sa.Engine,
        *,
        pin: Callable[[], Mapping[str, int]],
        council: Callable[[Mapping[str, int]], CouncilFile],
        scanner_log: ScannerLog,
        clock: Callable[[], datetime] = utcnow,
    ) -> None:
        self.engine = engine
        self.pin = pin
        self.council = council
        self.scanner_log = scanner_log
        self.clock = clock

    def admit(self, candidate: Candidate) -> AdmissionResult:
        event_id = event_id_for(candidate)
        version_ids = dict(self.pin())
        council = self.council(version_ids)
        spacing = timedelta(minutes=council.min_event_spacing_min)
        window = timedelta(hours=council.scoring_window_h)
        e = CouncilEventRow
        with self.engine.begin() as conn:
            conn.execute(sa.select(sa.func.pg_advisory_xact_lock(candidate.coin_id)))
            if conn.execute(sa.select(e.event_id).where(e.event_id == event_id)).first() is not None:
                return AdmissionResult("duplicate", event_id)
            admitted = sa.and_(e.coin_id == candidate.coin_id, e.status != "skipped")
            near = conn.execute(
                sa.select(e.event_id)
                .where(admitted, e.as_of > candidate.as_of - spacing, e.as_of < candidate.as_of + spacing)
                .limit(1)
            ).first()
            skip_reason = SKIP_SPACING if near is not None else None
            if skip_reason is not None:
                status: Admission = "skipped"
                unscored = True
                shadow_only = False
            else:
                status = "admitted"
                in_window = conn.execute(
                    sa.select(e.event_id)
                    .where(
                        admitted,
                        e.status != "failed",
                        e.unscored.is_(False),
                        e.as_of > candidate.as_of - window,
                        e.as_of < candidate.as_of + window,
                    )
                    .limit(1)
                ).first()
                shadow_only = candidate.source is not CandidateSource.HELD and self.scanner_log.shadow_only(
                    candidate
                )
                unscored = candidate.source is CandidateSource.HELD or shadow_only or in_window is not None
            now = self.clock()
            conn.execute(
                sa.insert(e).values(
                    event_id=event_id,
                    coin_id=candidate.coin_id,
                    as_of=candidate.as_of,
                    source=candidate.source.value,
                    candidate=to_canonical(candidate),
                    config_version_ids=version_ids,
                    unscored=unscored,
                    shadow_only=shadow_only,
                    status="skipped" if status == "skipped" else "pending",
                    skip_reason=skip_reason,
                    attempts=0,
                    error=None,
                    msg_id=None,
                    created_at=now,
                    updated_at=now,
                )
            )
        log.info(
            "council event %s",
            status,
            extra={"event_id": event_id, "coin_id": candidate.coin_id, "source": candidate.source.value},
        )
        return AdmissionResult(status, event_id, unscored, shadow_only)


class CompiledMeeting(Protocol):
    async def aget_state(self, config: RunnableConfig) -> Any: ...

    async def ainvoke(self, input: Any, config: RunnableConfig, **kwargs: Any) -> Any: ...


class CheckpointThreads(Protocol):
    async def adelete_thread(self, thread_id: str) -> None: ...


@dataclass(frozen=True)
class PendingEvent:
    event_id: str
    candidate: dict[str, Any]
    config_version_ids: dict[str, int]
    unscored: bool
    shadow_only: bool
    attempts: int


def retry_delay(attempts: int) -> timedelta:
    """Wait before the next attempt of an event that has failed `attempts` times (exponential, capped)."""
    return timedelta(seconds=min(RETRY_BASE_S * 2 ** max(attempts - 1, 0), RETRY_MAX_S))


class MeetingWorker:
    """Runs admitted events through the compiled meeting graph, one at a time, oldest first.

    `checkpoints`: when given, the checkpoint thread of every `done` or `failed` event is deleted (the
    decision card, or the recorded error, holds what the meeting produced) by `prune_checkpoints`, which
    runs after each pass and marks `checkpoint_pruned_at`; a crash before the delete is swept next pass."""

    def __init__(
        self,
        engine: sa.Engine,
        app: CompiledMeeting,
        *,
        max_attempts: int = 3,
        on_done: Callable[[str], None] | None = None,
        checkpoints: CheckpointThreads | None = None,
        clock: Callable[[], datetime] = utcnow,
    ) -> None:
        self.engine = engine
        self.app = app
        self.max_attempts = max_attempts
        self.on_done = on_done
        self.checkpoints = checkpoints
        self.clock = clock

    def _open(self) -> list[PendingEvent]:
        """Open events, oldest first; a `pending` event that failed before waits out its retry delay (a
        `running` one was interrupted by a crash and resumes at once)."""
        e = CouncilEventRow
        with self.engine.connect() as conn:
            rows = conn.execute(
                sa.select(
                    e.event_id,
                    e.candidate,
                    e.config_version_ids,
                    e.unscored,
                    e.shadow_only,
                    e.attempts,
                    e.status,
                    e.updated_at,
                )
                .where(e.status.in_(("pending", "running")))
                .order_by(e.created_at, e.event_id)
            ).all()
        now = self.clock()
        return [
            PendingEvent(
                r.event_id,
                dict(r.candidate),
                {str(k): int(v) for k, v in r.config_version_ids.items()},
                r.unscored,
                r.shadow_only,
                r.attempts,
            )
            for r in rows
            if not (r.status == "pending" and r.attempts > 0 and now < r.updated_at + retry_delay(r.attempts))
        ]

    def _set(self, event_id: str, **values: Any) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                sa.update(CouncilEventRow)
                .where(CouncilEventRow.event_id == event_id)
                .values(updated_at=self.clock(), **values)
            )

    def _fail(self, event: PendingEvent, error: str) -> None:
        """Mark the event `failed` and raise its alert in the same transaction."""
        with Session(self.engine) as session, session.begin():
            session.execute(
                sa.update(CouncilEventRow)
                .where(CouncilEventRow.event_id == event.event_id)
                .values(status="failed", error=error[:ERROR_MAX], updated_at=self.clock())
            )
            alert(
                session,
                kind="council_event_failed",
                severity="warning",
                title=f"council meeting for coin {event.candidate.get('coin_id')} failed permanently",
                detail=f"event {event.event_id}: {error}"[:ERROR_MAX],
                service=ALERT_SERVICE,
                episode=event.event_id,
            )
        COUNCIL_EVENTS_FAILED.inc()

    async def run_event(self, event: PendingEvent) -> bool:
        """Run (or resume) one meeting; True when it finished."""
        if event.attempts >= self.max_attempts:
            # every attempt was used by a run that never recorded its outcome (a crash mid-meeting)
            log.error("meeting attempts used up by interrupted runs", extra={"event_id": event.event_id})
            await asyncio.to_thread(
                self._fail, event, f"interrupted {event.attempts} times without finishing"
            )
            return False
        config: RunnableConfig = {"configurable": {"thread_id": event.event_id}}
        await asyncio.to_thread(self._set, event.event_id, status="running", attempts=event.attempts + 1)
        try:
            snapshot = await self.app.aget_state(config)
            if snapshot.values and not snapshot.next:
                log.info("meeting already finished; marking it done", extra={"event_id": event.event_id})
            elif snapshot.values:
                log.info("resuming meeting from its checkpoint", extra={"event_id": event.event_id})
                await self.app.ainvoke(None, config, durability="sync")
            else:
                start = {
                    "event": {
                        "event_id": event.event_id,
                        "candidate": event.candidate,
                        "config_version_ids": event.config_version_ids,
                        "unscored": event.unscored,
                        "shadow_only": event.shadow_only,
                    }
                }
                await self.app.ainvoke(start, config, durability="sync")
        except Exception as exc:
            failed = event.attempts + 1 >= self.max_attempts
            log.exception(
                "meeting failed%s",
                " permanently" if failed else "; will resume",
                extra={"event_id": event.event_id},
            )
            error = f"{type(exc).__name__}: {exc}"[:ERROR_MAX]
            if failed:
                await asyncio.to_thread(self._fail, event, error)
            else:
                await asyncio.to_thread(self._set, event.event_id, status="pending", error=error)
            return False
        await asyncio.to_thread(self._set, event.event_id, status="done", error=None)
        if self.on_done is not None:
            self.on_done(event.event_id)
        return True

    def _unpruned(self, limit: int) -> list[str]:
        e = CouncilEventRow
        with self.engine.connect() as conn:
            return list(
                conn.execute(
                    sa.select(e.event_id)
                    .where(e.status.in_(("done", "failed")), e.checkpoint_pruned_at.is_(None))
                    .order_by(e.updated_at, e.event_id)
                    .limit(limit)
                ).scalars()
            )

    def _mark_pruned(self, event_id: str) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                sa.update(CouncilEventRow)
                .where(CouncilEventRow.event_id == event_id)
                .values(checkpoint_pruned_at=self.clock())
            )

    async def prune_checkpoints(self, limit: int = PRUNE_BATCH) -> int:
        """Delete the checkpoint threads of finished (`done` / `failed`) events not pruned yet; returns how
        many were pruned. A failing delete is logged and retried next pass."""
        if self.checkpoints is None:
            return 0
        pruned = 0
        for event_id in await asyncio.to_thread(self._unpruned, limit):
            try:
                await self.checkpoints.adelete_thread(event_id)
                await asyncio.to_thread(self._mark_pruned, event_id)
            except Exception:
                log.exception(
                    "could not delete a finished meeting's checkpoints", extra={"event_id": event_id}
                )
                continue
            pruned += 1
        return pruned

    async def run_pending(self) -> int:
        """Run every due open event once; returns how many finished."""
        finished = 0
        for event in await asyncio.to_thread(self._open):
            finished += int(await self.run_event(event))
        await self.prune_checkpoints()
        return finished
