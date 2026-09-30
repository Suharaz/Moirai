"""Council admission and delivery safety on Postgres: a missing scanner_log row fails closed, a candidate is
never skipped for its age, scoring windows look both ways and ignore shadow-only events, failed meetings are
counted and alerted, and replay pins are the union of the earlier rounds' captures."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

import pytest
import sqlalchemy as sa

from council_builders import AS_OF, candidate, settings
from fixtures.quant_db import QuantDb
from hdt.contracts.candidate import Candidate
from hdt.contracts.common import CandidateSource
from hdt.council.adapters import PgScannerLog
from hdt.council.trigger import CouncilTrigger, MeetingWorker, PendingEvent
from hdt.db.models.ops import AlertRow
from hdt.db.models.quant import ScannerLogRow
from hdt.ops.metrics import COUNCIL_EVENTS_FAILED

pytestmark = [pytest.mark.pg, pytest.mark.integration]


class Log:
    def __init__(self, shadow: set[tuple[int, Any]] | None = None) -> None:
        self.shadow = shadow or set()

    def shadow_only(self, cand: Candidate) -> bool:
        return (cand.coin_id, cand.as_of) in self.shadow


def trigger(db: QuantDb, *, log: Any = None, now_offset: timedelta = timedelta(seconds=2)) -> CouncilTrigger:
    council = settings().council
    return CouncilTrigger(
        db.council,
        pin=lambda: db.version_ids,
        council=lambda _ids: council,
        scanner_log=log or Log(),
        clock=lambda: AS_OF + timedelta(days=40) + now_offset,
    )


def scanner_row(cand: Candidate, *, strict: bool) -> dict[str, Any]:
    return {
        "coin_id": cand.coin_id,
        "as_of": cand.as_of,
        "rule": cand.source.value,
        "rule_version": cand.rule_version,
        "side": "LONG",
        "conditions": {},
        "strict_pass": strict,
        "loose_pass": True,
        "contagion_blocked": None,
        "emitted": True,
        "dropped_budget": False,
        "score": 2.5,
        "target_type": cand.target_type.value,
        "label_spec_version": cand.label_spec_version,
        "universe_date": cand.as_of.date(),
        "feature_ver": "f1",
        "created_at": cand.as_of,
    }


def test_a_missing_or_uncommitted_scanner_log_row_is_shadow_only(quant_db: QuantDb) -> None:
    """C3 repro: a missing row counted as a strict pass, so a superset-only candidate could OPEN."""
    cand = candidate(as_of=AS_OF + timedelta(days=30), coin_id=31)
    log = PgScannerLog(quant_db.council, wait_s=0.5, poll_s=0.1)
    assert log.shadow_only(cand)
    with quant_db.admin.connect() as conn:
        tx = conn.begin()
        conn.execute(sa.insert(ScannerLogRow).values(**scanner_row(cand, strict=True)))
        assert log.shadow_only(cand)  # not committed yet
        tx.commit()
    assert not log.shadow_only(cand)
    loose = candidate(as_of=AS_OF + timedelta(days=30), coin_id=32)
    with quant_db.admin.begin() as conn:
        conn.execute(sa.insert(ScannerLogRow).values(**scanner_row(loose, strict=False)))
    assert log.shadow_only(loose)


def test_a_candidate_is_admitted_however_old_it_is(quant_db: QuantDb) -> None:
    """Owner decision 2026-09-28: no decision TTL, so admission no longer skips a candidate as `stale`; Risk
    refuses an OPEN whose levels the market has invalidated meanwhile."""
    t = trigger(quant_db)
    old = t.admit(candidate(as_of=AS_OF + timedelta(days=39), coin_id=41))
    assert (old.status, old.unscored) == ("admitted", False)
    with quant_db.council.connect() as conn:
        row = conn.execute(
            sa.text("SELECT status, skip_reason FROM council_events WHERE event_id = :e"),
            {"e": old.event_id},
        ).one()
    assert tuple(row) == ("pending", None)


def test_scoring_windows_look_both_ways_and_ignore_shadow_only_events(quant_db: QuantDb) -> None:
    """M13: an earlier as_of arriving later was scored inside a scored event's window, and a shadow-only
    event consumed the window of a later tradable one."""
    base = AS_OF + timedelta(days=40)
    t = trigger(quant_db, log=Log({(52, base + timedelta(hours=1))}))
    later = t.admit(candidate(as_of=base + timedelta(hours=5), coin_id=51))
    assert (later.status, later.unscored) == ("admitted", False)
    earlier = t.admit(candidate(CandidateSource.MIGRATION, as_of=base + timedelta(hours=1), coin_id=51))
    assert (earlier.status, earlier.unscored) == ("admitted", True)
    shadow = t.admit(candidate(as_of=base + timedelta(hours=1), coin_id=52))
    assert (shadow.status, shadow.shadow_only, shadow.unscored) == ("admitted", True, True)
    strict = t.admit(candidate(CandidateSource.MIGRATION, as_of=base + timedelta(hours=5), coin_id=52))
    assert (strict.status, strict.unscored) == ("admitted", False)


def alerts(db: QuantDb, kind: str) -> list[str]:
    with db.admin.connect() as conn:
        return [
            str(d) for d in conn.execute(sa.select(AlertRow.detail).where(AlertRow.kind == kind)).scalars()
        ]


class Failing:
    async def aget_state(self, config: Any) -> Any:
        raise RuntimeError("meeting crashed")

    async def ainvoke(self, input: Any, config: Any, **kwargs: Any) -> Any:
        raise AssertionError("never reached")


def test_a_meeting_failing_permanently_backs_off_then_is_counted_and_alerted(quant_db: QuantDb) -> None:
    t = trigger(quant_db)
    admitted = t.admit(candidate(as_of=AS_OF + timedelta(days=40), coin_id=61))
    clock = {"now": AS_OF + timedelta(days=40, seconds=5)}
    worker = MeetingWorker(quant_db.council, Failing(), max_attempts=2, clock=lambda: clock["now"])
    before = COUNCIL_EVENTS_FAILED._value.get()
    ours = [e for e in worker._open() if e.event_id == admitted.event_id]
    assert asyncio.run(worker.run_event(ours[0])) is False
    assert all(e.event_id != admitted.event_id for e in worker._open())  # waiting out the retry delay
    clock["now"] += timedelta(minutes=5)
    [again] = [e for e in worker._open() if e.event_id == admitted.event_id]
    assert asyncio.run(worker.run_event(again)) is False
    assert COUNCIL_EVENTS_FAILED._value.get() == before + 1
    assert any(admitted.event_id in d for d in alerts(quant_db, "council_event_failed"))
    crashed = PendingEvent(admitted.event_id, {"coin_id": 61}, {}, False, False, attempts=2)
    assert asyncio.run(worker.run_event(crashed)) is False  # attempts used up by crashes: failed, not run
