"""Council checkpoint resume and event admission on Postgres under the council role (phase 06 success
criterion, test-strategy 4): a meeting killed in round 2 resumes by `thread_id = event_id` in a new graph
(new process) and completes the same event with the same card hash, without re-running finished agents."""

from __future__ import annotations

from datetime import timedelta

import pytest
import sqlalchemy as sa
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.memory import InMemorySaver

from council_builders import (
    AS_OF,
    LONG_B,
    MemoryDecisionStore,
    ScriptedRunner,
    Turn,
    candidate,
    claim,
    event_input,
    services,
    settings,
)
from fixtures.quant_db import QuantDb
from hdt.contracts.candidate import Candidate
from hdt.contracts.common import AgentName, CandidateSource, ClaimKind
from hdt.core.clock import utcnow
from hdt.council.checkpoint import open_checkpointer
from hdt.council.decision_card import PgDecisionStore
from hdt.council.graph import CouncilGraph
from hdt.council.ports import AgentRequest, AgentResult
from hdt.council.trigger import CouncilTrigger, MeetingWorker, event_id_for
from hdt.tools.base import ToolResult

pytestmark = [pytest.mark.pg, pytest.mark.integration]

C, T, M, F, N, X = (
    AgentName.CROWDING,
    AgentName.TECHNICAL,
    AgentName.MICRO,
    AgentName.FUNDAMENTAL,
    AgentName.NEWS,
    AgentName.MACRO,
)
P_MODEL = {C: 0.62, T: 0.45, M: 0.58, F: 0.6, N: 0.5, X: 0.52}
FEATURES = {C: {"ltx_strict_pass": True, "ltx_side": "LONG"}}
SNAPSHOT_A = ToolResult(
    tool="get_snapshot", status="ok", result_id="get_snapshot:" + "a" * 24, data={"x": 1.0}
)
SNAPSHOT_B = ToolResult(
    tool="get_snapshot", status="ok", result_id="get_snapshot:" + "b" * 24, data={"x": 2.0}
)
TURNS = {
    (C, 1): Turn(
        0.64,
        claims=(claim("a", ClaimKind.PACKET, "ltx_strict_pass", "LTX strict fired"),),
        candidate_id=LONG_B,
    ),
    (T, 1): Turn(0.42),
    (M, 1): Turn(0.60, candidate_id=LONG_B, tool_results=(SNAPSHOT_B, SNAPSHOT_A)),
    (F, 1): Turn(0.61, candidate_id=LONG_B),
    (N, 1): Turn(None),
    (X, 1): Turn(0.50),
    (T, 2): Turn(0.60, cite_where=(("statement", "LTX strict fired"),)),
    (M, 2): Turn(0.62, cite_where=(("statement", "LTX strict fired"),), candidate_id=LONG_B),
    (F, 2): Turn(0.63, cite_where=(("statement", "LTX strict fired"),), candidate_id=LONG_B),
    (X, 2): Turn(0.58, cite_where=(("statement", "LTX strict fired"),)),
    (C, 2): Turn(0.64),
}


class NoScannerLog:
    def shadow_only(self, candidate: Candidate) -> bool:
        return False


def url_of(engine: sa.Engine) -> str:
    return engine.url.render_as_string(hide_password=False)


def trigger(db: QuantDb) -> CouncilTrigger:
    council = settings().council
    return CouncilTrigger(
        db.council,
        pin=lambda: db.version_ids,
        council=lambda _ids: council,
        scanner_log=NoScannerLog(),
        clock=lambda: AS_OF + timedelta(seconds=2),
    )


async def uninterrupted(event_id: str, version_ids: dict[str, int]) -> tuple[str, ScriptedRunner]:
    runner = ScriptedRunner(p_model=P_MODEL, turns=TURNS, features=FEATURES)
    store = MemoryDecisionStore()
    app = CouncilGraph(services(runner, store=store)).compile(InMemorySaver())
    start = event_input(candidate(), event_id=event_id)
    start["event"]["config_version_ids"] = version_ids
    await app.ainvoke(start, {"configurable": {"thread_id": event_id}}, durability="sync")
    return store.records[event_id].card["card_sha256"], runner


async def test_meeting_killed_in_round_two_resumes_in_a_new_process(quant_db: QuantDb) -> None:
    cand = candidate()
    admitted = trigger(quant_db).admit(cand)
    assert admitted.status == "admitted"
    event_id = admitted.event_id
    dsn = url_of(quant_db.council)

    first = ScriptedRunner(p_model=P_MODEL, turns=TURNS, features=FEATURES, fail_on={(M, 2)})
    with open_checkpointer(dsn) as saver:
        app = CouncilGraph(services(first, store=PgDecisionStore(quant_db.council))).compile(saver)
        worker = MeetingWorker(quant_db.council, app, max_attempts=3)
        assert await worker.run_pending() == 0
        snapshot = await app.aget_state({"configurable": {"thread_id": event_id}})
        assert snapshot.values["round"] == 2
        assert "agent" in snapshot.next
    assert {(a, r) for _, a, r in first.calls if r == 1} == {(a, 1) for a in AgentName}
    with quant_db.council.connect() as conn:
        status, error = conn.execute(
            sa.text("SELECT status, error FROM council_events WHERE event_id = :e"), {"e": event_id}
        ).one()
    assert status == "pending"
    assert "killed" in error

    second = ScriptedRunner(p_model=P_MODEL, turns=TURNS, features=FEATURES)
    with open_checkpointer(dsn) as saver:
        app = CouncilGraph(services(second, store=PgDecisionStore(quant_db.council))).compile(saver)
        # the failed attempt waits out its retry delay first
        assert await MeetingWorker(quant_db.council, app).run_pending() == 0
        later = MeetingWorker(quant_db.council, app, clock=lambda: utcnow() + timedelta(minutes=10))
        assert await later.run_pending() == 1
    # round 1 and the round-2 agents that had finished are not asked again
    assert all(r == 2 for _, _, r in second.calls)
    assert (M, 2) in {(a, r) for _, a, r in second.calls}
    finished_before = {(a, r) for _, a, r in first.calls if r == 2}
    assert finished_before, "some round-2 agents finished before the kill"
    assert finished_before.isdisjoint({(a, r) for _, a, r in second.calls})

    with quant_db.council.connect() as conn:
        card_hash = conn.execute(
            sa.text("SELECT card_sha256 FROM decision_cards WHERE event_id = :e"), {"e": event_id}
        ).scalar_one()
        status = conn.execute(
            sa.text("SELECT status FROM council_events WHERE event_id = :e"), {"e": event_id}
        ).scalar_one()
    assert status == "done"
    expected_hash, whole = await uninterrupted(event_id, dict(quant_db.version_ids))
    assert card_hash == expected_hash
    # the resumed round-2 request carries the agent's own round-1 result, rebuilt from the checkpoint
    resumed = next(r for r in second.requests if (r.agent, r.round) == (M, 2))
    straight = next(r for r in whole.requests if (r.agent, r.round) == (M, 2))
    assert [e.forecast.round for e in resumed.earlier] == [1]
    assert [t.result_id for t in resumed.earlier[0].tool_results] == [
        SNAPSHOT_A.result_id,
        SNAPSHOT_B.result_id,
    ]
    assert resumed.earlier == straight.earlier


class FailsFor(ScriptedRunner):
    """Every agent call of `event_id` raises after `load_context` was checkpointed (the meeting fails)."""

    def __init__(self, event_id: str) -> None:
        super().__init__(p_model=P_MODEL, turns=TURNS, features=FEATURES)
        self.failing = event_id

    async def forecast(self, request: AgentRequest) -> AgentResult:
        if request.event_id == self.failing:
            raise RuntimeError("process killed mid-meeting")
        return await super().forecast(request)


async def test_checkpoints_of_failed_and_crash_leaked_done_events_are_pruned(quant_db: QuantDb) -> None:
    """m7: only a `done` event's thread was deleted, right after `_set(done)`: a failed event kept its
    checkpoints forever, and a crash between the two leaked the thread of a done one."""
    t = trigger(quant_db)
    first, second = t.admit(candidate(coin_id=7001)), t.admit(candidate(coin_id=7002))
    assert (first.status, second.status) == ("admitted", "admitted")
    failing, leaked = first.event_id, second.event_id
    dsn = url_of(quant_db.council)
    with open_checkpointer(dsn) as saver:
        # A worker without `checkpoints` stands for a crash before the delete: both threads stay behind.
        runner = FailsFor(failing)
        app = CouncilGraph(services(runner, store=PgDecisionStore(quant_db.council))).compile(saver)
        await MeetingWorker(quant_db.council, app, max_attempts=1).run_pending()
        threads: dict[str, RunnableConfig] = {
            e: {"configurable": {"thread_id": e, "checkpoint_ns": ""}} for e in (failing, leaked)
        }
        with quant_db.council.connect() as conn:
            rows = conn.execute(
                sa.text("SELECT event_id, status FROM council_events WHERE event_id IN (:a, :b)"),
                {"a": failing, "b": leaked},
            ).all()
        statuses: dict[str, str] = {str(r.event_id): str(r.status) for r in rows}
        assert statuses == {failing: "failed", leaked: "done"}
        assert all(saver.get_tuple(cfg) is not None for cfg in threads.values())

        worker = MeetingWorker(quant_db.council, app, checkpoints=saver)
        assert await worker.prune_checkpoints() >= 2
        assert all(saver.get_tuple(cfg) is None for cfg in threads.values())
        assert await worker.prune_checkpoints() == 0
    with quant_db.council.connect() as conn:
        pruned: int = conn.execute(
            sa.text(
                "SELECT count(*) FROM council_events "
                "WHERE event_id IN (:a, :b) AND checkpoint_pruned_at IS NOT NULL"
            ),
            {"a": failing, "b": leaked},
        ).scalar_one()
    assert pruned == 2


def test_trigger_spacing_windows_and_duplicates(quant_db: QuantDb) -> None:
    t = trigger(quant_db)
    coin = 777
    base = AS_OF + timedelta(days=2)
    first = t.admit(candidate(as_of=base, coin_id=coin))
    assert (first.status, first.unscored) == ("admitted", False)
    assert t.admit(candidate(as_of=base, coin_id=coin)).status == "duplicate"
    # within 15 minutes of an admitted event of the same coin: skipped
    close = t.admit(candidate(CandidateSource.MIGRATION, as_of=base + timedelta(minutes=10), coin_id=coin))
    assert close.status == "skipped"
    # after the spacing but inside the 12 h scoring window: a meeting, unscored
    later = t.admit(candidate(as_of=base + timedelta(minutes=30), coin_id=coin))
    assert (later.status, later.unscored) == ("admitted", True)
    # the next non-overlapping window scores again
    next_window = t.admit(candidate(as_of=base + timedelta(hours=12, minutes=1), coin_id=coin))
    assert (next_window.status, next_window.unscored) == ("admitted", False)
    # a held-coin re-evaluation is always unscored
    held = t.admit(candidate(CandidateSource.HELD, as_of=base + timedelta(hours=30), coin_id=coin))
    assert (held.status, held.unscored) == ("admitted", True)
    assert event_id_for(candidate(as_of=base, coin_id=coin)) == first.event_id
    assert len(first.event_id) <= 80
