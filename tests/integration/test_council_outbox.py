"""Decision outbox on Postgres + Redis under the council role (test-strategy 4): the card, forecasts, claims
and outbox row are written in one transaction; the relay delivers each DecisionMsg to `decisions` exactly
once, however late (a decision has no time limit); the blind round-1 commit reaches the append-only audit
log."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta
from typing import Any

import pytest
import sqlalchemy as sa
from langgraph.checkpoint.memory import InMemorySaver
from redis.asyncio import Redis

from council_builders import AGENTS, AS_OF, LONG_B, ScriptedRunner, Turn, claim, event_input, services
from fixtures.quant_db import QuantDb
from hdt.contracts.common import AgentName, ClaimKind, Intent
from hdt.contracts.decision import DecisionMsg
from hdt.core.streams import Stream
from hdt.council.commit import CommitMismatchError, RoundCommit
from hdt.council.decision_card import AUDIT_ACTION, DecisionOutbox, PgDecisionStore
from hdt.council.graph import CouncilGraph, DecisionRecord
from hdt.memory.episodic import EpisodicReader
from hdt.memory.store import open_postgres_store

pytestmark = [pytest.mark.pg, pytest.mark.redis, pytest.mark.integration]


class CapturingStore(PgDecisionStore):
    """The Postgres store, keeping the last record so a test can re-emit or corrupt it."""

    last: DecisionRecord | None = None

    def emit(self, record: DecisionRecord) -> bool:
        self.last = record
        return super().emit(record)


def turns() -> dict[tuple[AgentName, int], Turn]:
    first = {(a, 1): Turn(0.66, candidate_id=LONG_B) for a in AGENTS}
    first[(AgentName.TECHNICAL, 1)] = Turn(
        0.66, claims=(claim("x", ClaimKind.PACKET, "rsi_14", "RSI 61", value=61.0),), candidate_id=LONG_B
    )
    return first


async def meet(store: PgDecisionStore, event_id: str) -> DecisionRecord:
    runner = ScriptedRunner(p_model=dict.fromkeys(AGENTS, 0.62), turns=turns())
    app = CouncilGraph(services(runner, store=store)).compile(InMemorySaver())
    await app.ainvoke(
        event_input(event_id=event_id), {"configurable": {"thread_id": event_id}}, durability="sync"
    )
    assert isinstance(store, CapturingStore)
    assert store.last is not None
    return store.last


def count(engine: sa.Engine, table: str, event_id: str) -> int:
    with engine.connect() as conn:
        return int(
            conn.execute(
                sa.text(f"SELECT count(*) FROM {table} WHERE event_id = :e"), {"e": event_id}
            ).scalar_one()
        )


def url_of(engine: sa.Engine) -> str:
    return engine.url.render_as_string(hide_password=False)


async def test_card_rows_and_outbox_are_written_together(quant_db: QuantDb) -> None:
    with open_postgres_store(url_of(quant_db.council)) as memory:
        store = CapturingStore(quant_db.council, memory=memory, clock=lambda: AS_OF + timedelta(seconds=30))
        record = await meet(store, "ev_outbox_1")
        assert record.decision is not None
        assert record.decision.intent is Intent.OPEN
        for table, expected in (
            ("decision_cards", 1),
            ("decision_forecasts", len(AGENTS)),
            ("decision_claims", 1),
            ("decision_outbox", 1),
        ):
            assert count(quant_db.council, table, "ev_outbox_1") == expected
        with quant_db.council.connect() as conn:
            row = conn.execute(
                sa.text("SELECT outcome, candidate_id, card_sha256 FROM decision_cards WHERE event_id = :e"),
                {"e": "ev_outbox_1"},
            ).one()
        assert (row.outcome, row.candidate_id, row.card_sha256) == (
            "LONG",
            LONG_B,
            record.card["card_sha256"],
        )
        # a retried emit (resumed node) writes nothing new and keeps the episodes identical
        assert store.emit(record) is False
        assert count(quant_db.council, "decision_forecasts", "ev_outbox_1") == len(AGENTS)
        episodes = EpisodicReader(memory, AgentName.MICRO, AS_OF + timedelta(days=1)).recent(
            coin_id=record.card["coin_id"]
        )
        assert [e.decision.event_id for e in episodes] == ["ev_outbox_1"]
        assert episodes[0].decision.council_intent is Intent.OPEN


async def test_a_failing_row_rolls_back_the_whole_decision(quant_db: QuantDb) -> None:
    store = CapturingStore(quant_db.council)
    record = await meet(store, "ev_outbox_2")
    with quant_db.admin.begin() as conn:
        for table in ("decision_outbox", "decision_claims", "decision_forecasts", "decision_cards"):
            conn.execute(sa.text(f"DELETE FROM {table} WHERE event_id = 'ev_outbox_2'"))
    broken = replace(record, forecasts=[*record.forecasts, record.forecasts[0]])  # duplicate primary key
    with pytest.raises(sa.exc.IntegrityError):
        store.emit(broken)
    for table in ("decision_cards", "decision_forecasts", "decision_claims", "decision_outbox"):
        assert count(quant_db.council, table, "ev_outbox_2") == 0


async def test_relay_publishes_each_decision_exactly_once(quant_db: QuantDb, redis_client: Redis) -> None:
    store = CapturingStore(quant_db.council)
    await meet(store, "ev_outbox_3")
    now: dict[str, datetime] = {"t": AS_OF + timedelta(seconds=40)}
    outbox = DecisionOutbox(engine=quant_db.council, redis=redis_client, clock=lambda: now["t"])
    first = await outbox.relay_once()
    second = await outbox.relay_once()
    assert second == 0
    assert first >= 1
    entries: Any = await redis_client.xrange(str(Stream.DECISIONS))
    msgs = [DecisionMsg.model_validate_json(fields[b"data"]) for _, fields in entries]
    assert [m.event_id for m in msgs].count("ev_outbox_3") == 1
    with quant_db.council.connect() as conn:
        row = conn.execute(
            sa.text("SELECT status, stream_id FROM decision_outbox WHERE event_id = 'ev_outbox_3'")
        ).one()
    assert row.status == "published"
    assert row.stream_id is not None


async def test_a_decision_is_relayed_however_long_after_its_candidate(
    quant_db: QuantDb, redis_client: Redis
) -> None:
    """Owner decision 2026-09-28: the council emits no `expires_at` and the relay never expires a decision
    for its age (before, a relay past candidate `as_of` + 120 s marked it `expired` and Risk never saw it)."""
    store = CapturingStore(quant_db.council)
    record = await meet(store, "ev_outbox_4")
    assert record.decision is not None
    assert record.decision.expires_at is None
    late = record.decision.as_of + timedelta(hours=1)
    outbox = DecisionOutbox(engine=quant_db.council, redis=redis_client, clock=lambda: late)
    await outbox.relay_once()
    entries: Any = await redis_client.xrange(str(Stream.DECISIONS))
    sent = [DecisionMsg.model_validate_json(f[b"data"]) for _, f in entries]
    assert [m.event_id for m in sent].count("ev_outbox_4") == 1
    with quant_db.council.connect() as conn:
        status = conn.execute(
            sa.text("SELECT status FROM decision_outbox WHERE event_id = 'ev_outbox_4'")
        ).scalar_one()
    assert status == "published"


async def test_round_one_commit_is_logged_once_and_cannot_be_rewritten(quant_db: QuantDb) -> None:
    store = CapturingStore(quant_db.council)
    record = await meet(store, "ev_outbox_5")
    with quant_db.admin.connect() as conn:
        logged = (
            conn.execute(
                sa.text(
                    "SELECT diff_redacted FROM audit_log "
                    "WHERE action = :a AND diff_redacted->>'event_id' = :e"
                ),
                {"a": AUDIT_ACTION, "e": "ev_outbox_5"},
            )
            .scalars()
            .all()
        )
    assert len(logged) == 1
    assert logged[0]["round_sha256"] == record.card["round1_sha256"]
    tampered = RoundCommit("ev_outbox_5", 1, {"crowding": "0" * 64}, "1" * 64)
    with pytest.raises(CommitMismatchError):
        store.commit_round(tampered, AS_OF)
    with pytest.raises(sa.exc.ProgrammingError), quant_db.council.begin() as conn:
        conn.execute(
            sa.text("UPDATE decision_commits SET forecast_sha256 = 'x' WHERE event_id = 'ev_outbox_5'")
        )
