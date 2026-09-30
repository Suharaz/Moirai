"""Risk consumer idempotency end to end (dev Postgres + Redis + in-process exchange).

Success criterion: the same `DecisionMsg` delivered 3 times (including after a risk restart) gives exactly
one entry; a decision with a wrong packet hash gives 0 orders; a held coin receiving a same-direction
decision gives `HOLD` and 0 new orders. A new consumer group starts at the stream tail. A decision has no
time limit (owner decision 2026-09-28): one that reaches Risk 30 minutes after its candidate is judged
against the current mark only.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

import pytest
import sqlalchemy as sa
from redis.asyncio import Redis
from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

from alert_logs import raised
from fake_exchange import FakeClock, ledger_sessions, leg_of_call, make_harness
from hdt.contracts.account import AccountState
from hdt.contracts.common import Account, Leg
from hdt.contracts.decision import DecisionMsg
from hdt.contracts.order import OrderIntent
from hdt.contracts.streams import Stream
from hdt.core.clock import utcnow
from hdt.core.ids import to_canonical
from hdt.core.streams import RawPayload, publish
from hdt.db.models.quant import QuantPacketRow
from hdt.db.models.risk import ProcessedDecisionRow, RiskIntentRow, RiskVerdictRow
from hdt.quant.packet import store_candidate_set, store_packet
from hdt.risk.consumer import AccountStateBook, DecisionConsumer, RiskRuntime, stream_consumer
from hdt.risk.gate import MarketInputs
from hdt.risk.veto import FlagsBook
from intent_builders import KEYRING, RISK_SIGNER
from risk_builders import (
    NOW,
    account_state,
    candidate_set,
    decision,
    default_candidate_set,
    default_packet,
    flags,
    market,
    packet,
    position,
    risk_file,
)

pytestmark = [pytest.mark.pg, pytest.mark.redis, pytest.mark.integration]

PLAN_LEGS = [Leg.SL, Leg.TP1, Leg.TRAIL, Leg.ENTRY_IOC, Leg.ENTRY]


class FixedMarket:
    def __init__(self, inputs: MarketInputs) -> None:
        self.inputs = inputs
        self.reads = 0

    def market(self, coin_id: int, now: datetime) -> MarketInputs:
        self.reads += 1
        return self.inputs


def _runtime() -> RiskRuntime:
    return RiskRuntime(risk=risk_file(), stale_s=180.0, config_version_ids={"risk": 1}, mode=Account.PAPER)


def _consumer(
    sessions: sessionmaker[Session],
    redis: Redis,
    *,
    clock: FakeClock | None = None,
    state: AccountState | None = None,
) -> DecisionConsumer:
    states = AccountStateBook()
    states.offer(state or account_state())
    book = FlagsBook.empty()
    book.offer(flags())
    return DecisionConsumer(
        account=Account.PAPER,
        sessions=sessions,
        redis=redis,
        signer=RISK_SIGNER,
        market=FixedMarket(market()),
        flags=book,
        states=states,
        runtime=_runtime,
        clock=clock or FakeClock(NOW),
    )


@pytest.fixture
def sessions(pg_engine: Engine) -> sessionmaker[Session]:
    factory = ledger_sessions(pg_engine)
    with pg_engine.begin() as conn:
        store_candidate_set(conn, default_candidate_set())
        store_packet(conn, default_packet())
    return factory


async def _orders(redis: Redis) -> list[OrderIntent]:
    entries = await redis.xrange(str(Stream.ORDERS))
    return [OrderIntent.model_validate_json(fields[b"data"]) for _id, fields in entries]


async def _restart_and_redeliver(sessions: sessionmaker[Session], redis: Redis, d: DecisionMsg) -> None:
    """A fresh risk process reading the same message from `decisions` (reclaims overdue pending ones).

    The group exists before the message is published, as in production where the first run created it."""
    restarted = _consumer(sessions, redis)
    reader = stream_consumer(restarted, block_ms=10, reclaim_idle_ms=0)
    await reader.start()
    await publish(redis, Stream.DECISIONS, d)
    assert await reader.run_once() == 1
    assert await redis.xpending(str(Stream.DECISIONS), reader.group) == {
        "pending": 0,
        "min": None,
        "max": None,
        "consumers": [],
    }


def _count(sessions: sessionmaker[Session], model: type, **where: object) -> int:
    with sessions() as s:
        q = sa.select(sa.func.count()).select_from(model)
        for column, value in where.items():
            q = q.where(getattr(model, column) == value)
        return int(s.scalar(q) or 0)


async def test_same_decision_three_times_including_after_a_restart_gives_one_entry(
    sessions: sessionmaker[Session], redis_client: Redis
) -> None:
    d = decision()
    raw = RawPayload(d.model_dump(mode="json"))
    first = _consumer(sessions, redis_client)
    assert await first.handle("1-1", raw)
    assert await first.handle("1-2", raw)
    await _restart_and_redeliver(sessions, redis_client, d)

    intents = await _orders(redis_client)
    assert [i.leg for i in intents] == PLAN_LEGS
    assert all(i.signature for i in intents)
    assert _count(sessions, ProcessedDecisionRow, event_id=d.event_id) == 1
    assert _count(sessions, RiskVerdictRow, event_id=d.event_id, verdict="approved") == 1
    assert _count(sessions, RiskIntentRow, event_id=d.event_id, leg=Leg.ENTRY.value) == 1

    # Execution receives every published intent twice (stream redelivery): one entry order, ever.
    h = make_harness(sessions, Account.PAPER, KEYRING, clock=FakeClock(NOW))
    await h.start()
    for n, intent in enumerate(intents * 2):
        await h.intake.handle(f"9-{n}", RawPayload(json.loads(intent.model_dump_json())))
    entries = [c for c in h.exchange.mutating_calls("place_order") if leg_of_call(c) is Leg.ENTRY]
    assert len(entries) == 1
    (entry,) = [i for i in intents if i.leg is Leg.ENTRY]
    assert h.exchange.venue.orders[entry.client_id].qty == entry.qty


async def test_crash_between_commit_and_publish_is_relayed_exactly_once_after_restart(
    sessions: sessionmaker[Session], redis_client: Redis
) -> None:
    d = decision()
    crashed = _consumer(sessions, redis_client)
    assert crashed.decide("1-1", d, market(), _runtime(), NOW) is not None  # committed, never relayed
    assert await _orders(redis_client) == []
    await _restart_and_redeliver(sessions, redis_client, d)
    intents = await _orders(redis_client)
    assert [i.leg for i in intents] == PLAN_LEGS
    assert await crashed.relay_once() == 0
    assert len(await _orders(redis_client)) == len(PLAN_LEGS)


async def test_unverifiable_decision_produces_no_orders(
    sessions: sessionmaker[Session], redis_client: Redis
) -> None:
    d = decision(event_id="evt-hash", packet_sha256="ab" * 32)
    consumer = _consumer(sessions, redis_client)
    assert await consumer.handle("1-1", RawPayload(d.model_dump(mode="json")))
    assert _count(sessions, RiskVerdictRow, event_id=d.event_id, verdict="rejected") == 1
    assert _count(sessions, RiskIntentRow) == 0
    assert await _orders(redis_client) == []


async def test_held_coin_same_direction_decision_is_hold_with_no_new_order(
    sessions: sessionmaker[Session], redis_client: Redis
) -> None:
    held = account_state(positions=(position(qty="9"),))
    consumer = _consumer(sessions, redis_client, state=held)
    d = decision(event_id="evt-held")
    assert await consumer.handle("1-1", RawPayload(d.model_dump(mode="json")))
    with sessions() as s:
        verdict = s.scalars(sa.select(RiskVerdictRow).where(RiskVerdictRow.event_id == d.event_id)).one()
    assert (verdict.verdict, verdict.effective_intent) == ("hold", "HOLD")
    assert _count(sessions, RiskIntentRow) == 0
    assert await _orders(redis_client) == []


async def test_malformed_decision_is_dead_lettered_and_acked(
    sessions: sessionmaker[Session], redis_client: Redis
) -> None:
    consumer = _consumer(sessions, redis_client)
    payload = {**decision(event_id="evt-bad").model_dump(mode="json"), "schema_version": 99, "p": "high"}
    assert await consumer.handle("1-1", RawPayload(payload))
    dead = await redis_client.xrange(str(Stream.DEAD_LETTER))
    assert len(dead) == 1
    assert _count(sessions, RiskVerdictRow) == 0
    assert await _orders(redis_client) == []


async def test_new_consumer_group_starts_at_the_stream_tail(
    sessions: sessionmaker[Session], redis_client: Redis
) -> None:
    # A decision published before the namespace was ever attached is not replayed on its first attach.
    await publish(redis_client, Stream.DECISIONS, decision(event_id="evt-backlog"))
    reader = stream_consumer(_consumer(sessions, redis_client), block_ms=10, reclaim_idle_ms=0)
    await reader.start()
    assert await reader.run_once() == 0
    await publish(redis_client, Stream.DECISIONS, decision(event_id="evt-new"))
    assert await reader.run_once() == 1
    assert _count(sessions, ProcessedDecisionRow, event_id="evt-backlog") == 0
    assert _count(sessions, ProcessedDecisionRow, event_id="evt-new") == 1


def _late_consumer(sessions: sessionmaker[Session], redis: Redis, mark: str) -> DecisionConsumer:
    """Risk 30 minutes after the candidate `as_of` (NOW): fresh AccountState and flags, mark `mark`."""
    late = NOW + timedelta(minutes=30)
    consumer = _consumer(sessions, redis, clock=FakeClock(late))
    consumer.states.offer(account_state(ts=late))
    consumer.flags.offer(flags(now=late))
    consumer.market = FixedMarket(market(mark=mark))
    return consumer


async def test_a_decision_after_a_30_minute_debate_is_approved_while_its_levels_hold(
    sessions: sessionmaker[Session], redis_client: Redis
) -> None:
    """Before: rejected `ttl` (older than 120 s) without a market read, so a long debate never traded."""
    consumer = _late_consumer(sessions, redis_client, "100.50")
    d = decision(event_id="evt-slow-debate")
    assert await consumer.handle("1-1", RawPayload(d.model_dump(mode="json")))
    with sessions() as s:
        verdict = s.scalars(sa.select(RiskVerdictRow).where(RiskVerdictRow.event_id == d.event_id)).one()
    assert (verdict.verdict, verdict.reason) == ("approved", None)
    assert [i.leg for i in await _orders(redis_client)] == PLAN_LEGS


async def test_a_late_open_whose_stop_the_mark_crossed_is_rejected_with_its_reason(
    sessions: sessionmaker[Session], redis_client: Redis
) -> None:
    consumer = _late_consumer(sessions, redis_client, "97.90")  # the LONG stop is 98.00
    d = decision(event_id="evt-crossed")
    assert await consumer.handle("1-1", RawPayload(d.model_dump(mode="json")))
    with sessions() as s:
        verdict = s.scalars(sa.select(RiskVerdictRow).where(RiskVerdictRow.event_id == d.event_id)).one()
    assert (verdict.verdict, verdict.reason) == ("rejected", "stop_crossed")
    assert any("97.90" in c["text"] and not c["passed"] for c in verdict.checks)
    assert _count(sessions, RiskIntentRow) == 0
    assert await _orders(redis_client) == []


async def test_packet_row_holding_another_packet_is_an_integrity_rejection(
    sessions: sessionmaker[Session], redis_client: Redis, caplog: pytest.LogCaptureFixture
) -> None:
    # The row is keyed by the hash the decision names but holds another self-consistent packet.
    other = packet(candidate_set(), features={"atr_1h": 1.6})
    key = "cd" * 32
    with sessions.begin() as s:
        s.add(
            QuantPacketRow(
                packet_sha256=key,
                agent=other.agent.value,
                coin_id=other.coin_id,
                as_of=other.as_of,
                candidate_set_sha256=other.candidate_set_sha256,
                payload=to_canonical(other),
                created_at=utcnow(),
            )
        )
    consumer = _consumer(sessions, redis_client)
    d = decision(event_id="evt-swapped", packet_sha256=key)
    with caplog.at_level(logging.WARNING):
        assert await consumer.handle("1-1", RawPayload(d.model_dump(mode="json")))
    with sessions() as s:
        verdict = s.scalars(sa.select(RiskVerdictRow).where(RiskVerdictRow.event_id == d.event_id)).one()
    assert (verdict.verdict, verdict.reason) == ("rejected", "integrity")
    assert [r for r in caplog.records if raised(r, "integrity_error")]
    assert _count(sessions, RiskIntentRow) == 0
    assert await _orders(redis_client) == []
