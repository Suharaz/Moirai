from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest
from redis.asyncio import Redis

from hdt.contracts import Candidate, CandidateSource, TargetType
from hdt.contracts.streams import Stream
from hdt.core.streams import StreamConsumer, publish

pytestmark = pytest.mark.redis

T0 = datetime(2026, 9, 27, 10, 0, tzinfo=UTC)


def _candidate(coin: int) -> Candidate:
    return Candidate(
        coin_id=coin,
        as_of=T0,
        source=CandidateSource.LTX,
        score=1.0,
        rule_version="r1",
        target_type=TargetType.RESID_12H,
        label_spec_version="l1",
    )


def _consumer(redis: Redis, name: str, handler, reclaim_idle_ms: int = 60000) -> StreamConsumer[Candidate]:  # type: ignore[no-untyped-def]
    return StreamConsumer(
        redis=redis,
        stream=Stream.CANDIDATES,
        group="trigger",
        consumer=name,
        model=Candidate,
        handler=handler,
        block_ms=10,
        reclaim_idle_ms=reclaim_idle_ms,
    )


async def test_message_is_acked_only_after_durable_handling(redis_client: Redis) -> None:
    seen: list[int] = []

    async def failing(_id: str, msg: Candidate) -> bool:
        seen.append(msg.coin_id)
        return False

    first = _consumer(redis_client, "c1", failing)
    await first.start()
    await publish(redis_client, Stream.CANDIDATES, _candidate(7))
    assert await first.run_once() == 0
    pending = await redis_client.xpending(str(Stream.CANDIDATES), "trigger")
    assert pending["pending"] == 1

    async def ok(_id: str, msg: Candidate) -> bool:
        seen.append(msg.coin_id)
        return True

    second = _consumer(redis_client, "c2", ok, reclaim_idle_ms=1)
    assert await second.run_once() == 1  # reclaimed from the crashed consumer
    assert seen == [7, 7]
    assert (await redis_client.xpending(str(Stream.CANDIDATES), "trigger"))["pending"] == 0


async def test_unparseable_message_goes_to_dead_letter(redis_client: Redis) -> None:
    async def never(_id: str, _msg: Candidate) -> bool:
        raise AssertionError("handler must not run for invalid messages")

    consumer = _consumer(redis_client, "c1", never)
    await consumer.start()
    await redis_client.xadd(
        str(Stream.CANDIDATES), {"type": "Candidate", "v": "9", "data": '{"coin_id": -1}'}
    )
    assert await consumer.run_once() == 1
    dead = await redis_client.xrange(str(Stream.DEAD_LETTER))
    assert len(dead) == 1
    assert (await redis_client.xpending(str(Stream.CANDIDATES), "trigger"))["pending"] == 0


async def test_poison_message_is_dead_lettered_after_max_deliveries(redis_client: Redis) -> None:
    async def always_fails(_id: str, _msg: Candidate) -> bool:
        raise RuntimeError("handler bug")

    consumer = StreamConsumer(
        redis=redis_client,
        stream=Stream.CANDIDATES,
        group="trigger",
        consumer="c1",
        model=Candidate,
        handler=always_fails,
        block_ms=10,
        reclaim_idle_ms=0,
        max_deliveries=3,
    )
    await consumer.start()
    await publish(redis_client, Stream.CANDIDATES, _candidate(9))
    for _ in range(3):
        assert await consumer.run_once() == 0
    assert await consumer.run_once() == 1
    dead = await redis_client.xrange(str(Stream.DEAD_LETTER))
    assert len(dead) == 1
    assert b"poison" in dead[0][1][b"reason"]
    assert (await redis_client.xpending(str(Stream.CANDIDATES), "trigger"))["pending"] == 0


async def test_run_loop_stops_when_asked(redis_client: Redis) -> None:
    handled: list[int] = []
    stop = asyncio.Event()

    async def handle(_id: str, msg: Candidate) -> bool:
        handled.append(msg.coin_id)
        stop.set()
        return True

    consumer = _consumer(redis_client, "c1", handle)
    await consumer.start()
    await publish(redis_client, Stream.CANDIDATES, _candidate(11))
    await asyncio.wait_for(consumer.run(stop), timeout=5)
    assert handled == [11]
