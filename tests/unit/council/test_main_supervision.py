"""N3: a council task that dies must stop the service instead of leaving it half running."""

from __future__ import annotations

import asyncio

from hdt.council.main import supervise


async def test_a_task_that_dies_stops_the_service_and_is_reported() -> None:
    stop = asyncio.Event()

    async def dies() -> None:
        raise RuntimeError("consumer died")

    async def loops() -> None:
        await stop.wait()

    died = await supervise(stop, [asyncio.create_task(dies()), asyncio.create_task(loops())])
    assert died is True
    assert stop.is_set()


async def test_a_stop_signal_drains_without_a_failure() -> None:
    stop = asyncio.Event()

    async def loops() -> None:
        await stop.wait()

    task = asyncio.create_task(loops())
    asyncio.get_running_loop().call_later(0.01, stop.set)
    assert await supervise(stop, [task]) is False
    assert task.done()
