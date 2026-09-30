"""telegram-bot service supervision: a dead task stops the process, a failing iteration does not.

No database, Redis or Telegram is reached: the vault loader never returns a token, so the loop only runs
its secret refresh step, and Redis is a scripted stand-in.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from nacl.public import PrivateKey
from prometheus_client import REGISTRY
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker

from hdt.ops.telegram_bot import TelegramBotService
from hdt.vault import owner as owner_module
from hdt.vault.loader import SecretNotAvailableError
from hdt.vault.owner import OwnerWorker

HEARTBEAT = "hdt_telegram_poll_heartbeat_timestamp_seconds"


class Idle:
    async def run(self, stop: asyncio.Event) -> None:
        await stop.wait()


class Crashing:
    async def run(self, stop: asyncio.Event) -> None:
        raise RuntimeError("vault owner worker died")


class FlakyLoader:
    """First call: the database is down. Second call: stop the service (no secret configured)."""

    def __init__(self, stop: asyncio.Event) -> None:
        self._loop = asyncio.get_running_loop()
        self._stop = stop
        self.calls = 0

    def get(self, name: str) -> object:
        self.calls += 1
        if self.calls == 1:
            raise OperationalError("SELECT 1", {}, Exception("server closed the connection unexpectedly"))
        self._loop.call_soon_threadsafe(self._stop.set)
        raise SecretNotAvailableError(name)


class NoSecret:
    def get(self, name: str) -> object:
        raise SecretNotAvailableError(name)


def _service(*, loader: object, owner: object) -> TelegramBotService:
    return TelegramBotService(
        session_factory=sessionmaker(),
        redis=None,  # type: ignore[arg-type]
        loader=loader,  # type: ignore[arg-type]
        owner=owner,  # type: ignore[arg-type]
        monitor=Idle(),  # type: ignore[arg-type]
        totp_secret=None,
        controls_maxlen=None,
    )


async def test_a_dead_service_task_makes_run_return_non_zero() -> None:
    stop = asyncio.Event()
    service = _service(loader=NoSecret(), owner=Crashing())
    assert await asyncio.wait_for(service.run(stop), timeout=5) == 1


async def test_a_failing_iteration_is_retried_and_keeps_the_heartbeat(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(TelegramBotService, "ITERATION_ERROR_SLEEP_S", 0.01)
    errors_before = REGISTRY.get_sample_value("hdt_telegram_loop_errors_total", {"stage": "iteration"}) or 0.0
    stop = asyncio.Event()
    loader = FlakyLoader(stop)
    service = _service(loader=loader, owner=Idle())
    assert await asyncio.wait_for(service.run(stop), timeout=5) == 0
    assert loader.calls == 2  # the loop survived the database error and ran again
    errors = REGISTRY.get_sample_value("hdt_telegram_loop_errors_total", {"stage": "iteration"})
    assert errors == errors_before + 1
    assert (REGISTRY.get_sample_value(HEARTBEAT) or 0.0) > 0


class RestartingRedis:
    """Redis is down when the worker starts (the group cannot be created `down_starts` times), then comes
    back after a restart without its data (NOGROUP), then serves normally."""

    def __init__(self, down_starts: int = 1) -> None:
        self.down_starts = down_starts
        self.creates = 0
        self.claims = 0
        self.serving = asyncio.Event()

    async def xgroup_create(self, *args: Any, **kwargs: Any) -> None:
        self.creates += 1
        if self.creates <= self.down_starts:
            raise RedisConnectionError("Error 10061 connecting to redis:6379. Connection refused.")

    async def xautoclaim(self, *args: Any, **kwargs: Any) -> list[Any]:
        self.claims += 1
        if self.claims == 1:
            raise ResponseError("NOGROUP No such key 'secret_test_request' or consumer group 'owner-ops'")
        self.serving.set()
        return [b"0-0", []]

    async def xreadgroup(self, *args: Any, **kwargs: Any) -> list[Any]:
        await asyncio.sleep(0.01)  # a blocking read that returns nothing
        return []


async def test_redis_errors_do_not_kill_the_owner_worker_or_the_bot(monkeypatch: pytest.MonkeyPatch) -> None:
    # The service exits 1 as soon as one of its tasks ends, so an owner worker that died on a Redis
    # blip used to take `/kill` down with it (N4).
    monkeypatch.setattr(owner_module, "OWNER_RESTART_MIN_S", 0.01)
    redis = RestartingRedis()
    worker = OwnerWorker(
        scope="ops",
        redis=redis,  # type: ignore[arg-type]
        session_factory=sessionmaker(),
        private_key=PrivateKey.generate(),
        checks={},
        consumer="telegram-bot-test",
        block_ms=10,
    )
    stop = asyncio.Event()
    task = asyncio.create_task(_service(loader=NoSecret(), owner=worker).run(stop))
    await asyncio.wait_for(redis.serving.wait(), timeout=5)
    assert not task.done()
    stop.set()
    assert await asyncio.wait_for(task, timeout=5) == 0
    assert redis.creates == 3  # first start failed, then a restart after the connection error and NOGROUP


async def test_owner_backoff_restarts_from_the_minimum_once_the_group_exists(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # A long outage doubled the delay; after Redis came back, the next failure (NOGROUP) must not keep
    # waiting that long (review m5).
    monkeypatch.setattr(owner_module, "OWNER_RESTART_MIN_S", 0.01)
    redis = RestartingRedis(down_starts=3)
    worker = OwnerWorker(
        scope="ops",
        redis=redis,  # type: ignore[arg-type]
        session_factory=sessionmaker(),
        private_key=PrivateKey.generate(),
        checks={},
        consumer="telegram-bot-test",
        block_ms=10,
    )
    stop = asyncio.Event()
    with caplog.at_level("WARNING", logger=owner_module.log.name):
        task = asyncio.create_task(worker.run(stop))
        await asyncio.wait_for(redis.serving.wait(), timeout=5)
        stop.set()
        await asyncio.wait_for(task, timeout=5)
    delays = [
        getattr(r, "retry_in_s", None)
        for r in caplog.records
        if r.getMessage() == "owner worker lost Redis, restarting"
    ]
    assert delays == [0.01, 0.02, 0.04, 0.01]  # three failed starts, then NOGROUP after a good start
