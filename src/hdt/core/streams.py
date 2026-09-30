"""Redis Streams helper: publish contracts, consume with consumer groups, ack after the durable write.

Message layout: fields `type` (contract class name), `v` (schema version) and `data` (JSON body).
Consumers:
- create the group idempotently (`ensure_group`),
- reclaim messages left pending by a crashed consumer (`XAUTOCLAIM` older than `reclaim_idle_ms`);
  a message delivered more than `max_deliveries` times is a poison message: dead-lettered and acked,
- parse with the contract model; unparseable messages go to `dead_letter` and are acked,
- call the handler, which must make its effect durable and deduplicate by natural key (see
  `hdt.db.dedupe.insert_once`, in the same transaction as the effect) before returning `True`; only then
  the message is acked. A handler returning `False` or raising leaves it pending,
- `run(stop)` is the long-running XREADGROUP loop with capped exponential backoff on Redis errors.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, RootModel, ValidationError
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError
from redis.exceptions import TimeoutError as RedisTimeoutError

from hdt.contracts.streams import Stream
from hdt.core.clock import utcnow

log = logging.getLogger(__name__)

type Handler[M: BaseModel] = Callable[[str, M], Awaitable[bool]]


class RawPayload(RootModel[dict[str, Any]]):
    """Any JSON object: the handler validates the real contract itself so a failure can be alerted on."""


# redis-py >= 6 defaults to a 5 s socket timeout, which equals the XREADGROUP block time: a blocking read
# would time out whenever the stream stays idle. Services connect through `connect_redis`, whose read
# timeout outlasts any configured block.
REDIS_SOCKET_TIMEOUT_S = 30.0


def connect_redis(url: str) -> Redis:
    """Async Redis client for a service (stream consumers block up to `streams.block_ms`)."""
    client: Redis = Redis.from_url(url, socket_timeout=REDIS_SOCKET_TIMEOUT_S, socket_connect_timeout=5.0)
    return client


def encode(model: BaseModel) -> dict[str, str]:
    version = getattr(model, "schema_version", 1)
    return {"type": type(model).__name__, "v": str(version), "data": model.model_dump_json()}


async def publish(redis: Redis, stream: Stream | str, model: BaseModel, maxlen: int | None = None) -> str:
    """XADD a contract; returns the stream id."""
    fields: Any = encode(model)
    msg_id = await redis.xadd(str(stream), fields, maxlen=maxlen, approximate=maxlen is not None)
    return _s(msg_id)


async def ensure_group(redis: Redis, stream: Stream | str, group: str, start_id: str = "0") -> None:
    try:
        await redis.xgroup_create(str(stream), group, id=start_id, mkstream=True)
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


def _s(value: bytes | str) -> str:
    return value.decode("utf-8") if isinstance(value, bytes) else value


def _fields(raw: dict[Any, Any]) -> dict[str, str]:
    return {_s(k): _s(v) for k, v in raw.items()}


async def dead_letter(
    redis: Redis,
    *,
    stream: Stream | str,
    group: str,
    msg_id: str,
    reason: str,
    fields: dict[str, str],
    dead_letter_stream: Stream | str = Stream.DEAD_LETTER,
) -> None:
    """Copy one message to the dead-letter stream with the reason (the caller acks the original)."""
    body: Any = {
        "stream": str(stream),
        "group": group,
        "msg_id": msg_id,
        "reason": reason[:2000],
        "fields": json.dumps(fields)[:20000],
        "at": utcnow().isoformat(),
    }
    await redis.xadd(str(dead_letter_stream), body)
    log.error("message dead-lettered", extra={"stream": str(stream), "msg_id": msg_id})


@dataclass
class DeadLetter:
    stream: str
    group: str
    msg_id: str
    reason: str
    fields: dict[str, str]


@dataclass
class StreamConsumer[M: BaseModel]:
    redis: Redis
    stream: Stream | str
    group: str
    consumer: str
    model: type[M]
    handler: Handler[M]
    batch_size: int = 50
    block_ms: int = 5000
    reclaim_idle_ms: int = 60000
    max_deliveries: int = 10
    dead_letter_stream: Stream | str = Stream.DEAD_LETTER
    backoff_max_s: float = 30.0
    start_id: str = "0"  # where a new group starts: "0" the whole stream, "$" only new messages

    async def start(self) -> None:
        await ensure_group(self.redis, self.stream, self.group, start_id=self.start_id)

    async def run(self, stop: asyncio.Event) -> None:
        """Consume until `stop` is set. Redis errors back off exponentially (0.5 s doubling to the cap)."""
        await self.start()
        delay = 0.5
        while not stop.is_set():
            try:
                await self.run_once()
                delay = 0.5
            except (RedisConnectionError, RedisTimeoutError, OSError):
                log.warning("stream consumer lost Redis, backing off", extra={"stream": str(self.stream)})
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=delay)
                delay = min(delay * 2, self.backoff_max_s)

    async def run_once(self) -> int:
        """Process reclaimed + new messages once; returns the number of messages acked."""
        acked = await self._reclaim()
        response: Any = await self.redis.xreadgroup(
            self.group, self.consumer, {str(self.stream): ">"}, count=self.batch_size, block=self.block_ms
        )
        for _stream, messages in response or []:
            for msg_id, raw in messages:
                acked += await self._process(_s(msg_id), _fields(raw))
        return acked

    async def _reclaim(self) -> int:
        acked = 0
        cursor = "0-0"
        while True:
            result = await self.redis.xautoclaim(
                str(self.stream),
                self.group,
                self.consumer,
                min_idle_time=self.reclaim_idle_ms,
                start_id=cursor,
                count=self.batch_size,
            )
            cursor = _s(result[0])
            for msg_id, raw in result[1]:
                if raw is None:  # entry was trimmed from the stream
                    await self.redis.xack(str(self.stream), self.group, msg_id)
                    continue
                if await self._delivery_count(_s(msg_id)) > self.max_deliveries:
                    await self._dead_letter(_s(msg_id), "poison: exceeded max_deliveries", _fields(raw))
                    await self.redis.xack(str(self.stream), self.group, msg_id)
                    acked += 1
                    continue
                acked += await self._process(_s(msg_id), _fields(raw))
            if cursor == "0-0":
                return acked

    async def _delivery_count(self, msg_id: str) -> int:
        entries: Any = await self.redis.xpending_range(
            str(self.stream), self.group, min=msg_id, max=msg_id, count=1
        )
        return int(entries[0]["times_delivered"]) if entries else 0

    async def _process(self, msg_id: str, fields: dict[str, str]) -> int:
        try:
            model = self.model.model_validate_json(fields.get("data", ""))
        except (ValidationError, ValueError) as exc:
            await self._dead_letter(msg_id, f"parse error: {exc.__class__.__name__}: {exc}", fields)
            await self.redis.xack(str(self.stream), self.group, msg_id)
            return 1
        try:
            done = await self.handler(msg_id, model)
        except Exception:
            log.exception("stream handler failed", extra={"stream": str(self.stream), "msg_id": msg_id})
            return 0
        if done:
            await self.redis.xack(str(self.stream), self.group, msg_id)
            return 1
        return 0

    async def _dead_letter(self, msg_id: str, reason: str, fields: dict[str, str]) -> None:
        await dead_letter(
            self.redis,
            stream=self.stream,
            group=self.group,
            msg_id=msg_id,
            reason=reason,
            fields=fields,
            dead_letter_stream=self.dead_letter_stream,
        )
