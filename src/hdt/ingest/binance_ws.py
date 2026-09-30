"""Binance public WebSocket recorder: `/public` (depth), `/market` (kline, aggTrade, markPrice, forceOrder).

Messages are buffered and written to the lake once per `flush_s` as one record per stream family
(`ws_depth20`, `ws_kline_5m`, `ws_aggtrade`, ...), each holding `{"recv_ts", "stream", "data"}` items.
Partial-book depth keeps only the last 20-level snapshot per symbol per flush (a 1 s snapshot); the
100 ms diff stream (`<symbol>@depth@100ms`, hot set only) keeps every event.

Gap detection (`GapDetector`): depth by `pu != previous u`, aggTrade by aggregate id jumps, markPrice by
event-time jumps over 3 s; a reconnect is always one gap per stream family. The connection is recycled
before Binance's 24 h limit.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections import deque
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Final

import websockets

from hdt.core.clock import utcnow
from hdt.core.ids import canonical_json
from hdt.lake.schemas import Capture

log = logging.getLogger(__name__)

MARK_PRICE_MAX_GAP_MS: Final[int] = 3000
# Partial-book streams are full snapshots: only the newest per flush is kept. Diff streams keep every event.
SNAPSHOT_FAMILIES: Final[frozenset[str]] = frozenset({"ws_depth5", "ws_depth10", "ws_depth20"})


def stream_family(stream: str) -> str:
    """`btcusdt@depth20@100ms` -> `ws_depth20`; `btcusdt@kline_5m` -> `ws_kline_5m`; `!forceOrder@arr`."""
    parts = stream.split("@")
    kind = parts[1] if len(parts) > 1 else parts[0]
    if stream.startswith("!forceorder") or stream.startswith("!forceOrder"):
        return "ws_forceorder"
    return "ws_" + kind.lower()


@dataclass
class GapDetector:
    """Per-stream continuity check; returns True when the new message proves messages were lost."""

    _last: dict[str, int] = field(default_factory=dict)

    def observe(self, stream: str, data: dict[str, Any]) -> bool:
        family = stream_family(stream)
        if family.startswith("ws_depth"):
            current, previous = data.get("u"), data.get("pu")
            last = self._last.get(stream)
            if isinstance(current, int):
                self._last[stream] = current
            return last is not None and isinstance(previous, int) and previous != last
        if family == "ws_aggtrade":
            agg = data.get("a")
            last = self._last.get(stream)
            if isinstance(agg, int):
                self._last[stream] = agg
                return last is not None and agg > last + 1
            return False
        if family == "ws_markprice":
            event = data.get("E")
            last = self._last.get(stream)
            if isinstance(event, int):
                self._last[stream] = event
                return last is not None and event - last > MARK_PRICE_MAX_GAP_MS
        return False

    def reset(self) -> None:
        self._last.clear()


@dataclass
class _Buffer:
    items: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    depth: dict[str, dict[str, Any]] = field(default_factory=dict)

    def add(self, stream: str, data: Any, recv_ts: int) -> None:
        item = {"recv_ts": recv_ts, "stream": stream, "data": data}
        family = stream_family(stream)
        if family in SNAPSHOT_FAMILIES:
            self.depth[stream] = item
        else:
            self.items.setdefault(family, []).append(item)

    def drain(self) -> dict[str, list[dict[str, Any]]]:
        out = self.items
        for stream, item in self.depth.items():
            out.setdefault(stream_family(stream), []).append(item)
        self.items, self.depth = {}, {}
        return out


CaptureWriter = Callable[[list[Capture]], Awaitable[None]]
HealthSink = Callable[[str, str, int, datetime | None, datetime | None], Awaitable[None]]


class BinanceWsRecorder:
    def __init__(
        self,
        *,
        name: str,
        base_url: str,
        streams: Iterable[str],
        write: CaptureWriter,
        health: HealthSink,
        flush_s: float = 1.0,
        recycle_after_s: float = 85_800,
        max_backoff_s: float = 60.0,
    ) -> None:
        self.name = name
        self.streams = sorted(set(streams))
        self.url = f"{base_url.rstrip('/')}/stream?streams={'/'.join(self.streams)}"
        self._write = write
        self._health = health
        self._flush_s = flush_s
        self._recycle_after_s = recycle_after_s
        self._max_backoff_s = max_backoff_s
        self.gaps = GapDetector()
        self._gap_times: deque[datetime] = deque()
        self._buffer = _Buffer()
        self._connected_since: datetime | None = None
        self._last_message_at: datetime | None = None

    async def run(self, stop: asyncio.Event) -> None:
        backoff = 1.0
        flusher = asyncio.create_task(self._flush_loop(stop))
        try:
            while not stop.is_set():
                try:
                    await self._session(stop)
                    backoff = 1.0
                except (OSError, websockets.WebSocketException) as exc:
                    log.warning(
                        "binance ws disconnected", extra={"ws": self.name, "error": type(exc).__name__}
                    )
                    await self._report("reconnecting")
                    await _sleep_or_stop(stop, backoff)
                    backoff = min(backoff * 2, self._max_backoff_s)
                self.gaps.reset()
                if not stop.is_set():
                    self._gap(utcnow())  # the reconnect itself loses messages
        finally:
            flusher.cancel()
            await self.flush()
            await self._report("down")

    async def _session(self, stop: asyncio.Event) -> None:
        async with websockets.connect(self.url, max_size=2**22, ping_interval=None) as ws:
            self._connected_since = utcnow()
            await self._report("connected")
            deadline = asyncio.get_running_loop().time() + self._recycle_after_s
            while not stop.is_set():
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    log.info("recycling binance ws before the 24 h limit", extra={"ws": self.name})
                    return
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=min(remaining, 5.0))
                except TimeoutError:
                    continue
                self.handle(raw, utcnow())

    def handle(self, raw: str | bytes, received_at: datetime) -> None:
        try:
            message = json.loads(raw)
            stream, data = message["stream"], message["data"]
        except (ValueError, KeyError, TypeError):
            log.warning("unexpected binance ws frame", extra={"ws": self.name})
            return
        self._last_message_at = received_at
        if isinstance(data, dict) and self.gaps.observe(stream, data):
            self._gap(received_at)
            log.warning("binance ws gap", extra={"ws": self.name, "stream": stream})
        self._buffer.add(stream, data, int(received_at.timestamp() * 1000))

    async def flush(self) -> None:
        batches = self._buffer.drain()
        if not batches:
            return
        now = utcnow()
        captures = [
            Capture(
                source="binance",
                route=family,
                fetched_at=now,
                http_status=101,
                body=canonical_json(items),
                params={"connection": self.name},
            )
            for family, items in sorted(batches.items())
        ]
        await self._write(captures)

    async def _flush_loop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await _sleep_or_stop(stop, self._flush_s)
            try:
                await self.flush()
            except Exception:
                log.exception("binance ws flush failed", extra={"ws": self.name})

    async def _report(self, status: str) -> None:
        try:
            await self._health(
                self.name, status, self.gaps_24h(), self._connected_since, self._last_message_at
            )
        except Exception:
            log.exception("ws health update failed", extra={"ws": self.name})

    def _gap(self, at: datetime) -> None:
        self._gap_times.append(at)

    def gaps_24h(self, now: datetime | None = None) -> int:
        cutoff = (now or utcnow()) - timedelta(hours=24)
        while self._gap_times and self._gap_times[0] < cutoff:
            self._gap_times.popleft()
        return len(self._gap_times)


async def _sleep_or_stop(stop: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except TimeoutError:
        return
