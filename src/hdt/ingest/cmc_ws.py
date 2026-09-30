"""Optional CMC WebSocket route #30 `market@crypto_latest_price` (Startup plan and above, beta).

Consumer: Risk cross-checks the prices of held coins (at most `max_coins`, default 5). Protocol from the
official WebSocket overview:
- one endpoint `wss://pro-stream.coinmarketcap.com/v1`, API key in the `X-CMC_PRO_API_KEY` handshake
  header (never in the URL, so it can never reach a log line);
- the server greets with `{"type": "welcome", "ping_interval_ms": ...}`; the client sends
  `{"id": n, "method": "ping"}` at that interval and gets `{"type": "pong"}`;
- subscribe `{"id": n, "method": "subscribe", "channel": ..., "params": {"crypto_ids": [...]}}` is
  acknowledged with `{"type": "ack", "code": 0}`; data frames are `{"type": "data", "data": {...}, "ts"}`;
- billing: 0.025 credits per message received, metered into the credit governor as whole credits.

Frames are buffered and written to the lake once per `flush_s` (route `ws_crypto_latest_price`).
Close codes 4100/4101/4102 (invalid key, disabled key, plan without WS) stop the client; others reconnect
with exponential backoff.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable, Sequence
from datetime import datetime
from typing import Any, Final

import websockets
from websockets.exceptions import ConnectionClosed

from hdt.core.clock import utcnow
from hdt.core.ids import canonical_json
from hdt.ingest.cmc_client import KEY_HEADER
from hdt.lake.schemas import Capture

log = logging.getLogger(__name__)

CHANNEL: Final[str] = "market@crypto_latest_price"
ROUTE: Final[str] = "ws_crypto_latest_price"
CREDITS_PER_MESSAGE: Final[float] = 0.025
DEFAULT_PING_MS: Final[int] = 10_000
FATAL_CLOSE_CODES: Final[frozenset[int]] = frozenset({4100, 4101, 4102})


class CmcWsFatalError(RuntimeError):
    """The key or plan cannot use the WebSocket; reconnecting cannot help."""


CaptureWriter = Callable[[list[Capture]], Awaitable[None]]
CreditSink = Callable[[int, datetime], Awaitable[None]]


class CreditAccumulator:
    """Turns fractional per-message credits into whole credits for the meter."""

    def __init__(self) -> None:
        self._messages = 0
        self._charged = 0

    def add(self, messages: int = 1) -> int:
        """Record messages; returns the whole credits newly due."""
        self._messages += messages
        due = int(self._messages * CREDITS_PER_MESSAGE) - self._charged
        self._charged += due
        return due


class CmcWsClient:
    def __init__(
        self,
        *,
        url: str,
        api_key: Callable[[], str | None],
        crypto_ids: Callable[[], Sequence[int]],
        write: CaptureWriter,
        on_credits: CreditSink,
        max_coins: int,
        flush_s: float = 1.0,
        max_backoff_s: float = 60.0,
    ) -> None:
        self._url = url
        self._api_key = api_key
        self._crypto_ids = crypto_ids
        self._write = write
        self._on_credits = on_credits
        self._max_coins = max_coins
        self._flush_s = flush_s
        self._max_backoff_s = max_backoff_s
        self._buffer: list[dict[str, Any]] = []
        self._credits = CreditAccumulator()
        self._next_id = 0
        self.subscribed: tuple[int, ...] = ()

    def targets(self) -> tuple[int, ...]:
        return tuple(sorted(set(self._crypto_ids())))[: self._max_coins]

    async def run(self, stop: asyncio.Event) -> None:
        backoff = 1.0
        flusher = asyncio.create_task(self._flush_loop(stop))
        try:
            while not stop.is_set():
                if not self.targets():
                    await _sleep_or_stop(stop, 30.0)  # nothing held: no connection, no credits
                    continue
                try:
                    await self._session(stop)
                    backoff = 1.0
                except CmcWsFatalError:
                    log.exception("cmc websocket disabled for this run")
                    return
                except (OSError, websockets.WebSocketException) as exc:
                    log.warning("cmc websocket disconnected", extra={"error": type(exc).__name__})
                    await _sleep_or_stop(stop, backoff)
                    backoff = min(backoff * 2, self._max_backoff_s)
        finally:
            flusher.cancel()
            await self.flush()

    async def _session(self, stop: asyncio.Event) -> None:
        key = self._api_key()
        if not key:
            raise CmcWsFatalError("no active CMC key")
        try:
            async with websockets.connect(
                self._url, additional_headers={KEY_HEADER: key}, ping_interval=None, max_size=2**22
            ) as ws:
                await self._serve(ws, stop)
        except ConnectionClosed as exc:
            code = exc.rcvd.code if exc.rcvd is not None else None
            if code in FATAL_CLOSE_CODES:
                raise CmcWsFatalError(f"closed with code {code}") from None
            raise

    async def _serve(self, ws: Any, stop: asyncio.Event) -> None:
        ping_s = DEFAULT_PING_MS / 1000
        loop = asyncio.get_running_loop()
        next_ping = loop.time() + ping_s
        self.subscribed = ()
        while not stop.is_set():
            wanted = self.targets()
            if wanted != self.subscribed:
                await self._resubscribe(ws, wanted)
            if not wanted:
                return
            timeout = max(0.05, next_ping - loop.time())
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=min(timeout, 5.0))
            except TimeoutError:
                raw = None
            if raw is not None:
                interval = await self.handle(raw, utcnow())
                if interval is not None:
                    ping_s = interval / 1000
            if loop.time() >= next_ping:
                await ws.send(json.dumps({"id": self._id(), "method": "ping"}))
                next_ping = loop.time() + ping_s

    async def _resubscribe(self, ws: Any, wanted: tuple[int, ...]) -> None:
        if self.subscribed:
            await ws.send(json.dumps({"id": self._id(), "method": "unsubscribe", "channel": CHANNEL}))
        if wanted:
            message = {
                "id": self._id(),
                "method": "subscribe",
                "channel": CHANNEL,
                "params": {"crypto_ids": list(wanted)},
            }
            await ws.send(json.dumps(message))
        self.subscribed = wanted
        log.info("cmc websocket subscriptions", extra={"crypto_ids": list(wanted)})

    async def handle(self, raw: str | bytes, received_at: datetime) -> int | None:
        """Process one frame; returns a new ping interval (ms) when the server announces one."""
        try:
            frame = json.loads(raw)
        except ValueError:
            log.warning("unexpected cmc websocket frame")
            return None
        if not isinstance(frame, dict):
            return None
        kind = frame.get("type")
        if kind == "welcome":
            interval = frame.get("ping_interval_ms")
            return interval if isinstance(interval, int) and interval > 0 else None
        if kind == "error":
            log.warning("cmc websocket error frame", extra={"status": frame.get("status")})
            return None
        if kind == "data":
            self._buffer.append({"recv_ts": int(received_at.timestamp() * 1000), "frame": frame})
            due = self._credits.add()
            if due:
                await self._on_credits(due, received_at)
        return None

    async def flush(self) -> None:
        if not self._buffer:
            return
        items, self._buffer = self._buffer, []
        capture = Capture(
            source="cmc",
            route=ROUTE,
            fetched_at=utcnow(),
            http_status=101,
            body=canonical_json(items),
            params={"channel": CHANNEL},
        )
        await self._write([capture])

    async def _flush_loop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await _sleep_or_stop(stop, self._flush_s)
            try:
                await self.flush()
            except Exception:
                log.exception("cmc websocket flush failed")

    def _id(self) -> int:
        self._next_id += 1
        return self._next_id


async def _sleep_or_stop(stop: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except TimeoutError:
        return
