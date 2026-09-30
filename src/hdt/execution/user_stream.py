"""Binance user data stream of one namespace (phase 09 section 5).

- listenKey: `POST /fapi/v1/listenKey` (API key only), keepalive `PUT` every `listen_key_keepalive_s`
  (30-50 min); error -1125 or a `listenKeyExpired` event -> a new key and a new connection;
- WS URL `<ws_private>/ws/<listenKey>` (host `/private`). The demo host's path routing is not documented:
  when the `/private` form is refused, the legacy `<host>/ws/<listenKey>` form is tried and the working
  form is kept for the process lifetime;
- events: `ORDER_TRADE_UPDATE` (order state + trade), `ALGO_UPDATE` (conditional orders: trigger, fill,
  cancel, reject reason), `ACCOUNT_UPDATE` (wallet / positions changed);
- every (re)connect enters RESYNCING and runs `Resync`; the events buffered meanwhile (frames keep being
  read so the connection never stalls) are applied before the namespace reopens;
- only `stop` ends the stream: a failure of any kind (network, exchange, a malformed frame, the database)
  reconnects with backoff, and an unexpected one raises a critical `namespace_error` alert that the next
  successful RESYNC resolves.

The URL carries the listenKey and is never logged (the redaction filter masks `/ws/<key>` anyway).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Final

import websockets
from websockets.exceptions import ConnectionClosed, InvalidStatus, InvalidURI

from hdt.contracts.common import OrderSide
from hdt.execution.adapter_base import (
    AlgoEvent,
    AlgoSnapshot,
    ExchangeError,
    ExchangeEventSink,
    OrderEvent,
    OrderSnapshot,
    TradeFill,
)
from hdt.execution.binance_adapter import MARGIN_ASSET, dec, ms_to_dt, opt_dec
from hdt.execution.binance_client import BinanceClient
from hdt.execution.resync import Resync
from hdt.execution.runtime import Namespace

log = logging.getLogger(__name__)

LISTEN_KEY_PATH: Final[str] = "/fapi/v1/listenKey"
LISTEN_KEY_MISSING: Final[int] = -1125
MAX_BACKOFF_S: Final[float] = 60.0
USER_STREAM_EPISODE: Final[str] = "user_stream"


class StreamRestartError(Exception):
    """The listenKey expired or keepalive failed: reconnect with a new key."""


@dataclass(frozen=True)
class AccountUpdate:
    wallet_balance: Decimal | None


UserEvent = OrderEvent | AlgoEvent | AccountUpdate


def parse_order_update(msg: dict[str, Any]) -> OrderEvent:
    o = msg["o"]
    symbol = str(o["s"])
    order_id = int(o["i"]) if o.get("i") is not None else None
    side = OrderSide(str(o["S"]))
    snap = OrderSnapshot(
        symbol=symbol,
        client_id=str(o.get("c", "")),
        order_id=order_id,
        side=side,
        order_type=str(o.get("o", "")),
        tif=str(o["f"]) if o.get("f") else None,
        status=str(o["X"]),
        qty=dec(o.get("q")),
        executed_qty=dec(o.get("z")),
        avg_price=opt_dec(o.get("ap")),
        price=opt_dec(o.get("p")),
        reduce_only=bool(o.get("R", False)),
        updated_at=ms_to_dt(o.get("T") or msg.get("T") or msg.get("E")),
    )
    fill = None
    if o.get("x") == "TRADE" and dec(o.get("l")) > 0:
        fill = TradeFill(
            symbol=symbol,
            trade_id=int(o["t"]),
            order_id=order_id,
            client_id=snap.client_id,
            side=side,
            price=dec(o.get("L")),
            qty=dec(o.get("l")),
            fee=dec(o.get("n")),
            fee_asset=str(o.get("N") or MARGIN_ASSET),
            maker=bool(o["m"]) if o.get("m") is not None else None,
            realized_pnl=dec(o.get("rp")),
            time=ms_to_dt(o.get("T") or msg.get("T")),
        )
    return OrderEvent(snap, fill)


def parse_algo_update(msg: dict[str, Any]) -> AlgoEvent:
    o = msg["o"]
    actual = o.get("ai")
    return AlgoEvent(
        AlgoSnapshot(
            symbol=str(o["s"]),
            client_algo_id=str(o.get("caid", "")),
            algo_id=int(o["aid"]) if o.get("aid") not in (None, "") else None,
            side=OrderSide(str(o["S"])),
            order_type=str(o.get("o", "")),
            status=str(o.get("X", "")),
            trigger_price=dec(o.get("tp")),
            qty=opt_dec(o.get("q")),
            close_position=bool(o.get("cp", False)),
            reduce_only=bool(o.get("R", False)),
            triggered_order_id=int(actual) if actual not in (None, "", 0, "0") else None,
            updated_at=ms_to_dt(msg.get("T") or msg.get("E")),
        )
    )


def parse_account_update(msg: dict[str, Any]) -> AccountUpdate:
    for bal in (msg.get("a") or {}).get("B") or []:
        if isinstance(bal, dict) and bal.get("a") == MARGIN_ASSET:
            return AccountUpdate(dec(bal.get("wb")))
    return AccountUpdate(None)


def parse_user_event(msg: Any) -> UserEvent | None:
    """One decoded frame -> event (None for types execution does not act on)."""
    if not isinstance(msg, dict):
        return None
    kind = msg.get("e")
    if kind == "ORDER_TRADE_UPDATE":
        return parse_order_update(msg)
    if kind == "ALGO_UPDATE":
        return parse_algo_update(msg)
    if kind == "ACCOUNT_UPDATE":
        return parse_account_update(msg)
    if kind == "listenKeyExpired":
        raise StreamRestartError("listenKey expired")
    return None


def stream_urls(ws_private: str, listen_key: str) -> list[str]:
    """`/private/ws/<key>` first, then the legacy host-root form (demo host routing fallback)."""
    base = ws_private.rstrip("/")
    urls = [f"{base}/ws/{listen_key}"]
    if base.endswith("/private"):
        urls.append(f"{base.removesuffix('/private')}/ws/{listen_key}")
    return urls


async def dispatch(sink: ExchangeEventSink, event: UserEvent) -> None:
    if isinstance(event, OrderEvent):
        await sink.on_order_event(event)
    elif isinstance(event, AlgoEvent):
        await sink.on_algo_event(event)
    else:
        await sink.on_account_update(event.wallet_balance)


class UserStream:
    def __init__(
        self,
        ns: Namespace,
        client: BinanceClient,
        ws_private: str,
        sink: ExchangeEventSink,
        resync: Resync,
        *,
        keepalive_s: float,
        connect: Callable[[str], Any] = websockets.connect,
    ) -> None:
        self.ns = ns
        self.client = client
        self.ws_private = ws_private
        self.sink = sink
        self.resync = resync
        self.keepalive_s = keepalive_s
        self._connect = connect
        self._url_index: int | None = None
        self._alerted = False  # a `namespace_error` alert of this stream is open
        self._resynced = False  # the current connection completed its RESYNC
        self.connected = asyncio.Event()

    async def create_key(self) -> str:
        data = await self.client.keyed("POST", LISTEN_KEY_PATH)
        key = str(data.get("listenKey", "")) if isinstance(data, dict) else ""
        if not key:
            raise StreamRestartError("no listenKey in the response")
        return key

    async def run(self, stop: asyncio.Event) -> None:
        """Reconnect loop; only `stop` ends it. A network or exchange failure reconnects quietly; anything
        else (a malformed frame, a database error while applying an event) is logged and raises a
        critical alert once per episode, then the stream reconnects the same way: every reconnect is a
        RESYNC, so the REST snapshot catches up whatever the failed connection missed. The backoff
        resets only once a connection completed its RESYNC: a failure that persists after connecting
        (Postgres down) backs off like a connection failure instead of reconnecting every second."""
        backoff = 1.0
        while not stop.is_set():
            self._resynced = False
            try:
                self._begin_resync()
                key = await self.create_key()
                async with await self._open(key) as ws:
                    await self._serve(ws, stop)
            except (
                ConnectionClosed,
                OSError,
                StreamRestartError,
                ExchangeError,
                InvalidStatus,
                TimeoutError,
            ) as exc:
                log.warning("user stream down: %s", type(exc).__name__, extra={"account": self.ns.name})
            except Exception as exc:
                log.exception("user stream failed, reconnecting", extra={"account": self.ns.name})
                self._alerted = True
                self.ns.alert(
                    "namespace_error",
                    "critical",
                    f"{self.ns.name}: user stream failed ({type(exc).__name__}); re-syncing",
                    episode=USER_STREAM_EPISODE,
                )
            finally:
                self.connected.clear()
                self._begin_resync()
            if self._resynced:
                backoff = 1.0
            await self._pause(stop, backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_S)

    async def _pause(self, stop: asyncio.Event, seconds: float) -> None:
        """Wait `seconds` before the next connection attempt (returns at once when `stop` is set)."""
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=seconds)

    def _begin_resync(self) -> None:
        """Close the gate; a failed `exec_accounts` write (database down) never ends the stream: the
        in-memory gate is already closed by then."""
        try:
            self.resync.begin()
        except Exception:
            log.exception("sync state could not be written", extra={"account": self.ns.name})

    async def _open(self, key: str) -> Any:
        urls = stream_urls(self.ws_private, key)
        order = [self._url_index] if self._url_index is not None else list(range(len(urls)))
        last: Exception | None = None
        for index in order:
            try:
                ws = await self._connect(urls[index])
            except (InvalidStatus, InvalidURI, OSError) as exc:
                last = exc
                continue
            self._url_index = index
            return ws
        raise StreamRestartError(f"no user stream endpoint accepted the connection ({type(last).__name__})")

    async def _serve(self, ws: Any, stop: asyncio.Event) -> None:
        queue: asyncio.Queue[UserEvent] = asyncio.Queue()
        reader = asyncio.create_task(self._read(ws, queue))
        self.connected.set()
        try:
            await self.resync.run(drain=lambda: self._drain(queue))
            self._resynced = True
            if self._alerted:
                self._alerted = False
                self.ns.resolve("namespace_error", episode=USER_STREAM_EPISODE)
            tasks = {
                reader,
                asyncio.create_task(self._pump(queue)),
                asyncio.create_task(self._keepalive()),
                asyncio.create_task(stop.wait()),
            }
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in pending:
                task.cancel()
            for task in done:
                exc = task.exception()
                if exc is not None:
                    raise exc
        finally:
            if not reader.done():
                reader.cancel()
            with contextlib.suppress(Exception):
                await self.client.keyed("DELETE", LISTEN_KEY_PATH)

    async def _read(self, ws: Any, queue: asyncio.Queue[UserEvent]) -> None:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except ValueError:
                log.warning("unreadable user stream frame", extra={"account": self.ns.name})
                continue
            event = parse_user_event(msg)
            if event is not None:
                queue.put_nowait(event)

    async def _drain(self, queue: asyncio.Queue[UserEvent]) -> None:
        """Apply the events buffered during the RESYNC snapshot (before the namespace reopens)."""
        while not queue.empty():
            await dispatch(self.sink, queue.get_nowait())

    async def _pump(self, queue: asyncio.Queue[UserEvent]) -> None:
        while True:
            event = await queue.get()
            await dispatch(self.sink, event)

    async def _keepalive(self) -> None:
        while True:
            await asyncio.sleep(self.keepalive_s)
            try:
                await self.client.keyed("PUT", LISTEN_KEY_PATH)
            except ExchangeError as exc:
                if exc.code == LISTEN_KEY_MISSING:
                    raise StreamRestartError("listenKey no longer exists (-1125)") from exc
                raise
