"""User data stream down while REST stays up (phase 09 section 5, success criterion).

The real `UserStream` runs against an in-process WebSocket that carries the exchange's events as Binance
`ORDER_TRADE_UPDATE` / `ALGO_UPDATE` frames. The stream is blocked for 20 s (namespace clock) while the
resting entry fills 6 of 9; those events are lost. After reconnect: no new action before the re-sync
completes, the STOP is on the exchange within that one reconcile cycle, and the IOC fallback is exactly
the remaining shortfall per the exchange's `executedQty` (no overfill).

Also (review cycle 1): a malformed frame never ends the stream task (H3), and the events buffered during a
re-sync are applied before the namespace reopens (L6). Review cycle 2 (I7): a failure that persists after
connecting (Postgres down during the RESYNC) backs off exponentially; only a completed RESYNC resets it.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import Callable
from datetime import datetime
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy import Engine
from sqlalchemy.exc import OperationalError
from websockets.exceptions import ConnectionClosedError

from alert_logs import raised
from fake_exchange import (
    ETH,
    SOL,
    FakeExchange,
    Harness,
    ledger_sessions,
    leg_of_call,
    make_harness,
    seed_market,
    working,
)
from hdt.contracts.common import Account, Leg, Side
from hdt.execution import actions
from hdt.execution.adapter_base import AlgoEvent, OrderEvent
from hdt.execution.paper_venue import VenueEvent
from hdt.execution.reconciler import ReconcileReport
from hdt.execution.user_stream import UserStream
from intent_builders import KEYRING, open_plan

pytestmark = [pytest.mark.pg, pytest.mark.integration]


def _ms(at: datetime) -> int:
    return int(at.timestamp() * 1000)


def frame(event: VenueEvent) -> str:
    """The Binance user-stream frame of one exchange event."""
    if isinstance(event, OrderEvent):
        o, f = event.order, event.fill
        body: dict[str, Any] = {
            "s": o.symbol,
            "c": o.client_id,
            "S": o.side.value,
            "o": o.order_type,
            "f": o.tif,
            "x": "NEW",
            "X": o.status,
            "i": o.order_id,
            "q": str(o.qty),
            "z": str(o.executed_qty),
            "ap": str(o.avg_price or 0),
            "p": str(o.price or 0),
            "R": o.reduce_only,
            "T": _ms(o.updated_at),
        }
        if f is not None:
            body.update(
                x="TRADE",
                t=f.trade_id,
                L=str(f.price),
                l=str(f.qty),
                n=str(f.fee),
                N=f.fee_asset,
                m=f.maker,
                rp=str(f.realized_pnl),
                T=_ms(f.time),
            )
        return json.dumps({"e": "ORDER_TRADE_UPDATE", "E": body["T"], "T": body["T"], "o": body})
    a = event.algo
    return json.dumps(
        {
            "e": "ALGO_UPDATE",
            "E": _ms(a.updated_at),
            "T": _ms(a.updated_at),
            "o": {
                "s": a.symbol,
                "caid": a.client_algo_id,
                "aid": a.algo_id,
                "S": a.side.value,
                "o": a.order_type,
                "X": a.status,
                "tp": str(a.trigger_price),
                "q": None if a.qty is None else str(a.qty),
                "cp": a.close_position,
                "R": a.reduce_only,
                "ai": a.triggered_order_id or 0,
            },
        }
    )


class ListenKeys:
    """The REST side of the user stream (listenKey create / keepalive / delete)."""

    async def keyed(self, method: str, path: str, params: Any = None) -> dict[str, str]:
        return {"listenKey": "L" * 60} if method == "POST" else {}


class Socket:
    def __init__(self) -> None:
        self.frames: asyncio.Queue[str | None] = asyncio.Queue()

    async def __aenter__(self) -> Socket:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    def __aiter__(self) -> Socket:
        return self

    async def __anext__(self) -> str:
        item = await self.frames.get()
        if item is None:
            raise ConnectionClosedError(None, None)
        return item


class Network:
    """The user-stream WebSocket path only: `block()` drops it and refuses reconnects; REST is untouched."""

    def __init__(self, exchange: FakeExchange) -> None:
        self.exchange = exchange
        self.blocked = False
        self.socket: Socket | None = None
        exchange.stream_up = False  # no events before the first connection

    async def connect(self, url: str) -> Socket:
        if self.blocked:
            raise OSError("user stream blocked")
        self.socket = Socket()
        self.exchange.stream_up = True
        return self.socket

    def block(self) -> None:
        self.blocked = True
        self.exchange.stream_up = False
        if self.socket is not None:
            self.socket.frames.put_nowait(None)
            self.socket = None

    def unblock(self) -> None:
        self.blocked = False

    async def forward(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            while self.socket is not None and self.exchange.pushes:
                self.socket.frames.put_nowait(frame(self.exchange.pushes.pop(0)))
            await asyncio.sleep(0.005)


async def until(predicate: Callable[[], bool], timeout: float = 15.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        assert loop.time() < deadline, "condition not reached in time"
        await asyncio.sleep(0.01)


@pytest.fixture(autouse=True)
def _no_retry_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(actions, "CANCEL_CONFIRM_BACKOFF_S", 0.0)
    monkeypatch.setattr(actions, "UNKNOWN_VERIFY_DELAY_S", 0.0)


def _plan(h: Harness, event_id: str, symbol: str) -> list:  # type: ignore[type-arg]
    return open_plan(
        account=Account.PAPER,
        event_id=event_id,
        symbol=symbol,
        side=Side.LONG,
        qty=Decimal("9"),
        entry=Decimal("100.00"),
        stop=Decimal("98.00"),
        tp1=Decimal("104.00"),
        ioc_price=Decimal("100.10"),
        now=h.clock(),
    )


def _position_qty(h: Harness) -> Decimal | None:
    pos = h.ledger.position(SOL)
    return None if pos is None else pos.qty


async def test_user_stream_down_20s_while_the_entry_fills(pg_engine: Engine) -> None:
    h = make_harness(ledger_sessions(pg_engine), Account.PAPER, KEYRING)
    seed_market(h.exchange, ETH)
    network = Network(h.exchange)
    stream = UserStream(
        h.ns,
        ListenKeys(),  # type: ignore[arg-type]
        "wss://demo.test/private",
        h.manager,
        h.resync,
        keepalive_s=3600,
        connect=network.connect,
    )
    stop = asyncio.Event()
    tasks = [asyncio.create_task(stream.run(stop)), asyncio.create_task(network.forward(stop))]
    try:
        await until(lambda: h.ns.synced)
        plan = _plan(h, "evt-1", SOL)
        entry = plan[-1]
        assert await h.send(plan) == ["accepted"] * 5
        await until(lambda: (row := h.ledger.order(entry.client_id)) is not None and row.status == "NEW")

        # --- the user stream goes down for 20 s; REST keeps working
        network.block()
        await until(lambda: not h.ns.synced)
        quiet_from = len(h.exchange.calls)
        h.exchange.trade(SOL, "100.00", "11", buyer_maker=True)  # queue 5, then 6 of 9 fill: never pushed
        assert h.exchange.venue.orders[entry.client_id].executed == Decimal("6")
        await h.tick(20)
        refused = await h.send(_plan(h, "evt-2", ETH))
        assert refused[-2:] == ["rejected", "rejected"]  # no exposure while re-syncing
        assert h.exchange.calls[quiet_from:] == []
        assert h.ledger.position(SOL) is None

        # --- reconnect: re-sync first, then events
        resync_from = len(h.exchange.synced_at_call)
        network.unblock()
        await until(lambda: h.ns.synced)
        during_resync = [call for call, synced in h.exchange.synced_at_call[resync_from:] if not synced]
        assert all(not call.startswith("place_order") for call in during_resync)
        (stop_algo,) = working(h, Leg.SL)
        assert stop_algo.qty == Decimal("6")
        assert (
            f"place_algo:{stop_algo.client_algo_id}" in during_resync
        )  # within the re-sync's reconcile cycle
        assert _position_qty(h) == Decimal("6")

        # --- the entry watchdog: cancel, confirm, IOC for the real shortfall
        await h.tick(200)
        (ioc,) = [c for c in h.exchange.mutating_calls("place_order") if leg_of_call(c) is Leg.ENTRY_IOC]
        assert h.exchange.venue.orders[ioc.split(":", 1)[1]].qty == Decimal("3")
        await until(lambda: _position_qty(h) == Decimal("9"))
        assert h.exchange.venue.positions[SOL].qty == Decimal("9")  # no overfill
        await until(lambda: [a.qty for a in working(h, Leg.SL)] == [Decimal("9")])
        assert (await h.reconcile()).clean
    finally:
        stop.set()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.wait_for(task, timeout=10)


class RecordingSink:
    """The order manager's event sink, noting whether the namespace was synced at each dispatch."""

    def __init__(self, h: Harness) -> None:
        self.h = h
        self.synced_at_dispatch: list[bool] = []

    async def on_order_event(self, event: OrderEvent) -> None:
        self.synced_at_dispatch.append(self.h.ns.synced)
        await self.h.manager.on_order_event(event)

    async def on_algo_event(self, event: AlgoEvent) -> None:
        self.synced_at_dispatch.append(self.h.ns.synced)
        await self.h.manager.on_algo_event(event)

    async def on_account_update(self, wallet_balance: Decimal | None) -> None:
        self.synced_at_dispatch.append(self.h.ns.synced)
        await self.h.manager.on_account_update(wallet_balance)


ACCOUNT_UPDATE = json.dumps({"e": "ACCOUNT_UPDATE", "E": 1, "T": 1, "a": {"B": [{"a": "USDT", "wb": "1"}]}})


async def test_a_malformed_frame_alerts_and_reconnects_instead_of_ending_the_stream(
    pg_engine: Engine, caplog: pytest.LogCaptureFixture
) -> None:
    h = make_harness(ledger_sessions(pg_engine), Account.PAPER, KEYRING)
    network = Network(h.exchange)
    caplog.set_level(logging.INFO, logger="hdt.core.alerts")
    connects = 0

    async def counting_connect(url: str) -> Socket:
        nonlocal connects
        connects += 1
        return await network.connect(url)

    stream = UserStream(
        h.ns,
        ListenKeys(),  # type: ignore[arg-type]
        "wss://demo.test/private",
        h.manager,
        h.resync,
        keepalive_s=3600,
        connect=counting_connect,
    )
    stop = asyncio.Event()
    task = asyncio.create_task(stream.run(stop))
    try:
        await until(lambda: h.ns.synced)
        assert network.socket is not None
        no_side = {"e": "ORDER_TRADE_UPDATE", "E": 1, "T": 1, "o": {"s": SOL, "c": "x", "X": "NEW"}}
        network.socket.frames.put_nowait(json.dumps(no_side))  # no `S`: the parser raises KeyError
        await until(lambda: connects == 2 and h.ns.synced)
        assert not task.done()
        alerts = [r for r in caplog.records if raised(r, "namespace_error")]
        assert [(r.levelno, "KeyError" in r.getMessage()) for r in alerts] == [(logging.CRITICAL, True)]
        resolved = [r.getMessage() for r in caplog.records if r.getMessage().startswith("alert resolved")]
        assert resolved == ["alert resolved: namespace_error"]  # the clean re-sync closed the episode
    finally:
        stop.set()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=10)


async def test_events_buffered_during_the_resync_are_applied_before_the_namespace_reopens(
    pg_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    h = make_harness(ledger_sessions(pg_engine), Account.PAPER, KEYRING)
    network = Network(h.exchange)
    sink = RecordingSink(h)
    reconciler = h.resync.reconciler
    run_once = reconciler.run_once

    async def run_once_then_an_event_arrives() -> ReconcileReport:
        report = await run_once()
        assert network.socket is not None
        network.socket.frames.put_nowait(ACCOUNT_UPDATE)  # pushed right after the snapshot
        await asyncio.sleep(0.05)  # the reader buffers it
        return report

    monkeypatch.setattr(reconciler, "run_once", run_once_then_an_event_arrives)
    stream = UserStream(
        h.ns,
        ListenKeys(),  # type: ignore[arg-type]
        "wss://demo.test/private",
        sink,
        h.resync,
        keepalive_s=3600,
        connect=network.connect,
    )
    stop = asyncio.Event()
    task = asyncio.create_task(stream.run(stop))
    try:
        await until(lambda: h.ns.synced)
        assert sink.synced_at_dispatch == [False]  # applied before intents and timers may act
    finally:
        stop.set()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=10)


async def test_a_resync_that_keeps_failing_after_connecting_backs_off_exponentially(
    pg_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    h = make_harness(ledger_sessions(pg_engine), Account.PAPER, KEYRING)
    network = Network(h.exchange)
    failures = 4
    real_run = h.resync.run

    async def postgres_down_then_up(drain: Callable[[], Any] | None = None) -> ReconcileReport:
        nonlocal failures
        if failures:
            failures -= 1
            raise OperationalError("SELECT 1", {}, OSError("connection refused"))
        return await real_run(drain=drain)

    monkeypatch.setattr(h.resync, "run", postgres_down_then_up)
    stream = UserStream(
        h.ns,
        ListenKeys(),  # type: ignore[arg-type]
        "wss://demo.test/private",
        h.manager,
        h.resync,
        keepalive_s=3600,
        connect=network.connect,
    )
    pauses: list[float] = []

    async def recorded_pause(stop: asyncio.Event, seconds: float) -> None:
        pauses.append(seconds)
        await asyncio.sleep(0)

    monkeypatch.setattr(stream, "_pause", recorded_pause)
    stop = asyncio.Event()
    task = asyncio.create_task(stream.run(stop))
    try:
        await until(lambda: h.ns.synced)  # the fifth connection's RESYNC completed
        assert pauses == [1.0, 2.0, 4.0, 8.0]  # every connection opened, then failed in the RESYNC
        network.block()  # the healthy connection drops: after a completed RESYNC the backoff restarts
        await until(lambda: len(pauses) >= 6)
        assert pauses[4:6] == [1.0, 2.0]
    finally:
        stop.set()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=10)
