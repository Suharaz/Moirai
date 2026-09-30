"""No Binance key, secret, `signature`, listenKey or Ed25519 signature in the logs (phase 09 criterion).

Every service installs `hdt.core.logging.configure_logging`; these tests capture that exact JSON output at
DEBUG while the execution paths that handle credentials run for real: the signed REST client (including a
connection reset whose traceback holds the signed URL), the user stream against a local WebSocket server
(listenKey in the path, a refused endpoint, a failed keepalive, an expired key) and intake refusing
tampered, unsigned, malformed and wrong-namespace intents. Nothing is registered with `register_secret`,
so what is proven is that the code and the filter keep them out, not the exact-match list.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from collections.abc import Callable, Iterator
from decimal import Decimal
from http import HTTPStatus
from io import StringIO

import httpx
import pytest
from sqlalchemy import Engine
from websockets.asyncio.server import ServerConnection, serve
from websockets.http11 import Request, Response

from fake_exchange import SOL, ledger_sessions, make_harness
from hdt.contracts.common import Account, Side
from hdt.contracts.order import OrderIntent
from hdt.core.logging import configure_logging
from hdt.core.service import every
from hdt.execution import actions
from hdt.execution.binance_client import BinanceClient
from hdt.execution.user_stream import UserStream
from hdt.risk.gate import intent_id_for
from intent_builders import KEYRING, RISK_LINE, open_plan

API_KEY = "hdtTestApiKey" + "A1b2C3d4E5" * 5
API_SECRET = "hdtTestApiSecret" + "Z9y8X7w6V5" * 5
LISTEN_KEY = "hdtTestListenKey" + "Q1w2E3r4T5" * 5
BASE_URL = "https://demo-fapi.binance.com"


@pytest.fixture
def service_log() -> Iterator[StringIO]:
    """The JSON log stream of a service configured exactly like production, at DEBUG."""
    buf = StringIO()
    root = logging.getLogger()
    level = root.level
    noisy = {name: logging.getLogger(name).level for name in ("httpx", "httpcore", "websockets", "urllib3")}
    handler = configure_logging("execution", "DEBUG", stream=buf)
    try:
        yield buf
    finally:
        root.removeHandler(handler)
        root.setLevel(level)
        for name, previous in noisy.items():
            logging.getLogger(name).setLevel(previous)


@pytest.fixture(autouse=True)
def _no_retry_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(actions, "CANCEL_CONFIRM_BACKOFF_S", 0.0)
    monkeypatch.setattr(actions, "UNKNOWN_VERIFY_DELAY_S", 0.0)


def binance(seen: list[httpx.Request]) -> httpx.MockTransport:
    """Binance REST as the client sees it; an order request lands and then the connection resets."""

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        path = request.url.path
        if path == "/fapi/v1/time":
            return httpx.Response(200, json={"serverTime": int(time.time() * 1000)})
        if path == "/fapi/v1/listenKey":
            if request.method == "PUT":
                return httpx.Response(400, json={"code": -1125, "msg": "This listenKey does not exist."})
            return httpx.Response(200, json={"listenKey": LISTEN_KEY} if request.method == "POST" else {})
        if path == "/fapi/v3/account":
            return httpx.Response(200, json={"totalWalletBalance": "10000.00"})
        if path == "/fapi/v1/order":
            raise httpx.ConnectError(f"connection reset while sending {request.url}", request=request)
        return httpx.Response(404, json={"code": -1000, "msg": "unknown path"})

    return httpx.MockTransport(handler)


def client(seen: list[httpx.Request]) -> BinanceClient:
    return BinanceClient(base_url=BASE_URL, api_key=API_KEY, api_secret=API_SECRET, transport=binance(seen))


def assert_clean(text: str, *values: str) -> None:
    for value in (API_KEY, API_SECRET, LISTEN_KEY, *values):
        assert value not in text


async def until(predicate: Callable[[], bool], timeout: float = 15.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        assert loop.time() < deadline, "condition not reached in time"
        await asyncio.sleep(0.02)


async def test_signed_rest_failure_traceback_is_logged_without_the_signed_url(service_log: StringIO) -> None:
    seen: list[httpx.Request] = []
    rest = client(seen)
    stop = asyncio.Event()

    async def place_order() -> None:
        try:
            await rest.signed("GET", "/fapi/v3/account")
            await rest.keyed("POST", "/fapi/v1/listenKey")
            await rest.signed(
                "POST",
                "/fapi/v1/order",
                {"symbol": SOL, "side": "BUY", "type": "LIMIT", "quantity": "9", "price": "100.00"},
            )
        finally:
            stop.set()

    await every(stop, 0.0, place_order, "order placement")  # the service loop logs the failure with traceback
    await rest.aclose()

    order = next(r for r in seen if r.url.path == "/fapi/v1/order")
    signature = order.url.params["signature"]
    assert len(signature) == 64
    assert {r.headers.get("X-MBX-APIKEY") for r in seen if r.url.path != "/fapi/v1/time"} == {API_KEY}
    text = service_log.getvalue()
    assert "order placement failed" in text
    assert "ConnectError" in text  # the chained cause, whose message held the signed URL
    assert_clean(text, signature)
    assert "signature=[REDACTED]" in text


async def test_user_stream_logs_carry_no_listen_key(pg_engine: Engine, service_log: StringIO) -> None:
    h = make_harness(ledger_sessions(pg_engine), Account.PAPER, KEYRING)
    seen: list[httpx.Request] = []
    paths: list[str] = []
    quiet = logging.getLogger("tests.fake_binance_ws")  # the fake server's own log is not a service log
    quiet.propagate = False

    def process_request(connection: ServerConnection, request: Request) -> Response | None:
        paths.append(request.path)
        if request.path.startswith("/private/"):
            return connection.respond(HTTPStatus.NOT_FOUND, "no such route\n")
        return None

    async def handler(ws: ServerConnection) -> None:
        await ws.send("not json")
        if len(paths) > 2:  # the second connection: the key expires
            await ws.send(json.dumps({"e": "listenKeyExpired", "E": 1, "listenKey": LISTEN_KEY}))
        await ws.wait_closed()

    async with serve(handler, "127.0.0.1", 0, process_request=process_request, logger=quiet) as server:
        port = server.sockets[0].getsockname()[1]
        rest = client(seen)
        stream = UserStream(
            h.ns, rest, f"ws://127.0.0.1:{port}/private", h.manager, h.resync, keepalive_s=0.3
        )
        stop = asyncio.Event()
        task = asyncio.create_task(stream.run(stop))
        try:
            await until(lambda: service_log.getvalue().count("user stream down") >= 2)
        finally:
            stop.set()
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.wait_for(task, timeout=10)
        await rest.aclose()

    assert paths[:3] == [f"/private/ws/{LISTEN_KEY}", f"/ws/{LISTEN_KEY}", f"/ws/{LISTEN_KEY}"]
    assert any(r.method == "PUT" and r.url.path == "/fapi/v1/listenKey" for r in seen)  # the failed keepalive
    text = service_log.getvalue()
    assert "unreadable user stream frame" in text
    assert "StreamRestartError" in text
    assert_clean(text)


async def test_intent_refusals_are_logged_without_signatures_or_keys(
    pg_engine: Engine, service_log: StringIO
) -> None:
    h = make_harness(ledger_sessions(pg_engine), Account.PAPER, KEYRING)
    await h.start()
    plan = open_plan(
        account=Account.PAPER,
        event_id="evt-log",
        symbol=SOL,
        side=Side.LONG,
        qty=Decimal("9"),
        entry=Decimal("100.00"),
        stop=Decimal("98.00"),
        tp1=Decimal("104.00"),
        ioc_price=Decimal("100.10"),
        now=h.clock(),
    )
    sl, entry = plan[0], plan[-1]
    tampered = entry.model_copy(update={"qty": Decimal("90")})
    relabelled = OrderIntent.model_validate(
        {
            **sl.model_dump(),
            "account": Account.LIVE,
            "intent_id": intent_id_for(Account.LIVE, sl.event_id, sl.leg, sl.seq),
        }
    )
    malformed = {**plan[1].model_dump(mode="json"), "qty": "-1"}
    assert await h.send([tampered]) == ["rejected"]
    assert await h.intake.process(relabelled.model_dump(mode="json")) == "rejected"
    assert await h.intake.process(malformed) == "rejected"

    text = service_log.getvalue()
    assert text.count("intent_signature") >= 3
    signatures = [i.signature for i in (entry, relabelled) if i.signature] + [str(malformed["signature"])]
    assert len(signatures) == 3
    assert_clean(text, *signatures, RISK_LINE.partition(":")[2])
