"""Redis candidate -> recorder hot set -> B9 depth snapshot + 100 ms depth diffs in the lake.

Real Redis (the `candidates` stream), real lake, real Postgres (route/WS health); Binance REST is an
`httpx.MockTransport` and the Binance WebSocket a local server that sends a chain of depth diff events on the
hot-set connection.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest
from nacl.public import PrivateKey
from redis.asyncio import Redis
from sqlalchemy import Engine
from sqlalchemy.orm import sessionmaker
from websockets.asyncio.server import ServerConnection, serve

from hdt.contracts import Candidate, CandidateSource, TargetType
from hdt.contracts.streams import Stream
from hdt.core.clock import utcnow
from hdt.core.config import StaticConfig, static_config
from hdt.core.streams import publish
from hdt.ingest.scheduler import Recorder
from hdt.lake.pit_query import PitQuery
from hdt.lake.raw_store import RawStore
from hdt.lake.universe import Universe, UniverseMember, universe_capture
from hdt.vault.loader import SecretLoader

pytestmark = [pytest.mark.pg, pytest.mark.redis, pytest.mark.integration]

REST = "https://fapi.test"
SOL = UniverseMember(
    cmc_id=5426, cmc_symbol="SOL", binance_symbol="SOLUSDT", multiplier=1, cmc_rank=5, open_interest_usd=2e9
)
ETH = UniverseMember(
    cmc_id=1027, cmc_symbol="ETH", binance_symbol="ETHUSDT", multiplier=1, cmc_rank=2, open_interest_usd=9e9
)
HOT_STREAM = "solusdt@depth@100ms"
DIFFS = 5
FIRST_U = 1001


@dataclass
class FakeBinanceWs:
    """Binance `/public` and `/market`: records each connection's streams; diff streams get `DIFFS` events."""

    connections: list[list[str]] = field(default_factory=list)

    async def handler(self, ws: ServerConnection) -> None:
        assert ws.request is not None
        streams = parse_qs(urlsplit(ws.request.path).query)["streams"][0].split("/")
        self.connections.append(streams)
        for stream in (s for s in streams if s.endswith("@depth@100ms")):
            for n in range(DIFFS):
                u = FIRST_U + n
                bids = [["150.00", str(n + 1)]]
                event = {"e": "depthUpdate", "s": "SOLUSDT", "U": u, "u": u, "pu": u - 1, "b": bids, "a": []}
                await ws.send(json.dumps({"stream": stream, "data": event}))
                await asyncio.sleep(0.05)
        await ws.wait_closed()


@dataclass
class FakeBinanceRest:
    requests: list[httpx.Request] = field(default_factory=list)

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if request.url.path == "/fapi/v1/depth":
            return httpx.Response(
                200, json={"lastUpdateId": FIRST_U - 1, "bids": [["150.00", "3"]], "asks": [["150.10", "2"]]}
            )
        return httpx.Response(200, json=[])


def _static(port: int) -> StaticConfig:
    static = static_config()
    hosts = static.binance.live.model_copy(
        update={
            "rest": REST,
            "ws_public": f"ws://127.0.0.1:{port}/public",
            "ws_market": f"ws://127.0.0.1:{port}/market",
        }
    )
    return static.model_copy(update={"binance": static.binance.model_copy(update={"live": hosts})})


async def _until(check: Callable[[], bool], timeout_s: float = 15.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    while not check():
        assert loop.time() < deadline, "condition not reached in time"
        await asyncio.sleep(0.05)


def _diffs(pit: PitQuery) -> list[dict[str, Any]]:
    now = utcnow()
    records = pit.series("binance", "ws_depth", now - timedelta(minutes=5), now, as_of=now)
    return [item for record in records for item in json.loads(record.body())]


async def test_candidate_puts_its_symbol_in_the_hot_set_with_snapshot_and_every_diff_recorded(
    tmp_path: Path, pg_engine: Engine, redis_client: Redis
) -> None:
    staging, lake = tmp_path / "staging", tmp_path / "lake"
    staging.mkdir()
    lake.mkdir()
    store, pit = RawStore(staging, lake), PitQuery(staging, lake)
    now = utcnow()
    universe = Universe(
        date=now.date(),
        built_at=now - timedelta(minutes=1),
        members=(ETH, SOL),
        ltx_size=2,
        watchlist_size=2,
        sources={},
    )
    store.append(universe_capture(universe))
    candidate = Candidate(
        coin_id=SOL.cmc_id,
        as_of=now,
        source=CandidateSource.LTX,
        score=1.0,
        rule_version="r1",
        target_type=TargetType.RESID_12H,
        label_spec_version="l1",
    )
    await publish(redis_client, Stream.CANDIDATES, candidate)

    ws, rest = FakeBinanceWs(), FakeBinanceRest()
    sessions = sessionmaker(bind=pg_engine)
    quiet = logging.getLogger("tests.fake_binance_ws")
    quiet.propagate = False
    async with (
        serve(ws.handler, "127.0.0.1", 0, logger=quiet) as server,
        httpx.AsyncClient(transport=httpx.MockTransport(rest)) as http,
    ):
        rec = Recorder(
            static=_static(server.sockets[0].getsockname()[1]),
            sessions=sessions,
            redis=redis_client,
            secrets=SecretLoader("data", sessions, PrivateKey.generate()),
            http=http,
            store=store,
            pit=pit,
        )
        try:
            await rec.refresh_streams()
            await _until(lambda: [HOT_STREAM] in ws.connections)
            await _until(lambda: len(_diffs(pit)) >= DIFFS)
        finally:
            await rec.ws.stop_all()

    # hot-set subscription: its own connection with only the 100 ms diff stream of the candidate's symbol
    assert ws.connections.count([HOT_STREAM]) == 1
    # B9: one REST snapshot of the entering symbol, recorded in the lake
    depth = [r for r in rest.requests if r.url.path == "/fapi/v1/depth"]
    assert [dict(r.url.params) for r in depth] == [{"symbol": "SOLUSDT", "limit": "1000"}]
    snapshot = pit.latest("binance", "depth", utcnow(), key="SOLUSDT")
    assert snapshot is not None
    assert snapshot.http_status == 200
    assert json.loads(snapshot.body())["lastUpdateId"] == FIRST_U - 1
    # every diff event is kept, in order, none collapsed into a snapshot
    diffs = _diffs(pit)
    assert [d["stream"] for d in diffs] == [HOT_STREAM] * DIFFS
    assert [d["data"]["u"] for d in diffs] == list(range(FIRST_U, FIRST_U + DIFFS))
