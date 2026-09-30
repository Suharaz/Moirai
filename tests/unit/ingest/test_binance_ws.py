"""Binance WS recorder: gaps are detected per stream, depth keeps one snapshot per symbol per flush."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

from hdt.ingest.binance_ws import BinanceWsRecorder, GapDetector, stream_family
from hdt.lake.schemas import Capture

T0 = datetime(2026, 9, 27, 10, 0, tzinfo=UTC)


def test_stream_families() -> None:
    assert stream_family("btcusdt@depth20@100ms") == "ws_depth20"
    assert stream_family("btcusdt@kline_15m") == "ws_kline_15m"
    assert stream_family("btcusdt@markPrice@1s") == "ws_markprice"
    assert stream_family("!forceOrder@arr") == "ws_forceorder"


def test_depth_gap_is_a_broken_update_id_link() -> None:
    gaps = GapDetector()
    assert not gaps.observe("btcusdt@depth20@100ms", {"u": 10, "pu": 9})
    assert not gaps.observe("btcusdt@depth20@100ms", {"u": 15, "pu": 10})
    assert gaps.observe("btcusdt@depth20@100ms", {"u": 30, "pu": 20})
    assert not gaps.observe("ethusdt@depth20@100ms", {"u": 3, "pu": 1})  # streams are independent


def test_aggtrade_and_markprice_gaps() -> None:
    gaps = GapDetector()
    assert not gaps.observe("btcusdt@aggTrade", {"a": 1})
    assert not gaps.observe("btcusdt@aggTrade", {"a": 2})
    assert gaps.observe("btcusdt@aggTrade", {"a": 5})
    assert not gaps.observe("btcusdt@markPrice@1s", {"E": 1_000})
    assert not gaps.observe("btcusdt@markPrice@1s", {"E": 2_000})
    assert gaps.observe("btcusdt@markPrice@1s", {"E": 6_000})


async def test_flush_writes_one_record_per_family_with_the_last_depth_snapshot() -> None:
    written: list[Capture] = []

    async def write(captures: list[Capture]) -> None:
        written.extend(captures)

    async def health(*_args: object) -> None:
        return None

    rec = BinanceWsRecorder(
        name="public", base_url="wss://example/public", streams=["x"], write=write, health=health
    )
    for u in (1, 2, 3):
        frame = {"stream": "btcusdt@depth20@100ms", "data": {"u": u, "pu": u - 1}}
        rec.handle(json.dumps(frame), T0)
    rec.handle(json.dumps({"stream": "btcusdt@aggTrade", "data": {"a": 7}}), T0)
    rec.handle(json.dumps({"stream": "btcusdt@aggTrade", "data": {"a": 9}}), T0 + timedelta(seconds=1))
    await rec.flush()
    by_route = {c.route: json.loads(c.body) for c in written}
    assert [i["data"]["u"] for i in by_route["ws_depth20"]] == [3]
    assert [i["data"]["a"] for i in by_route["ws_aggtrade"]] == [7, 9]
    assert rec.gaps_24h(T0 + timedelta(seconds=2)) == 1
    assert rec.gaps_24h(T0 + timedelta(days=2)) == 0
