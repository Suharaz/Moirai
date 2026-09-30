"""Paper feed replay of the recorded lake (review cycle 1 L5): a malformed record never drops the rest of
the window, a failed lake read changes nothing, and `read` never changes the venue (the adapter runs it in
a worker thread while the event loop reads the venue)."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from hdt.contracts.common import OrderSide, OrderType
from hdt.execution.adapter_base import AlgoRequest, OrderRequest
from hdt.execution.paper_feed import PaperFeed
from hdt.execution.paper_venue import Book, PaperVenue
from hdt.lake.pit_query import PitQuery
from hdt.lake.raw_store import RawStore
from hdt.lake.schemas import Capture, RawRecord

SYM = "SOLUSDT"
NOW = datetime(2026, 9, 28, 12, 0, tzinfo=UTC)
FEED_LOG = "hdt.execution.paper_feed"


def _ms(at: datetime) -> int:
    return int(at.timestamp() * 1000)


def _ws(route: str, at: datetime, stream: str, data: dict[str, Any]) -> Capture:
    body = [{"recv_ts": _ms(at), "stream": stream, "data": data}]
    return Capture("binance", route, at, 101, json.dumps(body).encode(), params={"connection": route})


def _trade(at: datetime, price: str) -> Capture:
    data = {"e": "aggTrade", "E": _ms(at), "s": SYM, "p": price, "q": "1", "T": _ms(at), "m": True}
    return _ws("ws_aggtrade", at, "solusdt@aggTrade", data)


def _mark(at: datetime, price: str) -> Capture:
    data = {
        "e": "markPriceUpdate",
        "E": _ms(at),
        "s": SYM,
        "p": price,
        "r": "0.0001",
        "T": _ms(at) + 3_600_000,
    }
    return _ws("ws_markprice", at, "solusdt@markPrice@1s", data)


def _lake(root: Path, *captures: Capture) -> None:
    RawStore(root / "staging", root / "lake").append_many(captures)


class FlakyPit(PitQuery):
    """The lake read of one route fails `failures` times (disk, network share, a compaction swap)."""

    def __init__(self, root: Path, route: str, failures: int) -> None:
        super().__init__(root / "staging", root / "lake")
        self.route, self.failures = route, failures

    def series(
        self, source: str, route: str, start: datetime, end: datetime, *, as_of: datetime, key: str = ""
    ) -> list[RawRecord]:
        if route == self.route and self.failures > 0:
            self.failures -= 1
            raise OSError("lake read failed")
        return super().series(source, route, start, end, as_of=as_of, key=key)


def _long_with_stop() -> PaperVenue:
    """10 SOLUSDT long from 100.00 with a STOP_MARKET SELL at 95.00; book and mark known (no seeding)."""
    v = PaperVenue(
        maker_fee=Decimal("0.0002"),
        taker_fee=Decimal("0.0005"),
        slippage_bp=Decimal(0),
        wallet=Decimal(10000),
    )
    book = Book()
    book.apply_snapshot([("94.80", "50")], [("100.00", "50")], u=1, ts_ms=0)
    v.books[SYM] = book
    t0 = _ms(NOW - timedelta(minutes=5))
    v.on_mark(SYM, Decimal("100"), t0)
    open_long = OrderRequest(
        symbol=SYM, side=OrderSide.BUY, order_type=OrderType.MARKET, qty=Decimal("10"), client_id="open-l"
    )
    v.place_order(open_long, t0 + 1)
    stop = AlgoRequest(
        symbol=SYM,
        side=OrderSide.SELL,
        order_type=OrderType.STOP_MARKET,
        trigger_price=Decimal("95.00"),
        client_algo_id="sl-1",
        qty=Decimal("10"),
    )
    v.place_algo(stop, t0 + 2)
    return v


def _skipped(caplog: pytest.LogCaptureFixture) -> list[tuple[int, str]]:
    return [(r.levelno, r.getMessage()) for r in caplog.records if r.name == FEED_LOG]


def test_malformed_record_is_logged_and_skipped_and_the_rest_of_the_window_replays(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    _lake(tmp_path, _trade(NOW - timedelta(seconds=30), "abc"), _mark(NOW - timedelta(seconds=20), "94.00"))
    v = _long_with_stop()
    feed = PaperFeed(PitQuery(tmp_path / "staging", tmp_path / "lake"), v)

    batch = feed.read(NOW)
    assert (v.algos["sl-1"].status, v.mark(SYM), feed.cursor) == ("NEW", Decimal("100"), None)  # read only

    feed.apply(batch)
    assert v.algos["sl-1"].status == "FINISHED"  # the stop-crossing mark behind the bad trade triggered
    assert SYM not in v.positions
    assert feed.cursor == NOW
    ((level, skipped),) = _skipped(caplog)
    assert level == logging.ERROR
    assert skipped.startswith("paper feed: skipped malformed ws_aggtrade event")

    assert feed.apply(feed.read(NOW + timedelta(seconds=1))) == []  # each record is replayed once
    assert len(_skipped(caplog)) == 1


def test_failed_lake_read_changes_nothing_and_the_next_poll_reads_the_same_window(tmp_path: Path) -> None:
    _lake(
        tmp_path, _trade(NOW - timedelta(seconds=61), "100.00"), _mark(NOW - timedelta(seconds=60), "94.00")
    )
    v = _long_with_stop()
    feed = PaperFeed(FlakyPit(tmp_path, "ws_markprice", failures=1), v)

    with pytest.raises(OSError, match="lake read failed"):
        feed.read(NOW)
    assert feed.cursor is None

    feed.apply(feed.read(NOW + timedelta(seconds=1)))
    assert v.algos["sl-1"].status == "FINISHED"  # 61 s old by then, beyond the overlap, yet not lost
    assert SYM not in v.positions
