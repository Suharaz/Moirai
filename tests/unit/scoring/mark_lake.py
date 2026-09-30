"""A synthetic Binance mark lake around one 12 h label window (1 s mark events and 1m mark klines)."""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

from hdt.lake.raw_store import RawStore
from hdt.lake.schemas import Capture

AS_OF = datetime(2026, 3, 2, 6, tzinfo=UTC)
HORIZON = timedelta(hours=12)
END = AS_OF + HORIZON
COIN, BTC = "ETHUSDT", "BTCUSDT"
RISE = {COIN: 0.02, BTC: 0.04}
"""Linear rise over the horizon: RAW is up, RESID with beta 1 is down (0.02 - 0.04 < 0)."""
FAR = datetime(2100, 1, 1, tzinfo=UTC)


def ms(t: datetime) -> int:
    return int(t.timestamp() * 1000)


def price(symbol: str, t: datetime) -> float:
    frac = min(max((t - AS_OF) / HORIZON, 0.0), 1.0)
    return 100.0 * (1.0 + RISE[symbol] * frac)


def ticks(start: datetime, end: datetime, step: timedelta) -> Iterator[datetime]:
    t = start
    while t <= end:
        yield t
        t += step


def ws_capture(at: datetime, events: list[dict[str, object]]) -> Capture:
    body = json.dumps([{"recv_ts": ms(at), "stream": "!markPrice@arr@1s", "data": events}]).encode()
    return Capture("binance", "ws_markprice", at, 101, body, params={"connection": "ws_markprice"})


def mark_events(at: datetime, mark: float | None = None) -> Capture:
    return ws_capture(
        at,
        [{"e": "markPriceUpdate", "E": ms(at), "s": s, "p": f"{mark or price(s, at):.8f}"} for s in RISE],
    )


def kline_hour(symbol: str, hour: datetime, fetched: datetime, spike: float | None = None) -> Capture:
    rows = []
    for opened in ticks(hour, hour + timedelta(minutes=59), timedelta(minutes=1)):
        o, c = price(symbol, opened), price(symbol, opened + timedelta(minutes=1))
        hi = spike if spike is not None else max(o, c)
        rows.append(
            [
                ms(opened),
                f"{o:.8f}",
                f"{hi:.8f}",
                f"{min(o, c):.8f}",
                f"{c:.8f}",
                "0",
                ms(opened + timedelta(minutes=1)) - 1,
                "0",
                0,
                "0",
                "0",
                "0",
            ]
        )
    return Capture(
        "binance", "mark_price_klines", fetched, 200, json.dumps(rows).encode(), key=f"{symbol}:1m"
    )


def build_lake(root: Path, *, ws: bool = True) -> RawStore:
    store = RawStore(root / "staging", root / "lake")
    captures: list[Capture] = []
    if ws:
        captures += [mark_events(t) for t in ticks(AS_OF - timedelta(hours=1), END, timedelta(minutes=5))]
    for symbol in RISE:
        for hour in ticks(AS_OF - timedelta(hours=1), END - timedelta(hours=1), timedelta(hours=1)):
            captures.append(kline_hour(symbol, hour, hour + timedelta(hours=1)))
    store.append_many(captures)
    return store
