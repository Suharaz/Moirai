"""Microstructure features (Microstructure agent) from Binance WS records.

- CVD per window from `ws_aggtrade` (`m=true`: the buyer is the maker, so the taker sold): taker buy quote
  minus taker sell quote over trades with trade time in (as_of - w, as_of]; taker buy ratio = buy / total.
- Order book from the newest `ws_depth20` snapshot of the symbol within `micro.book_max_age_s` (REST
  `depth` as a fallback): imbalance over 5 / 10 levels (notional), spread in bp, and notional within
  +-`depth_band_pct` % of mid on each side (a lower bound when 20 levels do not reach the band).
Per-record aggregation is memoized by record hash, so the open hour is re-aggregated only for new records.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta

from hdt.contracts.common import DataQualityFlag
from hdt.core.config import IndicatorsFile
from hdt.features.bars import INTERVAL_MS
from hdt.features.common import FeatureBlock, ratio
from hdt.features.lake_io import BINANCE, LakeView, as_float
from hdt.lake.schemas import RawRecord

_MINUTE_MS = 60_000

# symbol -> minute bucket (epoch ms) -> [taker buy quote, taker sell quote, last trade time]
TradeBuckets = dict[str, dict[int, list[float]]]


def _trade_buckets(view: LakeView, record: RawRecord) -> TradeBuckets:
    out: TradeBuckets = {}
    items = view.body_json(record)
    for item in items if isinstance(items, list) else []:
        data = item.get("data") if isinstance(item, Mapping) else None
        if not isinstance(data, Mapping):
            continue
        symbol, ts = data.get("s"), data.get("T")
        price, qty, maker = as_float(data.get("p")), as_float(data.get("q")), data.get("m")
        if (
            not isinstance(symbol, str)
            or not isinstance(ts, int)
            or price is None
            or qty is None
            or not isinstance(maker, bool)
        ):
            continue
        slot = out.setdefault(symbol, {}).setdefault(ts - ts % _MINUTE_MS, [0.0, 0.0])
        slot[1 if maker else 0] += price * qty
    return out


@dataclass(frozen=True)
class TradeFlow:
    """Minute buckets per symbol over the longest window; empty when no aggTrade record was found."""

    buckets: Mapping[str, Mapping[int, Sequence[float]]]
    records: int


def trade_flow(view: LakeView, as_of: datetime, window: timedelta) -> TradeFlow:
    """Aggregate aggTrade records fetched within (as_of - window - 1 min, as_of] into minute buckets.

    Only whole minutes that ended at or before `as_of` are kept, so a result never depends on trades after
    `as_of` even when a record fetched at `as_of` carries them.
    """
    lo = as_of - window - timedelta(minutes=1)
    end_ms = int(as_of.timestamp() * 1000)
    last_minute = end_ms - end_ms % _MINUTE_MS
    first_minute = last_minute - int(window.total_seconds() * 1000)
    merged: dict[str, dict[int, list[float]]] = {}
    count = 0
    for record in view.records(BINANCE, "ws_aggtrade", lo, as_of, as_of=as_of):
        count += 1
        for symbol, minutes in view.per_record(
            "aggtrade_minutes", record, lambda r: _trade_buckets(view, r)
        ).items():
            slot = merged.setdefault(symbol, {})
            for minute, (buy, sell) in minutes.items():
                if not first_minute <= minute < last_minute:
                    continue
                acc = slot.setdefault(minute, [0.0, 0.0])
                acc[0] += buy
                acc[1] += sell
    return TradeFlow(merged, count)


def flow_features(
    flow: TradeFlow, symbol: str, as_of: datetime, windows: Iterable[str], open_interest_usd: float | None
) -> FeatureBlock:
    """Per window: CVD in USD, CVD / open interest (USD, scale-free across coins) and taker buy ratio."""
    block = FeatureBlock()
    end_ms = int(as_of.timestamp() * 1000)
    last_minute = end_ms - end_ms % _MINUTE_MS
    minutes = flow.buckets.get(symbol)
    for window in windows:
        names = (f"cvd_{window}_usd", f"taker_buy_ratio_{window}", f"cvd_{window}_oi")
        if flow.records == 0 or minutes is None:
            block.update(dict.fromkeys(names))
            block.flag(DataQualityFlag.MISSING_ROUTE, *names)
            continue
        start = last_minute - INTERVAL_MS[window]
        buy = sell = 0.0
        for minute, (b, s) in minutes.items():
            if start <= minute < last_minute:
                buy += b
                sell += s
        block.set(names[0], buy - sell)
        block.set(names[1], ratio(buy, buy + sell))
        block.set(names[2], ratio(buy - sell, open_interest_usd))
        if open_interest_usd is None:
            block.flag(DataQualityFlag.MISSING_ROUTE, names[2])
    return block


# --------------------------------------------------------------------------- order book


@dataclass(frozen=True)
class Book:
    bids: tuple[tuple[float, float], ...]
    asks: tuple[tuple[float, float], ...]
    ts: datetime

    @property
    def mid(self) -> float | None:
        if not self.bids or not self.asks:
            return None
        return (self.bids[0][0] + self.asks[0][0]) / 2.0


def _levels(raw: object) -> tuple[tuple[float, float], ...]:
    out = []
    for level in raw if isinstance(raw, list) else []:
        if isinstance(level, list) and len(level) >= 2:
            price, qty = as_float(level[0]), as_float(level[1])
            if price is not None and qty is not None and price > 0 and qty > 0:
                out.append((price, qty))
    return tuple(out)


def latest_book(view: LakeView, symbol: str, as_of: datetime, max_age: timedelta) -> Book | None:
    stream = f"{symbol.lower()}@depth20"
    for record in reversed(view.records(BINANCE, "ws_depth20", as_of - max_age, as_of, as_of=as_of)):
        items = view.body_json(record)
        for item in reversed(items) if isinstance(items, list) else []:
            if not isinstance(item, Mapping) or not str(item.get("stream", "")).startswith(stream):
                continue
            data = item.get("data")
            if isinstance(data, Mapping):
                bids, asks = _levels(data.get("b")), _levels(data.get("a"))
                if bids and asks:
                    return Book(tuple(sorted(bids, reverse=True)), tuple(sorted(asks)), record.fetched_at)
    snapshot = view.latest(BINANCE, "depth", as_of, key=symbol, lookback=max_age)
    data = view.binance_data(snapshot)
    if snapshot is not None and isinstance(data, Mapping):
        bids, asks = _levels(data.get("bids")), _levels(data.get("asks"))
        if bids and asks:
            return Book(tuple(sorted(bids, reverse=True)), tuple(sorted(asks)), snapshot.fetched_at)
    return None


def book_features(book: Book | None, params: IndicatorsFile) -> FeatureBlock:
    block = FeatureBlock()
    names = [f"book_imbalance_{n}" for n in params.book_imbalance_levels]
    names += ["spread_bp", "depth_bid_usd", "depth_ask_usd"]
    mid = book.mid if book is not None else None
    if book is None or mid is None:
        block.update(dict.fromkeys(names))
        block.flag(DataQualityFlag.MISSING_ROUTE, *names)
        return block
    for n in params.book_imbalance_levels:
        bid = sum(p * q for p, q in book.bids[:n])
        ask = sum(p * q for p, q in book.asks[:n])
        block.set(f"book_imbalance_{n}", ratio(bid - ask, bid + ask))
    block.set("spread_bp", (book.asks[0][0] - book.bids[0][0]) / mid * 1e4)
    band = params.depth_band_pct / 100.0
    block.set("depth_bid_usd", sum(p * q for p, q in book.bids if p >= mid * (1 - band)))
    block.set("depth_ask_usd", sum(p * q for p, q in book.asks if p <= mid * (1 + band)))
    return block
