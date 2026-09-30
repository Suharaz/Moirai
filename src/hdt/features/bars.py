"""OHLCV bars from the lake, point in time.

Only closed bars are returned: a Binance REST kline counts once its close time is before the fetch time
of the record that carried it, a CMC OHLCV bar once its `time_close` is at or before the fetch time. The
same bar seen in several records keeps the value from the newest record.

- Binance klines (`klines`, key `<SYMBOL>:<interval>`, rows `[t, o, h, l, c, v, T, q, n, V, Q, _]`) serve
  timeframes <= 15m, the Risk ATR and the level candidates (Binance contract price units);
- CMC OHLCV hourly (`ohlcv_historical`, key "", one multi-id call per hour) serves 1h/4h/1d technicals and
  beta (aggregate USD price per coin). Both hourly passes are memoized per closed lake hour. The one-off
  backfill of the same route (lake route `ohlcv_backfill`, key `<cmc_id>`, the last 30 days of one coin) only
  fills hours the hourly records do not have, so indicators have their history from the first day instead
  of after weeks of recording.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Final

import numpy as np

from hdt.core.clock import ensure_utc
from hdt.features.lake_io import BINANCE, CMC, LakeView, as_float, cmc_ok, parse_ts, usd_quote
from hdt.lake.schemas import CMC_OHLCV_BACKFILL_ROUTE, RawRecord

INTERVAL_MS: Final[dict[str, int]] = {
    "1m": 60_000,
    "5m": 300_000,
    "15m": 900_000,
    "1h": 3_600_000,
    "4h": 14_400_000,
    "1d": 86_400_000,
}
_HOUR = timedelta(hours=1)
_BACKFILL_LOOKBACK: Final[timedelta] = timedelta(days=3650)


@dataclass(frozen=True)
class Bars:
    """Closed bars, oldest first. Times are epoch milliseconds (UTC)."""

    open_time: np.ndarray
    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    volume: np.ndarray
    quote_volume: np.ndarray
    taker_buy_quote: np.ndarray
    interval_ms: int

    @classmethod
    def empty(cls, interval_ms: int) -> Bars:
        z = np.zeros(0, dtype=float)
        return cls(np.zeros(0, dtype=np.int64), z, z, z, z, z, z, z, interval_ms)

    @classmethod
    def from_rows(
        cls, rows: Sequence[tuple[int, float, float, float, float, float, float, float]], interval_ms: int
    ) -> Bars:
        if not rows:
            return cls.empty(interval_ms)
        arr = np.array(rows, dtype=float)
        return cls(
            arr[:, 0].astype(np.int64),
            arr[:, 1],
            arr[:, 2],
            arr[:, 3],
            arr[:, 4],
            arr[:, 5],
            arr[:, 6],
            arr[:, 7],
            interval_ms,
        )

    def __len__(self) -> int:
        return int(self.open_time.size)

    @property
    def close_time(self) -> np.ndarray:
        return self.open_time + self.interval_ms

    def last_close_time(self) -> datetime | None:
        if not len(self):
            return None
        return datetime.fromtimestamp(float(self.close_time[-1]) / 1000.0, tz=UTC)

    def tail(self, n: int) -> Bars:
        return self.slice(max(0, len(self) - n), len(self))

    def slice(self, start: int, stop: int) -> Bars:
        return Bars(
            self.open_time[start:stop],
            self.open[start:stop],
            self.high[start:stop],
            self.low[start:stop],
            self.close[start:stop],
            self.volume[start:stop],
            self.quote_volume[start:stop],
            self.taker_buy_quote[start:stop],
            self.interval_ms,
        )

    def since(self, start_ms: int) -> Bars:
        idx = int(np.searchsorted(self.open_time, start_ms, side="left"))
        return self.slice(idx, len(self))

    def scaled(self, factor: float) -> Bars:
        """Prices multiplied by `factor` (CMC per-coin price -> Binance contract units)."""
        return Bars(
            self.open_time,
            self.open * factor,
            self.high * factor,
            self.low * factor,
            self.close * factor,
            self.volume / factor if factor else self.volume,
            self.quote_volume,
            self.taker_buy_quote,
            self.interval_ms,
        )


def resample(bars: Bars, interval: str) -> Bars:
    """Aggregate into UTC-aligned `interval` bars; a bucket is kept only when every sub-bar is present."""
    target = INTERVAL_MS[interval]
    if target % bars.interval_ms or not len(bars):
        return Bars.empty(target)
    per = target // bars.interval_ms
    buckets = bars.open_time // target
    # Runs of consecutive bars in the same bucket; only full runs (every sub-bar present) become a bar.
    starts = np.flatnonzero(np.r_[True, buckets[1:] != buckets[:-1]])
    stops = np.r_[starts[1:], len(bars)]
    starts = starts[stops - starts == per]
    if not starts.size:
        return Bars.empty(target)
    idx = starts[:, None] + np.arange(per)
    return Bars(
        buckets[starts] * target,
        bars.open[starts],
        bars.high[idx].max(axis=1),
        bars.low[idx].min(axis=1),
        bars.close[starts + per - 1],
        bars.volume[idx].sum(axis=1),
        bars.quote_volume[idx].sum(axis=1),
        bars.taker_buy_quote[idx].sum(axis=1),
        target,
    )


# --------------------------------------------------------------------------- Binance REST klines


def binance_klines(
    view: LakeView,
    symbol: str,
    interval: str,
    as_of: datetime,
    window: timedelta,
    *,
    route: str = "klines",
    max_records: int = 64,
) -> Bars:
    """Closed Binance klines with `open_time >= as_of - window` and `close_time <= as_of`.

    `route="mark_price_klines"` reads mark price klines (same row layout, volumes are zero).
    """
    as_of = ensure_utc(as_of)
    interval_ms = INTERVAL_MS[interval]
    as_of_ms = _ms(as_of)
    start_ms = _ms(as_of - window)
    key = f"{symbol}:{interval}"
    collected: dict[int, tuple[int, float, float, float, float, float, float, float]] = {}
    upper = as_of
    for _ in range(max_records):
        record = view.latest(BINANCE, route, upper, key=key, lookback=window + _HOUR)
        if record is None:
            break
        rows = view.per_record("kline_rows", record, lambda r: _kline_rows(view.binance_data(r)))
        fetched_ms = _ms(record.fetched_at)
        earliest: int | None = None
        for parsed in rows:
            open_ms = parsed[0]
            earliest = open_ms if earliest is None else min(earliest, open_ms)
            close_ms = open_ms + interval_ms
            if close_ms > fetched_ms or close_ms > as_of_ms or open_ms < start_ms:
                continue
            collected.setdefault(open_ms, parsed)
        if earliest is None or earliest <= start_ms:
            break
        nxt = min(
            record.fetched_at - timedelta(microseconds=1),
            datetime.fromtimestamp((earliest + interval_ms) / 1000.0, tz=UTC),
        )
        if nxt >= upper:
            break
        upper = nxt
    return Bars.from_rows([collected[k] for k in sorted(collected)], interval_ms)


def _kline_rows(rows: Any) -> list[tuple[int, float, float, float, float, float, float, float]]:
    """Every well-formed kline row of one record body (memoized per record by `binance_klines`)."""
    return [p for row in (rows if isinstance(rows, list) else []) if (p := _kline_row(row)) is not None]


def _kline_row(row: Any) -> tuple[int, float, float, float, float, float, float, float] | None:
    if not isinstance(row, list) or len(row) < 11:
        return None
    values = [as_float(row[i]) for i in (1, 2, 3, 4, 5, 7, 10)]
    open_ms = row[0]
    if not isinstance(open_ms, int) or any(v is None for v in values):
        return None
    o, h, lo, c, v, q, taker_q = (float(x) for x in values)  # type: ignore[arg-type]
    return (open_ms, o, h, lo, c, v, q, taker_q)


# --------------------------------------------------------------------------- CMC OHLCV hourly

_CmcRow = tuple[int, float, float, float, float, float, float, float]


def cmc_hourly(view: LakeView, as_of: datetime, days: int) -> dict[int, Bars]:
    """Closed CMC hourly bars per coin id over the last `days` days (aggregate USD prices)."""
    as_of = ensure_utc(as_of)
    as_of_ms = _ms(as_of)
    start = as_of - timedelta(days=days)
    merged: dict[int, dict[int, tuple[datetime, _CmcRow]]] = {}
    hour = start.replace(minute=0, second=0, microsecond=0)
    while hour <= as_of:
        for fetched_at, coin_id, row in _cmc_hour(view, hour):
            if fetched_at > as_of or row[0] + INTERVAL_MS["1h"] > as_of_ms or row[0] < _ms(start):
                continue
            slot = merged.setdefault(coin_id, {})
            previous = slot.get(row[0])
            if previous is None or fetched_at >= previous[0]:
                slot[row[0]] = (fetched_at, row)
        hour += _HOUR
    for coin_id, slot in merged.items():
        for fetched_at, row in _backfill_rows(view, coin_id, as_of):
            if row[0] + INTERVAL_MS["1h"] <= as_of_ms and row[0] >= _ms(start):
                slot.setdefault(row[0], (fetched_at, row))
    return {
        coin_id: Bars.from_rows([rows[k][1] for k in sorted(rows)], INTERVAL_MS["1h"])
        for coin_id, rows in merged.items()
    }


def _backfill_rows(view: LakeView, coin_id: int, as_of: datetime) -> list[tuple[datetime, _CmcRow]]:
    """Bars of the coin's newest successful backfill record fetched at or before `as_of`."""
    record = view.pit.latest(
        CMC, CMC_OHLCV_BACKFILL_ROUTE, as_of, key=str(coin_id), lookback=_BACKFILL_LOOKBACK
    )
    if record is None:
        return []
    body = view.body_json(record)
    if not cmc_ok(record, body) or not _hourly_sampled(record):
        return []
    out = []
    for item_id, quotes in _ohlcv_items(body.get("data")):
        if item_id != coin_id:
            continue
        for quote in quotes:
            row = _ohlcv_row(quote, record.fetched_at)
            if row is not None:
                out.append((record.fetched_at, row))
    return out


def _cmc_hour(view: LakeView, hour: datetime) -> list[tuple[datetime, int, _CmcRow]]:
    def compute() -> list[tuple[datetime, int, _CmcRow]]:
        out: list[tuple[datetime, int, _CmcRow]] = []
        records = view.records(
            CMC, "ohlcv_historical", hour, hour + _HOUR - timedelta(microseconds=1), as_of=hour + _HOUR
        )
        for record in records:
            body = view.body_json(record)
            if not cmc_ok(record, body) or not _hourly_sampled(record):
                continue
            for coin_id, quotes in _ohlcv_items(body.get("data")):
                for quote in quotes:
                    row = _ohlcv_row(quote, record.fetched_at)
                    if row is not None:
                        out.append((record.fetched_at, coin_id, row))
        return out

    return view.per_hour("cmc_ohlcv_hourly", hour, compute)


def _hourly_sampled(record: RawRecord) -> bool:
    """The call asked for `interval=hourly`: without it CMC samples the hourly periods daily (each day's
    00:00 bar only), and those records would leave 23-hour gaps in the series."""
    return bool(json.loads(record.params_json).get("interval") == "hourly")


def _ohlcv_items(data: Any) -> list[tuple[int, list[Any]]]:
    """`data` is `{id, quotes}` for one coin, `{"<id>": {id, quotes}}` or a list of such items."""
    items: list[Any]
    if isinstance(data, Mapping) and "quotes" in data:
        items = [data]
    elif isinstance(data, Mapping):
        items = []
        for value in data.values():
            items.extend(value if isinstance(value, list) else [value])
    elif isinstance(data, list):
        items = data
    else:
        return []
    out = []
    for item in items:
        if not isinstance(item, Mapping) or not isinstance(item.get("quotes"), list):
            continue
        coin_id = item.get("id")
        if isinstance(coin_id, int) and not isinstance(coin_id, bool):
            out.append((coin_id, list(item["quotes"])))
    return out


def _ohlcv_row(quote: Any, fetched_at: datetime) -> _CmcRow | None:
    if not isinstance(quote, Mapping):
        return None
    opened, closed = parse_ts(quote.get("time_open")), parse_ts(quote.get("time_close"))
    usd = usd_quote(quote)
    if opened is None or closed is None or usd is None or closed > fetched_at:
        return None
    values = [as_float(usd.get(k)) for k in ("open", "high", "low", "close", "volume")]
    if any(v is None for v in values):
        return None
    o, h, lo, c, v = (float(x) for x in values)  # type: ignore[arg-type]
    open_ms = _ms(opened.replace(minute=0, second=0, microsecond=0))
    return (open_ms, o, h, lo, c, v, v, 0.0)


def _ms(moment: datetime) -> int:
    return int(ensure_utc(moment).timestamp() * 1000)


def to_ms(moment: datetime) -> int:
    return _ms(moment)
