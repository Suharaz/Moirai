"""12h labels, Design Contract section 4 (`label_spec_version` in config/scanner.yaml `labels`).

- mark at time t: the newest Binance 1 s mark event (`ws_markprice`) with event time in [t - max_gap, t]
  from records fetched in [t - max_gap, t + max_gap] (an earlier record wins a tie on event time);
  fallback: the close of the newest 1m mark price kline (`mark_price_klines`) closing in [t - max_gap, t].
  No mark within `max_mark_gap_s`: the label is missing (None), never interpolated;
- RAW_12H return: mark(t + h) / mark(t) - 1; RESID_12H return: r_coin - beta_btc(t) x r_btc, with
  `beta_btc` taken point in time at t (the same value the packet carries);
- the binary label is 1 when the return is > 0, else 0; the signal-direction return multiplies by +1 for
  LONG and -1 for SHORT.
Labels necessarily read the future of `as_of`; they read only records fetched by t + h + max_gap.

`MarkBook` answers exactly like `mark_at` but parses each lake hour of 1 s mark events once, for callers
that need thousands of marks (gate G1, the p_model fit). Realized funding for cost netting comes from the
Binance `funding_rate` route (`funding_settlements`).
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Protocol

import numpy as np

from hdt.contracts.common import TargetType
from hdt.core.clock import ensure_utc
from hdt.core.config import LabelParams
from hdt.features.bars import binance_klines
from hdt.features.lake_io import BINANCE, LakeView, as_float

MARK_ROUTE = "ws_markprice"
MARK_KLINE_ROUTE = "mark_price_klines"
FUNDING_RATE_ROUTE = "funding_rate"
_HOUR = timedelta(hours=1)
_US = timedelta(microseconds=1)


def _mark_events(body: Any) -> Iterator[tuple[str, int, float]]:
    """(symbol, event time ms, mark price) of every well-formed event in one `ws_markprice` record."""
    for item in body if isinstance(body, list) else []:
        data = item.get("data") if isinstance(item, Mapping) else None
        for event in data if isinstance(data, list) else [data]:
            if not isinstance(event, Mapping):
                continue
            symbol, ts, price = event.get("s"), event.get("E"), as_float(event.get("p"))
            if (
                isinstance(symbol, str)
                and isinstance(ts, int)
                and not isinstance(ts, bool)
                and price is not None
            ):
                yield symbol, ts, price


def _kline_mark(view: LakeView, symbol: str, t: datetime, max_gap: timedelta) -> float | None:
    """Close of the newest 1m mark price kline closing in [t - max_gap, t] (the `mark_at` fallback)."""
    lo_ms = int(t.timestamp() * 1000) - int(max_gap.total_seconds() * 1000)
    bars = binance_klines(view, symbol, "1m", t, max_gap + timedelta(minutes=1), route=MARK_KLINE_ROUTE)
    if len(bars) and int(bars.close_time[-1]) >= lo_ms:
        return float(bars.close[-1])
    return None


def mark_at(view: LakeView, symbol: str, t: datetime, max_gap: timedelta) -> float | None:
    t = ensure_utc(t)
    t_ms = int(t.timestamp() * 1000)
    lo_ms = t_ms - int(max_gap.total_seconds() * 1000)
    best: tuple[int, float] | None = None
    for record in view.records(BINANCE, MARK_ROUTE, t - max_gap, t + max_gap, as_of=t + max_gap):
        for event_symbol, ts, price in _mark_events(view.body_json(record)):
            if event_symbol == symbol and lo_ms <= ts <= t_ms and (best is None or ts > best[0]):
                best = (ts, price)
    if best is not None:
        return best[1]
    return _kline_mark(view, symbol, t, max_gap)


@dataclass(frozen=True)
class _SymbolMarks:
    """One symbol's mark events of one lake hour, sorted by (event time, lake order)."""

    event_ms: np.ndarray
    price: np.ndarray
    fetched_us: np.ndarray


class MarkBook:
    """Many `mark_at` lookups over one lake view; each closed hour of mark events is parsed once.

    Returns exactly what `mark_at(view, symbol, t, max_gap)` returns. At most `max_hours` parsed hours are
    kept (least recently used first out); open hours are never cached, so live callers stay exact.
    """

    def __init__(self, view: LakeView, *, max_hours: int = 48) -> None:
        self.view = view
        self._max_hours = max_hours
        self._hours: OrderedDict[datetime, dict[str, _SymbolMarks]] = OrderedDict()

    def mark(self, symbol: str, t: datetime, max_gap: timedelta) -> float | None:
        t = ensure_utc(t)
        t_ms = int(t.timestamp() * 1000)
        lo_ms = t_ms - int(max_gap.total_seconds() * 1000)
        f_lo, f_hi = _us(t - max_gap), _us(t + max_gap)
        best: tuple[int, float] | None = None
        hour = (t - max_gap).replace(minute=0, second=0, microsecond=0)
        while hour <= t + max_gap:
            marks = self._hour(hour).get(symbol)
            hour += _HOUR
            if marks is None:
                continue
            found = _newest(marks, lo_ms, t_ms, f_lo, f_hi)
            if found is not None and (best is None or found[0] > best[0]):
                best = found
        if best is not None:
            return best[1]
        return _kline_mark(self.view, symbol, t, max_gap)

    def _hour(self, hour: datetime) -> dict[str, _SymbolMarks]:
        cached = self._hours.get(hour)
        if cached is not None:
            self._hours.move_to_end(hour)
            return cached
        end = hour + _HOUR - _US
        acc: dict[str, list[tuple[int, float, int]]] = {}
        for record in self.view.records(BINANCE, MARK_ROUTE, hour, end, as_of=end):
            fetched = _us(record.fetched_at)
            for symbol, ts, price in _mark_events(self.view.body_json(record)):
                acc.setdefault(symbol, []).append((ts, price, fetched))
        parsed: dict[str, _SymbolMarks] = {}
        for symbol, rows in acc.items():
            # A stable sort keeps lake order among equal event times (the earlier record wins a tie).
            rows.sort(key=lambda row: row[0])
            parsed[symbol] = _SymbolMarks(
                np.fromiter((r[0] for r in rows), dtype=np.int64, count=len(rows)),
                np.fromiter((r[1] for r in rows), dtype=np.float64, count=len(rows)),
                np.fromiter((r[2] for r in rows), dtype=np.int64, count=len(rows)),
            )
        if self.view.is_closed(hour):
            self._hours[hour] = parsed
            while len(self._hours) > self._max_hours:
                self._hours.popitem(last=False)
        return parsed


def _newest(marks: _SymbolMarks, lo_ms: int, hi_ms: int, f_lo: int, f_hi: int) -> tuple[int, float] | None:
    """The newest event in [lo_ms, hi_ms] fetched in [f_lo, f_hi]; the earliest in lake order on a tie."""
    start = int(np.searchsorted(marks.event_ms, lo_ms, side="left"))
    stop = int(np.searchsorted(marks.event_ms, hi_ms, side="right"))
    if stop <= start:
        return None
    window = slice(start, stop)
    ok = (marks.fetched_us[window] >= f_lo) & (marks.fetched_us[window] <= f_hi)
    if not ok.any():
        return None
    events = marks.event_ms[window]
    newest = int(events[ok].max())
    first = start + int(np.flatnonzero(ok & (events == newest))[0])
    return newest, float(marks.price[first])


def _us(moment: datetime) -> int:
    delta = ensure_utc(moment) - datetime(1970, 1, 1, tzinfo=moment.tzinfo)
    return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds


class MarkSource(Protocol):
    def __call__(self, symbol: str, t: datetime, max_gap: timedelta) -> float | None: ...


def simple_return(start: float | None, end: float | None) -> float | None:
    if start is None or end is None or start <= 0:
        return None
    return end / start - 1.0


@dataclass(frozen=True)
class Label:
    target_type: TargetType
    horizon_h: int
    coin_return: float | None
    btc_return: float | None
    beta_btc: float | None
    value: float | None
    """RAW: coin return; RESID: coin return - beta x BTC return."""

    @property
    def y(self) -> int | None:
        return None if self.value is None else int(self.value > 0)

    def signed(self, side: str) -> float | None:
        if self.value is None:
            return None
        return self.value if side == "LONG" else -self.value


def resolve_label(
    view: LakeView,
    *,
    symbol: str,
    btc_symbol: str,
    as_of: datetime,
    horizon_h: int,
    target_type: TargetType,
    beta_btc: float | None,
    params: LabelParams,
    marks: MarkSource | None = None,
) -> Label:
    """The label of (symbol, as_of); `marks` (e.g. `MarkBook(view).mark`) replaces `mark_at` for speed."""
    gap = timedelta(seconds=params.max_mark_gap_s)
    end = ensure_utc(as_of) + timedelta(hours=horizon_h)

    def mark(sym: str, t: datetime) -> float | None:
        return marks(sym, t, gap) if marks is not None else mark_at(view, sym, t, gap)

    coin = simple_return(mark(symbol, as_of), mark(symbol, end))
    if target_type is TargetType.RAW_12H:
        return Label(target_type, horizon_h, coin, None, None, coin)
    btc = simple_return(mark(btc_symbol, as_of), mark(btc_symbol, end))
    value = coin - beta_btc * btc if coin is not None and btc is not None and beta_btc is not None else None
    return Label(target_type, horizon_h, coin, btc, beta_btc, value)


# --------------------------------------------------------------------------- realized funding


@dataclass(frozen=True)
class Settlement:
    time_ms: int
    rate: float
    mark: float | None
    """Mark price at the settlement (Binance `markPrice`; None when not reported)."""


def funding_settlements(
    view: LakeView, symbol: str, start: datetime, end: datetime, *, fetched_by: datetime
) -> list[Settlement] | None:
    """Binance funding settlements of `symbol` with start < fundingTime <= end.

    Read from the earliest `funding_rate` record fetched in [end, fetched_by] whose history (a contiguous
    list of the latest settlements) reaches back to `start`, so no settlement of the window can be missing.
    None when no such record exists (the window is not known yet or was never recorded).
    """
    start, end = ensure_utc(start), ensure_utc(end)
    start_ms, end_ms = int(start.timestamp() * 1000), int(end.timestamp() * 1000)
    for record in view.records(BINANCE, FUNDING_RATE_ROUTE, end, fetched_by, as_of=fetched_by, key=symbol):
        rows = _settlement_rows(view.binance_data(record), symbol)
        if not rows or rows[0].time_ms > start_ms:
            continue
        return [row for row in rows if start_ms < row.time_ms <= end_ms]
    return None


def _settlement_rows(data: Any, symbol: str) -> list[Settlement]:
    out: dict[int, Settlement] = {}
    for item in data if isinstance(data, list) else []:
        if not isinstance(item, Mapping) or item.get("symbol") != symbol:
            continue
        ts, rate = item.get("fundingTime"), as_float(item.get("fundingRate"))
        if not isinstance(ts, int) or isinstance(ts, bool) or rate is None:
            continue
        mark = as_float(item.get("markPrice"))
        out[ts] = Settlement(ts, rate, mark if mark is not None and mark > 0 else None)
    return [out[k] for k in sorted(out)]


def funding_paid(settlements: list[Settlement], entry_mark: float) -> float:
    """Funding a long position of notional 1 at `entry_mark` pays (negative: it receives).

    Each settlement charges rate x the position notional at that settlement (entry notional scaled by the
    settlement mark; the entry notional when the mark is not reported). A short pays the negative.
    """
    total = 0.0
    for item in settlements:
        notional = item.mark / entry_mark if item.mark is not None and entry_mark > 0 else 1.0
        total += item.rate * notional
    return total
