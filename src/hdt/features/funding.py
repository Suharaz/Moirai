"""Funding features on an hourly basis.

Binance adjusts funding intervals per symbol (8h -> 4h -> 1h). A per-period rate halves when the interval
halves at the same hourly cost, so every funding comparison uses `funding_per_h = rate / interval_h`.

Interval resolution for a Binance symbol at time t (red-team #32):
1. listed: `funding_info` (daily REST `/fapi/v1/fundingInfo`) lists only symbols with an adjusted interval,
   so a symbol absent from a recorded list runs at Binance's default (`funding.default_interval_h`, 8 h);
   without any recorded list nothing is listed;
2. inferred: the latest gap between consecutive distinct `nextFundingTime` values seen in `premium_index`
   records over `funding.infer_window_h`;
3. both known and different: the inferred value wins and `funding_interval_changed` is raised; only one
   known: that one; none: the exchange is excluded from `F_c` with `funding_interval_unknown`.
Non-Binance venues (CMC route #2 pairs) have no interval source at all: excluded and flagged, never guessed.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import partial
from typing import Any

from hdt.core.config import FundingParams
from hdt.features.lake_io import BINANCE, LakeView, as_float

_HOUR = timedelta(hours=1)
_MS_PER_H = 3_600_000


@dataclass(frozen=True)
class PremiumRow:
    symbol: str
    mark: float | None
    index: float | None
    rate: float | None
    next_funding_ms: int | None
    time_ms: int


def premium_rows(view: LakeView, as_of: datetime, lookback: timedelta) -> dict[str, PremiumRow]:
    """Newest Binance premium index per symbol: the REST snapshot overlaid with a newer 1 s mark event."""
    rows: dict[str, PremiumRow] = {}
    record = view.latest(BINANCE, "premium_index", as_of, lookback=lookback)
    for item in _as_list(view.binance_data(record)):
        row = _premium_row(item, record.fetched_at if record else None)
        if row is not None:
            rows[row.symbol] = row
    ws = view.latest(BINANCE, "ws_markprice", as_of, lookback=lookback)
    items = view.body_json(ws) if ws is not None else None
    for item in items if isinstance(items, list) else []:
        data = item.get("data") if isinstance(item, Mapping) else None
        for event in data if isinstance(data, list) else [data]:
            row = _mark_event(event)
            if row is not None and (row.symbol not in rows or row.time_ms > rows[row.symbol].time_ms):
                rows[row.symbol] = row
    return rows


def _as_list(data: Any) -> list[Any]:
    if isinstance(data, list):
        return data
    return [data] if isinstance(data, Mapping) else []


def _premium_row(item: Any, fetched_at: datetime | None) -> PremiumRow | None:
    if not isinstance(item, Mapping) or not isinstance(item.get("symbol"), str):
        return None
    ts = item.get("time")
    if not isinstance(ts, int):
        if fetched_at is None:
            return None
        ts = int(fetched_at.timestamp() * 1000)
    nft = item.get("nextFundingTime")
    return PremiumRow(
        item["symbol"],
        as_float(item.get("markPrice")),
        as_float(item.get("indexPrice")),
        as_float(item.get("lastFundingRate")),
        nft if isinstance(nft, int) and nft > 0 else None,
        ts,
    )


def _mark_event(event: Any) -> PremiumRow | None:
    if (
        not isinstance(event, Mapping)
        or not isinstance(event.get("s"), str)
        or not isinstance(event.get("E"), int)
    ):
        return None
    nft = event.get("T")
    return PremiumRow(
        event["s"],
        as_float(event.get("p")),
        as_float(event.get("i")),
        as_float(event.get("r")),
        nft if isinstance(nft, int) and nft > 0 else None,
        event["E"],
    )


# --------------------------------------------------------------------------- interval resolution


@dataclass(frozen=True)
class IntervalResolution:
    hours: float | None
    changed: bool
    """`funding_info` and the nextFundingTime gaps disagree (the inferred value is used)."""
    listed: float | None
    inferred: float | None


def funding_info(view: LakeView, as_of: datetime, lookback: timedelta) -> dict[str, float] | None:
    """Symbol -> fundingIntervalHours from the newest successful `funding_info` record (None: no record)."""
    record = view.latest(BINANCE, "funding_info", as_of, lookback=lookback)
    data = view.binance_data(record)
    if not isinstance(data, list):
        return None
    out: dict[str, float] = {}
    for item in data:
        if isinstance(item, Mapping) and isinstance(item.get("symbol"), str):
            hours = as_float(item.get("fundingIntervalHours"))
            if hours is not None and hours > 0:
                out[item["symbol"]] = hours
    return out


def next_funding_times(view: LakeView, as_of: datetime, window: timedelta) -> dict[str, list[int]]:
    """Per symbol, distinct nextFundingTime values first seen in records fetched within the window."""
    seen: dict[str, dict[int, datetime]] = {}
    hour = (as_of - window).replace(minute=0, second=0, microsecond=0)
    lo = as_of - window
    while hour <= as_of:
        for symbol, values in view.per_hour(
            "next_funding_times", hour, partial(_nft_hour, view, hour)
        ).items():
            slot = seen.setdefault(symbol, {})
            for nft, first_seen in values:
                if lo <= first_seen <= as_of and (nft not in slot or first_seen < slot[nft]):
                    slot[nft] = first_seen
        hour += _HOUR
    return {symbol: sorted(values) for symbol, values in seen.items()}


def _nft_hour(view: LakeView, hour: datetime) -> dict[str, list[tuple[int, datetime]]]:
    first: dict[str, dict[int, datetime]] = {}
    end = hour + _HOUR - timedelta(microseconds=1)
    for record in view.records(BINANCE, "premium_index", hour, end, as_of=end):
        for item in _as_list(view.binance_data(record)):
            row = _premium_row(item, record.fetched_at)
            if row is not None and row.next_funding_ms is not None:
                first.setdefault(row.symbol, {}).setdefault(row.next_funding_ms, record.fetched_at)
    return {symbol: sorted(values.items()) for symbol, values in first.items()}


def inferred_interval(next_times: Sequence[int]) -> float | None:
    """Hours between the two most recent distinct funding times (None with fewer than two)."""
    distinct = sorted(set(next_times))
    if len(distinct) < 2:
        return None
    gap = distinct[-1] - distinct[-2]
    return gap / _MS_PER_H if gap > 0 else None


def resolve_interval(listed: float | None, inferred: float | None) -> IntervalResolution:
    if inferred is not None and listed is not None and abs(inferred - listed) > 1e-9:
        return IntervalResolution(inferred, True, listed, inferred)
    hours = inferred if inferred is not None else listed
    return IntervalResolution(hours, False, listed, inferred)


@dataclass(frozen=True)
class FundingState:
    """Funding inputs of one moment (t or t - lookback) for every Binance symbol."""

    rows: Mapping[str, PremiumRow]
    listed: Mapping[str, float] | None
    """Symbols of the newest recorded `funding_info` list (None: no list recorded)."""
    next_times: Mapping[str, Sequence[int]]
    default_interval_h: float | None = None
    """Interval of a symbol absent from a recorded list (Binance lists only adjusted symbols)."""

    def interval(self, symbol: str) -> IntervalResolution:
        listed = self.listed.get(symbol, self.default_interval_h) if self.listed is not None else None
        return resolve_interval(listed, inferred_interval(self.next_times.get(symbol, ())))

    def per_hour(self, symbol: str) -> tuple[float | None, IntervalResolution]:
        row = self.rows.get(symbol)
        resolution = self.interval(symbol)
        if row is None or row.rate is None or resolution.hours is None:
            return None, resolution
        return row.rate / resolution.hours, resolution


def funding_state(view: LakeView, at: datetime, params: FundingParams, stale: timedelta) -> FundingState:
    return FundingState(
        premium_rows(view, at, stale),
        funding_info(view, at, timedelta(days=2)),
        next_funding_times(view, at, timedelta(hours=params.infer_window_h)),
        params.default_interval_h,
    )


# --------------------------------------------------------------------------- F_c and the drop condition


@dataclass(frozen=True)
class VenueFunding:
    venue: str
    rate: float | None
    interval_h: float | None
    oi_usd: float | None


def weighted_funding(venues: Sequence[VenueFunding]) -> tuple[float | None, list[str]]:
    """OI-weighted hourly funding over venues with a known interval; also the excluded venue names."""
    num = den = 0.0
    excluded: list[str] = []
    for venue in venues:
        if venue.rate is None:
            continue
        if venue.interval_h is None or venue.interval_h <= 0:
            excluded.append(venue.venue)
            continue
        weight = venue.oi_usd if venue.oi_usd is not None and venue.oi_usd > 0 else None
        if weight is None:
            continue
        num += weight * venue.rate / venue.interval_h
        den += weight
    return (num / den if den > 0 else None), sorted(excluded)


def funding_drop(now_per_h: float | None, before_per_h: float | None, min_abs_per_h: float) -> float | None:
    """(F_before - F_now) / |F_before|: +0.5 means hourly funding fell by half of its magnitude.

    LONG LTX needs drop >= threshold (crowded-long funding released); SHORT uses -drop (funding rose).
    Undefined (None) when |F_before| is below `min_abs_per_h`.
    """
    if now_per_h is None or before_per_h is None or abs(before_per_h) < min_abs_per_h:
        return None
    return (before_per_h - now_per_h) / abs(before_per_h)
