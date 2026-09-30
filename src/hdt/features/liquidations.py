"""Liquidation features: CMC per-coin windows, SPIKE / DECAY / Skew, the SPIKE floor and the Binance
forceOrder lower bound.

Semantics (Design Contract section 6, red-team #32):
- CMC `liquidations_by_crypto` aggregates every tracked exchange; windows are rolling 1h / 4h / 24h. A coin
  absent from a *complete* list cycle had 0 liquidations. An error page or a missing page is
  `missing_route` and nothing is inferred (never 0, never an older record).
- SPIKE = ((L4h - L1h) / 3) / den with den = max((L24h - L4h) / 20, floor_c) and
  floor_c = max(liq_floor_usd, q20 of the coin's hourly liquidations over 30 days). SPIKE is null unless
  L4h - L1h >= min_liq_notional_usd and >= min_liq_oi_frac x OI(t - 4h).
- DECAY = L1h / ((L4h - L1h) / 3).
- Skew = long share of liquidated notional (long liquidations = forced sells): 1 = only longs flushed.
- Binance `!forceOrder@arr` keeps one event per symbol per second and saturates in cascades, so its sums
  are only a lower bound for the Binance part of the CMC total (`liq_mismatch` when CMC is below it).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import partial
from typing import Any

from hdt.core.config import LiquidationParams, SpikeParams
from hdt.features.common import quantile
from hdt.features.lake_io import BINANCE, LakeView, ListStatus, as_float, parse_ts

LIQ_ROUTE = "liquidations_by_crypto"
WINDOWS = ("1h", "4h", "24h")
_HOUR = timedelta(hours=1)


@dataclass(frozen=True)
class CoinLiq:
    """Liquidated notional (USD) per window; long = longs liquidated (forced sells)."""

    total_1h: float
    total_4h: float
    total_24h: float
    long_1h: float
    long_4h: float
    long_24h: float
    short_1h: float
    short_4h: float
    short_24h: float
    last_updated: datetime | None

    @classmethod
    def zero(cls, at: datetime | None) -> CoinLiq:
        return cls(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, at)

    def total(self, window: str) -> float:
        return float(getattr(self, f"total_{window}"))

    def long(self, window: str) -> float:
        return float(getattr(self, f"long_{window}"))

    def short(self, window: str) -> float:
        return float(getattr(self, f"short_{window}"))


@dataclass(frozen=True)
class LiqSnapshot:
    status: ListStatus
    by_coin: Mapping[int, CoinLiq]
    fetched_at: datetime | None
    cycle_id: str | None

    def coin(self, coin_id: int) -> CoinLiq | None:
        """The coin's windows; zero when absent from a complete list; None when the list is not usable."""
        if self.status != "complete":
            return None
        return self.by_coin.get(coin_id) or CoinLiq.zero(self.fetched_at)


def read_liquidations(view: LakeView, as_of: datetime, params: LiquidationParams) -> LiqSnapshot:
    listing = view.latest_list(
        LIQ_ROUTE,
        as_of,
        items_field="cryptocurrencies",
        paging="has_more",
        lookback=timedelta(seconds=params.list_cycle_window_s) + _HOUR,
        cycle_window=timedelta(seconds=params.list_cycle_window_s),
        max_pages=params.list_max_pages,
    )
    if not listing.complete:
        return LiqSnapshot(listing.status, {}, listing.fetched_at, listing.cycle_id)
    return LiqSnapshot("complete", parse_liq_items(listing.items), listing.fetched_at, listing.cycle_id)


def parse_liq_items(items: Sequence[Any]) -> dict[int, CoinLiq]:
    out: dict[int, CoinLiq] = {}
    for item in items:
        if not isinstance(item, Mapping):
            continue
        coin_id = item.get("crypto_id", item.get("id"))
        quote = _usd_liq_quote(item)
        if not isinstance(coin_id, int) or isinstance(coin_id, bool) or quote is None:
            continue
        parsed = parse_liq_quote(quote)
        if parsed is not None:
            out[coin_id] = parsed
    return out


def _usd_liq_quote(item: Mapping[str, Any]) -> Mapping[str, Any] | None:
    quotes = item.get("quotes")
    if isinstance(quotes, list):
        for quote in quotes:
            if isinstance(quote, Mapping) and (
                quote.get("symbol") == "USD" or quote.get("crypto_id") == 2781
            ):
                return quote
        if len(quotes) == 1 and isinstance(quotes[0], Mapping):
            return quotes[0]
    return None


def parse_liq_quote(quote: Mapping[str, Any]) -> CoinLiq | None:
    values: dict[str, float] = {}
    for side in ("total", "long", "short"):
        for window in WINDOWS:
            value = as_float(quote.get(f"{side}_liquidations_{window}"))
            if value is None or value < 0:
                return None
            values[f"{side}_{window}"] = value
    return CoinLiq(**values, last_updated=parse_ts(quote.get("last_updated")))


# --------------------------------------------------------------------------- SPIKE / DECAY / Skew


@dataclass(frozen=True)
class Spike:
    spike: float | None
    decay: float | None
    den: float | None
    floor_applied: bool
    valid: bool
    """False when the notional or OI validity test failed (SPIKE is then null)."""


def spike_decay(liq: CoinLiq, oi_4h_ago_usd: float | None, floor_c: float, params: SpikeParams) -> Spike:
    body = liq.total_4h - liq.total_1h
    per_hour_3h = body / 3.0
    decay = liq.total_1h / per_hour_3h if per_hour_3h > 0 else None
    base = (liq.total_24h - liq.total_4h) / 20.0
    den = max(base, floor_c)
    floor_applied = floor_c > base
    notional_ok = body >= params.min_liq_notional_usd
    oi_ok = oi_4h_ago_usd is not None and oi_4h_ago_usd > 0 and body >= params.min_liq_oi_frac * oi_4h_ago_usd
    valid = notional_ok and oi_ok
    spike = per_hour_3h / den if valid and den > 0 else None
    return Spike(spike, decay, den, floor_applied, valid)


def skew(long_usd: float, total_usd: float) -> float | None:
    """Long share of liquidated notional; None when nothing was liquidated."""
    if total_usd <= 0:
        return None
    return min(max(long_usd / total_usd, 0.0), 1.0)


def spike_floor(hourly_samples: Sequence[float], params: SpikeParams, min_samples: int) -> float:
    """floor_c = max(liq_floor_usd, q20 of hourly liquidations); the quantile needs `min_samples`."""
    if len(hourly_samples) < min_samples:
        return params.liq_floor_usd
    q = quantile(list(hourly_samples), params.floor_quantile)
    return max(params.liq_floor_usd, q if q is not None else 0.0)


@dataclass(frozen=True)
class HourlyLiq:
    """`total_liquidations_1h` sampled at each hour boundary whose list cycle was complete."""

    hours: int
    by_coin: Mapping[int, Sequence[float]]

    def samples(self, coin_id: int) -> list[float]:
        """The coin's samples; a coin absent from every complete cycle has only zero hours."""
        values = self.by_coin.get(coin_id)
        return list(values) if values is not None else [0.0] * self.hours


def hourly_liquidations(view: LakeView, as_of: datetime, days: int, params: LiquidationParams) -> HourlyLiq:
    """Per coin hourly samples over `days` days; coins absent from a complete cycle contribute 0."""
    end = as_of.replace(minute=0, second=0, microsecond=0)
    boundary = end - timedelta(days=days)
    by_coin: dict[int, list[float]] = {}
    hours = 0
    while boundary <= end:
        snap = view.per_hour(
            "liq_hourly_sample", boundary - _HOUR, partial(_sample_at, view, boundary, params)
        )
        boundary += _HOUR
        if snap is None:
            continue
        for coin_id in snap.keys() - by_coin.keys():
            by_coin[coin_id] = [0.0] * hours
        for coin_id, values in by_coin.items():
            values.append(snap.get(coin_id, 0.0))
        hours += 1
    return HourlyLiq(hours, by_coin)


def _sample_at(view: LakeView, boundary: datetime, params: LiquidationParams) -> dict[int, float] | None:
    snap = read_liquidations(view, boundary, params)
    if snap.status != "complete":
        return None
    return {coin_id: liq.total_1h for coin_id, liq in snap.by_coin.items()}


# --------------------------------------------------------------------------- Binance forceOrder


@dataclass(frozen=True)
class ForceEvent:
    ts_ms: int
    symbol: str
    long_liquidated: bool
    notional_usd: float


def force_events(view: LakeView, as_of: datetime, window: timedelta) -> list[ForceEvent]:
    """forceOrder events with trade time in (as_of - window, as_of] from records fetched by `as_of`."""
    lo_ms = int((as_of - window).timestamp() * 1000)
    hi_ms = int(as_of.timestamp() * 1000)
    out: list[ForceEvent] = []
    hour = (as_of - window).replace(minute=0, second=0, microsecond=0)
    while hour <= as_of:
        for fetched_at, event in view.per_hour("force_events", hour, partial(_force_hour, view, hour)):
            if fetched_at <= as_of and lo_ms < event.ts_ms <= hi_ms:
                out.append(event)
        hour += _HOUR
    return out


def _force_hour(view: LakeView, hour: datetime) -> list[tuple[datetime, ForceEvent]]:
    out: list[tuple[datetime, ForceEvent]] = []
    for record in view.records(
        BINANCE, "ws_forceorder", hour, hour + _HOUR - timedelta(microseconds=1), as_of=hour + _HOUR
    ):
        items = view.body_json(record)
        for item in items if isinstance(items, list) else []:
            event = _force_event(item.get("data") if isinstance(item, Mapping) else None)
            if event is not None:
                out.append((record.fetched_at, event))
    return out


def _force_event(data: Any) -> ForceEvent | None:
    if not isinstance(data, Mapping) or not isinstance(data.get("o"), Mapping):
        return None
    order = data["o"]
    symbol, side, ts = order.get("s"), order.get("S"), order.get("T")
    qty = as_float(order.get("z")) or as_float(order.get("q"))
    price = as_float(order.get("ap")) or as_float(order.get("p"))
    if (
        not isinstance(symbol, str)
        or side not in ("BUY", "SELL")
        or not isinstance(ts, int)
        or not qty
        or not price
    ):
        return None
    # A liquidated long is closed by a forced SELL order; a liquidated short by a forced BUY.
    return ForceEvent(ts, symbol, side == "SELL", qty * price)


@dataclass(frozen=True)
class ForceLowerBound:
    total_1h: float
    total_4h: float
    long_4h: float
    short_4h: float


def force_lower_bounds(events: Sequence[ForceEvent], as_of: datetime) -> dict[str, ForceLowerBound]:
    """Per Binance symbol lower bounds of liquidated notional over 1h and 4h (USDT ~ USD)."""
    hi_ms = int(as_of.timestamp() * 1000)
    one_h = hi_ms - 3_600_000
    acc: dict[str, list[float]] = {}
    for event in events:
        slot = acc.setdefault(event.symbol, [0.0, 0.0, 0.0, 0.0])
        slot[1] += event.notional_usd
        if event.long_liquidated:
            slot[2] += event.notional_usd
        else:
            slot[3] += event.notional_usd
        if event.ts_ms > one_h:
            slot[0] += event.notional_usd
    return {symbol: ForceLowerBound(*values) for symbol, values in acc.items()}
