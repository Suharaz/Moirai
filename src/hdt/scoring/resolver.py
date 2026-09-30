"""Label resolver (phase 08): the 12 h label of every scored council event, by its own `target_type`.

- `RAW_12H`: y = 1 when the Binance mark rises over the horizon; `RESID_12H` (setups with a beta hedge):
  y = 1 when the residual return `r_coin - beta_btc x r_btc` is positive, with `beta_btc` taken from the
  event's committed packet (the value the council saw). Both come from `hdt.quant.labels.resolve_label`,
  which reads the 1 s mark events and falls back to the 1m `mark_price_klines` close.
- Kline fallback of a label: B2 fetches `mark_price_klines` hourly, so the 1m kline closing at t is in a
  record fetched up to `kline_fetch_lag` (1 h) after t. `label_marks` therefore re-reads the klines from
  records fetched by `t + kline_fetch_lag + max_mark_gap` (still only klines closing in [t - gap, t]) when
  the live `mark_at` bound finds nothing.
- Triple-barrier label (the stacking target): along the 1m mark kline path of `(as_of, as_of + horizon]`,
  +1 when the mark first reaches `entry x (1 + tp_atr x atr)`, -1 when it first reaches
  `entry x (1 - sl_atr x atr)` (a bar touching both counts as the stop), 0 at the timeout. `atr` is the
  packet ATR(1h) as a fraction of the packet mark. `barrier_y` = 1 for +1, 0 for -1, and the 12 h mark
  direction at the timeout.
- Look-ahead bound: every mark read is at or before `as_of + horizon` (event time), from lake records
  fetched by `as_of + horizon + kline_fetch_lag + max_mark_gap` at the latest.
- A label that cannot be resolved (no mark within `max_mark_gap_s`, or no BTC mark / beta for RESID) is
  retried until `missing_after_h` after the horizon and then recorded as `missing`: no event is dropped
  silently (`LabelOutcome.status`). The service re-resolves a `missing` / `error` label for a bounded window
  (`hdt.scoring.store.retry_cards`); marks stay point in time, so only records fetched by the bound count.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Final, Literal

from hdt.contracts.common import TargetType
from hdt.contracts.packet import QuantPacket
from hdt.core.clock import ensure_utc
from hdt.core.config import BarrierParams, LabelParams
from hdt.features.bars import binance_klines
from hdt.features.lake_io import LakeView
from hdt.lake.universe import Universe
from hdt.quant.labels import MARK_KLINE_ROUTE, MarkSource, mark_at, resolve_label
from hdt.scoring.regime_weights import regime_of

LabelStatus = Literal["resolved", "missing", "pending"]
_MINUTE_MS: Final[int] = 60_000


@dataclass(frozen=True)
class LabelRequest:
    event_id: str
    coin_id: int
    symbol: str
    btc_symbol: str
    as_of: datetime
    horizon_h: int
    target_type: TargetType
    label_spec_version: str
    beta_btc: float | None
    atr_frac: float | None
    regime: str | None = None

    @property
    def end(self) -> datetime:
        return ensure_utc(self.as_of) + timedelta(hours=self.horizon_h)


@dataclass(frozen=True)
class LabelOutcome:
    request: LabelRequest
    status: LabelStatus
    y: int | None
    value: float | None
    coin_return: float | None
    btc_return: float | None
    barrier: int | None
    barrier_y: int | None


def _num(packet: QuantPacket, name: str) -> float | None:
    value = packet.features.get(name)
    return float(value) if isinstance(value, int | float) and not isinstance(value, bool) else None


def _late_kline_mark(
    view: LakeView, symbol: str, t: datetime, max_gap: timedelta, fetch_lag: timedelta
) -> float | None:
    """Close of the newest 1m mark kline closing in [t - max_gap, t], from records fetched by
    t + fetch_lag + max_gap (the hourly B2 fetch lands after the WS-era `mark_at` bound)."""
    t = ensure_utc(t)
    t_ms = int(t.timestamp() * 1000)
    lo_ms = t_ms - int(max_gap.total_seconds() * 1000)
    fetched_by = t + fetch_lag + max_gap
    bars = binance_klines(
        view,
        symbol,
        "1m",
        fetched_by,
        fetch_lag + 2 * max_gap + timedelta(minutes=1),
        route=MARK_KLINE_ROUTE,
    )
    close = bars.close_time
    for i in range(len(bars) - 1, -1, -1):
        if lo_ms <= int(close[i]) <= t_ms:
            return float(bars.close[i])
    return None


def label_marks(view: LakeView, fetch_lag: timedelta, marks: MarkSource | None = None) -> MarkSource:
    """`marks` (or `mark_at`), then the late kline fallback when it finds nothing."""

    def mark(symbol: str, t: datetime, max_gap: timedelta) -> float | None:
        found = marks(symbol, t, max_gap) if marks is not None else mark_at(view, symbol, t, max_gap)
        if found is not None or fetch_lag <= timedelta(0):
            return found
        return _late_kline_mark(view, symbol, t, max_gap, fetch_lag)

    return mark


def build_request(
    *,
    event_id: str,
    coin_id: int,
    card_symbol: str,
    as_of: datetime,
    horizon_h: int,
    target_type: TargetType,
    label_spec_version: str,
    packets: Mapping[str, QuantPacket],
    universe: Universe | None,
    btc_cmc_symbol: str,
) -> LabelRequest:
    """The label inputs of one event, all known at `as_of`: Binance symbols from the point-in-time
    universe (the card symbol when it is unavailable), `beta_btc`, ATR and mark from its committed packets."""
    member = next((m for m in universe.members if m.cmc_id == coin_id), None) if universe else None
    btc = next((m for m in universe.members if m.cmc_symbol == btc_cmc_symbol), None) if universe else None
    beta: float | None = None
    atr_frac: float | None = None
    for agent in sorted(packets):
        packet = packets[agent]
        if agent == "news":
            continue
        if beta is None:
            beta = _num(packet, "beta_btc")
        atr, mark = _num(packet, "atr_1h"), _num(packet, "mark_price")
        if atr_frac is None and atr is not None and mark is not None and mark > 0:
            atr_frac = atr / mark
    return LabelRequest(
        event_id=event_id,
        coin_id=coin_id,
        symbol=member.binance_symbol if member is not None else card_symbol,
        btc_symbol=btc.binance_symbol if btc is not None else "",
        as_of=ensure_utc(as_of),
        horizon_h=horizon_h,
        target_type=TargetType(target_type),
        label_spec_version=label_spec_version,
        beta_btc=beta if TargetType(target_type) is TargetType.RESID_12H else None,
        atr_frac=atr_frac,
        regime=regime_of(packets),
    )


def triple_barrier(
    view: LakeView,
    *,
    symbol: str,
    as_of: datetime,
    horizon_h: int,
    entry: float,
    atr_frac: float,
    params: BarrierParams,
    max_gap: timedelta,
    fetch_lag: timedelta = timedelta(0),
) -> int | None:
    """+1 / -1 / 0 as in the module docstring; None when the 1m mark path does not cover the horizon."""
    as_of = ensure_utc(as_of)
    end = as_of + timedelta(hours=horizon_h)
    start_ms = int(as_of.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    fetched_by = end + fetch_lag + max_gap
    bars = binance_klines(
        view,
        symbol,
        "1m",
        fetched_by,
        fetched_by - as_of,
        route=MARK_KLINE_ROUTE,
        max_records=4 * 60 * horizon_h,
    )
    upper = entry * (1.0 + params.tp_atr * atr_frac)
    lower = entry * (1.0 - params.sl_atr * atr_frac)
    seen = 0
    for i in range(len(bars)):
        opened = int(bars.open_time[i])
        if opened < start_ms or opened + bars.interval_ms > end_ms:
            continue
        seen += 1
        if float(bars.low[i]) <= lower:
            return -1
        if float(bars.high[i]) >= upper:
            return 1
    expected = (end_ms - start_ms) // _MINUTE_MS
    gap_bars = int(max_gap.total_seconds() // 60)
    if seen < expected - gap_bars:
        return None
    return 0


def resolve(
    view: LakeView,
    request: LabelRequest,
    *,
    labels: LabelParams,
    barrier: BarrierParams,
    now: datetime,
    missing_after: timedelta,
    marks: MarkSource | None = None,
    kline_fetch_lag: timedelta = timedelta(0),
) -> LabelOutcome:
    """Resolve one event; `pending` while its data may still arrive, `missing` once it no longer can."""
    source = label_marks(view, kline_fetch_lag, marks)
    gap = timedelta(seconds=labels.max_mark_gap_s)
    if ensure_utc(now) < request.end + gap:
        return LabelOutcome(request, "pending", None, None, None, None, None, None)
    label = resolve_label(
        view,
        symbol=request.symbol,
        btc_symbol=request.btc_symbol,
        as_of=request.as_of,
        horizon_h=request.horizon_h,
        target_type=request.target_type,
        beta_btc=request.beta_btc,
        params=labels,
        marks=source,
    )
    if label.y is None:
        status: LabelStatus = "missing" if ensure_utc(now) >= request.end + missing_after else "pending"
        return LabelOutcome(request, status, None, None, label.coin_return, label.btc_return, None, None)
    tb: int | None = None
    entry = source(request.symbol, request.as_of, gap)
    if entry is not None and request.atr_frac is not None and request.atr_frac > 0:
        tb = triple_barrier(
            view,
            symbol=request.symbol,
            as_of=request.as_of,
            horizon_h=request.horizon_h,
            entry=entry,
            atr_frac=request.atr_frac,
            params=barrier,
            max_gap=gap,
            fetch_lag=kline_fetch_lag,
        )
        # The label resolved from live marks before the hourly kline fetch covering the horizon end: stay
        # pending until that fetch can land, so the triple-barrier label is not locked in as NULL.
        if tb is None and ensure_utc(now) < request.end + kline_fetch_lag + gap:
            return LabelOutcome(request, "pending", None, None, None, None, None, None)
    raw_up = label.coin_return is not None and label.coin_return > 0
    barrier_y = None if tb is None else (1 if tb == 1 else 0 if tb == -1 else int(raw_up))
    return LabelOutcome(
        request, "resolved", label.y, label.value, label.coin_return, label.btc_return, tb, barrier_y
    )
