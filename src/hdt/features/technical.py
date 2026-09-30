"""Technical features (Technical agent): TA-Lib indicators per timeframe, compressed to last / slope_atr / z.

Timeframes <= 15m use Binance klines, >= 1h use CMC OHLCV hourly bars (aggregated USD price) scaled into
Binance contract units by the symbol multiplier, so price-unit values are comparable with `mark_price`.
Candle patterns count only at an S/R zone (close within `sr_zone_atr` ATR of a swing pivot) with a volume
ratio >= `candle_pattern_min_volume_ratio`. Route #10 adds ATH / ATL distance and period performance.

Feature names are `<tf>_<indicator>_<last|slope_atr|z>` plus a few scale-free extras per timeframe.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import numpy as np
import talib
from talib._ta_lib import MA_Type

from hdt.contracts.common import DataQualityFlag
from hdt.core.config import IndicatorsFile
from hdt.features.bars import Bars
from hdt.features.common import FeatureBlock, compress, finite, last_valid, ratio
from hdt.features.lake_io import as_float, usd_quote

PERF_PERIODS = ("7d", "30d", "90d", "365d")


def swing_pivots(high: np.ndarray, low: np.ndarray, width: int) -> tuple[list[int], list[int]]:
    """Confirmed swing highs / lows: the extreme of the `width` bars on each side (ties keep the first)."""
    n = high.size
    span = 2 * width + 1
    if n < span:
        return [], []
    win_h = np.lib.stride_tricks.sliding_window_view(high, span)
    win_l = np.lib.stride_tricks.sliding_window_view(low, span)
    # A window holding NaN has a NaN extreme, which equals nothing: no pivot, as in a per-bar scan.
    is_high = (high[width : n - width] == win_h.max(axis=1)) & (win_h.argmax(axis=1) == width)
    is_low = (low[width : n - width] == win_l.min(axis=1)) & (win_l.argmin(axis=1) == width)
    return (np.flatnonzero(is_high) + width).tolist(), (np.flatnonzero(is_low) + width).tolist()


def structure_score(high: np.ndarray, low: np.ndarray, width: int) -> int | None:
    """+1 higher high and higher low, -1 lower high and lower low, 0 mixed; None without two of each."""
    highs, lows = swing_pivots(high, low, width)
    if len(highs) < 2 or len(lows) < 2:
        return None
    hh = high[highs[-1]] > high[highs[-2]]
    hl = low[lows[-1]] > low[lows[-2]]
    if hh and hl:
        return 1
    if not hh and not hl:
        return -1
    return 0


def rsi_divergence(
    close: np.ndarray, high: np.ndarray, low: np.ndarray, rsi: np.ndarray, width: int
) -> int | None:
    """+1 bullish (price lower low, RSI higher low), -1 bearish (higher high, RSI lower high), else 0."""
    highs, lows = swing_pivots(high, low, width)
    bull = bear = False
    if len(lows) >= 2:
        a, b = lows[-2], lows[-1]
        if not (np.isnan(rsi[a]) or np.isnan(rsi[b])):
            bull = bool(low[b] < low[a] and rsi[b] > rsi[a])
    if len(highs) >= 2:
        a, b = highs[-2], highs[-1]
        if not (np.isnan(rsi[a]) or np.isnan(rsi[b])):
            bear = bool(high[b] > high[a] and rsi[b] < rsi[a])
    if len(lows) < 2 and len(highs) < 2:
        return None
    return int(bull) - int(bear)


def session_vwap(bars: Bars) -> float | None:
    """VWAP of the current UTC day up to the last closed bar (typical price x volume)."""
    if not len(bars):
        return None
    day = bars.open_time // 86_400_000
    mask = day == day[-1]
    vol = bars.volume[mask]
    if float(vol.sum()) <= 0:
        return None
    typical = (bars.high[mask] + bars.low[mask] + bars.close[mask]) / 3.0
    return finite(float((typical * vol).sum() / vol.sum()))


def volume_ratio(volume: np.ndarray, window: int) -> float | None:
    if volume.size <= window:
        return None
    base = float(np.mean(volume[-window - 1 : -1]))
    return ratio(float(volume[-1]), base) if base > 0 else None


def pattern_score(
    bars: Bars, patterns: tuple[str, ...], atr: float | None, params: IndicatorsFile
) -> int | None:
    """Sum of TA-Lib pattern signs on the last bar, counted only at an S/R zone with volume confirmation."""
    tech = params.technical
    if len(bars) < tech.min_bars or atr is None or atr <= 0:
        return None
    vr = volume_ratio(bars.volume, params.volume_ratio_window)
    highs, lows = swing_pivots(bars.high, bars.low, tech.pivot_width)
    levels = [bars.high[i] for i in highs] + [bars.low[i] for i in lows]
    close = float(bars.close[-1])
    at_zone = any(abs(close - float(level)) <= tech.sr_zone_atr * atr for level in levels)
    if vr is None or vr < params.candle_pattern_min_volume_ratio or not at_zone:
        return 0
    score = 0
    for name in patterns:
        fn = getattr(talib, name)
        out = fn(bars.open, bars.high, bars.low, bars.close)
        last = int(out[-1])
        score += (last > 0) - (last < 0)
    return score


def timeframe_features(tf: str, bars: Bars, params: IndicatorsFile) -> FeatureBlock:
    block = FeatureBlock()
    tech = params.technical
    names = _names(tf, params)
    if len(bars) < tech.min_bars:
        block.update(dict.fromkeys(names))
        block.flag(DataQualityFlag.MISSING_ROUTE, *names)
        return block
    h, lo, c, v = bars.high, bars.low, bars.close, bars.volume
    close = float(c[-1])
    atr_series = talib.ATR(h, lo, c, timeperiod=params.atr_period)
    atr = last_valid(atr_series)
    kw: dict[str, Any] = {
        "atr": atr,
        "close": close,
        "slope_bars": tech.slope_bars,
        "z_window": params.zscore_window,
    }
    for period in params.ema_periods:
        ema = talib.EMA(c, timeperiod=period)
        block.update(compress(f"{tf}_ema{period}", ema, price_unit=True, **kw))
        last = last_valid(ema)
        block.set(f"{tf}_close_vs_ema{period}_atr", ratio(close - last, atr) if last is not None else None)
    adx = talib.ADX(h, lo, c, timeperiod=params.adx_period)
    block.update(compress(f"{tf}_adx{params.adx_period}", adx, price_unit=False, **kw))
    di = talib.PLUS_DI(h, lo, c, timeperiod=params.adx_period) - talib.MINUS_DI(
        h, lo, c, timeperiod=params.adx_period
    )
    block.set(f"{tf}_di_diff", last_valid(di))
    rsi = talib.RSI(c, timeperiod=params.rsi_period)
    block.update(compress(f"{tf}_rsi{params.rsi_period}", rsi, price_unit=False, **kw))
    fast, slow, signal = params.macd
    _, _, hist = talib.MACD(c, fastperiod=fast, slowperiod=slow, signalperiod=signal)
    block.update(compress(f"{tf}_macd_hist", hist, price_unit=True, **kw))
    rsi_p, stoch_p, k_p, d_p = params.stoch_rsi
    base_rsi = talib.RSI(c, timeperiod=rsi_p)
    valid_rsi = np.where(np.isnan(base_rsi), np.nan, base_rsi)
    k, _ = talib.STOCH(
        valid_rsi,
        valid_rsi,
        valid_rsi,
        fastk_period=stoch_p,
        slowk_period=k_p,
        slowk_matype=MA_Type.SMA,
        slowd_period=d_p,
        slowd_matype=MA_Type.SMA,
    )
    block.update(compress(f"{tf}_stochrsi_k", k, price_unit=False, **kw))
    block.update(compress(f"{tf}_atr{params.atr_period}", atr_series, price_unit=True, **kw))
    block.set(f"{tf}_atr_pct", ratio(atr, close))
    bb_period, bb_dev = params.bollinger
    upper, middle, lower = talib.BBANDS(
        c, timeperiod=bb_period, nbdevup=bb_dev, nbdevdn=bb_dev, matype=MA_Type.SMA
    )
    width = (upper - lower) / middle
    block.update(compress(f"{tf}_bb_width", width, price_unit=False, **kw))
    vwap = session_vwap(bars)
    block.set(f"{tf}_close_vs_vwap_atr", ratio(close - vwap, atr) if vwap is not None else None)
    obv = talib.OBV(c, v)
    obv_c = compress(f"{tf}_obv", obv, price_unit=False, **kw)
    mean_vol = float(np.mean(v[-tech.slope_bars :])) if v.size >= tech.slope_bars else 0.0
    obv_c[f"{tf}_obv_slope_atr"] = (
        finite((float(obv[-1]) - float(obv[-1 - tech.slope_bars])) / (tech.slope_bars * mean_vol))
        if obv.size > tech.slope_bars and mean_vol > 0
        else None
    )
    block.update(obv_c)
    block.set(f"{tf}_volume_ratio", volume_ratio(v, params.volume_ratio_window))
    block.set(f"{tf}_structure", structure_score(h, lo, tech.pivot_width))
    block.set(f"{tf}_rsi_divergence", rsi_divergence(c, h, lo, rsi, tech.pivot_width))
    if tf in tech.pattern_timeframes:
        block.set(f"{tf}_pattern_score", pattern_score(bars, tech.patterns, atr, params))
    return block


def _names(tf: str, params: IndicatorsFile) -> list[str]:
    stems = [f"ema{p}" for p in params.ema_periods] + [
        f"adx{params.adx_period}",
        f"rsi{params.rsi_period}",
        "macd_hist",
        "stochrsi_k",
        f"atr{params.atr_period}",
        "bb_width",
        "obv",
    ]
    names = [f"{tf}_{stem}_{part}" for stem in stems for part in ("last", "slope_atr", "z")]
    names += [f"{tf}_close_vs_ema{p}_atr" for p in params.ema_periods]
    names += [
        f"{tf}_{x}"
        for x in ("di_diff", "atr_pct", "close_vs_vwap_atr", "volume_ratio", "structure", "rsi_divergence")
    ]
    if tf in params.technical.pattern_timeframes:
        names.append(f"{tf}_pattern_score")
    return names


def technical_features(
    bars_by_tf: Mapping[str, Bars],
    perf: Mapping[str, Any] | None,
    close: float | None,
    params: IndicatorsFile,
) -> FeatureBlock:
    block = FeatureBlock()
    for tf in params.technical.timeframes:
        block.merge(timeframe_features(tf, bars_by_tf.get(tf, Bars.empty(0)), params))
    block.merge(performance_features(perf, close))
    return block


def performance_features(periods: Mapping[str, Any] | None, close_usd: float | None) -> FeatureBlock:
    """Route #10 `data["<id>"].periods`: ATH / ATL distance (all_time) and period returns (fractions)."""
    block = FeatureBlock()
    names = ["ath_distance", "atl_distance", *(f"perf_{p}" for p in PERF_PERIODS)]
    if not isinstance(periods, Mapping):
        block.update(dict.fromkeys(names))
        block.flag(DataQualityFlag.MISSING_ROUTE, *names)
        return block
    all_time = _period_usd(periods.get("all_time"))
    high = as_float(all_time.get("high")) if all_time else None
    low = as_float(all_time.get("low")) if all_time else None
    block.set("ath_distance", close_usd / high - 1.0 if close_usd is not None and high else None)
    block.set("atl_distance", close_usd / low - 1.0 if close_usd is not None and low else None)
    for period in PERF_PERIODS:
        quote = _period_usd(periods.get(period))
        pct = as_float(quote.get("percent_change")) if quote else None
        block.set(f"perf_{period}", pct / 100.0 if pct is not None else None)
    missing = [name for name in names if block.values.get(name) is None]
    if missing:
        block.flag(DataQualityFlag.MISSING_ROUTE, *missing)
    return block


def _period_usd(period: Any) -> Mapping[str, Any] | None:
    if not isinstance(period, Mapping):
        return None
    return usd_quote(period)
