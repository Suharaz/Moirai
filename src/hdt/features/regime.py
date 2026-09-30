"""Macro features (Macro agent): market context, market-wide liquidations, regime label, exchange reserves.

- Fear & Greed, altcoin season index, BTC dominance, altcoin market cap, CMC20 / CMC100 24h return;
- total liquidations 1h / 4h / 24h and their long share (route #4);
- regime = trend / sideways (BTC 4h ADX >= `regime.adx_trend_min`) x high / low volatility (realized vol of
  BTC 1h returns over `vol_window_h` vs the median of that rolling vol over `vol_reference_days`);
- route #28 (named consumer): `exch_reserve_{stable,btc}_chg_{1d,7d}`, the change in stablecoin (units ~ USD)
  and BTC (coin units, so the BTC price does not leak in) balances over the route #2 exchanges present at both
  times.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from typing import Any

import numpy as np
import talib

from hdt.contracts.common import DataQualityFlag
from hdt.core.config import IndicatorsFile
from hdt.features.bars import Bars, resample
from hdt.features.common import FeatureBlock, change
from hdt.features.lake_io import LakeView, as_float
from hdt.features.liquidations import parse_liq_quote, skew

MACRO_ROUTES = {
    "fear_greed": "fear_greed_latest",
    "altcoin_season": "altcoin_season_latest",
    "global": "global_metrics_latest",
    "cmc20": "cmc20_latest",
    "cmc100": "cmc100_latest",
    "total_liq": "total_liquidations",
}
RESERVE_ROUTE = "exchange_assets"


def context_features(
    view: LakeView, as_of: datetime, stale: timedelta, cadence: Mapping[str, timedelta]
) -> FeatureBlock:
    """`stale` is the market-data threshold: a route counts as stale only once its newest record is older
    than its own `cadence` plus `stale`, since a 15 min route has nothing newer to offer in between."""
    block = FeatureBlock()
    lookback = timedelta(hours=2)

    def data(route: str) -> tuple[Any, bool]:
        record, value = view.latest_cmc_ok(route, as_of, lookback=lookback)
        allowed = stale + cadence.get(route, timedelta(0))
        return value, record is not None and as_of - record.fetched_at > allowed

    groups: list[tuple[str, dict[str, float | None], Any, bool]] = []
    fg, fg_stale = data(MACRO_ROUTES["fear_greed"])
    groups.append(("fear_greed", {"fear_greed": _num(fg, "value")}, fg, fg_stale))
    alt, alt_stale = data(MACRO_ROUTES["altcoin_season"])
    groups.append(("altcoin_season", {"altcoin_index": _num(alt, "altcoin_index")}, alt, alt_stale))
    glob, glob_stale = data(MACRO_ROUTES["global"])
    usd = (
        glob.get("quote", {}).get("USD")
        if isinstance(glob, Mapping) and isinstance(glob.get("quote"), Mapping)
        else None
    )
    groups.append(
        (
            "global",
            {
                "btc_dominance": _num(glob, "btc_dominance"),
                "altcoin_market_cap": _num(usd, "altcoin_market_cap"),
                "total_market_cap": _num(usd, "total_market_cap"),
            },
            glob,
            glob_stale,
        )
    )
    for name in ("cmc20", "cmc100"):
        idx, idx_stale = data(MACRO_ROUTES[name])
        pct = _num(idx, "value_24h_percentage_change")
        groups.append(
            (name, {f"{name}_return_24h": pct / 100.0 if pct is not None else None}, idx, idx_stale)
        )
    tot, tot_stale = data(MACRO_ROUTES["total_liq"])
    quotes = tot.get("quotes") if isinstance(tot, Mapping) else None
    parsed = (
        parse_liq_quote(quotes[0])
        if isinstance(quotes, list) and quotes and isinstance(quotes[0], Mapping)
        else None
    )
    groups.append(
        (
            "total_liq",
            {
                "market_liq_1h_usd": parsed.total_1h if parsed else None,
                "market_liq_4h_usd": parsed.total_4h if parsed else None,
                "market_liq_24h_usd": parsed.total_24h if parsed else None,
                "market_liq_skew4": skew(parsed.long_4h, parsed.total_4h) if parsed else None,
            },
            tot,
            tot_stale,
        )
    )
    for _, values, raw, is_stale in groups:
        block.update(values)
        if raw is None:
            block.flag(DataQualityFlag.MISSING_ROUTE, *values)
        elif is_stale:
            block.flag(DataQualityFlag.STALE, *values)
    return block


def _num(data: Any, key: str) -> float | None:
    return as_float(data.get(key)) if isinstance(data, Mapping) else None


def regime_features(btc_1h: Bars, params: IndicatorsFile) -> FeatureBlock:
    block = FeatureBlock()
    names = ("regime", "regime_trend", "regime_high_vol", "btc_adx_4h", "btc_realized_vol")
    rp = params.regime
    bars_4h = resample(btc_1h, "4h")
    needed_vol = rp.vol_window_h + 1
    if len(bars_4h) < 2 * params.adx_period + 1 or len(btc_1h) < needed_vol:
        block.update(dict.fromkeys(names))
        block.flag(DataQualityFlag.MISSING_ROUTE, *names)
        return block
    adx = talib.ADX(bars_4h.high, bars_4h.low, bars_4h.close, timeperiod=params.adx_period)
    adx_last = float(adx[-1]) if not np.isnan(adx[-1]) else None
    returns = np.diff(np.log(btc_1h.close))
    rolling = _rolling_std(returns, rp.vol_window_h)
    reference = rolling[-rp.vol_reference_days * 24 :]
    reference = reference[~np.isnan(reference)]
    vol_now = float(rolling[-1]) if rolling.size and not np.isnan(rolling[-1]) else None
    trend = adx_last is not None and adx_last >= rp.adx_trend_min
    high_vol = vol_now is not None and reference.size > 0 and vol_now >= float(np.median(reference))
    known = adx_last is not None and vol_now is not None
    block.set("btc_adx_4h", adx_last)
    block.set("btc_realized_vol", vol_now)
    block.set("regime_trend", trend if known else None)
    block.set("regime_high_vol", high_vol if known else None)
    block.set(
        "regime", f"{'trend' if trend else 'sideways'}_{'high' if high_vol else 'low'}_vol" if known else None
    )
    if not known:
        block.flag(DataQualityFlag.MISSING_ROUTE, *names)
    return block


def _rolling_std(values: np.ndarray, window: int) -> np.ndarray:
    out = np.full(values.size, np.nan)
    if values.size < window:
        return out
    view = np.lib.stride_tricks.sliding_window_view(values, window)
    out[window - 1 :] = np.std(view, axis=1, ddof=1)
    return out


# --------------------------------------------------------------------------- exchange reserves (#28)


def reserves_at(
    view: LakeView, slugs: Sequence[str], at: datetime, params: IndicatorsFile
) -> dict[str, tuple[float, float]]:
    """Per exchange: (stablecoin balance, BTC balance) from the newest successful record at or before `at`."""
    out: dict[str, tuple[float, float]] = {}
    stables = {s.upper() for s in params.reserves.stablecoins}
    btc = params.reserves.btc_symbol.upper()
    for slug in slugs:
        _, data = view.latest_cmc_ok(RESERVE_ROUTE, at, key=slug, lookback=timedelta(days=2))
        if not isinstance(data, list):
            continue
        stable_sum = btc_sum = 0.0
        for item in data:
            currency = item.get("currency") if isinstance(item, Mapping) else None
            symbol = str(currency.get("symbol", "")).upper() if isinstance(currency, Mapping) else ""
            balance = as_float(item.get("balance")) if isinstance(item, Mapping) else None
            if balance is None:
                continue
            if symbol in stables:
                stable_sum += balance
            elif symbol == btc:
                btc_sum += balance
        out[slug] = (stable_sum, btc_sum)
    return out


def reserve_features(
    view: LakeView, slugs: Sequence[str], as_of: datetime, params: IndicatorsFile
) -> FeatureBlock:
    block = FeatureBlock()
    now = reserves_at(view, slugs, as_of, params)
    for label, delta in (("1d", timedelta(days=1)), ("7d", timedelta(days=7))):
        then = reserves_at(view, slugs, as_of - delta, params)
        common = sorted(set(now) & set(then))
        for i, kind in enumerate(("stable", "btc")):
            name = f"exch_reserve_{kind}_chg_{label}"
            value = (
                change(sum(now[s][i] for s in common), sum(then[s][i] for s in common)) if common else None
            )
            block.set(name, value)
            if value is None:
                block.flag(DataQualityFlag.MISSING_ROUTE, name)
    return block
