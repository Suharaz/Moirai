"""Shared price-level candidate set (red-team #10): one set per (coin, as_of), identical for every agent.

Level sources (Binance contract price units): confirmed 1h / 4h swing pivots from Binance 15m klines over
`levels.lookback_days`, high-volume nodes of a volume profile over the same bars, and order book walls
(a level with qty >= `wall_mult` x the median level qty). Supports lie below the mark, resistances above.

Per side, entries are the structural supports (LONG) / resistances (SHORT) within the pinned risk
`max_entry_distance_atr` ATR of the mark, plus ATR-anchored entries at `entry_atr_offsets` (same bound).
For each entry:
- invalidation: beyond the nearest structural level on the far side of the entry, `stop_buffer_atr` ATR
  further, when that makes a stop distance within [stop_min_atr, stop_max_atr] ATR; else `stop_default_atr`;
- tp1: the nearest opposing structural level reaching R:R >= `min_rr` within `tp_max_atr` ATR; else the
  larger of `min_rr` x risk and `tp_fallback_atr` ATR (dropped when that exceeds `tp_max_atr`).
Prices are rounded to the exchange tick away from the favorable side (LONG entry/stop/tp1 down, SHORT up),
R:R is recomputed on the rounded values, and candidates with R:R < `min_rr` or wrong geometry are dropped.
Entries closer than `min_separation_atr` ATR to an accepted one are skipped; at most `max_per_side`.
`candidate_id = "lc_" + base32(sha256(coin_id|as_of|side|entry|invalidation|tp1|levels_ver))[:16]`.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Any

import numpy as np

from hdt.contracts.candidate import CandidateSet, LevelCandidate
from hdt.contracts.common import Side
from hdt.core.config import LevelsParams
from hdt.core.ids import b32_digest, decimal_str, utc_timestamp
from hdt.features.bars import Bars
from hdt.features.micro import Book
from hdt.features.technical import swing_pivots


@dataclass(frozen=True)
class LevelRules:
    """Runtime bounds from the pinned risk config (Risk re-checks the same values)."""

    min_rr: float
    max_entry_atr: float


@dataclass(frozen=True)
class LevelInputs:
    mark: float | None
    atr: float | None
    tick: Decimal | None
    bars_1h: Bars
    bars_4h: Bars
    book: Book | None


def tick_size(exchange_info: Any, symbol: str) -> Decimal | None:
    """PRICE_FILTER tickSize of `symbol` from a Binance exchangeInfo body."""
    symbols = exchange_info.get("symbols") if isinstance(exchange_info, Mapping) else None
    for item in symbols if isinstance(symbols, list) else []:
        if not isinstance(item, Mapping) or item.get("symbol") != symbol:
            continue
        for flt in item.get("filters") or []:
            if isinstance(flt, Mapping) and flt.get("filterType") == "PRICE_FILTER":
                try:
                    tick = Decimal(str(flt.get("tickSize")))
                except ArithmeticError:
                    return None
                return Decimal(decimal_str(tick)) if tick.is_finite() and tick > 0 else None
    return None


def structural_levels(inputs: LevelInputs, params: LevelsParams) -> list[float]:
    """Pivot prices (1h, 4h), volume-profile nodes and book walls; sorted, de-duplicated."""
    levels: list[float] = []
    for bars in (inputs.bars_1h, inputs.bars_4h):
        if len(bars) > 2 * params.pivot_width:
            highs, lows = swing_pivots(bars.high, bars.low, params.pivot_width)
            levels += [float(bars.high[i]) for i in highs] + [float(bars.low[i]) for i in lows]
    levels += volume_nodes(inputs.bars_1h, params)
    levels += book_walls(inputs.book, params)
    return sorted({round(x, 12) for x in levels if np.isfinite(x) and x > 0})


def volume_nodes(bars: Bars, params: LevelsParams) -> list[float]:
    if len(bars) < 2:
        return []
    lo, hi = float(np.min(bars.low)), float(np.max(bars.high))
    if hi <= lo:
        return []
    typical = (bars.high + bars.low + bars.close) / 3.0
    hist, edges = np.histogram(typical, bins=params.volume_profile_bins, range=(lo, hi), weights=bars.volume)
    order = np.argsort(-hist, kind="stable")[: params.volume_nodes]
    return [float((edges[i] + edges[i + 1]) / 2.0) for i in order if hist[i] > 0]


def book_walls(book: Book | None, params: LevelsParams) -> list[float]:
    if book is None:
        return []
    levels = list(book.bids) + list(book.asks)
    if not levels:
        return []
    median = float(np.median([q for _, q in levels]))
    return [p for p, q in levels if median > 0 and q >= params.wall_mult * median]


def _round(value: float, tick: Decimal, mode: str) -> Decimal:
    return (Decimal(repr(value)) / tick).to_integral_value(rounding=mode) * tick


def _candidate_id(
    coin_id: int, as_of: datetime, side: Side, entry: Decimal, stop: Decimal, tp1: Decimal, ver: str
) -> str:
    raw = "|".join(
        (
            str(coin_id),
            utc_timestamp(as_of),
            side.value,
            decimal_str(entry),
            decimal_str(stop),
            decimal_str(tp1),
            ver,
        )
    )
    return "lc_" + b32_digest(raw, 16)


def side_candidates(
    coin_id: int,
    as_of: datetime,
    side: Side,
    inputs: LevelInputs,
    levels: Sequence[float],
    params: LevelsParams,
    rules: LevelRules,
) -> list[LevelCandidate]:
    mark, atr, tick = inputs.mark, inputs.atr, inputs.tick
    min_rr = rules.min_rr
    if mark is None or atr is None or tick is None or atr <= 0 or mark <= 0:
        return []
    sign = 1.0 if side is Side.LONG else -1.0
    mode = ROUND_FLOOR if side is Side.LONG else ROUND_CEILING
    # Structural entries: supports below (LONG) / resistances above (SHORT), nearest first.
    near = [x for x in levels if 0 < sign * (mark - x) <= rules.max_entry_atr * atr]
    near.sort(key=lambda x: abs(mark - x))
    anchored = [mark - sign * off * atr for off in params.entry_atr_offsets if off <= rules.max_entry_atr]
    out: list[LevelCandidate] = []
    accepted: list[float] = []
    for entry_f in near + anchored:
        if len(out) >= params.max_per_side:
            break
        if any(abs(entry_f - a) < params.min_separation_atr * atr for a in accepted):
            continue
        candidate = _build(coin_id, as_of, side, entry_f, levels, atr, tick, mode, sign, params, min_rr)
        if candidate is not None:
            out.append(candidate)
            accepted.append(entry_f)
    return out


def _build(
    coin_id: int,
    as_of: datetime,
    side: Side,
    entry_f: float,
    levels: Sequence[float],
    atr: float,
    tick: Decimal,
    mode: str,
    sign: float,
    params: LevelsParams,
    min_rr: float,
) -> LevelCandidate | None:
    buffer = params.stop_buffer_atr * atr
    stop_f: float | None = None
    for level in sorted((x for x in levels if sign * (entry_f - x) > 0), key=lambda x: abs(entry_f - x)):
        distance = abs(entry_f - level) + buffer
        if distance < params.stop_min_atr * atr:
            continue
        if distance <= params.stop_max_atr * atr:
            stop_f = entry_f - sign * distance
        break
    if stop_f is None:
        stop_f = entry_f - sign * params.stop_default_atr * atr
    entry, stop = _round(entry_f, tick, mode), _round(stop_f, tick, mode)
    if min(entry, stop) <= 0 or entry == stop:
        return None
    risk = float(abs(entry - stop))
    tp1: Decimal | None = None
    for level in sorted(
        (x for x in levels if sign * (x - float(entry)) > 0), key=lambda x: abs(x - float(entry))
    ):
        rounded = _round(level, tick, mode)
        reward = float(abs(rounded - entry))
        if reward > params.tp_max_atr * atr:
            break
        if sign * float(rounded - entry) > 0 and reward >= min_rr * risk:
            tp1 = rounded
            break
    if tp1 is None:
        reward = max(min_rr * risk, params.tp_fallback_atr * atr)
        if reward > params.tp_max_atr * atr:
            return None
        # Rounded away from the entry so the rounded R:R never falls below min_rr.
        away = ROUND_CEILING if side is Side.LONG else ROUND_FLOOR
        tp1 = _round(float(entry) + sign * reward, tick, away)
    if tp1 <= 0:
        return None
    rr = float(abs(tp1 - entry) / abs(entry - stop))
    ok = stop < entry < tp1 if side is Side.LONG else tp1 < entry < stop
    if not ok or rr < min_rr:
        return None
    return LevelCandidate(
        candidate_id=_candidate_id(coin_id, as_of, side, entry, stop, tp1, params.levels_ver),
        side=side,
        entry=entry,
        invalidation=stop,
        tp1=tp1,
        rr=rr,
        tick=tick,
    )


def candidate_set(
    coin_id: int, as_of: datetime, inputs: LevelInputs, params: LevelsParams, rules: LevelRules
) -> CandidateSet:
    levels = structural_levels(inputs, params)
    candidates: dict[str, LevelCandidate] = {}
    for side in (Side.LONG, Side.SHORT):
        for candidate in side_candidates(coin_id, as_of, side, inputs, levels, params, rules):
            candidates.setdefault(candidate.candidate_id, candidate)
    ordered = tuple(candidates[k] for k in sorted(candidates))
    return CandidateSet(coin_id=coin_id, as_of=as_of, levels_ver=params.levels_ver, candidates=ordered)
