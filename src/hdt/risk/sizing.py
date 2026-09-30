"""Deterministic position sizing (Design Contract section 5). Pure functions, no I/O.

    p_side   = p if LONG else 1 - p                   (must be > 0.5)
    conf     = clip((p_side - 0.5) / 0.2, 0, 1)
    risk_usd = equity x risk_pct x conf x manager_size x flags.size_mult x mode.size_multiplier
    qty      = risk_usd / |entry - stop|, rounded DOWN to the lot step (never up to reach minNotional)
    notional <= min(max_margin_pct x equity x leverage_max, max_oi_frac x OI, max_volume_1h_frac x vol_1h)
    leverage = min(leverage_max, ceil(notional / (max_margin_pct x equity))), isolated

The isolated liquidation estimate (one-way, no cumulative maintenance amount, conservative `mmr`) must
sit farther from the entry than the stop by `liq_distance_mult` times the stop distance; otherwise the
leverage is lowered (margin stays within `max_margin_pct`) and, when that is not enough, the quantity.
Every parameter is clipped to its hard ceiling in `hdt.settings.ceilings` (a looser pinned value never
applies) and every step is recorded in the breakdown written to `risk_verdicts.sizing`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import ROUND_CEILING, Decimal
from typing import Any

from hdt.contracts.common import Side
from hdt.execution.filters import SymbolFilters
from hdt.settings.ceilings import (
    LEVERAGE_MAX,
    LIQ_DISTANCE_MULT_MIN,
    MAX_MARGIN_PCT,
    MAX_OI_FRAC,
    MAX_VOLUME_1H_FRAC,
    RISK_PCT_MAX,
    SIZE_MULTIPLIER_MAX,
)

CONF_SPAN = 0.2


class SizingRejectedError(ValueError):
    """The decision cannot be sized; `reason` is the verdict reason code."""

    def __init__(self, reason: str, detail: str, breakdown: dict[str, Any] | None = None) -> None:
        super().__init__(f"{reason}: {detail}")
        self.reason = reason
        self.detail = detail
        self.breakdown = breakdown or {}


def p_side_of(p: float, side: Side) -> float:
    return p if side is Side.LONG else 1.0 - p


def confidence(p_side: float) -> float:
    return min(1.0, max(0.0, (p_side - 0.5) / CONF_SPAN))


def liquidation_price(entry: Decimal, side: Side, leverage: int, mmr: Decimal) -> Decimal:
    """Isolated one-way estimate: LONG `E(1 - 1/L)/(1 - mmr)`, SHORT `E(1 + 1/L)/(1 + mmr)`."""
    inv = Decimal(1) / Decimal(leverage)
    if side is Side.LONG:
        return entry * (1 - inv) / (1 - mmr)
    return entry * (1 + inv) / (1 + mmr)


def liquidation_ratio(
    entry: Decimal, stop_distance: Decimal, side: Side, leverage: int, mmr: Decimal
) -> Decimal:
    """Liquidation distance from the entry in units of the stop distance."""
    return abs(entry - liquidation_price(entry, side, leverage, mmr)) / stop_distance


@dataclass(frozen=True)
class SizingInput:
    equity: Decimal
    equity_age_s: float | None
    side: Side
    p: float
    manager_size: float
    flags_size_mult: float
    flags_age_s: float | None
    mode: str
    mode_size_multiplier: float
    entry: Decimal
    stop: Decimal
    filters: SymbolFilters
    base_asset: str
    open_interest_usd: float
    quote_volume_1h_usd: float
    # parameters (pinned RiskFile values, already checked against the hard ceilings)
    risk_pct: float
    leverage_max: int
    max_margin_pct: float
    max_oi_frac: float
    max_volume_1h_frac: float
    mmr_assumed: float
    liq_distance_mult: float


@dataclass(frozen=True)
class SizingResult:
    qty: Decimal
    notional: Decimal
    leverage: int
    margin: Decimal
    risk_usd: Decimal
    risk_usd_actual: Decimal
    stop_distance: Decimal
    liquidation_price: Decimal
    binding: str | None
    breakdown: dict[str, Any] = field(default_factory=dict)


def _d(value: float) -> Decimal:
    return Decimal(repr(value))


def size_position(inp: SizingInput) -> SizingResult:
    """Size one OPEN; raises `SizingRejectedError` with a verdict reason when it cannot be opened."""
    if inp.equity <= 0:
        raise SizingRejectedError("no_equity", f"equity {inp.equity} is not positive")
    if not 0 < inp.risk_pct <= RISK_PCT_MAX:
        raise SizingRejectedError(
            "ceiling", f"risk_pct {inp.risk_pct} exceeds the hard ceiling {RISK_PCT_MAX}"
        )
    leverage_max = min(inp.leverage_max, LEVERAGE_MAX)
    max_margin_pct = min(inp.max_margin_pct, MAX_MARGIN_PCT)
    max_oi_frac = min(inp.max_oi_frac, MAX_OI_FRAC)
    max_volume_1h_frac = min(inp.max_volume_1h_frac, MAX_VOLUME_1H_FRAC)
    liq_distance_mult = max(inp.liq_distance_mult, LIQ_DISTANCE_MULT_MIN)
    mode_size_multiplier = min(inp.mode_size_multiplier, SIZE_MULTIPLIER_MAX)
    p_side = p_side_of(inp.p, inp.side)
    conf = confidence(p_side)
    stop_distance = abs(inp.entry - inp.stop)
    breakdown: dict[str, Any] = {
        "equity": float(inp.equity),
        "equity_age_s": inp.equity_age_s,
        "risk_pct": inp.risk_pct,
        "p_side": p_side,
        "conf": conf,
        "manager_size": inp.manager_size,
        "flags_size_mult": inp.flags_size_mult,
        "flags_age_s": inp.flags_age_s,
        "mode": inp.mode,
        "mode_size_multiplier": mode_size_multiplier,
        "entry": float(inp.entry),
        "stop": float(inp.stop),
        "stop_distance": float(stop_distance),
        "base_asset": inp.base_asset,
        "margin_cap_fraction": max_margin_pct,
        "risk_usd": 0.0,
        "qty": 0.0,
        "notional": 0.0,
        "leverage": 1,
        "margin": 0.0,
    }
    if p_side <= 0.5:
        raise SizingRejectedError("sign_mismatch", f"p_side {p_side:.4f} is not above 0.5", breakdown)
    if stop_distance <= 0:
        raise SizingRejectedError("bad_levels", "entry equals stop", breakdown)
    if inp.side is Side.LONG and not inp.stop < inp.entry:
        raise SizingRejectedError("bad_levels", "LONG stop must be below the entry", breakdown)
    if inp.side is Side.SHORT and not inp.stop > inp.entry:
        raise SizingRejectedError("bad_levels", "SHORT stop must be above the entry", breakdown)

    risk_usd = (
        inp.equity
        * _d(inp.risk_pct)
        * _d(conf)
        * _d(inp.manager_size)
        * _d(inp.flags_size_mult)
        * _d(mode_size_multiplier)
    )
    breakdown["risk_usd"] = float(risk_usd)
    if risk_usd <= 0:
        raise SizingRejectedError(
            "too_small", "risk budget is zero (conf, size or flags multiplier is 0)", breakdown
        )
    qty_raw = risk_usd / stop_distance

    margin_cap = inp.equity * _d(max_margin_pct)
    caps = {
        "margin cap": margin_cap * leverage_max,
        "open interest cap": _d(max_oi_frac) * _d(inp.open_interest_usd),
        "volume cap": _d(max_volume_1h_frac) * _d(inp.quote_volume_1h_usd),
    }
    cap_name, notional_cap = min(caps.items(), key=lambda kv: kv[1])
    breakdown["notional_cap"] = float(notional_cap)
    binding: str | None = None
    qty = qty_raw
    if qty * inp.entry > notional_cap:
        qty = notional_cap / inp.entry
        binding = cap_name
    qty = inp.filters.floor_qty(qty)

    leverage = _leverage(qty * inp.entry, margin_cap, leverage_max)
    mmr = _d(inp.mmr_assumed)
    mult = _d(liq_distance_mult)
    if liquidation_ratio(inp.entry, stop_distance, inp.side, leverage, mmr) < mult:
        safe = [
            lev
            for lev in range(leverage, 0, -1)
            if liquidation_ratio(inp.entry, stop_distance, inp.side, lev, mmr) >= mult
        ]
        if not safe:
            raise SizingRejectedError(
                "liquidation_too_close", "no leverage keeps liquidation beyond the stop multiple", breakdown
            )
        leverage = safe[0]
        max_qty = inp.filters.floor_qty(margin_cap * leverage / inp.entry)
        if qty > max_qty:
            qty = max_qty
            binding = "liquidation distance"

    notional = qty * inp.entry
    margin = notional / leverage
    liq = liquidation_price(inp.entry, inp.side, leverage, mmr)
    breakdown.update(
        {
            "qty_raw": float(qty_raw),
            "qty": float(qty),
            "notional": float(notional),
            "leverage": leverage,
            "margin": float(margin),
            "liquidation_price": float(liq),
            "liquidation_distance_ratio": float(abs(inp.entry - liq) / stop_distance),
            "risk_usd_actual": float(qty * stop_distance),
            "cap_binding": binding,
        }
    )
    problem = inp.filters.qty_problem(qty, inp.entry)
    if problem is not None:
        raise SizingRejectedError("too_small", problem, breakdown)
    if margin > margin_cap:  # pragma: no cover - guaranteed by the leverage rule
        raise SizingRejectedError(
            "ceiling", f"margin {margin} above {max_margin_pct:.2%} of equity", breakdown
        )
    return SizingResult(
        qty=qty,
        notional=notional,
        leverage=leverage,
        margin=margin,
        risk_usd=risk_usd,
        risk_usd_actual=qty * stop_distance,
        stop_distance=stop_distance,
        liquidation_price=liq,
        binding=binding,
        breakdown=breakdown,
    )


def _leverage(notional: Decimal, margin_cap: Decimal, leverage_max: int) -> int:
    if notional <= 0 or margin_cap <= 0:
        return 1
    needed = (notional / margin_cap).to_integral_value(rounding=ROUND_CEILING)
    return max(1, min(leverage_max, int(needed)))
