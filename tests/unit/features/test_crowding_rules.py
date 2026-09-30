"""Liquidation, funding and LTX rule semantics (Design Contract sections 2 and 6)."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from hdt.core.config import scanner_config, static_config
from hdt.features.funding import FundingState, PremiumRow, funding_drop, resolve_interval
from hdt.features.liquidations import CoinLiq, LiqSnapshot, skew, spike_decay
from hdt.features.ltx import Breadth, LtxInputs, contagion_blocked, evaluate

AT = datetime(2026, 9, 1, 12, tzinfo=UTC)


def _liq(total_1h: float, total_4h: float, total_24h: float, long_share: float = 0.8) -> CoinLiq:
    return CoinLiq(
        total_1h,
        total_4h,
        total_24h,
        total_1h * long_share,
        total_4h * long_share,
        total_24h * long_share,
        total_1h * (1 - long_share),
        total_4h * (1 - long_share),
        total_24h * (1 - long_share),
        AT,
    )


def test_coin_absent_from_complete_list_is_zero_but_missing_list_is_unknown() -> None:
    complete = LiqSnapshot("complete", {1: _liq(1.0, 4.0, 24.0)}, AT, "c1")
    absent = complete.coin(999)
    assert absent is not None
    assert absent.total_4h == 0.0
    assert skew(absent.long_4h, absent.total_4h) is None
    assert LiqSnapshot("incomplete", {}, AT, "c2").coin(999) is None
    assert LiqSnapshot("error", {}, AT, "c3").coin(1) is None


def test_spike_and_decay_match_the_contract_formula() -> None:
    params = scanner_config().spike
    body = 3 * max(params.min_liq_notional_usd, params.liq_floor_usd) * 10
    liq = _liq(total_1h=body / 6, total_4h=body / 6 + body, total_24h=body / 6 + body + 20 * body / 3 / 8)
    oi = body / params.min_liq_oi_frac / 2  # the 4 h body is twice the OI validity threshold
    result = spike_decay(liq, oi, params.liq_floor_usd, params)
    base = (liq.total_24h - liq.total_4h) / 20
    assert result.valid
    assert result.den == pytest.approx(max(base, params.liq_floor_usd))
    assert result.spike == pytest.approx((body / 3) / result.den)
    assert result.decay == pytest.approx(liq.total_1h / (body / 3))


def test_spike_is_null_below_the_notional_or_oi_threshold_and_floor_guards_zero_base() -> None:
    params = scanner_config().spike
    small = params.min_liq_notional_usd / 2
    tiny = spike_decay(_liq(0.0, small, small), 1e12, params.liq_floor_usd, params)
    assert tiny.spike is None
    assert not tiny.valid
    body = params.min_liq_notional_usd * 3
    no_oi = spike_decay(_liq(0.0, body, body), None, params.liq_floor_usd, params)
    assert no_oi.spike is None
    # L24h - L4h = 0: the denominator is the floor, never zero.
    flat = spike_decay(_liq(0.0, body, body), body / params.min_liq_oi_frac / 2, params.liq_floor_usd, params)
    assert flat.floor_applied
    assert flat.spike == pytest.approx((body / 3) / params.liq_floor_usd)


def _row(symbol: str, rate: float) -> PremiumRow:
    return PremiumRow(symbol, 100.0, 100.0, rate, None, int(AT.timestamp() * 1000))


def test_interval_change_8h_to_4h_at_same_hourly_cost_is_not_a_funding_drop() -> None:
    before = FundingState({"XUSDT": _row("XUSDT", 0.0008)}, {"XUSDT": 8.0}, {})
    now = FundingState({"XUSDT": _row("XUSDT", 0.0004)}, {"XUSDT": 4.0}, {})
    f_before, _ = before.per_hour("XUSDT")
    f_now, _ = now.per_hour("XUSDT")
    assert f_before == pytest.approx(f_now)
    min_abs = static_config().indicators.funding.min_abs_per_h
    drop = funding_drop(f_now, f_before, min_abs)
    assert drop == pytest.approx(0.0)
    assert drop < scanner_config().ltx.loose.funding_drop_min


def test_inferred_interval_wins_over_funding_info_and_is_flagged() -> None:
    changed = resolve_interval(listed=8.0, inferred=4.0)
    assert changed.hours == 4.0
    assert changed.changed
    assert resolve_interval(listed=None, inferred=None).hours is None
    assert not resolve_interval(listed=8.0, inferred=None).changed


def test_symbol_absent_from_a_recorded_funding_info_list_runs_at_the_default_interval() -> None:
    default = static_config().indicators.funding.default_interval_h
    hour_ms = 3_600_000
    listed_other = FundingState({"XUSDT": _row("XUSDT", 0.0008)}, {"YUSDT": 4.0}, {}, default)
    per_h, resolution = listed_other.per_hour("XUSDT")
    assert resolution.hours == default
    assert not resolution.changed
    assert per_h == pytest.approx(0.0008 / default)
    # A disagreeing interval inferred from nextFundingTime still wins and is flagged.
    inferred = FundingState({}, {"YUSDT": 4.0}, {"XUSDT": [0, 4 * hour_ms]}, default).interval("XUSDT")
    assert inferred.hours == 4.0
    assert inferred.changed
    # Without any recorded fundingInfo list nothing is listed, so nothing is assumed.
    assert FundingState({}, None, {}, default).interval("XUSDT").hours is None


def _passing(side: str) -> LtxInputs:
    t = scanner_config().ltx.strict
    skew4 = t.skew4_min if side == "LONG" else 1 - t.skew4_min
    drop = t.funding_drop_min if side == "LONG" else -t.funding_drop_min
    return LtxInputs(skew4, t.spike_min, t.decay_max, t.fdz_min, t.doi4_max, drop)


@pytest.mark.parametrize("side", ["LONG", "SHORT"])
def test_ltx_strict_passes_at_the_thresholds_and_is_symmetric(side: str) -> None:
    result = evaluate(_passing(side), scanner_config().ltx)
    assert result.side == side
    assert result.strict_pass
    assert result.loose_pass


@pytest.mark.parametrize("missing", ["spike", "decay", "fdz", "doi4", "funding_drop"])
def test_a_missing_ltx_input_fails_the_rule(missing: str) -> None:
    values = _passing("LONG").values()
    values[missing] = None
    result = evaluate(LtxInputs(**values), scanner_config().ltx)
    assert result.strict[missing] is None
    assert not result.strict_pass
    assert not result.loose_pass


def test_contagion_blocks_only_while_btc_has_not_flushed() -> None:
    params = scanner_config().contagion
    wide = Breadth(params.b_block, 0.0, 20)
    assert contagion_blocked("LONG", wide, flushed=False, params=params) is True
    assert contagion_blocked("LONG", wide, flushed=True, params=params) is False
    assert contagion_blocked("SHORT", wide, flushed=False, params=params) is False
    assert contagion_blocked("LONG", Breadth(None, None, 0), flushed=False, params=params) is None
