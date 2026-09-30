"""Deterministic sizing (phase 09 section 2): conf, risk budget, lot rounding, margin, leverage, liquidation.

Success criterion `test_sizing`: SHORT p = 0.35 gives conf 0.75 and a size > 0; p on the wrong side of
`side` is rejected; every sample keeps margin <= max_margin_pct x equity and the estimated liquidation
farther from the entry than the stop by >= 1.5x the stop distance.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest
from hypothesis import assume, given, settings
from hypothesis import strategies as st

from hdt.contracts.common import Side
from hdt.execution.filters import SymbolFilters
from hdt.risk.sizing import (
    SizingInput,
    SizingRejectedError,
    confidence,
    liquidation_price,
    p_side_of,
    size_position,
)
from risk_builders import risk_file, symbol_filters

RISK = risk_file()
LIQ_MULT = Decimal("1.5")
MAX_MARGIN = Decimal(repr(RISK.max_margin_pct))


def _inp(**overrides: Any) -> SizingInput:
    side = overrides.get("side", Side.LONG)
    values: dict[str, Any] = {
        "equity": Decimal("10000"),
        "equity_age_s": 3.0,
        "side": side,
        "p": 0.65 if side is Side.LONG else 0.35,
        "manager_size": 1.0,
        "flags_size_mult": 1.0,
        "flags_age_s": 30.0,
        "mode": "paper",
        "mode_size_multiplier": 1.0,
        "entry": Decimal("100.00"),
        "stop": Decimal("98.00") if side is Side.LONG else Decimal("102.00"),
        "filters": symbol_filters(),
        "base_asset": "SOL",
        "open_interest_usd": 5e8,
        "quote_volume_1h_usd": 2e8,
        "risk_pct": RISK.risk_pct,
        "leverage_max": RISK.leverage_max,
        "max_margin_pct": RISK.max_margin_pct,
        "max_oi_frac": RISK.max_oi_frac,
        "max_volume_1h_frac": RISK.max_volume_1h_frac,
        "mmr_assumed": RISK.mmr_assumed,
        "liq_distance_mult": RISK.liq_distance_mult,
    }
    values.update(overrides)
    return SizingInput(**values)


def test_short_p_035_gives_conf_075_and_a_positive_size() -> None:
    p_side = p_side_of(0.35, Side.SHORT)
    assert p_side == pytest.approx(0.65)
    assert confidence(p_side) == pytest.approx(0.75)

    res = size_position(_inp(side=Side.SHORT, p=0.35))

    assert res.breakdown["conf"] == pytest.approx(0.75)
    assert res.risk_usd == pytest.approx(Decimal("37.5"))  # 10 000 x 0.5% x 0.75
    assert res.qty > 0
    assert res.liquidation_price > Decimal("102.00")  # a SHORT liquidates above its stop


def test_long_and_short_mirror_images_size_identically() -> None:
    long = size_position(_inp(side=Side.LONG, p=0.65, stop=Decimal("99.00")))
    short = size_position(_inp(side=Side.SHORT, p=0.35, stop=Decimal("101.00")))
    assert long.qty == short.qty > 0
    assert long.leverage == short.leverage


@pytest.mark.parametrize(
    ("side", "p"),
    [(Side.LONG, 0.40), (Side.LONG, 0.50), (Side.SHORT, 0.60), (Side.SHORT, 0.50)],
)
def test_p_on_the_wrong_side_of_the_decision_is_rejected(side: Side, p: float) -> None:
    with pytest.raises(SizingRejectedError) as err:
        size_position(_inp(side=side, p=p))
    assert err.value.reason == "sign_mismatch"


def test_quantity_is_rounded_down_to_the_lot_never_up() -> None:
    # risk 10 000 x 0.5% x 1.0 = 50 USD over a 7.00 stop -> 7.142857 units -> 7.142 (step 0.001)
    res = size_position(_inp(p=0.9, stop=Decimal("93.00")))
    assert res.qty == Decimal("7.142")
    assert res.risk_usd_actual <= res.risk_usd


def test_notional_below_min_notional_is_rejected_not_rounded_up() -> None:
    tiny = symbol_filters(step="1", min_qty="1", min_notional="5")
    # risk 100 x 0.5% x 0.75 = 0.375 USD over 2.00 -> 0.1875 units -> floors to 0
    with pytest.raises(SizingRejectedError) as err:
        size_position(_inp(equity=Decimal("100"), filters=tiny))
    assert err.value.reason == "too_small"


def test_margin_cap_binds_and_sets_leverage() -> None:
    # 37.5 USD / 2.00 = 18.75 units = 1875 USD notional, capped at 3% x 10 000 x 3 = 900 USD
    res = size_position(_inp())
    assert res.binding == "margin cap"
    assert res.notional == Decimal("900.000")
    assert res.leverage == 3
    assert res.margin == Decimal("300")


def test_open_interest_and_volume_caps_bind_when_smaller() -> None:
    res = size_position(_inp(open_interest_usd=40_000.0))  # 0.5% of OI = 200 USD
    assert res.binding == "open interest cap"
    assert res.notional <= Decimal("200")
    res = size_position(_inp(quote_volume_1h_usd=5_000.0))  # 2% of 1 h volume = 100 USD
    assert res.binding == "volume cap"
    assert res.notional <= Decimal("100")


def test_live_mode_multiplier_quarters_the_risk() -> None:
    paper = size_position(_inp(p=0.6, stop=Decimal("90.00")))
    live = size_position(_inp(p=0.6, stop=Decimal("90.00"), mode="live", mode_size_multiplier=0.25))
    assert live.risk_usd == paper.risk_usd * Decimal("0.25")
    assert live.qty < paper.qty


def test_risk_pct_above_the_hard_ceiling_is_refused() -> None:
    with pytest.raises(SizingRejectedError) as err:
        size_position(_inp(risk_pct=0.02))
    assert err.value.reason == "ceiling"


def test_leverage_max_from_config_can_only_tighten() -> None:
    assert size_position(_inp(leverage_max=10)).leverage <= 3
    assert size_position(_inp(leverage_max=1)).leverage == 1


def test_notional_caps_from_config_can_only_tighten() -> None:
    # Values looser than the hard ceilings (config that bypassed the schema) size like the ceilings.
    cases = [
        ({}, {"max_margin_pct": 0.5}),  # margin 3% at 3x binds at 9.000
        ({"open_interest_usd": 1e5}, {"max_oi_frac": 0.5}),  # 0.5% of OI = 500 USD
        ({"quote_volume_1h_usd": 2e4}, {"max_volume_1h_frac": 0.5}),  # 2% of 1 h volume = 400 USD
        # mmr 29%: at 3x the liquidation is 1.22x the 5.00 stop distance, allowed only by a 1.0 multiple
        ({"p": 0.9, "stop": Decimal("95.00"), "mmr_assumed": 0.29}, {"liq_distance_mult": 1.0}),
        ({"mode": "live", "p": 0.6, "stop": Decimal("90.00")}, {"mode_size_multiplier": 2.0}),
    ]
    for base, loose in cases:
        strict = size_position(_inp(**base))
        clipped = size_position(_inp(**base, **loose))
        assert (clipped.qty, clipped.leverage) == (strict.qty, strict.leverage), loose


def test_close_liquidation_lowers_leverage_then_quantity() -> None:
    # mmr 30%: at 3x a LONG liquidates at 100 x (2/3) / 0.7 = 95.24, only 0.95x the 5.00 stop distance
    res = size_position(_inp(p=0.9, stop=Decimal("95.00"), mmr_assumed=0.3))
    assert res.leverage == 2
    assert res.binding == "liquidation distance"
    assert res.margin <= Decimal("300")
    assert abs(res.liquidation_price - Decimal("100")) >= LIQ_MULT * Decimal("5")


def test_no_leverage_keeps_liquidation_beyond_the_stop_multiple() -> None:
    # a LONG at 1x never liquidates above 0: a 70.00 stop distance is still only 1.43x away from it
    with pytest.raises(SizingRejectedError) as err:
        size_position(_inp(p=0.9, stop=Decimal("30.00"), mmr_assumed=0.3))
    assert err.value.reason == "liquidation_too_close"


def test_levels_on_the_wrong_side_are_rejected() -> None:
    with pytest.raises(SizingRejectedError) as err:
        size_position(_inp(side=Side.LONG, stop=Decimal("101.00")))
    assert err.value.reason == "bad_levels"


def _stop(entry: Decimal, frac: float, side: Side, filters: SymbolFilters) -> Decimal:
    distance = entry * Decimal(repr(frac))
    return (
        filters.floor_price(entry - distance) if side is Side.LONG else filters.ceil_price(entry + distance)
    )


@settings(max_examples=300, deadline=None)
@given(
    side=st.sampled_from(list(Side)),
    p_side=st.floats(min_value=0.5, max_value=0.99, exclude_min=True),
    equity=st.decimals(min_value=Decimal("50"), max_value=Decimal("2000000"), places=2),
    entry=st.decimals(min_value=Decimal("0.50"), max_value=Decimal("90000"), places=2),
    stop_frac=st.floats(min_value=0.001, max_value=0.4),
    manager_size=st.floats(min_value=0.05, max_value=1.0),
    flags_mult=st.floats(min_value=0.05, max_value=1.0),
    mode_mult=st.sampled_from([1.0, 0.25]),
    mmr=st.floats(min_value=0.004, max_value=0.3),
    oi=st.floats(min_value=1e4, max_value=1e10),
    vol=st.floats(min_value=1e4, max_value=1e10),
)
def test_every_sample_respects_margin_leverage_lot_and_liquidation(
    side: Side,
    p_side: float,
    equity: Decimal,
    entry: Decimal,
    stop_frac: float,
    manager_size: float,
    flags_mult: float,
    mode_mult: float,
    mmr: float,
    oi: float,
    vol: float,
) -> None:
    filters = symbol_filters()
    stop = _stop(entry, stop_frac, side, filters)
    assume(stop > 0 and stop != entry)
    inp = _inp(
        side=side,
        p=p_side if side is Side.LONG else 1.0 - p_side,
        equity=equity,
        entry=entry,
        stop=stop,
        manager_size=manager_size,
        flags_size_mult=flags_mult,
        mode_size_multiplier=mode_mult,
        mmr_assumed=mmr,
        open_interest_usd=oi,
        quote_volume_1h_usd=vol,
    )
    refusal: str | None = None
    try:
        res = size_position(inp)
    except SizingRejectedError as exc:
        refusal = exc.reason
    if refusal is not None:
        # The only legitimate refusals for a correctly signed decision with valid levels.
        assert refusal in {"too_small", "liquidation_too_close"}
        return
    stop_distance = abs(entry - stop)
    assert res.qty > 0
    assert filters.qty_aligned(res.qty)
    assert res.qty * entry >= filters.min_notional
    assert res.risk_usd_actual <= res.risk_usd * (1 + Decimal("1e-20"))  # rounded down, never above budget
    assert 1 <= res.leverage <= 3
    assert res.margin <= equity * MAX_MARGIN
    caps = min(
        equity * MAX_MARGIN * 3,
        Decimal(repr(RISK.max_oi_frac)) * Decimal(repr(oi)),
        Decimal(repr(RISK.max_volume_1h_frac)) * Decimal(repr(vol)),
    )
    assert res.notional <= caps * (1 + Decimal("1e-20"))
    liq = liquidation_price(entry, side, res.leverage, Decimal(repr(mmr)))
    assert liq == res.liquidation_price
    assert abs(entry - liq) >= LIQ_MULT * stop_distance
    # the stop sits between the entry and the liquidation price
    assert (liq < stop < entry) if side is Side.LONG else (entry < stop < liq)


@settings(max_examples=200, deadline=None)
@given(
    p_side=st.floats(min_value=0.55, max_value=0.99),
    equity=st.decimals(min_value=Decimal("10000"), max_value=Decimal("1000000"), places=2),
    entry=st.decimals(min_value=Decimal("1.00"), max_value=Decimal("1000"), places=2),
    stop_frac=st.floats(min_value=0.005, max_value=0.1),
    manager_size=st.floats(min_value=0.5, max_value=1.0),
    flags_mult=st.floats(min_value=0.5, max_value=1.0),
)
def test_both_sides_get_a_positive_mirrored_size_when_p_side_is_above_half(
    p_side: float,
    equity: Decimal,
    entry: Decimal,
    stop_frac: float,
    manager_size: float,
    flags_mult: float,
) -> None:
    filters = symbol_filters()
    distance = filters.floor_price(entry * Decimal(repr(stop_frac)))
    assume(distance > 0)
    common = {
        "equity": equity,
        "entry": entry,
        "manager_size": manager_size,
        "flags_size_mult": flags_mult,
    }
    long = size_position(_inp(side=Side.LONG, p=p_side, stop=entry - distance, **common))
    short = size_position(_inp(side=Side.SHORT, p=1.0 - p_side, stop=entry + distance, **common))
    assert long.qty > 0
    assert short.qty > 0
    assert long.breakdown["conf"] == pytest.approx(short.breakdown["conf"])
    assert long.qty == short.qty
