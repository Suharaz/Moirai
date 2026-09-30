"""`test_liquidation_semantics` (phase 03 success criterion), end to end on the synthetic lake.

The coins and the planted liquidation lists are documented in `tests/fixtures/synthetic_lake.py`; every
expected number below follows from its constants, not from the code under test.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from fixtures.synthetic_lake import (
    A_ERR,
    A_INFLIGHT,
    A_MISS,
    ABSN,
    ABSN_STALE_LIQ,
    BRST,
    BRST_LIQ,
    LIQ_FLOOR_USD,
    MAIN_COINS,
    QUIE,
    XTRG,
    ZERO,
    ZERO_LIQ,
    A,
    Coin,
    SyntheticLake,
    floor_of,
    quiet_liq,
)
from hdt.contracts.common import DataQualityFlag
from hdt.core.config import scanner_config, static_config
from hdt.features.engine import FeatureEngine, Snapshot
from hdt.lake.universe import UniverseMember, load_universe
from hdt.quant.scanner import Evaluation, evaluate_rules

LIQ_NAMES = tuple(f"liq_{side}_{w}_usd" for side in ("total", "long", "short") for w in ("1h", "4h", "24h"))


def _snap(lake: SyntheticLake, features: FeatureEngine, as_of: datetime) -> Snapshot:
    return features.snapshot(as_of, load_universe(lake.pit, as_of), static_config().cmc_routes)


def _member(snap: Snapshot, coin: Coin) -> UniverseMember:
    member = snap.member(coin.cmc_id)
    assert member is not None
    return member


def _ltx(snap: Snapshot, coin: Coin) -> Evaluation:
    return next(
        ev
        for ev in evaluate_rules(snap, scanner_config())
        if ev.coin_id == coin.cmc_id and ev.rule.value == "LTX"
    )


def test_coin_absent_from_the_complete_list_is_zero_not_the_older_record(
    main_lake: SyntheticLake, main_features: FeatureEngine
) -> None:
    before = _snap(main_lake, main_features, A - timedelta(minutes=5))
    block = before.crowding(_member(before, ABSN))
    assert block.values["liq_total_4h_usd"] == pytest.approx(ABSN_STALE_LIQ.h4)

    snap = _snap(main_lake, main_features, A)
    assert snap.crowding_inputs.liq.status == "complete"
    block = snap.crowding(_member(snap, ABSN))
    assert {name: block.values[name] for name in LIQ_NAMES} == dict.fromkeys(LIQ_NAMES, 0.0)
    assert block.values["spike"] is None
    assert block.values["skew_4h"] is None
    assert DataQualityFlag.MISSING_ROUTE not in block.data_quality()
    # Present coins of the same cycle keep their own values: the list itself was read.
    assert snap.crowding(_member(snap, ZERO)).values["liq_total_4h_usd"] == pytest.approx(ZERO_LIQ.h4)


def test_missing_page_is_missing_route_and_no_older_cycle_is_used(
    main_lake: SyntheticLake, main_features: FeatureEngine
) -> None:
    # Page 2 of the 13:57:00 cycle never arrives. While that cycle is younger than the cycle window,
    # the previous complete cycle (13:55:20) answers; past the window nothing does.
    inflight = _snap(main_lake, main_features, A_INFLIGHT)
    assert inflight.crowding_inputs.liq.status == "complete"
    assert inflight.crowding_inputs.liq.fetched_at == A_INFLIGHT - timedelta(minutes=2, seconds=10)

    snap = _snap(main_lake, main_features, A_MISS)
    assert snap.crowding_inputs.liq.status == "incomplete"
    for coin in MAIN_COINS:
        if coin.symbol == "BTC":
            continue
        block = snap.crowding(_member(snap, coin))
        assert all(block.values[name] is None for name in LIQ_NAMES)
        assert block.values["spike"] is None
        assert set(LIQ_NAMES) <= set(block.data_quality()[DataQualityFlag.MISSING_ROUTE])
        ltx = _ltx(snap, coin)
        assert ltx.side is None
        assert ltx.contagion_blocked is None
        assert ltx.emit_class is None
    assert snap.cross.breadth.long is None
    assert snap.cross.breadth.short is None


def test_error_response_is_missing_route(main_lake: SyntheticLake, main_features: FeatureEngine) -> None:
    snap = _snap(main_lake, main_features, A_ERR)
    assert snap.crowding_inputs.liq.status == "error"
    block = snap.crowding(_member(snap, XTRG))
    assert all(block.values[name] is None for name in LIQ_NAMES)
    assert set(LIQ_NAMES) <= set(block.data_quality()[DataQualityFlag.MISSING_ROUTE])


def test_zero_spike_base_uses_the_floor_not_infinity(
    main_lake: SyntheticLake, main_features: FeatureEngine
) -> None:
    snap = _snap(main_lake, main_features, A)
    block = snap.crowding(_member(snap, ZERO))
    assert ZERO_LIQ.h24 - ZERO_LIQ.h4 == 0.0
    floor_c = floor_of(ZERO)
    assert floor_c > LIQ_FLOOR_USD  # the 30-day q20 of the coin's own hourly liquidations
    assert block.values["spike_floor_usd"] == pytest.approx(floor_c)
    assert block.values["spike_den_usd"] == pytest.approx(floor_c)
    assert block.values["spike"] == pytest.approx(((ZERO_LIQ.h4 - ZERO_LIQ.h1) / 3) / floor_c)
    assert block.values["decay"] == pytest.approx(ZERO_LIQ.h1 / ((ZERO_LIQ.h4 - ZERO_LIQ.h1) / 3))
    assert "spike" in block.data_quality()[DataQualityFlag.SPIKE_FLOOR_APPLIED]


def test_spike_floor_is_the_config_floor_for_a_quiet_coin(
    main_lake: SyntheticLake, main_features: FeatureEngine
) -> None:
    snap = _snap(main_lake, main_features, A)
    assert quiet_liq(QUIE).h1 < LIQ_FLOOR_USD
    assert snap.crowding(_member(snap, QUIE)).values["spike_floor_usd"] == pytest.approx(LIQ_FLOOR_USD)


def test_burst_below_min_notional_does_not_trigger_ltx(
    main_lake: SyntheticLake, main_features: FeatureEngine
) -> None:
    params = scanner_config().spike
    assert BRST_LIQ.h4 - BRST_LIQ.h1 < params.min_liq_notional_usd
    snap = _snap(main_lake, main_features, A)
    block = snap.crowding(_member(snap, BRST))
    assert block.values["spike_valid"] is False
    assert block.values["spike"] is None
    ltx = _ltx(snap, BRST)
    # Every other strict condition holds, so the notional threshold alone keeps the coin out.
    failing = {name for name, ok in ltx.conditions["strict"].items() if ok is not True}
    assert failing == {"spike"}
    assert not ltx.strict_pass
    assert not ltx.loose_pass
    assert ltx.emit_class is None
    # The planted trigger with a notional above the threshold does fire in the same cycle.
    assert _ltx(snap, XTRG).emit_class == "strict"
