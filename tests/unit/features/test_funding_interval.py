"""`test_funding_interval` (phase 03 success criterion), end to end on the synthetic lake.

FEIG and FLST pay 0.0008 per 8 h until `FUNDING_CHANGE`, then 0.0004 per 4 h: the per-period rate halves
while the hourly cost stays 0.0001. FLST's `fundingInfo` already lists 4 h; FEIG's still lists the default
8 h, so only the `nextFundingTime` grid shows the change. XTRG is the control: a real drop from 0.0008 to
0.0003 per 8 h.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from fixtures.synthetic_lake import FEIG, FLST, FUNDING_CHANGE, NVEN, NVEN_VENUE, XTRG, A, Coin, SyntheticLake
from hdt.contracts.common import DataQualityFlag
from hdt.core.config import scanner_config, static_config
from hdt.features.engine import FeatureEngine, Snapshot
from hdt.lake.universe import UniverseMember, load_universe
from hdt.quant.scanner import evaluate_rules

HOURLY_COST = 0.0001


def _member(snap: Snapshot, coin: Coin) -> UniverseMember:
    member = snap.member(coin.cmc_id)
    assert member is not None
    return member


@pytest.fixture(scope="module")
def snap(main_lake: SyntheticLake, main_features: FeatureEngine) -> Snapshot:
    return main_features.snapshot(A, load_universe(main_lake.pit, A), static_config().cmc_routes)


@pytest.mark.parametrize("coin", [FEIG, FLST], ids=lambda c: c.symbol)
def test_interval_change_at_the_same_hourly_cost_is_not_a_funding_drop(snap: Snapshot, coin: Coin) -> None:
    lookback = timedelta(hours=snap.static.indicators.funding.lookback_h)
    assert A - lookback < FUNDING_CHANGE < A  # "before" reads the old 8 h regime, "now" the new 4 h one
    block = snap.crowding(_member(snap, coin))
    assert block.values["funding_interval_h"] == 4.0
    assert block.values["f_c_per_h"] == pytest.approx(HOURLY_COST)
    assert block.values["f_c_per_h_before"] == pytest.approx(HOURLY_COST)
    assert block.values["df_c_per_h"] == pytest.approx(0.0, abs=1e-12)
    assert block.values["funding_drop"] == pytest.approx(0.0, abs=1e-9)

    strict = scanner_config().ltx.strict
    ltx = next(
        ev
        for ev in evaluate_rules(snap, scanner_config())
        if ev.coin_id == coin.cmc_id and ev.rule.value == "LTX"
    )
    assert ltx.conditions["strict"]["funding_drop"] is False
    # Read per period, the same data would pass the ">= 50 %" drop condition.
    now = snap.funding_now.rows[coin.binance].rate
    before = snap.funding_before.rows[coin.binance].rate
    assert now is not None
    assert before is not None
    assert (before - now) / before >= strict.funding_drop_min


def test_inferred_interval_wins_over_a_stale_listed_interval(snap: Snapshot) -> None:
    feig = snap.funding_now.interval(FEIG.binance)
    assert (feig.listed, feig.inferred, feig.hours, feig.changed) == (8.0, 4.0, 4.0, True)
    assert (
        "funding_interval_h"
        in snap.crowding(_member(snap, FEIG)).data_quality()[DataQualityFlag.FUNDING_INTERVAL_CHANGED]
    )
    flst = snap.funding_now.interval(FLST.binance)
    assert (flst.listed, flst.inferred, flst.hours, flst.changed) == (4.0, 4.0, 4.0, False)
    assert DataQualityFlag.FUNDING_INTERVAL_CHANGED not in snap.crowding(_member(snap, FLST)).data_quality()


def test_real_drop_on_an_unchanged_interval_is_detected(snap: Snapshot) -> None:
    block = snap.crowding(_member(snap, XTRG))
    assert block.values["funding_interval_h"] == 8.0
    assert block.values["f_c_per_h"] == pytest.approx(0.0003 / 8)
    assert block.values["f_c_per_h_before"] == pytest.approx(0.0008 / 8)
    assert block.values["funding_drop"] == pytest.approx(0.625)


def test_venue_without_an_interval_source_is_excluded_not_guessed(snap: Snapshot) -> None:
    block = snap.crowding(_member(snap, NVEN))
    assert snap.core(_member(snap, NVEN)).excluded_venues == (NVEN_VENUE[0],)
    # F_c stays the Binance hourly rate; the venue's per-period rate is not blended in.
    assert block.values["f_c_per_h"] == pytest.approx(0.0001 / 8)
    assert "f_c_per_h" in block.data_quality()[DataQualityFlag.FUNDING_INTERVAL_UNKNOWN]
