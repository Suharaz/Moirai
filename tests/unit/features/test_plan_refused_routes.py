"""CMC routes the plan refuses (1006: #7 OHLCV, #10 price performance, #15 new listings) leave their features
missing, never invented: the values are None with a `missing_route` flag (not `stale`, not CMC-degraded), an
agent whose model needs them abstains, and every other feature is computed as usual."""

from __future__ import annotations

import pytest

from fixtures.synthetic_lake import ETH, FAR_FUTURE, MACRO, A, build_main
from hdt.agents.runner import BLOCKING_FLAGS, main_features
from hdt.contracts.common import AgentName, DataQualityFlag, TargetType
from hdt.core.config import scanner_config, static_config
from hdt.features.engine import FeatureEngine, Snapshot
from hdt.features.lake_io import LakeView
from hdt.lake.universe import UniverseMember, load_universe

REFUSED = frozenset({"ohlcv_historical", "price_performance_stats", "listings_new"})
PERFORMANCE = ("ath_distance", "atl_distance", "perf_7d", "perf_30d", "perf_90d", "perf_365d")


@pytest.fixture(scope="module")
def snap(tmp_path_factory: pytest.TempPathFactory) -> Snapshot:
    lake = build_main(tmp_path_factory.mktemp("plan_refused_lake"), refused=REFUSED)
    features = FeatureEngine(
        LakeView(lake.pit, clock=lambda: FAR_FUTURE), static_config(), scanner_config(), max_snapshots=2
    )
    return features.snapshot(A, load_universe(lake.pit, A), static_config().cmc_routes)


def _eth(snap: Snapshot) -> UniverseMember:
    member = snap.member(ETH.cmc_id)
    assert member is not None
    return member


def _missing(quality: dict[DataQualityFlag, tuple[str, ...]]) -> set[str]:
    return set(quality.get(DataQualityFlag.MISSING_ROUTE, ()))


def test_technical_cmc_timeframes_and_performance_are_missing_and_the_agent_abstains(snap: Snapshot) -> None:
    block = snap.agent_block(AgentName.TECHNICAL, _eth(snap))
    quality = block.data_quality()
    cmc_timeframes = {name for name in block.values if name.split("_", 1)[0] in ("1h", "4h", "1d")}
    assert cmc_timeframes
    assert {name: block.values[name] for name in cmc_timeframes | set(PERFORMANCE)} == dict.fromkeys(
        cmc_timeframes | set(PERFORMANCE)
    )
    assert cmc_timeframes | set(PERFORMANCE) <= _missing(quality)
    assert DataQualityFlag.STALE not in quality
    assert not snap.cmc_degraded
    assert block.values["15m_rsi14_last"] is not None  # Binance klines are unaffected
    # The existing missing-data convention: a blocking flag on a p_model input forces the agent to abstain.
    model = main_features(AgentName.TECHNICAL, TargetType.RAW_12H.value)
    assert model & _missing(quality)
    assert DataQualityFlag.MISSING_ROUTE in BLOCKING_FLAGS


def test_macro_regime_is_missing_and_market_context_is_kept(snap: Snapshot) -> None:
    block = snap.agent_block(AgentName.MACRO, _eth(snap))
    assert block.values["regime"] is None
    assert {"regime", "regime_trend", "regime_high_vol"} <= _missing(block.data_quality())
    assert block.values["fear_greed"] == pytest.approx(MACRO["fear_greed"])


def test_fundamental_new_listing_is_unknown_not_false(snap: Snapshot) -> None:
    block = snap.agent_block(AgentName.FUNDAMENTAL, _eth(snap))
    assert block.values["is_new_listing"] is None
    missing = _missing(block.data_quality())
    assert "is_new_listing" in missing
    assert block.values["oi_mcap"] is not None
    # The fundamental model does not use the listing flag: it keeps forecasting.
    assert not main_features(AgentName.FUNDAMENTAL, TargetType.RAW_12H.value) & missing
