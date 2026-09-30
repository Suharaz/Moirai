"""Per-agent feature modules and CMC vs Binance reconciliation, on the synthetic lake (phase 03 steps 3-4).

Expected values are computed by hand from the fixture constants and the closed-form market model in
`tests/fixtures/synthetic_lake.py` (the snapshot at `A` reads records fetched 2-20 s before it).
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from fixtures.synthetic_lake import (
    A_ERR,
    ATH_MULT,
    ATL_MULT,
    BTC_BY_AGE_DAYS,
    CIRCULATING,
    CORE,
    DEX_HOLDER_BALANCES,
    DEX_POOLS_USD,
    DEX_TOTAL_SUPPLY,
    ETH,
    ETH_FORCE_1H,
    ETH_FORCE_OLD,
    INDEX_DISCOUNT,
    LISTING_AGE_DAYS,
    MACRO,
    MAX_SUPPLY,
    MPEP,
    NVEN,
    PERF_PCT,
    PERIPHERAL,
    RCON,
    RCON_CMC_FUNDING,
    RCON_OI_BIAS,
    STABLE_BY_AGE_DAYS,
    TOTAL_SUPPLY,
    A,
    Coin,
    Liq,
    SyntheticLake,
    main_market,
    quiet_liq,
)
from hdt.contracts.common import AgentName, DataQualityFlag
from hdt.core.config import static_config
from hdt.features.engine import FeatureEngine, Snapshot
from hdt.lake.universe import UniverseMember, load_universe

MARKET = main_market()
QUOTES_AT = A - timedelta(seconds=20)
"""`quotes_latest`, `open_interest`, klines and the route #2 books of the snapshot were fetched here."""
MARK_AT = A - timedelta(seconds=10)
"""Newest premium index / 1 s mark event at or before `A`."""


def _snap(lake: SyntheticLake, features: FeatureEngine, as_of: datetime) -> Snapshot:
    return features.snapshot(as_of, load_universe(lake.pit, as_of), static_config().cmc_routes)


def _member(snap: Snapshot, coin: Coin) -> UniverseMember:
    member = snap.member(coin.cmc_id)
    assert member is not None
    return member


@pytest.fixture(scope="module")
def snap(main_lake: SyntheticLake, main_features: FeatureEngine) -> Snapshot:
    return _snap(main_lake, main_features, A)


# --------------------------------------------------------------------------- Technical (#10)


def test_technical_reads_route_10_performance_and_every_timeframe(snap: Snapshot) -> None:
    block = snap.agent_block(AgentName.TECHNICAL, _member(snap, ETH))
    close = MARKET.quote_price(ETH, QUOTES_AT)
    assert block.values["ath_distance"] == pytest.approx(close / (ATH_MULT * ETH.price) - 1)
    assert block.values["atl_distance"] == pytest.approx(close / (ATL_MULT * ETH.price) - 1)
    for period, pct in PERF_PCT.items():
        assert block.values[f"perf_{period}"] == pytest.approx(pct / 100)
    for tf in ("15m", "1h", "4h", "1d"):
        assert block.values[f"{tf}_rsi14_last"] is not None
        assert block.values[f"{tf}_atr_pct"] is not None
    assert DataQualityFlag.MISSING_ROUTE not in block.data_quality()


# --------------------------------------------------------------------------- Macro (#28 and context)


def test_macro_reads_route_28_reserves_over_the_cohort_exchanges(snap: Snapshot) -> None:
    assert snap.groups is not None
    assert tuple(snap.groups.all) == CORE + PERIPHERAL
    block = snap.agent_block(AgentName.MACRO, _member(snap, ETH))
    stable, btc = STABLE_BY_AGE_DAYS, BTC_BY_AGE_DAYS
    # Every exchange holds the same balances, so the cohort sum changes by the per-exchange ratio; the ETH
    # balance (which grows with age) is neither a stablecoin nor BTC and must not move either value.
    assert block.values["exch_reserve_stable_chg_1d"] == pytest.approx(stable[0] / stable[1] - 1)
    assert block.values["exch_reserve_stable_chg_7d"] == pytest.approx(stable[0] / stable[7] - 1)
    assert block.values["exch_reserve_btc_chg_1d"] == pytest.approx(btc[0] / btc[1] - 1)
    assert block.values["exch_reserve_btc_chg_7d"] == pytest.approx(btc[0] / btc[7] - 1)


def test_macro_context_and_regime(snap: Snapshot) -> None:
    block = snap.agent_block(AgentName.MACRO, _member(snap, ETH))
    total = Liq(40e6, 150e6, 600e6, 0.6)
    expected = {
        "fear_greed": MACRO["fear_greed"],
        "altcoin_index": MACRO["altcoin_index"],
        "btc_dominance": MACRO["btc_dominance"],
        "total_market_cap": MACRO["total_market_cap"],
        "altcoin_market_cap": MACRO["altcoin_market_cap"],
        "cmc20_return_24h": MACRO["cmc20_pct"] / 100,
        "cmc100_return_24h": MACRO["cmc100_pct"] / 100,
        "market_liq_1h_usd": total.h1,
        "market_liq_4h_usd": total.h4,
        "market_liq_24h_usd": total.h24,
        "market_liq_skew4": total.long_share,
    }
    assert {name: block.values[name] for name in expected} == pytest.approx(expected)
    assert block.values["regime"] in {
        "trend_high_vol",
        "trend_low_vol",
        "sideways_high_vol",
        "sideways_low_vol",
    }
    assert DataQualityFlag.MISSING_ROUTE not in block.data_quality()
    # Market-wide: every coin sees the same macro values.
    other = snap.agent_block(AgentName.MACRO, _member(snap, RCON))
    assert {k: other.values[k] for k in expected} == {k: block.values[k] for k in expected}


def test_macro_context_is_fresh_within_its_route_cadence(
    main_lake: SyntheticLake, main_features: FeatureEngine
) -> None:
    # Fear & Greed and the altcoin index are fetched every 15 min: a 10 min old record is the newest there
    # is, not stale (the 180 s market-data threshold alone made Macro abstain 12 minutes in 15).
    def stale_names(as_of: datetime) -> set[str]:
        snap = _snap(main_lake, main_features, as_of)
        block = snap.agent_block(AgentName.MACRO, _member(snap, ETH))
        return set(block.data_quality().get(DataQualityFlag.STALE, ())) & {"fear_greed", "altcoin_index"}

    assert stale_names(A + timedelta(minutes=10)) == set()
    assert stale_names(A + timedelta(minutes=20)) == {"fear_greed", "altcoin_index"}


# --------------------------------------------------------------------------- Micro


def test_micro_trade_flow_and_book(snap: Snapshot) -> None:
    block = snap.agent_block(AgentName.MICRO, _member(snap, ETH))
    # One 10 000 USD trade every 120 s before A - 2 s, every third a taker sell; windows end at the
    # minute of A (12:00): 5m holds trades 1-2, 15m trades 1-7 (3 and 6 sells), 1h trades 1-30.
    flows = {"5m": (2, 0), "15m": (5, 2), "1h": (20, 10)}
    oi_usd = MARKET.venue_oi(ETH, "binance", QUOTES_AT)
    for window, (buys, sells) in flows.items():
        assert block.values[f"cvd_{window}_usd"] == pytest.approx((buys - sells) * 10_000.0)
        assert block.values[f"taker_buy_ratio_{window}"] == pytest.approx(buys / (buys + sells))
        assert block.values[f"cvd_{window}_oi"] == pytest.approx((buys - sells) * 10_000.0 / oi_usd, rel=1e-3)
    # 20 levels of 5 000 USD each side at 2 bp steps; walls of 8x at bid level 8 and ask level 13.
    assert block.values["book_imbalance_5"] == pytest.approx(0.0, abs=1e-9)
    assert block.values["book_imbalance_10"] == pytest.approx((85_000 - 50_000) / (85_000 + 50_000))
    assert block.values["spread_bp"] == pytest.approx(4.0)
    assert block.values["depth_bid_usd"] == pytest.approx(19 * 5_000 + 40_000)
    assert block.values["depth_ask_usd"] == pytest.approx(19 * 5_000 + 40_000)
    assert DataQualityFlag.MISSING_ROUTE not in block.data_quality()


# --------------------------------------------------------------------------- Fundamental


def test_fundamental_supply_listing_and_dex(snap: Snapshot) -> None:
    block = snap.agent_block(AgentName.FUNDAMENTAL, _member(snap, ETH))
    price = MARKET.quote_price(ETH, QUOTES_AT)
    oi_agg = sum(MARKET.venue_oi(ETH, slug, QUOTES_AT) for slug in CORE + PERIPHERAL)
    top10 = sum(sorted(DEX_HOLDER_BALANCES, reverse=True)[:10]) / DEX_TOTAL_SUPPLY
    expected = {
        "circ_total_supply": CIRCULATING / TOTAL_SUPPLY,
        "circ_max_supply": CIRCULATING / MAX_SUPPLY,
        "listing_age_days": LISTING_AGE_DAYS + 20 / 86_400,
        "oi_mcap": oi_agg / (price * CIRCULATING),
        "oi_volume_turnover": oi_agg / (20.0 * ETH.oi_usd),
        "dex_liquidity_usd": sum(DEX_POOLS_USD),
        "holder_top10_share": top10,
        "sec_risk_hits": 2,
        "sec_buy_tax": 0.01,
        "sec_sell_tax": 0.02,
        "sec_honeypot": 0.0,
    }
    assert {name: block.values[name] for name in expected} == pytest.approx(expected)
    assert block.values["sec_vendor_flagged"] is False
    assert block.values["is_new_listing"] is False
    assert snap.agent_block(AgentName.FUNDAMENTAL, _member(snap, NVEN)).values["is_new_listing"] is True
    assert DataQualityFlag.MISSING_ROUTE not in block.data_quality()


# --------------------------------------------------------------------------- reconciliation


def _price_gap(coin: Coin) -> float:
    return MARKET.quote_price(coin, QUOTES_AT) * coin.multiplier / MARKET.mark(coin, MARK_AT) - 1


def test_reconcile_agrees_for_a_consistent_coin_including_a_contract_multiplier(snap: Snapshot) -> None:
    for coin in (ETH, MPEP):
        block = snap.reconcile(_member(snap, coin))
        assert block.values["price_gap_cmc_binance"] == pytest.approx(_price_gap(coin), abs=1e-9)
        assert abs(block.values["price_gap_cmc_binance"]) < 1e-4
        assert block.values["funding_gap_cmc_binance_per_h"] == pytest.approx(0.0, abs=1e-12)
        assert abs(block.values["oi_gap_cmc_binance"]) < 1e-4
        assert block.values["basis_binance"] == pytest.approx(INDEX_DISCOUNT / (1 - INDEX_DISCOUNT))
        assert block.values["mark_age_s"] == pytest.approx(10.0)
        assert block.values["cmc_quote_age_s"] == pytest.approx(80.0)
        assert block.data_quality() == {}
    # ETH's forceOrder lower bound (50 000 in the last hour, 70 000 in 4 h) sits under the CMC totals.
    eth = snap.crowding(_member(snap, ETH))
    assert eth.values["liq_binance_lb_1h_usd"] == pytest.approx(ETH_FORCE_1H)
    assert eth.values["liq_binance_lb_4h_usd"] == pytest.approx(ETH_FORCE_1H + ETH_FORCE_OLD)


def test_reconcile_flags_price_funding_oi_and_liquidation_mismatches(snap: Snapshot) -> None:
    block = snap.reconcile(_member(snap, RCON))
    oi_gap = (1 + RCON_OI_BIAS) * MARKET.mark(RCON, QUOTES_AT) / MARKET.mark(RCON, MARK_AT) - 1
    assert block.values["price_gap_cmc_binance"] == pytest.approx(_price_gap(RCON))
    assert block.values["funding_gap_cmc_binance_per_h"] == pytest.approx((RCON_CMC_FUNDING - 0.0001) / 8)
    assert block.values["oi_gap_cmc_binance"] == pytest.approx(oi_gap)
    quality = block.data_quality()
    assert quality[DataQualityFlag.RECONCILE_MISMATCH] == (
        "funding_gap_cmc_binance_per_h",
        "oi_gap_cmc_binance",
        "price_gap_cmc_binance",
    )
    # CMC's all-exchange 1 h total is below Binance's own forceOrder sum; the 4 h total is not.
    assert quiet_liq(RCON).h1 < 300_000 < quiet_liq(RCON).h4
    assert quality[DataQualityFlag.LIQ_MISMATCH] == ("liq_total_1h_usd",)
    # Reconciliation travels in every non-news packet.
    for agent in (AgentName.CROWDING, AgentName.TECHNICAL, AgentName.MICRO, AgentName.FUNDAMENTAL):
        packet_block = snap.agent_block(agent, _member(snap, RCON))
        assert DataQualityFlag.RECONCILE_MISMATCH in packet_block.data_quality()


def test_reconcile_marks_a_stale_cmc_quote(snap: Snapshot) -> None:
    block = snap.reconcile(_member(snap, NVEN))
    assert block.values["cmc_quote_age_s"] == pytest.approx(620.0)
    assert block.data_quality() == {DataQualityFlag.STALE: ("cmc_quote_age_s", "price_gap_cmc_binance")}


def test_error_response_on_a_core_cmc_route_degrades_the_whole_packet(
    main_lake: SyntheticLake, main_features: FeatureEngine, snap: Snapshot
) -> None:
    assert not snap.cmc_degraded
    degraded = _snap(main_lake, main_features, A_ERR)
    assert degraded.cmc_degraded
    block = degraded.agent_block(AgentName.TECHNICAL, _member(degraded, ETH))
    assert block.data_quality()[DataQualityFlag.CMC_DEGRADED] == ()
