"""Coin attribution of news text (hdt.news.coins) and of official announcements (hdt.news.official)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from hdt.core.config import static_config
from hdt.news.coins import CoinDirectory, CoinRef
from hdt.news.config import load_news_config
from hdt.news.official import ItemEvidence, OfficialCheck, check_official, names_coin
from hdt.news.sources import parse_time
from hdt.news.veto import VetoVerdict, decide_veto
from hdt.tools.ports import Announcement

CFG = load_news_config()
NOW = datetime(2026, 9, 1, 12, tzinfo=UTC)
ETH = CoinRef(1027, "ETH", "Ethereum")
BNB = CoinRef(1839, "BNB", "BNB")
FOO = CoinRef(9999, "FOO", "Foo Network")
DELIST_PAIRS = "Binance Margin Will Delist FOO/ETH, FOO/BNB Isolated Margin Pairs"
TUSD_PAIRS = "Binance Will Delist ETH/TUSD, BNB/TUSD Spot Trading Pairs"
PERP = "Binance Futures Will Delist USDⓈ-M FOOUSDT Perpetual Contract"


def _official_delist(title: str, coin: CoinRef) -> OfficialCheck:
    url = "https://www.binance.com/en/support/announcement/detail/abc"
    notice = Announcement(
        exchange="binance",
        title=title,
        url=url,
        catalog="delist",
        published_at=NOW - timedelta(minutes=5),
        recorded_at=NOW - timedelta(minutes=4),
    )
    return check_official(
        "DELIST",
        coin,
        as_of=NOW,
        item=ItemEvidence(url, title, NOW - timedelta(minutes=5)),
        announcements=[notice],
        sources=static_config().news_sources,
        onchain=None,
        drain_usd_min=CFG.modes.onchain_drain_usd_min,
        unlocks=(),
        unlock_tier="T2",
        unlock_ahead=timedelta(hours=24),
    )


def _veto(title: str, coin: CoinRef) -> str:
    official = _official_delist(title, coin).official
    verdict = VetoVerdict("binance:1", "agreed", "DELIST", "DELIST", "DELIST", official=official)
    return decide_veto([verdict], [], {}, [], as_of=NOW, cfg=CFG.veto).kind


def test_only_the_base_leg_of_a_pair_maps_and_no_pair_leg_names_the_coin() -> None:
    coins = CoinDirectory([ETH, BNB, FOO])
    assert coins.match(DELIST_PAIRS) == (9999,)
    assert coins.match(TUSD_PAIRS) == (1027, 1839)
    for coin in (ETH, BNB, FOO):
        assert not names_coin(DELIST_PAIRS, coin)
        assert not names_coin(TUSD_PAIRS, coin)


def test_pair_removal_notice_is_at_most_a_soft_veto() -> None:
    assert _veto(TUSD_PAIRS, ETH) == "soft"
    assert _veto(TUSD_PAIRS, BNB) == "soft"
    assert _veto(DELIST_PAIRS, FOO) == "soft"


def test_contract_ticker_names_the_coin_and_its_delist_is_official() -> None:
    coins = CoinDirectory([ETH, BNB, FOO])
    assert coins.match(PERP) == (9999,)
    assert coins.match("Binance Will Delist FOOUSDT and ETH perpetual contracts") == (1027, 9999)
    assert coins.match("Binance Futures Will Launch 1000FOOUSDC Perpetual") == (9999,)
    assert names_coin(PERP, FOO)
    assert _official_delist(PERP, FOO).official
    assert _veto(PERP, FOO) == "hard"
    assert not names_coin("Binance Will Delist FOOBARUSDT", FOO)


def test_bare_symbol_matches_case_sensitively_and_never_an_ambiguous_word() -> None:
    alpha = CoinRef(7232, "ALPHA", "Stella")
    assert not names_coin("Binance Alpha Adds New Tokens", alpha)
    assert names_coin("Binance Will Delist ALPHA", alpha)
    assert names_coin("Binance Will Delist Stella (ALPHA)", alpha)
    one = CoinRef(3945, "ONE", "Harmony")
    assert not names_coin("Binance Will Delist ONE Token Pair", one)
    assert names_coin("Binance Will Delist Harmony (ONE)", one)


def test_out_of_range_feed_times_are_unknown_not_errors() -> None:
    for raw in (
        "9999-12-31T23:59:59-01:00",
        "0001-01-01T00:00:00+01:00",
        "Fri, 31 Dec 9999 23:59:59 -0100",
        "9" * 30,
        "99999999999999999",
    ):
        assert parse_time(raw) is None
    assert parse_time("2026-09-01T12:00:00Z") == NOW
    assert parse_time("1788264000000") == NOW
