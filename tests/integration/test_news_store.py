"""News tables on Postgres: point-in-time reads, duplicate hiding, versioned unlocks, assessor replay."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy as sa

from hdt.lake.pit_query import PitQuery
from hdt.lake.raw_store import RawStore
from hdt.lake.schemas import Capture
from hdt.news.assess import PgNewsAssessor
from hdt.news.config import load_news_config
from hdt.news.modes import MarketState
from hdt.news.onchain import OnchainState, onchain_state
from hdt.news.sources import RawUnlock
from hdt.news.store import ITEMS, VERDICTS, PgNewsIndex, insert_rows
from hdt.news.unlocks import PgUnlockCalendar, record_unlocks
from hdt.tools.impl.dex import LIQUIDITY_ROUTE, SECURITY_ROUTE

T0 = datetime(2026, 9, 1, 12, tzinfo=UTC)
CFG = load_news_config()


class Market:
    def state(self, coin_id: int, as_of: datetime) -> MarketState | None:
        return MarketState(z_funding=-1.5, doi_4h=0.0, liq_long_1h_usd=0.0, liq_short_1h_usd=0.0)

    def onchain(self, coin_id: int, start: datetime, end: datetime) -> OnchainState | None:
        return None


class LakeOnchainMarket:
    """Crowd short (zF = -2.5, a Fade panic setting); on-chain data from a real point-in-time lake."""

    def __init__(self, pit: PitQuery) -> None:
        self.pit = pit

    def state(self, coin_id: int, as_of: datetime) -> MarketState | None:
        return MarketState(z_funding=-2.5, doi_4h=0.0, liq_long_1h_usd=0.0, liq_short_1h_usd=0.0)

    def onchain(self, coin_id: int, start: datetime, end: datetime) -> OnchainState | None:
        return onchain_state(self.pit, coin_id, start, end)


def item(item_id: str, url: str, dup: str | None = None) -> dict[str, Any]:
    return {
        "item_id": item_id,
        "source_key": "binance_announcements",
        "source_kind": "binance",
        "source_name": "Binance announcements",
        "url": url,
        "url_key": url,
        "domain": "www.binance.com",
        "tier": "T0",
        "title": "Binance Will List Xyz (XYZ)",
        "summary": None,
        "content": None,
        "published_at": T0,
        "ingested_at": T0,
        "coin_ids": [5],
        "title_fingerprint": "f",
        "duplicate_of": dup,
        "duplicate_kind": "exact" if dup else None,
        "similarity": None,
        "raw_sha256": "r",
    }


def verdict(processed_at: datetime) -> dict[str, Any]:
    return {
        "item_id": "binance:1",
        "rule_version": CFG.rule_version,
        "processed_at": processed_at,
        "processor": "news",
        "coin_ids": [5],
        "status": "agreed",
        "event_key": "binance_spot_listing:XYZ.20260901",
        "event_class": "LISTING",
        "direction": "up",
        "hardness": 0.9,
        "novelty": 0.9,
        "confidence": 0.9,
        "event_time": None,
        "quote": "Binance Will List Xyz (XYZ)",
        "quote_ok": True,
        "domain": "www.binance.com",
        "tier": "T0",
        "official": True,
        "official_ref": "x",
        "class_a": "LISTING",
        "class_b": "LISTING",
        "direction_a": "up",
        "direction_b": "up",
        "known_event": "new",
        "detail": {"official_coins": [5]},
    }


def test_point_in_time_items_verdicts_and_assessment(pg_engine: sa.Engine) -> None:
    with pg_engine.begin() as conn:
        insert_rows(conn, ITEMS, [item("binance:1", "https://www.binance.com/en/a/1")])
        insert_rows(conn, ITEMS, [item("binance:2", "https://www.binance.com/en/a/2", dup="binance:1")])
        insert_rows(conn, VERDICTS, [verdict(T0 + timedelta(minutes=10))])
        assert insert_rows(conn, VERDICTS, [verdict(T0 + timedelta(minutes=20))]) == 0  # insert-only
    index = PgNewsIndex(pg_engine)
    assert index.item("binance:1", T0 - timedelta(seconds=1)) is None
    assert [i.item_id for i in index.items(5, T0 - timedelta(hours=1), T0, 10)] == ["binance:1"]
    assessor = PgNewsAssessor(pg_engine, Market(), config=CFG)
    before = assessor.assess(5, T0 + timedelta(minutes=5))
    assert (before.mode, before.evidence) == ("none", ())
    after = assessor.assess(5, T0 + timedelta(minutes=30))
    assert (after.mode, after.p_model) == ("ride", CFG.modes.p_ride_up)
    assert (after.evidence[0].tier.value, after.evidence[0].official) == ("T0", True)


def test_unlock_schedule_is_versioned(pg_engine: sa.Engine) -> None:
    at = T0 + timedelta(days=3)
    first = [RawUnlock(5, at, 1000.0, 3.0, "team")]
    with pg_engine.begin() as conn:
        assert record_unlocks(conn, "cal", "T1", first, recorded_at=T0) == 1
        assert record_unlocks(conn, "cal", "T1", first, recorded_at=T0 + timedelta(hours=1)) == 0
        assert record_unlocks(conn, "cal", "T1", [], recorded_at=T0 + timedelta(hours=2)) == 1
    cal = PgUnlockCalendar(pg_engine)
    window = (T0, T0 + timedelta(days=7))
    assert len(cal.unlocks(5, *window, as_of=T0 + timedelta(minutes=30))) == 1
    assert cal.unlocks(5, *window, as_of=T0 + timedelta(hours=3)) == []
    assert cal.unlocks(5, *window, as_of=T0 - timedelta(minutes=1)) == []


def test_agreed_verdict_needs_a_found_quote(pg_engine: sa.Engine) -> None:
    row = verdict(T0) | {"item_id": "binance:unquoted", "quote_ok": False}
    with pytest.raises(sa.exc.IntegrityError, match="agreed_quote"), pg_engine.begin() as conn:
        insert_rows(conn, VERDICTS, [row])


def _panic_rows(coin_id: int) -> tuple[dict[str, Any], dict[str, Any]]:
    item_id = f"rss:panic-{coin_id}"
    row = item(item_id, f"https://www.coindesk.com/panic/{coin_id}")
    row.update(
        source_key="coindesk",
        source_kind="rss",
        source_name="CoinDesk",
        domain="www.coindesk.com",
        tier="T1",
        title="Rumor: Xyz pools drained",
        coin_ids=[coin_id],
    )
    judged = verdict(T0 + timedelta(minutes=2))
    judged.update(
        item_id=item_id,
        coin_ids=[coin_id],
        event_key=f"rumor_drain:XYZ{coin_id}.20260901",
        event_class="RUMOR",
        direction="down",
        hardness=0.2,
        quote="Rumor: Xyz pools drained",
        domain="www.coindesk.com",
        tier="T1",
        official=False,
        official_ref=None,
        class_a="RUMOR",
        class_b="RUMOR",
        direction_a="down",
        direction_b="down",
        detail={},
    )
    return row, judged


def _dex(store: RawStore, coin_id: int, route: str, at: datetime, data: Any) -> None:
    body = json.dumps({"status": {"error_code": 0}, "data": data}).encode()
    store.append(Capture("cmc", route, at, 200, body, params={"address": "0xabc"}, key=str(coin_id)))


def _change(at: datetime, kind: str, usd: float, tx: str) -> dict[str, Any]:
    return {"ts": int(at.timestamp() * 1000), "tp": kind, "tu": usd, "txId": tx}


@pytest.mark.parametrize("case", ["drained_before_headline", "security_only", "clear_liquidity"])
def test_fade_panic_needs_liquidity_data_clear_since_before_the_headline(
    pg_engine: sa.Engine, tmp_path: Path, case: str
) -> None:
    coin_id = {"drained_before_headline": 601, "security_only": 602, "clear_liquidity": 603}[case]
    store = RawStore(tmp_path / "staging", tmp_path / "lake")
    pit = PitQuery(tmp_path / "staging", tmp_path / "lake")
    captured = T0 + timedelta(minutes=10)
    drain = CFG.modes.onchain_drain_usd_min * 2
    if case == "drained_before_headline":
        changes = [_change(T0 - timedelta(minutes=20), "remove", drain, "0xdrain")]
        _dex(store, coin_id, LIQUIDITY_ROUTE, captured, {"lcs": changes})
    elif case == "security_only":
        _dex(store, coin_id, SECURITY_ROUTE, captured, [{"securityItems": []}])
    else:
        changes = [_change(T0 - timedelta(minutes=20), "add", 5_000.0, "0xadd")]
        _dex(store, coin_id, LIQUIDITY_ROUTE, captured, {"lcs": changes})
    row, judged = _panic_rows(coin_id)
    with pg_engine.begin() as conn:
        insert_rows(conn, ITEMS, [row])
        insert_rows(conn, VERDICTS, [judged])
    out = PgNewsAssessor(pg_engine, LakeOnchainMarket(pit), config=CFG).assess(
        coin_id, T0 + timedelta(minutes=30)
    )
    expected = "fade_panic" if case == "clear_liquidity" else "abstain"
    assert out.mode == expected
