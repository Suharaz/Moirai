"""Ingestion dedupe (hdt.news.dedupe, hdt.news.text.url_key) and veto rules (hdt.news.veto)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from hdt.news.config import load_news_config
from hdt.news.dedupe import IngestDeduper, RecentItem
from hdt.news.text import url_key
from hdt.news.veto import VetoVerdict, decide_veto, scan_fresh_at
from hdt.tools.ports import UnlockEvent

CFG = load_news_config()
NOW = datetime(2026, 9, 1, 12, tzinfo=UTC)


def test_url_key_ignores_tracking_and_fragment() -> None:
    assert url_key("HTTPS://www.CoinDesk.com/a/b/?utm_source=x&id=2#top") == url_key(
        "https://www.coindesk.com/a/b?id=2"
    )
    assert url_key("https://x.com/a?id=1") != url_key("https://x.com/a?id=2")


def test_rewritten_article_same_event_is_near_duplicate() -> None:
    first = RecentItem(
        "rss:1",
        (5,),
        "Hackers drain 12 million dollars from Xyz Protocol bridge",
        "Attackers exploited a bug in the Xyz Protocol bridge on Monday and drained about 12 million dollars "
        "in ETH and USDC, the team confirmed in a post.",
        None,
    )
    dd = IngestDeduper([first], CFG.ingest.near_duplicate_jaccard)
    near = dd.check(
        (5,),
        "Hackers drain 12 million dollars from Xyz Protocol bridge, team confirms",
        "Attackers exploited a bug in the Xyz Protocol bridge on Monday and drained about 12 million dollars "
        "in ETH and USDC, the team confirmed in a post.",
    )
    assert (near.duplicate_of, near.kind) == ("rss:1", "near")
    exact = dd.check((5,), "HACKERS drain 12 million dollars from Xyz Protocol bridge!", None)
    assert (exact.duplicate_of, exact.kind) == ("rss:1", "exact")
    other_coin = dd.check((9,), first.title, first.summary)
    assert other_coin.duplicate_of is None
    unrelated = dd.check(
        (5,), "Xyz Protocol launches staking on mainnet", "Staking goes live for all holders."
    )
    assert unrelated.duplicate_of is None


def test_duplicate_chain_points_to_root() -> None:
    dd = IngestDeduper([], 0.8)
    dd.add("rss:1", (5,), "Binance lists XYZ", None)
    dd.add("rss:2", (5,), "Binance lists XYZ today", None, duplicate_of="rss:1")
    assert dd.check((5,), "Binance lists XYZ today", None).duplicate_of == "rss:1"


def v(
    status: str, cls: str | None, official: bool = False, a: str | None = None, b: str | None = None
) -> VetoVerdict:
    return VetoVerdict("rss:1", status, cls, a, b, official)


def decide(
    *verdicts: VetoVerdict,
    unlocks: tuple[UnlockEvent, ...] = (),
    tiers: dict[str, str] | None = None,
    unjudged: tuple[str, ...] = (),
):  # type: ignore[no-untyped-def]
    return decide_veto(verdicts, unlocks, tiers or {}, unjudged, as_of=NOW, cfg=CFG.veto)


def test_hard_and_soft_veto() -> None:
    hard = decide(v("agreed", "EXPLOIT", official=True))
    assert (hard.veto_long, hard.veto_short, hard.kind) == (True, False, "hard")
    soft = decide(v("agreed", "DELIST", official=False))
    assert (soft.veto_long, soft.size_mult) == (False, CFG.veto.soft_size_mult)
    assert decide(v("agreed", "LISTING", official=True)).kind == "clear"


def test_official_unlock_verdict_is_soft_the_calendar_decides_size() -> None:
    assert decide(v("agreed", "UNLOCK", official=True)).kind == "soft"


def test_failed_keyword_verdict_is_soft_veto_until_judged() -> None:
    failed = VetoVerdict("rss:2", "quote_failed", None, None, None, False, unconfirmed_keyword_hit=True)
    out = decide(failed)
    assert (out.veto_long, out.size_mult) == (False, CFG.veto.soft_size_mult)
    assert out.reasons["unconfirmed"] == [{"item_id": "rss:2", "status": "quote_failed"}]
    assert decide(v("quote_failed", None)).kind == "clear"  # no bad-catalyst keyword: not a veto


def test_exploit_delist_disagreement_is_soft_veto_not_silent() -> None:
    out = decide(v("disagree", None, a="EXPLOIT", b="RUMOR"))
    assert (out.veto_long, out.size_mult) == (False, CFG.veto.soft_size_mult)
    assert decide(v("disagree", None, a="PRODUCT", b="PARTNERSHIP")).kind == "clear"
    assert decide(unjudged=("rss:9",)).kind == "soft"


def test_large_unlock_hard_only_from_t0_calendar() -> None:
    unlock = UnlockEvent(
        coin_id=5, unlock_at=NOW + timedelta(hours=10), pct_of_circulating=5.0, source="cal", recorded_at=NOW
    )
    assert decide(unlocks=(unlock,), tiers={"cal": "T0"}).veto_long is True
    assert decide(unlocks=(unlock,), tiers={"cal": "T1"}).kind == "soft"
    small = unlock.model_copy(update={"pct_of_circulating": 0.5})
    assert decide(unlocks=(small,), tiers={"cal": "T0"}).kind == "clear"


def test_scan_fresh_at_aged_by_stale_required_source() -> None:
    fresh, stale = scan_fresh_at(NOW, {"binance_announcements": NOW - timedelta(seconds=60)}, CFG.veto, 300)
    assert (fresh, stale) == (NOW, [])
    old = NOW - timedelta(hours=1)
    fresh, stale = scan_fresh_at(NOW, {"binance_announcements": old}, CFG.veto, 300)
    assert (fresh, stale) == (old, ["binance_announcements"])
    fresh, _ = scan_fresh_at(NOW, {}, CFG.veto, 300)
    assert NOW - fresh > timedelta(seconds=2 * 300)
