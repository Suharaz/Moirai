"""News decision modes (hdt.news.modes): table tests, especially no Fade panic without refutation."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from hdt.contracts.common import DirectionHint, Tier
from hdt.news.config import load_news_config
from hdt.news.modes import Disagreement, EvidenceView, MarketState, ModeInputs, decide
from hdt.news.onchain import LiquidityCapture, OnchainState

CFG = load_news_config().modes
NOW = datetime(2026, 9, 1, 12, tzinfo=UTC)


def ev(
    cls: str,
    direction: DirectionHint,
    *,
    hardness: float = 0.1,
    novelty: float = 0.9,
    tier: Tier = Tier.T2,
    domain: str = "news.example.com",
    age_min: int = 30,
    item_id: str = "rss:a",
) -> EvidenceView:
    t = NOW - timedelta(minutes=age_min)
    return EvidenceView(
        item_id=item_id,
        event_key=f"{cls.lower()}:X.{item_id}",
        event_class=cls,
        direction=direction,
        tier=tier,
        official=tier is Tier.T0,
        hardness=hardness,
        novelty=novelty,
        quote="a verbatim sentence of the item",
        known_at=t + timedelta(minutes=2),
        news_time=t,
        domain=domain,
        known_event="new",
    )


def market(
    z: float | None, doi: float | None = 0.2, long_liq: float = 1e6, short_liq: float = 1e5
) -> MarketState:
    return MarketState(z_funding=z, doi_4h=doi, liq_long_1h_usd=long_liq, liq_short_1h_usd=short_liq)


def inputs(*evidence: EvidenceView, **kw: object) -> ModeInputs:
    base = ModeInputs(
        as_of=NOW,
        coin_id=1,
        evidence=evidence,
        disagreements=(),
        attention=False,
        article_count=len(evidence),
        market=market(0.0),
        onchain_around_panic=None,
    )
    return replace(base, **kw)  # type: ignore[arg-type]


def onchain(minutes_after: int, removals: float = 0.0, changes: int = 3) -> OnchainState:
    at = NOW - timedelta(minutes=30 - minutes_after)
    oldest = NOW - timedelta(hours=5) if changes else None
    capture = LiquidityCapture(at, changes, oldest, complete=False)
    return OnchainState(1, at, removals, changes, (), (), (capture,))


PANIC = ev("RUMOR", DirectionHint.DOWN)


def test_fade_panic_without_refutation_abstains() -> None:
    out = decide(inputs(PANIC, market=market(-2.5)), CFG)
    assert out.mode == "abstain"
    assert out.p_model == 0.5
    assert "refuting" in (out.abstain_reason or "")


def test_project_own_denial_does_not_refute() -> None:
    denial = ev("DENIAL", DirectionHint.UP, tier=Tier.T2, domain="project.io", item_id="rss:d")
    out = decide(inputs(PANIC, denial, market=market(-2.5)), CFG)
    assert out.mode == "abstain"


def test_fake_official_denial_on_lookalike_domain_does_not_refute() -> None:
    denial = ev("DENIAL", DirectionHint.UP, tier=Tier.T0, domain="www.binance.com.evil.io", item_id="rss:d")
    assert decide(inputs(PANIC, denial, market=market(-2.5)), CFG).mode == "abstain"


def test_third_party_t0_denial_refutes() -> None:
    denial = ev(
        "DENIAL", DirectionHint.UP, tier=Tier.T0, domain="www.binance.com", item_id="binance:d", age_min=10
    )
    out = decide(inputs(PANIC, denial, market=market(-2.5)), CFG)
    assert out.mode == "fade_panic"
    assert out.p_model == CFG.p_fade_panic


def test_onchain_clear_within_window_refutes_but_late_capture_does_not() -> None:
    assert (
        decide(inputs(PANIC, market=market(-2.5), onchain_around_panic=onchain(20)), CFG).mode == "fade_panic"
    )
    late = onchain(CFG.refute_window_min + 5)
    assert decide(inputs(PANIC, market=market(-2.5), onchain_around_panic=late), CFG).mode == "abstain"


def test_capture_without_liquidity_data_refutes_nothing() -> None:
    empty = onchain(20, changes=0)  # security-only capture, or a liquidity capture listing no change
    assert decide(inputs(PANIC, market=market(-2.5), onchain_around_panic=empty), CFG).mode == "abstain"
    at = NOW - timedelta(minutes=10)
    no_changes = OnchainState(1, at, 0.0, 0, (), (), (LiquidityCapture(at, 0, None, complete=True),))
    assert decide(inputs(PANIC, market=market(-2.5), onchain_around_panic=no_changes), CFG).mode == "abstain"


def test_post_headline_capture_must_itself_reach_before_the_headline() -> None:
    before = LiquidityCapture(NOW - timedelta(hours=10), 1, NOW - timedelta(hours=11), complete=True)
    after = NOW - timedelta(minutes=20)
    # Older data exists, but the capture inside the refutation window lists nothing: no refutation.
    empty_after = OnchainState(1, after, 0.0, 1, (), (), (before, LiquidityCapture(after, 0, None, True)))
    assert decide(inputs(PANIC, market=market(-2.5), onchain_around_panic=empty_after), CFG).mode == "abstain"
    # A full page (limit reached) whose oldest change is after the headline may hide a pre-headline drain.
    truncated = LiquidityCapture(after, 100, NOW - timedelta(minutes=25), complete=False)
    state = OnchainState(1, after, 0.0, 100, (), (), (truncated,))
    assert decide(inputs(PANIC, market=market(-2.5), onchain_around_panic=state), CFG).mode == "abstain"
    # The same list when complete (fewer than the limit) does cover the pre-headline span.
    complete = replace(state, liquidity=(replace(truncated, complete=True),))
    assert decide(inputs(PANIC, market=market(-2.5), onchain_around_panic=complete), CFG).mode == "fade_panic"


def test_denial_of_good_news_does_not_refute_a_panic() -> None:
    denial = ev(
        "DENIAL", DirectionHint.DOWN, tier=Tier.T0, domain="www.binance.com", item_id="binance:d", age_min=10
    )
    assert decide(inputs(PANIC, denial, market=market(-2.5)), CFG).mode == "abstain"


def test_onchain_incident_cancels_denial_project_exploited_while_denying() -> None:
    denial = ev(
        "DENIAL", DirectionHint.UP, tier=Tier.T0, domain="www.binance.com", item_id="binance:d", age_min=10
    )
    drained = onchain(10, removals=CFG.onchain_drain_usd_min * 2)
    out = decide(inputs(PANIC, denial, market=market(-2.5), onchain_around_panic=drained), CFG)
    assert out.mode == "abstain"


def test_binance_denial_is_not_third_party_for_bnb() -> None:
    denial = ev(
        "DENIAL", DirectionHint.UP, tier=Tier.T0, domain="www.binance.com", item_id="binance:d", age_min=10
    )
    assert decide(inputs(PANIC, denial, market=market(-2.5), coin_id=1839), CFG).mode == "abstain"


def test_denial_long_after_the_panic_news_is_not_tied_to_it() -> None:
    old_panic = ev("RUMOR", DirectionHint.DOWN, age_min=CFG.refute_window_min + 60)
    denial = ev(
        "DENIAL", DirectionHint.UP, tier=Tier.T0, domain="www.binance.com", item_id="binance:d", age_min=10
    )
    assert decide(inputs(old_panic, denial, market=market(-2.5)), CFG).mode == "abstain"


def test_panic_without_extreme_funding_is_none() -> None:
    assert decide(inputs(PANIC, market=market(-1.0)), CFG).mode == "none"


def test_fade_hype_needs_all_conditions() -> None:
    hype = ev("KOL_MEME", DirectionHint.UP)
    ok = decide(inputs(hype, attention=True, market=market(2.5, 0.2)), CFG)
    assert (ok.mode, ok.p_model) == ("fade_hype", CFG.p_fade_hype)
    assert decide(inputs(hype, attention=False, market=market(2.5, 0.2)), CFG).mode == "none"
    assert decide(inputs(hype, attention=True, market=market(1.5, 0.2)), CFG).mode == "none"
    assert decide(inputs(hype, attention=True, market=market(2.5, 0.05)), CFG).mode == "none"
    squeeze = market(2.5, 0.2, long_liq=1e5, short_liq=3e5)
    assert decide(inputs(hype, attention=True, market=squeeze), CFG).mode == "none"
    hard_up = ev("LISTING", DirectionHint.UP, hardness=0.9, item_id="rss:l")
    assert decide(inputs(hype, hard_up, attention=True, market=market(2.5, 0.2)), CFG).mode == "none"


def test_ride_follows_t0_catalyst_against_crowd() -> None:
    listing = ev("LISTING", DirectionHint.UP, hardness=0.9, tier=Tier.T0, age_min=30)
    out = decide(inputs(listing, market=market(-1.5)), CFG)
    assert (out.mode, out.p_model) == ("ride", CFG.p_ride_up)
    assert decide(inputs(listing, market=market(1.5)), CFG).mode == "none"
    stale = replace(listing, news_time=NOW - timedelta(hours=3))
    assert decide(inputs(stale, market=market(-1.5)), CFG).mode == "none"
    rehash = replace(listing, known_event="near")
    assert decide(inputs(rehash, market=market(-1.5)), CFG).mode == "none"
    exploit = ev("EXPLOIT", DirectionHint.DOWN, hardness=0.9, tier=Tier.T0)
    out = decide(inputs(exploit, market=market(1.5)), CFG)
    assert out.mode == "ride"
    assert out.p_model == pytest.approx(1 - CFG.p_ride_up)


def test_judge_disagreement_abstains() -> None:
    hype = ev("KOL_MEME", DirectionHint.UP)
    out = decide(
        inputs(
            hype,
            attention=True,
            market=market(2.5, 0.2),
            disagreements=(Disagreement("rss:x", frozenset({"RUMOR", "LISTING"})),),
        ),
        CFG,
    )
    assert out.mode == "abstain"
    immaterial = (Disagreement("rss:x", frozenset({"NO_EVENT", "OTHER_FACT"})),)
    assert (
        decide(inputs(hype, attention=True, market=market(2.5, 0.2), disagreements=immaterial), CFG).mode
        == "fade_hype"
    )


def test_missing_market_or_sources_abstain() -> None:
    assert decide(inputs(PANIC, market=None), CFG).mode == "abstain"
    assert decide(inputs(attention=True, article_count=0), CFG).abstain_reason is not None
    assert decide(inputs(), CFG).mode == "none"
