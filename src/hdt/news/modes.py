"""Decision modes of the News agent (pure rules, no I/O). The output is a regime-rule probability of up,
never an order. First match wins:

1. Abstain on model disagreement: two judges disagreed on the class or direction of a material item.
2. Abstain on missing sources: the coin is an attention candidate but no article about it was ingested.
3. Ride: agreed evidence with hardness >= 0.7, code tier T0, novelty >= 0.7 (a first report, not an
   already known event), at most 2 h old, and the crowd positioned opposite (zF <= -crowd_z for an up
   catalyst, >= crowd_z for a down one): p follows the catalyst.
4. Fade panic: bad news of a hollow class (RUMOR, KOL_MEME, CIRCULAR) and zF <= -2 leans LONG only with
   refuting evidence: a liquidity capture recorded within `refute_window_min` after the news that itself
   lists liquidity changes reaching back to before the news (or its complete list) and, over
   `incident_lookback_h` before the news, no incident; or a DENIAL of bad news (direction up) at code
   tier T0 from a third-party domain (an exchange that is not the coin's own project)
   published between the first panic item and `refute_window_min` after the newest one. A denial by the
   project itself never counts, a security-only capture refutes nothing, and an on-chain incident
   anywhere in the look-back (a drain just before the headline included) cancels any refutation.
   Without refutation -> abstain (never a Fade panic).
5. Fade hype: attention candidate, up evidence only of hollow classes with hardness <= 0.3, zF >= 2,
   dOI4 >= 15% and no squeeze (short liquidations 1 h < 2 x long liquidations 1 h): p leans SHORT.
6. None: p = 0.5.
A rule that needs market inputs (zF, dOI4, liquidations) and does not have them abstains.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Final

from hdt.contracts.common import DirectionHint, Tier
from hdt.core.clock import ensure_utc
from hdt.news.config import ModesConfig
from hdt.news.dedupe import collapse_events
from hdt.news.onchain import OnchainState
from hdt.news.tiering import domain_in
from hdt.tools.ports import NewsMode

IMMATERIAL_CLASSES: Final[frozenset[str]] = frozenset({"NO_EVENT", "OTHER_FACT"})
HARD_DOWN_CLASSES: Final[frozenset[str]] = frozenset({"EXPLOIT", "DELIST", "UNLOCK", "REGULATORY"})

REASON_DISAGREE: Final[str] = "judges disagree on the class or direction of a material item"
REASON_NO_ARTICLES: Final[str] = "missing sources: attention candidate without ingested articles"
REASON_MARKET: Final[str] = "missing market inputs (funding z, open interest or liquidations)"
REASON_NO_REFUTATION: Final[str] = "panic news without independent refuting evidence"


@dataclass(frozen=True)
class EvidenceView:
    """One agreed verdict as the rules see it (tier and official are code-computed for this coin)."""

    item_id: str
    event_key: str
    event_class: str
    direction: DirectionHint
    tier: Tier
    official: bool
    hardness: float
    novelty: float
    quote: str
    known_at: datetime
    """When the verdict became known (`processed_at`)."""
    news_time: datetime
    """Event time when stated, else published / ingested time (never after `known_at`)."""
    domain: str
    known_event: str | None

    @property
    def first_report(self) -> bool:
        return self.known_event in (None, "new")


@dataclass(frozen=True)
class Disagreement:
    item_id: str
    classes: frozenset[str]

    @property
    def material(self) -> bool:
        return bool(self.classes - IMMATERIAL_CLASSES)


@dataclass(frozen=True)
class MarketState:
    z_funding: float | None
    doi_4h: float | None
    liq_long_1h_usd: float | None
    liq_short_1h_usd: float | None


@dataclass(frozen=True)
class ModeInputs:
    as_of: datetime
    coin_id: int
    evidence: tuple[EvidenceView, ...]
    disagreements: tuple[Disagreement, ...]
    attention: bool
    article_count: int
    market: MarketState | None
    onchain_around_panic: OnchainState | None
    """On-chain data recorded in [panic news - incident look-back, as_of] (see `panic_time`)."""


@dataclass(frozen=True)
class ModeDecision:
    mode: NewsMode
    p_model: float
    abstain_reason: str | None
    evidence: tuple[EvidenceView, ...]
    rule: str


def is_panic(e: EvidenceView, cfg: ModesConfig) -> bool:
    return e.direction is DirectionHint.DOWN and e.event_class in cfg.hollow_classes


def panic_time(evidence: Sequence[EvidenceView], cfg: ModesConfig) -> datetime | None:
    """News time of the newest panic item (the refutation window starts there)."""
    times = [e.news_time for e in evidence if is_panic(e, cfg)]
    return max(times) if times else None


def _ordered(evidence: Sequence[EvidenceView]) -> list[EvidenceView]:
    return collapse_events(evidence, lambda e: e.event_key, lambda e: (e.news_time, e.item_id))


def _pick(first: Sequence[EvidenceView], rest: Sequence[EvidenceView]) -> tuple[EvidenceView, ...]:
    seen = {e.item_id for e in first}
    return (*first, *[e for e in rest if e.item_id not in seen])[:20]


def decide(inputs: ModeInputs, cfg: ModesConfig) -> ModeDecision:
    as_of = ensure_utc(inputs.as_of)
    evidence = _ordered(inputs.evidence)

    def out(
        mode: NewsMode, p: float, rule: str, used: Sequence[EvidenceView] = (), reason: str | None = None
    ) -> ModeDecision:
        return ModeDecision(mode, p, reason, _pick(used, evidence), rule)

    if any(d.material for d in inputs.disagreements):
        return out("abstain", 0.5, "disagreement", reason=REASON_DISAGREE)
    if inputs.attention and inputs.article_count == 0:
        return out("abstain", 0.5, "missing_sources", reason=REASON_NO_ARTICLES)
    market = inputs.market
    z = market.z_funding if market is not None else None

    ride = [
        e
        for e in evidence
        if e.hardness >= cfg.ride_hardness_min
        and e.tier is Tier.T0
        and e.novelty >= cfg.ride_novelty_min
        and e.first_report
        and e.direction is not DirectionHint.NONE
        and as_of - e.news_time <= timedelta(hours=cfg.ride_max_age_h)
    ]
    if ride:
        if z is None:
            return out("abstain", 0.5, "ride_no_market", ride, REASON_MARKET)
        catalyst = max(ride, key=lambda e: (e.hardness, e.news_time))
        up = catalyst.direction is DirectionHint.UP
        if (up and z <= -cfg.crowd_z) or (not up and z >= cfg.crowd_z):
            p = cfg.p_ride_up if up else 1 - cfg.p_ride_up
            return out("ride", p, "ride", [catalyst])

    panic = [e for e in evidence if is_panic(e, cfg)]
    hard_down = [
        e
        for e in evidence
        if e.direction is DirectionHint.DOWN
        and e.event_class in HARD_DOWN_CLASSES
        and (e.official or e.hardness >= cfg.ride_hardness_min)
    ]
    if panic and not hard_down:
        if z is None:
            return out("abstain", 0.5, "panic_no_market", panic, REASON_MARKET)
        if z <= cfg.fade_panic_zf_max:
            refuters = _refuters(inputs.coin_id, evidence, panic, inputs.onchain_around_panic, cfg)
            if refuters is None:
                return out("abstain", 0.5, "panic_unrefuted", panic, REASON_NO_REFUTATION)
            return out("fade_panic", cfg.p_fade_panic, "fade_panic", [*panic, *refuters])

    rising = [e for e in evidence if e.direction is DirectionHint.UP]
    hollow = [
        e for e in rising if e.event_class in cfg.hollow_classes and e.hardness <= cfg.fade_hype_hardness_max
    ]
    if inputs.attention and hollow and len(hollow) == len(rising):
        if market is None or z is None or market.doi_4h is None:
            return out("abstain", 0.5, "hype_no_market", hollow, REASON_MARKET)
        if z >= cfg.fade_hype_zf_min and market.doi_4h >= cfg.fade_hype_doi4_min:
            if market.liq_long_1h_usd is None or market.liq_short_1h_usd is None:
                return out("abstain", 0.5, "hype_no_market", hollow, REASON_MARKET)
            if market.liq_short_1h_usd < cfg.squeeze_ratio * market.liq_long_1h_usd:
                return out("fade_hype", cfg.p_fade_hype, "fade_hype", hollow)
            return out("none", 0.5, "hype_squeeze", hollow)
    return out("none", 0.5, "none")


def _refuters(
    coin_id: int,
    evidence: Sequence[EvidenceView],
    panic: Sequence[EvidenceView],
    onchain: OnchainState | None,
    cfg: ModesConfig,
) -> tuple[EvidenceView, ...] | None:
    """Independent refutation of the newest panic item; None when there is none (or it is cancelled)."""
    first = min(e.news_time for e in panic)
    start = max(e.news_time for e in panic)
    window = timedelta(minutes=cfg.refute_window_min)
    if onchain is not None and onchain.incident(cfg.onchain_drain_usd_min):
        return None
    own = cfg.project_domains.get(coin_id, ())
    third_party = tuple(
        e
        for e in evidence
        if e.event_class == "DENIAL"
        and e.direction is DirectionHint.UP  # it denies bad news; a denial of good news refutes no panic
        and e.tier is Tier.T0
        and domain_in(e.domain, cfg.third_party_t0_domains)
        and not domain_in(e.domain, own)
        and e.known_at >= start
        and first <= e.news_time <= start + window
    )
    if third_party:
        return third_party
    if onchain is not None and onchain.covered_after(start, start + window):
        return ()
    return None
