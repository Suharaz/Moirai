"""`PgNewsAssessor`: the News agent's regime-rule assessment of one coin at `as_of` (port `NewsAssessor`).

Point in time: only items ingested at or before `as_of`, verdicts processed at or before `as_of`,
attention computed at or before `as_of`, and lake data fetched at or before `as_of` are read, so a replay
reproduces the live assessment without any model call. The rules are `hdt.news.modes`.
Tier and official status are recomputed for the assessed coin: a verdict confirmed for another coin of
the same item is not official for this one.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta

import sqlalchemy as sa

from hdt.contracts.common import DirectionHint
from hdt.core.clock import ensure_utc
from hdt.core.config import NewsSourcesFile, static_config
from hdt.news.config import NewsFile, news_config
from hdt.news.market import MarketView
from hdt.news.modes import (
    Disagreement,
    EvidenceView,
    ModeInputs,
    decide,
    panic_time,
)
from hdt.news.store import StoredItem, StoredVerdict, attention_at, items_for_coin, verdicts_for_coin
from hdt.news.tiering import code_tier
from hdt.tools.ports import NewsAssessment, NewsEvidence


def evidence_view(
    item: StoredItem, verdict: StoredVerdict, coin_id: int, sources: NewsSourcesFile
) -> EvidenceView | None:
    """The agreed verdict as evidence about `coin_id` (None when it is not usable evidence)."""
    if (
        verdict.status != "agreed"
        or not verdict.quote_ok
        or not verdict.quote
        or verdict.event_key is None
        or verdict.event_class is None
        or verdict.direction is None
        or verdict.hardness is None
        or verdict.novelty is None
    ):
        return None
    official = coin_id in set(verdict.detail.get("official_coins") or ())
    tier = code_tier(
        item.url,
        sources,
        event_class=verdict.event_class,
        official=official,
        recorded_announcement=item.source_kind == "binance",
    )
    news_time = item.news_time
    return EvidenceView(
        item_id=item.item_id,
        event_key=verdict.event_key,
        event_class=verdict.event_class,
        direction=verdict.direction,
        tier=tier,
        official=official,
        hardness=verdict.hardness,
        novelty=verdict.novelty,
        quote=verdict.quote[:280],
        known_at=verdict.processed_at,
        news_time=news_time,
        domain=item.domain,
        known_event=verdict.known_event,
    )


def news_evidence(views: Sequence[EvidenceView]) -> tuple[NewsEvidence, ...]:
    return tuple(
        NewsEvidence(
            item_id=v.item_id,
            event_key=v.event_key,
            event_class=v.event_class,
            direction=DirectionHint(v.direction),
            tier=v.tier,
            official=v.official,
            hardness=v.hardness,
            novelty=v.novelty,
            quote=v.quote,
            known_at=v.known_at,
        )
        for v in views[:20]
    )


class PgNewsAssessor:
    def __init__(
        self,
        engine: sa.Engine,
        market: MarketView,
        *,
        config: NewsFile | None = None,
        sources: NewsSourcesFile | None = None,
    ) -> None:
        self._engine = engine
        self._market = market
        self._config = config or news_config()
        self._sources = sources or static_config().news_sources

    def assess(self, coin_id: int, as_of: datetime) -> NewsAssessment:
        cfg = self._config
        as_of = ensure_utc(as_of)
        since = as_of - timedelta(hours=cfg.modes.window_h)
        with self._engine.connect() as conn:
            pairs = verdicts_for_coin(conn, coin_id, cfg.rule_version, since, as_of)
            articles = len(items_for_coin(conn, coin_id, since, as_of))
            attention = attention_at(conn, coin_id, as_of, timedelta(seconds=2 * cfg.attention.cadence_s))
        views = [
            view
            for item, verdict in pairs
            if (view := evidence_view(item, verdict, coin_id, self._sources)) is not None
        ]
        disagreements = tuple(
            Disagreement(verdict.item_id, frozenset(c for c in (verdict.class_a, verdict.class_b) if c))
            for _item, verdict in pairs
            if verdict.status == "disagree"
        )
        market = self._market.state(coin_id, as_of)
        start = panic_time(views, cfg.modes)
        onchain = None
        if start is not None:
            # Everything recorded from the incident look-back before the headline up to `as_of`: a drain
            # just before the headline, or listed by a later capture, cancels the refutation.
            lookback = start - timedelta(hours=cfg.modes.incident_lookback_h)
            onchain = self._market.onchain(coin_id, lookback, as_of)
        decision = decide(
            ModeInputs(
                as_of=as_of,
                coin_id=coin_id,
                evidence=tuple(views),
                disagreements=disagreements,
                attention=attention,
                article_count=articles,
                market=market,
                onchain_around_panic=onchain,
            ),
            cfg.modes,
        )
        return NewsAssessment(
            coin_id=coin_id,
            as_of=as_of,
            p_model=decision.p_model,
            mode=decision.mode,
            abstain_reason=decision.abstain_reason,
            evidence=news_evidence(decision.evidence),
            rule_version=cfg.rule_version,
        )
