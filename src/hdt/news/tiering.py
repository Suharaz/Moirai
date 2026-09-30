"""Source tier computed by code (design contract section 10): never taken from an LLM.

The base tier is the exact host of the final URL after redirects looked up in `config/news_sources.yaml`
(`hdt.tools.impl.news.tier_for_url`, the same rule the `get_news` / `fetch_source` tools apply). Then:
- an item that IS a recorded exchange announcement (ingested from the Binance announcement API, on an
  official listing domain) is T0 whatever its class: it is the exchange's own statement;
- a registered event (listing, delist, exploit, unlock) confirmed by `check_official` is T0 evidence
  (Binance announcement, on-chain record, T0 unlock calendar), whatever outlet reported it;
- otherwise nothing is T0: a page on a T0 domain that was not recorded from the official channel (a
  Binance Square post, a fake "official" page reached through a redirect) is demoted to T2.
"""

from __future__ import annotations

from typing import Final

from hdt.contracts.common import Tier
from hdt.core.config import NewsSourcesFile
from hdt.news.classes import REGISTERED_CLASSES
from hdt.tools.impl.news import tier_for_url, url_domain

UNCONFIRMED_OFFICIAL_TIER: Final[Tier] = Tier.T2


def base_tier(url: str, sources: NewsSourcesFile) -> Tier:
    return tier_for_url(url, sources)[0]


def code_tier(
    final_url: str,
    sources: NewsSourcesFile,
    *,
    event_class: str | None,
    official: bool,
    recorded_announcement: bool = False,
) -> Tier:
    if recorded_announcement and domain_in(final_url, sources.official_listing_domains):
        return Tier.T0
    if event_class in REGISTERED_CLASSES and official:
        return Tier.T0
    tier = base_tier(final_url, sources)
    if tier is Tier.T0:
        return UNCONFIRMED_OFFICIAL_TIER
    return tier


def domain_in(url_or_domain: str, domains: tuple[str, ...] | frozenset[str]) -> bool:
    """Exact host match (a look-alike such as `www.binance.com.evil.io` never matches)."""
    host = url_domain(url_or_domain) if "://" in url_or_domain else url_or_domain.rstrip(".").lower()
    return host in {d.rstrip(".").lower() for d in domains}
