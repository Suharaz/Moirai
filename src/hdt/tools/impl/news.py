"""`get_news(coin, since, as_of)`: news items ingested for the event coin, point-in-time.

Items come from the phase 07 news index (`NewsIndex`); only items with `since <= ingested_at <= as_of`
are returned, and the tool checks that bound itself. The source tier of an item is computed by code from
its URL's domain against `config/news_sources.yaml` (unknown domains are T3 and flagged); any tier an
LLM might claim is irrelevant. Titles and summaries are sanitized and truncated.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import ClassVar, Final
from urllib.parse import urlsplit

from pydantic import Field, PositiveInt

from hdt.contracts.common import Tier, UtcDatetime
from hdt.core.config import NewsSourcesFile
from hdt.tools.base import (
    ShortText,
    Text,
    Tool,
    ToolArgs,
    ToolBackendError,
    ToolContext,
    ToolData,
    ToolInputError,
    ToolMode,
)
from hdt.tools.ports import NewsIndex

DEFAULT_SINCE: Final[timedelta] = timedelta(hours=24)
MAX_SINCE: Final[timedelta] = timedelta(days=7)
MAX_ITEMS: Final[int] = 30


def url_domain(url: str) -> str:
    host = urlsplit(url).hostname
    return (host or "").rstrip(".").lower()


def tier_for_url(url: str, sources: NewsSourcesFile) -> tuple[Tier, bool]:
    """(tier, known domain) from the exact host of `url`; unknown hosts get the default tier."""
    domain = url_domain(url)
    for tier, domains in sorted(sources.tiers.items()):
        if domain in domains:
            return Tier(tier), True
    return Tier(sources.default_tier), False


class NewsArgs(ToolArgs):
    coin_id: PositiveInt | None = Field(default=None, description="CMC id; only the event coin (default)")
    since: UtcDatetime | None = Field(default=None, description="oldest ingestion time; default 24 h back")
    as_of: UtcDatetime | None = Field(default=None, description="read time; not after the event time")
    limit: int = Field(default=20, ge=1, le=MAX_ITEMS)


class NewsEntry(ToolData):
    item_id: str
    title: ShortText
    source_name: ShortText
    domain: str
    tier: Tier
    domain_known: bool
    published_at: UtcDatetime | None
    ingested_at: UtcDatetime
    age_h: float = Field(description="hours between ingestion and as_of")
    summary: Text | None


class NewsData(ToolData):
    coin_id: int
    since: UtcDatetime
    as_of: UtcDatetime
    items: tuple[NewsEntry, ...]


class GetNewsTool(Tool[NewsArgs, NewsData]):
    name: ClassVar[str] = "get_news"
    description: ClassVar[str] = (
        "News items ingested for the event coin between `since` (default 24 h back, at most 7 days) and "
        "the event time, newest first, with the source tier computed from the domain. Cite items by "
        "item_id; read the original with fetch_source."
    )
    args_model = NewsArgs
    data_model = NewsData

    def __init__(self, mode: ToolMode, news: NewsIndex, sources: NewsSourcesFile) -> None:
        super().__init__(mode)
        self._news = news
        self._sources = sources

    async def run(self, ctx: ToolContext, args: NewsArgs) -> NewsData:
        coin_id = ctx.event_coin(args.coin_id)
        as_of = ctx.read_as_of(args.as_of)
        since = as_of - DEFAULT_SINCE if args.since is None else ctx.read_as_of(args.since)
        if since < as_of - MAX_SINCE:
            raise ToolInputError("since may reach back at most 7 days")
        items = await asyncio.to_thread(self._news.items, coin_id, since, as_of, args.limit)
        entries = []
        for item in items:
            if not since <= item.ingested_at <= as_of or coin_id not in item.coin_ids:
                raise ToolBackendError("news index returned an item outside the requested coin or time range")
            tier, known = tier_for_url(item.url, self._sources)
            entries.append(
                NewsEntry(
                    item_id=item.item_id,
                    title=item.title,
                    source_name=item.source_name,
                    domain=url_domain(item.url),
                    tier=tier,
                    domain_known=known,
                    published_at=item.published_at,
                    ingested_at=item.ingested_at,
                    age_h=_hours(as_of - item.ingested_at),
                    summary=item.summary,
                )
            )
        entries.sort(key=lambda e: (e.ingested_at, e.item_id), reverse=True)
        return NewsData(coin_id=coin_id, since=since, as_of=as_of, items=tuple(entries[: args.limit]))


def _hours(delta: timedelta) -> float:
    return round(delta.total_seconds() / 3600, 3)
