"""`check_official(exchange, keyword)`: is there an official announcement matching the keyword?

Announcements come from the phase 07 `OfficialSource` (recorded exchange announcements; Binance's are the
only T0 listing source). Only exchanges whose domains are listed in `official_listing_domains` of
`config/news_sources.yaml` can be checked, and a match counts only when the announcement URL is on one of
those domains. A keyword matches when all of its words appear, in order, as whole words of the title
(case-insensitive). Everything returned was recorded at or before the event `as_of`.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import ClassVar, Final

from pydantic import Field

from hdt.contracts.common import Tier, UtcDatetime
from hdt.core.config import NewsSourcesFile
from hdt.memory.dedupe import normalize_text
from hdt.tools.base import (
    ShortText,
    Tool,
    ToolArgs,
    ToolBackendError,
    ToolContext,
    ToolData,
    ToolInputError,
    ToolMode,
)
from hdt.tools.impl.news import url_domain
from hdt.tools.ports import OfficialSource

MAX_MATCHES: Final[int] = 10


def official_exchanges(sources: NewsSourcesFile) -> dict[str, frozenset[str]]:
    """Exchange name (registrable label, e.g. `binance`) -> its official domains."""
    out: dict[str, set[str]] = {}
    for domain in sources.official_listing_domains:
        labels = domain.lower().rstrip(".").split(".")
        name = labels[-2] if len(labels) >= 2 else labels[0]
        out.setdefault(name, set()).add(domain.lower().rstrip("."))
    return {name: frozenset(domains) for name, domains in out.items()}


def keyword_matches(keyword: str, title: str) -> bool:
    words, text = normalize_text(keyword).split(), normalize_text(title).split()
    if not words:
        return False
    return any(text[i : i + len(words)] == words for i in range(len(text) - len(words) + 1))


class CheckOfficialArgs(ToolArgs):
    exchange: str = Field(min_length=2, max_length=32, pattern=r"^[a-z0-9]+$", description="e.g. binance")
    keyword: str = Field(min_length=2, max_length=64, description="coin symbol, name or phrase")
    lookback_days: int = Field(default=30, ge=1, le=90)


class OfficialMatch(ToolData):
    title: ShortText
    url: str
    catalog: ShortText
    published_at: UtcDatetime
    recorded_at: UtcDatetime


class CheckOfficialData(ToolData):
    exchange: str
    keyword: str
    official: bool
    tier: Tier | None = Field(description="T0 when an official announcement matches")
    matches: tuple[OfficialMatch, ...]


class CheckOfficialTool(Tool[CheckOfficialArgs, CheckOfficialData]):
    name: ClassVar[str] = "check_official"
    description: ClassVar[str] = (
        "Check the exchange's recorded official announcements (e.g. binance) for a keyword over the last "
        "lookback_days before the event time. A match is T0 evidence."
    )
    args_model = CheckOfficialArgs
    data_model = CheckOfficialData

    def __init__(self, mode: ToolMode, official: OfficialSource, sources: NewsSourcesFile) -> None:
        super().__init__(mode)
        self._official = official
        self._exchanges = official_exchanges(sources)

    async def run(self, ctx: ToolContext, args: CheckOfficialArgs) -> CheckOfficialData:
        domains = self._exchanges.get(args.exchange)
        if domains is None:
            raise ToolInputError(f"no official source for {args.exchange}; known: {sorted(self._exchanges)}")
        since = ctx.as_of - timedelta(days=args.lookback_days)
        announcements = await asyncio.to_thread(self._official.announcements, args.exchange, since, ctx.as_of)
        matches = []
        for announcement in announcements:
            if announcement.recorded_at > ctx.as_of or announcement.exchange != args.exchange:
                raise ToolBackendError("official source returned an announcement recorded after as_of")
            if url_domain(announcement.url) not in domains or announcement.published_at < since:
                continue
            if keyword_matches(args.keyword, announcement.title):
                matches.append(
                    OfficialMatch(
                        title=announcement.title,
                        url=announcement.url,
                        catalog=announcement.catalog or "unspecified",
                        published_at=announcement.published_at,
                        recorded_at=announcement.recorded_at,
                    )
                )
        matches.sort(key=lambda m: (m.published_at, m.url), reverse=True)
        return CheckOfficialData(
            exchange=args.exchange,
            keyword=args.keyword,
            official=bool(matches),
            tier=Tier.T0 if matches else None,
            matches=tuple(matches[:MAX_MATCHES]),
        )
