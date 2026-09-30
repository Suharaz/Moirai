"""Ingestion of the configured news sources into `news_items`, `news_announcements` and `news_unlocks`.

Per poll and source: download (egress proxy, size bound) -> parse -> map to universe coins -> exact URL
dedupe (`url_key`) -> title / MinHash dedupe against the recent window -> insert-only rows, and the
source's health in `news_source_state`, all in one transaction per source. Items that map to no coin of
the point-in-time universe are dropped (they cannot be evidence about a tradable coin); items published
before the processing window are not ingested as news (Binance announcements are still recorded as
announcements: `check_official` looks 14 days back).
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Final

import sqlalchemy as sa

from hdt.core.clock import ensure_utc, utcnow
from hdt.core.config import NewsSourcesFile
from hdt.core.ids import canonical_sha256
from hdt.memory.dedupe import fingerprint
from hdt.news.coins import CoinDirectory
from hdt.news.config import BINANCE_SOURCE_KEY, NewsFile
from hdt.news.dedupe import IngestDeduper
from hdt.news.metrics import INGESTED, SOURCE_ERRORS, SOURCE_LAST_SUCCESS
from hdt.news.sources import (
    BinanceArticle,
    RawItem,
    SourceError,
    SourceHttp,
    binance_query,
    parse_binance,
    parse_feed,
    parse_news_api,
    parse_unlocks,
)
from hdt.news.store import (
    ANNOUNCEMENTS,
    ITEMS,
    insert_rows,
    known_url_keys,
    recent_items,
    record_source_state,
)
from hdt.news.text import url_key
from hdt.news.tiering import code_tier
from hdt.news.unlocks import record_unlocks
from hdt.tools.impl.news import url_domain

log = logging.getLogger(__name__)

UNLOCK_POLL: Final[timedelta] = timedelta(hours=1)
MATCH_TEXT_CHARS: Final[int] = 4000

ApiKey = Callable[[], str]


@dataclass
class SourceResult:
    source_key: str
    ok: bool
    stored: int = 0
    duplicates: int = 0
    unmapped: int = 0
    stale: int = 0
    announcements: int = 0
    error: str | None = None


@dataclass
class IngestCounts:
    results: list[SourceResult] = field(default_factory=list)

    @property
    def failed(self) -> list[str]:
        return [r.source_key for r in self.results if not r.ok]


def map_coins(raw: RawItem, coins: CoinDirectory) -> tuple[int, ...]:
    """Coins of the item: the source's own tags when present, else self-mapping by symbol + name."""
    tagged = {c for c in raw.coin_ids if coins.get(c) is not None}
    for symbol in raw.symbols:
        found = coins.by_symbol(symbol)
        if len(found) == 1:
            tagged.add(found[0])
    if tagged or raw.symbols or raw.coin_ids:
        return tuple(sorted(tagged))
    if raw.source_kind == "binance":
        return coins.match(raw.title)
    text = "\n".join(p for p in (raw.title, raw.summary, (raw.content or "")[:MATCH_TEXT_CHARS]) if p)
    return coins.match(text)


def item_row(
    raw: RawItem,
    coin_ids: Sequence[int],
    *,
    ingested_at: datetime,
    sources: NewsSourcesFile,
    duplicate_of: str | None,
    duplicate_kind: str | None,
    similarity: float | None,
) -> dict[str, Any]:
    return {
        "item_id": raw.item_id,
        "source_key": raw.source_key,
        "source_kind": raw.source_kind,
        "source_name": raw.source_name[:80],
        "url": raw.url,
        "url_key": url_key(raw.url),
        "domain": url_domain(raw.url),
        "tier": code_tier(
            raw.url,
            sources,
            event_class=None,
            official=False,
            recorded_announcement=raw.source_kind == "binance",
        ).value,
        "title": raw.title,
        "summary": raw.summary,
        "content": raw.content,
        "published_at": raw.published_at,
        "ingested_at": ingested_at,
        "coin_ids": list(coin_ids),
        "title_fingerprint": fingerprint(raw.title),
        "duplicate_of": duplicate_of,
        "duplicate_kind": duplicate_kind,
        "similarity": similarity,
        "raw_sha256": canonical_sha256(
            {
                "source_key": raw.source_key,
                "external_id": raw.external_id,
                "title": raw.title,
                "url": raw.url,
                "published_at": raw.published_at.isoformat() if raw.published_at else None,
                "summary": raw.summary,
                "content": raw.content,
            }
        ),
    }


def store_items(
    conn: sa.Connection,
    raws: Sequence[RawItem],
    coins: CoinDirectory,
    *,
    now: datetime,
    config: NewsFile,
    sources: NewsSourcesFile,
    result: SourceResult,
) -> None:
    """Map, dedupe and insert `raws` (oldest first, so the first copy of a story is the canonical one)."""
    oldest = now - timedelta(hours=config.ingest.process_lookback_h)
    candidates: list[tuple[RawItem, tuple[int, ...]]] = []
    for raw in raws:
        if raw.published_at is not None and raw.published_at < oldest:
            result.stale += 1
            continue
        coin_ids = map_coins(raw, coins)
        if not coin_ids:
            result.unmapped += 1
            continue
        candidates.append((raw, coin_ids))
    if not candidates:
        return
    seen_urls = known_url_keys(conn, [url_key(raw.url) for raw, _ in candidates])
    window = now - timedelta(hours=config.ingest.near_duplicate_window_h)
    deduper = IngestDeduper(recent_items(conn, window), config.ingest.near_duplicate_jaccard)
    rows: list[dict[str, Any]] = []
    ordered = sorted(candidates, key=lambda c: (c[0].published_at or now, c[0].item_id))
    for raw, coin_ids in ordered:
        key = url_key(raw.url)
        if key in seen_urls:
            continue
        seen_urls.add(key)
        mark = deduper.check(coin_ids, raw.title, raw.summary)
        deduper.add(raw.item_id, coin_ids, raw.title, raw.summary, mark.duplicate_of)
        if mark.duplicate_of is not None:
            result.duplicates += 1
        rows.append(
            item_row(
                raw,
                coin_ids,
                ingested_at=now,
                sources=sources,
                duplicate_of=mark.duplicate_of,
                duplicate_kind=mark.kind,
                similarity=mark.similarity,
            )
        )
    stored = insert_rows(conn, ITEMS, rows)
    result.stored += stored
    if stored:
        INGESTED.labels(source=result.source_key, kind=raws[0].source_kind).inc(stored)


def announcement_rows(articles: Sequence[BinanceArticle]) -> list[dict[str, Any]]:
    return [
        {
            "exchange": a.announcement.exchange,
            "announcement_id": a.announcement_id,
            "catalog": a.announcement.catalog,
            "title": a.announcement.title,
            "url": a.announcement.url,
            "published_at": a.announcement.published_at,
            "recorded_at": a.announcement.recorded_at,
        }
        for a in articles
    ]


class Ingestor:
    def __init__(
        self,
        engine: sa.Engine,
        http: SourceHttp,
        *,
        config: NewsFile,
        sources: NewsSourcesFile,
        api_key: ApiKey | None = None,
        clock: Callable[[], datetime] = utcnow,
    ) -> None:
        self._engine = engine
        self._http = http
        self._config = config
        self._sources = sources
        self._api_key = api_key
        self._clock = clock
        self._last_unlock_poll: datetime | None = None

    async def poll(self, coins: CoinDirectory) -> IngestCounts:
        counts = IngestCounts()
        cfg = self._config
        for feed in cfg.feeds:
            if feed.enabled:
                counts.results.append(await self._feed(feed.key, feed.url, coins))
        if cfg.news_api.enabled:
            counts.results.append(await self._news_api(coins))
        if cfg.binance_announcements.enabled:
            counts.results.append(await self._binance(coins))
        now = self._clock()
        if cfg.unlock_calendar.enabled and (
            self._last_unlock_poll is None or now - self._last_unlock_poll >= UNLOCK_POLL
        ):
            counts.results.append(await self._unlocks(coins))
            self._last_unlock_poll = now
        return counts

    # ------------------------------------------------------------------ sources

    async def _feed(self, key: str, url: str, coins: CoinDirectory) -> SourceResult:
        feed = next(f for f in self._config.feeds if f.key == key)
        ingest = self._config.ingest
        try:
            data = await self._http.get(url)
            raws = parse_feed(
                data, feed, max_entries=ingest.max_entries_per_source, content_max=ingest.content_max_chars
            )
        except SourceError as exc:
            return self._failed(key, str(exc))
        return self._store(key, raws, coins)

    async def _news_api(self, coins: CoinDirectory) -> SourceResult:
        api = self._config.news_api
        ingest = self._config.ingest
        if api.url is None or self._api_key is None:
            return self._failed(api.key, "news API has no URL or no key loader")
        try:
            key = self._api_key()
        except Exception as exc:  # vault errors: missing, integrity, key file
            return self._failed(api.key, f"news_feed secret unavailable: {type(exc).__name__}")
        try:
            data = await self._http.get(api.url, params=api.query, headers={api.auth_header: key})
            raws = parse_news_api(
                data, api, max_entries=ingest.max_entries_per_source, content_max=ingest.content_max_chars
            )
        except SourceError as exc:
            return self._failed(api.key, str(exc))
        return self._store(api.key, raws, coins)

    async def _binance(self, coins: CoinDirectory) -> SourceResult:
        cfg = self._config.binance_announcements
        articles: list[BinanceArticle] = []
        try:
            for catalog_id in sorted(cfg.catalogs):
                data = await self._http.get(cfg.url, params=binance_query(cfg, catalog_id))
                parsed = parse_binance(data, cfg, catalog_id, recorded_at=self._clock())
                if not parsed:
                    raise SourceError(f"catalog {catalog_id} parsed to no articles (page structure changed?)")
                articles.extend(parsed)
        except SourceError as exc:
            return self._failed(BINANCE_SOURCE_KEY, str(exc))
        now = self._clock()
        result = SourceResult(BINANCE_SOURCE_KEY, ok=True)
        with self._engine.begin() as conn:
            result.announcements = insert_rows(conn, ANNOUNCEMENTS, announcement_rows(articles))
            store_items(
                conn,
                [a.item for a in articles],
                coins,
                now=now,
                config=self._config,
                sources=self._sources,
                result=result,
            )
            record_source_state(conn, BINANCE_SOURCE_KEY, at=now, ok=True, count=len(articles), error=None)
        SOURCE_LAST_SUCCESS.labels(source=BINANCE_SOURCE_KEY).set(now.timestamp())
        return result

    async def _unlocks(self, coins: CoinDirectory) -> SourceResult:
        cal = self._config.unlock_calendar
        if cal.url is None:
            return self._failed(cal.key, "unlock calendar has no URL")
        headers: dict[str, str] = {}
        if cal.auth == "news_feed":
            if self._api_key is None:
                return self._failed(cal.key, "unlock calendar needs the news_feed key")
            try:
                headers[cal.auth_header] = self._api_key()
            except Exception as exc:  # vault errors: missing, integrity, key file
                return self._failed(cal.key, f"news_feed secret unavailable: {type(exc).__name__}")
        try:
            data = await self._http.get(cal.url, headers=headers)
            entries = parse_unlocks(data, cal, coins)
        except SourceError as exc:
            return self._failed(cal.key, str(exc))
        now = self._clock()
        result = SourceResult(cal.key, ok=True)
        with self._engine.begin() as conn:
            result.stored = record_unlocks(conn, cal.key, cal.tier, entries, recorded_at=now)
            record_source_state(conn, cal.key, at=now, ok=True, count=len(entries), error=None)
        SOURCE_LAST_SUCCESS.labels(source=cal.key).set(now.timestamp())
        return result

    # ------------------------------------------------------------------ helpers

    def _store(self, key: str, raws: Sequence[RawItem], coins: CoinDirectory) -> SourceResult:
        now = self._clock()
        result = SourceResult(key, ok=True)
        with self._engine.begin() as conn:
            store_items(conn, raws, coins, now=now, config=self._config, sources=self._sources, result=result)
            record_source_state(conn, key, at=now, ok=True, count=len(raws), error=None)
        SOURCE_LAST_SUCCESS.labels(source=key).set(now.timestamp())
        return result

    def _failed(self, key: str, error: str) -> SourceResult:
        SOURCE_ERRORS.labels(source=key).inc()
        log.warning("news source poll failed", extra={"source": key, "error": error})
        with self._engine.begin() as conn:
            record_source_state(conn, key, at=ensure_utc(self._clock()), ok=False, count=0, error=error)
        return SourceResult(key, ok=False, error=error)
