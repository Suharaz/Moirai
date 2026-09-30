"""Config-driven news source adapters (phase 07 step 1: the feed and unlock sources are not selected yet).

- RSS 2.0 / Atom feeds (`feeds` in config/news.yaml);
- a licensed JSON news API (`news_api`; key from vault scope `data`, secret `news_feed`);
- Binance announcements (the CMS article list of the listing / delisting catalogs): the only T0 listing
  source, recorded both as news items and as official announcements;
- an unlock calendar published as JSON (`unlock_calendar`).

Parsing is pure (bytes in, records out) so every adapter is tested on fixtures. XML is parsed with a DTD
refused outright (no entity expansion, no external entities). HTTP goes through `SourceHttp`: the egress
proxy from the environment, a response size bound and at most 3 redirects.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any, Final, Literal
from xml.etree import ElementTree as ET

import httpx

from hdt.core.ids import sha256_hex
from hdt.news.coins import CoinDirectory
from hdt.news.config import (
    BINANCE_SOURCE_KEY,
    BinanceAnnouncementsConfig,
    FeedConfig,
    NewsApiConfig,
    UnlockCalendarConfig,
)
from hdt.news.text import plain_text
from hdt.tools.ports import HTTP_URL_PATTERN, Announcement

SourceKind = Literal["rss", "api", "binance"]
MAX_REDIRECTS: Final[int] = 3
TITLE_MAX: Final[int] = 300
SUMMARY_MAX: Final[int] = 2000
_URL: Final = re.compile(HTTP_URL_PATTERN)
_SPACES: Final = re.compile(r"\s+")


class SourceError(RuntimeError):
    """A source answered with something unusable (HTTP error, oversized, unparseable)."""


@dataclass(frozen=True)
class RawItem:
    source_key: str
    source_kind: SourceKind
    source_name: str
    external_id: str
    title: str
    url: str
    published_at: datetime | None
    summary: str | None
    content: str | None
    symbols: tuple[str, ...] = ()
    """Symbols the source itself tags the item with (licensed APIs); empty for feeds."""
    coin_ids: tuple[int, ...] = ()
    """CMC ids the source itself tags the item with."""

    @property
    def item_id(self) -> str:
        return f"{self.source_key}:{sha256_hex(self.external_id)[:20]}"


@dataclass(frozen=True)
class RawUnlock:
    coin_id: int
    unlock_at: datetime
    amount_tokens: float | None
    pct_of_circulating: float | None
    category: str


# --------------------------------------------------------------------------- HTTP


class SourceHttp:
    def __init__(
        self,
        *,
        timeout_s: float,
        max_bytes: int,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._max = max_bytes
        self._client = httpx.AsyncClient(
            timeout=timeout_s,
            follow_redirects=True,
            max_redirects=MAX_REDIRECTS,
            transport=transport,
            headers={"User-Agent": "hdt-news/1"},
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def get(
        self, url: str, *, params: Mapping[str, str] | None = None, headers: Mapping[str, str] | None = None
    ) -> bytes:
        try:
            async with self._client.stream("GET", url, params=params, headers=headers) as response:
                if response.status_code != 200:
                    raise SourceError(f"HTTP {response.status_code}")
                chunks: list[bytes] = []
                total = 0
                async for chunk in response.aiter_bytes():
                    total += len(chunk)
                    if total > self._max:
                        raise SourceError(f"response larger than {self._max} bytes")
                    chunks.append(chunk)
                return b"".join(chunks)
        except httpx.HTTPError as exc:
            raise SourceError(f"{type(exc).__name__}") from exc


# --------------------------------------------------------------------------- helpers


def _clean(value: str | None, max_chars: int) -> str | None:
    return plain_text(value, max_chars)


def _title(value: str | None) -> str | None:
    text = _clean(value, TITLE_MAX)
    return _SPACES.sub(" ", text).strip() if text else None


def parse_time(value: Any) -> datetime | None:
    """ISO 8601, RFC 822 (RSS) or epoch seconds / milliseconds; None when absent or unparseable."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        try:
            seconds = value / 1000 if value > 1e11 else value
            return datetime.fromtimestamp(seconds, tz=UTC)
        except (OverflowError, OSError, ValueError):
            return None
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        return parse_time(int(text)) if len(text) <= 20 else None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = parsedate_to_datetime(text)
        except (TypeError, ValueError, IndexError, OverflowError):
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    try:
        # A time at the edge of the calendar (9999-12-31T23:59:59-01:00) overflows on conversion.
        return parsed.astimezone(UTC)
    except (OverflowError, ValueError):
        return None


def _path(value: Any, path: Sequence[str]) -> Any:
    for key in path:
        if not isinstance(value, Mapping):
            return None
        value = value.get(key)
    return value


def _field(item: Mapping[str, Any], name: str | None) -> Any:
    if name is None:
        return None
    return _path(item, name.split("."))


def _valid_url(url: str | None) -> str | None:
    if url is None:
        return None
    url = url.strip()
    return url if _URL.match(url) else None


# --------------------------------------------------------------------------- XML feeds


def _refuse_dtd(*_args: object) -> None:
    raise SourceError("feed declares a DTD (refused)")


def parse_xml(data: bytes) -> ET.Element:
    parser = ET.XMLParser()  # noqa: S314 (DTD refused below: no entity expansion is possible)
    expat = parser.parser
    expat.StartDoctypeDeclHandler = _refuse_dtd
    expat.EntityDeclHandler = _refuse_dtd
    try:
        parser.feed(data)
        return parser.close()
    except ET.ParseError as exc:
        raise SourceError(f"feed is not well-formed XML: {exc}") from exc


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def _child(element: ET.Element, name: str) -> ET.Element | None:
    for child in element:
        if _local(child.tag) == name:
            return child
    return None


def _child_text(element: ET.Element, *names: str) -> str | None:
    for name in names:
        child = _child(element, name)
        if child is not None:
            text = "".join(child.itertext()).strip()
            if text:
                return text
    return None


def _atom_link(entry: ET.Element) -> str | None:
    fallback = None
    for child in entry:
        if _local(child.tag) != "link":
            continue
        href = child.get("href")
        rel = child.get("rel", "alternate")
        if href and rel == "alternate":
            return href
        fallback = fallback or href
    return fallback


def parse_feed(data: bytes, feed: FeedConfig, *, max_entries: int, content_max: int) -> list[RawItem]:
    """RSS 2.0 `channel/item` or Atom `feed/entry` entries, newest first as the feed lists them."""
    root = parse_xml(data)
    kind = _local(root.tag)
    if kind == "rss":
        channel = _child(root, "channel")
        entries = [e for e in (channel if channel is not None else []) if _local(e.tag) == "item"]
    elif kind == "feed":
        entries = [e for e in root if _local(e.tag) == "entry"]
    elif kind == "RDF":
        entries = [e for e in root if _local(e.tag) == "item"]
    else:
        raise SourceError(f"unknown feed root <{kind}>")
    items: list[RawItem] = []
    for entry in entries[:max_entries]:
        title = _title(_child_text(entry, "title"))
        if kind == "feed":
            link = _atom_link(entry)
            published = _child_text(entry, "published", "updated")
            summary = _child_text(entry, "summary")
            content = _child_text(entry, "content")
            guid = _child_text(entry, "id")
        else:
            link = _child_text(entry, "link")
            published = _child_text(entry, "pubDate", "date")
            summary = _child_text(entry, "description")
            content = _child_text(entry, "encoded")
            guid = _child_text(entry, "guid")
        url = _valid_url(link)
        if title is None or url is None:
            continue
        items.append(
            RawItem(
                source_key=feed.key,
                source_kind="rss",
                source_name=feed.source_name,
                external_id=guid or url,
                title=title,
                url=url,
                published_at=parse_time(published),
                summary=_clean(summary, SUMMARY_MAX),
                content=_clean(content, content_max),
            )
        )
    return items


# --------------------------------------------------------------------------- licensed JSON news API


def parse_news_api(data: bytes, api: NewsApiConfig, *, max_entries: int, content_max: int) -> list[RawItem]:
    try:
        body = json.loads(data)
    except ValueError as exc:
        raise SourceError("news API answered with invalid JSON") from exc
    raw = _path(body, api.items_path)
    if not isinstance(raw, list):
        raise SourceError(f"news API items not found at {'.'.join(api.items_path)}")
    items: list[RawItem] = []
    for entry in raw[:max_entries]:
        if not isinstance(entry, Mapping):
            continue
        title = _title(_str(_field(entry, api.fields.title)))
        url = _valid_url(_str(_field(entry, api.fields.url)))
        ident = _str(_field(entry, api.fields.id)) or url
        if title is None or url is None or ident is None:
            continue
        symbols_raw = _field(entry, api.fields.symbols)
        symbols: tuple[str, ...] = ()
        if isinstance(symbols_raw, list):
            symbols = tuple(str(s).strip().upper() for s in symbols_raw if isinstance(s, (str, int)))
        elif isinstance(symbols_raw, str):
            symbols = tuple(s.strip().upper() for s in symbols_raw.split(",") if s.strip())
        items.append(
            RawItem(
                source_key=api.key,
                source_kind="api",
                source_name=api.source_name,
                external_id=ident,
                title=title,
                url=url,
                published_at=parse_time(_field(entry, api.fields.published_at)),
                summary=_clean(_str(_field(entry, api.fields.summary)), SUMMARY_MAX),
                content=_clean(_str(_field(entry, api.fields.content)), content_max),
                symbols=symbols,
            )
        )
    return items


def _str(value: Any) -> str | None:
    if value is None or isinstance(value, (dict, list, bool)):
        return None
    text = str(value).strip()
    return text or None


def _float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number and abs(number) != float("inf") and number >= 0 else None


# --------------------------------------------------------------------------- Binance announcements


@dataclass(frozen=True)
class BinanceArticle:
    announcement: Announcement
    announcement_id: str
    item: RawItem


def binance_query(cfg: BinanceAnnouncementsConfig, catalog_id: int) -> dict[str, str]:
    return {"type": "1", "catalogId": str(catalog_id), "pageNo": "1", "pageSize": str(cfg.page_size)}


def parse_binance(
    data: bytes, cfg: BinanceAnnouncementsConfig, catalog_id: int, *, recorded_at: datetime
) -> list[BinanceArticle]:
    """Articles of one catalog of the Binance CMS list (`data.catalogs[].articles` or `data.articles`)."""
    try:
        body = json.loads(data)
    except ValueError as exc:
        raise SourceError("Binance announcements answered with invalid JSON") from exc
    payload = body.get("data") if isinstance(body, Mapping) else None
    if not isinstance(payload, Mapping):
        raise SourceError("Binance announcements: no data object")
    articles: list[Any] = []
    catalogs = payload.get("catalogs")
    if isinstance(catalogs, list):
        for catalog in catalogs:
            if isinstance(catalog, Mapping) and catalog.get("catalogId") in (catalog_id, str(catalog_id)):
                articles.extend(catalog.get("articles") or [])
    elif isinstance(payload.get("articles"), list):
        articles.extend(payload["articles"])
    kind = cfg.catalogs[catalog_id]
    out: list[BinanceArticle] = []
    for article in articles:
        if not isinstance(article, Mapping):
            continue
        code = _str(article.get("code"))
        title = _title(_str(article.get("title")))
        published = parse_time(article.get("releaseDate") or article.get("publishDate"))
        ident = _str(article.get("id")) or code
        if code is None or title is None or published is None or ident is None:
            continue
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", code):
            continue
        url = cfg.article_url.format(code=code)
        out.append(
            BinanceArticle(
                announcement=Announcement(
                    exchange="binance",
                    title=title,
                    url=url,
                    catalog=kind,
                    published_at=published,
                    recorded_at=recorded_at,
                ),
                announcement_id=ident,
                item=RawItem(
                    source_key=BINANCE_SOURCE_KEY,
                    source_kind="binance",
                    source_name="Binance announcements",
                    external_id=f"binance:{ident}",
                    title=title,
                    url=url,
                    published_at=published,
                    summary=None,
                    content=None,
                ),
            )
        )
    return out


# --------------------------------------------------------------------------- unlock calendar


def parse_unlocks(data: bytes, cal: UnlockCalendarConfig, coins: CoinDirectory) -> list[RawUnlock]:
    """Entries of the configured calendar mapped to universe coins (unmapped entries are dropped)."""
    try:
        body = json.loads(data)
    except ValueError as exc:
        raise SourceError("unlock calendar answered with invalid JSON") from exc
    raw = _path(body, cal.items_path)
    if not isinstance(raw, list):
        raise SourceError(f"unlock entries not found at {'.'.join(cal.items_path)}")
    out: dict[tuple[int, datetime, str], RawUnlock] = {}
    for entry in raw:
        if not isinstance(entry, Mapping):
            continue
        unlock_at = parse_time(_field(entry, cal.fields.unlock_at))
        if unlock_at is None:
            continue
        coin_ids: tuple[int, ...] = ()
        raw_id = _field(entry, cal.fields.coin_id)
        if isinstance(raw_id, int) and not isinstance(raw_id, bool) and coins.get(raw_id) is not None:
            coin_ids = (raw_id,)
        elif cal.fields.symbol is not None:
            symbol = _str(_field(entry, cal.fields.symbol))
            coin_ids = coins.by_symbol(symbol) if symbol else ()
        if len(coin_ids) != 1:
            continue  # unknown or ambiguous symbol: never guess
        category = (_str(_field(entry, cal.fields.category)) or "unspecified")[:80]
        unlock = RawUnlock(
            coin_id=coin_ids[0],
            unlock_at=unlock_at,
            amount_tokens=_float(_field(entry, cal.fields.amount_tokens)),
            pct_of_circulating=_float(_field(entry, cal.fields.pct_of_circulating)),
            category=category,
        )
        out[(unlock.coin_id, unlock.unlock_at, unlock.category)] = unlock
    return list(out.values())
