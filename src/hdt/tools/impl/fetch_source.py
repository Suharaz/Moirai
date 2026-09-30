"""`fetch_source(item_id)`: the original article of a news item already ingested for this event.

The URL is never an argument: it is the ingested item's URL, and the item must map to the event coin and
have been ingested at or before the event `as_of`. The article is looked up in the raw store in this order:
1. a capture of the item recorded at or before `as_of` (source `news`, route `fetch_source`,
   key = `item_id`), for example fetched by an earlier event;
2. the capture this event's live meeting created, only when it is listed in the event's pinned capture
   manifest (`ToolContext.pinned`, from `AgentForecast.capture_manifest` on the decision card): exactly that
   record is opened (read at its own `fetched_at`, the pinned cutoff), its sha256 must match and its params
   must name this `event_id` and `item_id`, so a manifest entry cannot open another event's capture.
   Nothing else recorded after `as_of` is ever visible, even for the same `event_id`; without a manifest
   entry the replay returns `not_available`.
   The live tool remembers the captures it made during the meeting, so a repeated call inside the same
   meeting returns the same copy without fetching again.
Only the live tool then calls the phase 07 sandboxed fetcher (never the network from this process). It
checks the fetcher's answer (sha256 of the bytes, requested URL, at most 3 redirects, final URL adds no
query parameter absent from the original URL), writes the verbatim bytes to the raw store through the
phase 02 `RawStore.append` (the single write path) and only then returns. The replay tool returns
`not_available` when neither capture exists; it has no fetcher and no raw-store writer.

The tier is computed by code from the final domain after redirects (`config/news_sources.yaml`); an
unknown domain is T3 and flagged. HTML is reduced to visible text (scripts, styles and similar dropped),
sanitized and truncated to 8k characters before it can enter a prompt.
"""

from __future__ import annotations

import asyncio
import re
from collections import OrderedDict
from datetime import timedelta
from html.parser import HTMLParser
from typing import ClassVar, Final
from urllib.parse import parse_qsl, urlsplit

from pydantic import Field

from hdt.contracts.common import Tier
from hdt.contracts.forecast import LakeRef
from hdt.core.clock import utcnow
from hdt.core.config import NewsSourcesFile
from hdt.core.ids import sha256_hex
from hdt.lake.pit_query import PitQuery
from hdt.lake.raw_store import RawStore
from hdt.lake.schemas import Capture, RawRecord
from hdt.ops.metrics import FETCH_BLOCKED
from hdt.tools.base import (
    ArticleText,
    NotAvailableError,
    Tool,
    ToolArgs,
    ToolBackendError,
    ToolContext,
    ToolData,
    ToolInputError,
    ToolMode,
)
from hdt.tools.impl.news import tier_for_url, url_domain
from hdt.tools.pit import PitView, record_params
from hdt.tools.ports import ITEM_ID_PATTERN, FetchedDocument, FetcherClient, NewsIndex, NewsItem

SOURCE: Final = "news"
ROUTE: Final[str] = "fetch_source"
MEETING_MEMORY: Final[int] = 1024  # (event_id, item_id) captures the live tool remembers
CAPTURE_LOOKBACK: Final[timedelta] = timedelta(days=8)
MAX_REDIRECTS: Final[int] = 3
_TEXT_TYPES: Final = ("text/html", "application/xhtml+xml", "text/plain", "application/json", "text/xml")
_SKIPPED_TAGS: Final = frozenset(
    {"script", "style", "noscript", "template", "svg", "iframe", "head", "object"}
)
_BLOCK_TAGS: Final = frozenset(
    {
        "p",
        "div",
        "br",
        "li",
        "ul",
        "ol",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "tr",
        "table",
        "section",
        "article",
        "header",
        "footer",
        "blockquote",
        "pre",
        "hr",
    }
)
_CHARSET: Final = re.compile(r"charset=([\w.:-]+)", re.IGNORECASE)


class FetchSourceArgs(ToolArgs):
    item_id: str = Field(pattern=ITEM_ID_PATTERN, description="item_id of a news item from get_news")


class FetchSourceData(ToolData):
    item_id: str
    url: str
    final_url: str
    domain: str
    tier: Tier
    domain_known: bool
    redirects: int
    http_status: int
    content_type: str
    body_sha256: str
    text: ArticleText
    source: LakeRef


class _TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._skip = 0
        self.parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in _SKIPPED_TAGS:
            self._skip += 1
        elif tag in _BLOCK_TAGS:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag in _SKIPPED_TAGS:
            self._skip = max(0, self._skip - 1)
        elif tag in _BLOCK_TAGS:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if not self._skip:
            self.parts.append(data)


def document_text(body: bytes, content_type: str) -> str:
    """Visible text of a fetched document; raises `ToolBackendError` for non-text content."""
    media = content_type.split(";", 1)[0].strip().lower()
    if media and media not in _TEXT_TYPES:
        raise ToolBackendError(f"unsupported content type {media}")
    match = _CHARSET.search(content_type)
    encoding = match.group(1) if match else "utf-8"
    try:
        text = body.decode(encoding, errors="replace")
    except LookupError:
        text = body.decode("utf-8", errors="replace")
    if media in ("text/html", "application/xhtml+xml") or (not media and "<html" in text[:2048].lower()):
        parser = _TextExtractor()
        parser.feed(text)
        parser.close()
        text = "".join(parser.parts)
    return text


class FetchSourceReplayTool(Tool[FetchSourceArgs, FetchSourceData]):
    """Raw-store lookups only (the replay implementation, and the first steps of the live one)."""

    name: ClassVar[str] = "fetch_source"
    description: ClassVar[str] = (
        "Original text of a news item already ingested for this event (by item_id from get_news), with "
        "the tier of its final domain. Nothing else can be fetched."
    )
    args_model = FetchSourceArgs
    data_model = FetchSourceData
    modes: ClassVar[frozenset[ToolMode]] = frozenset({"replay"})

    def __init__(self, mode: ToolMode, pit: PitQuery, news: NewsIndex, sources: NewsSourcesFile) -> None:
        super().__init__(mode)
        self._pit = pit
        self._news = news
        self._sources = sources

    async def run(self, ctx: ToolContext, args: FetchSourceArgs) -> FetchSourceData:
        item = await asyncio.to_thread(self._event_item, ctx, args.item_id)
        record = await asyncio.to_thread(self._recorded, ctx, item)
        if record is None:
            raise NotAvailableError(f"no recorded copy of item {item.item_id} for this event")
        return self._data(item, record)

    def _event_item(self, ctx: ToolContext, item_id: str) -> NewsItem:
        item = self._news.item(item_id, ctx.as_of)
        if item is None:
            raise ToolInputError(f"item {item_id} was not ingested at or before the event time")
        if item.item_id != item_id or item.ingested_at > ctx.as_of:
            raise ToolBackendError("news index returned another item or one ingested after as_of")
        if ctx.coin_id not in item.coin_ids:
            raise ToolInputError(f"item {item_id} is not news of the event coin")
        return item

    def _recorded(self, ctx: ToolContext, item: NewsItem) -> RawRecord | None:
        prior = PitView(self._pit, ctx.as_of).latest(
            SOURCE, ROUTE, key=item.item_id, lookback=CAPTURE_LOOKBACK
        )
        if prior is not None:
            return prior
        ref = ctx.pinned_capture(SOURCE, ROUTE, item.item_id)
        return None if ref is None else self._pinned(ctx, item, ref)

    def _pinned(self, ctx: ToolContext, item: NewsItem, ref: LakeRef) -> RawRecord:
        """Exactly the manifest's record: read at its own `fetched_at` (the pinned cutoff), sha256 checked,
        and created by this event's meeting for this item (another event's capture is never opened)."""
        cutoff = ref.fetched_at
        foreign = False
        for record in self._pit.series(SOURCE, ROUTE, cutoff, cutoff, as_of=cutoff, key=ref.key):
            if record.body_sha256 != ref.body_sha256 or record.http_status != ref.http_status:
                continue
            params = record_params(record)
            if params.get("event_id") == ctx.event_id and params.get("item_id") == item.item_id:
                return record
            foreign = True
        if foreign:
            raise ToolBackendError(
                f"pinned capture of {ref.key} at {cutoff.isoformat()} belongs to another event or item"
            )
        raise ToolBackendError(f"pinned capture of {ref.key} at {cutoff.isoformat()} is missing or altered")

    def _data(self, item: NewsItem, record: RawRecord) -> FetchSourceData:
        params = record_params(record)
        final_url = str(params.get("final_url") or item.url)
        chain = params.get("redirect_chain")
        content_type = str(params.get("content_type") or "")
        tier, known = tier_for_url(final_url, self._sources)
        return FetchSourceData(
            item_id=item.item_id,
            url=item.url,
            final_url=final_url,
            domain=url_domain(final_url),
            tier=tier,
            domain_known=known,
            redirects=len(chain) if isinstance(chain, list) else 0,
            http_status=record.http_status,
            content_type=content_type,
            body_sha256=record.body_sha256,
            text=document_text(record.body(), content_type) if record.http_status == 200 else "",
            source=LakeRef.of(record),
        )


class FetchSourceLiveTool(FetchSourceReplayTool):
    """Raw-store lookups first, then the sandboxed fetcher; the capture is stored before returning."""

    modes: ClassVar[frozenset[ToolMode]] = frozenset({"live"})

    def __init__(
        self,
        mode: ToolMode,
        pit: PitQuery,
        news: NewsIndex,
        sources: NewsSourcesFile,
        *,
        fetcher: FetcherClient,
        raw_store: RawStore,
    ) -> None:
        super().__init__(mode, pit, news, sources)
        self._fetcher = fetcher
        self._raw_store = raw_store
        self._meeting: OrderedDict[tuple[str, str], RawRecord] = OrderedDict()

    async def run(self, ctx: ToolContext, args: FetchSourceArgs) -> FetchSourceData:
        item = await asyncio.to_thread(self._event_item, ctx, args.item_id)
        record = await asyncio.to_thread(self._recorded, ctx, item)
        if record is None:
            record = self._meeting.get((ctx.event_id, item.item_id))
        if record is None:
            document = await self._fetcher.fetch(item.item_id, item.url)
            capture = self._capture(ctx, item, document)
            record = await asyncio.to_thread(self._raw_store.append, capture)
            self._meeting[(ctx.event_id, item.item_id)] = record
            while len(self._meeting) > MEETING_MEMORY:
                self._meeting.popitem(last=False)
        return self._data(item, record)

    def _capture(self, ctx: ToolContext, item: NewsItem, document: FetchedDocument) -> Capture:
        check_document(item, document)
        return Capture(
            source=SOURCE,
            route=ROUTE,
            fetched_at=utcnow(),
            http_status=document.http_status,
            body=document.body,
            params={
                "item_id": item.item_id,
                "event_id": ctx.event_id,
                "url": item.url,
                "final_url": document.final_url,
                "redirect_chain": list(document.redirect_chain),
                "content_type": document.content_type,
            },
            key=item.item_id,
        )


def _blocked(reason: str, message: str) -> ToolBackendError:
    """Count one refused fetcher answer (`hdt_fetch_blocked_total{reason}`) and build its error."""
    FETCH_BLOCKED.labels(reason=reason).inc()
    return ToolBackendError(message)


def check_document(item: NewsItem, document: FetchedDocument) -> None:
    """Reject a fetcher answer that breaks the fetch contract (the fetcher is not trusted blindly)."""
    if document.item_id != item.item_id or document.requested_url != item.url:
        raise _blocked("wrong_item", "fetcher answered for another item or URL")
    if sha256_hex(document.body) != document.body_sha256:
        raise _blocked("sha256_mismatch", "fetched body does not match its sha256")
    if len(document.redirect_chain) > MAX_REDIRECTS:
        raise _blocked("too_many_redirects", f"fetcher followed more than {MAX_REDIRECTS} redirects")
    expected_final = document.redirect_chain[-1] if document.redirect_chain else item.url
    if document.final_url != expected_final:
        raise _blocked("final_url_mismatch", "final URL is not the end of the redirect chain")
    original = _query_keys(item.url)
    for hop in document.redirect_chain:
        if urlsplit(hop).scheme not in ("http", "https"):
            raise _blocked("non_http_redirect", "redirect to a non-http(s) URL")
        added = _query_keys(hop) - original
        if added:
            raise _blocked(
                "redirect_added_query",
                f"redirect added query parameters {sorted(added)} absent from the original URL",
            )


def _query_keys(url: str) -> set[str]:
    return {key for key, _ in parse_qsl(urlsplit(url).query, keep_blank_values=True)}
