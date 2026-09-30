"""`PgSourceLookup`: the council verifier's view of a news item's stored copy (port `SourceLookup`).

The text is the fetched original when the live `fetch_source` tool captured one (lake source `news`, route
`fetch_source`, key = item_id; read point in time, or exactly the capture pinned by this event's meeting),
otherwise the recorded item itself (title, summary, content as ingested). Tier, event class and official
status are computed by code from the final URL and the item's verdict known at `as_of`:
- `event_class` is one of listing / delist / exploit / unlock when both judges agreed on it, else None;
- `official` is True only when `check_official` confirmed that registered event for the claim's coin
  (`coin_id`): an item confirmed for another coin it mentions is not official for this one. Without a
  coin the item-level result is used;
- `event_coin_ids` are the coins the agreed verdict was judged for (the item's mapped coins); empty
  without an agreed verdict.
"""

from __future__ import annotations

from datetime import datetime

import sqlalchemy as sa

from hdt.contracts.forecast import LakeRef
from hdt.core.config import NewsSourcesFile
from hdt.core.ids import sha256_hex
from hdt.council.ports import StoredSource
from hdt.lake.pit_query import PitQuery
from hdt.lake.schemas import RawRecord
from hdt.news.classes import REGISTERED_CLASSES
from hdt.news.config import NewsFile, news_config
from hdt.news.store import item_at, verdict_at
from hdt.news.text import item_text
from hdt.news.tiering import code_tier
from hdt.tools.base import ToolBackendError
from hdt.tools.impl.fetch_source import CAPTURE_LOOKBACK, ROUTE, SOURCE, document_text
from hdt.tools.impl.news import url_domain
from hdt.tools.pit import PitView, record_params


class PgSourceLookup:
    def __init__(
        self,
        engine: sa.Engine,
        pit: PitQuery,
        sources: NewsSourcesFile,
        *,
        config: NewsFile | None = None,
    ) -> None:
        self._engine = engine
        self._pit = pit
        self._sources = sources
        self._rule_version = (config or news_config()).rule_version

    def stored_source(
        self,
        item_id: str,
        *,
        as_of: datetime,
        pinned: tuple[LakeRef, ...],
        coin_id: int | None = None,
    ) -> StoredSource | None:
        with self._engine.connect() as conn:
            item = item_at(conn, item_id, as_of)
            if item is None:
                return None
            verdict = verdict_at(conn, item_id, self._rule_version, as_of)
        registered = None
        official = False
        event_coins: tuple[int, ...] = ()
        if verdict is not None and verdict.status == "agreed":
            registered = REGISTERED_CLASSES.get(verdict.event_class or "")
            event_coins = tuple(sorted(set(verdict.coin_ids)))
            if coin_id is None:
                confirmed = verdict.official
            else:
                confirmed = coin_id in set(verdict.detail.get("official_coins") or ())
            official = registered is not None and confirmed
        record = self._capture(item_id, as_of, pinned)
        final_url = item.url
        text: str | None = None
        body_sha256: str | None = None
        if record is not None and record.http_status == 200:
            params = record_params(record)
            try:
                text = document_text(record.body(), str(params.get("content_type") or ""))
            except ToolBackendError:
                text = None
            if text is not None:
                final_url = str(params.get("final_url") or item.url)
                body_sha256 = record.body_sha256
        if text is None or body_sha256 is None:
            text = item_text(item.title, item.summary, item.content)
            body_sha256 = sha256_hex(text)
        tier = code_tier(
            final_url,
            self._sources,
            event_class=verdict.event_class if verdict is not None and verdict.status == "agreed" else None,
            official=official,
            recorded_announcement=item.source_kind == "binance" and final_url == item.url,
        )
        return StoredSource(
            item_id=item.item_id,
            text=text,
            final_url=final_url,
            domain=url_domain(final_url) or item.domain,
            tier=tier,
            event_class=registered,
            official=official,
            event_coin_ids=event_coins,
            body_sha256=body_sha256,
        )

    def _capture(self, item_id: str, as_of: datetime, pinned: tuple[LakeRef, ...]) -> RawRecord | None:
        prior = PitView(self._pit, as_of).latest(SOURCE, ROUTE, key=item_id, lookback=CAPTURE_LOOKBACK)
        if prior is not None:
            return prior
        for ref in pinned:
            if (ref.source, ref.route, ref.key) != (SOURCE, ROUTE, item_id):
                continue
            cutoff = ref.fetched_at
            for record in self._pit.series(SOURCE, ROUTE, cutoff, cutoff, as_of=cutoff, key=item_id):
                if record.body_sha256 == ref.body_sha256 and record.http_status == ref.http_status:
                    return record
        return None
