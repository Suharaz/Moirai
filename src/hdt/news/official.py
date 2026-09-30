"""`check_official` for the registered event classes, and `PgOfficialSource` (tool port `OfficialSource`).

The check confirms the item's event, not merely the coin. Every window is anchored on the item's own news
time (published / ingested, never after ingestion), never on a time a model stated.
- LISTING / DELIST: a recorded Binance announcement (the only T0 listing source) of the matching catalog,
  on an official domain, naming the coin under the ingestion mapping rule (`hdt.news.coins.mentions`),
  recorded at or before `as_of`, and bound to the item: the item IS that announcement (same URL), or the
  item mentions Binance and the announcement was published between `ANNOUNCEMENT_BEFORE` before and
  `ANNOUNCEMENT_AFTER` after the item's news time. A Binance listing ten days ago never confirms a fresh
  "XYZ to list on Coinbase" article. A page that merely looks official never counts: only rows of
  `news_announcements` do.
- EXPLOIT: the recorded on-chain DEX data (`hdt.news.onchain`) shows the incident (liquidity drained above
  the configured USD amount, or a high-level security hit). A statement of the project never confirms or
  refutes anything here.
- UNLOCK: the unlock calendar holds an unlock of the coin around the item's news time and the calendar
  source is T0.
Other classes are never "official".
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Final

import sqlalchemy as sa

from hdt.core.clock import ensure_utc
from hdt.core.config import NewsSourcesFile
from hdt.news.coins import CoinRef, mentions
from hdt.news.onchain import OnchainState
from hdt.news.store import announcements as stored_announcements
from hdt.news.tiering import domain_in
from hdt.tools.ports import Announcement, UnlockEvent

ANNOUNCEMENT_WINDOW: Final[timedelta] = timedelta(days=14)
"""How far back the recorded announcements are loaded (the veto scan re-checks a whole window)."""
ANNOUNCEMENT_BEFORE: Final[timedelta] = timedelta(hours=48)
ANNOUNCEMENT_AFTER: Final[timedelta] = timedelta(hours=2)
UNLOCK_PAST: Final[timedelta] = timedelta(days=7)
CATALOG_OF: Final[dict[str, str]] = {"LISTING": "listing", "DELIST": "delist"}
_BINANCE: Final = re.compile(r"(?<![A-Za-z0-9])binance(?![A-Za-z0-9])", re.IGNORECASE)


@dataclass(frozen=True)
class OfficialCheck:
    official: bool
    basis: str
    ref: str | None = None


@dataclass(frozen=True)
class ItemEvidence:
    """What `check_official` binds an announcement to: the item's URL, text and news time."""

    url: str
    text: str
    news_time: datetime


def names_coin(title: str, coin: CoinRef) -> bool:
    return mentions(coin, title)


def _bound_to_item(announcement: Announcement, item: ItemEvidence) -> bool:
    if announcement.url == item.url:
        return True
    news_time = ensure_utc(item.news_time)
    return (
        _BINANCE.search(item.text) is not None
        and news_time - ANNOUNCEMENT_BEFORE <= ensure_utc(announcement.published_at)
        and ensure_utc(announcement.published_at) <= news_time + ANNOUNCEMENT_AFTER
    )


def check_official(
    event_class: str | None,
    coin: CoinRef,
    *,
    as_of: datetime,
    item: ItemEvidence,
    announcements: Sequence[Announcement],
    sources: NewsSourcesFile,
    onchain: OnchainState | None,
    drain_usd_min: float,
    unlocks: Sequence[UnlockEvent],
    unlock_tier: str,
    unlock_ahead: timedelta,
) -> OfficialCheck:
    as_of = ensure_utc(as_of)
    anchor = min(ensure_utc(item.news_time), as_of)
    if event_class in CATALOG_OF:
        catalog = CATALOG_OF[event_class]
        for announcement in sorted(announcements, key=lambda a: (a.published_at, a.url), reverse=True):
            if (
                announcement.recorded_at <= as_of
                and announcement.catalog == catalog
                and domain_in(announcement.url, sources.official_listing_domains)
                and announcement.published_at <= as_of
                and _bound_to_item(announcement, item)
                and names_coin(announcement.title, coin)
            ):
                return OfficialCheck(True, "binance_announcement", announcement.url)
        return OfficialCheck(False, "no_announcement")
    if event_class == "EXPLOIT":
        if onchain is not None and onchain.captured_at <= as_of and onchain.incident(drain_usd_min):
            ref = onchain.refs[0]
            return OfficialCheck(
                True, "onchain", f"{ref.source}/{ref.route}/{ref.key}@{ref.fetched_at.isoformat()}"
            )
        return OfficialCheck(False, "no_onchain_incident")
    if event_class == "UNLOCK":
        if unlock_tier != "T0":
            return OfficialCheck(False, "unlock_source_not_t0")
        for unlock in unlocks:
            if (
                unlock.coin_id == coin.coin_id
                and unlock.recorded_at <= as_of
                and anchor - UNLOCK_PAST <= unlock.unlock_at <= as_of + unlock_ahead
            ):
                return OfficialCheck(
                    True, "unlock_calendar", f"{unlock.source}@{unlock.unlock_at.isoformat()}"
                )
        return OfficialCheck(False, "no_unlock")
    return OfficialCheck(False, "not_registered")


class PgOfficialSource:
    """`hdt.tools.ports.OfficialSource` over `news_announcements` (recorded announcements only)."""

    def __init__(self, engine: sa.Engine) -> None:
        self._engine = engine

    def announcements(self, exchange: str, since: datetime, as_of: datetime) -> list[Announcement]:
        with self._engine.connect() as conn:
            return stored_announcements(conn, exchange, since, as_of)
