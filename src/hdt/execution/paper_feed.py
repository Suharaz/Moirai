"""Recorded mainnet market data -> paper venue (point in time: only records with `fetched_at <= now`).

Every poll reads the lake records written since the last poll (with a short overlap for records whose
write landed late, de-duplicated by record hash) for the symbols the venue watches (resting orders,
working conditional orders, positions), merges them by exchange event time and replays them:
- `ws_depth` (100 ms diffs, hot set): maintains the depth-diff book (`Book.apply_diff`, update-id
  continuity); these books give fill model `queue`;
- `ws_depth20` (1 s snapshots): re-bases the book when no continuous diffs are flowing (fill model
  `degraded`), and seeds the diff book;
- `ws_aggtrade`: queue consumption and through-trades of resting orders;
- `ws_markprice` / REST `premium_index`: marks (conditional triggers) and funding (`r`, `T`).
A symbol the venue starts watching is seeded from the newest depth20 snapshot (REST `depth` fallback) and
the newest mark, so an order placed now sees the book as of now.

A poll is two calls, so the adapter keeps lake IO off the event loop and every venue change on it: `read`
(lake IO and parsing; it reads the venue but never changes it) then `apply` (venue changes, then the
cursor). `read_seed` / `apply_seed` split the seeding of one symbol the same way. The cursor and the
seen-record set advance only in `apply`, once every record read was replayed: a failed lake read changes
nothing and the next poll reads the same window again. An unreadable record or a malformed event is logged
and skipped; it never drops the rest of the window (a stop-crossing mark behind it still triggers).
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from functools import partial
from typing import Any, Final

import zstandard

from hdt.execution.paper_venue import Book, PaperVenue, VenueEvent
from hdt.lake.pit_query import PitQuery
from hdt.lake.schemas import LakeIntegrityError, RawRecord

log = logging.getLogger(__name__)

SOURCE: Final[str] = "binance"
ROUTE_ORDER: Final[dict[str, int]] = {
    "ws_aggtrade": 0,
    "ws_depth": 1,
    "ws_depth20": 2,
    "ws_markprice": 3,
    "premium_index": 4,
}
OVERLAP: Final[timedelta] = timedelta(seconds=10)
FIRST_LOOKBACK: Final[timedelta] = timedelta(minutes=2)
SEED_BOOK_MAX_AGE: Final[timedelta] = timedelta(seconds=30)
SEED_MARK_MAX_AGE: Final[timedelta] = timedelta(minutes=10)
DIFF_STALE_MS: Final[int] = 3_000


@dataclass(frozen=True)
class _Item:
    ts_ms: int
    rank: int
    route: str
    data: Mapping[str, Any]


@dataclass(frozen=True)
class BookSeed:
    bids: list[Any]
    asks: list[Any]
    last_u: int | None
    ts_ms: int


@dataclass(frozen=True)
class MarkSeed:
    price: Any
    ts_ms: int | None
    rate: Any
    next_ms: int | None


@dataclass(frozen=True)
class MarketSeed:
    """What `read_seed` found for a symbol the venue has no book or no mark of (None: not needed/none)."""

    symbol: str
    book: BookSeed | None
    mark: MarkSeed | None


@dataclass(frozen=True)
class MarketBatch:
    """One poll's lake reads, parsed but not applied yet (see `PaperFeed.apply`)."""

    now: datetime
    seeds: tuple[MarketSeed, ...]
    items: tuple[_Item, ...]  # merged by exchange event time
    records: tuple[tuple[str, datetime], ...]  # (record hash, fetched_at) of every new record read


def _int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _guarded[T](what: str, fn: Callable[[], T], fallback: T) -> T:
    """Apply one piece of recorded market data; a malformed one is logged and skipped."""
    try:
        return fn()
    except Exception:  # whatever one record holds, the rest of the window must still replay
        log.exception("paper feed: skipped malformed %s", what)
        return fallback


def _json_body(record: RawRecord) -> Any:
    """Parsed body of `record`; None (logged) when the stored body is corrupt or not JSON."""
    try:
        return json.loads(record.body())
    except (ValueError, LakeIntegrityError, zstandard.ZstdError) as exc:
        log.warning(
            "paper feed: skipped unreadable lake record %s/%s seq %d: %s",
            record.source,
            record.route,
            record.seq,
            exc,
        )
        return None


def _ws_items(record: RawRecord) -> list[Mapping[str, Any]]:
    body = _json_body(record)
    out: list[Mapping[str, Any]] = []
    for item in body if isinstance(body, list) else []:
        data = item.get("data") if isinstance(item, Mapping) else None
        for event in data if isinstance(data, list) else [data]:
            if isinstance(event, Mapping):
                out.append(event)
    return out


def _rest_items(record: RawRecord) -> list[Mapping[str, Any]]:
    if record.http_status != 200:
        return []
    body = _json_body(record)
    items = body if isinstance(body, list) else [body]
    return [i for i in items if isinstance(i, Mapping)]


class PaperFeed:
    def __init__(self, pit: PitQuery, venue: PaperVenue) -> None:
        self.pit = pit
        self.venue = venue
        self.cursor: datetime | None = None
        self._seen: dict[str, datetime] = {}
        self._last_diff_ms: dict[str, int] = {}

    # ------------------------------------------------------------------ seeding
    def read_seed(self, symbol: str, now: datetime) -> MarketSeed:
        """Lake reads seeding `symbol` where the venue has no book or no mark of it yet (no venue change)."""
        book = self._book_seed(symbol, now) if self.venue.book(symbol) is None else None
        mark = self._mark_seed(symbol, now) if self.venue.mark(symbol) is None else None
        return MarketSeed(symbol, book, mark)

    def apply_seed(self, seed: MarketSeed) -> list[VenueEvent]:
        """Seed the venue from `read_seed` (a first mark may trigger); what it got meanwhile is kept."""
        symbol, venue = seed.symbol, self.venue
        if seed.book is not None and venue.book(symbol) is None:
            _guarded(f"{symbol} book seed", partial(self._install_book, symbol, seed.book), None)
        if seed.mark is not None and venue.mark(symbol) is None:
            m = seed.mark
            apply = partial(self._apply_mark, symbol, m.price, m.ts_ms, m.rate, m.next_ms)
            return _guarded(f"{symbol} mark seed", apply, [])
        return []

    def _install_book(self, symbol: str, seed: BookSeed) -> None:
        book = self.venue.books.setdefault(symbol, Book())
        book.apply_snapshot(seed.bids, seed.asks, seed.last_u, seed.ts_ms)

    def _book_seed(self, symbol: str, now: datetime) -> BookSeed | None:
        stream = f"{symbol.lower()}@depth20"
        for record in reversed(
            self.pit.series(SOURCE, "ws_depth20", now - SEED_BOOK_MAX_AGE, now, as_of=now)
        ):
            body = _json_body(record)
            for item in reversed(body) if isinstance(body, list) else []:
                if not isinstance(item, Mapping) or not str(item.get("stream", "")).startswith(stream):
                    continue
                data = item.get("data")
                if isinstance(data, Mapping):
                    return BookSeed(
                        data.get("b") or [],
                        data.get("a") or [],
                        _int(data.get("u")),
                        _int(data.get("E")) or 0,
                    )
        snapshot = self.pit.latest(SOURCE, "depth", now, key=symbol, lookback=SEED_BOOK_MAX_AGE)
        found = _rest_items(snapshot) if snapshot is not None else []
        if snapshot is None or not found:
            return None
        data = found[0]
        return BookSeed(
            data.get("bids") or [],
            data.get("asks") or [],
            _int(data.get("lastUpdateId")),
            _int(data.get("E")) or int(snapshot.fetched_at.timestamp() * 1000),
        )

    def _mark_seed(self, symbol: str, now: datetime) -> MarkSeed | None:
        record = self.pit.latest(SOURCE, "ws_markprice", now, lookback=SEED_MARK_MAX_AGE)
        best: Mapping[str, Any] | None = None
        if record is not None:
            for event in _ws_items(record):
                if event.get("s") == symbol and (
                    best is None or (_int(event.get("E")) or 0) > (_int(best.get("E")) or 0)
                ):
                    best = event
        if best is not None:
            return MarkSeed(best.get("p"), _int(best.get("E")), best.get("r"), _int(best.get("T")))
        rest = self.pit.latest(SOURCE, "premium_index", now, lookback=SEED_MARK_MAX_AGE)
        for item in _rest_items(rest) if rest is not None else []:
            if item.get("symbol") == symbol:
                return MarkSeed(
                    item.get("markPrice"),
                    _int(item.get("time")),
                    item.get("lastFundingRate"),
                    _int(item.get("nextFundingTime")),
                )
        return None

    def _apply_mark(
        self, symbol: str, price: Any, ts_ms: int | None, rate: Any, next_ms: int | None
    ) -> list[VenueEvent]:
        if price in (None, "") or ts_ms is None:
            return []
        mark = Decimal(str(price))
        if mark <= 0:
            return []
        funding_rate = Decimal(str(rate)) if rate not in (None, "") else None
        return self.venue.on_mark(symbol, mark, ts_ms, funding_rate, next_ms if next_ms else None)

    # ------------------------------------------------------------------ replay
    def read(self, now: datetime) -> MarketBatch:
        """Lake IO of one poll up to `now`: seeds of newly watched symbols and every record since the
        cursor not replayed yet, parsed and merged by event time. Reads the venue, never changes it."""
        symbols = self.venue.watched_symbols()
        if not symbols:
            return MarketBatch(now, (), (), ())
        start = (self.cursor - OVERLAP) if self.cursor is not None else now - FIRST_LOOKBACK
        seeds = [self.read_seed(symbol, now) for symbol in sorted(symbols)]
        items: list[_Item] = []
        records: list[tuple[str, datetime]] = []
        for route, rank in ROUTE_ORDER.items():
            for record in self.pit.series(SOURCE, route, start, now, as_of=now):
                if record.record_hash in self._seen:
                    continue
                records.append((record.record_hash, record.fetched_at))
                parse = partial(self._items, record, route, rank, symbols)
                items.extend(_guarded(f"{route} record seq {record.seq}", parse, []))
        items.sort(key=lambda i: (i.ts_ms, i.rank))
        return MarketBatch(
            now,
            tuple(s for s in seeds if s.book is not None or s.mark is not None),
            tuple(items),
            tuple(records),
        )

    def apply(self, batch: MarketBatch) -> list[VenueEvent]:
        """Replay `batch` into the venue, then advance the cursor and the seen records past it."""
        events: list[VenueEvent] = []
        for seed in batch.seeds:
            events.extend(self.apply_seed(seed))
        for item in batch.items:
            events.extend(_guarded(f"{item.route} event at {item.ts_ms}", partial(self._replay, item), []))
        self._seen.update(batch.records)
        # the next read starts at `now - OVERLAP`: an older record can never come back
        self._seen = {h: t for h, t in self._seen.items() if t >= batch.now - OVERLAP}
        self.cursor = batch.now
        return events

    def _items(self, record: RawRecord, route: str, rank: int, symbols: set[str]) -> list[_Item]:
        fetched_ms = int(record.fetched_at.timestamp() * 1000)
        out: list[_Item] = []
        if route == "premium_index":
            for item in _rest_items(record):
                if item.get("symbol") in symbols:
                    out.append(_Item(_int(item.get("time")) or fetched_ms, rank, route, item))
            return out
        for event in _ws_items(record):
            if event.get("s") in symbols:
                ts = _int(event.get("T")) if route == "ws_aggtrade" else _int(event.get("E"))
                out.append(_Item(ts or fetched_ms, rank, route, event))
        return out

    def _replay(self, item: _Item) -> list[VenueEvent]:
        d = item.data
        venue = self.venue
        if item.route == "ws_aggtrade":
            symbol = str(d["s"])
            return venue.on_trade(
                symbol, Decimal(str(d["p"])), Decimal(str(d["q"])), bool(d.get("m")), item.ts_ms
            )
        if item.route == "ws_depth":
            symbol = str(d["s"])
            book = venue.books.get(symbol)
            if book is None or book.last_u is None:
                return []
            U, u, pu = _int(d.get("U")), _int(d.get("u")), _int(d.get("pu"))  # noqa: N806 - Binance field names
            if U is None or u is None or pu is None:
                return []
            if book.apply_diff(U, u, pu, d.get("b") or [], d.get("a") or [], item.ts_ms):
                self._last_diff_ms[symbol] = item.ts_ms
            return venue.on_book(symbol, item.ts_ms)
        if item.route == "ws_depth20":
            symbol = str(d["s"])
            book = venue.books.setdefault(symbol, Book())
            diffs_flowing = book.diff_ok and item.ts_ms - self._last_diff_ms.get(symbol, 0) <= DIFF_STALE_MS
            if not diffs_flowing:
                book.apply_snapshot(d.get("b") or [], d.get("a") or [], _int(d.get("u")), item.ts_ms)
                return venue.on_book(symbol, item.ts_ms)
            return []
        if item.route == "ws_markprice":
            return self._apply_mark(str(d["s"]), d.get("p"), item.ts_ms, d.get("r"), _int(d.get("T")))
        return self._apply_mark(
            str(d["symbol"]),
            d.get("markPrice"),
            item.ts_ms,
            d.get("lastFundingRate"),
            _int(d.get("nextFundingTime")),
        )
