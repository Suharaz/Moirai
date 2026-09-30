"""Point-in-time lake access for feature code.

`LakeView` is the only door from features to the raw lake. It wraps `PitQuery`, so a record with
`fetched_at > as_of` can never be returned, and adds what every feature needs:

- CMC envelope checks: error responses are stored in the lake too, so a record is usable only when
  `http_status == 200` and `status.error_code` is 0 (CMC index routes send it as the string "0");
- complete-list reads over paged CMC routes (`latest_list`): all pages of one cycle, each successful,
  the last one closing the list. A coin absent from a complete list had nothing to report; an error or a
  missing page is `missing_route` and nothing is inferred;
- a bounded memo of closed lake hours: a partition whose hour ended before the wall clock can no longer
  grow, so re-reading it for the next `as_of` is pure waste. Memoized content is still filtered by the
  caller's `as_of`, so results are identical with or without the memo.

Lake key conventions (phase 02 recorder): paged CMC lists store page 1 under the base key and page n > 1
under `p<n>` (empty base) or `<base>:p<n>`; Binance REST routes use the symbol, klines `<SYMBOL>:<interval>`.
"""

from __future__ import annotations

import json
from collections import OrderedDict
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Final, Literal

from hdt.core.clock import ensure_utc, utcnow
from hdt.lake.pit_query import PitQuery
from hdt.lake.schemas import Partition, RawRecord

CMC: Final = "cmc"
BINANCE: Final = "binance"
_US: Final = timedelta(microseconds=1)

Paging = Literal["has_more", "start_limit", "none"]
ListStatus = Literal["complete", "missing", "error", "incomplete"]


def page_key(base: str, page: int) -> str:
    """Lake key of page `page` (1-based) of a paged CMC list."""
    if page <= 1:
        return base
    return f"{base}:p{page}" if base else f"p{page}"


def cmc_ok(record: RawRecord, body: Any) -> bool:
    """A CMC response is usable only when HTTP 200 and `status.error_code` is 0."""
    if record.http_status != 200 or not isinstance(body, dict):
        return False
    status = body.get("status")
    if not isinstance(status, dict):
        return "data" in body
    code = status.get("error_code")
    return code in (0, "0", None)


def binance_ok(record: RawRecord) -> bool:
    return record.http_status == 200


def params_of(record: RawRecord) -> dict[str, Any]:
    try:
        parsed = json.loads(record.params_json)
    except ValueError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


@dataclass(frozen=True)
class ListRead:
    """One CMC list cycle as seen at `as_of`."""

    status: ListStatus
    items: tuple[Any, ...] = ()
    fetched_at: datetime | None = None
    """Fetch time of page 1 (cycle start)."""
    cycle_id: str | None = None
    """Record hash of page 1: pages of the same cycle are fetched at or after it."""
    last_page_at: datetime | None = None

    @property
    def complete(self) -> bool:
        return self.status == "complete"


class _HourMemo:
    """LRU of closed hour partitions, bounded by compressed body bytes."""

    def __init__(self, max_bytes: int) -> None:
        self._max = max_bytes
        self._size = 0
        self._items: OrderedDict[tuple[str, str, str, datetime], tuple[list[RawRecord], int]] = OrderedDict()

    def get(self, key: tuple[str, str, str, datetime]) -> list[RawRecord] | None:
        hit = self._items.get(key)
        if hit is None:
            return None
        self._items.move_to_end(key)
        return hit[0]

    def put(self, key: tuple[str, str, str, datetime], records: list[RawRecord]) -> None:
        size = sum(len(r.body_zstd) for r in records) + 256
        if size > self._max:
            return
        old = self._items.pop(key, None)
        if old is not None:
            self._size -= old[1]
        self._items[key] = (records, size)
        self._size += size
        while self._size > self._max and self._items:
            _, (_, dropped) = self._items.popitem(last=False)
            self._size -= dropped


class LakeView:
    """Point-in-time reads plus parsing; one instance may serve many `as_of` values."""

    def __init__(
        self,
        pit: PitQuery,
        *,
        clock: Callable[[], datetime] = utcnow,
        closed_grace: timedelta = timedelta(minutes=5),
        memo_bytes: int = 256 * 1024 * 1024,
        json_cache: int = 2048,
    ) -> None:
        self.pit = pit
        self._clock = clock
        self._grace = closed_grace
        self._hours = _HourMemo(memo_bytes)
        self._json: OrderedDict[str, Any] = OrderedDict()
        self._json_max = json_cache
        self._derived: OrderedDict[tuple[str, datetime], Any] = OrderedDict()
        self._derived_max = 50_000
        self._per_record: OrderedDict[tuple[str, str], Any] = OrderedDict()
        self._per_record_max = 100_000
        self.facts: dict[tuple[str, Any], Any] = {}
        """Facts frozen once observed (e.g. a month's exchange ranking); owners define validity."""

    def is_closed(self, hour_start: datetime) -> bool:
        """True when no record can still be added to the hour starting at `hour_start`."""
        return hour_start + timedelta(hours=1) + self._grace <= ensure_utc(self._clock())

    def per_hour[T](self, name: str, hour_start: datetime, compute: Callable[[], T]) -> T:
        """`compute()` for one lake hour, memoized once the hour is closed.

        `compute` must depend only on records of that hour (never on a caller's `as_of`), so the memo is
        exact: callers filter the result by their own `as_of` afterwards.
        """
        key = (name, hour_start)
        if key in self._derived:
            self._derived.move_to_end(key)
            return self._derived[key]  # type: ignore[no-any-return]
        value = compute()
        if self.is_closed(hour_start):
            self._derived[key] = value
            if len(self._derived) > self._derived_max:
                self._derived.popitem(last=False)
        return value

    def per_record[T](self, name: str, record: RawRecord, compute: Callable[[RawRecord], T]) -> T:
        """`compute(record)` memoized by record hash (records are immutable, so the memo is exact)."""
        key = (name, record.record_hash)
        if key in self._per_record:
            self._per_record.move_to_end(key)
            return self._per_record[key]  # type: ignore[no-any-return]
        value = compute(record)
        self._per_record[key] = value
        if len(self._per_record) > self._per_record_max:
            self._per_record.popitem(last=False)
        return value

    # ------------------------------------------------------------------ raw records

    def records(
        self, source: str, route: str, start: datetime, end: datetime, *, as_of: datetime, key: str = ""
    ) -> list[RawRecord]:
        """Records with `start <= fetched_at <= min(end, as_of)`, oldest first."""
        lo, hi = ensure_utc(start), min(ensure_utc(end), ensure_utc(as_of))
        if hi < lo:
            return []
        out: list[RawRecord] = []
        for hour_start in _hour_starts(lo, hi):
            for record in self._hour(source, route, key, hour_start):
                if lo <= record.fetched_at <= hi:
                    out.append(record)
        return out

    def latest(
        self, source: str, route: str, as_of: datetime, *, key: str = "", lookback: timedelta
    ) -> RawRecord | None:
        """Newest record at or before `as_of` within `lookback`."""
        hi = ensure_utc(as_of)
        lo = hi - lookback
        for hour_start in reversed(_hour_starts(lo, hi)):
            found = [r for r in self._hour(source, route, key, hour_start) if lo <= r.fetched_at <= hi]
            if found:
                return max(found, key=lambda r: (r.fetched_at, r.seq))
        return None

    def _hour(self, source: str, route: str, key: str, hour_start: datetime) -> list[RawRecord]:
        hour_end = hour_start + timedelta(hours=1)
        memo_key = (source, route, key, hour_start)
        closed = self.is_closed(hour_start)
        if closed:
            cached = self._hours.get(memo_key)
            if cached is not None:
                return cached
        records = self.pit.series(source, route, hour_start, hour_end - _US, as_of=hour_end - _US, key=key)
        if closed:
            self._hours.put(memo_key, records)
        return records

    # ------------------------------------------------------------------ bodies

    def body_json(self, record: RawRecord) -> Any:
        """Parsed JSON body (None when the body is not JSON); bounded LRU by record hash."""
        cached = self._json.get(record.record_hash, _MISSING)
        if cached is not _MISSING:
            self._json.move_to_end(record.record_hash)
            return cached
        try:
            value: Any = json.loads(record.body())
        except ValueError:
            value = None
        self._json[record.record_hash] = value
        if len(self._json) > self._json_max:
            self._json.popitem(last=False)
        return value

    def cmc_data(self, record: RawRecord | None) -> Any:
        """The `data` member of a successful CMC response, else None."""
        if record is None:
            return None
        body = self.body_json(record)
        if not cmc_ok(record, body):
            return None
        return body.get("data")

    def binance_data(self, record: RawRecord | None) -> Any:
        if record is None or not binance_ok(record):
            return None
        return self.body_json(record)

    def latest_cmc(
        self, route: str, as_of: datetime, *, key: str = "", lookback: timedelta
    ) -> tuple[RawRecord | None, Any]:
        """Newest record and its data (data is None when the newest response is an error)."""
        record = self.latest(CMC, route, as_of, key=key, lookback=lookback)
        return record, self.cmc_data(record)

    def latest_cmc_ok(
        self, route: str, as_of: datetime, *, key: str = "", lookback: timedelta
    ) -> tuple[RawRecord | None, Any]:
        """Newest successful record within `lookback` (older successes are reached past errors)."""
        hi = ensure_utc(as_of)
        lo = hi - lookback
        for hour_start in reversed(_hour_starts(lo, hi)):
            found = [r for r in self._hour(CMC, route, key, hour_start) if lo <= r.fetched_at <= hi]
            for record in sorted(found, key=lambda r: (r.fetched_at, r.seq), reverse=True):
                data = self.cmc_data(record)
                if data is not None:
                    return record, data
        return None, None

    # ------------------------------------------------------------------ paged CMC lists

    def latest_list(
        self,
        route: str,
        as_of: datetime,
        *,
        base_key: str = "",
        items_field: str | None,
        paging: Paging,
        lookback: timedelta,
        cycle_window: timedelta,
        max_pages: int,
    ) -> ListRead:
        """The newest complete list cycle at `as_of`.

        When the newest cycle is still being fetched (its later pages are not recorded yet and the cycle
        window has not elapsed), the previous cycle is used if it is complete. Any error page, or a page
        still missing after the cycle window, yields `error` / `incomplete` (both mean `missing_route`).
        """
        as_of = ensure_utc(as_of)
        first = self.latest(CMC, route, as_of, key=base_key, lookback=lookback)
        if first is None:
            return ListRead("missing")
        read = self._cycle(route, first, as_of, base_key, items_field, paging, cycle_window, max_pages)
        if read.status == "incomplete" and as_of - first.fetched_at <= cycle_window:
            previous = self.latest(CMC, route, first.fetched_at - _US, key=base_key, lookback=lookback)
            if previous is not None:
                older = self._cycle(
                    route,
                    previous,
                    first.fetched_at - _US,
                    base_key,
                    items_field,
                    paging,
                    cycle_window,
                    max_pages,
                )
                if older.complete:
                    return older
        return read

    def _cycle(
        self,
        route: str,
        first: RawRecord,
        upper: datetime,
        base_key: str,
        items_field: str | None,
        paging: Paging,
        cycle_window: timedelta,
        max_pages: int,
    ) -> ListRead:
        data = self.cmc_data(first)
        if data is None:
            return ListRead("error", fetched_at=first.fetched_at, cycle_id=first.record_hash)
        items = _items(data, items_field)
        if items is None:
            return ListRead("error", fetched_at=first.fetched_at, cycle_id=first.record_hash)
        collected = list(items)
        page, record = 1, first
        while _more(paging, data, items, record):
            page += 1
            if page > max_pages:
                return ListRead("incomplete", fetched_at=first.fetched_at, cycle_id=first.record_hash)
            record_n = self.latest(
                CMC, route, upper, key=page_key(base_key, page), lookback=cycle_window + timedelta(hours=1)
            )
            if (
                record_n is None
                or record_n.fetched_at < first.fetched_at
                or record_n.fetched_at > first.fetched_at + cycle_window
            ):
                return ListRead("incomplete", fetched_at=first.fetched_at, cycle_id=first.record_hash)
            data = self.cmc_data(record_n)
            items = _items(data, items_field) if data is not None else None
            if items is None:
                return ListRead("error", fetched_at=first.fetched_at, cycle_id=first.record_hash)
            collected.extend(items)
            record = record_n
        return ListRead(
            "complete",
            tuple(collected),
            fetched_at=first.fetched_at,
            cycle_id=first.record_hash,
            last_page_at=record.fetched_at,
        )


_MISSING: Final = object()


def _items(data: Any, field: str | None) -> list[Any] | None:
    if field is None:
        return data if isinstance(data, list) else None
    if isinstance(data, dict) and isinstance(data.get(field), list):
        return list(data[field])
    return None


def _more(paging: Paging, data: Any, items: list[Any], record: RawRecord) -> bool:
    if paging == "has_more":
        return isinstance(data, dict) and data.get("has_more") is True
    if paging == "start_limit":
        limit = params_of(record).get("limit")
        try:
            return limit is not None and len(items) >= int(limit)
        except (TypeError, ValueError):
            return False
    return False


def _hour_starts(lo: datetime, hi: datetime) -> list[datetime]:
    current = Partition.of("cmc", "x", lo).start
    last = Partition.of("cmc", "x", hi).start
    out = []
    while current <= last:
        out.append(current)
        current += timedelta(hours=1)
    return out


def as_float(value: Any) -> float | None:
    """Numbers and numeric strings (Binance sends decimals as strings) to float; anything else None."""
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int | float):
        result = float(value)
    elif isinstance(value, str):
        try:
            result = float(value)
        except ValueError:
            return None
    else:
        return None
    return result if result == result and result not in (float("inf"), float("-inf")) else None


def parse_ts(value: Any) -> datetime | None:
    """ISO-8601 strings (with `Z`), epoch seconds or epoch milliseconds to aware UTC datetimes."""
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int | float) or (isinstance(value, str) and value.isdigit()):
        number = float(value)
        seconds = number / 1000.0 if number > 1e11 else number
        return datetime.fromtimestamp(seconds, tz=UTC)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    return None


def usd_quote(item: Mapping[str, Any]) -> Mapping[str, Any] | None:
    """The USD quote of a CMC v3 item: `quote` is either an array of quotes or a `{"USD": {...}}` map."""
    quote = item.get("quote")
    if isinstance(quote, Mapping):
        usd = quote.get("USD")
        return usd if isinstance(usd, Mapping) else None
    if isinstance(quote, list):
        for entry in quote:
            if isinstance(entry, Mapping) and (entry.get("symbol") == "USD" or entry.get("id") == 2781):
                return entry
    quotes = item.get("quotes")
    if isinstance(quotes, list):
        for entry in quotes:
            if isinstance(entry, Mapping) and entry.get("symbol") in ("USD", None):
                return entry
    return None
