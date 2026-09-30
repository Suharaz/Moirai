"""`get_snapshot(coin, fields, as_of)`: recorded market state of the event coin, point-in-time.

Reads only the lake, through a `PitView` bounded by the event `as_of` (identical in live and replay):
- Binance `premium_index` (key "", all symbols): mark and index price, basis, last funding rate, next
  funding time;
- Binance `open_interest` (key = symbol): open interest in contracts, and in USD at the mark price;
- Binance `funding_info` (key "", adjusted symbols only): funding interval in hours;
- CMC `liquidations_by_crypto` (paged: key "" then "p2", "p3", ...): long/short/total liquidations over
  1 h / 4 h / 24 h in USD, from the newest complete list (every page HTTP 200 with `error_code` 0, the last
  page with `has_more` false, page n recorded after page 1 and before the next cycle's page 1). A coin
  absent from a complete list had no liquidations (0), per the design contract.
The newest successful record is used; each value carries its lake provenance, its age at `as_of` and a
`stale` flag (`settings.data.stale_s`). Missing values are listed with the reason, never guessed.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar, Final, Literal, get_args

from pydantic import Field, PositiveInt

from hdt.contracts.common import UtcDatetime
from hdt.contracts.forecast import LakeRef
from hdt.lake.pit_query import PitQuery
from hdt.lake.schemas import RawRecord
from hdt.lake.universe import UniverseMember
from hdt.tools.base import ShortText, Tool, ToolArgs, ToolContext, ToolData, ToolMode
from hdt.tools.pit import PitView, age_seconds, cmc_data, record_json

SnapshotField = Literal[
    "mark_price",
    "index_price",
    "basis_pct",
    "funding_rate",
    "next_funding_time",
    "funding_interval_h",
    "open_interest",
    "open_interest_usd",
    "liq_long_1h",
    "liq_short_1h",
    "liq_total_1h",
    "liq_long_4h",
    "liq_short_4h",
    "liq_total_4h",
    "liq_long_24h",
    "liq_short_24h",
    "liq_total_24h",
]
SNAPSHOT_FIELDS: Final[tuple[SnapshotField, ...]] = get_args(SnapshotField)
_PREMIUM: Final = frozenset({"mark_price", "index_price", "basis_pct", "funding_rate", "next_funding_time"})
_OI: Final = frozenset({"open_interest", "open_interest_usd"})
_LIQ: Final = frozenset(f for f in SNAPSHOT_FIELDS if f.startswith("liq_"))
_UNITS: Final[dict[str, str]] = {
    "mark_price": "USDT",
    "index_price": "USDT",
    "basis_pct": "percent",
    "funding_rate": "rate per funding interval",
    "next_funding_time": "UTC",
    "funding_interval_h": "hours",
    "open_interest": "contracts",
    "open_interest_usd": "USD",
    **{f: "USD" for f in SNAPSHOT_FIELDS if f.startswith("liq_")},
}
LIQ_ROUTE: Final[str] = "liquidations_by_crypto"
LIQ_LOOKBACK: Final[timedelta] = timedelta(minutes=30)
LIQ_CYCLES_TRIED: Final[int] = 3
MAX_LIQ_PAGES: Final[int] = 40
REST_LOOKBACK: Final[timedelta] = timedelta(hours=6)
FUNDING_INFO_LOOKBACK: Final[timedelta] = timedelta(days=2)
FALLBACK_WINDOW: Final[timedelta] = timedelta(minutes=30)


class SnapshotArgs(ToolArgs):
    coin_id: PositiveInt | None = Field(default=None, description="CMC id; only the event coin (default)")
    fields: tuple[SnapshotField, ...] = Field(
        default=(), max_length=len(SNAPSHOT_FIELDS), description="fields to read; empty = all"
    )
    as_of: UtcDatetime | None = Field(default=None, description="read time; not after the event time")


class SnapshotValue(ToolData):
    field: SnapshotField
    value: float | str
    unit: str
    age_s: float
    stale: bool
    source: LakeRef


class MissingField(ToolData):
    field: SnapshotField
    reason: ShortText


class SnapshotData(ToolData):
    coin_id: int
    binance_symbol: str
    multiplier: int = Field(description="Binance contract units per coin (1000 for 1000PEPEUSDT)")
    as_of: UtcDatetime
    stale_after_s: int
    values: tuple[SnapshotValue, ...]
    missing: tuple[MissingField, ...]


class GetSnapshotTool(Tool[SnapshotArgs, SnapshotData]):
    name: ClassVar[str] = "get_snapshot"
    description: ClassVar[str] = (
        "Recorded market snapshot of the event coin: Binance mark/index price, basis, funding, open "
        "interest and CMC liquidations (1h/4h/24h). Each value has its source record, age and a stale flag."
    )
    args_model = SnapshotArgs
    data_model = SnapshotData

    def __init__(self, mode: ToolMode, pit: PitQuery, *, stale_s: int) -> None:
        super().__init__(mode)
        self._pit = pit
        self._stale_s = stale_s

    async def run(self, ctx: ToolContext, args: SnapshotArgs) -> SnapshotData:
        coin_id = ctx.event_coin(args.coin_id)
        as_of = ctx.read_as_of(args.as_of)
        wanted = frozenset(args.fields) if args.fields else frozenset(SNAPSHOT_FIELDS)
        return await asyncio.to_thread(self.snapshot, PitView(self._pit, as_of), coin_id, wanted)

    def snapshot(self, view: PitView, coin_id: int, wanted: frozenset[str]) -> SnapshotData:
        member = view.member(coin_id)
        builder = _Builder(view.horizon, self._stale_s, wanted)
        if wanted & (_PREMIUM | {"open_interest_usd"}):
            _premium(view, member, builder)
        if wanted & _OI:
            _open_interest(view, member, builder)
        if "funding_interval_h" in wanted:
            _funding_interval(view, member, builder)
        if wanted & _LIQ:
            _liquidations(view, coin_id, builder)
        return SnapshotData(
            coin_id=coin_id,
            binance_symbol=member.binance_symbol,
            multiplier=member.multiplier,
            as_of=view.horizon,
            stale_after_s=self._stale_s,
            values=tuple(builder.values[f] for f in SNAPSHOT_FIELDS if f in builder.values and f in wanted),
            missing=tuple(
                MissingField(field=f, reason=builder.missing[f])
                for f in SNAPSHOT_FIELDS
                if f in wanted and f not in builder.values
            ),
        )


class _Builder:
    def __init__(self, as_of: datetime, stale_s: int, wanted: frozenset[str]) -> None:
        self.as_of = as_of
        self.stale_s = stale_s
        self.wanted = wanted
        self.values: dict[str, SnapshotValue] = {}
        self.missing: dict[str, str] = {}
        self.mark: float | None = None

    def put(
        self,
        field: SnapshotField,
        value: float | str | None,
        record: RawRecord,
        reason: str,
        *,
        stale_after_s: float | None = None,
    ) -> None:
        """`stale_after_s` overrides the market-data threshold for slower routes (daily funding info)."""
        if value is None:
            self.fail((field,), reason)
            return
        age = age_seconds(record, self.as_of)
        self.values[field] = SnapshotValue(
            field=field,
            value=value,
            unit=_UNITS[field],
            age_s=age,
            stale=age > (self.stale_s if stale_after_s is None else stale_after_s),
            source=LakeRef.of(record),
        )

    def fail(self, fields: tuple[str, ...] | frozenset[str], reason: str) -> None:
        for field in fields:
            self.missing.setdefault(field, reason)


def latest_ok(
    view: PitView,
    source: str,
    route: str,
    ok: Callable[[RawRecord], bool],
    *,
    key: str = "",
    lookback: timedelta = REST_LOOKBACK,
) -> RawRecord | None:
    """The newest successful record: the latest one, else the newest success of the last 30 minutes."""
    record = view.latest(source, route, key=key, lookback=lookback)
    if record is None or ok(record):
        return record
    for older in reversed(view.series(source, route, view.horizon - FALLBACK_WINDOW, key=key)):
        if ok(older):
            return older
    return None


def _http_ok(record: RawRecord) -> bool:
    return record.http_status == 200


def _float(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number and number not in (float("inf"), float("-inf")) else None


def _entry(body: Any, symbol: str) -> dict[str, Any] | None:
    entries = body if isinstance(body, list) else [body] if isinstance(body, dict) else []
    return next((e for e in entries if isinstance(e, dict) and e.get("symbol") == symbol), None)


def _premium(view: PitView, member: UniverseMember, builder: _Builder) -> None:
    record = latest_ok(view, "binance", "premium_index", _http_ok)
    if record is None:
        builder.fail(_PREMIUM | {"open_interest_usd"}, "no successful premium_index record in the last 6 h")
        return
    entry = _entry(record_json(record), member.binance_symbol)
    if entry is None:
        builder.fail(_PREMIUM | {"open_interest_usd"}, "symbol absent from the premium_index record")
        return
    mark, index = _float(entry.get("markPrice")), _float(entry.get("indexPrice"))
    builder.mark = mark
    builder.put("mark_price", mark, record, "markPrice missing")
    builder.put("index_price", index, record, "indexPrice missing")
    basis = round((mark - index) / index * 100, 6) if mark is not None and index else None
    builder.put("basis_pct", basis, record, "basis needs mark and index price")
    builder.put("funding_rate", _float(entry.get("lastFundingRate")), record, "lastFundingRate missing")
    next_ms = entry.get("nextFundingTime")
    next_time = (
        datetime.fromtimestamp(next_ms / 1000, tz=UTC).isoformat().replace("+00:00", "Z")
        if isinstance(next_ms, int) and not isinstance(next_ms, bool) and next_ms > 0
        else None
    )
    builder.put("next_funding_time", next_time, record, "nextFundingTime missing")


def _open_interest(view: PitView, member: UniverseMember, builder: _Builder) -> None:
    record = latest_ok(view, "binance", "open_interest", _http_ok, key=member.binance_symbol)
    if record is None:
        builder.fail(_OI, "no successful open_interest record in the last 6 h")
        return
    body = record_json(record)
    contracts = _float(body.get("openInterest")) if isinstance(body, dict) else None
    builder.put("open_interest", contracts, record, "openInterest missing")
    usd = round(contracts * builder.mark, 2) if contracts is not None and builder.mark is not None else None
    builder.put("open_interest_usd", usd, record, "open interest in USD needs the mark price")


def _funding_interval(view: PitView, member: UniverseMember, builder: _Builder) -> None:
    record = latest_ok(view, "binance", "funding_info", _http_ok, lookback=FUNDING_INFO_LOOKBACK)
    if record is None:
        builder.fail(("funding_interval_h",), "no successful funding_info record in the last 2 days")
        return
    entry = _entry(record_json(record), member.binance_symbol)
    if entry is None:
        builder.fail(("funding_interval_h",), "symbol not listed in the latest funding_info record")
        return
    interval = _float(entry.get("fundingIntervalHours"))
    daily = FUNDING_INFO_LOOKBACK.total_seconds()
    builder.put("funding_interval_h", interval, record, "interval missing", stale_after_s=daily)


def complete_liquidation_list(view: PitView) -> tuple[list[RawRecord], list[dict[str, Any]]] | None:
    """Pages and items of the newest complete liquidation list at or before the view horizon."""
    firsts = view.series("cmc", LIQ_ROUTE, view.horizon - LIQ_LOOKBACK, key="")
    for i in range(len(firsts) - 1, max(-1, len(firsts) - 1 - LIQ_CYCLES_TRIED), -1):
        end = firsts[i + 1].fetched_at if i + 1 < len(firsts) else None
        cycle = _cycle(view, firsts[i], end)
        if cycle is not None:
            return cycle
    return None


def _cycle(
    view: PitView, first: RawRecord, end: datetime | None
) -> tuple[list[RawRecord], list[dict[str, Any]]] | None:
    pages: list[RawRecord] = [first]
    items: list[dict[str, Any]] = []
    record, page = first, 1
    while True:
        data = cmc_data(record)
        if not isinstance(data, dict) or not isinstance(data.get("cryptocurrencies"), list):
            return None
        items.extend(e for e in data["cryptocurrencies"] if isinstance(e, dict))
        if data.get("has_more") is not True:
            return pages, items
        page += 1
        if page > MAX_LIQ_PAGES:
            return None
        later = [
            r
            for r in view.series("cmc", LIQ_ROUTE, first.fetched_at, key=f"p{page}")
            if end is None or r.fetched_at < end
        ]
        if not later:
            return None
        record = later[0]
        pages.append(record)


def _liquidations(view: PitView, coin_id: int, builder: _Builder) -> None:
    found = complete_liquidation_list(view)
    if found is None:
        builder.fail(_LIQ, "no complete liquidation list in the 30 min before as_of")
        return
    pages, items = found
    item = next((i for i in items if i.get("crypto_id") == coin_id), None)
    if item is None:
        for field in sorted(_LIQ):
            builder.put(field, 0.0, pages[0], "")
        return
    quotes = item.get("quotes")
    quote = None
    if isinstance(quotes, list):
        dict_quotes = [q for q in quotes if isinstance(q, dict)]
        quote = next(
            (q for q in dict_quotes if q.get("symbol") == "USD"), dict_quotes[0] if dict_quotes else None
        )
    source = _page_of(pages, coin_id)
    if quote is None:
        builder.fail(_LIQ, "liquidation entry without a quote")
        return
    for field in sorted(_LIQ):
        side, window = field.removeprefix("liq_").split("_")
        builder.put(field, _float(quote.get(f"{side}_liquidations_{window}")), source, f"{field} missing")


def _page_of(pages: list[RawRecord], coin_id: int) -> RawRecord:
    for page in pages:
        data = cmc_data(page)
        entries = data.get("cryptocurrencies") if isinstance(data, dict) else None
        if isinstance(entries, list) and any(
            isinstance(e, dict) and e.get("crypto_id") == coin_id for e in entries
        ):
            return page
    return pages[0]
