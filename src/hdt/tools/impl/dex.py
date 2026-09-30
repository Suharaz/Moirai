"""`dex_security(address)` and `liquidity_changes(address)`: on-chain checks of the event coin's token.

Both read only the lake, identically in live and replay. The phase 02 recorder calls CMC DEX routes #24
(`dex_security_detail`) and #26 (`dex_liquidity_change`) on events (a new candidate or a held coin, at most
once per 24 h) with the token contract from `crypto_map` and stores them under key = the coin's CMC id,
the request params (`address`, `platformName` or `platform`) recorded with them. The council holds no CMC
key (vault scope `data` belongs to the recorder), so these tools never call CMC; when nothing successful
was recorded in the lookback window they answer `not_available`.

The `address` argument may be omitted (the event coin's recorded contract) or must name that contract;
any other address is refused, so an agent cannot probe arbitrary tokens. EVM addresses (0x...) compare
case-insensitively, others exactly. All strings from the response are sanitized.
"""

from __future__ import annotations

import asyncio
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar, Final

from pydantic import Field

from hdt.contracts.common import UtcDatetime
from hdt.contracts.forecast import LakeRef
from hdt.lake.pit_query import PitQuery
from hdt.lake.schemas import RawRecord
from hdt.tools.base import (
    NotAvailableError,
    ShortText,
    Text,
    Tool,
    ToolArgs,
    ToolContext,
    ToolData,
    ToolInputError,
    ToolMode,
    sanitize_text,
)
from hdt.tools.pit import PitView, record_json, record_params

SECURITY_ROUTE: Final[str] = "dex_security_detail"
LIQUIDITY_ROUTE: Final[str] = "dex_liquidity_change"
DEX_LOOKBACK: Final[timedelta] = timedelta(days=7)
DEX_STALE_AFTER: Final[timedelta] = timedelta(hours=25)
MAX_CHANGES: Final[int] = 50
MAX_SECURITY_ITEMS: Final[int] = 40
_STATUS_KEYS: Final = (
    "honeypotStatus",
    "mintableStatus",
    "freezableStatus",
    "rugPullStatus",
    "fakeTokenStatus",
    "unverifiedContractStatus",
)
_ADDRESS_PATTERN: Final[str] = r"^[A-Za-z0-9_:.-]{16,128}$"


class DexArgs(ToolArgs):
    address: str | None = Field(
        default=None, pattern=_ADDRESS_PATTERN, description="token contract; default the event coin's"
    )


def same_address(a: str, b: str) -> bool:
    if a.startswith("0x") and b.startswith("0x"):
        return a.lower() == b.lower()
    return a == b


def dex_payload(record: RawRecord) -> Any | None:
    """Body of a successful CMC DEX response: the `data` envelope when present, else the bare body."""
    if record.http_status != 200:
        return None
    body = record_json(record)
    if isinstance(body, dict):
        status = body.get("status")
        if isinstance(status, dict) and str(status.get("error_code", "0")) != "0":
            return None
        if "data" in body:
            return body["data"]
    return body


def recorded_dex(view: PitView, route: str, coin_id: int) -> tuple[RawRecord, Any]:
    """Newest successful capture of `route` for the coin in the lookback window, with its payload."""
    for record in reversed(view.series("cmc", route, view.horizon - DEX_LOOKBACK, key=str(coin_id))):
        payload = dex_payload(record)
        if payload is not None:
            return record, payload
    raise NotAvailableError(f"no successful {route} capture for coin {coin_id} in the 7 days before as_of")


def checked_address(record: RawRecord, requested: str | None) -> tuple[str, str]:
    """(platform, address) of the capture; a different requested address is refused."""
    params = record_params(record)
    address = str(params.get("address") or "")
    platform = str(params.get("platformName") or params.get("platform") or "")
    if not address:
        raise NotAvailableError("the capture does not record the token address it was made for")
    if requested is not None and not same_address(requested, address):
        raise ToolInputError("only the event coin's own token contract can be checked")
    return sanitize_text(platform, 64), address


def _text(value: Any, max_chars: int = 200) -> str | None:
    if value is None or isinstance(value, (dict, list)):
        return None
    return sanitize_text(str(value), max_chars) or None


def _float(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if abs(number) != float("inf") and number == number else None


def _bool(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def _entries(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [e for e in payload if isinstance(e, dict)]
    return [payload] if isinstance(payload, dict) else []


def _freshness(record: RawRecord, as_of: datetime) -> tuple[float, bool]:
    age = as_of - record.fetched_at
    return round(age.total_seconds() / 3600, 3), age > DEX_STALE_AFTER


class SecurityHit(ToolData):
    code: ShortText | None
    risk_code: ShortText | None
    risky_level: ShortText | None
    description: Text | None


class SecurityEntry(ToolData):
    platform: ShortText | None
    security_level: ShortText | None
    category_level: ShortText | None
    buy_tax: float | None
    sell_tax: float | None
    flagged_by_vendor: bool | None
    verified: bool | None
    reported: bool | None
    exist: bool | None
    statuses: dict[str, ShortText] = Field(
        description="honeypot, mintable, freezable, rug pull, ... statuses"
    )
    tags: tuple[ShortText, ...]
    hits: tuple[SecurityHit, ...] = Field(description="security items that were hit")
    items_checked: int


class DexSecurityData(ToolData):
    coin_id: int
    platform: str
    address: str
    age_h: float
    stale: bool
    entries: tuple[SecurityEntry, ...]
    source: LakeRef


class DexSecurityTool(Tool[DexArgs, DexSecurityData]):
    name: ClassVar[str] = "dex_security"
    description: ClassVar[str] = (
        "Recorded CMC DEX security check of the event coin's token contract: security level, taxes, "
        "honeypot/mint/freeze/rug-pull statuses and the risk items that were hit, with the capture age."
    )
    args_model = DexArgs
    data_model = DexSecurityData

    def __init__(self, mode: ToolMode, pit: PitQuery) -> None:
        super().__init__(mode)
        self._pit = pit

    async def run(self, ctx: ToolContext, args: DexArgs) -> DexSecurityData:
        return await asyncio.to_thread(
            self.security, PitView(self._pit, ctx.as_of), ctx.coin_id, args.address
        )

    def security(self, view: PitView, coin_id: int, address: str | None) -> DexSecurityData:
        record, payload = recorded_dex(view, SECURITY_ROUTE, coin_id)
        platform, recorded_address = checked_address(record, address)
        age_h, stale = _freshness(record, view.horizon)
        return DexSecurityData(
            coin_id=coin_id,
            platform=platform,
            address=recorded_address,
            age_h=age_h,
            stale=stale,
            entries=tuple(_security_entry(e) for e in _entries(payload)),
            source=LakeRef.of(record),
        )


def _security_entry(entry: dict[str, Any]) -> SecurityEntry:
    raw_extra = entry.get("extra")
    extra: dict[str, Any] = raw_extra if isinstance(raw_extra, dict) else {}
    display = entry.get("evmDisplay") or entry.get("solanaDisplay")
    statuses = {}
    if isinstance(display, dict):
        for key in _STATUS_KEYS:
            value = _text(display.get(key), 64)
            if value is not None:
                statuses[key.removesuffix("Status")] = value
    items = [i for i in entry.get("securityItems") or [] if isinstance(i, dict)]
    hits = [
        SecurityHit(
            code=_text(i.get("code")),
            risk_code=_text(i.get("riskCode")),
            risky_level=_text(i.get("riskyLevel")),
            description=_text(i.get("des"), 2000),
        )
        for i in items
        if i.get("isHit") is True
    ]
    tags = entry.get("tags")
    return SecurityEntry(
        platform=_text(entry.get("platformName")),
        security_level=_text(entry.get("securityLevel")),
        category_level=_text(entry.get("categoryLevel")),
        buy_tax=_float(extra.get("buyTax")),
        sell_tax=_float(extra.get("sellTax")),
        flagged_by_vendor=_bool(extra.get("isFlaggedByVendor")),
        verified=_bool(extra.get("isVerified")),
        reported=_bool(extra.get("isReported")),
        exist=_bool(entry.get("exist")),
        statuses=statuses,
        tags=tuple(t for t in (_text(x, 64) for x in tags) if t) if isinstance(tags, list) else (),
        hits=tuple(hits[:MAX_SECURITY_ITEMS]),
        items_checked=len(items),
    )


class LiquidityChange(ToolData):
    ts: UtcDatetime | None
    tp: ShortText | None = Field(description="change type as CMC reports it")
    exchange: ShortText | None
    token0: ShortText | None
    token1: ShortText | None
    amount0: float | None
    amount1: float | None
    tu: float | None = Field(description="value of the change as CMC reports it (USD)")
    tx: ShortText | None


class TypeTotal(ToolData):
    count: int
    tu_sum: float


class LiquidityChangesData(ToolData):
    coin_id: int
    platform: str
    address: str
    age_h: float
    stale: bool
    total_changes: int
    by_type: dict[str, TypeTotal]
    changes: tuple[LiquidityChange, ...] = Field(description="newest first, at most 50")
    source: LakeRef


class LiquidityChangesTool(Tool[DexArgs, LiquidityChangesData]):
    name: ClassVar[str] = "liquidity_changes"
    description: ClassVar[str] = (
        "Recorded CMC DEX liquidity add/remove events of the event coin's token (newest first), with totals "
        "per change type and the capture age. Large removals can signal a rug pull or an exploit."
    )
    args_model = DexArgs
    data_model = LiquidityChangesData

    def __init__(self, mode: ToolMode, pit: PitQuery) -> None:
        super().__init__(mode)
        self._pit = pit

    async def run(self, ctx: ToolContext, args: DexArgs) -> LiquidityChangesData:
        return await asyncio.to_thread(self.changes, PitView(self._pit, ctx.as_of), ctx.coin_id, args.address)

    def changes(self, view: PitView, coin_id: int, address: str | None) -> LiquidityChangesData:
        record, payload = recorded_dex(view, LIQUIDITY_ROUTE, coin_id)
        platform, recorded_address = checked_address(record, address)
        age_h, stale = _freshness(record, view.horizon)
        raw = payload.get("lcs") if isinstance(payload, dict) else payload
        changes = [_change(c) for c in raw if isinstance(c, dict)] if isinstance(raw, list) else []
        epoch = datetime.min.replace(tzinfo=UTC)
        changes.sort(key=lambda c: (c.ts or epoch, c.tx or ""), reverse=True)
        counts: Counter[str] = Counter()
        sums: defaultdict[str, float] = defaultdict(float)
        for change in changes:
            kind = change.tp or "unknown"
            counts[kind] += 1
            sums[kind] += change.tu or 0.0
        return LiquidityChangesData(
            coin_id=coin_id,
            platform=platform,
            address=recorded_address,
            age_h=age_h,
            stale=stale,
            total_changes=len(changes),
            by_type={k: TypeTotal(count=counts[k], tu_sum=round(sums[k], 2)) for k in sorted(counts)},
            changes=tuple(changes[:MAX_CHANGES]),
            source=LakeRef.of(record),
        )


def _change(item: dict[str, Any]) -> LiquidityChange:
    ts = item.get("ts")
    when = None
    if isinstance(ts, (int, float)) and not isinstance(ts, bool) and ts > 0:
        when = datetime.fromtimestamp(ts / 1000 if ts > 1e11 else ts, tz=UTC)
    return LiquidityChange(
        ts=when,
        tp=_text(item.get("tp"), 32),
        exchange=_text(item.get("en"), 64),
        token0=_text(item.get("t0s"), 32),
        token1=_text(item.get("t1s"), 32),
        amount0=_float(item.get("a0")),
        amount1=_float(item.get("a1")),
        tu=_float(item.get("tu")),
        tx=_text(item.get("txId") or item.get("h"), 128),
    )
