"""On-chain evidence of an incident from the recorded CMC DEX routes (#24 security, #26 liquidity changes).

The recorder stores both routes under key = CMC id (`hdt.tools.impl.dex`). This module reads, point in
time, every successful capture recorded in a window and summarizes it:
- `removals_usd`: total USD value of the liquidity removals with a timestamp in the window, over every
  liquidity capture of the window (a change listed by several captures counts once), so a drain listed
  only by a capture taken before a later, shorter one is never lost;
- `changes_seen`: the distinct liquidity changes of the window (0 means no liquidity data at all);
- `security_flags`: security items hit at a high / critical level and rug-pull / honeypot statuses in the
  newest security capture;
- `liquidity`: per liquidity capture, when it was fetched and what it lists over the window.
It confirms an exploit (`incident`) or, with one liquidity capture recorded shortly after a panic headline
that itself lists real changes reaching back to before the headline (or its complete list), and no
incident over the look-back, is independent evidence that no incident happened (Fade panic,
`hdt.news.modes`). An empty or truncated post-headline capture never refutes (fail closed).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Final

from hdt.contracts.forecast import LakeRef
from hdt.core.clock import ensure_utc
from hdt.lake.pit_query import PitQuery
from hdt.tools.base import ToolBackendError
from hdt.tools.impl.dex import LIQUIDITY_ROUTE, SECURITY_ROUTE, dex_payload

HIGH_LEVELS: Final[frozenset[str]] = frozenset({"high", "critical", "danger", "severe"})
DEFAULT_LIQUIDITY_LIMIT: Final[int] = 100
"""Changes per liquidity capture when the recorded params carry no `limit` (the recorder asks for 100)."""
FLAGGED_STATUS_KEYS: Final[tuple[str, ...]] = ("rugPullStatus", "honeypotStatus")
FLAGGED_STATUS_VALUES: Final[frozenset[str]] = frozenset({"1", "true", "yes", "high", "risky", "danger"})


@dataclass(frozen=True)
class LiquidityCapture:
    """One successful liquidity capture as the refutation rule sees it."""

    fetched_at: datetime
    listed_in_window: int
    """Changes this capture lists with a timestamp in the window."""
    oldest_listed: datetime | None
    """Oldest such change (None when it lists none)."""
    complete: bool
    """The capture listed fewer changes than the page limit, so nothing older was cut off."""

    def covers_since(self, headline: datetime) -> bool:
        """This capture alone shows the pool's changes from before `headline` up to its fetch."""
        if self.listed_in_window == 0 or self.oldest_listed is None:
            return False
        return self.oldest_listed <= headline or self.complete


@dataclass(frozen=True)
class OnchainState:
    coin_id: int
    captured_at: datetime
    """`fetched_at` of the newest capture used (liquidity capture when present)."""
    removals_usd: float
    changes_seen: int
    security_flags: tuple[str, ...]
    refs: tuple[LakeRef, ...]
    """Captures that listed a removal first (the evidence of a drain), then the newest ones."""
    liquidity: tuple[LiquidityCapture, ...] = ()
    """Every successful liquidity capture read, oldest first."""

    def incident(self, drain_usd_min: float) -> bool:
        return self.removals_usd >= drain_usd_min or bool(self.security_flags)

    def covered_after(self, headline: datetime, end: datetime) -> bool:
        """A liquidity capture fetched in `[headline, end]` itself covers the span since before `headline`
        (evaluated per capture: an empty post-headline capture never borrows an older capture's data)."""
        headline, end = ensure_utc(headline), ensure_utc(end)
        return any(headline <= c.fetched_at <= end and c.covers_since(headline) for c in self.liquidity)


def onchain_state(pit: PitQuery, coin_id: int, start: datetime, as_of: datetime) -> OnchainState | None:
    """Summary of the DEX captures of `coin_id` recorded in `[start, as_of]` (None when none); liquidity
    changes count when their own timestamp is in the same window."""
    start, as_of = ensure_utc(start), ensure_utc(as_of)
    key = str(coin_id)
    liquidity = _successful(pit, LIQUIDITY_ROUTE, key, start, as_of)
    security = _newest(pit, SECURITY_ROUTE, key, start, as_of)
    if not liquidity and security is None:
        return None
    removals = 0.0
    seen: set[tuple[Any, ...]] = set()
    drain_refs: list[LakeRef] = []
    captures: list[LiquidityCapture] = []
    for record, payload in liquidity:
        raw = payload.get("lcs") if isinstance(payload, dict) else payload
        listed = raw if isinstance(raw, list) else []
        listed_removal = False
        in_window = 0
        oldest: datetime | None = None
        for change in listed:
            if not isinstance(change, dict):
                continue
            ts = _ts(change.get("ts"))
            if ts is None or not start <= ts <= as_of:
                continue
            in_window += 1
            oldest = ts if oldest is None else min(oldest, ts)
            identity = _identity(change, ts)
            if identity in seen:
                continue
            seen.add(identity)
            if "remove" in str(change.get("tp") or "").lower():
                amount = _usd(change.get("tu"))
                removals += amount
                listed_removal = listed_removal or amount > 0
        if listed_removal:
            drain_refs.append(LakeRef.of(record))
        captures.append(
            LiquidityCapture(
                ensure_utc(record.fetched_at), in_window, oldest, len(listed) < _page_limit(record)
            )
        )
    refs = [*drain_refs]
    captured: datetime | None = None
    if liquidity:
        newest = LakeRef.of(liquidity[-1][0])
        if newest not in refs:
            refs.append(newest)
        captured = liquidity[-1][0].fetched_at
    flags: list[str] = []
    if security is not None:
        record, payload = security
        refs.append(LakeRef.of(record))
        captured = captured or record.fetched_at
        for entry in payload if isinstance(payload, list) else [payload]:
            if isinstance(entry, dict):
                flags.extend(_security_flags(entry))
    assert captured is not None
    return OnchainState(
        coin_id,
        captured,
        round(removals, 2),
        len(seen),
        tuple(sorted(set(flags))),
        tuple(refs),
        tuple(captures),
    )


def _identity(change: dict[str, Any], ts: datetime) -> tuple[Any, ...]:
    """Key of one liquidity change across captures (the transaction hash when the route lists one)."""
    tx = change.get("txId") or change.get("h")
    if isinstance(tx, str) and tx:
        return ("tx", tx, str(change.get("tp") or ""))
    return ("change", ts, str(change.get("tp") or ""), _usd(change.get("tu")))


def _successful(
    pit: PitQuery, route: str, key: str, start: datetime, as_of: datetime
) -> list[tuple[Any, Any]]:
    out: list[tuple[Any, Any]] = []
    for record in pit.series("cmc", route, start, as_of, as_of=as_of, key=key):
        try:
            payload = dex_payload(record)
        except ToolBackendError:
            continue
        if payload is not None:
            out.append((record, payload))
    return out


def _newest(pit: PitQuery, route: str, key: str, start: datetime, as_of: datetime) -> tuple[Any, Any] | None:
    for record in reversed(pit.series("cmc", route, start, as_of, as_of=as_of, key=key)):
        try:
            payload = dex_payload(record)
        except ToolBackendError:
            continue
        if payload is not None:
            return record, payload
    return None


def _page_limit(record: Any) -> int:
    try:
        params = json.loads(getattr(record, "params_json", None) or "{}")
        limit = int(params.get("limit", DEFAULT_LIQUIDITY_LIMIT)) if isinstance(params, dict) else 0
    except (TypeError, ValueError):
        return DEFAULT_LIQUIDITY_LIMIT
    return limit if limit > 0 else DEFAULT_LIQUIDITY_LIMIT


def _ts(value: Any) -> datetime | None:
    """Epoch seconds or milliseconds as UTC; None for anything not a representable positive time."""
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not value > 0:
        return None
    try:
        return datetime.fromtimestamp(value / 1000 if value > 1e11 else value, tz=UTC)
    except (OverflowError, OSError, ValueError):
        return None


def _usd(value: Any) -> float:
    if isinstance(value, bool):
        return 0.0
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return abs(number) if number == number and abs(number) != float("inf") else 0.0


def _security_flags(entry: dict[str, Any]) -> list[str]:
    flags: list[str] = []
    for item in entry.get("securityItems") or []:
        if isinstance(item, dict) and item.get("isHit") is True:
            level = str(item.get("riskyLevel") or "").lower()
            if level in HIGH_LEVELS:
                flags.append(f"hit:{item.get('code') or item.get('riskCode') or 'unknown'}")
    display = entry.get("evmDisplay") or entry.get("solanaDisplay")
    if isinstance(display, dict):
        for key in FLAGGED_STATUS_KEYS:
            if str(display.get(key) or "").lower() in FLAGGED_STATUS_VALUES:
                flags.append(f"status:{key.removesuffix('Status')}")
    return flags
