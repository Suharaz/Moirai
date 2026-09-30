"""Hard evidence policy (Design Contract section 3), versioned: which verified claims are `hard`.

`hard` is decided here by code, never by the LLM and never by the claim's source type alone. A claim is
hard only through one of three branches, and its `direction_hint` is then derived by code from the same
rule (the LLM's self-declared hint is replaced):

- (a) a `url` claim about a T0 event of a registered class (listing, delist, exploit, unlock) whose stored
  copy passed `check_official` (`StoredSource.official`) at tier T0; direction from `EVENT_CLASS_DIRECTION`;
- (b) an on-chain event (a `tool` claim on an on-chain tool result) independently confirmed by >= 2 data
  sources in the same direction. The direction of an on-chain result comes from the code rule of its tool
  (`ONCHAIN_RULES`: a vendor-flagged honeypot / rug pull or a high-risk item for `dex_security`, NET
  liquidity removals (removals minus adds) of at least `ONCHAIN_DRAIN_USD_MIN` within
  `ONCHAIN_DRAIN_WINDOW` before the capture for `liquidity_changes`; a stale capture gives none), never
  from the claim. Independence counts data VENDORS (the lake source of the result, e.g. every CMC DEX route
  is one vendor), not routes. A verified `url` claim confirms the on-chain event only when it describes
  the same event: its stored copy's code-classified event class is one `ONCHAIN_CONFIRMING_CLASSES` lists
  for that tool (an exploit for a drain or a security flag; never an unlock or a listing), its verdict
  names the event coin (`StoredSource.event_coin_ids`) and it comes from tier T0 / T1 or was confirmed
  official. Such url claims count as one more source, all of them together as one (one piece of evidence);
- (c) a `packet` claim on a registered hard signal whose sign rule fires on the committed packet (for
  example the LTX strict trigger with its side); the direction comes from the rule.

Packet claims outside `HARD_SIGNALS`, even with correct numbers, are never hard. A change of any table in
this module requires a new `POLICY_VERSION` (recorded on every decision card).
"""

from __future__ import annotations

import math
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from types import MappingProxyType
from typing import Any, Final

from hdt.contracts.common import ClaimKind, DirectionHint, Tier
from hdt.contracts.packet import FeatureValue

POLICY_VERSION: Final[str] = "hard-evidence-v3"

EVENT_CLASS_DIRECTION: Final[Mapping[str, DirectionHint]] = MappingProxyType(
    {
        "listing": DirectionHint.UP,
        "delist": DirectionHint.DOWN,
        "exploit": DirectionHint.DOWN,
        "unlock": DirectionHint.DOWN,
    }
)
"""Branch (a): registered T0 event classes and the price direction each one implies."""

ONCHAIN_TOOLS: Final[frozenset[str]] = frozenset({"dex_security", "liquidity_changes"})
"""Branch (b): tools whose results describe on-chain events (each result names its lake source)."""

MIN_ONCHAIN_SOURCES: Final[int] = 2
ONCHAIN_DRAIN_USD_MIN: Final[float] = 250_000.0
"""Net liquidity removals (USD, removals minus adds) at or above this are an on-chain drain (price DOWN)."""
ONCHAIN_DRAIN_WINDOW: Final[timedelta] = timedelta(hours=24)
"""Only liquidity changes this close before the capture (`source.fetched_at`) count toward a drain."""
ONCHAIN_CONFIRMING_CLASSES: Final[Mapping[str, frozenset[str]]] = MappingProxyType(
    {"dex_security": frozenset({"exploit"}), "liquidity_changes": frozenset({"exploit"})}
)
"""Branch (b): the news event classes that describe the same on-chain event as each on-chain tool."""
URL_CONFIRMING_TIERS: Final[frozenset[Tier]] = frozenset({Tier.T0, Tier.T1})
"""Branch (b): a url confirmation needs one of these tiers, or an official confirmation."""
SECURITY_HIGH_LEVELS: Final[frozenset[str]] = frozenset({"high", "critical", "danger", "severe"})
SECURITY_FLAGGED_STATUSES: Final[frozenset[str]] = frozenset({"honeypot", "rugPull"})
SECURITY_FLAGGED_VALUES: Final[frozenset[str]] = frozenset({"1", "true", "yes", "high", "risky", "danger"})


def _flagged(value: Any) -> bool:
    return str(value if value is not None else "").strip().lower() in SECURITY_FLAGGED_VALUES


def _security_direction(data: Mapping[str, Any]) -> DirectionHint:
    """`dex_security`: DOWN when a vendor flagged the token, a honeypot / rug-pull status is set or a
    security item was hit at a high level; otherwise no direction (a clean check implies none)."""
    entries = data.get("entries")
    for entry in entries if isinstance(entries, list | tuple) else ():
        if not isinstance(entry, Mapping):
            continue
        if entry.get("flagged_by_vendor") is True:
            return DirectionHint.DOWN
        statuses = entry.get("statuses")
        if isinstance(statuses, Mapping) and any(
            _flagged(statuses.get(key)) for key in SECURITY_FLAGGED_STATUSES
        ):
            return DirectionHint.DOWN
        hits = entry.get("hits")
        for hit in hits if isinstance(hits, list | tuple) else ():
            level = hit.get("risky_level") if isinstance(hit, Mapping) else None
            if str(level or "").strip().lower() in SECURITY_HIGH_LEVELS:
                return DirectionHint.DOWN
    return DirectionHint.NONE


def _when(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else None
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def _usd(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float) or not math.isfinite(value):
        return None
    return abs(float(value))


def _liquidity_direction(data: Mapping[str, Any]) -> DirectionHint:
    """`liquidity_changes`: DOWN when the net removals (removals minus adds, USD) of the changes within
    `ONCHAIN_DRAIN_WINDOW` before the capture reach `ONCHAIN_DRAIN_USD_MIN`. Changes without a time, and a
    result without a capture time, count for nothing (routine two-way LP churn is not a drain)."""
    source = data.get("source")
    end = _when(source.get("fetched_at")) if isinstance(source, Mapping) else None
    changes = data.get("changes")
    if end is None or not isinstance(changes, list | tuple):
        return DirectionHint.NONE
    start = end - ONCHAIN_DRAIN_WINDOW
    net = 0.0
    for change in changes:
        if not isinstance(change, Mapping):
            continue
        at, usd = _when(change.get("ts")), _usd(change.get("tu"))
        if at is None or usd is None or not start <= at <= end:
            continue
        kind = str(change.get("tp") or "").lower()
        if "remove" in kind:
            net += usd
        elif "add" in kind:
            net -= usd
    return DirectionHint.DOWN if net >= ONCHAIN_DRAIN_USD_MIN else DirectionHint.NONE


ONCHAIN_RULES: Final[Mapping[str, Callable[[Mapping[str, Any]], DirectionHint]]] = MappingProxyType(
    {"dex_security": _security_direction, "liquidity_changes": _liquidity_direction}
)
"""Branch (b): the code direction of each on-chain tool's result data."""


def onchain_direction(tool: str | None, data: Any) -> DirectionHint | None:
    """Branch (b): the code direction of an on-chain tool result, or None (not on-chain, stale, no signal)."""
    rule = ONCHAIN_RULES.get(tool or "")
    if rule is None or not isinstance(data, Mapping) or data.get("stale") is not False:
        return None
    direction = rule(data)
    return direction if direction is not DirectionHint.NONE else None


def result_vendor(data: Any) -> str | None:
    """The data vendor of a tool result: the lake source of its provenance (`source.source`)."""
    source = data.get("source") if isinstance(data, Mapping) else None
    vendor = source.get("source") if isinstance(source, Mapping) else None
    return vendor.strip().lower() if isinstance(vendor, str) and vendor.strip() else None


def _side_rule(pass_field: str, side_field: str) -> Callable[[Mapping[str, FeatureValue]], DirectionHint]:
    def rule(features: Mapping[str, FeatureValue]) -> DirectionHint:
        if features.get(pass_field) is not True:
            return DirectionHint.NONE
        side = features.get(side_field)
        if side == "LONG":
            return DirectionHint.UP
        if side == "SHORT":
            return DirectionHint.DOWN
        return DirectionHint.NONE

    return rule


@dataclass(frozen=True)
class HardSignal:
    """A packet field registered as a hard signal, with the sign rule that gives its direction."""

    fields: frozenset[str]
    """Packet feature names a claim may cite for this signal (the trigger and its side)."""
    rule: Callable[[Mapping[str, FeatureValue]], DirectionHint]
    """Direction derived from the committed packet; NONE when the signal did not fire."""


HARD_SIGNALS: Final[Mapping[str, HardSignal]] = MappingProxyType(
    {
        "ltx_strict": HardSignal(
            frozenset({"ltx_strict_pass", "ltx_side"}), _side_rule("ltx_strict_pass", "ltx_side")
        ),
        "migration_strict": HardSignal(
            frozenset({"migration_strict_pass", "migration_side"}),
            _side_rule("migration_strict_pass", "migration_side"),
        ),
    }
)


def packet_direction(field: str, features: Mapping[str, FeatureValue]) -> DirectionHint | None:
    """Branch (c): the code direction of a packet claim on `field`, or None when it is not hard."""
    for signal in HARD_SIGNALS.values():
        if field in signal.fields:
            direction = signal.rule(features)
            return direction if direction is not DirectionHint.NONE else None
    return None


def url_direction(*, event_class: str | None, official: bool, tier: Tier) -> DirectionHint | None:
    """Branch (a): the code direction of a verified url claim, or None when it is not hard."""
    if event_class is None or not official or tier is not Tier.T0:
        return None
    return EVENT_CLASS_DIRECTION.get(event_class)


def url_event_direction(
    *, event_class: str | None, tier: Tier, official: bool, event_coin_ids: Iterable[int], coin_id: int
) -> DirectionHint | None:
    """Branch (b) confirmation: the direction of a verified url claim that may confirm an on-chain event, or
    None: its stored copy has a registered class, its verdict names the event coin and it is tier T0 / T1
    or official. Whether the class describes the same on-chain event is checked per tool
    (`ONCHAIN_CONFIRMING_CLASSES`)."""
    if event_class is None or coin_id not in set(event_coin_ids):
        return None
    if tier not in URL_CONFIRMING_TIERS and not official:
        return None
    return EVENT_CLASS_DIRECTION.get(event_class)


@dataclass(frozen=True)
class EvidenceItem:
    """One verified claim as the on-chain confirmation rule sees it; every field is computed by code."""

    key: str
    kind: ClaimKind
    direction: DirectionHint | None
    """Code direction: `onchain_direction` for tool claims, `url_event_direction` for url claims."""
    tool: str | None = None
    vendor: str | None = None
    """Data vendor (lake source) of the tool result the claim cites."""
    event_class: str | None = None
    """Url claims: the stored copy's code-classified event class."""


def independent_sources(items: Iterable[EvidenceItem], direction: DirectionHint, tool: str) -> int:
    """Independent pieces of evidence for the on-chain event of `tool` in `direction`: distinct on-chain
    data vendors, plus one for any number of url claims of a class that describes the same event."""
    if direction is DirectionHint.NONE:
        return 0
    classes = ONCHAIN_CONFIRMING_CLASSES.get(tool, frozenset())
    vendors: set[str] = set()
    has_url = False
    for item in items:
        if item.direction is not direction:
            continue
        if item.kind is ClaimKind.URL:
            has_url = has_url or item.event_class in classes
        elif item.kind is ClaimKind.TOOL and item.tool in ONCHAIN_TOOLS and item.vendor is not None:
            vendors.add(item.vendor)
    return len(vendors) + (1 if has_url else 0)


def onchain_hard_keys(items: Iterable[EvidenceItem]) -> set[str]:
    """Branch (b): keys of on-chain tool claims confirmed by >= 2 independent sources in their code
    direction."""
    pool = list(items)
    hard: set[str] = set()
    for item in pool:
        if item.kind is not ClaimKind.TOOL or item.tool not in ONCHAIN_TOOLS or item.vendor is None:
            continue
        if item.direction is None or item.direction is DirectionHint.NONE:
            continue
        if independent_sources(pool, item.direction, item.tool or "") >= MIN_ONCHAIN_SOURCES:
            hard.add(item.key)
    return hard
