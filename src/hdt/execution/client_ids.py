"""Deterministic exchange client ids: `base32(sha256(event_id))[:20]-{leg}-{seq}`.

Regular orders use it as `newClientOrderId`, conditional orders as `clientAlgoId` (separate namespaces on
Binance). The id is a pure function of the event, the leg and the attempt number, so a retry after a
crash re-sends the same id and the exchange rejects a duplicate instead of opening twice. `seq` grows per
attempt/re-placement and is always derived from the ledger (`next_seq`).
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Final

from hdt.contracts.common import Leg
from hdt.contracts.order import CLIENT_ID_PATTERN
from hdt.core.ids import b32_digest

PREFIX_LEN: Final[int] = 20
MAX_LEN: Final[int] = 36
MAX_SEQ: Final[int] = 99_999  # 20 + "-entry_ioc-" (11) + 5 digits = 36 characters
_PARSE: Final = re.compile(rf"^([A-Z2-7]{{{PREFIX_LEN}}})-([a-z_0-9]+)-(\d{{1,5}})$")
_ALLOWED: Final = re.compile(CLIENT_ID_PATTERN)


@dataclass(frozen=True)
class ParsedClientId:
    prefix: str
    leg: Leg
    seq: int


def event_prefix(event_id: str) -> str:
    if not event_id:
        raise ValueError("event_id must not be empty")
    return b32_digest(event_id, PREFIX_LEN)


def client_id(event_id: str, leg: Leg, seq: int) -> str:
    if not 0 <= seq <= MAX_SEQ:
        raise ValueError(f"seq must be in [0, {MAX_SEQ}]")
    value = f"{event_prefix(event_id)}-{Leg(leg).value}-{seq}"
    if len(value) > MAX_LEN or not _ALLOWED.fullmatch(value):  # pragma: no cover - guarded by MAX_SEQ
        raise ValueError(f"client id {value!r} violates the Binance format")
    return value


def parse_client_id(value: str) -> ParsedClientId | None:
    """Inverse of `client_id` (None for ids not generated here, e.g. exchange-generated ones)."""
    match = _PARSE.fullmatch(value)
    if match is None:
        return None
    try:
        leg = Leg(match.group(2))
    except ValueError:
        return None
    return ParsedClientId(match.group(1), leg, int(match.group(3)))


def belongs_to(value: str, event_id: str) -> bool:
    parsed = parse_client_id(value)
    return parsed is not None and parsed.prefix == event_prefix(event_id)


def next_seq(existing: Iterable[str], event_id: str, leg: Leg) -> int:
    """First unused seq for (event, leg) given every client id already in the ledger."""
    prefix = event_prefix(event_id)
    used = [
        parsed.seq
        for parsed in (parse_client_id(v) for v in existing)
        if parsed is not None and parsed.prefix == prefix and parsed.leg is leg
    ]
    seq = max(used) + 1 if used else 0
    if seq > MAX_SEQ:
        raise ValueError(f"no client id left for {leg.value} of {event_id}")
    return seq
