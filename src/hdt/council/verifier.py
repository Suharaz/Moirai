"""Claim verifier (pure code; Design Contract section 3). `verified`, `hard`, `tier`, `domain_url` are
assigned here; whatever the LLM declared for them is ignored.

Per claim kind:
- `packet`: the cited field must exist in the agent's committed packet (loaded by the forecast's
  `packet_sha256`, which the store re-hashes); a numeric value must be within 1 % of the packet value, any
  other value must be equal; a claim without a value gets the packet value;
- `tool`: `ref` must be the `result_id` of an `ok` tool result returned to that agent in that round, and the
  claim must carry a value that matches a content leaf of the result data (numbers within 1 %, strings
  normalized). Provenance and identity leaves (`source`, hashes, versions, `p_model`, ...) and booleans are
  never content, so a tool claim cannot be "verified" by an HTTP status or a flag;
- `url`: `ref` is a news item_id whose stored copy exists at `as_of` (or was pinned by the live meeting);
  the quote (>= `MIN_QUOTE_CHARS`) must appear in the stored text; `tier` and `domain_url` come from the
  stored copy (computed by phase 07 code), never from the claim.
Every claim is also rejected when its statement, quote, text value or ref reads like an instruction
(checked on the sanitized text an agent would read), when a text value is longer than `VALUE_MAX_CHARS`,
or when the same claim (kind + ref + statement + value) was shared in an earlier round or already accepted
from another agent this round. A wrong claim (missing source, value mismatch, fabricated quote) carries a
penalty for its source agent. `hard` then follows `hdt.council.hard_evidence`; the direction of a hard
claim always comes from its code rule.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final

from hdt.contracts.common import ClaimKind, DirectionHint
from hdt.contracts.forecast import AgentForecast, Claim, LakeRef
from hdt.contracts.packet import FeatureValue, QuantPacket
from hdt.council import hard_evidence
from hdt.council.claims import (
    VALUE_MAX_CHARS,
    claim_hash,
    instruction_like,
    normalize_text,
    packet_field,
)
from hdt.council.ports import SourceLookup, StoredSource

REL_TOLERANCE: Final[float] = 0.01
ABS_TOLERANCE: Final[float] = 1e-12
MIN_QUOTE_CHARS: Final[int] = 12

# reject reasons (decision_claims.reject_reason)
REPEATED: Final[str] = "repeated"
INSTRUCTION_LIKE: Final[str] = "instruction_like"
PACKET_MISSING: Final[str] = "packet_missing"
FIELD_MISSING: Final[str] = "field_missing"
VALUE_MISMATCH: Final[str] = "value_mismatch"
TOOL_RESULT_MISSING: Final[str] = "tool_result_missing"
VALUE_MISSING: Final[str] = "value_missing"
VALUE_TOO_LONG: Final[str] = "value_too_long"
SOURCE_MISSING: Final[str] = "source_missing"
QUOTE_MISSING: Final[str] = "quote_missing"
QUOTE_NOT_FOUND: Final[str] = "quote_not_found"
PENALIZED: Final[frozenset[str]] = frozenset(
    {FIELD_MISSING, VALUE_MISMATCH, TOOL_RESULT_MISSING, SOURCE_MISSING, QUOTE_MISSING, QUOTE_NOT_FOUND}
)
NON_CONTENT_KEYS: Final[frozenset[str]] = frozenset(
    {
        "source",
        "sources",
        "refs",
        "fetched_at",
        "http_status",
        "body_sha256",
        "route",
        "key",
        "coin_id",
        "as_of",
        "agent",
        "address",
        "platform",
        "age_h",
        "stale",
        "packet_sha256",
        "candidate_set_sha256",
        "config_version_ids",
        "universe_date",
        "feature_ver",
        "p_model_ver",
        "label_spec_version",
        "target_type",
        "schema_version",
        "data_quality",
        "p_model",
    }
)
"""Keys of tool result data (at any depth) whose values are provenance, identity or the agent's own model
output, never evidence a tool claim may cite."""


@dataclass(frozen=True)
class AgentEvidence:
    """What one agent's claims of one round are checked against."""

    agent: str
    forecast: AgentForecast
    packet: QuantPacket | None
    """The committed packet of `forecast.packet_sha256` (None for the News agent or when absent)."""
    tool_results: Mapping[str, Mapping[str, Any]]
    """`result_id` -> `{"tool": name, "data": {...}}` of the `ok` results returned in this round."""


@dataclass(frozen=True)
class VerifiedClaim:
    round: int
    source_agent: str
    original_id: str
    claim: Claim
    """With the code-assigned fields (and the packet value filled in when the claim had none)."""
    claim_sha256: str
    reject_reason: str | None
    penalty: bool

    @property
    def accepted(self) -> bool:
        return self.reject_reason is None


def _number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _close(claimed: float, actual: float) -> bool:
    return abs(claimed - actual) <= max(REL_TOLERANCE * abs(actual), ABS_TOLERANCE)


def value_matches(claimed: float | str, actual: FeatureValue | Any) -> bool:
    """1 % relative tolerance for numbers; booleans accept true/false/1/0; strings compare normalized."""
    if isinstance(actual, bool):
        if isinstance(claimed, str):
            return normalize_text(claimed) in (("true", "yes", "1") if actual else ("false", "no", "0"))
        return claimed == (1.0 if actual else 0.0)
    number = _number(actual)
    if number is not None:
        if isinstance(claimed, str):
            try:
                return _close(float(claimed), number)
            except ValueError:
                return False
        return _close(float(claimed), number)
    if isinstance(actual, str):
        return isinstance(claimed, str) and normalize_text(claimed) == normalize_text(actual)
    return False


def _leaves(data: Any) -> Iterable[Any]:
    """Content leaves of tool result data: numbers and strings outside `NON_CONTENT_KEYS` (no booleans)."""
    if isinstance(data, Mapping):
        for key, item in data.items():
            if str(key) not in NON_CONTENT_KEYS:
                yield from _leaves(item)
    elif isinstance(data, list | tuple):
        for item in data:
            yield from _leaves(item)
    elif isinstance(data, str) or _number(data) is not None:
        yield data


@dataclass
class _Checked:
    claim: Claim
    reason: str | None
    hard_direction: DirectionHint | None = None
    """Branch (a) / (c): the code direction when the claim is hard by its own rule."""
    confirms: DirectionHint | None = None
    """Branch (b): the code direction this claim confirms (on-chain tool result, url event class)."""
    tool: str | None = None
    vendor: str | None = None


def _packet(claim: Claim, packet: QuantPacket | None) -> _Checked:
    if packet is None:
        return _Checked(claim, PACKET_MISSING)
    name = packet_field(claim.ref)
    if name not in packet.features:
        return _Checked(claim, FIELD_MISSING)
    actual = packet.features[name]
    if claim.value is None:
        filled: float | str | None = actual if isinstance(actual, str) else _number(actual)
        if isinstance(actual, bool):
            filled = "true" if actual else "false"
        claim = claim.model_copy(update={"value": filled})
    elif not value_matches(claim.value, actual):
        return _Checked(claim, VALUE_MISMATCH)
    return _Checked(claim, None, hard_evidence.packet_direction(name, packet.features))


def _tool(claim: Claim, results: Mapping[str, Mapping[str, Any]]) -> _Checked:
    result = results.get(claim.ref.strip())
    if result is None:
        return _Checked(claim, TOOL_RESULT_MISSING)
    if claim.value is None:
        return _Checked(claim, VALUE_MISSING)
    data = result.get("data")
    if not any(value_matches(claim.value, leaf) for leaf in _leaves(data)):
        return _Checked(claim, VALUE_MISMATCH)
    raw_tool = result.get("tool")
    tool = raw_tool if isinstance(raw_tool, str) else None
    return _Checked(
        claim,
        None,
        None,
        hard_evidence.onchain_direction(tool, data),
        tool,
        hard_evidence.result_vendor(data),
    )


def _url(
    claim: Claim, sources: SourceLookup, as_of: datetime, pinned: tuple[LakeRef, ...], coin_id: int
) -> tuple[_Checked, StoredSource | None]:
    stored = sources.stored_source(claim.ref.strip(), as_of=as_of, pinned=pinned, coin_id=coin_id)
    if stored is None:
        return _Checked(claim, SOURCE_MISSING), None
    quote = normalize_text(claim.quote or "")
    if len(quote) < MIN_QUOTE_CHARS:
        return _Checked(claim, QUOTE_MISSING), stored
    if quote not in normalize_text(stored.text):
        return _Checked(claim, QUOTE_NOT_FOUND), stored
    direction = hard_evidence.url_direction(
        event_class=stored.event_class, official=stored.official, tier=stored.tier
    )
    confirms = hard_evidence.url_event_direction(
        event_class=stored.event_class,
        tier=stored.tier,
        official=stored.official,
        event_coin_ids=stored.event_coin_ids,
        coin_id=coin_id,
    )
    return _Checked(claim, None, direction, confirms), stored


def _screen(claim: Claim) -> str | None:
    """Reasons that reject a claim whatever its kind: an instruction-like text or an oversized text value."""
    value = claim.value if isinstance(claim.value, str) else None
    if instruction_like(claim.statement, claim.quote, value, claim.ref):
        return INSTRUCTION_LIKE
    if value is not None and len(value) > VALUE_MAX_CHARS:
        return VALUE_TOO_LONG
    return None


def verify_round(
    evidence: Sequence[AgentEvidence],
    *,
    round_: int,
    sources: SourceLookup,
    as_of: datetime,
    pinned: tuple[LakeRef, ...],
    already_shared: Iterable[str],
) -> list[VerifiedClaim]:
    """Verify every claim of one round, agents in the given order (deterministic). Only an accepted claim
    blocks a later identical one (a rejected claim never shadows another agent's valid one)."""
    seen = set(already_shared)
    checked: list[tuple[str, Claim, str, _Checked, StoredSource | None]] = []
    for item in evidence:
        for raw in item.forecast.claims:
            base = raw.model_copy(update={"verified": False, "hard": False, "tier": None, "domain_url": None})
            digest = claim_hash(base)
            stored: StoredSource | None = None
            screened = _screen(base)
            if digest in seen:
                result = _Checked(base, REPEATED)
            elif screened is not None:
                result = _Checked(base, screened)
            elif base.kind is ClaimKind.PACKET:
                result = _packet(base, item.packet)
            elif base.kind is ClaimKind.TOOL:
                result = _tool(base, item.tool_results)
            else:
                result, stored = _url(base, sources, as_of, pinned, item.forecast.coin_id)
            if result.reason is None:
                seen.add(digest)
            checked.append((item.agent, raw, digest, result, stored))
    items = [
        hard_evidence.EvidenceItem(
            key=f"{agent}|{digest}",
            kind=result.claim.kind,
            direction=result.confirms,
            tool=result.tool,
            vendor=result.vendor,
            event_class=stored.event_class if stored is not None else None,
        )
        for agent, _raw, digest, result, stored in checked
        if result.reason is None
    ]
    onchain_hard = hard_evidence.onchain_hard_keys(items)
    out: list[VerifiedClaim] = []
    for agent, raw, digest, result, stored in checked:
        claim = result.claim
        if result.reason is None:
            hard = False
            direction = claim.direction_hint
            if result.hard_direction is not None:
                hard, direction = True, result.hard_direction
            elif f"{agent}|{digest}" in onchain_hard and result.confirms is not None:
                hard, direction = True, result.confirms
            update: dict[str, Any] = {"verified": True, "hard": hard, "direction_hint": direction}
            if stored is not None:
                update |= {"tier": stored.tier, "domain_url": stored.domain}
            claim = claim.model_copy(update=update)
        elif stored is not None:
            claim = claim.model_copy(update={"tier": stored.tier, "domain_url": stored.domain})
        out.append(
            VerifiedClaim(
                round=round_,
                source_agent=agent,
                original_id=raw.claim_id,
                claim=claim,
                claim_sha256=digest,
                reject_reason=result.reason,
                penalty=result.reason in PENALIZED,
            )
        )
    return out
