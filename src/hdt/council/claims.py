"""Claim identity, the instruction-like filter and anonymized sharing between debate rounds, pure.

Only verified claims are shared, and only as schema data:
- `agent` never travels with a claim (the share list keeps the source agent privately, to exclude an
  agent's own claims from what it is shown and to attribute penalties);
- `claim_id` is reissued (`sc_<16 base32>`), derived from the event, the round and the claim hash, so it is
  unguessable for the agents yet identical on replay;
- the order is shuffled by a keyed hash of the same inputs (deterministic, not by source agent);
- `statement`, `quote` and a text `value` are sanitized (no control / format characters) and truncated to
  `claim_max_chars` (280);
- a claim whose statement, quote, text value or ref reads like an instruction is never shared (rejected by
  the verifier); the filter runs on the same sanitized, NFKC-folded text the agents would read, line by line;
- what an agent is shown (`shown_to`) carries the shared id as `ref` for `packet` and `tool` claims (a
  field name or a tool result id would reveal the source agent's family); `url` claims keep the news item;
- the News agent is shown only news-safe claims (`news_safe`): `url` claims and claims on news tools,
  never a claim that repeats a price level of the candidate set or any agent's probability (Design
  Contract section 10: the News agent sees neither price-level candidates nor other agents' `p_model`).
The repeated-claim identity is the hash of kind + ref + statement + value (Design Contract section 3).
"""

from __future__ import annotations

import math
import re
import unicodedata
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Final

from hdt.contracts.common import ClaimKind
from hdt.contracts.forecast import Claim
from hdt.core.ids import b32_digest, canonical_sha256, sha256_hex
from hdt.tools.base import sanitize_text

PACKET_REF_PREFIX: Final[str] = "features."
VALUE_MAX_CHARS: Final[int] = 280
"""A text `value` longer than this is rejected (it would be an unfiltered free-text channel)."""
NEWS_TOOLS: Final[frozenset[str]] = frozenset(
    {
        "get_news",
        "fetch_source",
        "check_official",
        "unlock_schedule",
        "known_events",
        "dex_security",
        "liquidity_changes",
    }
)
"""Tools whose verified claims the News agent may be shown (no quant packet, no price levels)."""
_FILTER_DROPPED: Final[frozenset[str]] = frozenset({"Cc", "Cf", "Co", "Cs", "Cn"})
_NUMBER: Final = re.compile(r"(?<![\w.])[-+]?(?:\d+(?:[.,]\d+)*|\.\d+)(?:(\s*%)|([kKmMbB])(?![A-Za-z]))?")
_SUFFIX_EXPONENT: Final[dict[str, int]] = {"k": 3, "m": 6, "b": 9}
NEAR_RELATIVE: Final[float] = 0.001
"""A number with >= 3 significant digits within this relative distance of a secret repeats it (`65,430`
or `65.4k` for a level of 65432.1)."""

_SPACES: Final = re.compile(r"\s+")
INSTRUCTION_PATTERNS: Final[tuple[re.Pattern[str], ...]] = tuple(
    re.compile(pattern, re.IGNORECASE | re.MULTILINE)
    for pattern in (
        r"\b(ignore|disregard|forget|override)\b.{0,40}\b(instruction|instructions|prompt|rules?|above|previous|prior)\b",
        r"\b(system|developer)\s+(prompt|message|instruction)",
        r"\byou\s+(are|must|should|will)\s+(now\s+)?(an?\s+)?(ai|assistant|model|agent|required|forced)\b",
        r"\b(act|behave|respond)\s+as\b",
        r"\bnew\s+instructions?\b",
        r"^\s*(system|assistant|user|developer)\s*:",
        r"<\s*/?\s*(system|assistant|user|developer|instructions?)\s*>",
        r"\b(set|change|output|return)\s+(your\s+)?(p_llm|p_used|probability|forecast|stance)\s+(to|=)",
        r"\bdo\s+not\s+(follow|obey)\b",
        r"```",
    )
)


def normalize_text(value: str) -> str:
    """Comparison form of free text: NFKC, case-folded, whitespace collapsed."""
    return _SPACES.sub(" ", unicodedata.normalize("NFKC", value)).strip().casefold()


def packet_field(ref: str) -> str:
    """The packet feature name a packet claim cites (`features.` prefix accepted)."""
    ref = ref.strip()
    return ref[len(PACKET_REF_PREFIX) :] if ref.startswith(PACKET_REF_PREFIX) else ref


def claim_hash(claim: Claim) -> str:
    """Identity of a claim across agents and rounds: kind + ref + statement + value."""
    ref = packet_field(claim.ref) if claim.kind is ClaimKind.PACKET else claim.ref.strip()
    value: float | str | None = claim.value
    if isinstance(value, str):
        value = normalize_text(value)
    return canonical_sha256(
        {
            "kind": claim.kind.value,
            "ref": ref.casefold(),
            "statement": normalize_text(claim.statement),
            "value": value,
        }
    )


def filter_form(text: str) -> str:
    """The form the instruction filter reads: control / format / private-use characters removed (as
    `sanitize_text` removes them before an agent sees the text; newlines and tabs kept), then NFKC."""
    kept = "".join(ch for ch in text if ch in "\n\t" or unicodedata.category(ch) not in _FILTER_DROPPED)
    return unicodedata.normalize("NFKC", kept.replace("\r\n", "\n").replace("\r", "\n"))


def instruction_like(*texts: str | None) -> bool:
    for text in texts:
        if text is None:
            continue
        folded = filter_form(text)
        if any(pattern.search(folded) for pattern in INSTRUCTION_PATTERNS):
            return True
    return False


def shared_claim_id(event_id: str, round_: int, digest: str) -> str:
    return "sc_" + b32_digest(f"{event_id}|{round_}|{digest}", 16).lower()


@dataclass(frozen=True)
class SharedClaim:
    """A claim as shown to the other agents in the next round, plus its private attribution."""

    shared_id: str
    source_agent: str
    claim_sha256: str
    claim: Claim
    """Anonymized: `claim_id` = `shared_id`, truncated texts, code-assigned fields kept."""


def anonymize(claim: Claim, *, shared_id: str, max_chars: int) -> Claim:
    value = sanitize_text(claim.value, max_chars) if isinstance(claim.value, str) else claim.value
    return Claim(
        claim_id=shared_id,
        kind=claim.kind,
        ref=claim.ref,
        statement=sanitize_text(claim.statement, max_chars),
        value=value if value != "" else None,
        quote=sanitize_text(claim.quote, max_chars) if claim.quote else None,
        direction_hint=claim.direction_hint,
        verified=claim.verified,
        hard=claim.hard,
        tier=claim.tier,
        domain_url=claim.domain_url,
    )


def share(
    verified: Iterable[tuple[str, str, Claim]],
    *,
    event_id: str,
    round_: int,
    max_chars: int,
) -> list[SharedClaim]:
    """Anonymize `(source_agent, claim_sha256, verified claim)` triples and shuffle them deterministically."""
    out = [
        SharedClaim(
            shared_id=(sid := shared_claim_id(event_id, round_, digest)),
            source_agent=agent,
            claim_sha256=digest,
            claim=anonymize(claim, shared_id=sid, max_chars=max_chars),
        )
        for agent, digest, claim in verified
    ]
    out.sort(key=lambda s: sha256_hex(f"shuffle|{event_id}|{round_}|{s.claim_sha256}"))
    return out


def _opaque(claim: Claim) -> Claim:
    """The claim as an agent reads it: `packet` / `tool` refs replaced by the shared id."""
    if claim.kind is ClaimKind.URL:
        return claim
    return claim.model_copy(update={"ref": claim.claim_id})


def _numbers(text: str) -> Iterable[tuple[float, int, int]]:
    """(value, decimals of precision, significant digits) of every number in `text`. `62%` also yields
    0.62 with 2 more decimals; `65.4k` / `1.2m` / `3b` are scaled (precision in the scaled unit); trailing
    zeros of a whole number are read as rounding (`65,400`: to the hundred)."""
    for match in _NUMBER.finditer(text):
        raw = match.group(0).rstrip("% \t").rstrip("kKmMbB").replace(",", "")
        try:
            number = Decimal(raw)
        except InvalidOperation:
            continue
        digits = number.as_tuple().digits
        exponent = number.as_tuple().exponent
        if not isinstance(exponent, int):
            continue
        written = "".join(map(str, digits)).lstrip("0")
        significant = len(written)
        if exponent >= 0 and not match.group(1):
            trailing = len(written) - len(written.rstrip("0"))
            precision = -min(trailing, max(significant - 2, 0))
            significant += precision
        else:
            precision = max(-exponent, 0)
        suffix = match.group(2)
        if suffix:
            scale = _SUFFIX_EXPONENT[suffix.lower()]
            yield float(number) * 10.0**scale, precision - scale, significant
            continue
        yield float(number), precision, significant
        if match.group(1):
            yield float(number) / 100, precision + 2, significant


def _repeats(number: float, decimals: int, significant: int, secret: float) -> bool:
    """True when `number`, written to `decimals` decimals (negative: rounded to tens, hundreds, ...) with
    >= 2 significant digits, is `secret` rounded to that precision (so `1.23` repeats a level of 1.2345,
    `0.6` does not repeat 0.62), or has >= 3 significant digits and lies within `NEAR_RELATIVE` of it."""
    if number == 0 or not math.isfinite(number) or not math.isfinite(secret):
        return False
    if significant < 2:
        return False
    tolerance = 0.5 * 10.0 ** (-decimals) * (1 + 1e-9)
    if significant >= 3:
        tolerance = max(tolerance, NEAR_RELATIVE * abs(secret))
    return abs(number - secret) <= tolerance


def news_safe(claim: Claim, secrets: Sequence[float]) -> bool:
    """Whether the News agent may be shown `claim`: a `url` claim or a claim on a news tool, repeating
    none of `secrets` (the candidate set's price levels and every agent's probabilities)."""
    if claim.kind is ClaimKind.PACKET:
        return False
    if claim.kind is ClaimKind.TOOL and claim.ref.split(":", 1)[0].strip() not in NEWS_TOOLS:
        return False
    texts = [claim.statement, claim.quote or ""]
    if isinstance(claim.value, str):
        texts.append(claim.value)
    for text in texts:
        for number, decimals, significant in _numbers(filter_form(text)):
            if any(_repeats(number, decimals, significant, s) for s in secrets):
                return False
    if isinstance(claim.value, int | float) and not isinstance(claim.value, bool):
        value = float(claim.value)
        if any(abs(value - s) <= 1e-6 * max(abs(s), 1e-9) for s in secrets):
            return False
    return True


def visible(
    agent: str, shared: Sequence[SharedClaim], *, news: bool, secrets: Sequence[float] = ()
) -> list[SharedClaim]:
    """The shared claims `agent` may see: everyone's but its own; for the News agent only `news_safe` ones."""
    return [s for s in shared if s.source_agent != agent and (not news or news_safe(s.claim, secrets))]


def shown_to(
    agent: str, shared: Sequence[SharedClaim], *, news: bool = False, secrets: Sequence[float] = ()
) -> tuple[Claim, ...]:
    """What `agent` is shown next round: `visible` claims with opaque `packet` / `tool` refs."""
    return tuple(_opaque(s.claim) for s in visible(agent, shared, news=news, secrets=secrets))
