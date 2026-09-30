"""Veto rules of the independent veto scan (pure; the service is `hdt.news.veto_scan`).

Per coin and scan, from the verdicts of the window, the unlock calendar and the sources' health:
- hard veto (`veto_long`): an agreed EXPLOIT or DELIST verdict confirmed by `check_official` for this
  coin (T0 verification), or a large unlock (>= `unlock_large_pct` of circulating supply) within
  `unlock_ahead_h` from a T0 calendar. An agreed UNLOCK verdict is never hard by itself: the size check
  belongs to the calendar rule, so a small confirmed unlock is only a soft veto;
- soft veto (`size_mult = soft_size_mult`): a bad catalyst known only at T1-T3 / unverified (agreed
  EXPLOIT / DELIST / UNLOCK not confirmed, any agreed UNLOCK, a large unlock from a non-T0 calendar), a
  judge disagreement in which either judge said EXPLOIT or DELIST (never a silent abstain), an item
  matching the bad-catalyst keywords that no model has judged yet, or one whose verdict failed
  (`llm_failed`, `quote_failed`) or that only the extractor saw (`unconfirmed_keyword_hit`);
- `veto_short` is never set: no rule of the design vetoes shorts.
`scan_fresh_at` is the scan time unless a required source has not succeeded within `source_max_age_s`:
then it is that source's last success (risk ages the flags on it and stops new LONGs), or, for a source
that never succeeded, a time old enough for risk to treat the scan as stale.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Final

from hdt.contracts.risk_flags import RiskFlags
from hdt.core.clock import ensure_utc
from hdt.news.classes import DISAGREEMENT_VETO_CLASSES, VETO_CLASSES
from hdt.news.config import VetoConfig
from hdt.tools.ports import UnlockEvent

HARD_CLASSES: Final[frozenset[str]] = frozenset({"EXPLOIT", "DELIST"})


@dataclass(frozen=True)
class VetoVerdict:
    """One verdict about the coin, with `official` recomputed for this coin at scan time."""

    item_id: str
    status: str
    event_class: str | None
    class_a: str | None
    class_b: str | None
    official: bool
    unconfirmed_keyword_hit: bool = False
    """A bad-catalyst keyword item that no judge pair cleared (`hdt.news.veto_scan.unconfirmed_hit`)."""


@dataclass(frozen=True)
class VetoDecision:
    veto_long: bool
    veto_short: bool
    size_mult: float
    evidence_ref: str | None
    reasons: dict[str, Any] = field(default_factory=dict)

    @property
    def kind(self) -> str:
        if self.veto_long or self.veto_short:
            return "hard"
        return "soft" if self.size_mult < 1 else "clear"


def keyword_pattern(keywords: Sequence[str]) -> re.Pattern[str]:
    words = sorted({k.strip().lower() for k in keywords if k.strip()}, key=len, reverse=True)
    return re.compile(
        r"(?<![a-z0-9])(?:" + "|".join(re.escape(w) for w in words) + r")(?![a-z0-9])", re.IGNORECASE
    )


def decide_veto(
    verdicts: Sequence[VetoVerdict],
    unlocks: Sequence[UnlockEvent],
    unlock_tiers: Mapping[str, str],
    unjudged_hits: Sequence[str],
    *,
    as_of: datetime,
    cfg: VetoConfig,
) -> VetoDecision:
    as_of = ensure_utc(as_of)
    hard: list[str] = []
    soft: list[str] = []
    reasons: dict[str, Any] = {}
    for v in verdicts:
        if v.status == "agreed" and v.event_class in VETO_CLASSES:
            if v.official and v.event_class in HARD_CLASSES:
                hard.append(f"news:{v.item_id}")
                reasons.setdefault("hard", []).append({"item_id": v.item_id, "class": v.event_class})
            else:
                soft.append(f"news:{v.item_id}")
                reasons.setdefault("soft", []).append({"item_id": v.item_id, "class": v.event_class})
        elif v.status == "disagree" and {v.class_a, v.class_b} & DISAGREEMENT_VETO_CLASSES:
            soft.append(f"news:{v.item_id}")
            reasons.setdefault("disagree", []).append(
                {"item_id": v.item_id, "classes": sorted(c for c in (v.class_a, v.class_b) if c)}
            )
        elif v.unconfirmed_keyword_hit:
            soft.append(f"news:{v.item_id}")
            reasons.setdefault("unconfirmed", []).append({"item_id": v.item_id, "status": v.status})
    horizon = as_of + timedelta(hours=cfg.unlock_ahead_h)
    for unlock in unlocks:
        if not (as_of <= unlock.unlock_at <= horizon):
            continue
        if unlock.pct_of_circulating is None or unlock.pct_of_circulating < cfg.unlock_large_pct:
            continue
        ref = f"unlock:{unlock.source}:{unlock.unlock_at.isoformat()}"
        entry = {
            "source": unlock.source,
            "unlock_at": unlock.unlock_at.isoformat(),
            "pct": unlock.pct_of_circulating,
        }
        if unlock_tiers.get(unlock.source) == "T0":
            hard.append(ref)
            reasons.setdefault("hard", []).append(entry)
        else:
            soft.append(ref)
            reasons.setdefault("soft", []).append(entry)
    for item_id in unjudged_hits:
        soft.append(f"news:{item_id}")
        reasons.setdefault("unjudged", []).append(item_id)
    if hard:
        return VetoDecision(True, False, cfg.soft_size_mult, hard[0], reasons)
    if soft:
        return VetoDecision(False, False, cfg.soft_size_mult, soft[0], reasons)
    return VetoDecision(False, False, 1.0, None, reasons)


def scan_fresh_at(
    as_of: datetime,
    last_success: Mapping[str, datetime | None],
    cfg: VetoConfig,
    interval_s: int,
) -> tuple[datetime, list[str]]:
    """(scan_fresh_at, stale required sources)."""
    as_of = ensure_utc(as_of)
    fresh = as_of
    stale: list[str] = []
    never = as_of - timedelta(seconds=(cfg.expires_cycles + 1) * interval_s)
    for source in cfg.required_sources:
        last = last_success.get(source)
        if last is None:
            stale.append(source)
            fresh = min(fresh, never)
        elif (as_of - ensure_utc(last)).total_seconds() > cfg.source_max_age_s:
            stale.append(source)
            fresh = min(fresh, ensure_utc(last))
    return fresh, stale


def risk_flags(
    coin_id: int,
    decision: VetoDecision,
    *,
    as_of: datetime,
    fresh_at: datetime,
    cfg: VetoConfig,
    interval_s: int,
) -> RiskFlags:
    as_of = ensure_utc(as_of)
    return RiskFlags(
        coin_id=coin_id,
        as_of=as_of,
        veto_long=decision.veto_long,
        veto_short=decision.veto_short,
        size_mult=decision.size_mult,
        evidence_ref=decision.evidence_ref,
        expires_at=as_of + timedelta(seconds=cfg.expires_cycles * interval_s),
        scan_fresh_at=ensure_utc(fresh_at),
    )
