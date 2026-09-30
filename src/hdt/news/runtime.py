"""Helpers shared by the news worker and the veto scan: pinned model roles and official evidence."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from functools import partial

import sqlalchemy as sa

from hdt.core.clock import ensure_utc
from hdt.lake.pit_query import PitQuery
from hdt.news.config import NewsFile
from hdt.news.official import ANNOUNCEMENT_WINDOW
from hdt.news.onchain import onchain_state
from hdt.news.store import announcements
from hdt.news.unlocks import known_unlocks
from hdt.news.verdict import OfficialEvidence, RoleConfigs
from hdt.settings.versions import ConfigPin, SectionNotConfiguredError
from hdt.tools.ports import UnlockEvent

log = logging.getLogger(__name__)


def pinned_roles(pin: ConfigPin) -> RoleConfigs | None:
    """The extractor and both judges' model configs, or None while any of them is not configured (the
    pipeline then waits: items stay unjudged, fail closed)."""
    try:
        roles = pin.models().roles
    except SectionNotConfiguredError:
        return None
    extractor = roles.get("news_extractor")
    judge_a = roles.get("news_judge_a")
    judge_b = roles.get("news_judge_b")
    if extractor is None or judge_a is None or judge_b is None:
        return None
    return RoleConfigs(extractor, judge_a, judge_b)


def official_evidence(
    conn: sa.Connection, pit: PitQuery, as_of: datetime, config: NewsFile
) -> OfficialEvidence:
    """Announcements, unlocks and on-chain data known at `as_of` for `check_official`."""
    as_of = ensure_utc(as_of)
    rows = known_unlocks(conn, as_of)
    unlocks = tuple(
        UnlockEvent(
            coin_id=r["coin_id"],
            unlock_at=ensure_utc(r["unlock_at"]),
            amount_tokens=r["amount_tokens"],
            pct_of_circulating=r["pct_of_circulating"],
            category=r["category"],
            source=r["source"],
            recorded_at=ensure_utc(r["recorded_at"]),
        )
        for r in rows
        if ensure_utc(r["unlock_at"]) >= as_of - timedelta(days=7)
    )
    calendar = config.unlock_calendar
    tier = next((r["tier"] for r in rows if r["source"] == calendar.key), calendar.tier)
    return OfficialEvidence(
        announcements=tuple(announcements(conn, "binance", as_of - ANNOUNCEMENT_WINDOW * 2, as_of)),
        unlocks=unlocks,
        unlock_tier=tier,
        unlock_tiers={r["source"]: r["tier"] for r in rows},
        onchain=partial(onchain_state, pit),
    )
