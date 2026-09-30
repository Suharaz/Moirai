"""Unlock calendar storage: versioned rows in `news_unlocks` and `PgUnlockCalendar` (port `UnlockCalendar`).

The calendar source is polled as a whole document. Each entry is keyed by (source, coin, unlock time,
category); a new or changed entry is a new row, an entry that disappeared from the source is a `removed`
row. The schedule known at `as_of` is the newest row per key recorded at or before `as_of`, minus removed
ones, so a replay sees exactly what was known then.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any

import sqlalchemy as sa

from hdt.core.clock import ensure_utc
from hdt.core.ids import canonical_sha256
from hdt.db.models.news import NewsUnlockRow
from hdt.news.sources import RawUnlock
from hdt.news.store import insert_rows
from hdt.tools.ports import UnlockEvent

UNLOCKS: sa.Table = NewsUnlockRow.__table__  # type: ignore[assignment]
_KEY = (UNLOCKS.c.source, UNLOCKS.c.coin_id, UNLOCKS.c.unlock_at, UNLOCKS.c.category)


def _latest(as_of: datetime) -> sa.Subquery:
    ranked = sa.select(
        UNLOCKS,
        sa.func.row_number().over(partition_by=_KEY, order_by=UNLOCKS.c.recorded_at.desc()).label("rn"),
    ).where(UNLOCKS.c.recorded_at <= ensure_utc(as_of))
    return ranked.subquery()


def known_unlocks(
    conn: sa.Connection, as_of: datetime, *, coin_id: int | None = None, source: str | None = None
) -> list[dict[str, Any]]:
    latest = _latest(as_of)
    query = sa.select(latest).where(latest.c.rn == 1, latest.c.removed.is_(False))
    if coin_id is not None:
        query = query.where(latest.c.coin_id == coin_id)
    if source is not None:
        query = query.where(latest.c.source == source)
    return [dict(row._mapping) for row in conn.execute(query)]


def _content(unlock: RawUnlock) -> str:
    return canonical_sha256(
        {"amount_tokens": unlock.amount_tokens, "pct_of_circulating": unlock.pct_of_circulating}
    )


def record_unlocks(
    conn: sa.Connection, source: str, tier: str, entries: Sequence[RawUnlock], *, recorded_at: datetime
) -> int:
    """Store the changes of the whole calendar document `entries`; returns the rows written."""
    current = {
        (row["coin_id"], ensure_utc(row["unlock_at"]), row["category"]): row
        for row in known_unlocks(conn, recorded_at, source=source)
    }
    rows: list[dict[str, Any]] = []
    seen: set[tuple[int, datetime, str]] = set()
    for entry in entries:
        key = (entry.coin_id, ensure_utc(entry.unlock_at), entry.category)
        seen.add(key)
        content = _content(entry)
        known = current.get(key)
        if known is not None and known["content_sha256"] == content and known["tier"] == tier:
            continue
        rows.append(
            {
                "source": source,
                "coin_id": entry.coin_id,
                "unlock_at": key[1],
                "category": entry.category,
                "recorded_at": recorded_at,
                "amount_tokens": entry.amount_tokens,
                "pct_of_circulating": entry.pct_of_circulating,
                "tier": tier,
                "removed": False,
                "content_sha256": content,
            }
        )
    for key, known in current.items():
        if key not in seen:
            rows.append({**{k: known[k] for k in _ROW_FIELDS}, "recorded_at": recorded_at, "removed": True})
    return insert_rows(conn, UNLOCKS, rows)


_ROW_FIELDS = (
    "source",
    "coin_id",
    "unlock_at",
    "category",
    "amount_tokens",
    "pct_of_circulating",
    "tier",
    "content_sha256",
)


class PgUnlockCalendar:
    """`hdt.tools.ports.UnlockCalendar` over the versioned `news_unlocks` rows."""

    def __init__(self, engine: sa.Engine) -> None:
        self._engine = engine

    def unlocks(self, coin_id: int, start: datetime, end: datetime, as_of: datetime) -> list[UnlockEvent]:
        start, end = ensure_utc(start), ensure_utc(end)
        with self._engine.connect() as conn:
            rows = known_unlocks(conn, as_of, coin_id=coin_id)
        events = [
            UnlockEvent(
                coin_id=row["coin_id"],
                unlock_at=ensure_utc(row["unlock_at"]),
                amount_tokens=row["amount_tokens"],
                pct_of_circulating=row["pct_of_circulating"],
                category=row["category"],
                source=row["source"],
                recorded_at=ensure_utc(row["recorded_at"]),
            )
            for row in rows
            if start <= ensure_utc(row["unlock_at"]) <= end
        ]
        events.sort(key=lambda e: (e.unlock_at, e.category, e.source))
        return events

    def tier(self, source: str, as_of: datetime) -> str | None:
        """The tier recorded for `source` (the calendar's configured tier when it was polled)."""
        with self._engine.connect() as conn:
            rows = known_unlocks(conn, as_of, source=source)
        return rows[0]["tier"] if rows else None
