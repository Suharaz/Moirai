"""Postgres access of the news tables (`hdt.db.models.news`) and `PgNewsIndex` (tool port `NewsIndex`).

Readers are point-in-time: an item is visible at `as_of` only when `ingested_at <= as_of`, a verdict only
when `processed_at <= as_of`. Writers are insert-only (`ON CONFLICT DO NOTHING` on the natural keys), so a
restarted worker never rewrites history.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert

from hdt.contracts.common import DirectionHint, TargetType, Tier
from hdt.core.clock import ensure_utc
from hdt.db.models.news import (
    NewsAnnouncementRow,
    NewsAttentionRow,
    NewsCandidateRow,
    NewsItemRow,
    NewsSourceStateRow,
    NewsVerdictRow,
    NewsVetoScanRow,
)
from hdt.news.dedupe import RecentItem
from hdt.tools.ports import Announcement, NewsItem

ITEMS: sa.Table = NewsItemRow.__table__  # type: ignore[assignment]
ANNOUNCEMENTS: sa.Table = NewsAnnouncementRow.__table__  # type: ignore[assignment]
VERDICTS: sa.Table = NewsVerdictRow.__table__  # type: ignore[assignment]
ATTENTION: sa.Table = NewsAttentionRow.__table__  # type: ignore[assignment]
CANDIDATES: sa.Table = NewsCandidateRow.__table__  # type: ignore[assignment]
SOURCE_STATE: sa.Table = NewsSourceStateRow.__table__  # type: ignore[assignment]
VETO_SCANS: sa.Table = NewsVetoScanRow.__table__  # type: ignore[assignment]


@dataclass(frozen=True)
class StoredItem:
    item_id: str
    source_key: str
    source_kind: str
    source_name: str
    url: str
    domain: str
    tier: Tier
    title: str
    summary: str | None
    content: str | None
    published_at: datetime | None
    ingested_at: datetime
    coin_ids: tuple[int, ...]
    duplicate_of: str | None

    @property
    def news_time(self) -> datetime:
        """Published time, never after ingestion (a feed cannot date an item into the future)."""
        return min(self.published_at or self.ingested_at, self.ingested_at)

    def news_item(self) -> NewsItem:
        return NewsItem(
            item_id=self.item_id,
            coin_ids=self.coin_ids,
            title=self.title,
            url=self.url,
            source_name=self.source_name,
            published_at=self.published_at,
            ingested_at=self.ingested_at,
            summary=self.summary,
        )


def _item(row: Any) -> StoredItem:
    return StoredItem(
        item_id=row.item_id,
        source_key=row.source_key,
        source_kind=row.source_kind,
        source_name=row.source_name,
        url=row.url,
        domain=row.domain,
        tier=Tier(row.tier),
        title=row.title,
        summary=row.summary,
        content=row.content,
        published_at=ensure_utc(row.published_at) if row.published_at else None,
        ingested_at=ensure_utc(row.ingested_at),
        coin_ids=tuple(row.coin_ids),
        duplicate_of=row.duplicate_of,
    )


@dataclass(frozen=True)
class StoredVerdict:
    item_id: str
    rule_version: str
    processed_at: datetime
    processor: str
    coin_ids: tuple[int, ...]
    status: str
    event_key: str | None
    event_class: str | None
    direction: DirectionHint | None
    hardness: float | None
    novelty: float | None
    confidence: float | None
    event_time: datetime | None
    quote: str | None
    quote_ok: bool
    domain: str
    tier: Tier
    official: bool
    official_ref: str | None
    class_a: str | None
    class_b: str | None
    direction_a: str | None
    direction_b: str | None
    known_event: str | None
    detail: dict[str, Any]


def _verdict(row: Any) -> StoredVerdict:
    return StoredVerdict(
        item_id=row.item_id,
        rule_version=row.rule_version,
        processed_at=ensure_utc(row.processed_at),
        processor=row.processor,
        coin_ids=tuple(row.coin_ids),
        status=row.status,
        event_key=row.event_key,
        event_class=row.event_class,
        direction=DirectionHint(row.direction) if row.direction else None,
        hardness=row.hardness,
        novelty=row.novelty,
        confidence=row.confidence,
        event_time=ensure_utc(row.event_time) if row.event_time else None,
        quote=row.quote,
        quote_ok=row.quote_ok,
        domain=row.domain,
        tier=Tier(row.tier),
        official=row.official,
        official_ref=row.official_ref,
        class_a=row.class_a,
        class_b=row.class_b,
        direction_a=row.direction_a,
        direction_b=row.direction_b,
        known_event=row.known_event,
        detail=dict(row.detail or {}),
    )


# --------------------------------------------------------------------------- readers


def item_at(conn: sa.Connection, item_id: str, as_of: datetime) -> StoredItem | None:
    row = conn.execute(
        sa.select(ITEMS).where(ITEMS.c.item_id == item_id, ITEMS.c.ingested_at <= ensure_utc(as_of))
    ).first()
    return _item(row) if row is not None else None


def items_for_coin(
    conn: sa.Connection,
    coin_id: int,
    since: datetime,
    as_of: datetime,
    *,
    limit: int | None = None,
    include_duplicates: bool = False,
) -> list[StoredItem]:
    """Items of `coin_id` with `since <= ingested_at <= as_of`, newest first."""
    query = sa.select(ITEMS).where(
        sa.literal(coin_id) == sa.any_(ITEMS.c.coin_ids),
        ITEMS.c.ingested_at >= ensure_utc(since),
        ITEMS.c.ingested_at <= ensure_utc(as_of),
    )
    if not include_duplicates:
        query = query.where(ITEMS.c.duplicate_of.is_(None))
    query = query.order_by(ITEMS.c.ingested_at.desc(), ITEMS.c.item_id.desc())
    if limit is not None:
        query = query.limit(limit)
    return [_item(row) for row in conn.execute(query)]


def recent_items(conn: sa.Connection, since: datetime) -> list[RecentItem]:
    """Items ingested since `since` (dedupe window), oldest first."""
    rows = conn.execute(
        sa.select(ITEMS.c.item_id, ITEMS.c.coin_ids, ITEMS.c.title, ITEMS.c.summary, ITEMS.c.duplicate_of)
        .where(ITEMS.c.ingested_at >= ensure_utc(since))
        .order_by(ITEMS.c.ingested_at, ITEMS.c.item_id)
    )
    return [RecentItem(r.item_id, tuple(r.coin_ids), r.title, r.summary, r.duplicate_of) for r in rows]


def known_url_keys(conn: sa.Connection, keys: Iterable[str]) -> set[str]:
    wanted = sorted(set(keys))
    if not wanted:
        return set()
    return set(conn.execute(sa.select(ITEMS.c.url_key).where(ITEMS.c.url_key.in_(wanted))).scalars())


def pending_items(conn: sa.Connection, rule_version: str, since: datetime, limit: int) -> list[StoredItem]:
    """Canonical items without a verdict of `rule_version`, oldest first."""
    judged = sa.select(VERDICTS.c.item_id).where(
        VERDICTS.c.item_id == ITEMS.c.item_id, VERDICTS.c.rule_version == rule_version
    )
    rows = conn.execute(
        sa.select(ITEMS)
        .where(
            ITEMS.c.ingested_at >= ensure_utc(since),
            ITEMS.c.duplicate_of.is_(None),
            ~sa.exists(judged),
        )
        .order_by(ITEMS.c.ingested_at, ITEMS.c.item_id)
        .limit(limit)
    )
    return [_item(row) for row in rows]


def verdicts_for_coin(
    conn: sa.Connection, coin_id: int, rule_version: str, since: datetime, as_of: datetime
) -> list[tuple[StoredItem, StoredVerdict]]:
    """(item, verdict) pairs of `coin_id`: item ingested in `[since, as_of]`, verdict known at `as_of`."""
    as_of = ensure_utc(as_of)
    rows = conn.execute(
        sa.select(ITEMS, *[c.label(f"v_{c.name}") for c in VERDICTS.c])
        .join(VERDICTS, VERDICTS.c.item_id == ITEMS.c.item_id)
        .where(
            sa.literal(coin_id) == sa.any_(ITEMS.c.coin_ids),
            ITEMS.c.ingested_at >= ensure_utc(since),
            ITEMS.c.ingested_at <= as_of,
            VERDICTS.c.rule_version == rule_version,
            VERDICTS.c.processed_at <= as_of,
        )
        .order_by(ITEMS.c.ingested_at, ITEMS.c.item_id)
    )
    out = []
    for row in rows:
        mapping = row._mapping
        verdict = _verdict(_Prefixed(mapping, "v_"))
        out.append((_item(row), verdict))
    return out


def verdict_at(conn: sa.Connection, item_id: str, rule_version: str, as_of: datetime) -> StoredVerdict | None:
    row = conn.execute(
        sa.select(VERDICTS).where(
            VERDICTS.c.item_id == item_id,
            VERDICTS.c.rule_version == rule_version,
            VERDICTS.c.processed_at <= ensure_utc(as_of),
        )
    ).first()
    return _verdict(row) if row is not None else None


class _Prefixed:
    def __init__(self, mapping: Any, prefix: str) -> None:
        self._mapping = mapping
        self._prefix = prefix

    def __getattr__(self, name: str) -> Any:
        return self._mapping[f"{self._prefix}{name}"]


def announcements(conn: sa.Connection, exchange: str, since: datetime, as_of: datetime) -> list[Announcement]:
    rows = conn.execute(
        sa.select(ANNOUNCEMENTS)
        .where(
            ANNOUNCEMENTS.c.exchange == exchange,
            ANNOUNCEMENTS.c.published_at >= ensure_utc(since),
            ANNOUNCEMENTS.c.recorded_at <= ensure_utc(as_of),
        )
        .order_by(ANNOUNCEMENTS.c.published_at.desc(), ANNOUNCEMENTS.c.announcement_id)
    )
    return [
        Announcement(
            exchange=r.exchange,
            title=r.title,
            url=r.url,
            catalog=r.catalog,
            published_at=ensure_utc(r.published_at),
            recorded_at=ensure_utc(r.recorded_at),
        )
        for r in rows
    ]


def source_state(conn: sa.Connection) -> dict[str, tuple[datetime | None, str | None]]:
    """source_key -> (last success, last error)."""
    return {
        r.source_key: (ensure_utc(r.last_success_at) if r.last_success_at else None, r.last_error)
        for r in conn.execute(sa.select(SOURCE_STATE))
    }


def news_counts(
    conn: sa.Connection, coin_ids: Sequence[int], since: datetime, as_of: datetime
) -> dict[int, list[datetime]]:
    """Ingestion times of canonical items per coin in `[since, as_of]` (NewsZ_i)."""
    wanted = set(coin_ids)
    out: dict[int, list[datetime]] = {c: [] for c in wanted}
    rows = conn.execute(
        sa.select(ITEMS.c.coin_ids, ITEMS.c.ingested_at).where(
            ITEMS.c.ingested_at >= ensure_utc(since),
            ITEMS.c.ingested_at <= ensure_utc(as_of),
            ITEMS.c.duplicate_of.is_(None),
        )
    )
    for row in rows:
        for coin in row.coin_ids:
            if coin in wanted:
                out[coin].append(ensure_utc(row.ingested_at))
    return out


# --------------------------------------------------------------------------- writers


def insert_rows(conn: sa.Connection, table: sa.Table, rows: Sequence[dict[str, Any]]) -> int:
    """Insert-only; rows whose natural key exists are skipped. Returns the number inserted."""
    if not rows:
        return 0
    stmt: Any = insert(table).values(list(rows)).on_conflict_do_nothing().returning(sa.literal_column("1"))
    return len(conn.execute(stmt).all())


def record_source_state(
    conn: sa.Connection, source_key: str, *, at: datetime, ok: bool, count: int, error: str | None
) -> None:
    stmt = insert(SOURCE_STATE).values(
        source_key=source_key,
        last_attempt_at=at,
        last_success_at=at if ok else None,
        last_error=None if ok else (error or "error")[:500],
        last_count=count,
        consecutive_failures=0 if ok else 1,
    )
    excluded = stmt.excluded
    conn.execute(
        stmt.on_conflict_do_update(
            index_elements=[SOURCE_STATE.c.source_key],
            set_={
                "last_attempt_at": excluded.last_attempt_at,
                "last_success_at": sa.func.coalesce(excluded.last_success_at, SOURCE_STATE.c.last_success_at),
                "last_error": excluded.last_error,
                "last_count": excluded.last_count,
                "consecutive_failures": sa.case(
                    (excluded.last_success_at.is_not(None), 0),
                    else_=SOURCE_STATE.c.consecutive_failures + 1,
                ),
            },
        )
    )


class PgNewsIndex:
    """`hdt.tools.ports.NewsIndex` over `news_items` (duplicates are not listed)."""

    def __init__(self, engine: sa.Engine) -> None:
        self._engine = engine

    def items(self, coin_id: int, since: datetime, as_of: datetime, limit: int) -> list[NewsItem]:
        with self._engine.connect() as conn:
            rows = items_for_coin(conn, coin_id, since, as_of, limit=limit)
        return [row.news_item() for row in rows]

    def item(self, item_id: str, as_of: datetime) -> NewsItem | None:
        with self._engine.connect() as conn:
            row = item_at(conn, item_id, as_of)
        return row.news_item() if row is not None else None


def attention_at(conn: sa.Connection, coin_id: int, as_of: datetime, max_age: timedelta) -> bool:
    """The coin's newest attention flag computed at or before `as_of` and not older than `max_age`.

    Only coins with a signal get a row, so a missing row means no attention."""
    as_of = ensure_utc(as_of)
    row = conn.execute(
        sa.select(ATTENTION.c.attention)
        .where(
            ATTENTION.c.coin_id == coin_id,
            ATTENTION.c.as_of <= as_of,
            ATTENTION.c.as_of >= as_of - max_age,
            ATTENTION.c.computed_at <= as_of,
        )
        .order_by(ATTENTION.c.as_of.desc())
        .limit(1)
    ).first()
    return row is not None and bool(row.attention)


def verdicts_since(
    conn: sa.Connection, rule_version: str, since: datetime, as_of: datetime
) -> list[tuple[StoredItem, StoredVerdict]]:
    """(item, verdict) pairs of every coin: item ingested in `[since, as_of]`, verdict known at `as_of`."""
    as_of = ensure_utc(as_of)
    rows = conn.execute(
        sa.select(ITEMS, *[c.label(f"v_{c.name}") for c in VERDICTS.c])
        .join(VERDICTS, VERDICTS.c.item_id == ITEMS.c.item_id)
        .where(
            ITEMS.c.ingested_at >= ensure_utc(since),
            ITEMS.c.ingested_at <= as_of,
            VERDICTS.c.rule_version == rule_version,
            VERDICTS.c.processed_at <= as_of,
        )
        .order_by(ITEMS.c.ingested_at, ITEMS.c.item_id)
    )
    return [(_item(row), _verdict(_Prefixed(row._mapping, "v_"))) for row in rows]


def unjudged_items(
    conn: sa.Connection, rule_version: str, since: datetime, as_of: datetime
) -> list[StoredItem]:
    """Canonical items ingested in `[since, as_of]` without a verdict of `rule_version` known at `as_of`."""
    as_of = ensure_utc(as_of)
    judged = sa.select(VERDICTS.c.item_id).where(
        VERDICTS.c.item_id == ITEMS.c.item_id,
        VERDICTS.c.rule_version == rule_version,
        VERDICTS.c.processed_at <= as_of,
    )
    rows = conn.execute(
        sa.select(ITEMS)
        .where(
            ITEMS.c.ingested_at >= ensure_utc(since),
            ITEMS.c.ingested_at <= as_of,
            ITEMS.c.duplicate_of.is_(None),
            ~sa.exists(judged),
        )
        .order_by(ITEMS.c.ingested_at, ITEMS.c.item_id)
    )
    return [_item(row) for row in rows]


def attention_coins(conn: sa.Connection, as_of: datetime, max_age: timedelta) -> dict[int, float]:
    """coin_id -> NewsZ of the coins flagged for attention in the newest computation within `max_age`."""
    as_of = ensure_utc(as_of)
    newest = conn.execute(
        sa.select(sa.func.max(ATTENTION.c.as_of)).where(
            ATTENTION.c.as_of <= as_of, ATTENTION.c.as_of >= as_of - max_age
        )
    ).scalar()
    if newest is None:
        return {}
    rows = conn.execute(
        sa.select(ATTENTION.c.coin_id, ATTENTION.c.news_z).where(
            ATTENTION.c.as_of == newest, ATTENTION.c.attention.is_(True)
        )
    )
    return {r.coin_id: r.news_z for r in rows}


def recently_judged_coins(
    conn: sa.Connection, rule_version: str, since: datetime, as_of: datetime
) -> set[int]:
    rows = conn.execute(
        sa.select(VERDICTS.c.coin_ids).where(
            VERDICTS.c.rule_version == rule_version,
            VERDICTS.c.status == "agreed",
            VERDICTS.c.processed_at > ensure_utc(since),
            VERDICTS.c.processed_at <= ensure_utc(as_of),
        )
    )
    return {coin for row in rows for coin in row.coin_ids}


def candidates_since(conn: sa.Connection, since: datetime) -> list[tuple[int, datetime, datetime | None]]:
    """(coin_id, as_of, published_at) of the HOLLOW_HYPE candidates recorded since `since`."""
    rows = conn.execute(
        sa.select(CANDIDATES.c.coin_id, CANDIDATES.c.as_of, CANDIDATES.c.published_at).where(
            CANDIDATES.c.as_of >= ensure_utc(since)
        )
    )
    return [
        (r.coin_id, ensure_utc(r.as_of), ensure_utc(r.published_at) if r.published_at else None) for r in rows
    ]


@dataclass(frozen=True)
class StoredCandidate:
    """A recorded `news_candidates` row: everything its `Candidate` is rebuilt from, config never read."""

    coin_id: int
    as_of: datetime
    score: float
    rule_version: str
    mode: str
    target_type: TargetType
    label_spec_version: str

    @classmethod
    def from_row(cls, row: Mapping[Any, Any]) -> StoredCandidate:
        """From an inserted row dict or a SQLAlchemy `RowMapping` of `news_candidates`."""
        return cls(
            coin_id=int(row["coin_id"]),
            as_of=ensure_utc(row["as_of"]),
            score=float(row["score"]),
            rule_version=str(row["rule_version"]),
            mode=str(row["mode"]),
            target_type=TargetType(row["target_type"]),
            label_spec_version=str(row["label_spec_version"]),
        )


_STORED_COLUMNS = ("coin_id", "as_of", "score", "rule_version", "mode", "target_type", "label_spec_version")


def unpublished_candidates(conn: sa.Connection, since: datetime) -> list[StoredCandidate]:
    """Candidates recorded since `since` but never published (the worker stopped between the insert and
    the XADD), oldest first."""
    rows = conn.execute(
        sa.select(*(CANDIDATES.c[name] for name in _STORED_COLUMNS))
        .where(CANDIDATES.c.as_of >= ensure_utc(since), CANDIDATES.c.published_at.is_(None))
        .order_by(CANDIDATES.c.as_of, CANDIDATES.c.coin_id)
    )
    return [StoredCandidate.from_row(r._mapping) for r in rows]


def mark_candidate_published(conn: sa.Connection, coin_id: int, as_of: datetime, at: datetime) -> None:
    conn.execute(
        sa.update(CANDIDATES)
        .where(CANDIDATES.c.coin_id == coin_id, CANDIDATES.c.as_of == ensure_utc(as_of))
        .values(published_at=at)
    )
