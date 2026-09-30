"""Recorder writes to Postgres (role `hdt_recorder`): credit metering, key info, route/WS health, lake stats.

All statements are idempotent upserts or append-only inserts; callers run them on a worker thread.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from hdt.db.models.ingest import (
    CmcCreditUsageRow,
    CmcGovernorDayRow,
    CmcKeyInfoRow,
    LakeStatsRow,
    RouteHealthRow,
    WsHealthRow,
)
from hdt.ingest.credit_governor import CreditGovernor, KeyInfo


def add_credit_usage(session: Session, day: date, route: str, credits: int) -> None:
    stmt = insert(CmcCreditUsageRow).values(date=day, route=route, credits=credits, calls=1)
    session.execute(
        stmt.on_conflict_do_update(
            index_elements=["date", "route"],
            set_={
                "credits": CmcCreditUsageRow.credits + stmt.excluded.credits,
                "calls": CmcCreditUsageRow.calls + 1,
            },
        )
    )


def record_key_info(session: Session, info: KeyInfo, governor: CreditGovernor) -> None:
    session.add(
        CmcKeyInfoRow(
            checked_at=info.checked_at,
            credits_used_cycle=info.credits_used_cycle,
            credit_limit_cycle=info.credit_limit_cycle,
            cycle_start=info.cycle_start,
            cycle_end=info.cycle_end,
            credits_used_today=governor.used_today,
            governor_daily_budget=governor.daily_budget,
            rate_limit_minute=info.rate_limit_minute,
            halt_state=governor.halt_state(info.checked_at),
            degraded=governor.degraded,
        )
    )
    stmt = insert(CmcGovernorDayRow).values(date=info.checked_at.date(), budget=governor.daily_budget)
    session.execute(
        stmt.on_conflict_do_update(index_elements=["date"], set_={"budget": stmt.excluded.budget})
    )


@dataclass(frozen=True)
class RouteStatus:
    route_key: str
    sort_order: int
    route: str
    source: str
    cadence_label: str
    consumers: str
    credits_per_day: int | None


def upsert_route_health(
    session: Session,
    route: RouteStatus,
    *,
    attempted_at: datetime,
    success: bool,
    status: str,
    error: str | None = None,
) -> None:
    """One attempt of a route. `disabled` (a CMC route the key's plan does not include) is not a failure:
    the failure streak and lateness are reset, `last_error` keeps the reason, `last_success_at` is kept."""
    refused = not success and status == "disabled"
    values = {
        "route_key": route.route_key,
        "sort_order": route.sort_order,
        "route": route.route,
        "source": route.source,
        "cadence_label": route.cadence_label,
        "last_success_at": attempted_at if success else None,
        "last_attempt_at": attempted_at,
        "cycles_late": 0,
        "consecutive_failures": 0 if success or refused else 1,
        "status": status,
        "credits_per_day": route.credits_per_day,
        "consumers": route.consumers,
        "last_error": None if success else error,
    }
    stmt = insert(RouteHealthRow).values(**values)
    table = RouteHealthRow
    updates: dict[str, Any] = {
        "sort_order": stmt.excluded.sort_order,
        "route": stmt.excluded.route,
        "cadence_label": stmt.excluded.cadence_label,
        "last_attempt_at": stmt.excluded.last_attempt_at,
        "status": stmt.excluded.status,
        "credits_per_day": stmt.excluded.credits_per_day,
        "consumers": stmt.excluded.consumers,
    }
    if success:
        updates |= {
            "last_success_at": stmt.excluded.last_success_at,
            "consecutive_failures": 0,
            "last_error": None,
        }
    elif refused:
        updates |= {
            "cycles_late": 0,
            "consecutive_failures": 0,
            "last_error": stmt.excluded.last_error,
        }
    else:
        updates |= {
            "consecutive_failures": table.consecutive_failures + 1,
            "last_error": stmt.excluded.last_error,
        }
    session.execute(stmt.on_conflict_do_update(index_elements=["route_key"], set_=updates))


def mark_late_routes(session: Session, now: datetime, cadences: dict[str, int]) -> None:
    """Recompute `cycles_late` for interval routes from the last success and the cadence (never for a
    `disabled` route: it is not expected to record anything while its plan refuses it)."""
    for row in session.query(RouteHealthRow).filter(RouteHealthRow.route_key.in_(list(cadences))):
        if row.status == "disabled":
            row.cycles_late = 0
            continue
        cadence = cadences[row.route_key]
        reference = row.last_success_at or row.last_attempt_at
        if reference is None:
            continue
        late = max(0, int((now - reference).total_seconds() // cadence) - 1)
        row.cycles_late = late
        if row.status == "ok" and late > 2:
            row.status = "late"


def upsert_ws_health(
    session: Session,
    connection: str,
    status: str,
    gaps_24h: int,
    connected_since: datetime | None,
    last_message_at: datetime | None,
) -> None:
    values = {
        "connection": connection,
        "status": status,
        "gaps_24h": gaps_24h,
        "connected_since": connected_since,
        "last_message_at": last_message_at,
    }
    stmt = insert(WsHealthRow).values(**values)
    session.execute(
        stmt.on_conflict_do_update(
            index_elements=["connection"], set_={k: stmt.excluded[k] for k in values if k != "connection"}
        )
    )


def record_lake_stats(
    session: Session,
    *,
    as_of: datetime,
    lake_bytes: int,
    disk_used_fraction: float | None,
    retention_days: int | None,
    merkle_date: date | None,
    merkle_root: str | None,
) -> None:
    session.add(
        LakeStatsRow(
            as_of=as_of,
            lake_bytes=lake_bytes,
            disk_used_fraction=disk_used_fraction,
            retention_days=retention_days,
            merkle_date=merkle_date,
            merkle_root=merkle_root,
            object_locked=False,  # object-lock upload is the phase 10 backup job
        )
    )
