"""Phase 02 recorder tables: CMC credit metering and governor state, route/WS health, lake statistics.

Written only by `hdt_recorder`; read by the console (`hdt_console_ro`). The raw responses themselves live
in the immutable lake, not in Postgres.
"""

from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import BigInteger, Boolean, CheckConstraint, Date, Float, Integer, Text
from sqlalchemy.orm import Mapped, mapped_column

from hdt.db.base import Base

HALT_STATES = ("daily_cap", "monthly_cap", "ip_limit")
ROUTE_STATUSES = ("ok", "late", "failing", "halted", "shed", "disabled", "pending")
# `last_error` prefix of a CMC route the key's plan does not include (1006): status `disabled`, not a
# failure; the route keeps its cadence and returns to `ok` by itself once the plan allows it.
NOT_ON_PLAN_ERROR = "not on current CMC plan (1006)"
WS_STATUSES = ("connected", "reconnecting", "down")


def _in(column: str, values: tuple[str, ...]) -> str:
    return f"{column} IN ({', '.join(repr(v) for v in values)})"


class CmcCreditUsageRow(Base):
    """Credits charged per UTC day and route, summed from `status.credit_count` of every response."""

    __tablename__ = "cmc_credit_usage"

    date: Mapped[date] = mapped_column(Date, primary_key=True)
    route: Mapped[str] = mapped_column(Text, primary_key=True)
    credits: Mapped[int] = mapped_column(BigInteger)
    calls: Mapped[int] = mapped_column(BigInteger)


class CmcKeyInfoRow(Base):
    """Hourly `/v1/key/info` snapshot (0 credits) plus the governor budget derived from it."""

    __tablename__ = "cmc_key_info"
    __table_args__ = (
        CheckConstraint(f"halt_state IS NULL OR {_in('halt_state', HALT_STATES)}", name="halt"),
    )

    checked_at: Mapped[datetime] = mapped_column(primary_key=True)
    credits_used_cycle: Mapped[int] = mapped_column(BigInteger)
    credit_limit_cycle: Mapped[int] = mapped_column(BigInteger)
    cycle_start: Mapped[datetime]
    cycle_end: Mapped[datetime]
    credits_used_today: Mapped[int] = mapped_column(BigInteger)
    governor_daily_budget: Mapped[int] = mapped_column(BigInteger)
    rate_limit_minute: Mapped[int | None] = mapped_column(Integer)
    halt_state: Mapped[str | None] = mapped_column(Text)
    degraded: Mapped[bool] = mapped_column(Boolean)


class CmcGovernorDayRow(Base):
    __tablename__ = "cmc_governor_days"

    date: Mapped[date] = mapped_column(Date, primary_key=True)
    budget: Mapped[int] = mapped_column(BigInteger)


class RouteHealthRow(Base):
    """Freshness of every REST collector (one row per route job, upserted after each cycle)."""

    __tablename__ = "route_health"
    __table_args__ = (CheckConstraint(_in("status", ROUTE_STATUSES), name="status"),)

    route_key: Mapped[str] = mapped_column(Text, primary_key=True)
    sort_order: Mapped[int] = mapped_column(Integer)
    route: Mapped[str] = mapped_column(Text)
    source: Mapped[str] = mapped_column(Text)
    cadence_label: Mapped[str] = mapped_column(Text)
    last_success_at: Mapped[datetime | None]
    last_attempt_at: Mapped[datetime | None]
    cycles_late: Mapped[int] = mapped_column(Integer)
    consecutive_failures: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(Text)
    credits_per_day: Mapped[int | None] = mapped_column(BigInteger)
    consumers: Mapped[str] = mapped_column(Text)
    last_error: Mapped[str | None] = mapped_column(Text)


class WsHealthRow(Base):
    __tablename__ = "ws_health"
    __table_args__ = (CheckConstraint(_in("status", WS_STATUSES), name="status"),)

    connection: Mapped[str] = mapped_column(Text, primary_key=True)
    status: Mapped[str] = mapped_column(Text)
    gaps_24h: Mapped[int] = mapped_column(Integer)
    connected_since: Mapped[datetime | None]
    last_message_at: Mapped[datetime | None]


class LakeStatsRow(Base):
    __tablename__ = "lake_stats"

    as_of: Mapped[datetime] = mapped_column(primary_key=True)
    lake_bytes: Mapped[int] = mapped_column(BigInteger)
    disk_used_fraction: Mapped[float | None] = mapped_column(Float)
    retention_days: Mapped[int | None] = mapped_column(Integer)
    merkle_date: Mapped[date | None] = mapped_column(Date)
    merkle_root: Mapped[str | None] = mapped_column(Text)
    object_locked: Mapped[bool] = mapped_column(Boolean)
