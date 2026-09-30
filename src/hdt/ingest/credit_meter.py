"""CMC credit meter: every response's `status.credit_count` is summed per UTC day and route, and
`/v1/key/info` (0 credits) is read hourly to re-anchor the governor and project the monthly total.

Projection = credits used this cycle + (credits used over the last 7 metered days / days) x days left.
Above `alert_projection_frac` of the cycle limit the meter raises the `cmc_credit_projection` alert
(log at WARNING + Prometheus gauge `hdt_cmc_credit_projection_fraction`; phase 10 routes it to Telegram).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from prometheus_client import Counter, Gauge
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from hdt.core.clock import utcnow
from hdt.db.models.ingest import CmcCreditUsageRow
from hdt.ingest.cmc_client import PLAN_REFUSED_CODE, CmcClient, CmcResult
from hdt.ingest.credit_governor import CreditGovernor, KeyInfo, parse_key_info
from hdt.ingest.recorder_db import add_credit_usage, record_key_info

log = logging.getLogger(__name__)

KEY_INFO_ROUTE = "key_info"
KEY_INFO_PATH = "/v1/key/info"
RUN_RATE_DAYS = 7

CREDITS = Counter("hdt_cmc_credits_total", "CMC credits charged (status.credit_count)", ["route"])
PROJECTION = Gauge("hdt_cmc_credit_projection_fraction", "Projected cycle credits / cycle credit limit")
USED_TODAY = Gauge("hdt_cmc_credits_used_today", "CMC credits used today (UTC)")
DAILY_BUDGET = Gauge("hdt_cmc_governor_daily_budget", "Credit governor daily budget")


@dataclass(frozen=True)
class Projection:
    used_cycle: int
    limit_cycle: int
    run_rate_per_day: float
    days_left: float
    projected: float

    @property
    def fraction(self) -> float:
        return self.projected / self.limit_cycle if self.limit_cycle > 0 else 0.0


def project_cycle(info: KeyInfo, daily_credits: list[int], now: datetime) -> Projection:
    """Monthly projection from the cycle usage so far and the recent daily run rate."""
    run_rate = sum(daily_credits) / len(daily_credits) if daily_credits else 0.0
    days_left = max(0.0, (info.cycle_end - now).total_seconds() / 86_400)
    projected = info.credits_used_cycle + run_rate * days_left
    return Projection(info.credits_used_cycle, info.credit_limit_cycle, run_rate, days_left, projected)


class CreditMeter:
    def __init__(
        self,
        *,
        governor: CreditGovernor,
        sessions: sessionmaker[Session],
        alert_fraction: float,
        clock: Callable[[], datetime] = utcnow,
    ) -> None:
        self._governor = governor
        self._sessions = sessions
        self._alert_fraction = alert_fraction
        self._clock = clock
        self.last_projection: Projection | None = None

    async def on_response(self, result: CmcResult) -> None:
        """Meter one response (called for every CMC response, including errors that charged credits).

        A plan refusal (1006, not charged) is not usage: it is kept out of the usage table, the governor's
        day count and therefore the cycle projection."""
        if result.error_code == PLAN_REFUSED_CODE:
            return
        credits = result.credit_count
        at = result.capture.fetched_at
        self._governor.record(credits, at)
        if credits:
            CREDITS.labels(route=result.route).inc(credits)
        USED_TODAY.set(self._governor.used_today)
        await asyncio.to_thread(self._write_usage, at.date(), result.route, credits)

    async def record_stream_credits(self, route: str, credits: int, at: datetime) -> None:
        """Credits billed per streamed message (CMC WebSocket), metered like a REST response."""
        self._governor.record(credits, at)
        CREDITS.labels(route=route).inc(credits)
        USED_TODAY.set(self._governor.used_today)
        await asyncio.to_thread(self._write_usage, at.date(), route, credits)

    def _write_usage(self, day: date, route: str, credits: int) -> None:
        with self._sessions.begin() as session:
            add_credit_usage(session, day, route, credits)

    async def refresh_key_info(self, client: CmcClient) -> Projection:
        """Hourly: read `/v1/key/info`, cap the request rate to the plan, re-anchor the governor, persist the
        snapshot, check the projection."""
        result = await client.call(KEY_INFO_ROUTE, KEY_INFO_PATH)
        info = parse_key_info(result.data, result.capture.fetched_at)
        client.cap_rate_to_plan(info.rate_limit_minute)
        self._governor.update_key_info(info)
        projection = await asyncio.to_thread(self._persist_key_info, info)
        self.last_projection = projection
        PROJECTION.set(projection.fraction)
        DAILY_BUDGET.set(self._governor.daily_budget)
        if projection.fraction > self._alert_fraction:
            log.warning(
                "cmc_credit_projection alert",
                extra={
                    "alert": "cmc_credit_projection",
                    "projected": round(projection.projected),
                    "limit": projection.limit_cycle,
                    "fraction": round(projection.fraction, 4),
                },
            )
        return projection

    def _persist_key_info(self, info: KeyInfo) -> Projection:
        with self._sessions.begin() as session:
            record_key_info(session, info, self._governor)
            daily = recent_daily_credits(session, info.checked_at.date(), RUN_RATE_DAYS)
        return project_cycle(info, daily, info.checked_at)


def recent_daily_credits(session: Session, today: date, days: int) -> list[int]:
    """Credits per completed UTC day over the last `days` days (today excluded: it is partial)."""
    start = today - timedelta(days=days)
    rows = session.execute(
        select(CmcCreditUsageRow.date, func.sum(CmcCreditUsageRow.credits))
        .where(CmcCreditUsageRow.date >= start, CmcCreditUsageRow.date < today)
        .group_by(CmcCreditUsageRow.date)
    ).all()
    return [int(total) for _day, total in rows]
