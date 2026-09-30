"""CMC credit governor: daily budget from the billing anchor, load shedding, halt states.

Daily budget = credits left in the monthly cycle / days until the cycle resets, fixed at the first key
info of each UTC day (`/v1/key/info`, 0 credits). As today's metered use grows, route classes are shed in
order: context first, derivatives last; `system` (key info) is never shed. Error codes 1009/1010/1011
put the governor in a halt state until the cap resets; while halted nothing is admitted.

Any shedding or halt sets `degraded`: the council runs CMC-degraded (held positions are managed with
Binance data, stale CMC never triggers the kill switch).
"""

from __future__ import annotations

import calendar
import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any, Final

from hdt.core.clock import ensure_utc, utcnow
from hdt.core.config import ShedClass

# Fraction of today's budget above which a class is shed (evaluated on metered use before the call).
SHED_AT: Final[Mapping[str, float]] = {
    "context": 0.70,
    "dex": 0.75,
    "attention": 0.80,
    "price": 0.90,
    "derivatives": 1.00,
    "system": math.inf,
}
IP_LIMIT_COOLDOWN: Final[timedelta] = timedelta(minutes=5)


@dataclass(frozen=True)
class KeyInfo:
    checked_at: datetime
    credit_limit_cycle: int
    credits_used_cycle: int
    credits_left_cycle: int
    cycle_end: datetime
    credits_used_today: int
    rate_limit_minute: int | None
    daily_reset: datetime | None

    @property
    def cycle_start(self) -> datetime:
        return _add_months(self.cycle_end, -1)


class KeyInfoError(ValueError):
    """`/v1/key/info` returned a body without the fields the governor needs."""


def parse_key_info(data: Any, checked_at: datetime) -> KeyInfo:
    try:
        plan, usage = data["plan"], data["usage"]
        month = usage["current_month"]
        limit = int(plan["credit_limit_monthly"])
        used = int(month["credits_used"])
        left = int(month.get("credits_left", limit - used))
        cycle_end = _ts(plan["credit_limit_monthly_reset_timestamp"])
    except (KeyError, TypeError, ValueError) as exc:
        raise KeyInfoError(f"unexpected /v1/key/info body: {type(exc).__name__}: {exc}") from None
    day = usage.get("current_day") or {}
    daily_reset = plan.get("credit_limit_daily_reset_timestamp")
    rate = plan.get("rate_limit_minute")
    return KeyInfo(
        checked_at=ensure_utc(checked_at),
        credit_limit_cycle=limit,
        credits_used_cycle=used,
        credits_left_cycle=left,
        cycle_end=cycle_end,
        credits_used_today=int(day.get("credits_used") or 0),
        rate_limit_minute=int(rate) if isinstance(rate, int) else None,
        daily_reset=_ts(daily_reset) if isinstance(daily_reset, str) else None,
    )


@dataclass(frozen=True)
class Admission:
    allowed: bool
    reason: str | None = None


@dataclass
class _Halt:
    kind: str
    until: datetime


class CreditGovernor:
    def __init__(self, *, fallback_monthly_quota: int) -> None:
        self._fallback_quota = fallback_monthly_quota
        self.key_info: KeyInfo | None = None
        self._day: date | None = None
        self._budget: int = 0
        self._used_today = 0
        self._halt: _Halt | None = None
        self._shed: set[str] = set()
        self._budget_from_key_info = False

    # ------------------------------------------------------------------ inputs

    def update_key_info(self, info: KeyInfo) -> None:
        """Take a key info snapshot; today's budget is fixed from the first snapshot of the UTC day."""
        self.key_info = info
        self._roll(info.checked_at)
        if not self._budget_from_key_info:
            self._budget = _budget(info, info.checked_at)
            self._budget_from_key_info = True
        # CMC's own day counter also covers calls this process did not meter (e.g. before a restart).
        self._used_today = max(self._used_today, info.credits_used_today)

    def record(self, credits: int, at: datetime | None = None) -> None:
        self._roll(ensure_utc(at or utcnow()))
        self._used_today += max(credits, 0)

    def halt(self, kind: str, at: datetime | None = None) -> datetime:
        now = ensure_utc(at or utcnow())
        info = self.key_info
        if kind == "monthly_cap":
            until = info.cycle_end if info and info.cycle_end > now else _next_midnight(now)
        elif kind == "daily_cap":
            until = (
                info.daily_reset
                if info and info.daily_reset and info.daily_reset > now
                else _next_midnight(now)
            )
        else:
            until = now + IP_LIMIT_COOLDOWN
        self._halt = _Halt(kind, until)
        return until

    # ------------------------------------------------------------------ decisions

    def admit(self, shed_class: ShedClass, est_credits: int = 1, at: datetime | None = None) -> Admission:
        now = ensure_utc(at or utcnow())
        self._roll(now)
        halted = self.halt_state(now)
        if halted is not None:
            return Admission(False, f"halted ({halted})")
        if shed_class == "system":
            return Admission(True)
        if self._budget <= 0 or (self._used_today + est_credits) / self._budget > SHED_AT[shed_class]:
            self._shed.add(shed_class)
            return Admission(False, f"shed: {shed_class} over its share of today's budget")
        self._shed.discard(shed_class)
        return Admission(True)

    def keyed_fallback_allowed(self, shed_class: ShedClass = "context") -> bool:
        return self.admit(shed_class).allowed

    def halt_state(self, at: datetime | None = None) -> str | None:
        if self._halt is None:
            return None
        if ensure_utc(at or utcnow()) >= self._halt.until:
            self._halt = None
            return None
        return self._halt.kind

    @property
    def degraded(self) -> bool:
        return self.halt_state() is not None or bool(self._shed)

    @property
    def daily_budget(self) -> int:
        return self._budget

    @property
    def used_today(self) -> int:
        return self._used_today

    # ------------------------------------------------------------------ internals

    def _roll(self, now: datetime) -> None:
        today = now.date()
        if self._day == today:
            return
        self._day = today
        self._shed.clear()
        self._used_today = 0
        info = self.key_info
        self._budget = _budget(info, now) if info is not None else self._fallback_quota // 30
        self._budget_from_key_info = info is not None


def _budget(info: KeyInfo, now: datetime) -> int:
    days_left = max(1, math.ceil((info.cycle_end - now) / timedelta(days=1)))
    return max(info.credits_left_cycle, 0) // days_left


def _ts(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("timestamp must be a string")
    return ensure_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))


def _next_midnight(now: datetime) -> datetime:
    return datetime(now.year, now.month, now.day, tzinfo=UTC) + timedelta(days=1)


def _add_months(ts: datetime, months: int) -> datetime:
    month_index = ts.month - 1 + months
    year, month = ts.year + month_index // 12, month_index % 12 + 1
    return ts.replace(year=year, month=month, day=min(ts.day, calendar.monthrange(year, month)[1]))
