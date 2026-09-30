"""Single UTC time source, injectable for deterministic replay and tests.

Business code calls `utcnow()` (never `datetime.now()`); replay installs a `ManualClock` with `use_clock`.
The active clock is held in a context variable, so concurrent asyncio tasks can run on different clocks.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime, timedelta
from typing import Protocol


class Clock(Protocol):
    def now(self) -> datetime: ...


class SystemClock:
    """Wall clock in UTC."""

    def now(self) -> datetime:
        return datetime.now(UTC)


class ManualClock:
    """Clock that only moves when told to (replay, tests)."""

    def __init__(self, start: datetime) -> None:
        self._now = ensure_utc(start)

    def now(self) -> datetime:
        return self._now

    def set(self, value: datetime) -> None:
        value = ensure_utc(value)
        if value < self._now:
            raise ValueError("ManualClock cannot move backwards")
        self._now = value

    def advance(self, delta: timedelta) -> datetime:
        if delta < timedelta(0):
            raise ValueError("ManualClock cannot move backwards")
        self._now = self._now + delta
        return self._now


_SYSTEM = SystemClock()
_active: ContextVar[Clock] = ContextVar("hdt_clock", default=_SYSTEM)


def ensure_utc(value: datetime) -> datetime:
    """Return `value` as an aware UTC datetime; naive datetimes are rejected."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("naive datetime is not allowed; use an aware UTC datetime")
    return value.astimezone(UTC)


def get_clock() -> Clock:
    return _active.get()


def set_clock(clock: Clock) -> None:
    _active.set(clock)


def utcnow() -> datetime:
    return ensure_utc(_active.get().now())


@contextmanager
def use_clock(clock: Clock) -> Iterator[Clock]:
    token = _active.set(clock)
    try:
        yield clock
    finally:
        _active.reset(token)
