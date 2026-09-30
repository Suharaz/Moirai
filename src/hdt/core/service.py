"""Long-running service helpers: stop signals and periodic loops that survive a failing iteration."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
from collections.abc import Awaitable, Callable
from typing import Any

log = logging.getLogger(__name__)


def install_stop_signals(stop: asyncio.Event) -> None:
    """SIGINT / SIGTERM (and SIGBREAK on Windows) set `stop`, so services drain instead of dying mid-write."""
    loop = asyncio.get_running_loop()
    for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        sig = getattr(signal, name, None)
        if sig is None:
            continue
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            # Windows event loops have no add_signal_handler: set the event from the signal thread.
            def handler(_signum: int, _frame: Any) -> None:
                loop.call_soon_threadsafe(stop.set)

            with contextlib.suppress(ValueError, OSError):
                signal.signal(sig, handler)


async def sleep_or_stop(stop: asyncio.Event, seconds: float) -> bool:
    """Wait `seconds` or until `stop` is set; True when stopping."""
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=max(0.0, seconds))
    return stop.is_set()


async def every(
    stop: asyncio.Event,
    interval_s: float | Callable[[], float],
    fn: Callable[[], Awaitable[object]],
    what: str,
) -> None:
    """Run `fn` every `interval_s` until `stop`; an iteration that raises is logged and the loop goes on."""
    while not stop.is_set():
        try:
            await fn()
        except Exception:
            log.exception("%s failed", what)
        interval = interval_s() if callable(interval_s) else interval_s
        if await sleep_or_stop(stop, interval):
            return
