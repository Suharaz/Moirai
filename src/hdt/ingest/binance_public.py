"""Binance USDS-M public REST (no key): every response is recorded verbatim before it is parsed.

Weight: the IP limit (2400/min) is tracked from `X-MBX-USED-WEIGHT-1M`; calls pause when a request
would cross `weight_ceiling`. 429 honours `Retry-After`; 418 (IP ban) stops every call until the ban
ends and is surfaced to the caller so the recorder raises an alert.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Final

import httpx

from hdt.core.clock import utcnow
from hdt.lake.schemas import Capture

log = logging.getLogger(__name__)

WEIGHT_HEADER: Final[str] = "X-MBX-USED-WEIGHT-1M"
DEFAULT_RETRY_AFTER_S: Final[int] = 60


class BinanceBackoffError(RuntimeError):
    def __init__(self, message: str, *, until: datetime, banned: bool) -> None:
        super().__init__(message)
        self.until = until
        self.banned = banned


class BinanceHttpError(RuntimeError):
    def __init__(self, message: str, *, http_status: int, code: int | None) -> None:
        super().__init__(message)
        self.http_status = http_status
        self.code = code


@dataclass(frozen=True)
class BinanceResult:
    route: str
    capture: Capture
    data: Any
    used_weight: int | None


CaptureSink = Callable[[Capture], Awaitable[None]]


class BinancePublic:
    def __init__(
        self,
        http: httpx.AsyncClient,
        *,
        base_url: str,
        on_capture: CaptureSink,
        weight_ceiling: int = 2000,
    ) -> None:
        self._http = http
        self._base = base_url.rstrip("/")
        self._sink = on_capture
        self._ceiling = weight_ceiling
        self._used_weight = 0
        self._window_minute: int | None = None
        self._blocked_until: datetime | None = None
        self._banned = False

    async def get(
        self,
        route: str,
        path: str,
        params: Mapping[str, Any] | None = None,
        *,
        key: str = "",
        weight: int = 1,
    ) -> BinanceResult:
        await self._respect_limits(weight)
        query = {k: str(v) for k, v in (params or {}).items()}
        response = await self._http.get(f"{self._base}{path}", params=query)
        fetched_at = utcnow()
        capture = Capture(
            source="binance",
            route=route,
            fetched_at=fetched_at,
            http_status=response.status_code,
            body=response.content,
            params={"path": path, **query},
            key=key,
        )
        await self._sink(capture)
        used = response.headers.get(WEIGHT_HEADER)
        if used is not None and used.isdigit():
            self._used_weight, self._window_minute = int(used), _minute(fetched_at)
        if response.status_code in (418, 429):
            retry = response.headers.get("Retry-After", "")
            wait_s = int(retry) if retry.isdigit() else DEFAULT_RETRY_AFTER_S
            self._blocked_until = fetched_at + timedelta(seconds=wait_s)
            self._banned = response.status_code == 418
            log.warning(
                "binance rate limit",
                extra={"route": route, "status": response.status_code, "retry_s": wait_s},
            )
            raise BinanceBackoffError(
                f"{route}: HTTP {response.status_code}", until=self._blocked_until, banned=self._banned
            )
        data = _json(response.content)
        if response.status_code != 200:
            code = data.get("code") if isinstance(data, dict) else None
            raise BinanceHttpError(
                f"{route}: HTTP {response.status_code} {data.get('msg') if isinstance(data, dict) else ''}",
                http_status=response.status_code,
                code=code if isinstance(code, int) else None,
            )
        return BinanceResult(route, capture, data, self._used_weight)

    async def _respect_limits(self, weight: int) -> None:
        now = utcnow()
        if self._blocked_until is not None:
            if now < self._blocked_until:
                raise BinanceBackoffError(
                    "binance calls paused after a rate limit", until=self._blocked_until, banned=self._banned
                )
            self._blocked_until, self._banned = None, False
        if self._window_minute == _minute(now) and self._used_weight + weight > self._ceiling:
            await asyncio.sleep(60 - now.second - now.microsecond / 1e6 + 0.05)


def _minute(ts: datetime) -> int:
    return int(ts.timestamp()) // 60


def _json(body: bytes) -> Any:
    try:
        return json.loads(body)
    except ValueError:
        return None
