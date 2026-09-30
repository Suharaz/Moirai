"""CoinMarketCap REST client: keyed and keyless calls, envelope parsing, typed errors.

Every HTTP response (success or error) is handed to `on_response` before anything else happens, so the
recorder stores it verbatim in the lake and meters its `status.credit_count`.

Error policy (docs/api-endpoints-design.md section 1):
- 1006 (the key's plan does not include the endpoint): `CmcNotOnPlanError`, never retried; the route is
  not on the current plan, which is not a key problem (the error response is not charged).
- 1001-1005, 1007 and other 401/402/403 (key missing/invalid/disabled/unpaid): `CmcConfigError`, never
  retried. A keyed call without an active key raises `CmcNoKeyError` before any HTTP.
- 1008 per-minute limit and HTTP 429 / 5xx / network errors: retried with exponential backoff + jitter.
- 1009 daily cap, 1010 monthly cap, 1011 IP limit: `CmcHaltError`, never retried; the credit governor
  enters the matching halt state.
- Keyless calls that hit 429 or 1005 fall back to the keyed base only while `keyed_fallback_allowed()`.
- Keyed calls share the plan's per-minute window (`limiter`); keyless calls have their own, much lower one
  (`keyless_limiter`): the keyless base limits per IP and answers a burst with 1011, which halts every route.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from collections import deque
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final, Literal

import httpx

from hdt.core.clock import ensure_utc, utcnow
from hdt.lake.schemas import Capture

log = logging.getLogger(__name__)

KEY_HEADER: Final[str] = "X-CMC_PRO_API_KEY"
CONFIG_CODES: Final[frozenset[int]] = frozenset({1001, 1002, 1003, 1004, 1005, 1006, 1007})
PLAN_REFUSED_CODE: Final[int] = 1006  # API_KEY_PLAN_NOT_AUTHORIZED, HTTP 403
RETRY_CODES: Final[frozenset[int]] = frozenset({1008})
HALT_CODES: Final[dict[int, str]] = {1009: "daily_cap", 1010: "monthly_cap", 1011: "ip_limit"}
HaltKind = Literal["daily_cap", "monthly_cap", "ip_limit"]


class CmcError(RuntimeError):
    def __init__(self, message: str, *, code: int | None = None, http_status: int | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.http_status = http_status


class CmcConfigError(CmcError):
    """Key or plan problem: fix the configuration, retrying cannot help."""


class CmcNotOnPlanError(CmcConfigError):
    """1006: the key's subscription plan does not include this endpoint; a plan upgrade enables it."""


class CmcNoKeyError(CmcConfigError):
    """A keyed call was attempted while no CMC key is active in the vault."""


class CmcHaltError(CmcError):
    """A credit or IP cap was hit; the governor must stop calling until the cap resets."""

    def __init__(self, message: str, *, kind: str, code: int, http_status: int) -> None:
        super().__init__(message, code=code, http_status=http_status)
        self.kind = kind


class CmcUnavailableError(CmcError):
    """Retries exhausted on rate limits, server errors or network failures."""


@dataclass(frozen=True)
class CmcResult:
    route: str
    keyless: bool
    capture: Capture
    http_status: int
    error_code: int
    error_message: str | None
    credit_count: int
    status_ts: datetime | None
    data: Any

    @property
    def ok(self) -> bool:
        return self.http_status == 200 and self.error_code == 0


ResponseSink = Callable[[CmcResult], Awaitable[None]]


class MinuteRateLimiter:
    """At most `per_minute` calls start in any rolling 60 s window.

    `per_minute` is the configured ceiling; `cap_to_plan` sets the effective limit to the key's real plan
    limit (`/v1/key/info` `rate_limit_minute`), never above the ceiling. The limit is re-read on every
    attempt, so a lower cap also holds back callers that were already waiting when it was applied."""

    WINDOW_S: Final[float] = 60.0

    def __init__(
        self,
        per_minute: int,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._ceiling = per_minute
        self._limit = per_minute
        self._starts: deque[float] = deque()
        self._lock = asyncio.Lock()
        self._clock = clock
        self._sleep = sleep

    @property
    def per_minute(self) -> int:
        return self._limit

    def cap_to_plan(self, plan_per_minute: int) -> None:
        if plan_per_minute > 0:
            self._limit = min(self._ceiling, plan_per_minute)

    async def wait(self) -> None:
        while True:
            async with self._lock:
                now = self._clock()
                while self._starts and now - self._starts[0] >= self.WINDOW_S:
                    self._starts.popleft()
                excess = len(self._starts) - self._limit
                if excess < 0:
                    self._starts.append(now)
                    return
                # the window frees a slot once the start `excess` places after the oldest has aged out
                delay = self._starts[excess] + self.WINDOW_S - now
            await self._sleep(max(delay, 0.001))


class CmcClient:
    def __init__(
        self,
        http: httpx.AsyncClient,
        *,
        base_url: str,
        keyless_base_url: str,
        api_key: Callable[[], str | None],
        limiter: MinuteRateLimiter,
        keyless_limiter: MinuteRateLimiter,
        on_response: ResponseSink,
        keyed_fallback_allowed: Callable[[], bool] = lambda: True,
        max_attempts: int = 3,
        backoff_base_s: float = 1.0,
    ) -> None:
        self._http = http
        self._base = base_url.rstrip("/")
        self._keyless_base = keyless_base_url.rstrip("/")
        self._api_key = api_key
        self._limiter = limiter
        self._keyless_limiter = keyless_limiter
        self._sink = on_response
        self._fallback_allowed = keyed_fallback_allowed
        self._max_attempts = max_attempts
        self._backoff = backoff_base_s

    @property
    def rate_per_minute(self) -> int:
        return self._limiter.per_minute

    def cap_rate_to_plan(self, plan_per_minute: int | None) -> None:
        """Apply the key's real per-minute limit from `/v1/key/info` (the config value stays the ceiling)."""
        if plan_per_minute is not None:
            self._limiter.cap_to_plan(plan_per_minute)

    async def call(
        self,
        route: str,
        path: str,
        params: Mapping[str, Any] | None = None,
        *,
        keyless: bool = False,
        key: str = "",
        json_body: Mapping[str, Any] | None = None,
    ) -> CmcResult:
        """Call with the retry, halt and keyless-fallback policy; returns only successful results."""
        use_keyless = keyless
        last: CmcResult | None = None
        for attempt in range(1, self._max_attempts + 1):
            try:
                result = await self.fetch(
                    route, path, params, keyless=use_keyless, key=key, json_body=json_body
                )
            except httpx.HTTPError as exc:
                log.warning("cmc request failed", extra={"route": route, "error": type(exc).__name__})
                if attempt == self._max_attempts:
                    raise CmcUnavailableError(f"{route}: {type(exc).__name__}") from None
                await self._sleep(attempt)
                continue
            if result.ok:
                return result
            last = result
            code = result.error_code
            if code in HALT_CODES:
                raise CmcHaltError(
                    f"{route}: {result.error_message}",
                    kind=HALT_CODES[code],
                    code=code,
                    http_status=result.http_status,
                )
            if use_keyless and (result.http_status == 429 or code == 1005):
                if not self._fallback_allowed():
                    raise CmcUnavailableError(
                        f"{route}: keyless rate limited and the governor has no budget for a keyed call",
                        code=code or None,
                        http_status=result.http_status,
                    )
                use_keyless = False
                continue
            if code == PLAN_REFUSED_CODE:
                raise CmcNotOnPlanError(
                    f"{route}: {result.error_message or 'plan does not support this endpoint'}",
                    code=code,
                    http_status=result.http_status,
                )
            if code in CONFIG_CODES or result.http_status in (401, 402, 403):
                raise CmcConfigError(
                    f"{route}: {result.error_message or f'HTTP {result.http_status}'}",
                    code=code or None,
                    http_status=result.http_status,
                )
            retryable = code in RETRY_CODES or result.http_status == 429 or result.http_status >= 500
            if retryable and attempt < self._max_attempts:
                await self._sleep(attempt)
                continue
            break
        assert last is not None
        raise CmcUnavailableError(
            f"{last.route}: {last.error_message or f'HTTP {last.http_status}'}",
            code=last.error_code or None,
            http_status=last.http_status,
        )

    async def fetch(
        self,
        route: str,
        path: str,
        params: Mapping[str, Any] | None = None,
        *,
        keyless: bool = False,
        key: str = "",
        json_body: Mapping[str, Any] | None = None,
    ) -> CmcResult:
        """One HTTP call (GET, or POST when `json_body` is given); the response is always recorded."""
        headers = {"Accept": "application/json"}
        if not keyless:
            api_key = self._api_key()
            if not api_key:
                raise CmcNoKeyError(f"{route}: no active CMC key (enter it on the console API keys page)")
            headers[KEY_HEADER] = api_key
        base = self._keyless_base if keyless else self._base
        query = {k: _param(v) for k, v in (params or {}).items()}
        await (self._keyless_limiter if keyless else self._limiter).wait()
        if json_body is None:
            response = await self._http.get(f"{base}{path}", params=query, headers=headers)
        else:
            response = await self._http.post(
                f"{base}{path}", params=query, headers=headers, json=dict(json_body)
            )
            query = {**query, **{f"body.{k}": _param(v) for k, v in json_body.items()}}
        fetched_at = utcnow()
        body = response.content
        status, data = _envelope(body)
        status_ts = _parse_ts(status.get("timestamp"))
        result = CmcResult(
            route=route,
            keyless=keyless,
            capture=Capture(
                source="cmc",
                route=route,
                fetched_at=fetched_at,
                http_status=response.status_code,
                body=body,
                params={"path": path, "keyless": keyless, **query},
                key=key,
                status_ts=status_ts,
            ),
            http_status=response.status_code,
            error_code=_int(status.get("error_code")),
            error_message=status.get("error_message")
            if isinstance(status.get("error_message"), str)
            else None,
            credit_count=_int(status.get("credit_count")),
            status_ts=status_ts,
            data=data,
        )
        await self._sink(result)
        return result

    async def _sleep(self, attempt: int) -> None:
        delay = self._backoff * 2 ** (attempt - 1)
        await asyncio.sleep(delay + random.uniform(0, delay / 2))  # noqa: S311 - jitter, not security


def _envelope(body: bytes) -> tuple[dict[str, Any], Any]:
    try:
        parsed = json.loads(body)
    except ValueError:
        return {}, None
    if not isinstance(parsed, dict):
        return {}, None
    status = parsed.get("status")
    return (status if isinstance(status, dict) else {}), parsed.get("data")


def _int(value: object) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return 0


def _param(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, list | tuple):
        return ",".join(str(v) for v in value)
    return str(value)


def _parse_ts(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return ensure_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except ValueError:
        return None
