"""Self-written USD-M futures REST client: httpx + HMAC-SHA256 (no SDK in the service holding the key).

- Signed requests carry `timestamp` (local clock + offset from `GET /fapi/v1/time`, re-synced on -1021)
  and `recvWindow`; the signature is the HMAC-SHA256 hex of the query string.
- Rate limits: `X-MBX-USED-WEIGHT-1M`, `X-MBX-ORDER-COUNT-10S`, `X-MBX-ORDER-COUNT-1M` are tracked against
  the ceilings read from `exchangeInfo`; near a ceiling the client waits for the window to roll instead of
  sending. 429 raises `RateLimitedError` and 418 `IpBannedError`; until the response's `Retry-After` has
  passed (60 s / 120 s without one) the client sends nothing and raises the same error locally, so a
  rate-limited namespace never hammers its way into an IP ban and a banned one stays silent.
- Mutating requests that time out or get HTTP 503 "Unknown error" raise `UnknownOrderStateError`: the order
  may exist, so callers verify by client id before retrying. Other 4xx on mutating requests raise
  `OrderRejectedError` (definitive).
- Nothing here logs a URL, query string, signature, API key or listenKey.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Final, Literal
from urllib.parse import urlencode

import httpx

from hdt.execution.adapter_base import (
    ExchangeError,
    IpBannedError,
    OrderRejectedError,
    RateLimitedError,
    UnknownOrderStateError,
)
from hdt.execution.filters import DEFAULT_RATE_LIMITS

log = logging.getLogger(__name__)

Method = Literal["GET", "POST", "PUT", "DELETE"]
TIMESTAMP_ERROR: Final[int] = -1021
ORDER_PATHS: Final[frozenset[str]] = frozenset({"/fapi/v1/order", "/fapi/v1/algoOrder"})
_WINDOW_MARGIN: Final[float] = 0.9
RATE_LIMIT_DEFAULT_S: Final[float] = 60.0
IP_BAN_DEFAULT_S: Final[float] = 120.0  # Binance bans for 2 minutes up to 3 days


@dataclass
class RateState:
    limits: dict[str, int] = field(default_factory=lambda: dict(DEFAULT_RATE_LIMITS))
    weight_1m: int = 0
    orders_10s: int = 0
    orders_1m: int = 0
    seen_at: float = 0.0
    retry_at: float = 0.0  # 429: nothing is sent before this wall-clock time
    banned_until: float = 0.0  # 418

    def back_off(self, status: int, headers: httpx.Headers, now: float) -> None:
        retry = headers.get("retry-after")
        if status == 429:
            wait = float(retry) if retry and retry.isdigit() else RATE_LIMIT_DEFAULT_S
            self.retry_at = max(self.retry_at, now + wait)
        elif status == 418:
            wait = float(retry) if retry and retry.isdigit() else IP_BAN_DEFAULT_S
            self.banned_until = max(self.banned_until, now + wait)

    def observe(self, headers: httpx.Headers, now: float) -> None:
        self.seen_at = now
        for header, attr in (
            ("x-mbx-used-weight-1m", "weight_1m"),
            ("x-mbx-order-count-10s", "orders_10s"),
            ("x-mbx-order-count-1m", "orders_1m"),
        ):
            value = headers.get(header)
            if value is not None and value.isdigit():
                setattr(self, attr, int(value))

    def wait_s(self, *, order: bool, now: float) -> float:
        """Seconds to wait before the next request so no ceiling is crossed (0 when safe)."""
        waits = [0.0]
        if self.weight_1m >= self.limits["weight_1m"] * _WINDOW_MARGIN:
            waits.append(60.0 - (now % 60.0))
        if order and self.orders_1m >= self.limits["orders_1m"] * _WINDOW_MARGIN:
            waits.append(60.0 - (now % 60.0))
        if order and self.orders_10s >= self.limits["orders_10s"] * _WINDOW_MARGIN:
            waits.append(10.0 - (now % 10.0))
        return max(waits)


def sign_query(params: Mapping[str, Any], secret: str) -> str:
    """URL-encoded query with `signature` appended (HMAC-SHA256 hex over the encoded query)."""
    query = urlencode([(k, _fmt(v)) for k, v in params.items() if v is not None])
    digest = hmac.new(secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{query}&signature={digest}" if query else f"signature={digest}"


def _fmt(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


class BinanceClient:
    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        api_secret: str,
        recv_window_ms: int = 5000,
        timeout_s: float = 10.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._http = httpx.AsyncClient(base_url=base_url.rstrip("/"), timeout=timeout_s, transport=transport)
        self._key = api_key
        self._secret = api_secret
        self._recv_window = recv_window_ms
        self._offset_ms = 0
        self._synced_at = float("-inf")
        self.rate = RateState()

    async def aclose(self) -> None:
        await self._http.aclose()

    def rotate(self, api_key: str, api_secret: str) -> None:
        """Swap credentials (only call when no signed request is in flight)."""
        self._key, self._secret = api_key, api_secret

    # ------------------------------------------------------------------ time
    async def sync_time(self) -> int:
        started = time.time()
        data = await self.public("GET", "/fapi/v1/time")
        ended = time.time()
        server = int(data["serverTime"])
        self._offset_ms = server - int((started + ended) / 2 * 1000)
        self._synced_at = time.monotonic()
        return self._offset_ms

    def _timestamp(self) -> int:
        return int(time.time() * 1000) + self._offset_ms

    # ------------------------------------------------------------------ requests
    async def public(self, method: Method, path: str, params: Mapping[str, Any] | None = None) -> Any:
        query = urlencode([(k, _fmt(v)) for k, v in (params or {}).items() if v is not None])
        return await self._send(method, path, query, signed=False, api_key=False)

    async def keyed(self, method: Method, path: str, params: Mapping[str, Any] | None = None) -> Any:
        """API-key-only endpoints (listenKey)."""
        query = urlencode([(k, _fmt(v)) for k, v in (params or {}).items() if v is not None])
        return await self._send(method, path, query, signed=False, api_key=True)

    async def signed(self, method: Method, path: str, params: Mapping[str, Any] | None = None) -> Any:
        if time.monotonic() - self._synced_at > 1800:
            await self.sync_time()
        for attempt in (1, 2):
            query = sign_query(
                {**(params or {}), "recvWindow": self._recv_window, "timestamp": self._timestamp()},
                self._secret,
            )
            try:
                return await self._send(method, path, query, signed=True, api_key=True)
            except ExchangeError as exc:
                if exc.code == TIMESTAMP_ERROR and attempt == 1:
                    await self.sync_time()
                    continue
                raise
        raise AssertionError("unreachable")  # pragma: no cover

    async def _send(self, method: Method, path: str, query: str, *, signed: bool, api_key: bool) -> Any:
        mutating = method != "GET"
        is_order = mutating and path in ORDER_PATHS
        now = time.time()
        if now < self.rate.banned_until:
            raise IpBannedError(
                f"{method} {path} not sent: IP banned for {self.rate.banned_until - now:.0f} s more",
                http_status=418,
                banned_until=self.rate.banned_until,
            )
        if now < self.rate.retry_at:
            raise RateLimitedError(
                f"{method} {path} not sent: rate limited for {self.rate.retry_at - now:.0f} s more",
                retry_after_s=self.rate.retry_at - now,
            )
        wait = self.rate.wait_s(order=is_order, now=now)
        if wait > 0:
            log.warning("binance rate ceiling near, waiting", extra={"path": path, "wait_s": round(wait, 2)})
            await asyncio.sleep(wait)
        headers = {"X-MBX-APIKEY": self._key} if api_key else {}
        url = f"{path}?{query}" if query else path
        try:
            response = await self._http.request(method, url, headers=headers)
        except httpx.TimeoutException as exc:
            if mutating:
                raise UnknownOrderStateError(f"{method} {path} timed out") from exc
            raise ExchangeError(f"{method} {path} timed out") from exc
        except httpx.HTTPError as exc:
            # The request may have reached the exchange before the connection broke.
            if mutating:
                raise UnknownOrderStateError(f"{method} {path} failed: {type(exc).__name__}") from exc
            raise ExchangeError(f"{method} {path} failed: {type(exc).__name__}") from exc
        self.rate.observe(response.headers, time.time())
        if response.status_code in (418, 429):
            self.rate.back_off(response.status_code, response.headers, time.time())
        try:
            return _parse(method, path, response, mutating=mutating)
        except IpBannedError as exc:
            exc.banned_until = self.rate.banned_until
            raise

    def banned_until(self) -> float | None:
        """Epoch seconds of the current IP ban end (HTTP 418 `Retry-After`); None when not banned."""
        return self.rate.banned_until if self.rate.banned_until > time.time() else None


def _body(response: httpx.Response) -> tuple[int | None, str]:
    try:
        data = response.json()
    except ValueError:
        return None, response.text[:200]
    if isinstance(data, dict):
        code = data.get("code")
        return (int(code) if isinstance(code, int) else None), str(data.get("msg", ""))[:300]
    return None, ""


def _parse(method: str, path: str, response: httpx.Response, *, mutating: bool) -> Any:
    status = response.status_code
    if 200 <= status < 300:
        try:
            return response.json()
        except ValueError as exc:
            if mutating:
                raise UnknownOrderStateError(f"{method} {path} returned a non-JSON body") from exc
            raise ExchangeError(f"{method} {path} returned a non-JSON body", http_status=status) from exc
    code, msg = _body(response)
    where = f"{method} {path} -> HTTP {status}"
    if status == 429:
        retry = response.headers.get("retry-after")
        raise RateLimitedError(
            f"{where}: {msg}", retry_after_s=float(retry) if retry and retry.isdigit() else 60.0, code=code
        )
    if status == 418:
        raise IpBannedError(f"{where}: {msg}", code=code, http_status=status)
    if status >= 500:
        if "unknown error" in msg.lower() or (mutating and status != 503):
            raise UnknownOrderStateError(f"{where}: {msg}", code=code, http_status=status)
        raise ExchangeError(f"{where}: {msg}", code=code, http_status=status)
    if mutating:
        raise OrderRejectedError(f"{where} code {code}: {msg}", code=code, http_status=status)
    raise ExchangeError(f"{where} code {code}: {msg}", code=code, http_status=status)
