"""CMC client: every response is recorded, halts never retry, keyless falls back only with budget."""

from __future__ import annotations

import asyncio
import json

import httpx
import pytest
import respx

from hdt.ingest.cmc_client import (
    KEY_HEADER,
    CmcClient,
    CmcConfigError,
    CmcHaltError,
    CmcNoKeyError,
    CmcNotOnPlanError,
    CmcResult,
    CmcUnavailableError,
    MinuteRateLimiter,
)

BASE = "https://pro-api.coinmarketcap.com"
KEYLESS = f"{BASE}/public-api"
KEY = "b54bcf4d-1bca-4e8e-9a24-22ff2c3d462c"


def _body(code: int = 0, credits: int = 1, message: str | None = None) -> bytes:
    status = {"timestamp": "2026-09-27T10:00:00.000Z", "error_code": code, "error_message": message}
    status["credit_count"] = credits
    return json.dumps({"status": status, "data": {"ok": True} if code == 0 else None}).encode()


def _client(seen: list[CmcResult], *, fallback: bool = True) -> CmcClient:
    async def record(result: CmcResult) -> None:
        seen.append(result)

    return CmcClient(
        httpx.AsyncClient(),
        base_url=BASE,
        keyless_base_url=KEYLESS,
        api_key=lambda: KEY,
        limiter=MinuteRateLimiter(60_000),
        keyless_limiter=MinuteRateLimiter(60_000),
        on_response=record,
        keyed_fallback_allowed=lambda: fallback,
        backoff_base_s=0.001,
    )


@respx.mock
async def test_success_is_recorded_with_credits_and_without_the_key() -> None:
    route = respx.get(f"{BASE}/v1/test").mock(return_value=httpx.Response(200, content=_body(credits=3)))
    seen: list[CmcResult] = []
    result = await _client(seen).call("test", "/v1/test", {"id": [1, 1027]})
    assert result.credit_count == 3
    assert result.data == {"ok": True}
    assert route.calls.last.request.headers[KEY_HEADER] == KEY
    assert route.calls.last.request.url.params["id"] == "1,1027"
    assert len(seen) == 1
    assert KEY not in seen[0].capture.params.__repr__()
    assert seen[0].capture.body == _body(credits=3)


@respx.mock
async def test_monthly_cap_halts_without_retrying() -> None:
    route = respx.get(f"{BASE}/v1/test").mock(
        return_value=httpx.Response(429, content=_body(1010, 0, "Monthly credit limit exceeded"))
    )
    seen: list[CmcResult] = []
    with pytest.raises(CmcHaltError) as err:
        await _client(seen).call("test", "/v1/test")
    assert err.value.kind == "monthly_cap"
    assert route.call_count == 1
    assert len(seen) == 1


@respx.mock
async def test_minute_rate_limit_is_retried() -> None:
    route = respx.get(f"{BASE}/v1/test").mock(
        side_effect=[httpx.Response(429, content=_body(1008, 0)), httpx.Response(200, content=_body())]
    )
    seen: list[CmcResult] = []
    assert (await _client(seen).call("test", "/v1/test")).ok
    assert route.call_count == 2
    assert len(seen) == 2


@respx.mock
async def test_plan_refusal_is_not_on_plan_and_not_retried() -> None:
    route = respx.get(f"{BASE}/v1/test").mock(
        return_value=httpx.Response(403, content=_body(1006, 0, "Your plan doesn't support this endpoint."))
    )
    seen: list[CmcResult] = []
    with pytest.raises(CmcNotOnPlanError) as refused:
        await _client(seen).call("test", "/v1/test")
    assert (refused.value.code, refused.value.http_status) == (1006, 403)
    assert route.call_count == 1
    assert len(seen) == 1  # the refusal is still recorded in the lake


@respx.mock
@pytest.mark.parametrize(
    ("http_status", "code"),
    [(401, 1001), (401, 1002), (402, 1003), (402, 1004), (403, 1005), (403, 1007), (403, 0), (401, 0)],
)
async def test_key_and_auth_errors_stay_configuration_errors(http_status: int, code: int) -> None:
    route = respx.get(f"{BASE}/v1/test").mock(
        return_value=httpx.Response(http_status, content=_body(code, 0, "refused"))
    )
    with pytest.raises(CmcConfigError) as refused:
        await _client([]).call("test", "/v1/test")
    assert not isinstance(refused.value, CmcNotOnPlanError | CmcNoKeyError)
    assert route.call_count == 1


@respx.mock
async def test_keyless_rate_limit_falls_back_to_keyed_when_budget_allows() -> None:
    keyless = respx.get(f"{KEYLESS}/v3/q").mock(return_value=httpx.Response(429, content=b"{}"))
    keyed = respx.get(f"{BASE}/v3/q").mock(return_value=httpx.Response(200, content=_body()))
    seen: list[CmcResult] = []
    result = await _client(seen).call("q", "/v3/q", keyless=True)
    assert not result.keyless
    assert KEY_HEADER not in keyless.calls.last.request.headers
    assert keyed.call_count == 1


@respx.mock
async def test_keyless_rate_limit_without_budget_does_not_spend_credits() -> None:
    respx.get(f"{KEYLESS}/v3/q").mock(return_value=httpx.Response(429, content=b"{}"))
    keyed = respx.get(f"{BASE}/v3/q").mock(return_value=httpx.Response(200, content=_body()))
    with pytest.raises(CmcUnavailableError):
        await _client([], fallback=False).call("q", "/v3/q", keyless=True)
    assert keyed.call_count == 0


async def test_keyed_call_without_an_active_key_fails_before_any_request() -> None:
    seen: list[CmcResult] = []
    client = _client(seen)
    client._api_key = lambda: None
    with pytest.raises(CmcNoKeyError):
        await client.call("test", "/v1/test")
    assert seen == []


class _FakeTime:
    """Virtual clock: a sleeper wakes at its own deadline; `hold` parks every sleeper until released."""

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeping = 0
        self.released = asyncio.Event()
        self.released.set()

    def clock(self) -> float:
        return self.now

    async def sleep(self, delay: float) -> None:
        target = self.now + delay
        self.sleeping += 1
        await self.released.wait()
        await asyncio.sleep(0)
        self.sleeping -= 1
        self.now = max(self.now, target)


def _max_in_any_minute(starts: list[float]) -> int:
    ordered = sorted(starts)
    return max(sum(1 for s in ordered if t <= s < t + 60.0) for t in ordered)


async def test_more_than_the_plan_limit_of_concurrent_callers_never_exceed_it_in_any_minute() -> None:
    fake = _FakeTime()
    limiter = MinuteRateLimiter(600, clock=fake.clock, sleep=fake.sleep)
    limiter.cap_to_plan(50)  # config says 600/min, the key's plan allows 50
    starts: list[float] = []

    async def caller() -> None:
        await limiter.wait()
        starts.append(fake.now)

    await asyncio.gather(*(caller() for _ in range(130)))
    assert len(starts) == 130
    assert _max_in_any_minute(starts) == 50


async def test_a_lower_plan_cap_also_holds_back_callers_already_waiting() -> None:
    fake = _FakeTime()
    fake.released.clear()
    limiter = MinuteRateLimiter(100, clock=fake.clock, sleep=fake.sleep)
    starts: list[float] = []

    async def caller() -> None:
        await limiter.wait()
        starts.append(fake.now)

    tasks = [asyncio.create_task(caller()) for _ in range(150)]
    while len(starts) < 100 or fake.sleeping < 50:  # 100 started at the configured rate, 50 queued
        await asyncio.sleep(0)
    limiter.cap_to_plan(20)  # key info answers: the plan allows only 20/min
    fake.released.set()
    await asyncio.gather(*tasks)
    later = [s for s in starts if s >= 60.0]
    assert len(later) == 50
    assert _max_in_any_minute(later) == 20


def test_the_plan_cap_never_lifts_the_configured_ceiling() -> None:
    limiter = MinuteRateLimiter(600)
    limiter.cap_to_plan(50)
    assert limiter.per_minute == 50
    limiter.cap_to_plan(10_000)  # a bigger plan: the configured rate stays the ceiling
    assert limiter.per_minute == 600
    limiter.cap_to_plan(0)  # no usable plan value keeps the current limit
    assert limiter.per_minute == 600


@respx.mock
async def test_keyless_calls_have_their_own_minute_window_and_never_hold_back_keyed_calls() -> None:
    fake = _FakeTime()
    keyless = respx.post(f"{KEYLESS}/v1/dex").mock(return_value=httpx.Response(200, content=_body()))
    keyed = respx.get(f"{BASE}/v1/test").mock(return_value=httpx.Response(200, content=_body()))
    client = _client([])
    client._keyless_limiter = MinuteRateLimiter(15, clock=fake.clock, sleep=fake.sleep)
    starts: list[float] = []

    async def dex() -> None:
        await client.call("dex", "/v1/dex", keyless=True, json_body={"tokenAddress": "0x1"})
        starts.append(fake.now)

    await asyncio.gather(*(dex() for _ in range(40)))
    assert keyless.call_count == 40
    assert _max_in_any_minute(starts) == 15  # 40 holder lookups spread over three minutes
    before = fake.now
    await asyncio.gather(*(client.call("test", "/v1/test") for _ in range(40)))
    assert keyed.call_count == 40
    assert fake.now == before  # the keyed window is untouched by the keyless one
