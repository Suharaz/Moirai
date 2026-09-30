"""Owner self-test of the Binance keys (vault scope `exec`): signed `GET /fapi/v3/account` with the candidate.

A definitive refusal (HTTP 401, -2014 key format, -2015 key / IP / permission, -1022 signature) marks the
key `invalid`; anything that only means the check could not complete is `error`; the key's values never
appear in the outcome.
"""

from __future__ import annotations

from datetime import UTC, datetime

import httpx
import pytest

from binance_shapes import ACCOUNT_V3, API_KEY, API_SECRET, BASE_URL, BinanceRoutes, error
from hdt.execution.key_check import ACCOUNT_PATH, check_binance_key
from hdt.vault.owner import CheckOutcome
from hdt.vault.scopes import BinanceKeySecret

SECRET = BinanceKeySecret(api_key=API_KEY, api_secret=API_SECRET)


async def _check(routes: BinanceRoutes) -> CheckOutcome:
    return await check_binance_key(SECRET, base_url=BASE_URL, transport=routes.transport())


def _no_secret_in(outcome: CheckOutcome) -> None:
    text = f"{outcome.reason} {dict(outcome.details)}"
    assert API_KEY not in text
    assert API_SECRET not in text


async def test_a_working_key_is_valid_with_wallet_details() -> None:
    routes = BinanceRoutes()
    routes.queue("GET", ACCOUNT_PATH, ACCOUNT_V3)
    outcome = await _check(routes)
    assert outcome.status == "valid"
    assert dict(outcome.details) == {
        "host": BASE_URL,
        "total_wallet_balance": "15123.45000000",
        "available_balance": "14340.07466667",
        "positions": 1,
    }
    (sent,) = routes.calls("GET", ACCOUNT_PATH)
    assert sent.headers["x-mbx-apikey"] == API_KEY
    assert "signature" in sent.params
    _no_secret_in(outcome)


@pytest.mark.parametrize(
    "refusal",
    [
        error(401, -2015, "Invalid API-key, IP, or permissions for action."),
        error(400, -2015, "Invalid API-key, IP, or permissions for action."),
        error(400, -2014, "API-key format invalid."),
        error(400, -1022, "Signature for this request is not valid."),
        error(401, -1, ""),
    ],
)
async def test_definitive_refusals_mark_the_key_invalid(refusal: httpx.Response) -> None:
    routes = BinanceRoutes()
    routes.queue("GET", ACCOUNT_PATH, refusal)
    outcome = await _check(routes)
    assert outcome.status == "invalid"
    assert "refused" in (outcome.reason or "")
    _no_secret_in(outcome)


def _connect_error(request: httpx.Request) -> httpx.Response:
    raise httpx.ConnectError("unreachable", request=request)


@pytest.mark.parametrize(
    "failure",
    [
        error(429, -1003, "Too many requests.", {"Retry-After": "30"}),
        error(418, -1003, "IP banned.", {"Retry-After": "120"}),
        error(500, -1000, "Internal error."),
        error(400, -1021, "Timestamp for this request is outside of the recvWindow."),
        _connect_error,
        [ACCOUNT_V3],  # not an object
    ],
)
async def test_checks_that_cannot_complete_are_errors_not_verdicts(failure: object) -> None:
    routes = BinanceRoutes()
    routes.always("GET", ACCOUNT_PATH, failure)
    outcome = await _check(routes)
    assert outcome.status == "error"
    _no_secret_in(outcome)


async def test_an_ip_ban_is_reported_as_error_and_handed_to_the_recorder() -> None:
    routes = BinanceRoutes()
    routes.queue(
        "GET", ACCOUNT_PATH, error(418, -1003, "Way too many requests; IP banned.", {"Retry-After": "600"})
    )
    recorded: list[datetime] = []
    before = datetime.now(UTC)
    outcome = await check_binance_key(
        SECRET, base_url=BASE_URL, transport=routes.transport(), on_ip_ban=recorded.append
    )
    assert outcome.status == "error"
    assert "HTTP 418" in outcome.reason
    (until,) = recorded
    assert 590 <= (until - before).total_seconds() <= 610
    _no_secret_in(outcome)
