"""Console "Test" button for the CMC key: valid, refused and unreachable outcomes, never echoing the key."""

from __future__ import annotations

import json

import httpx
import pytest

from hdt.ingest.cmc_key_check import check_cmc_key
from hdt.vault.scopes import ApiKeySecret

BASE = "https://pro-api.example"
KEY = ApiKeySecret(api_key="b54bcf4d-1bca-4e8e-9a24-22ff2c3d462c")


def _transport(status: int, body: object) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/key/info"
        assert request.headers["X-CMC_PRO_API_KEY"] == KEY.api_key
        return httpx.Response(status, content=json.dumps(body).encode())

    return httpx.MockTransport(handler)


async def test_valid_key_reports_plan_and_month_use() -> None:
    body = {
        "status": {"error_code": "0"},
        "data": {
            "plan": {
                "credit_limit_monthly": 450000,
                "credit_limit_monthly_reset": "In 10 days",
                "rate_limit_minute": 600,
            },
            "usage": {"current_month": {"credits_used": 1234, "credits_left": 448766}},
        },
    }
    outcome = await check_cmc_key(KEY, base_url=BASE, transport=_transport(200, body))
    assert outcome.status == "valid"
    assert outcome.details == {
        "credit_limit_monthly": 450000,
        "rate_limit_minute": 600,
        "credits_used_month": 1234,
        "credit_limit_monthly_reset": "In 10 days",
    }


@pytest.mark.parametrize(
    ("status", "code", "message"),
    [(401, 1001, "This API Key is invalid."), (403, 1006, "Your plan is not authorized"), (200, 1002, "")],
)
async def test_refused_key_is_invalid_with_the_cmc_reason(status: int, code: int, message: str) -> None:
    body = {"status": {"error_code": code, "error_message": message}, "data": None}
    outcome = await check_cmc_key(KEY, base_url=BASE, transport=_transport(status, body))
    assert outcome.status == "invalid"
    assert outcome.reason is not None
    assert KEY.api_key not in outcome.reason


async def test_rate_limit_and_outage_are_errors_not_verdicts() -> None:
    limited = await check_cmc_key(
        KEY, base_url=BASE, transport=_transport(429, {"status": {"error_code": 1008}})
    )
    assert limited.status == "error"

    def down(_request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused")

    unreachable = await check_cmc_key(KEY, base_url=BASE, transport=httpx.MockTransport(down))
    assert unreachable.status == "error"
    assert unreachable.reason == "CoinMarketCap unreachable: ConnectError"
