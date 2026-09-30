"""API keys page "Test" of the DeepSeek key: valid, refused, empty balance, outage; the key never echoed."""

from __future__ import annotations

import httpx
import pytest

from hdt.llm.deepseek_key_check import check_deepseek_key
from hdt.vault.scopes import ApiKeySecret

KEY = ApiKeySecret(api_key="sk-0123456789abcdef0123456789abcdef")


def _transport(status: int, body: object) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        assert (request.url.host, request.url.path) == ("api.deepseek.com", "/user/balance")
        assert request.headers["Authorization"] == f"Bearer {KEY.api_key}"
        return httpx.Response(status, json=body)

    return httpx.MockTransport(handler)


def _balance(available: bool, total: str) -> dict[str, object]:
    return {
        "is_available": available,
        "balance_infos": [
            {"currency": "USD", "total_balance": total, "granted_balance": "0.00", "topped_up_balance": total}
        ],
    }


async def test_a_key_with_a_usable_balance_is_valid_and_reports_it() -> None:
    outcome = await check_deepseek_key(KEY, transport=_transport(200, _balance(True, "12.50")))
    assert outcome.status == "valid"
    assert outcome.details == {"is_available": True, "total_balance_usd": 12.5}


async def test_a_balance_that_cannot_pay_is_refused() -> None:
    outcome = await check_deepseek_key(KEY, transport=_transport(200, _balance(False, "0.00")))
    assert outcome.status == "invalid"
    assert outcome.details == {"is_available": False, "total_balance_usd": 0.0}


async def test_a_rejected_key_is_invalid_without_echoing_it() -> None:
    body = {"error": {"message": "Authentication Fails", "type": "authentication_error"}}
    outcome = await check_deepseek_key(KEY, transport=_transport(401, body))
    assert outcome.status == "invalid"
    assert outcome.reason is not None
    assert KEY.api_key not in outcome.reason


@pytest.mark.parametrize(("status", "body"), [(503, {}), (200, {"unexpected": True})])
async def test_an_outage_or_unknown_body_is_an_error_not_a_verdict(status: int, body: object) -> None:
    outcome = await check_deepseek_key(KEY, transport=_transport(status, body))
    assert outcome.status == "error"
