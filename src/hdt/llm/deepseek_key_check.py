"""Owner self-test for the `llm/deepseek_api_key` secret (runs in the `llm` scope owner service).

`GET https://api.deepseek.com/user/balance` with the candidate key: HTTP 200 proves the key is valid and
returns `is_available` (whether the balance can pay for API calls) and one balance per currency. A key
whose account cannot pay cannot serve any request, so it is refused. Only non-secret figures are returned
as check details: `is_available` and the total balance per currency.
"""

from __future__ import annotations

from typing import Final

import httpx

from hdt.llm.deepseek import DEEPSEEK_API_BASE
from hdt.vault.owner import CheckOutcome
from hdt.vault.scopes import ApiKeySecret

DEEPSEEK_BALANCE_URL: Final[str] = f"{DEEPSEEK_API_BASE}/user/balance"
SECRET_NAME: Final[str] = "deepseek_api_key"  # noqa: S105 - vault item name, not a credential
_CURRENCIES: Final[frozenset[str]] = frozenset({"USD", "CNY"})


def _amount(value: object) -> float | None:
    """DeepSeek reports balances as decimal strings (`"110.00"`)."""
    if isinstance(value, bool) or not isinstance(value, str | int | float):
        return None
    try:
        return float(value)
    except ValueError:
        return None


async def check_deepseek_key(
    secret: ApiKeySecret,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
    timeout_s: float = 15.0,
) -> CheckOutcome:
    try:
        async with httpx.AsyncClient(timeout=timeout_s, transport=transport) as client:
            response = await client.get(
                DEEPSEEK_BALANCE_URL,
                headers={"Authorization": f"Bearer {secret.api_key}", "Accept": "application/json"},
            )
    except httpx.HTTPError as exc:
        return CheckOutcome("error", f"DeepSeek unreachable: {type(exc).__name__}")
    if response.status_code in (401, 403):
        return CheckOutcome("invalid", f"DeepSeek rejected the key (HTTP {response.status_code})")
    if response.status_code != 200:
        return CheckOutcome("error", f"DeepSeek /user/balance returned HTTP {response.status_code}")
    try:
        body = response.json()
    except ValueError:
        return CheckOutcome("error", "DeepSeek /user/balance returned an unexpected body")
    available = body.get("is_available") if isinstance(body, dict) else None
    infos = body.get("balance_infos") if isinstance(body, dict) else None
    if not isinstance(available, bool) or not isinstance(infos, list):
        return CheckOutcome("error", "DeepSeek /user/balance returned an unexpected body")
    details: dict[str, str | int | float | bool | None] = {"is_available": available}
    for info in infos:
        currency = info.get("currency") if isinstance(info, dict) else None
        if isinstance(info, dict) and currency in _CURRENCIES:
            details[f"total_balance_{str(currency).lower()}"] = _amount(info.get("total_balance"))
    if not available:
        return CheckOutcome(
            "invalid", "the DeepSeek balance cannot pay for API calls (is_available false)", details
        )
    return CheckOutcome("valid", None, details)


async def owner_check(payload: object) -> CheckOutcome:
    if not isinstance(payload, ApiKeySecret):
        raise TypeError("deepseek_api_key payload must be an ApiKeySecret")
    return await check_deepseek_key(payload)
