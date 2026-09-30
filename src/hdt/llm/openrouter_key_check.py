"""Owner self-test for the `llm/openrouter_api_key` secret (runs in the `llm` scope owner service).

`GET https://openrouter.ai/api/v1/key` with the candidate key: HTTP 200 proves the key is valid and
returns `limit_remaining` (null = no per-key cap). A key whose credit cap is exhausted cannot serve any
request, so it is refused. Only non-secret figures are returned as check details; the key label is not
kept because OpenRouter may derive it from the key itself.
"""

from __future__ import annotations

from typing import Final

import httpx

from hdt.llm.openrouter_catalog import OPENROUTER_API_BASE
from hdt.vault.owner import CheckOutcome
from hdt.vault.scopes import ApiKeySecret

OPENROUTER_KEY_URL: Final[str] = f"{OPENROUTER_API_BASE}/key"
SECRET_NAME: Final[str] = "openrouter_api_key"  # noqa: S105 - vault item name, not a credential


def _number(value: object) -> float | None:
    return float(value) if isinstance(value, int | float) and not isinstance(value, bool) else None


async def check_openrouter_key(
    secret: ApiKeySecret,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
    timeout_s: float = 15.0,
) -> CheckOutcome:
    try:
        async with httpx.AsyncClient(timeout=timeout_s, transport=transport) as client:
            response = await client.get(
                OPENROUTER_KEY_URL,
                headers={"Authorization": f"Bearer {secret.api_key}", "Accept": "application/json"},
            )
    except httpx.HTTPError as exc:
        return CheckOutcome("error", f"OpenRouter unreachable: {type(exc).__name__}")
    if response.status_code in (401, 403):
        return CheckOutcome("invalid", f"OpenRouter rejected the key (HTTP {response.status_code})")
    if response.status_code != 200:
        return CheckOutcome("error", f"OpenRouter /key returned HTTP {response.status_code}")
    try:
        data = response.json()["data"]
    except (ValueError, KeyError, TypeError):
        return CheckOutcome("error", "OpenRouter /key returned an unexpected body")
    if not isinstance(data, dict):
        return CheckOutcome("error", "OpenRouter /key returned an unexpected body")
    limit_remaining = _number(data.get("limit_remaining"))
    details: dict[str, str | int | float | bool | None] = {
        "limit_remaining": limit_remaining,
        "limit": _number(data.get("limit")),
        "usage_daily": _number(data.get("usage_daily")),
        "usage_monthly": _number(data.get("usage_monthly")),
        "is_free_tier": data.get("is_free_tier") if isinstance(data.get("is_free_tier"), bool) else None,
    }
    if limit_remaining is not None and limit_remaining <= 0:
        return CheckOutcome("invalid", "the key's credit limit is exhausted (limit_remaining <= 0)", details)
    return CheckOutcome("valid", None, details)


async def owner_check(payload: object) -> CheckOutcome:
    if not isinstance(payload, ApiKeySecret):
        raise TypeError("openrouter_api_key payload must be an ApiKeySecret")
    return await check_openrouter_key(payload)
