"""Owner self-test for the `data/cmc_api_key` secret (runs in the recorder, the `data` scope owner).

`GET /v1/key/info` with the candidate key costs 0 credits. HTTP 200 with `error_code` 0 proves the key
works and returns the plan's monthly credit limit and per-minute rate limit, which are shown as check
details. 401/403 or error codes 1001-1002 (missing/invalid key) and 1003-1007 (disabled key or plan
without access) are definitive refusals; anything else means the check could not complete.
"""

from __future__ import annotations

from typing import Any, Final

import httpx

from hdt.core.config import static_config
from hdt.ingest.cmc_client import CONFIG_CODES, KEY_HEADER
from hdt.vault.owner import CheckOutcome, SecretCheck
from hdt.vault.scopes import ApiKeySecret

SECRET_NAME: Final[str] = "cmc_api_key"  # noqa: S105 - vault item name, not a credential
KEY_INFO_PATH: Final[str] = "/v1/key/info"


def _int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


async def check_cmc_key(
    secret: ApiKeySecret,
    *,
    base_url: str | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
    timeout_s: float = 15.0,
) -> CheckOutcome:
    base = (base_url or static_config().settings.cmc.base_url).rstrip("/")
    try:
        async with httpx.AsyncClient(timeout=timeout_s, transport=transport) as client:
            response = await client.get(
                f"{base}{KEY_INFO_PATH}",
                headers={KEY_HEADER: secret.api_key, "Accept": "application/json"},
            )
    except httpx.HTTPError as exc:
        return CheckOutcome("error", f"CoinMarketCap unreachable: {type(exc).__name__}")
    try:
        body = response.json()
    except ValueError:
        body = None
    status = body.get("status") if isinstance(body, dict) else None
    code = _int(status.get("error_code")) if isinstance(status, dict) else None
    message = status.get("error_message") if isinstance(status, dict) else None
    if response.status_code in (401, 403) or (code is not None and code in CONFIG_CODES):
        reason = message if isinstance(message, str) and message else f"HTTP {response.status_code}"
        return CheckOutcome("invalid", f"CoinMarketCap refused the key: {reason}")
    if response.status_code != 200 or code not in (0, None):
        return CheckOutcome("error", f"/v1/key/info returned HTTP {response.status_code}, error_code {code}")
    data = body.get("data") if isinstance(body, dict) else None
    plan = data.get("plan") if isinstance(data, dict) else None
    usage = data.get("usage") if isinstance(data, dict) else None
    if not isinstance(plan, dict) or not isinstance(usage, dict):
        return CheckOutcome("error", "/v1/key/info returned an unexpected body")
    raw_month = usage.get("current_month")
    month: dict[str, Any] = raw_month if isinstance(raw_month, dict) else {}
    details: dict[str, str | int | float | bool | None] = {
        "credit_limit_monthly": _int(plan.get("credit_limit_monthly")),
        "rate_limit_minute": _int(plan.get("rate_limit_minute")),
        "credits_used_month": _int(month.get("credits_used")),
        "credit_limit_monthly_reset": plan.get("credit_limit_monthly_reset")
        if isinstance(plan.get("credit_limit_monthly_reset"), str)
        else None,
    }
    return CheckOutcome("valid", None, details)


async def _check(payload: object) -> CheckOutcome:
    if not isinstance(payload, ApiKeySecret):
        raise TypeError("cmc_api_key payload must be an ApiKeySecret")
    return await check_cmc_key(payload)


DATA_SECRET_CHECKS: Final[dict[str, SecretCheck]] = {SECRET_NAME: _check}
