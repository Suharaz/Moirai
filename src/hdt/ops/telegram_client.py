"""Minimal Telegram Bot API client over httpx (only the methods the bot uses).

The token sits in the URL path, so neither the URL nor httpx exceptions are ever put in an error message;
`hdt.vault.redact` also masks bot tokens in any log line. Requests go through the egress proxy
(`HTTPS_PROXY`, honored by httpx) like every other outbound call.
"""

from __future__ import annotations

from typing import Any, Final

import httpx

API_BASE: Final[str] = "https://api.telegram.org"
MAX_MESSAGE_CHARS: Final[int] = 4096


class TelegramApiError(RuntimeError):
    """A Bot API call failed; `description` comes from Telegram, never includes the token.

    `retry_after` (seconds) is set when Telegram answers 429 Too Many Requests: no call may be made
    before it has elapsed (flood control). `transport` is True when no Bot API answer came back at all
    (connection error, timeout, proxy error page): every other call would fail the same way for now.
    """

    def __init__(
        self,
        method: str,
        description: str,
        *,
        error_code: int | None = None,
        retry_after: float | None = None,
        transport: bool = False,
    ) -> None:
        super().__init__(f"telegram {method} failed: {description}")
        self.method = method
        self.description = description
        self.error_code = error_code
        self.retry_after = retry_after
        self.transport = transport


def _retry_after(body: dict[str, Any], response: httpx.Response) -> float | None:
    """Telegram puts the flood-control delay in `parameters.retry_after`; the header is a fallback."""
    parameters = body.get("parameters")
    value: Any = parameters.get("retry_after") if isinstance(parameters, dict) else None
    if value is None and response.status_code == 429:
        value = response.headers.get("Retry-After")
    try:
        seconds = float(value) if value is not None else None
    except (TypeError, ValueError):
        return None
    return seconds if seconds is not None and 0 < seconds < 86_400 else None


class TelegramClient:
    def __init__(self, token: str, *, http: httpx.AsyncClient, base_url: str = API_BASE) -> None:
        self._token = token
        self._http = http
        self._base = base_url.rstrip("/")

    async def call(
        self, method: str, payload: dict[str, Any] | None = None, *, timeout_s: float = 15.0
    ) -> Any:
        try:
            response = await self._http.post(
                f"{self._base}/bot{self._token}/{method}", json=payload or {}, timeout=timeout_s
            )
        except httpx.HTTPError as exc:
            raise TelegramApiError(
                method, f"transport error ({type(exc).__name__})", transport=True
            ) from None
        try:
            body = response.json()
        except ValueError:
            raise TelegramApiError(
                method, f"HTTP {response.status_code} with a non-JSON body", transport=True
            ) from None
        if not isinstance(body, dict) or body.get("ok") is not True:
            description = str(body.get("description", "")) if isinstance(body, dict) else ""
            code = body.get("error_code") if isinstance(body, dict) else None
            raise TelegramApiError(
                method,
                description or f"HTTP {response.status_code}",
                error_code=code if isinstance(code, int) else None,
                retry_after=_retry_after(body, response) if isinstance(body, dict) else None,
            )
        return body.get("result")

    async def get_me(self) -> dict[str, Any]:
        result = await self.call("getMe")
        if not isinstance(result, dict):
            raise TelegramApiError("getMe", "unexpected result")
        return result

    async def get_webhook_info(self) -> dict[str, Any]:
        result = await self.call("getWebhookInfo")
        if not isinstance(result, dict):
            raise TelegramApiError("getWebhookInfo", "unexpected result")
        return result

    async def get_updates(self, offset: int | None, *, timeout_s: int = 25) -> list[dict[str, Any]]:
        payload: dict[str, Any] = {"timeout": timeout_s, "allowed_updates": ["message"]}
        if offset is not None:
            payload["offset"] = offset
        result = await self.call("getUpdates", payload, timeout_s=timeout_s + 10)
        if not isinstance(result, list):
            raise TelegramApiError("getUpdates", "unexpected result")
        return [u for u in result if isinstance(u, dict)]

    async def send_message(self, chat_id: int, text: str) -> None:
        """Plain text only (no parse_mode): nothing in a message can be interpreted as markup."""
        if len(text) > MAX_MESSAGE_CHARS:
            text = text[: MAX_MESSAGE_CHARS - 3] + "..."
        await self.call("sendMessage", {"chat_id": chat_id, "text": text, "disable_web_page_preview": True})
