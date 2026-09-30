"""`HttpFetcherClient`: the council's client of the sandboxed fetcher (`services/fetcher`, net_fetch).

The answer is parsed into `FetchedDocument` (the body's sha256 is re-checked here and again by
`fetch_source.check_document`). A refusal by the sandbox (SSRF guard, redirect / size / time limit) is
counted in `hdt_fetch_blocked_total{reason="sandbox_<reason>"}` (the council exports it; Prometheus alerts
on abnormal counts) and, like an unreachable fetcher or destination, surfaces as `ToolBackendError`, so
the News agent's tool call ends with status `error` instead of passing anything unchecked on.
"""

from __future__ import annotations

import base64
import binascii
from typing import Any, Final

import httpx
from pydantic import ValidationError

from hdt.core.ids import sha256_hex
from hdt.ops.metrics import FETCH_BLOCKED
from hdt.tools.base import ToolBackendError
from hdt.tools.ports import FetchedDocument

DEFAULT_BASE_URL: Final[str] = "http://fetcher:8080"
SANDBOX_REASONS: Final[frozenset[str]] = frozenset(
    {
        "bad_url",
        "scheme",
        "userinfo",
        "port",
        "hostname",
        "ip_literal",
        "private_address",
        "dns_failure",
        "too_many_redirects",
        "too_large",
        "encoding",
        "timeout",
        "proxy_denied",
    }
)
SANDBOX_LABELS: Final[tuple[str, ...]] = (*(f"sandbox_{r}" for r in sorted(SANDBOX_REASONS)), "sandbox_other")
for _label in SANDBOX_LABELS:
    FETCH_BLOCKED.labels(reason=_label)  # every series exists at 0 before the first refusal (rate alerts)


class HttpFetcherClient:
    """Implements `hdt.tools.ports.FetcherClient` over the fetcher's `POST /fetch`."""

    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        *,
        timeout_s: float = 20.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        # The sandbox enforces its own 10 s bound; the client waits a little longer for the answer. The
        # fetcher is reached directly (it is in NO_PROXY), never through the egress proxy.
        self._client = httpx.AsyncClient(
            base_url=base_url, timeout=timeout_s, transport=transport, trust_env=False
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def fetch(self, item_id: str, url: str) -> FetchedDocument:
        try:
            response = await self._client.post("/fetch", json={"item_id": item_id, "url": url})
        except httpx.HTTPError as exc:
            raise ToolBackendError(f"fetcher unreachable: {type(exc).__name__}") from exc
        answer = _json(response)
        if response.status_code == 422:
            reason = str(answer.get("reason", ""))
            label = f"sandbox_{reason}" if reason in SANDBOX_REASONS else "sandbox_other"
            FETCH_BLOCKED.labels(reason=label).inc()
            raise ToolBackendError(f"the fetcher sandbox refused the URL ({reason or 'unspecified'})")
        if response.status_code != 200:
            reason = str(answer.get("reason", response.status_code))
            raise ToolBackendError(f"the fetcher could not download the source ({reason})")
        return _document(answer)


def _json(response: httpx.Response) -> dict[str, Any]:
    try:
        value = response.json()
    except ValueError as exc:
        raise ToolBackendError("fetcher answered with invalid JSON") from exc
    if not isinstance(value, dict):
        raise ToolBackendError("fetcher answered with a non-object")
    return value


def _document(answer: dict[str, Any]) -> FetchedDocument:
    try:
        body = base64.b64decode(str(answer.get("body_b64", "")), validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ToolBackendError("fetcher body is not valid base64") from exc
    if sha256_hex(body) != answer.get("body_sha256"):
        FETCH_BLOCKED.labels(reason="sha256_mismatch").inc()
        raise ToolBackendError("fetched body does not match its sha256")
    try:
        return FetchedDocument(
            item_id=answer.get("item_id"),
            requested_url=answer.get("requested_url"),
            final_url=answer.get("final_url"),
            redirect_chain=tuple(answer.get("redirect_chain") or ()),
            http_status=answer.get("http_status"),
            content_type=str(answer.get("content_type") or ""),
            body=body,
            body_sha256=answer.get("body_sha256"),
        )
    except ValidationError as exc:
        raise ToolBackendError(f"fetcher answer failed validation ({exc.error_count()} errors)") from exc
