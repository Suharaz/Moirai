"""OpenRouter model catalog for the console model picker (phase 12).

`GET https://openrouter.ai/api/v1/models` is public (no key). Only models whose `supported_parameters`
include `structured_outputs` are kept, and slugs the council refuses (routers such as `openrouter/auto`,
floating `~` aliases, `:free` variants) are dropped. The list is cached for 24 h per process; when a
refresh fails the previous list keeps being served (its `fetched_at` shows its age).
Prices are USD per token, as published by OpenRouter.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any, Final

import httpx
from pydantic import BaseModel, ConfigDict

from hdt.core.clock import utcnow
from hdt.settings.schemas import slug_developer, slug_problem

log = logging.getLogger(__name__)

OPENROUTER_API_BASE: Final[str] = "https://openrouter.ai/api/v1"
OPENROUTER_MODELS_URL: Final[str] = f"{OPENROUTER_API_BASE}/models"
CATALOG_TTL: Final[timedelta] = timedelta(hours=24)
REQUIRED_PARAMETER: Final[str] = "structured_outputs"
MAX_PAGES: Final[int] = 50


class CatalogUnavailableError(RuntimeError):
    """The catalog could not be fetched and nothing is cached."""


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class CatalogPricing(_Frozen):
    prompt: float
    completion: float


class CatalogModel(_Frozen):
    slug: str
    name: str
    developer: str
    context_length: int | None
    pricing: CatalogPricing
    supported_parameters: tuple[str, ...]


class Catalog(_Frozen):
    fetched_at: datetime
    models: tuple[CatalogModel, ...]

    def get(self, slug: str) -> CatalogModel | None:
        return next((m for m in self.models if m.slug == slug), None)


def _price(value: Any) -> float | None:
    try:
        price = float(value)
    except (TypeError, ValueError):
        return None
    return price if price >= 0 else None


def parse_models(entries: list[dict[str, Any]]) -> list[CatalogModel]:
    """Keep structured-output models with a pinned slug and a published (non-negative) price."""
    models: list[CatalogModel] = []
    for entry in entries:
        slug = entry.get("id")
        params = entry.get("supported_parameters") or []
        pricing = entry.get("pricing") or {}
        if not isinstance(slug, str) or REQUIRED_PARAMETER not in params or slug_problem(slug) is not None:
            continue
        prompt, completion = _price(pricing.get("prompt")), _price(pricing.get("completion"))
        if prompt is None or completion is None:
            continue
        context = entry.get("context_length")
        models.append(
            CatalogModel(
                slug=slug,
                name=str(entry.get("name") or slug),
                developer=slug_developer(slug),
                context_length=int(context) if isinstance(context, int | float) else None,
                pricing=CatalogPricing(prompt=prompt, completion=completion),
                supported_parameters=tuple(str(p) for p in params),
            )
        )
    return sorted(models, key=lambda m: m.slug)


class OpenRouterCatalog:
    def __init__(
        self,
        *,
        url: str = OPENROUTER_MODELS_URL,
        ttl: timedelta = CATALOG_TTL,
        timeout_s: float = 20.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._url, self._ttl, self._timeout, self._transport = url, ttl, timeout_s, transport
        self._cached: Catalog | None = None
        self._lock = asyncio.Lock()

    @property
    def cached(self) -> Catalog | None:
        return self._cached

    async def get(self, *, refresh: bool = False) -> Catalog:
        async with self._lock:
            cached = self._cached
            if cached is not None and not refresh and utcnow() - cached.fetched_at < self._ttl:
                return cached
            try:
                fresh = await self._fetch()
            except (httpx.HTTPError, ValueError) as exc:
                if cached is None:
                    raise CatalogUnavailableError(
                        f"OpenRouter catalog fetch failed: {type(exc).__name__}"
                    ) from exc
                log.warning(
                    "OpenRouter catalog refresh failed, serving the cached list", extra={"error": str(exc)}
                )
                return cached
            self._cached = fresh
            log.info("OpenRouter catalog refreshed", extra={"models": len(fresh.models)})
            return fresh

    async def _fetch(self) -> Catalog:
        entries: list[dict[str, Any]] = []
        url: str | None = self._url
        pages = 0
        async with httpx.AsyncClient(timeout=self._timeout, transport=self._transport) as client:
            while url is not None:
                if pages == MAX_PAGES:
                    raise ValueError(f"/models pagination exceeded {MAX_PAGES} pages")
                pages += 1
                response = await client.get(url, headers={"Accept": "application/json"})
                response.raise_for_status()
                body = response.json()
                data = body.get("data") if isinstance(body, dict) else None
                if not isinstance(data, list):
                    raise ValueError("unexpected /models response: missing 'data' list")
                entries.extend(item for item in data if isinstance(item, dict))
                next_link = (body.get("links") or {}).get("next")
                url = str(httpx.URL(url).join(next_link)) if next_link else None
        return Catalog(fetched_at=utcnow(), models=tuple(parse_models(entries)))
