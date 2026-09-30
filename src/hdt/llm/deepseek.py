"""DeepSeek direct API: the temporary LLM gateway next to OpenRouter (owner decision 2026-09-28).

- `DEEPSEEK_API_BASE` is DeepSeek's OpenAI-compatible base URL (`/chat/completions`, `/user/balance`).
- `config/deepseek.yaml` holds the published USD prices per 1M tokens (cache-hit input, cache-miss input,
  output; peak and off-peak) with their source URL and read date. DeepSeek replies carry token counts
  (`prompt_cache_hit_tokens`, `prompt_cache_miss_tokens`, `completion_tokens`) but no price, so
  `DeepSeekFile.cost_usd` prices each generation for `llm_calls.cost_usd` and the AI cost views.
- `deepseek_catalog` is the console model picker under the deepseek gateway: the configured models with
  their prices, built without any request. The API's `GET /models` lists model ids only (no price, no
  context length), so it would add a keyed network call without adding anything the picker needs.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, time
from functools import cache
from pathlib import Path
from typing import Final, Literal, Self

import yaml
from pydantic import Field, NonNegativeFloat, PositiveInt, model_validator

from hdt.core.config import CONFIG_DIR, StrictModel
from hdt.llm.openrouter_catalog import Catalog, CatalogModel, CatalogPricing
from hdt.settings.schemas import DEEPSEEK_DEVELOPER, DEEPSEEK_MODELS

DEEPSEEK_API_BASE: Final[str] = "https://api.deepseek.com"
CONFIG_FILE: Final[str] = "deepseek.yaml"
PER_MILLION: Final[float] = 1_000_000.0
CATALOG_PARAMETERS: Final[tuple[str, ...]] = ("response_format", "tools", "thinking", "reasoning_effort")
"""What the router uses on DeepSeek: JSON Output (`json_object`), tool calls and the thinking toggle."""
SERVED_NAMES: Final[dict[str, frozenset[str]]] = {
    "deepseek-flash": frozenset({"deepseek-flash", "deepseek-v4-flash"}),
    "deepseek-v4-pro": frozenset({"deepseek-v4-pro"}),
}
"""The reply `model` names accepted for each requested model: the requested name and its documented
aliases (api-docs.deepseek.com/quick_start/pricing, read 2026-09-28: the legacy `deepseek-v4-flash` is
served by DeepSeek-V4.1-Flash). A Flash request never accepts a Pro reply and the reverse. The docs do not
state the `model` value a reply carries; `served_names` fails closed to the requested name alone."""


def served_names(requested: str) -> frozenset[str]:
    """The reply model names acceptable for a request of `requested`."""
    return SERVED_NAMES.get(requested, frozenset({requested}))


class TokenPrices(StrictModel):
    """USD per 1M tokens."""

    input_cache_hit: NonNegativeFloat
    input_cache_miss: NonNegativeFloat
    output: NonNegativeFloat


class DeepSeekModel(StrictModel):
    name: str = Field(min_length=1)
    context_length: PositiveInt
    peak: TokenPrices
    off_peak: TokenPrices


class PeakWindow(StrictModel):
    """Hours `[start, end)` in UTC."""

    start: int = Field(ge=0, le=23)
    end: int = Field(ge=1, le=24)

    @model_validator(mode="after")
    def _ordered(self) -> Self:
        if self.start >= self.end:
            raise ValueError("a peak window needs start < end")
        return self


class DeepSeekFile(StrictModel):
    """`config/deepseek.yaml`: the published DeepSeek prices and peak schedule."""

    schema_version: Literal[1]
    prices_source: str = Field(pattern=r"^https://")
    prices_read_on: date
    peak_weekdays_utc: tuple[int, ...] = Field(description="Monday = 0")
    peak_hours_utc: tuple[PeakWindow, ...]
    models: dict[str, DeepSeekModel]

    @model_validator(mode="after")
    def _every_model_priced(self) -> Self:
        if set(self.models) != set(DEEPSEEK_MODELS):
            raise ValueError(f"models must price exactly {list(DEEPSEEK_MODELS)}, got {sorted(self.models)}")
        if any(not 0 <= day <= 6 for day in self.peak_weekdays_utc):
            raise ValueError("peak_weekdays_utc takes weekday numbers 0 (Monday) to 6 (Sunday)")
        return self

    def is_peak(self, at: datetime) -> bool:
        at = at.astimezone(UTC)
        return at.weekday() in self.peak_weekdays_utc and any(
            window.start <= at.hour < window.end for window in self.peak_hours_utc
        )

    def cost_usd(
        self, model: str, *, cache_hit: int, cache_miss: int, output: int, at: datetime
    ) -> float | None:
        """The price of one generation of `model` requested at `at`, None for a model without a price."""
        entry = self.models.get(model)
        if entry is None:
            return None
        prices = entry.peak if self.is_peak(at) else entry.off_peak
        return (
            cache_hit * prices.input_cache_hit + cache_miss * prices.input_cache_miss + output * prices.output
        ) / PER_MILLION


def load_deepseek_config(config_dir: Path = CONFIG_DIR) -> DeepSeekFile:
    path = config_dir / CONFIG_FILE
    with path.open(encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    if not isinstance(data, dict):
        raise ValueError(f"{path} must contain a mapping")
    return DeepSeekFile.model_validate(data)


@cache
def deepseek_config() -> DeepSeekFile:
    """Process-wide cached DeepSeek prices from the repository `config/deepseek.yaml`."""
    return load_deepseek_config()


def deepseek_catalog(prices: DeepSeekFile | None = None) -> Catalog:
    """The DeepSeek models for the console picker. Catalog prices are USD per token (as the OpenRouter
    catalog publishes them): the peak cache-miss input and peak output rates, the upper bounds."""
    prices = prices or deepseek_config()
    return Catalog(
        fetched_at=datetime.combine(prices.prices_read_on, time(), tzinfo=UTC),
        models=tuple(
            CatalogModel(
                slug=name,
                name=entry.name,
                developer=DEEPSEEK_DEVELOPER,
                context_length=entry.context_length,
                pricing=CatalogPricing(
                    prompt=entry.peak.input_cache_miss / PER_MILLION,
                    completion=entry.peak.output / PER_MILLION,
                ),
                supported_parameters=CATALOG_PARAMETERS,
            )
            for name, entry in sorted(prices.models.items())
        ),
    )
