"""Shared fakes of the agent tests: an in-memory `LlmStore`, role model configs and DeepSeek prices."""

from __future__ import annotations

from typing import Any

from hdt.agents.llm_router import CachedReply, CacheKey, LlmCallRecord
from hdt.llm.deepseek import DeepSeekFile
from hdt.settings.schemas import RoleModelConfig

MODEL = "anthropic/claude-sonnet-4.5"
DEEPSEEK_MODEL = "deepseek-flash"


def config(**overrides: Any) -> RoleModelConfig:
    base: dict[str, Any] = {
        "model": MODEL,
        "temperature": 0.2,
        "max_tokens": 800,
        "timeout_s": 30,
        "provider": {"only": ["Anthropic"]},
    }
    return RoleModelConfig.model_validate({**base, **overrides})


def deepseek_config(**overrides: Any) -> RoleModelConfig:
    base: dict[str, Any] = {
        "gateway": "deepseek",
        "model": DEEPSEEK_MODEL,
        "temperature": 0.2,
        "max_tokens": 800,
        "timeout_s": 30,
    }
    return RoleModelConfig.model_validate({**base, **overrides})


def flat_deepseek_prices(*, hit: float, miss: float, output: float) -> DeepSeekFile:
    """A price table with the same rates at peak and off-peak (a cost independent of the clock)."""
    rates = {"input_cache_hit": hit, "input_cache_miss": miss, "output": output}
    model = {"name": "test", "context_length": 1000, "peak": rates, "off_peak": rates}
    return DeepSeekFile.model_validate(
        {
            "schema_version": 1,
            "prices_source": "https://api-docs.deepseek.com/quick_start/pricing",
            "prices_read_on": "2026-09-28",
            "peak_weekdays_utc": [0, 1, 2, 3, 4],
            "peak_hours_utc": [{"start": 1, "end": 4}],
            "models": {"deepseek-flash": model, "deepseek-v4-pro": model},
        }
    )


class MemoryLlmStore:
    def __init__(self) -> None:
        self.calls: list[LlmCallRecord] = []
        self.cache: dict[tuple[str, str, str, str], CachedReply] = {}

    def cached(self, key: CacheKey) -> CachedReply | None:
        return self.cache.get((key.prompt_hash, key.model_slug, key.provider, key.params_hash))

    def record(self, call: LlmCallRecord, cache: tuple[CacheKey, CachedReply] | None) -> None:
        self.calls.append(call)
        if cache is not None:
            key, reply = cache
            self.cache.setdefault((key.prompt_hash, key.model_slug, key.provider, key.params_hash), reply)
