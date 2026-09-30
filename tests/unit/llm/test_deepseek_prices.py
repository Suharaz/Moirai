"""`config/deepseek.yaml` and `DeepSeekFile`: every DeepSeek model priced, peak vs off-peak rates."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from hdt.llm.deepseek import deepseek_catalog, load_deepseek_config
from hdt.settings.schemas import DEEPSEEK_MODELS

PRICES = load_deepseek_config()


def test_the_repository_table_prices_every_deepseek_model_the_config_accepts() -> None:
    assert set(PRICES.models) == set(DEEPSEEK_MODELS)
    assert [m.slug for m in deepseek_catalog(PRICES).models] == sorted(DEEPSEEK_MODELS)


@pytest.mark.parametrize(
    ("at", "peak"),
    [
        (datetime(2026, 9, 28, 1, 0, tzinfo=UTC), True),  # Monday 01:00, first window opens
        (datetime(2026, 9, 28, 3, 59, tzinfo=UTC), True),
        (datetime(2026, 9, 28, 4, 0, tzinfo=UTC), False),  # between the windows
        (datetime(2026, 9, 28, 9, 30, tzinfo=UTC), True),
        (datetime(2026, 9, 28, 10, 0, tzinfo=UTC), False),
        (datetime(2026, 10, 3, 2, 0, tzinfo=UTC), False),  # Saturday
        (datetime(2026, 10, 2, 7, 0, tzinfo=UTC), True),  # Friday
    ],
)
def test_peak_hours_follow_the_published_schedule(at: datetime, peak: bool) -> None:
    assert PRICES.is_peak(at) is peak


def test_a_generation_is_priced_per_token_class_at_the_rate_of_its_hour() -> None:
    flash = PRICES.models["deepseek-flash"]
    tokens = {"cache_hit": 1_000_000, "cache_miss": 2_000_000, "output": 500_000}
    peak = PRICES.cost_usd("deepseek-flash", **tokens, at=datetime(2026, 9, 28, 2, tzinfo=UTC))
    off_peak = PRICES.cost_usd("deepseek-flash", **tokens, at=datetime(2026, 9, 27, 2, tzinfo=UTC))
    assert peak == pytest.approx(
        flash.peak.input_cache_hit + 2 * flash.peak.input_cache_miss + 0.5 * flash.peak.output
    )
    assert off_peak == pytest.approx(
        flash.off_peak.input_cache_hit + 2 * flash.off_peak.input_cache_miss + 0.5 * flash.off_peak.output
    )
    assert off_peak < peak
    assert PRICES.cost_usd("deepseek-chat", **tokens, at=datetime(2026, 9, 28, tzinfo=UTC)) is None
