"""`config/news.yaml`: static defaults of the phase 07 news evidence pipeline (typed, validated)."""

from __future__ import annotations

from functools import cache
from pathlib import Path
from typing import Literal, Self

import yaml
from pydantic import Field, NonNegativeFloat, PositiveFloat, PositiveInt, model_validator

from hdt.core.config import CONFIG_DIR, StrictModel
from hdt.news.classes import EVENT_CLASSES

SOURCE_KEY_PATTERN = r"^[a-z0-9_]{2,40}$"
BINANCE_SOURCE_KEY = "binance_announcements"


class IngestConfig(StrictModel):
    poll_s: PositiveInt
    request_timeout_s: PositiveFloat
    max_response_bytes: PositiveInt
    max_entries_per_source: PositiveInt
    content_max_chars: PositiveInt
    near_duplicate_jaccard: float = Field(gt=0, le=1)
    near_duplicate_window_h: PositiveInt
    process_batch: PositiveInt
    process_lookback_h: PositiveInt
    llm_text_max_chars: int = Field(ge=500, le=8000)


class FeedConfig(StrictModel):
    key: str = Field(pattern=SOURCE_KEY_PATTERN)
    url: str = Field(pattern=r"^https?://\S+$")
    source_name: str = Field(min_length=1, max_length=80)
    enabled: bool


class ApiFields(StrictModel):
    id: str
    title: str
    url: str
    published_at: str
    summary: str | None = None
    content: str | None = None
    symbols: str | None = None


class NewsApiConfig(StrictModel):
    enabled: bool
    key: str = Field(pattern=SOURCE_KEY_PATTERN)
    source_name: str = Field(min_length=1, max_length=80)
    url: str | None = Field(default=None, pattern=r"^https://\S+$")
    auth_header: str = Field(min_length=1)
    query: dict[str, str]
    items_path: tuple[str, ...]
    fields: ApiFields

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.enabled and self.url is None:
            raise ValueError("news_api.url is required when the adapter is enabled")
        return self


class UnlockFields(StrictModel):
    coin_id: str | None = None
    symbol: str | None = None
    unlock_at: str
    amount_tokens: str | None = None
    pct_of_circulating: str | None = None
    category: str | None = None

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.coin_id is None and self.symbol is None:
            raise ValueError("unlock_calendar.fields needs coin_id or symbol")
        return self


class UnlockCalendarConfig(StrictModel):
    enabled: bool
    key: str = Field(pattern=SOURCE_KEY_PATTERN)
    url: str | None = Field(default=None, pattern=r"^https://\S+$")
    auth: Literal["none", "news_feed"]
    auth_header: str = Field(min_length=1)
    tier: Literal["T0", "T1", "T2", "T3"]
    items_path: tuple[str, ...]
    fields: UnlockFields

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.enabled and self.url is None:
            raise ValueError("unlock_calendar.url is required when the adapter is enabled")
        return self


class BinanceAnnouncementsConfig(StrictModel):
    enabled: bool
    url: str = Field(pattern=r"^https://www\.binance\.com/\S+$")
    article_url: str = Field(pattern=r"^https://www\.binance\.com/\S*\{code\}\S*$")
    page_size: int = Field(ge=1, le=50)
    catalogs: dict[int, Literal["listing", "delist"]] = Field(min_length=1)


class AttentionConfig(StrictModel):
    cadence_s: PositiveInt
    top_new: PositiveInt
    prior_outside: PositiveInt
    prior_lookback_h: PositiveInt
    roll_window_h: PositiveInt
    news_window_h: PositiveInt
    news_baseline_days: PositiveInt
    news_baseline_floor: PositiveFloat
    news_z_min: PositiveFloat

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.prior_outside <= self.top_new:
            raise ValueError("attention.prior_outside must exceed top_new")
        if self.news_baseline_days * 24 <= self.news_window_h:
            raise ValueError("attention baseline must be longer than the news window")
        return self


class ModesConfig(StrictModel):
    window_h: PositiveInt
    hollow_classes: tuple[str, ...] = Field(min_length=1)
    fade_hype_hardness_max: float = Field(ge=0, le=1)
    fade_hype_zf_min: PositiveFloat
    fade_hype_doi4_min: PositiveFloat
    squeeze_ratio: PositiveFloat
    fade_panic_zf_max: float = Field(lt=0)
    refute_window_min: PositiveInt
    incident_lookback_h: PositiveInt
    ride_hardness_min: float = Field(ge=0, le=1)
    ride_novelty_min: float = Field(ge=0, le=1)
    ride_max_age_h: PositiveFloat
    crowd_z: PositiveFloat
    p_fade_hype: float = Field(gt=0, lt=0.5)
    p_fade_panic: float = Field(gt=0.5, lt=1)
    p_ride_up: float = Field(gt=0.5, lt=1)
    onchain_drain_usd_min: PositiveFloat
    third_party_t0_domains: tuple[str, ...]
    project_domains: dict[int, tuple[str, ...]] = Field(default_factory=dict)
    """CMC id -> domains of the coin's own project: a denial from them is never third-party (BNB: Binance)."""

    @model_validator(mode="after")
    def _check(self) -> Self:
        unknown = set(self.hollow_classes) - set(EVENT_CLASSES)
        if unknown:
            raise ValueError(f"modes.hollow_classes has unknown classes {sorted(unknown)}")
        return self


class CandidatesConfig(StrictModel):
    max_per_day: PositiveInt
    min_spacing_h: PositiveInt


class VetoConfig(StrictModel):
    window_h: PositiveInt
    expires_cycles: int = Field(ge=2, le=10)
    soft_size_mult: float = Field(gt=0, lt=1)
    unlock_ahead_h: PositiveInt
    unlock_large_pct: NonNegativeFloat
    source_max_age_s: PositiveInt
    required_sources: tuple[str, ...]
    judge_pending_max: PositiveInt
    prefilter_keywords: tuple[str, ...] = Field(min_length=1)


class NewsFile(StrictModel):
    schema_version: Literal[1]
    rule_version: str = Field(min_length=1, max_length=40)
    ingest: IngestConfig
    feeds: tuple[FeedConfig, ...]
    news_api: NewsApiConfig
    unlock_calendar: UnlockCalendarConfig
    binance_announcements: BinanceAnnouncementsConfig
    attention: AttentionConfig
    modes: ModesConfig
    candidates: CandidatesConfig
    veto: VetoConfig

    @model_validator(mode="after")
    def _check(self) -> Self:
        keys = [feed.key for feed in self.feeds] + [self.news_api.key, self.unlock_calendar.key]
        keys.append(BINANCE_SOURCE_KEY)
        if len(keys) != len(set(keys)):
            raise ValueError("news source keys must be unique")
        unknown = set(self.veto.required_sources) - set(keys)
        if unknown:
            raise ValueError(f"veto.required_sources names unknown sources {sorted(unknown)}")
        return self


def load_news_config(config_dir: Path = CONFIG_DIR) -> NewsFile:
    with (config_dir / "news.yaml").open(encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    if not isinstance(data, dict):
        raise ValueError("config/news.yaml must be a mapping")
    return NewsFile.model_validate(data)


@cache
def news_config() -> NewsFile:
    """Process-wide cached `config/news.yaml`."""
    return load_news_config()
