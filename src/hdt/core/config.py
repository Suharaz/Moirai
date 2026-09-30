"""Configuration: bootstrap settings from the environment and static defaults from `config/*.yaml`.

- `config/*.yaml` hold versioned static defaults only (no runtime state, no universe file). Runtime values
  are immutable config versions managed by phase 12 and pinned per council event.
- Bootstrap values (DSNs, Redis ACL users, secret file paths) come from environment variables, from
  `NAME_FILE` (docker secrets), or, in development only, from the repository `.env` file (parsed by
  python-dotenv). `BootstrapSettings` is the typed Pydantic Settings view of them.
- Every bootstrap secret that is read (DSN passwords, `*_PASSWORD_*`, secret files) is registered with the
  log redactor, so it can never appear in a log line even outside a key=value context.
- `HDT_CONFIG_DIR` overrides the static config directory (container images do not ship the repo layout).
- Nothing in this module ever logs or renders a secret value.
"""

from __future__ import annotations

import os
from datetime import date
from functools import cache
from pathlib import Path
from typing import Any, Literal, Self
from urllib.parse import unquote, urlsplit

import yaml
from dotenv import dotenv_values
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeFloat,
    PositiveFloat,
    PositiveInt,
    SecretStr,
    model_validator,
)
from pydantic.fields import FieldInfo
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict

from hdt.settings.ceilings import RiskLimits, check_risk_limits, check_size_multiplier
from hdt.vault.redact import is_secret_name, register_secret

REPO_ROOT = Path(__file__).resolve().parents[3]
CONFIG_DIR = Path(os.environ.get("HDT_CONFIG_DIR") or REPO_ROOT / "config")

AgentName = Literal["crowding", "technical", "micro", "fundamental", "news", "macro"]
AGENTS: tuple[AgentName, ...] = ("crowding", "technical", "micro", "fundamental", "news", "macro")
LlmRole = Literal[
    "crowding",
    "technical",
    "micro",
    "fundamental",
    "news",
    "macro",
    "news_extractor",
    "news_judge_a",
    "news_judge_b",
    "reflection",
]


# --------------------------------------------------------------------------- environment


@cache
def _dotenv(path: Path) -> dict[str, str]:
    if not path.is_file():
        return {}
    return {key: value for key, value in dotenv_values(path, interpolate=False).items() if value}


def _register_bootstrap_secret(name: str, value: str) -> None:
    if name.endswith("_FILE"):  # a path to a secret, not the secret itself
        return
    if "://" in value:
        password = urlsplit(value).password
        if password:
            register_secret(unquote(password))
            register_secret(password)
    elif is_secret_name(name):
        register_secret(value)


def read_env_value(name: str, *, dotenv: Path | None = REPO_ROOT / ".env") -> str | None:
    """Return NAME from the environment, NAME_FILE, or the dev `.env` file; None when absent/empty."""
    value: str | None
    direct = os.environ.get(name)
    if direct:
        value = direct
    else:
        file_path = os.environ.get(f"{name}_FILE") or (
            _dotenv(dotenv).get(f"{name}_FILE") if dotenv else None
        )
        if file_path:
            path = Path(file_path)
            value = (path.read_text(encoding="utf-8").strip() or None) if path.is_file() else None
            if value is not None:
                register_secret(value)
        else:
            value = _dotenv(dotenv).get(name) if dotenv is not None else None
    if value is not None:
        _register_bootstrap_secret(name, value)
    return value


def require_env_value(name: str) -> str:
    value = read_env_value(name)
    if value is None:
        raise RuntimeError(f"bootstrap variable {name} is not set (or {name}_FILE is missing)")
    return value


class _BootstrapSource(PydanticBaseSettingsSource):
    """Resolves field `x` from `HDT_X` with `read_env_value` semantics (env, `HDT_X_FILE`, dev `.env`)."""

    def get_field_value(self, field: FieldInfo, field_name: str) -> tuple[Any, str, bool]:
        env_name = f"HDT_{field_name.upper()}"
        return read_env_value(env_name), env_name, False

    def __call__(self) -> dict[str, Any]:
        values: dict[str, Any] = {}
        for field_name, field in self.settings_cls.model_fields.items():
            value, _, _ = self.get_field_value(field, field_name)
            if value is not None:
                values[field_name] = value
        return values


class BootstrapSettings(BaseSettings):
    """Typed bootstrap view. Secret values are `SecretStr` so they never render in reprs or logs."""

    model_config = SettingsConfigDict(frozen=True, extra="ignore")

    pg_dsn: SecretStr | None = None
    pg_dsn_migrator: SecretStr | None = None
    redis_url: SecretStr | None = None
    scope_private_key_file: Path | None = None
    order_signing_key_file: Path | None = None
    order_verify_keys_file: Path | None = None
    session_secret_file: Path | None = None
    tls_cert_file: Path | None = None
    tls_key_file: Path | None = None

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (init_settings, _BootstrapSource(settings_cls))


@cache
def bootstrap_settings() -> BootstrapSettings:
    return BootstrapSettings()


# --------------------------------------------------------------------------- base


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _load_yaml(name: str, config_dir: Path) -> dict[str, Any]:
    path = config_dir / name
    with path.open(encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    if not isinstance(data, dict):
        raise ValueError(f"{path} must contain a mapping")
    return data


# --------------------------------------------------------------------------- settings.yaml


class DataConfig(StrictModel):
    stale_s: PositiveInt = Field(description="Single staleness threshold for market data (seconds)")
    lake_root: str
    staging_root: str
    raw_depth_retention_days: PositiveInt


class CmcConfig(StrictModel):
    base_url: str
    keyless_base_url: str
    ws_url: str
    rate_limit_per_min: PositiveInt
    keyless_rate_limit_per_min: PositiveInt
    quota_monthly_design: PositiveInt
    alert_projection_frac: float = Field(gt=0, le=1)
    save_block_projection_frac: float = Field(gt=0, le=1)
    pilot_max_projection_frac: float = Field(gt=0, le=1)
    request_timeout_s: PositiveFloat


class StreamsConfig(StrictModel):
    block_ms: PositiveInt
    batch_size: PositiveInt
    reclaim_idle_ms: PositiveInt
    maxlen: dict[str, PositiveInt]


class LoggingConfig(StrictModel):
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"]


class SettingsFile(StrictModel):
    schema_version: Literal[1]
    data: DataConfig
    cmc: CmcConfig
    streams: StreamsConfig
    logging: LoggingConfig


# --------------------------------------------------------------------------- cmc_routes.yaml

ShedClass = Literal["context", "dex", "attention", "price", "derivatives", "system"]
Schedule = Literal["interval", "daily", "once", "on_event", "stream"]


class CmcRoute(StrictModel):
    id: int = Field(ge=1, le=30)
    name: str
    path: str
    transport: Literal["rest", "ws"]
    schedule: Schedule
    cadence_s: PositiveInt | None = None
    group: Literal["core", "backfill", "optional"]
    keyless: bool
    min_plan: Literal["basic", "startup"]
    shed_class: ShedClass
    consumers: tuple[str, ...]
    enabled: bool
    est_credits_month: NonNegativeFloat
    params: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.enabled and not self.consumers:
            raise ValueError(f"route #{self.id}: an enabled route must have a named consumer")
        if self.schedule == "interval" and self.cadence_s is None:
            raise ValueError(f"route #{self.id}: interval routes need cadence_s")
        if (self.transport == "ws") != (self.schedule == "stream"):
            raise ValueError(f"route #{self.id}: ws routes use schedule 'stream' and vice versa")
        return self


class CmcRoutesFile(StrictModel):
    schema_version: Literal[1]
    routes: tuple[CmcRoute, ...]

    @model_validator(mode="after")
    def _check(self) -> Self:
        ids = [r.id for r in self.routes]
        if sorted(ids) != list(range(1, 31)):
            raise ValueError("cmc_routes.yaml must define routes 1..30 exactly once")
        if sum(1 for r in self.routes if r.transport == "ws") != 1:
            raise ValueError("exactly one CMC WebSocket route is expected")
        return self

    def by_id(self, route_id: int) -> CmcRoute:
        for route in self.routes:
            if route.id == route_id:
                return route
        raise KeyError(route_id)


# --------------------------------------------------------------------------- binance.yaml


class BinanceHosts(StrictModel):
    rest: str
    ws_public: str
    ws_market: str
    ws_private: str


class SymbolOverride(StrictModel):
    binance_symbol: str
    cmc_symbol: str
    cmc_id: PositiveInt | None = None
    multiplier: PositiveInt


class UniverseRetry(StrictModel):
    """Same-day rebuilds of a failed universe build, only while today's universe is absent."""

    max_attempts: PositiveInt = Field(description="build attempts per UTC day, the first one included")
    backoff_s: PositiveFloat = Field(description="delay before the 2nd attempt; doubles after each failure")
    backoff_max_s: PositiveFloat
    jitter_frac: NonNegativeFloat = Field(le=1, description="up to this fraction of the delay is added")


class BinanceFile(StrictModel):
    schema_version: Literal[1]
    live: BinanceHosts
    demo: BinanceHosts
    recv_window_ms: PositiveInt
    request_timeout_s: PositiveFloat
    universe_top_n_by_oi: PositiveInt
    ltx_universe_size: PositiveInt
    watchlist_size: PositiveInt
    cmc_universe_top: PositiveInt
    banned_symbols: tuple[str, ...]
    symbol_overrides: tuple[SymbolOverride, ...]
    kline_intervals: tuple[str, ...]
    depth_levels: PositiveInt
    depth_snapshot_interval_s: PositiveFloat
    ws_reconnect_before_s: PositiveInt
    ws_batch_flush_s: PositiveFloat
    universe_retry: UniverseRetry


# --------------------------------------------------------------------------- indicators.yaml


class LiquidationParams(StrictModel):
    list_cycle_window_s: PositiveInt = Field(description="pages of one CMC list cycle arrive within this")
    list_max_pages: PositiveInt
    floor_history_min_samples: PositiveInt = Field(description="hourly samples needed for the q20 floor")


class FundingParams(StrictModel):
    lookback_h: PositiveInt = Field(description="window of dF_c and of the funding-drop condition")
    infer_window_h: PositiveInt = Field(description="nextFundingTime history used to infer the interval")
    min_abs_per_h: PositiveFloat = Field(description="|F_c(t-lookback)| below this: relative drop undefined")
    default_interval_h: PositiveFloat = Field(
        description="Binance interval of a symbol absent from a recorded fundingInfo list (not adjusted)"
    )


class ReconcileParams(StrictModel):
    funding_gap_per_h_max: PositiveFloat
    oi_gap_frac_max: PositiveFloat
    price_gap_frac_max: PositiveFloat
    liq_lb_tolerance_frac: NonNegativeFloat = Field(description="CMC below Binance lower bound by more")
    outlier_oi_jump_frac: PositiveFloat = Field(description="venue |dOI| above this within the window ...")
    outlier_window_min: PositiveInt
    outlier_price_move_max: PositiveFloat = Field(description="... while price moved less than this")


class TechnicalParams(StrictModel):
    timeframes: tuple[Literal["15m", "1h", "4h", "1d"], ...]
    cmc_history_days: PositiveInt = Field(description="CMC 1 h bars read (1h/4h/1d are built from them)")
    binance_history_days: PositiveInt = Field(description="Binance 15m bars read")
    min_bars: PositiveInt = Field(description="fewer closed bars: the timeframe's indicators are null")
    slope_bars: PositiveInt
    pivot_width: PositiveInt = Field(description="bars on each side of a swing pivot")
    divergence_pivots: PositiveInt
    sr_zone_atr: PositiveFloat = Field(description="close within this many ATR of a pivot = at an S/R zone")
    pattern_timeframes: tuple[Literal["1h", "4h"], ...]
    patterns: tuple[str, ...] = Field(description="TA-Lib CDL* function names")

    @model_validator(mode="after")
    def _check_patterns(self) -> Self:
        bad = [name for name in self.patterns if not name.startswith("CDL")]
        if bad:
            raise ValueError(f"technical.patterns must be TA-Lib CDL* names: {bad}")
        return self


class MicroParams(StrictModel):
    taker_windows: tuple[str, ...]
    book_max_age_s: PositiveInt


class RegimeParams(StrictModel):
    adx_trend_min: PositiveFloat
    vol_window_h: PositiveInt
    vol_reference_days: PositiveInt


class ReserveParams(StrictModel):
    stablecoins: tuple[str, ...]
    btc_symbol: str


class LevelsParams(StrictModel):
    levels_ver: str = Field(min_length=1)
    lookback_days: PositiveInt
    pivot_width: PositiveInt
    entry_atr_offsets: tuple[NonNegativeFloat, ...] = Field(min_length=1)
    min_separation_atr: PositiveFloat
    stop_buffer_atr: NonNegativeFloat
    stop_min_atr: PositiveFloat
    stop_max_atr: PositiveFloat
    stop_default_atr: PositiveFloat
    tp_max_atr: PositiveFloat
    tp_fallback_atr: PositiveFloat
    max_per_side: int = Field(ge=2, le=5)
    volume_profile_bins: PositiveInt
    volume_nodes: PositiveInt
    wall_mult: PositiveFloat

    @model_validator(mode="after")
    def _check(self) -> Self:
        if not self.stop_min_atr <= self.stop_default_atr <= self.stop_max_atr:
            raise ValueError("levels: stop_min_atr <= stop_default_atr <= stop_max_atr is required")
        return self


class IndicatorsFile(StrictModel):
    schema_version: Literal[1]
    feature_ver: str
    ema_periods: tuple[PositiveInt, ...]
    adx_period: PositiveInt
    rsi_period: PositiveInt
    macd: tuple[PositiveInt, PositiveInt, PositiveInt]
    stoch_rsi: tuple[PositiveInt, PositiveInt, PositiveInt, PositiveInt]
    atr_period: PositiveInt
    bollinger: tuple[PositiveInt, PositiveFloat]
    volume_ratio_window: PositiveInt
    candle_pattern_min_volume_ratio: PositiveFloat
    zscore_window: PositiveInt
    beta_window_days: PositiveInt
    beta_min_bars: PositiveInt
    migration_zm_window_days: PositiveInt
    migration_growth_window_h: PositiveInt
    fdz_min_coins: int = Field(ge=3)
    cvd_windows: tuple[str, ...]
    book_imbalance_levels: tuple[PositiveInt, ...]
    depth_band_pct: PositiveFloat
    liquidations: LiquidationParams
    funding: FundingParams
    reconcile: ReconcileParams
    technical: TechnicalParams
    micro: MicroParams
    regime: RegimeParams
    reserves: ReserveParams
    levels: LevelsParams


# --------------------------------------------------------------------------- council.yaml


class ManagerConfig(StrictModel):
    p_long: float = Field(gt=0.5, lt=1)
    p_short: float = Field(gt=0, lt=0.5)
    d_threshold: PositiveFloat
    size_consensus: float = Field(gt=0, le=1)
    size_fallback: float = Field(gt=0, le=1)


class WeightsConfig(StrictModel):
    floor: float = Field(gt=0, lt=1)
    ceiling: float = Field(gt=0, le=1)
    group_ceiling: float = Field(gt=0, le=1)
    new_version_cap: float = Field(gt=0, le=1)
    min_forecasts_per_version: PositiveInt
    a_initial: float = Field(ge=0, le=1)
    r_initial: float = Field(ge=0, le=1)


class ToolBudgetConfig(StrictModel):
    max_calls: PositiveInt
    max_seconds: PositiveFloat


class CouncilFile(StrictModel):
    schema_version: Literal[1]
    agents: tuple[AgentName, ...]
    horizon_h: PositiveInt
    stance_long: float = Field(gt=0.5, lt=1)
    stance_short: float = Field(gt=0, lt=0.5)
    quorum: PositiveInt
    supermajority: float = Field(gt=0.5, le=1)
    max_rounds: int = Field(ge=1, le=3)
    debate_enabled: bool
    logit_bound_round: PositiveFloat
    logit_bound_total: PositiveFloat
    llm_logit_margin: PositiveFloat
    p_clip: tuple[float, float]
    correlation_groups: dict[str, tuple[AgentName, ...]]
    manager: ManagerConfig
    weights: WeightsConfig
    claim_max_chars: int = Field(ge=13, le=280)  # sanitize_text needs >= 13; the contract caps at 280
    tool_budget: ToolBudgetConfig
    min_event_spacing_min: PositiveInt
    scoring_window_h: PositiveInt

    @model_validator(mode="after")
    def _check(self) -> Self:
        lo, hi = self.p_clip
        if not 0 < lo < 0.5 < hi < 1:
            raise ValueError("p_clip must satisfy 0 < lo < 0.5 < hi < 1")
        if self.quorum > len(self.agents):
            raise ValueError("quorum cannot exceed the number of agents")
        if sorted(self.agents) != sorted(AGENTS):
            raise ValueError("council.agents must list the six agents")
        for group, members in self.correlation_groups.items():
            if len(members) < 2:
                raise ValueError(f"correlation group {group} needs >= 2 agents")
        return self


# --------------------------------------------------------------------------- risk.yaml


class HedgeConfig(StrictModel):
    rebalance_threshold_frac: float = Field(gt=0, lt=1)
    rebalance_min_notional_mult: PositiveFloat
    stop_atr_mult: PositiveFloat


class FeesConfig(StrictModel):
    maker: NonNegativeFloat
    taker: NonNegativeFloat
    slippage_bp: NonNegativeFloat


class RiskFile(StrictModel):
    schema_version: Literal[1]
    risk_pct: float
    leverage_max: int
    max_positions: int
    max_positions_per_coin: int
    same_direction_risk_max: float
    daily_loss_kill: float
    max_margin_pct: float = Field(gt=0, le=1)
    max_oi_frac: float = Field(gt=0, le=1)
    max_volume_1h_frac: float = Field(gt=0, le=1)
    max_positions_per_narrative: PositiveInt
    liq_distance_mult: PositiveFloat
    mmr_assumed: float = Field(gt=0, lt=1)
    min_rr: PositiveFloat
    max_entry_distance_atr: PositiveFloat
    account_state_max_age_s: PositiveInt
    veto_scan_interval_s: PositiveInt
    post_only_wait_s: PositiveInt
    ioc_max_slippage: float = Field(gt=0, lt=0.05)
    tp1_fraction: float = Field(gt=0, lt=1)
    trail_atr_mult: PositiveFloat
    stop_place_max_attempts: PositiveInt
    reconcile_interval_s: PositiveInt
    account_state_interval_s: PositiveInt
    listen_key_keepalive_s: PositiveInt
    size_multiplier: dict[Literal["paper", "testnet", "live"], float]
    paper_starting_equity_usd: PositiveFloat = Field(
        default=10_000.0, description="paper wallet at namespace init (applied once, never re-applied)"
    )
    price_crosscheck_max_dev: float = Field(
        default=0.02, gt=0, lt=1, description="CMC vs Binance mark deviation that raises an alert (fraction)"
    )
    hedge: HedgeConfig
    fees: FeesConfig

    @model_validator(mode="after")
    def _check(self) -> Self:
        check_risk_limits(RiskLimits.of(self))
        missing = {"paper", "testnet", "live"} - set(self.size_multiplier)
        if missing:
            raise ValueError(f"size_multiplier needs every account: missing {sorted(missing)}")
        for account, value in self.size_multiplier.items():
            check_size_multiplier(account, value)
        return self


# --------------------------------------------------------------------------- news_sources.yaml


class NewsSourcesFile(StrictModel):
    schema_version: Literal[1]
    tiers: dict[Literal["T0", "T1", "T2"], tuple[str, ...]]
    official_listing_domains: tuple[str, ...]
    default_tier: Literal["T3"]

    @model_validator(mode="after")
    def _check(self) -> Self:
        seen: set[str] = set()
        for domains in self.tiers.values():
            for domain in domains:
                if domain in seen:
                    raise ValueError(f"domain {domain} listed in more than one tier")
                seen.add(domain)
        return self


# --------------------------------------------------------------------------- p_model/<agent>.<target>.yaml


class Standardization(StrictModel):
    mean: float
    std: PositiveFloat


class PModelFile(StrictModel):
    """Per-agent logistic coefficients: written by scripts/fit_p_model.py, reviewed, never overwritten."""

    schema_version: Literal[1]
    agent: Literal["crowding", "technical", "micro", "fundamental", "macro"]
    p_model_ver: str
    target_type: Literal["RAW_12H", "RESID_12H"]
    label_spec_version: str
    fit_window_start: date | None
    fit_window_end: date | None
    sample_count: int = Field(ge=0)
    oos_logloss: float | None
    intercept: float
    coefficients: dict[str, float]
    standardization: dict[str, Standardization]
    families: dict[str, tuple[str, ...]] = Field(
        default_factory=dict, description="feature family -> its coefficient names (shrinking unit)"
    )
    shrunk_families: tuple[str, ...]
    note: str

    @model_validator(mode="after")
    def _check(self) -> Self:
        missing = set(self.coefficients) - set(self.standardization)
        if missing:
            raise ValueError(f"coefficients without standardization: {sorted(missing)}")
        grouped = [name for names in self.families.values() for name in names]
        if len(grouped) != len(set(grouped)) or not set(grouped) <= set(self.coefficients):
            raise ValueError("families must partition coefficient names without repeats")
        unknown = set(self.shrunk_families) - set(self.families)
        if self.families and unknown:
            raise ValueError(f"shrunk_families not in families: {sorted(unknown)}")
        for family in self.shrunk_families:
            if any(self.coefficients[name] != 0.0 for name in self.families.get(family, ())):
                raise ValueError(f"shrunk family {family} must have zero coefficients")
        return self


# --------------------------------------------------------------------------- scanner.yaml


class LtxThresholds(StrictModel):
    skew4_min: float = Field(gt=0.5, lt=1, description="LONG: Skew4 >= this; SHORT: Skew4 <= 1 - this")
    spike_min: PositiveFloat
    decay_max: PositiveFloat
    fdz_min: float
    doi4_max: float = Field(lt=0, description="4 h open interest change must be at or below this")
    funding_drop_min: float = Field(gt=0, le=1, description="relative drop of hourly funding toward 0")


class MigrationThresholds(StrictModel):
    sp_min: float = Field(ge=0, lt=1, description="peripheral share of open interest")
    zm_min: PositiveFloat
    g_min: float = Field(description="peripheral minus core open interest growth")
    bg_abs_min: NonNegativeFloat = Field(description="|peripheral minus core basis|")


class LtxRule(StrictModel):
    strict: LtxThresholds
    loose: LtxThresholds

    @model_validator(mode="after")
    def _superset(self) -> Self:
        s, lo = self.strict, self.loose
        if not (
            lo.skew4_min <= s.skew4_min
            and lo.spike_min <= s.spike_min
            and lo.decay_max >= s.decay_max
            and lo.fdz_min <= s.fdz_min
            and lo.doi4_max >= s.doi4_max
            and lo.funding_drop_min <= s.funding_drop_min
        ):
            raise ValueError("ltx: every loose threshold must be at least as permissive as strict")
        return self


class MigrationRule(StrictModel):
    strict: MigrationThresholds
    loose: MigrationThresholds

    @model_validator(mode="after")
    def _superset(self) -> Self:
        s, lo = self.strict, self.loose
        if not (
            lo.sp_min <= s.sp_min
            and lo.zm_min <= s.zm_min
            and lo.g_min <= s.g_min
            and lo.bg_abs_min <= s.bg_abs_min
        ):
            raise ValueError("migration: every loose threshold must be at least as permissive as strict")
        return self


class SpikeParams(StrictModel):
    liq_floor_usd: PositiveFloat
    floor_quantile: float = Field(gt=0, lt=1)
    floor_window_days: PositiveInt
    min_liq_notional_usd: PositiveFloat
    min_liq_oi_frac: PositiveFloat


class ContagionParams(StrictModel):
    b_block: float = Field(gt=0, le=1)
    breadth_spike_min: PositiveFloat
    btc_flush_spike_min: PositiveFloat
    btc_flush_decay_max: PositiveFloat


class LabelParams(StrictModel):
    label_spec_version: str = Field(min_length=1)
    reference_horizons_h: tuple[PositiveInt, ...]
    mark_interval: Literal["1m"]
    max_mark_gap_s: PositiveInt


class ScannerFile(StrictModel):
    """Scanner rules, event budget and label spec. Changing any value requires a new `rule_version`."""

    schema_version: Literal[1]
    rule_version: str = Field(min_length=1)
    cadence_s: PositiveInt
    run_delay_s: int = Field(ge=0)
    superset_enabled: bool
    held_reeval_min: PositiveInt
    max_candidates_per_day: PositiveInt
    ltx: LtxRule
    migration: MigrationRule
    spike: SpikeParams
    contagion: ContagionParams
    labels: LabelParams

    @model_validator(mode="after")
    def _check(self) -> Self:
        if self.run_delay_s >= self.cadence_s:
            raise ValueError("run_delay_s must be shorter than cadence_s")
        if 86400 % self.cadence_s:
            raise ValueError("cadence_s must divide a day so the as_of grid is stable")
        return self


# --------------------------------------------------------------------------- aggregate loader


class StaticConfig(StrictModel):
    settings: SettingsFile
    cmc_routes: CmcRoutesFile
    binance: BinanceFile
    indicators: IndicatorsFile
    council: CouncilFile
    risk: RiskFile
    news_sources: NewsSourcesFile


def load_static_config(config_dir: Path = CONFIG_DIR) -> StaticConfig:
    return StaticConfig(
        settings=SettingsFile.model_validate(_load_yaml("settings.yaml", config_dir)),
        cmc_routes=CmcRoutesFile.model_validate(_load_yaml("cmc_routes.yaml", config_dir)),
        binance=BinanceFile.model_validate(_load_yaml("binance.yaml", config_dir)),
        indicators=IndicatorsFile.model_validate(_load_yaml("indicators.yaml", config_dir)),
        council=CouncilFile.model_validate(_load_yaml("council.yaml", config_dir)),
        risk=RiskFile.model_validate(_load_yaml("risk.yaml", config_dir)),
        news_sources=NewsSourcesFile.model_validate(_load_yaml("news_sources.yaml", config_dir)),
    )


class PModelFitFile(StrictModel):
    """`config/p_model_fit.yaml`: `scripts/fit_p_model.py` parameters; a change needs a new `fit_version`."""

    schema_version: Literal[1]
    fit_version: str = Field(min_length=1)
    l2_c: PositiveFloat
    folds: int = Field(ge=2)
    min_family_samples: PositiveInt


def load_p_model_fit(config_dir: Path = CONFIG_DIR) -> PModelFitFile:
    return PModelFitFile.model_validate(_load_yaml("p_model_fit.yaml", config_dir))


# --------------------------------------------------------------------------- scoring.yaml (phase 08)


class HedgeParams(StrictModel):
    """Sleeping-experts Hedge; `eta` and `alpha` are chosen by the synthetic oracle test
    (`hdt.scoring.oracle`)."""

    eta: PositiveFloat
    fixed_share_alpha: float = Field(ge=0, lt=1, description="fixed share applied once per UTC day")
    regime_shrink_n: PositiveFloat = Field(description="k in lambda = n / (n + k) for regime offsets")


class TrustParams(StrictModel):
    kappa_a: PositiveFloat
    kappa_r: PositiveFloat


class CalibrationParams(StrictModel):
    window_days: PositiveInt
    min_samples: PositiveInt
    bins: int = Field(ge=2, le=50)
    intercept_bound: PositiveFloat


class ClaimsAuditParams(StrictModel):
    window_days: PositiveInt
    max_wrong_rate: float = Field(gt=0, lt=1)
    min_claims: PositiveInt
    flagged_ceiling: float = Field(gt=0, le=1)


class ReflectionParams(StrictModel):
    enabled: bool
    ab_min_forecasts: int = Field(ge=50)
    bootstrap_resamples: int = Field(ge=200)
    bootstrap_seed: int = Field(ge=0)
    max_reason_chars: PositiveInt


class StackingParams(StrictModel):
    enabled: bool
    min_outcomes: int = Field(ge=300)
    folds: int = Field(ge=2)
    max_depth: int = Field(ge=1, le=3)
    n_estimators: PositiveInt
    learning_rate: PositiveFloat
    min_child_samples: PositiveInt
    seed: int = Field(ge=0)


class BarrierParams(StrictModel):
    tp_atr: PositiveFloat
    sl_atr: PositiveFloat


class ResolverParams(StrictModel):
    label_delay_s: int = Field(ge=0, description="wait after as_of + horizon before reading the lake")
    missing_after_h: PositiveFloat = Field(description="retry a missing mark this long, then record missing")
    batch: PositiveInt
    kline_fetch_lag_s: int = Field(
        default=3600,
        ge=0,
        description="mark_price_klines are fetched this long after they close (B2 runs hourly): a label "
        "reads kline records fetched up to t + kline_fetch_lag_s + max_mark_gap_s",
    )
    missing_retry_h: NonNegativeFloat = Field(
        default=72.0,
        description="a `missing` or `error` label is re-resolved (after a lake backfill) until this long "
        "after its horizon, then it stays final",
    )
    missing_retry_interval_s: PositiveInt = Field(
        default=3600, description="minimum wait between two re-resolution attempts of one label"
    )

    @model_validator(mode="after")
    def _kline_lag_inside_retry(self) -> Self:
        if self.missing_after_h * 3600 <= self.kline_fetch_lag_s:
            raise ValueError("missing_after_h must exceed kline_fetch_lag_s (the kline fallback lands late)")
        return self


class ScoringFile(StrictModel):
    """`config/scoring.yaml`: phase 08 scorer parameters (the weight floor and ceilings stay in
    council.yaml)."""

    schema_version: Literal[1]
    interval_s: PositiveInt
    loss_clip: tuple[float, float]
    hedge: HedgeParams
    trust: TrustParams
    calibration: CalibrationParams
    claims_audit: ClaimsAuditParams
    reflection: ReflectionParams
    stacking: StackingParams
    barrier: BarrierParams
    resolver: ResolverParams

    @model_validator(mode="after")
    def _check(self) -> Self:
        lo, hi = self.loss_clip
        if not 0 < lo < 0.5 < hi < 1:
            raise ValueError("loss_clip must satisfy 0 < lo < 0.5 < hi < 1")
        return self


def load_scoring_config(config_dir: Path = CONFIG_DIR) -> ScoringFile:
    return ScoringFile.model_validate(_load_yaml("scoring.yaml", config_dir))


@cache
def scoring_config() -> ScoringFile:
    """Process-wide cached scorer parameters from the repository `config/scoring.yaml`."""
    return load_scoring_config()


def p_model_path(agent: str, target_type: str) -> str:
    """`p_model/<agent>.<target_type>.yaml` under the config dir: one model per agent and target type."""
    return f"p_model/{agent}.{target_type.lower()}.yaml"


def load_p_model(agent: str, target_type: str, config_dir: Path = CONFIG_DIR) -> PModelFile:
    path = p_model_path(agent, target_type)
    spec = PModelFile.model_validate(_load_yaml(path, config_dir))
    if spec.agent != agent or spec.target_type != target_type:
        raise ValueError(f"config/{path} declares {spec.agent} / {spec.target_type}")
    return spec


def load_scanner_config(config_dir: Path = CONFIG_DIR) -> ScannerFile:
    return ScannerFile.model_validate(_load_yaml("scanner.yaml", config_dir))


@cache
def scanner_config() -> ScannerFile:
    """Process-wide cached scanner rules from the repository `config/scanner.yaml`."""
    return load_scanner_config()


@cache
def static_config() -> StaticConfig:
    """Process-wide cached static defaults from the repository `config/` directory."""
    return load_static_config()
