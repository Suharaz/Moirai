"""Runtime configuration sections (phase 12): payload schemas, seeds and materialization.

Every console change becomes an immutable version of one section. Field definitions are reused from
`hdt.core.config` (the static YAML schema), so a constraint is never written twice: section models only
select which static fields are editable at runtime, and `effective_*` merges a section back into the
static model so every static validator runs again. Hard ceilings come from `hdt.settings.ceilings`.

Units are those of `config/*.yaml`: fractions for percentages (risk_pct 0.005 = 0.5 %, daily_loss_kill
-0.02 = -2 %), seconds for `*_s`, OpenRouter `max_price` in USD per million tokens.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Mapping
from enum import StrEnum
from typing import Any, Final, Literal, Self, get_args

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeFloat,
    NonNegativeInt,
    PositiveFloat,
    PositiveInt,
    ValidationInfo,
    create_model,
    field_validator,
    model_validator,
)

from hdt.contracts.common import Account, UtcDatetime
from hdt.core.config import BinanceFile, CmcRoutesFile, CouncilFile, LlmRole, RiskFile, StaticConfig
from hdt.settings.ceilings import RiskLimits, check_risk_limits, check_size_multiplier

SECTION_SCHEMA_VERSION: Final[int] = 1
log = logging.getLogger(__name__)


class Section(StrEnum):
    MODELS = "models"
    CMC = "cmc"
    BINANCE = "binance"
    RISK = "risk"
    COUNCIL = "council"
    MODE = "mode"
    GOLIVE = "golive"


class SectionModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _pick(source: type[BaseModel], names: tuple[str, ...]) -> dict[str, Any]:
    """Reuse the exact field definitions (type + constraints) of a static config model."""
    return {name: (source.model_fields[name].annotation, source.model_fields[name]) for name in names}


# --------------------------------------------------------------------------- models (LLM gateways)

Gateway = Literal["openrouter", "deepseek"]
"""Where a role's requests go: OpenRouter (the design default) or the DeepSeek API directly (a temporary
owner decision of 2026-09-28, see docs/system-architecture.md "LLM gateway")."""
DEEPSEEK_MODELS: Final[tuple[str, ...]] = ("deepseek-flash", "deepseek-v4-pro")
"""Model names the DeepSeek API accepts: `deepseek-flash` is DeepSeek-V4.1-Flash. Priced in
`config/deepseek.yaml` (`hdt.llm.deepseek`)."""
DEEPSEEK_DEVELOPER: Final[str] = "deepseek"
Thinking = Literal["disabled", "low", "high", "max"]
"""DeepSeek thinking mode: `disabled`, or enabled with that `reasoning_effort`."""

SLUG_PATTERN: Final[str] = r"^[a-z0-9][a-z0-9._-]*/[a-z0-9][a-z0-9._-]*$"
"""No `:`: OpenRouter variants (`:free`, `:online`, `:nitro`, ...) change routing and are never pinned."""
_SLUG_RE = re.compile(SLUG_PATTERN)


def slug_problem(slug: str) -> str | None:
    """Why `slug` is not an acceptable pinned OpenRouter model, or None when it is acceptable."""
    if not _SLUG_RE.fullmatch(slug):
        return (
            "must be a pinned OpenRouter slug 'developer/model' (floating '~' aliases and ':' variants"
            " are rejected)"
        )
    if slug.startswith("openrouter/"):
        return "OpenRouter routers (openrouter/auto and similar) are not pinned models"
    return None


def model_problem(gateway: Gateway, model: str) -> str | None:
    """Why `model` is not an acceptable pinned model on `gateway`, or None when it is acceptable."""
    if gateway == "deepseek":
        if model not in DEEPSEEK_MODELS:
            return f"must be a DeepSeek model name, one of {list(DEEPSEEK_MODELS)}"
        return None
    return slug_problem(model)


def slug_developer(slug: str) -> str:
    return slug.split("/", 1)[0]


class ProviderRouting(SectionModel):
    order: tuple[str, ...] = ()
    only: tuple[str, ...] = ()


class MaxPrice(SectionModel):
    """Upper price bounds in USD per million tokens (OpenRouter `provider.max_price`)."""

    prompt: NonNegativeFloat
    completion: NonNegativeFloat


class RoleModelConfig(SectionModel):
    """One role's pinned model and request options.

    `gateway` `openrouter` (default): `model` is a pinned OpenRouter slug, with optional `fallback_models`
    and the OpenRouter provider preferences. `gateway` `deepseek`: `model` is a DeepSeek model name
    (`DEEPSEEK_MODELS`); the OpenRouter-only options (`fallback_models`, `provider`, `zdr`, `max_price`)
    must stay unset, since DeepSeek has no provider routing or fallback list.

    `thinking` (DeepSeek only) defaults to `disabled`: in thinking mode DeepSeek ignores `temperature`,
    counts the chain of thought against `max_tokens` (sized for the answer alone, so a JSON reply can be
    cut off) and adds latency against `timeout_s`. A role may enable it (`low`, `high`, `max` effort) after
    raising `max_tokens` and `timeout_s`; the router then passes the chain of thought back between tool
    steps as DeepSeek requires. OpenRouter roles keep `disabled`.
    """

    gateway: Gateway = "openrouter"
    model: str = Field(
        description="OpenRouter: a pinned 'developer/model' slug; DeepSeek: one of "
        + ", ".join(DEEPSEEK_MODELS)
    )
    fallback_models: tuple[str, ...] = ()
    provider: ProviderRouting = ProviderRouting()
    allow_fallbacks: Literal[False] = False
    require_parameters: Literal[True] = True
    data_collection: Literal["deny"] = "deny"
    zdr: bool = False
    thinking: Thinking = "disabled"
    temperature: float = Field(ge=0, le=2)
    max_tokens: PositiveInt
    max_price: MaxPrice | None = None
    timeout_s: PositiveFloat

    @field_validator("model")
    @classmethod
    def _model_slug(cls, value: str, info: ValidationInfo) -> str:
        gateway = info.data.get("gateway")
        if gateway is None:  # the gateway itself is invalid and already reported
            return value
        problem = model_problem(gateway, value)
        if problem:
            raise ValueError(problem)
        return value

    @field_validator("fallback_models")
    @classmethod
    def _fallback_slugs(cls, value: tuple[str, ...], info: ValidationInfo) -> tuple[str, ...]:
        if value and info.data.get("gateway") == "deepseek":
            raise ValueError("fallback_models are OpenRouter-only; the deepseek gateway serves one model")
        for slug in value:
            problem = slug_problem(slug)
            if problem:
                raise ValueError(f"{slug}: {problem}")
        if len(set(value)) != len(value):
            raise ValueError("fallback_models must not repeat a slug")
        return value

    @model_validator(mode="after")
    def _gateway_options(self) -> Self:
        if self.model in self.fallback_models:
            raise ValueError("fallback_models must not contain the primary model")
        if self.gateway == "deepseek":
            used = [
                name
                for name, set_ in (
                    ("provider", self.provider != ProviderRouting()),
                    ("zdr", self.zdr),
                    ("max_price", self.max_price is not None),
                )
                if set_
            ]
            if used:
                raise ValueError(f"OpenRouter-only options are not available on the deepseek gateway: {used}")
        elif self.thinking != "disabled":
            raise ValueError(
                "thinking applies to the deepseek gateway only; OpenRouter roles keep 'disabled'"
            )
        return self

    @property
    def developer(self) -> str:
        return DEEPSEEK_DEVELOPER if self.gateway == "deepseek" else slug_developer(self.model)

    def developers(self) -> frozenset[str]:
        """The developers of every model that may serve this role (the primary and each fallback)."""
        if self.gateway == "deepseek":
            return frozenset({DEEPSEEK_DEVELOPER})
        return frozenset(slug_developer(slug) for slug in self.slugs())

    def provider_preferences(self) -> dict[str, Any]:
        """The OpenRouter `provider` request object for this role (never sent on the deepseek gateway)."""
        prefs: dict[str, Any] = {
            "allow_fallbacks": self.allow_fallbacks,
            "require_parameters": self.require_parameters,
            "data_collection": self.data_collection,
            "zdr": self.zdr,
        }
        if self.provider.order:
            prefs["order"] = list(self.provider.order)
        if self.provider.only:
            prefs["only"] = list(self.provider.only)
        if self.max_price is not None:
            prefs["max_price"] = self.max_price.model_dump()
        return prefs

    def slugs(self) -> tuple[str, ...]:
        return (self.model, *self.fallback_models)


class ModelsSection(SectionModel):
    """Per-role model config. A role absent from `roles` is not configured yet and fails closed."""

    roles: dict[LlmRole, RoleModelConfig]

    @model_validator(mode="after")
    def _judges_differ(self) -> Self:
        judge_a, judge_b = self.roles.get("news_judge_a"), self.roles.get("news_judge_b")
        if judge_a is None or judge_b is None:
            return self
        if judge_a.gateway == judge_b.gateway == "deepseek":
            # Owner decision 2026-09-28: every role runs on DeepSeek for now, a single developer, so the
            # two judges cannot be independent; their agreement is accepted as correlated meanwhile.
            return self
        if judge_a.developer == judge_b.developer:
            raise ValueError(
                "news_judge_a and news_judge_b must come from different developers "
                f"(both {judge_a.developer})"
            )
        # A fallback serves the role when its primary is down: the two judges stay independent only when
        # no model of one role shares a developer with any model of the other.
        shared = judge_a.developers() & judge_b.developers()
        if shared:
            raise ValueError(
                "news_judge_a and news_judge_b must come from different developers across their "
                f"fallbacks too (shared {sorted(shared)})"
            )
        return self


def deepseek_profile(
    roles: Mapping[str, RoleModelConfig],
    *,
    model: str,
    thinking: Thinking,
    temperature: float,
    max_tokens: int,
    timeout_s: float,
) -> ModelsSection:
    """Every LLM role on the deepseek gateway with `model` and `thinking` (the console "Switch every role
    to DeepSeek" draft). A configured role keeps its own temperature, max_tokens and timeout_s; a role not
    configured yet takes the given ones. OpenRouter-only options are dropped."""
    configs: dict[str, RoleModelConfig] = {}
    for role in get_args(LlmRole):
        current = roles.get(role)
        configs[role] = RoleModelConfig(
            gateway="deepseek",
            model=model,
            thinking=thinking,
            temperature=current.temperature if current else temperature,
            max_tokens=current.max_tokens if current else max_tokens,
            timeout_s=current.timeout_s if current else timeout_s,
        )
    return ModelsSection.model_validate({"roles": configs})


# --------------------------------------------------------------------------- cmc


class CmcRouteOverride(SectionModel):
    id: int = Field(ge=1, le=30)
    enabled: bool
    cadence_s: PositiveInt | None = None


class CmcSection(SectionModel):
    routes: tuple[CmcRouteOverride, ...]
    route2_core_exchanges: PositiveInt
    route2_peripheral_exchanges: NonNegativeInt
    route3_watchlist_coins: PositiveInt
    ws_coins: NonNegativeInt

    @model_validator(mode="after")
    def _routes_once(self) -> Self:
        if sorted(r.id for r in self.routes) != list(range(1, 31)):
            raise ValueError("routes must list routes 1..30 exactly once")
        return self

    def route(self, route_id: int) -> CmcRouteOverride:
        return next(r for r in self.routes if r.id == route_id)


def effective_cmc_routes(section: CmcSection, static: StaticConfig) -> CmcRoutesFile:
    """Apply the section to the static route catalog and re-run every catalog validator."""
    routes: list[dict[str, Any]] = []
    for base in static.cmc_routes.routes:
        override = section.route(base.id)
        route = base.model_dump(mode="python")
        route["enabled"] = override.enabled
        if override.cadence_s is not None:
            if base.schedule != "interval":
                raise ValueError(f"route #{base.id}: cadence_s applies only to interval routes")
            route["cadence_s"] = override.cadence_s
        params = dict(route["params"])
        if base.id == 2:
            params.update(
                core_exchanges=section.route2_core_exchanges,
                peripheral_exchanges=section.route2_peripheral_exchanges,
                core_cadence_s=route["cadence_s"],
            )
        elif base.id == 3:
            params["watchlist_coins"] = section.route3_watchlist_coins
        elif base.transport == "ws":
            max_coins = int(params["max_coins"])
            if section.ws_coins > max_coins:
                raise ValueError(f"ws_coins must be <= {max_coins}")
            if override.enabled and section.ws_coins < 1:
                raise ValueError(f"route #{base.id}: an enabled WebSocket route needs ws_coins >= 1")
            params["coins"] = section.ws_coins
        route["params"] = params
        routes.append(route)
    return CmcRoutesFile.model_validate(
        {"schema_version": static.cmc_routes.schema_version, "routes": routes}
    )


# --------------------------------------------------------------------------- binance

_SYMBOL_RE = re.compile(r"^[0-9A-Z]{2,30}$")


class _BinanceBase(SectionModel):
    @field_validator("banned_symbols", check_fields=False)
    @classmethod
    def _symbols(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        bad = [s for s in value if not _SYMBOL_RE.fullmatch(s)]
        if bad:
            raise ValueError(f"symbols must be upper-case Binance symbols such as PEPEUSDT: {bad}")
        if len(set(value)) != len(value):
            raise ValueError("banned_symbols must not repeat a symbol")
        return value


BINANCE_FIELDS: Final = ("universe_top_n_by_oi", "banned_symbols")
BINANCE_RISK_FIELDS: Final = ("post_only_wait_s", "ioc_max_slippage")
BinanceSection: type[SectionModel] = create_model(
    "BinanceSection",
    __base__=_BinanceBase,
    **_pick(BinanceFile, BINANCE_FIELDS),
    **_pick(RiskFile, BINANCE_RISK_FIELDS),
)


def effective_binance(section: BaseModel, static: StaticConfig) -> BinanceFile:
    return BinanceFile.model_validate(
        {**static.binance.model_dump(), **section.model_dump(include=set(BINANCE_FIELDS))}
    )


# --------------------------------------------------------------------------- risk

# Fields owned by other sections: entry parameters (binance) and the namespace size multiplier (mode).
RISK_EXCLUDED: Final = ("schema_version", *BINANCE_RISK_FIELDS, "size_multiplier")
RISK_FIELDS: Final = tuple(name for name in RiskFile.model_fields if name not in RISK_EXCLUDED)
# Keys removed from the risk section that immutable stored versions may still carry: dropped on every
# parse (load, rollback, save) and from the payload the config-api shows, so old versions stay valid and a
# re-save never writes them again. `decision_ttl_s`: owner decision 2026-09-28, decisions have no TTL.
RETIRED_RISK_KEYS: Final[frozenset[str]] = frozenset({"decision_ttl_s"})
_retired_logged: set[str] = set()


def without_retired(section: Section, payload: Any) -> Any:
    """`payload` without the keys `section` retired (unchanged when it has none, or is no mapping)."""
    if section is not Section.RISK or not isinstance(payload, Mapping):
        return payload
    found = RETIRED_RISK_KEYS.intersection(payload)
    if not found:
        return payload
    new = found - _retired_logged
    if new:
        _retired_logged.update(new)
        log.info("stored risk config carries retired keys, ignored", extra={"retired_keys": sorted(new)})
    return {k: v for k, v in payload.items() if k not in RETIRED_RISK_KEYS}


class _RiskBase(SectionModel):
    @model_validator(mode="before")
    @classmethod
    def _drop_retired(cls, data: Any) -> Any:
        return without_retired(Section.RISK, data)

    @model_validator(mode="after")
    def _hard_ceilings(self) -> Self:
        check_risk_limits(RiskLimits.of(self))
        return self


RiskSection: type[SectionModel] = create_model(
    "RiskSection", __base__=_RiskBase, **_pick(RiskFile, RISK_FIELDS)
)


def effective_risk(risk: BaseModel, binance: BaseModel, mode: ModeSection, static: StaticConfig) -> RiskFile:
    """Runtime risk parameters: the risk section + binance entry parameters + the mode size multiplier."""
    size_multiplier = dict(static.risk.size_multiplier)
    size_multiplier[mode.mode.value] = mode.size_multiplier
    return RiskFile.model_validate(
        {
            "schema_version": static.risk.schema_version,
            **risk.model_dump(),
            **binance.model_dump(include=set(BINANCE_RISK_FIELDS)),
            "size_multiplier": size_multiplier,
        }
    )


# --------------------------------------------------------------------------- council

COUNCIL_FIELDS: Final = (
    "stance_long",
    "stance_short",
    "quorum",
    "supermajority",
    "max_rounds",
    "debate_enabled",
    "correlation_groups",
    "manager",
)
CouncilSection: type[SectionModel] = create_model(
    "CouncilSection", __base__=SectionModel, **_pick(CouncilFile, COUNCIL_FIELDS)
)


def effective_council(section: BaseModel, static: StaticConfig) -> CouncilFile:
    """Merge editable council fields over the static council (revision bounds stay in code/YAML)."""
    return CouncilFile.model_validate({**static.council.model_dump(), **section.model_dump()})


# --------------------------------------------------------------------------- mode


class ModeSection(SectionModel):
    mode: Account
    size_multiplier: float

    @model_validator(mode="after")
    def _multiplier(self) -> Self:
        check_size_multiplier(self.mode.value, self.size_multiplier)
        return self


def default_size_multiplier(mode: Account, static: StaticConfig) -> float:
    """Default multiplier when switching to `mode` (risk.yaml: paper 1.0, live 0.25)."""
    return static.risk.size_multiplier[mode.value]


# --------------------------------------------------------------------------- go-live checklist

GOLIVE_ITEMS: Final[tuple[tuple[str, str], ...]] = (
    ("live_key_trade_only", "Binance live key has trade permission only (withdrawals and transfers off)"),
    ("live_key_ip_whitelist", "Binance live key is restricted to the VPS IP whitelist"),
    ("live_sub_account", "Live trading runs on a Binance sub-account with capped capital"),
    ("initial_equity_limit", "Initial live equity limit decided by the owner"),
    ("one_way_mode", "Live account is in One-way position mode"),
    ("kill_switch_live_test", "Kill switch tested on live with a minimum-size order"),
    ("independent_kill_path", "Independent kill path tested: hdt-kill over SSH and the Binance app runbook"),
    ("backup_restore_drill", "Postgres point-in-time restore drill completed"),
    (
        "console_not_public",
        "Console and config-api unreachable from the internet (ss -tlnp and external scan)",
    ),
)
GOLIVE_LABELS: Final[dict[str, str]] = dict(GOLIVE_ITEMS)


class GoLiveItem(SectionModel):
    id: str
    checked: bool
    checked_by: str | None = None
    checked_at: UtcDatetime | None = None

    @model_validator(mode="after")
    def _attribution(self) -> Self:
        if self.checked != (self.checked_by is not None and self.checked_at is not None):
            raise ValueError(
                f"{self.id}: a checked item needs checked_by and checked_at, unchecked needs neither"
            )
        return self


class GoLiveSection(SectionModel):
    items: tuple[GoLiveItem, ...]

    @model_validator(mode="after")
    def _known_items(self) -> Self:
        if [item.id for item in self.items] != [item_id for item_id, _ in GOLIVE_ITEMS]:
            raise ValueError(f"items must be exactly {[item_id for item_id, _ in GOLIVE_ITEMS]} in order")
        return self

    @property
    def complete(self) -> bool:
        return all(item.checked for item in self.items)

    def missing(self) -> list[str]:
        return [item.id for item in self.items if not item.checked]


# --------------------------------------------------------------------------- registry, validation, seeds

SECTION_MODELS: Final[dict[Section, type[SectionModel]]] = {
    Section.MODELS: ModelsSection,
    Section.CMC: CmcSection,
    Section.BINANCE: BinanceSection,
    Section.RISK: RiskSection,
    Section.COUNCIL: CouncilSection,
    Section.MODE: ModeSection,
    Section.GOLIVE: GoLiveSection,
}


def parse_section(section: Section, payload: Any) -> SectionModel:
    """Parse a stored or submitted payload (field constraints + hard ceilings)."""
    return SECTION_MODELS[section].model_validate(payload)


def validate_section(section: Section, payload: Any, static: StaticConfig) -> SectionModel:
    """Parse, then materialize against the static config so every static cross-field validator runs.

    Raises `pydantic.ValidationError` or `ValueError` (incl. `CeilingViolationError`).
    """
    model = parse_section(section, payload)
    if isinstance(model, CmcSection):
        effective_cmc_routes(model, static)
    elif section is Section.BINANCE:
        effective_binance(model, static)
    elif section is Section.COUNCIL:
        effective_council(model, static)
    return model


def seed_payloads(static: StaticConfig) -> dict[Section, SectionModel]:
    """Version 1 of every section, taken from `config/*.yaml`.

    `models` has no static default: each role's model is chosen on the console (Design Contract
    section 3), and a role without a model fails closed.
    """
    routes = static.cmc_routes
    ws_route = next(r for r in routes.routes if r.transport == "ws")
    cmc = CmcSection(
        routes=tuple(
            CmcRouteOverride(
                id=r.id, enabled=r.enabled, cadence_s=r.cadence_s if r.schedule == "interval" else None
            )
            for r in routes.routes
        ),
        route2_core_exchanges=int(routes.by_id(2).params["core_exchanges"]),
        route2_peripheral_exchanges=int(routes.by_id(2).params["peripheral_exchanges"]),
        route3_watchlist_coins=int(routes.by_id(3).params["watchlist_coins"]),
        ws_coins=int(ws_route.params["max_coins"]) if ws_route.enabled else 0,
    )
    return {
        Section.CMC: cmc,
        Section.BINANCE: BinanceSection.model_validate(
            {
                **static.binance.model_dump(include=set(BINANCE_FIELDS)),
                **static.risk.model_dump(include=set(BINANCE_RISK_FIELDS)),
            }
        ),
        Section.RISK: RiskSection.model_validate(static.risk.model_dump(include=set(RISK_FIELDS))),
        Section.COUNCIL: CouncilSection.model_validate(
            static.council.model_dump(include=set(COUNCIL_FIELDS))
        ),
        Section.MODE: ModeSection(
            mode=Account.PAPER, size_multiplier=default_size_multiplier(Account.PAPER, static)
        ),
        Section.GOLIVE: GoLiveSection(
            items=tuple(GoLiveItem(id=item_id, checked=False) for item_id, _ in GOLIVE_ITEMS)
        ),
    }
