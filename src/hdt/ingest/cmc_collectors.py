"""CMC REST collectors: one job per route of `config/cmc_routes.yaml`, as overridden by the CMC section.

Every HTTP response is recorded verbatim by the client's `on_response` sink (lake + credit meter) before a
collector looks at it. Collectors are independent: a failing route never stops another one.

Targets are read point-in-time from the lake only:
- route #2 (market pairs by exchange): exchanges ranked by `liquidity_score` from route #1, re-ranked once
  per UTC month (the first route #1 record of the month); top `core_exchanges` run at `core_cadence_s`,
  the next `peripheral_exchanges` at `peripheral_cadence_s` (two jobs, `2:core` and `2:peripheral`);
- route #3, #9, #27: the universe watchlist; route #7: the LTX cross-section; route #8, #10: every
  universe member; route #28: the route #2 exchanges; DEX routes #24-26 run on events (a new candidate
  coin) with the token address and chain slug from route #12 (`platform.token_address`, `platform.slug`).

Paging: list routes billed per 250 items page with `start`/`limit` (limit 250 keeps one credit per page)
or follow `has_more` (liquidation lists). Page n > 1 is stored under key `<key>:p<n>` (`p<n>` for an
empty key); a liquidation list cycle is complete when its last stored page has `has_more = false`
(a coin absent from a complete list had 0 liquidations).

Admission: the credit governor admits each job by `shed_class` before any call; 1009/1010/1011 put the
governor into its halt state (no retry) and every later job is refused until the cap resets.

Plan refusal: a 1006 response (the key's plan does not include the endpoint, not charged) ends the job
with outcome `disabled` and an error starting with `NOT_ON_PLAN_ERROR`. It is not a failure: the route
keeps its normal cadence, its data stays empty, and the first call after a plan upgrade succeeds and
returns it to `ok`. A run refused for the key (1006, or no active key) is marked `key_blocked` so the
recorder can bring the job forward when a new CMC key version becomes active.

DEX targets (#24-27) are the map's `platform` objects whose `token_address` is an on-chain contract
address; a bare hex id of any other length (e.g. HYPE's 16-byte HyperCore token index on platform
`hyperliquid`) is not a DEX token and is left out (CMC answers `400 Parameter error` for it).
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Final, Literal

from hdt.core.clock import utcnow
from hdt.core.config import CmcRoute, CmcRoutesFile
from hdt.db.models.ingest import NOT_ON_PLAN_ERROR
from hdt.ingest.cmc_client import (
    CmcClient,
    CmcConfigError,
    CmcHaltError,
    CmcNoKeyError,
    CmcNotOnPlanError,
    CmcUnavailableError,
)
from hdt.ingest.credit_governor import CreditGovernor
from hdt.lake.pit_query import PitQuery
from hdt.lake.schemas import CMC_OHLCV_BACKFILL_ROUTE
from hdt.lake.universe import Universe, load_universe

log = logging.getLogger(__name__)

PAGE_LIMIT: Final[int] = 250
MAP_PAGE_LIMIT: Final[int] = 5000
HISTORY_PAGE_LIMIT: Final[int] = 500
QUOTES_BACKFILL_DAYS: Final[int] = 30  # Startup plan: 1 month of intraday history
OHLCV_COUNT: Final[int] = 2  # current + previous hour, so a late run never leaves a hole
OHLCV_BACKFILL_HOURS: Final[int] = QUOTES_BACKFILL_DAYS * 24  # the Startup plan's month of history
DEX_HOLDER_TAG: Final[str] = "tag_all"
MAX_MAP_PAGES: Final[int] = 20
DAY_S: Final[int] = 86_400

# Route params that configure the recorder itself and are never sent to CMC.
INTERNAL_PARAMS: Final[frozenset[str]] = frozenset(
    {
        "core_exchanges",
        "peripheral_exchanges",
        "core_cadence_s",
        "peripheral_cadence_s",
        "watchlist_coins",
        "max_coins",
        "coins",
    }
)

Paging = Literal["none", "start_limit", "has_more"]
Outcome = Literal["ok", "failing", "halted", "shed", "pending", "disabled"]


class TargetsUnavailableError(LookupError):
    """The lake does not yet hold what this route's targets are derived from."""


@dataclass(frozen=True)
class CallSpec:
    key: str = ""
    params: Mapping[str, Any] = field(default_factory=dict)
    json_body: Mapping[str, Any] | None = None
    lake_route: str | None = None
    """Lake route of the capture when it is not the catalog route name."""


@dataclass(frozen=True)
class ExchangeRef:
    exchange_id: int
    slug: str
    liquidity_score: float | None


@dataclass(frozen=True)
class TokenRef:
    cmc_id: int
    platform_slug: str
    token_address: str


@dataclass(frozen=True)
class RouteJob:
    route_key: str
    route: CmcRoute
    sort_order: int
    cadence_s: int | None
    cadence_label: str
    plan: Callable[[LakeView], list[CallSpec]]
    paging: Paging = "none"
    page_limit: int = PAGE_LIMIT
    once: bool = False

    @property
    def credits_per_day(self) -> int:
        return round(self.route.est_credits_month / 30)


@dataclass(frozen=True)
class JobResult:
    route_key: str
    outcome: Outcome
    calls: int
    error: str | None = None
    key_blocked: bool = False
    """The key could not serve the run: no active CMC key, or the key's plan lacks the endpoint (1006)."""


# --------------------------------------------------------------------------- lake view


class LakeView:
    """Point-in-time targets derived from recorded responses (as of the clock's now)."""

    def __init__(self, pit: PitQuery, clock: Callable[[], datetime] = utcnow) -> None:
        self._pit = pit
        self._clock = clock
        self._platform_cache: tuple[tuple[str, ...], dict[int, TokenRef]] | None = None

    def now(self) -> datetime:
        return self._clock()

    def universe(self) -> Universe:
        universe = load_universe(self._pit, self.now())
        if universe is None:
            raise TargetsUnavailableError("the universe has not been built yet")
        return universe

    def exchange_tiers(self, core_n: int, peripheral_n: int) -> tuple[list[ExchangeRef], list[ExchangeRef]]:
        """Route #1 exchanges ranked by liquidity score, frozen at the first record of the UTC month."""
        now = self.now()
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        records = self._pit.series("cmc", "derivatives_exchanges", month_start, now, as_of=now)
        ranked: list[ExchangeRef] = []
        for record in records if records else self._latest("derivatives_exchanges"):
            ranked = rank_exchanges(_data(record.body()))
            if ranked:
                break
        if not ranked:
            raise TargetsUnavailableError("no successful route #1 (derivatives exchanges) record yet")
        return ranked[:core_n], ranked[core_n : core_n + peripheral_n]

    def token_refs(self) -> dict[int, TokenRef]:
        """`cmc_id -> (chain slug, token address)` from the latest complete route #12 map."""
        pages = []
        for n in range(1, MAX_MAP_PAGES + 1):
            record = self._pit.latest("cmc", "crypto_map", self.now(), key=page_key("", n))
            if record is None:
                break
            pages.append(record)
            items = _data(record.body())
            if not isinstance(items, list) or len(items) < MAP_PAGE_LIMIT:
                break
        if not pages:
            raise TargetsUnavailableError("no route #12 (cryptocurrency map) record yet")
        signature = tuple(p.body_sha256 for p in pages)
        if self._platform_cache is not None and self._platform_cache[0] == signature:
            return self._platform_cache[1]
        refs: dict[int, TokenRef] = {}
        for page in pages:
            for item in _data(page.body()) or []:
                platform = item.get("platform") if isinstance(item, dict) else None
                if (
                    isinstance(platform, dict)
                    and platform.get("slug")
                    and _contract_address(platform.get("token_address"))
                ):
                    refs[int(item["id"])] = TokenRef(
                        int(item["id"]), str(platform["slug"]), str(platform["token_address"])
                    )
        self._platform_cache = (signature, refs)
        return refs

    def has_record(self, route: str, key: str, params: Mapping[str, Any] | None = None) -> bool:
        """A successful record of `route` / `key` exists; with `params`, one made with those query params
        (a record from before a params change does not count)."""
        record = self._pit.latest("cmc", route, self.now(), key=key, lookback=timedelta(days=3650))
        if record is None or record.http_status != 200:
            return False
        if not params:
            return True
        stored = json.loads(record.params_json)
        return all(str(stored.get(k)) == str(v) for k, v in params.items())

    def _latest(self, route: str) -> list[Any]:
        record = self._pit.latest("cmc", route, self.now())
        return [record] if record is not None else []


def rank_exchanges(data: Any) -> list[ExchangeRef]:
    exchanges = data.get("exchanges") if isinstance(data, dict) else None
    refs: list[ExchangeRef] = []
    for item in exchanges if isinstance(exchanges, list) else []:
        if not isinstance(item, dict) or not item.get("exchange_slug") or item.get("exchange_id") is None:
            continue
        score = item.get("liquidity_score")
        refs.append(
            ExchangeRef(
                int(item["exchange_id"]),
                str(item["exchange_slug"]),
                float(score) if isinstance(score, int | float) else None,
            )
        )
    refs.sort(key=lambda e: (e.liquidity_score is None, -(e.liquidity_score or 0.0), e.slug))
    return refs


def page_key(key: str, page: int) -> str:
    if page <= 1:
        return key
    return f"{key}:p{page}" if key else f"p{page}"


def _data(body: bytes) -> Any:
    try:
        parsed = json.loads(body)
    except ValueError:
        return None
    return parsed.get("data") if isinstance(parsed, dict) else None


_HEX_ADDRESS = re.compile(r"0x[0-9a-fA-F]+")
_HEX_CONTRACT_DIGITS: Final[frozenset[int]] = frozenset({40, 64})  # EVM 20-byte, Move/Starknet 32-byte


def _contract_address(value: object) -> bool:
    """A usable DEX `tokenAddress`: non-empty, and a plain hex id only when it has an address length."""
    if not isinstance(value, str) or not value:
        return False
    if _HEX_ADDRESS.fullmatch(value):
        return len(value) - 2 in _HEX_CONTRACT_DIGITS
    return True


def _items(data: Any) -> list[Any]:
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for name in ("market_pairs", "cryptocurrencies", "exchanges", "points"):
            value = data.get(name)
            if isinstance(value, list):
                return value
    return []


# --------------------------------------------------------------------------- job catalog


def api_params(route: CmcRoute) -> dict[str, Any]:
    return {k: v for k, v in route.params.items() if k not in INTERNAL_PARAMS}


def _ids(members: Iterable[Any]) -> str:
    return ",".join(str(m.cmc_id) for m in members)


def _cadence_label(route: CmcRoute, cadence_s: int | None) -> str:
    if route.schedule == "interval" and cadence_s is not None:
        return f"{cadence_s // 3600} h" if cadence_s % 3600 == 0 else f"{cadence_s // 60} min"
    return {"daily": "daily", "once": "once (backfill)", "on_event": "on event", "stream": "stream"}[
        route.schedule
    ]


def build_jobs(routes: CmcRoutesFile) -> list[RouteJob]:
    """Scheduled jobs for every enabled REST route except `key_info` (run by the credit meter)."""
    jobs: list[RouteJob] = []
    for route in routes.routes:
        if route.transport != "rest" or not route.enabled or route.name == "key_info":
            continue
        if route.schedule == "on_event":
            continue  # triggered by `dex_event_specs`
        cadence = route.cadence_s if route.schedule == "interval" else DAY_S
        if route.name == "exchange_derivative_market_pairs":
            p = route.params
            core, peripheral = int(p["core_exchanges"]), int(p["peripheral_exchanges"])
            core_s, peripheral_s = int(p["core_cadence_s"]), int(p["peripheral_cadence_s"])

            def core_plan(view: LakeView, c: int = core, q: int = peripheral) -> list[CallSpec]:
                return _exchange_specs(view.exchange_tiers(c, q)[0])

            def peripheral_plan(view: LakeView, c: int = core, q: int = peripheral) -> list[CallSpec]:
                return _exchange_specs(view.exchange_tiers(c, q)[1])

            jobs.append(
                RouteJob("2:core", route, 20, core_s, _cadence_label(route, core_s), core_plan, "start_limit")
            )
            if peripheral > 0:
                jobs.append(
                    RouteJob(
                        "2:peripheral",
                        route,
                        21,
                        peripheral_s,
                        _cadence_label(route, peripheral_s),
                        peripheral_plan,
                        "start_limit",
                    )
                )
            continue
        plan, paging, limit = _PLANNERS.get(route.name, (_single, "none", PAGE_LIMIT))
        if route.name == "exchange_assets":
            route2 = routes.by_id(2).params
            plan = _exchange_assets_for(int(route2["core_exchanges"]), int(route2["peripheral_exchanges"]))
        jobs.append(
            RouteJob(
                str(route.id),
                route,
                route.id * 10,
                cadence,
                _cadence_label(route, cadence),
                _bind(plan, route),
                paging,
                limit,
                once=route.schedule == "once",
            )
        )
    return jobs


Planner = Callable[[LakeView, CmcRoute], list[CallSpec]]


def _bind(plan: Planner, route: CmcRoute) -> Callable[[LakeView], list[CallSpec]]:
    return lambda view: plan(view, route)


def _single(_view: LakeView, route: CmcRoute) -> list[CallSpec]:
    return [CallSpec(params=api_params(route))]


def _exchange_specs(exchanges: list[ExchangeRef]) -> list[CallSpec]:
    return [CallSpec(key=e.slug, params={"exchange_slug": e.slug}) for e in exchanges]


def _watchlist_pairs(view: LakeView, route: CmcRoute) -> list[CallSpec]:
    return [
        CallSpec(key=str(m.cmc_id), params={**api_params(route), "crypto_id": m.cmc_id})
        for m in view.universe().watchlist
    ]


def _ohlcv(view: LakeView, route: CmcRoute) -> list[CallSpec]:
    """The newest two hourly bars of every LTX coin in one call (key ""), plus, once per coin until it has a
    successful record made with the route's current params, the last `OHLCV_BACKFILL_HOURS` bars (key = cmc
    id): Technical and the Macro regime then have their history at once instead of after weeks of hourly
    runs. A record from before a params change (the daily-sampled ones without `interval`) is fetched
    again."""
    ltx = view.universe().ltx
    base = api_params(route)
    specs = [CallSpec(params={**base, "id": _ids(ltx), "count": OHLCV_COUNT})]
    specs.extend(
        CallSpec(
            key=str(m.cmc_id),
            params={**base, "id": m.cmc_id, "count": OHLCV_BACKFILL_HOURS},
            lake_route=CMC_OHLCV_BACKFILL_ROUTE,
        )
        for m in ltx
        if not view.has_record(CMC_OHLCV_BACKFILL_ROUTE, str(m.cmc_id), base)
    )
    return specs


def _all_members(view: LakeView, route: CmcRoute) -> list[CallSpec]:
    return [CallSpec(params={**api_params(route), "id": _ids(view.universe().members)})]


def _quotes_backfill(view: LakeView, route: CmcRoute) -> list[CallSpec]:
    start = view.now() - timedelta(days=QUOTES_BACKFILL_DAYS)
    return [
        CallSpec(
            key=str(m.cmc_id),
            params={
                **api_params(route),
                "id": m.cmc_id,
                "time_start": start.isoformat().replace("+00:00", "Z"),
                "count": 10_000,
            },
        )
        for m in view.universe().watchlist
        if not view.has_record(route.name, str(m.cmc_id))
    ]


def _once_single(view: LakeView, route: CmcRoute) -> list[CallSpec]:
    return [] if view.has_record(route.name, "") else [CallSpec(params=api_params(route))]


def _map(_view: LakeView, route: CmcRoute) -> list[CallSpec]:
    return [CallSpec(params={**api_params(route), "aux": "platform,is_active"})]


def _holders(view: LakeView, _route: CmcRoute) -> list[CallSpec]:
    """Every coin the scanner can pick (the LTX universe, which contains the watchlist) with a DEX token:
    Fundamental needs `holder_top10_share` for any council candidate, not only the watchlist."""
    refs = view.token_refs()
    specs = []
    for member in view.universe().ltx:
        ref = refs.get(member.cmc_id)
        if ref is not None:
            specs.append(
                CallSpec(
                    key=str(member.cmc_id),
                    json_body={
                        "tokenAddress": ref.token_address,
                        "platform": ref.platform_slug,
                        "tag": DEX_HOLDER_TAG,
                    },
                )
            )
    return specs


def _exchange_assets_for(core_n: int, peripheral_n: int) -> Planner:
    def plan(view: LakeView, route: CmcRoute) -> list[CallSpec]:
        core, peripheral = view.exchange_tiers(core_n, peripheral_n)
        return [
            CallSpec(key=e.slug, params={**api_params(route), "id": e.exchange_id}) for e in core + peripheral
        ]

    return plan


def _history_once(view: LakeView, route: CmcRoute) -> list[CallSpec]:
    """Backfill routes #17/#19: run until one successful record exists."""
    if view.has_record(route.name, ""):
        return []
    params = api_params(route)
    if route.name == "altcoin_season_historical":
        params.setdefault("timeframe", "90d")  # longest documented window
    return [CallSpec(params=params)]


_PLANNERS: Final[dict[str, tuple[Planner, Paging, int]]] = {
    "derivatives_exchanges": (_single, "start_limit", PAGE_LIMIT),
    "crypto_derivative_market_pairs": (_watchlist_pairs, "start_limit", PAGE_LIMIT),
    "total_liquidations": (_single, "none", PAGE_LIMIT),
    "liquidations_by_exchange": (_single, "has_more", PAGE_LIMIT),
    "liquidations_by_crypto": (_single, "has_more", PAGE_LIMIT),
    "ohlcv_historical": (_ohlcv, "none", PAGE_LIMIT),
    "quotes_latest": (_all_members, "none", PAGE_LIMIT),
    "quotes_historical": (_quotes_backfill, "none", PAGE_LIMIT),
    "price_performance_stats": (_all_members, "none", PAGE_LIMIT),
    "listings_latest": (_single, "none", PAGE_LIMIT),
    "crypto_map": (_map, "start_limit", MAP_PAGE_LIMIT),
    "fear_greed_historical": (_history_once, "start_limit", HISTORY_PAGE_LIMIT),
    "altcoin_season_historical": (_history_once, "none", PAGE_LIMIT),
    "categories": (_single, "start_limit", PAGE_LIMIT),
    "dex_holders": (_holders, "none", PAGE_LIMIT),
}


# --------------------------------------------------------------------------- DEX on-event routes

DEX_EVENT_ROUTES: Final[tuple[str, ...]] = ("dex_security_detail", "dex_token_pools", "dex_liquidity_change")
DEX_EVENT_COOLDOWN: Final[timedelta] = timedelta(hours=24)


def dex_event_spec(route: CmcRoute, ref: TokenRef) -> CallSpec:
    """Query names verified with live keyless calls (chain = the map's `platform.slug`)."""
    key = str(ref.cmc_id)
    if route.name == "dex_security_detail":
        return CallSpec(key=key, params={"platformName": ref.platform_slug, "address": ref.token_address})
    if route.name == "dex_token_pools":
        return CallSpec(key=key, params={"platform": ref.platform_slug, "address": ref.token_address})
    if route.name == "dex_liquidity_change":
        return CallSpec(
            key=key,
            params={"platform": ref.platform_slug, "address": ref.token_address, "limit": 100},
        )
    raise ValueError(f"{route.name} is not an on-event DEX route")


# --------------------------------------------------------------------------- runner

HealthWriter = Callable[[RouteJob, datetime, Outcome, str | None], Awaitable[None]]


class CollectorRunner:
    """Runs one job: plan targets, ask the governor, call page by page, report route health."""

    def __init__(
        self,
        *,
        client: CmcClient,
        governor: CreditGovernor,
        view: LakeView,
        health: HealthWriter,
        clock: Callable[[], datetime] = utcnow,
    ) -> None:
        self._client = client
        self._governor = governor
        self._view = view
        self._health = health
        self._clock = clock

    async def run(self, job: RouteJob) -> JobResult:
        started = self._clock()
        try:
            specs = job.plan(self._view)
        except TargetsUnavailableError as exc:
            result = JobResult(job.route_key, "pending", 0, str(exc))
        else:
            result = await self._run_specs(job, specs)
        await self._health(job, started, result.outcome, result.error)
        return result

    async def run_specs(self, job: RouteJob, specs: list[CallSpec]) -> JobResult:
        """Run explicit specs (on-event DEX routes); reports health like a scheduled job."""
        started = self._clock()
        result = await self._run_specs(job, specs)
        await self._health(job, started, result.outcome, result.error)
        return result

    async def _run_specs(self, job: RouteJob, specs: list[CallSpec]) -> JobResult:
        route = job.route
        calls = 0
        errors: list[str] = []
        for spec in specs:
            page = 1
            while True:
                admission = self._governor.admit(route.shed_class, at=self._clock())
                if not admission.allowed:
                    halted = self._governor.halt_state(self._clock()) is not None
                    return JobResult(job.route_key, "halted" if halted else "shed", calls, admission.reason)
                params = dict(spec.params)
                if job.paging != "none":
                    params["start"] = 1 + (page - 1) * job.page_limit
                    params["limit"] = job.page_limit
                try:
                    result = await self._client.call(
                        spec.lake_route or route.name,
                        route.path,
                        params,
                        keyless=route.keyless,
                        key=page_key(spec.key, page),
                        json_body=spec.json_body,
                    )
                except CmcHaltError as exc:
                    until = self._governor.halt(exc.kind, self._clock())
                    log.error(
                        "cmc halt state entered",
                        extra={"route": route.name, "halt": exc.kind, "until": until.isoformat()},
                    )
                    return JobResult(job.route_key, "halted", calls + 1, f"{exc.kind}: {exc}")
                except CmcNotOnPlanError as exc:
                    return JobResult(
                        job.route_key, "disabled", calls + 1, f"{NOT_ON_PLAN_ERROR}: {exc}", key_blocked=True
                    )
                except CmcNoKeyError as exc:
                    return JobResult(job.route_key, "failing", calls, str(exc), key_blocked=True)
                except CmcConfigError as exc:
                    return JobResult(job.route_key, "failing", calls, str(exc))
                except CmcUnavailableError as exc:
                    calls += 1
                    errors.append(str(exc))
                    log.warning("cmc route call failed", extra={"route": route.name, "key": spec.key})
                    break
                calls += 1
                if not _has_next_page(job, result.data):
                    break
                page += 1
        if errors:
            return JobResult(job.route_key, "failing", calls, "; ".join(errors[:3]))
        return JobResult(job.route_key, "ok", calls)


def _has_next_page(job: RouteJob, data: Any) -> bool:
    if job.paging == "has_more":
        return isinstance(data, dict) and data.get("has_more") is True
    if job.paging == "start_limit":
        return len(_items(data)) >= job.page_limit
    return False
