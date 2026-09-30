"""Recorder service (`python -m hdt.ingest.scheduler`): every CMC and Binance public route into the lake.

Bootstrap inputs (docker secrets / env): HDT_PG_DSN (role hdt_recorder), HDT_REDIS_URL (ACL user
recorder), HDT_SCOPE_PRIVATE_KEY_FILE (private key of vault scope `data`), HDT_METRICS_PORT (default 9102).
The CMC key is never in the environment: it is read from vault item `data/cmc_api_key` on every call,
so a key rotated on the console is used by the next call. The recorder is the owner of scope `data` and
answers the console's key tests (`/v1/key/info`, 0 credits).

Jobs (APScheduler, one asyncio loop, every job isolated so one failure never stops another):
- CMC REST routes from the active CMC config section (rebuilt when a new version is applied);
- hourly `/v1/key/info` credit meter + governor anchor; key info also runs at startup;
- every 15 s the active version of vault item `data/cmc_api_key` is read: a new version (a key entered or
  rotated on the console) brings forward every CMC job whose last run the key could not serve (no
  active key, or 1006 not on the plan), so a failed `once` backfill (#9) runs again without a restart;
- a route refused by the plan (1006) is `disabled` in `route_health` ("not on current CMC plan") and
  keeps its normal cadence (the refusal is not charged): after a plan upgrade the next call succeeds
  and the route returns to `ok` with no restart;
- Binance REST (B1, B2, B7, B8, B9, premium index) and two WebSocket families (`/market`, `/public`),
  plus a hot-set connection with 100 ms depth diffs for candidate and held coins;
- universe build daily at 00:05 UTC; the first build of the process starts once CMC #11/#12 have run and
  their records are in the lake; a failed build is retried the same UTC day (`binance.universe_retry`);
- optional CMC WebSocket route #30 for held coins;
- lake maintenance: compaction of closed hours every minute, daily Merkle root + lake stats, raw depth
  retention; route lateness every minute.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import shutil
import signal
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Final

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from prometheus_client import Gauge, start_http_server
from redis.asyncio import Redis
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from hdt.core.clock import utcnow
from hdt.core.config import (
    BinanceFile,
    CmcRoutesFile,
    StaticConfig,
    UniverseRetry,
    read_env_value,
    require_env_value,
    static_config,
)
from hdt.core.logging import configure_logging
from hdt.core.streams import connect_redis
from hdt.db.session import make_engine, make_session_factory
from hdt.ingest.binance_collectors import (
    BinanceCollectors,
    UniverseInputsMissingError,
    ws_depth_streams,
    ws_hot_streams,
    ws_market_streams,
)
from hdt.ingest.binance_public import BinanceBackoffError, BinanceHttpError, BinancePublic
from hdt.ingest.binance_ws import BinanceWsRecorder
from hdt.ingest.cmc_client import CmcClient, CmcError, CmcResult, MinuteRateLimiter
from hdt.ingest.cmc_collectors import (
    DEX_EVENT_COOLDOWN,
    DEX_EVENT_ROUTES,
    CollectorRunner,
    LakeView,
    Outcome,
    RouteJob,
    TargetsUnavailableError,
    build_jobs,
    dex_event_spec,
)
from hdt.ingest.cmc_key_check import DATA_SECRET_CHECKS
from hdt.ingest.cmc_ws import ROUTE as CMC_WS_ROUTE
from hdt.ingest.cmc_ws import CmcWsClient
from hdt.ingest.credit_governor import CreditGovernor
from hdt.ingest.credit_meter import CreditMeter
from hdt.ingest.hot_set import HotSet
from hdt.ingest.recorder_db import (
    RouteStatus,
    mark_late_routes,
    record_lake_stats,
    upsert_route_health,
    upsert_ws_health,
)
from hdt.lake.pit_query import PitQuery
from hdt.lake.raw_store import RawStore
from hdt.lake.schemas import Capture
from hdt.ops.metrics import ROUTE_LATENCY
from hdt.settings.events import ConfigWatcher
from hdt.settings.schemas import Section
from hdt.settings.versions import SectionNotConfiguredError
from hdt.vault.loader import SecretLoader, SecretNotAvailableError
from hdt.vault.owner import OwnerWorker
from hdt.vault.scopes import load_private_key

log = logging.getLogger(__name__)

SERVICE: Final[str] = "recorder"
CMC_SECRET: Final[str] = "cmc_api_key"  # noqa: S105 - vault item name
FALLBACK_RATE_PER_MIN: Final[int] = 30  # smallest paid CMC plan; used until /v1/key/info answers
WS_STREAMS_PER_CONNECTION: Final[int] = 1000  # Binance allows 1024
RAW_DEPTH_ROUTES: Final[tuple[tuple[str, str], ...]] = (
    ("binance", "ws_depth20"),
    ("binance", "ws_depth"),
    ("binance", "depth"),
)
LAKE_DISK_ALERT: Final[float] = 0.80
# CMC routes the universe is built from (#11, #12): the first build of a process waits for both.
UNIVERSE_INPUT_ROUTES: Final[frozenset[str]] = frozenset({"listings_latest", "crypto_map"})
UNIVERSE_BUILD_JOB: Final[str] = "universe_build"

UP = Gauge("hdt_recorder_route_up", "1 when the last cycle of the route succeeded", ["route_key"])
DISK = Gauge("hdt_lake_disk_used_fraction", "Used fraction of the disk holding the lake")
WS_GAPS = Gauge("hdt_ws_gaps_24h", "Binance WebSocket gaps over the last 24 h", ["connection"])

# Binance REST route health rows (matrix section 5.2).
BINANCE_ROUTES: Final[dict[str, tuple[int, str, str, str, int]]] = {
    # key: (sort order, route label, cadence label, consumers, cadence seconds)
    "B1": (1010, "exchangeInfo, fundingInfo", "daily", "levels, risk, execution, crowding", 86_400),
    "B2": (1020, "klines, markPriceKlines", "1 h batch", "technical, scoring resolver", 3_600),
    "B7": (1070, "fundingRate", "daily", "crowding", 86_400),
    "B8": (1080, "openInterest, openInterestHist, premiumIndex", "5 min", "crowding, universe", 300),
    "B9": (1090, "depth snapshot", "on hot-set entry", "microstructure", 0),
    "U": (1100, "universe build", "daily", "quant core, recorder", 86_400),
}


def universe_retry_delay(retry: UniverseRetry, failures: int, jitter: float) -> float:
    """Seconds before the next same-day build after `failures` failed attempts (`jitter` in [0, 1))."""
    delay: float = min(retry.backoff_s * 2.0 ** (failures - 1), retry.backoff_max_s)
    return delay * (1 + retry.jitter_frac * jitter)


@dataclass
class WsSupervisor:
    """Runs one `BinanceWsRecorder` per connection name and restarts it when its stream set changes."""

    write: Callable[[list[Capture]], Awaitable[None]]
    health: Callable[[str, str, int, datetime | None, datetime | None], Awaitable[None]]
    flush_s: float
    recycle_after_s: float
    running: dict[str, tuple[BinanceWsRecorder, asyncio.Event, asyncio.Task[None]]] = field(
        default_factory=dict
    )

    async def ensure(self, name: str, base_url: str, streams: list[str]) -> bool:
        """Start, restart or stop connection `name`; returns True when the stream set changed."""
        current = self.running.get(name)
        if current is not None and current[0].streams == sorted(set(streams)):
            return False
        if current is not None:
            current[1].set()
            await current[2]
            del self.running[name]
        if not streams:
            return current is not None
        recorder = BinanceWsRecorder(
            name=name,
            base_url=base_url,
            streams=streams,
            write=self.write,
            health=self.health,
            flush_s=self.flush_s,
            recycle_after_s=self.recycle_after_s,
        )
        stop = asyncio.Event()
        self.running[name] = (recorder, stop, asyncio.create_task(recorder.run(stop)))
        return True

    async def ensure_family(self, family: str, base_url: str, streams: list[str]) -> None:
        """Split a stream family over as many connections as the per-connection limit needs."""
        chunks = [
            streams[i : i + WS_STREAMS_PER_CONNECTION]
            for i in range(0, len(streams), WS_STREAMS_PER_CONNECTION)
        ]
        wanted = {f"{family}-{n + 1}": chunk for n, chunk in enumerate(chunks)}
        for name in [n for n in self.running if n.startswith(f"{family}-") and n not in wanted]:
            await self.ensure(name, base_url, [])
        for name, chunk in wanted.items():
            await self.ensure(name, base_url, chunk)

    async def stop_all(self) -> None:
        for name in list(self.running):
            await self.ensure(name, "", [])

    def gaps(self) -> dict[str, int]:
        return {name: rec.gaps_24h() for name, (rec, _stop, _task) in self.running.items()}


class Recorder:
    def __init__(
        self,
        *,
        static: StaticConfig,
        sessions: sessionmaker[Session],
        redis: Redis,
        secrets: SecretLoader,
        http: httpx.AsyncClient,
        store: RawStore,
        pit: PitQuery,
    ) -> None:
        self.static = static
        self.sessions = sessions
        self.redis = redis
        self.secrets = secrets
        self.store = store
        self.pit = pit
        cmc = static.settings.cmc
        self.governor = CreditGovernor(fallback_monthly_quota=cmc.quota_monthly_design)
        self.meter = CreditMeter(
            governor=self.governor, sessions=sessions, alert_fraction=cmc.alert_projection_frac
        )
        self.watcher = ConfigWatcher(
            redis=redis,
            session_factory=sessions,
            service=SERVICE,
            consumer=f"{SERVICE}-1",
            sections=(Section.CMC, Section.BINANCE),
        )
        self.client = CmcClient(
            http,
            base_url=cmc.base_url,
            keyless_base_url=cmc.keyless_base_url,
            api_key=self._cmc_key,
            limiter=MinuteRateLimiter(cmc.rate_limit_per_min),
            keyless_limiter=MinuteRateLimiter(cmc.keyless_rate_limit_per_min),
            on_response=self._on_cmc_response,
            keyed_fallback_allowed=self.governor.keyed_fallback_allowed,
        )
        self.view = LakeView(pit)
        self.runner = CollectorRunner(
            client=self.client, governor=self.governor, view=self.view, health=self._route_health
        )
        self.binance_rest = BinancePublic(
            http, base_url=self.binance_cfg().live.rest, on_capture=self._write_one
        )
        self.bn = BinanceCollectors(rest=self.binance_rest, store=store, pit=pit, binance=self.binance_cfg)
        self.hot = HotSet(redis, self.bn.universe)
        cfg = self.binance_cfg()
        self.ws = WsSupervisor(
            write=self._write_many,
            health=self._ws_health,
            flush_s=cfg.ws_batch_flush_s,
            recycle_after_s=cfg.ws_reconnect_before_s,
        )
        # a job delayed by a busy loop still runs (once): no silent skips of first runs
        self.scheduler = AsyncIOScheduler(
            timezone="UTC", job_defaults={"coalesce": True, "misfire_grace_time": None}
        )
        self.jobs: dict[str, RouteJob] = {}
        self._hot_symbols: set[str] = set()
        self._dex_job_cache: dict[str, RouteJob] = {}
        self._universe_inputs_run: set[str] = set()
        """Universe input routes (#11, #12) whose job has run in this process, whatever the outcome."""
        self._key_blocked: set[str] = set()
        """CMC job ids whose last run the key could not serve (no active key, or 1006 not on the plan)."""
        self._key_version: int | None = None
        """Active version of the CMC key vault item as last seen by `cmc_key_watch`."""
        self._universe_attempts: tuple[date, int] | None = None
        """(UTC day, build attempts made that day)."""

    # ------------------------------------------------------------------ config

    def cmc_routes(self) -> CmcRoutesFile:
        try:
            return self.watcher.pin().cmc_routes(self.static)
        except SectionNotConfiguredError:
            return self.static.cmc_routes

    def binance_cfg(self) -> BinanceFile:
        try:
            return self.watcher.pin().binance(self.static)
        except SectionNotConfiguredError:
            return self.static.binance

    def _cmc_key(self) -> str | None:
        try:
            value = self.secrets.get(CMC_SECRET).values.get("api_key")
        except SecretNotAvailableError:
            return None
        return value if isinstance(value, str) and value else None

    # ------------------------------------------------------------------ sinks

    async def _write_one(self, capture: Capture) -> None:
        await asyncio.to_thread(self.store.append, capture)

    async def _write_many(self, captures: list[Capture]) -> None:
        await asyncio.to_thread(self.store.append_many, captures)

    async def _on_cmc_response(self, result: CmcResult) -> None:
        await self._write_one(result.capture)
        await self.meter.on_response(result)

    async def _route_health(self, job: RouteJob, at: datetime, outcome: Outcome, error: str | None) -> None:
        status = RouteStatus(
            route_key=job.route_key,
            sort_order=job.sort_order,
            route=f"#{job.route.id} {job.route.path}"
            + (f" ({job.route_key.split(':')[1]})" if ":" in job.route_key else ""),
            source="cmc",
            cadence_label=job.cadence_label,
            consumers=", ".join(job.route.consumers),
            credits_per_day=job.credits_per_day,
        )
        if outcome == "disabled":  # not on the plan: not recorded, and not a failing series either
            with contextlib.suppress(KeyError):
                UP.remove(job.route_key)
        else:
            UP.labels(route_key=job.route_key).set(1 if outcome == "ok" else 0)
        await asyncio.to_thread(self._health_tx, status, at, outcome == "ok", outcome, error)

    async def _binance_health(self, key: str, at: datetime, ok: bool, error: str | None = None) -> None:
        order, label, cadence, consumers, _secs = BINANCE_ROUTES[key]
        status = RouteStatus(key, order, f"{key} {label}", "binance", cadence, consumers, None)
        UP.labels(route_key=key).set(1 if ok else 0)
        await asyncio.to_thread(self._health_tx, status, at, ok, "ok" if ok else "failing", error)

    def _health_tx(
        self, status: RouteStatus, at: datetime, ok: bool, outcome: str, error: str | None
    ) -> None:
        with self.sessions.begin() as session:
            upsert_route_health(session, status, attempted_at=at, success=ok, status=outcome, error=error)

    async def _ws_health(
        self, name: str, status: str, gaps: int, since: datetime | None, last: datetime | None
    ) -> None:
        WS_GAPS.labels(connection=name).set(gaps)

        def tx() -> None:
            with self.sessions.begin() as session:
                upsert_ws_health(session, name, status, gaps, since, last)

        await asyncio.to_thread(tx)

    # ------------------------------------------------------------------ CMC jobs

    def schedule_cmc(self) -> None:
        for job_id in list(self.jobs):
            with contextlib.suppress(Exception):
                self.scheduler.remove_job(job_id)
        self.jobs = {f"cmc:{job.route_key}": job for job in build_jobs(self.cmc_routes())}
        now = utcnow()
        for job_id, job in self.jobs.items():
            seconds = job.cadence_s or 86_400
            self.scheduler.add_job(
                self._run_cmc,
                "interval",
                seconds=seconds,
                args=[job],
                id=job_id,
                next_run_time=now,
                max_instances=1,
                coalesce=True,
            )
        log.info("cmc jobs scheduled", extra={"jobs": len(self.jobs)})

    async def _run_cmc(self, job: RouteJob) -> None:
        started = time.monotonic()
        try:
            result = await self.runner.run(job)
        finally:
            ROUTE_LATENCY.labels(source="cmc", route=job.route_key).observe(time.monotonic() - started)
            if job.route.name in UNIVERSE_INPUT_ROUTES:
                self._universe_inputs_run.add(job.route.name)
        if result.key_blocked:
            self._key_blocked.add(f"cmc:{job.route_key}")
        else:
            self._key_blocked.discard(f"cmc:{job.route_key}")
        if job.route.name in UNIVERSE_INPUT_ROUTES:
            self._universe_after_inputs()

    async def cmc_key_watch(self) -> None:
        """A new active CMC key version runs every job the key could not serve at once (idempotent: the
        planners of `once` routes skip targets that already have a successful record)."""
        try:
            version = await asyncio.to_thread(self.secrets.active_version, CMC_SECRET)
        except SQLAlchemyError:
            log.warning("cmc key version read failed", exc_info=True)
            return
        previous, self._key_version = self._key_version, version
        if version is None or version == previous:
            return
        await self.key_info()  # the new key may belong to another plan: re-cap the request rate first
        now = utcnow()
        due = sorted(job_id for job_id in self._key_blocked if self.scheduler.get_job(job_id) is not None)
        for job_id in due:
            self.scheduler.modify_job(job_id, next_run_time=now)
        log.info("new cmc key version active", extra={"version": version, "jobs_brought_forward": due})

    async def key_info(self) -> bool:
        """Refresh `/v1/key/info` (plan quota and per-minute limit); False when it could not be read."""
        try:
            await self.meter.refresh_key_info(self.client)
        except CmcError as exc:
            log.warning("key info refresh failed", extra={"error": str(exc)})
            return False
        return True

    async def dex_events(self) -> None:
        """On-event DEX routes #24-26 for hot coins (new candidates and held positions), once per 24 h."""
        routes = {r.name: r for r in self.cmc_routes().routes if r.name in DEX_EVENT_ROUTES and r.enabled}
        if not routes:
            return
        universe = self.bn.universe()
        if universe is None:
            return
        try:
            refs = self.view.token_refs()
        except TargetsUnavailableError:
            return
        coins = [m.cmc_id for m in universe.members if m.binance_symbol in self._hot_symbols]
        for name, route in routes.items():
            job = self._dex_job_cache.get(name) or RouteJob(
                str(route.id), route, route.id * 10, None, "on event", lambda _v: []
            )
            self._dex_job_cache[name] = job
            specs = []
            for coin in coins:
                ref = refs.get(coin)
                if ref is None:
                    continue
                recent = self.pit.latest("cmc", name, utcnow(), key=str(coin), lookback=DEX_EVENT_COOLDOWN)
                if recent is None:
                    specs.append(dex_event_spec(route, ref))
            if specs:
                await self.runner.run_specs(job, specs)

    # ------------------------------------------------------------------ Binance jobs

    async def _binance_job(self, key: str, work: Callable[[], Awaitable[Any]]) -> None:
        at = utcnow()
        started = time.monotonic()
        try:
            try:
                await work()
            finally:
                ROUTE_LATENCY.labels(source="binance", route=key).observe(time.monotonic() - started)
        except BinanceBackoffError as exc:
            await self._binance_health(key, at, False, f"rate limited until {exc.until.isoformat()}")
        except (httpx.HTTPError, BinanceHttpError, UniverseInputsMissingError) as exc:
            await self._binance_health(key, at, False, f"{type(exc).__name__}: {exc}")
        else:
            await self._binance_health(key, at, True)

    async def b1(self) -> None:
        async def work() -> None:
            await self.bn.exchange_info()
            await self.bn.funding_info()

        await self._binance_job("B1", work)

    async def b2(self, backfill: bool = False) -> None:
        async def work() -> None:
            await self.bn.klines(backfill=backfill)
            await self.bn.mark_price_klines(backfill=backfill)

        await self._binance_job("B2", work)

    async def b7(self) -> None:
        await self._binance_job("B7", self.bn.funding_rate)

    async def b8(self) -> None:
        async def work() -> None:
            await self.bn.premium_index()
            await self.bn.open_interest()

        await self._binance_job("B8", work)

    async def b8_hist(self, backfill: bool = False) -> None:
        await self._binance_job("B8", lambda: self.bn.open_interest_hist(backfill=backfill))

    async def universe(self) -> None:
        """One build of today's universe (no-op when it exists); the first universe ever backfills B2/B8."""
        universe = self.bn.universe()
        if universe is not None and universe.date == utcnow().date():
            return
        first = universe is None
        await self._binance_job("U", self.bn.build_universe)
        if first and self.bn.universe() is not None:
            await self.b2(backfill=True)
            await self.b8_hist(backfill=True)
        await self.refresh_streams()

    def _has_universe(self, day: date) -> bool:
        universe = self.bn.universe()
        return universe is not None and universe.date == day

    def _universe_tries(self, day: date) -> int:
        attempts = self._universe_attempts
        return attempts[1] if attempts is not None and attempts[0] == day else 0

    def _schedule_universe(self, day: date, at: datetime) -> None:
        self.scheduler.add_job(
            self.universe_attempt,
            "date",
            run_date=at,
            args=[day],
            id=UNIVERSE_BUILD_JOB,
            replace_existing=True,
            max_instances=1,
        )

    def _universe_inputs_done(self) -> bool:
        """Every enabled input route (#11, #12) has run at least once in this process."""
        scheduled = {job.route.name for job in self.jobs.values()} & UNIVERSE_INPUT_ROUTES
        return scheduled <= self._universe_inputs_run

    def _universe_after_inputs(self) -> None:
        """Starts today's build once the input routes have run in this process and their records exist.

        Never races the keyless CMC routes #11/#12 at startup, and never spends build attempts before the
        lake holds their records; a day whose budget is already open or spent is left to its retries.
        """
        today = utcnow().date()
        if (
            self._universe_inputs_done()
            and self._universe_tries(today) == 0
            and not self._has_universe(today)
            and self.bn.universe_inputs_ready()
        ):
            self._schedule_universe(today, utcnow())

    async def universe_daily(self) -> None:
        """00:05 UTC (after B1): today's build, once this process has run the input routes."""
        if self._universe_inputs_done():
            await self.universe_attempt(utcnow().date())

    async def universe_attempt(self, day: date) -> None:
        """One build attempt for `day`; a failure schedules the next one (backoff + jitter) until the
        day's `universe_retry.max_attempts` are spent. Stops as soon as the day is over or built."""
        retry = self.binance_cfg().universe_retry
        if (
            day != utcnow().date()
            or self._has_universe(day)
            or self._universe_tries(day) >= retry.max_attempts
        ):
            return
        tries = self._universe_tries(day) + 1
        self._universe_attempts = (day, tries)
        try:
            await self.universe()
        except Exception:
            log.exception("universe build failed", extra={"date": day.isoformat(), "attempt": tries})
        if self._has_universe(day):
            return
        if tries >= retry.max_attempts:
            log.warning(
                "universe build attempts exhausted for the day",
                extra={"date": day.isoformat(), "attempts": tries},
            )
            return
        delay = universe_retry_delay(retry, tries, random.random())  # noqa: S311 - jitter, not security
        self._schedule_universe(day, utcnow() + timedelta(seconds=delay))

    async def refresh_streams(self) -> None:
        cfg = self.binance_cfg()
        symbols = self.bn.members()
        if symbols:
            await self.ws.ensure_family(
                "market", cfg.live.ws_market, ws_market_streams(symbols, cfg.kline_intervals)
            )
            await self.ws.ensure_family(
                "public", cfg.live.ws_public, ws_depth_streams(symbols, cfg.depth_levels)
            )
        try:
            hot = await self.hot.symbols()
        except Exception:
            log.exception("hot set read failed")
            return
        entered = hot - self._hot_symbols
        self._hot_symbols = hot
        changed = await self.ws.ensure("public-hot", cfg.live.ws_public, ws_hot_streams(sorted(hot)))
        if changed and entered:
            # rebuild each new book from a REST snapshot + the diffs that follow it (B9)
            await self._binance_job("B9", lambda: self.bn.depth_snapshot(sorted(entered)))
            await self.dex_events()

    # ------------------------------------------------------------------ lake maintenance

    async def compact(self) -> None:
        done = await asyncio.to_thread(self.store.compact_closed)
        if done:
            log.info("partitions compacted", extra={"partitions": len(done)})

    async def daily_root(self) -> None:
        day = (utcnow() - timedelta(days=1)).date()
        await self.compact()
        manifest = await asyncio.to_thread(self.store.daily_root, day)
        retention = self.static.settings.data.raw_depth_retention_days
        await asyncio.to_thread(self.store.expire_raw, RAW_DEPTH_ROUTES, day - timedelta(days=retention - 1))
        await self.lake_stats(merkle=(day, str(manifest["merkle_root"])))

    async def lake_stats(self, merkle: tuple[Any, str] | None = None) -> None:
        size = await asyncio.to_thread(self.store.lake_bytes)
        fraction = _disk_fraction(self.store)
        DISK.set(fraction)
        if fraction >= LAKE_DISK_ALERT:
            log.warning("lake disk alert", extra={"alert": "lake_disk", "fraction": round(fraction, 4)})

        def tx() -> None:
            with self.sessions.begin() as session:
                record_lake_stats(
                    session,
                    as_of=utcnow(),
                    lake_bytes=size,
                    disk_used_fraction=fraction,
                    retention_days=self.static.settings.data.raw_depth_retention_days,
                    merkle_date=merkle[0] if merkle else None,
                    merkle_root=merkle[1] if merkle else None,
                )

        await asyncio.to_thread(tx)

    async def lateness(self) -> None:
        cadences = {job.route_key: job.cadence_s for job in self.jobs.values() if job.cadence_s}
        cadences |= {key: spec[4] for key, spec in BINANCE_ROUTES.items() if spec[4]}

        def tx() -> None:
            with self.sessions.begin() as session:
                mark_late_routes(session, utcnow(), cadences)

        await asyncio.to_thread(tx)

    async def config_boundary(self) -> None:
        """Apply new CMC/Binance config versions between jobs (never mid-call)."""
        await self.watcher.poll_once()
        applied = self.watcher.apply_pending()
        if Section.CMC in applied:
            self.schedule_cmc()
        if Section.BINANCE in applied:
            await self.refresh_streams()

    # ------------------------------------------------------------------ CMC WebSocket

    def cmc_ws(self) -> CmcWsClient | None:
        route = next(r for r in self.cmc_routes().routes if r.transport == "ws")
        if not route.enabled:
            return None
        held: list[int] = []

        async def refresh() -> None:
            held[:] = await self.hot.held_cmc_ids()

        self.scheduler.add_job(refresh, "interval", seconds=30, id="cmc_ws_targets", next_run_time=utcnow())

        async def credits(amount: int, at: datetime) -> None:
            await self.meter.record_stream_credits(CMC_WS_ROUTE, amount, at)

        return CmcWsClient(
            url=self.static.settings.cmc.ws_url,
            api_key=self._cmc_key,
            crypto_ids=lambda: held,
            write=self._write_many,
            on_credits=credits,
            max_coins=int(route.params.get("coins", route.params["max_coins"])),
        )

    # ------------------------------------------------------------------ run

    async def run(self, stop: asyncio.Event) -> None:
        await self.watcher.start()
        s = self.scheduler
        self._key_version = await asyncio.to_thread(self.secrets.active_version, CMC_SECRET)
        # The key's real per-minute limit must be in force before the first CMC job becomes due: every job
        # starts at once, and the configured rate is only a ceiling (a smaller plan answers 1008 otherwise).
        if not await self.key_info():
            self.client.cap_rate_to_plan(FALLBACK_RATE_PER_MIN)
            log.warning(
                "cmc key info unavailable at start; request rate capped conservatively",
                extra={"per_minute": self.client.rate_per_minute},
            )
        s.add_job(
            self.key_info,
            "interval",
            hours=1,
            id="key_info",
            next_run_time=utcnow() + timedelta(hours=1),
            max_instances=1,
        )
        self.schedule_cmc()
        s.add_job(
            self.cmc_key_watch, "interval", seconds=15, id="cmc_key_watch", max_instances=1, coalesce=True
        )
        s.add_job(self.b1, "cron", hour=0, minute=2, id="b1", next_run_time=utcnow(), max_instances=1)
        # daily, after B1; the first build of the process follows CMC #11/#12 (`_universe_after_inputs`)
        s.add_job(self.universe_daily, "cron", hour=0, minute=5, id="universe", max_instances=1)
        self._universe_after_inputs()  # starts at once only when #11 and #12 are both disabled
        s.add_job(
            self.b8, "interval", minutes=5, id="b8", next_run_time=utcnow(), max_instances=1, coalesce=True
        )
        s.add_job(self.b8_hist, "interval", hours=1, id="b8_hist", max_instances=1, coalesce=True)
        s.add_job(self.b2, "interval", hours=1, id="b2", max_instances=1, coalesce=True)
        s.add_job(self.b7, "cron", hour=0, minute=20, id="b7", max_instances=1)
        s.add_job(self.refresh_streams, "interval", minutes=1, id="streams", max_instances=1, coalesce=True)
        s.add_job(self.compact, "interval", minutes=1, id="compact", max_instances=1, coalesce=True)
        s.add_job(self.daily_root, "cron", hour=0, minute=15, id="daily_root", max_instances=1)
        s.add_job(
            self.lake_stats, "interval", minutes=15, id="lake_stats", next_run_time=utcnow(), max_instances=1
        )
        s.add_job(self.lateness, "interval", minutes=1, id="lateness", max_instances=1, coalesce=True)
        s.add_job(self.config_boundary, "interval", seconds=15, id="config", max_instances=1, coalesce=True)
        s.start()
        owner = OwnerWorker(
            scope="data",
            redis=self.redis,
            session_factory=self.sessions,
            private_key=load_private_key("data"),
            checks=DATA_SECRET_CHECKS,
            consumer=f"{SERVICE}-1",
        )
        tasks = [asyncio.create_task(owner.run(stop))]
        ws_client = self.cmc_ws()
        if ws_client is not None:
            tasks.append(asyncio.create_task(ws_client.run(stop)))
        try:
            await stop.wait()
        finally:
            s.shutdown(wait=False)
            await self.ws.stop_all()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self.compact()


def _disk_fraction(store: RawStore) -> float:
    usage = shutil.disk_usage(store.lake_root)
    return usage.used / usage.total if usage.total else 0.0


async def amain() -> None:
    static = static_config()
    sessions = make_session_factory(make_engine())
    redis: Redis = connect_redis(require_env_value("HDT_REDIS_URL"))
    data = static.settings.data
    for root in (data.staging_root, data.lake_root):
        Path(root).mkdir(parents=True, exist_ok=True)
    store = RawStore(Path(data.staging_root), Path(data.lake_root))
    pit = PitQuery(Path(data.staging_root), Path(data.lake_root))
    start_http_server(int(read_env_value("HDT_METRICS_PORT") or "9102"))
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)
    async with httpx.AsyncClient(timeout=static.settings.cmc.request_timeout_s) as http:
        recorder = Recorder(
            static=static,
            sessions=sessions,
            redis=redis,
            secrets=SecretLoader.from_env("data", sessions),
            http=http,
            store=store,
            pit=pit,
        )
        try:
            await recorder.run(stop)
        finally:
            await redis.aclose()


def main() -> None:
    configure_logging(SERVICE)
    asyncio.run(amain())


if __name__ == "__main__":
    main()
