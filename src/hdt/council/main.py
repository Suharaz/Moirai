"""Council service: `python -m hdt.council.main`.

Env: HDT_PG_DSN (role hdt_council), HDT_REDIS_URL (ACL user council), HDT_SCOPE_PRIVATE_KEY (llm scope
owner key), HDT_METRICS_PORT (default 9464), optional HDT_FETCHER_URL (default `http://fetcher:8080`); each
also as `<NAME>_FILE`. The lake is mounted read-only.

Tasks:
- the phase 03 scanner on its fixed as_of grid (APScheduler), publishing `candidates`;
- the trigger (`council` group on `candidates`): admits events (spacing, scoring windows, config pin);
- the meeting worker: runs admitted events through the LangGraph meeting (checkpointed in Postgres,
  `thread_id = event_id`, so a restart resumes an interrupted meeting);
- the decision outbox relay (`decisions`);
- the config watcher (`config-council`): versions apply at event boundaries; a models change re-runs the
  structured-output self-test of the six agent roles;
- the llm scope vault owner (`OwnerWorker`, console secret and model tests);
- Prometheus metrics on 9464 (`hdt_council_*`, `hdt_fetch_blocked_total`).
SIGINT / SIGTERM drain every loop and close connections. A task that ends on its own (it died) stops the
service with exit code 1, so the orchestrator restarts it instead of leaving a half-running council.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Final

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from redis.asyncio import Redis

from hdt.agents.llm_router import LlmRouter
from hdt.agents.model_test import RoleHealth, model_test_runner
from hdt.agents.runner import LlmAgentRunner, PgPins
from hdt.agents.versions import AgentVersionBook
from hdt.contracts.candidate import Candidate
from hdt.contracts.common import Account
from hdt.contracts.streams import Stream
from hdt.core.config import StaticConfig, read_env_value, require_env_value, static_config
from hdt.core.logging import configure_logging
from hdt.core.service import every, install_stop_signals
from hdt.core.streams import StreamConsumer, connect_redis
from hdt.council.adapters import (
    EventQuantCore,
    LakeUniverse,
    PgPacketStore,
    PgScannerLog,
    PinnedSettings,
    QuantCandidateSets,
    StreamHeldPositions,
)
from hdt.council.checkpoint import open_checkpointer
from hdt.council.decision_card import DecisionOutbox, PgDecisionStore
from hdt.council.graph import CouncilGraph, CouncilServices
from hdt.council.trigger import CouncilTrigger, MeetingWorker
from hdt.db.session import make_engine, make_session_factory
from hdt.lake.pit_query import PitQuery
from hdt.lake.raw_store import RawStore
from hdt.llm.key_checks import LLM_SECRET_CHECKS
from hdt.memory.store import open_postgres_store
from hdt.news.assess import PgNewsAssessor
from hdt.news.fetcher_client import DEFAULT_BASE_URL, HttpFetcherClient
from hdt.news.market import LakeMarketView
from hdt.news.official import PgOfficialSource
from hdt.news.source_lookup import PgSourceLookup
from hdt.news.store import PgNewsIndex
from hdt.news.unlocks import PgUnlockCalendar
from hdt.ops.metrics import serve_metrics
from hdt.quant.quant_core import QuantCore
from hdt.quant.scanner import Scanner, register
from hdt.risk.pinned import pinned_config
from hdt.scoring.params import PgParamsSource
from hdt.scoring.shadow import MemoryShadowLessons, PgShadowForecastSink
from hdt.settings.events import ConfigWatcher
from hdt.settings.schemas import Section
from hdt.tools.live import build_live_registry
from hdt.vault.owner import OwnerWorker
from hdt.vault.scopes import load_private_key

log = logging.getLogger(__name__)

SERVICE: Final[str] = "council"
CONSUMER: Final[str] = "council-1"
TRIGGER_GROUP: Final[str] = "council"
MEETING_POLL_S: Final[float] = 1.0
RELAY_INTERVAL_S: Final[float] = 1.0
CONFIG_POLL_S: Final[float] = 1.0


def _loop(
    stop: asyncio.Event, interval_s: float, fn: Callable[[], Awaitable[object]], what: str
) -> asyncio.Task[None]:
    return asyncio.create_task(every(stop, interval_s, fn, what), name=what)


async def supervise(stop: asyncio.Event, tasks: list[asyncio.Task[None]]) -> bool:
    """Wait for `stop` or for any task to end first; set `stop` and drain every task. True: a task ended
    before `stop` was set (it died or returned), so the caller exits non-zero."""
    stopper = asyncio.create_task(stop.wait(), name="stop")
    done, _pending = await asyncio.wait([*tasks, stopper], return_when=asyncio.FIRST_COMPLETED)
    died = [] if stop.is_set() else [t for t in done if t is not stopper]
    for task in died:
        exc = task.exception() if not task.cancelled() else None
        log.error("task %s ended while the service was running", task.get_name(), exc_info=exc)
    stop.set()
    await asyncio.gather(*tasks, stopper, return_exceptions=True)
    return bool(died)


async def amain() -> None:
    static: StaticConfig = static_config()
    dsn = require_env_value("HDT_PG_DSN")
    engine = make_engine(dsn)
    sessions = make_session_factory(engine)
    redis: Redis = connect_redis(require_env_value("HDT_REDIS_URL"))
    data = static.settings.data
    pit = PitQuery(Path(data.staging_root), Path(data.lake_root))
    raw_store = RawStore(Path(data.staging_root), Path(data.lake_root))
    streams = static.settings.streams
    serve_metrics(SERVICE)

    stop = asyncio.Event()
    install_stop_signals(stop)

    watcher = ConfigWatcher(
        redis=redis,
        session_factory=sessions,
        service=SERVICE,
        consumer=CONSUMER,
        block_ms=streams.block_ms,
        reclaim_idle_ms=streams.reclaim_idle_ms,
    )
    await watcher.start()

    def running_account() -> Account:
        return pinned_config(watcher.pin(), static).mode

    core = QuantCore(pit, engine, static=static)
    event_core = EventQuantCore(core)
    universe = LakeUniverse(pit)
    settings = PinnedSettings(sessions, static)
    fetcher = HttpFetcherClient(read_env_value("HDT_FETCHER_URL") or DEFAULT_BASE_URL)
    llm = LlmRouter.from_env("live", sessions)
    health = RoleHealth(llm, sessions, service=SERVICE)
    scheduler = AsyncIOScheduler(timezone="UTC")
    tasks: list[asyncio.Task[None]] = []
    try:
        with open_postgres_store(dsn) as memory, open_checkpointer(dsn) as saver:
            news = PgNewsIndex(engine)
            registry = build_live_registry(
                pit=pit,
                raw_store=raw_store,
                stale_s=data.stale_s,
                news_sources=static.news_sources,
                quant_core=event_core,
                news=news,
                fetcher=fetcher,
                official=PgOfficialSource(engine),
                unlocks=PgUnlockCalendar(engine),
                memory=memory,
            )
            runner = LlmAgentRunner(
                llm=llm,
                registry=registry,
                pins=PgPins(sessions),
                memory=memory,
                versions=AgentVersionBook(sessions, write=True, author=SERVICE),
                health=health,
                news=PgNewsAssessor(engine, LakeMarketView(pit, features=core.features, static=static)),
                static=static,
            )
            store = PgDecisionStore(engine, shadow_sink=PgShadowForecastSink(), memory=memory)
            graph = CouncilGraph(
                CouncilServices(
                    runner=runner,
                    packets=PgPacketStore(engine),
                    candidate_sets=QuantCandidateSets(core),
                    sources=PgSourceLookup(engine, pit, static.news_sources),
                    params=PgParamsSource(engine, council=static.council),
                    held=StreamHeldPositions(redis, universe, running_account),
                    universe=universe,
                    store=store,
                    settings=settings,
                    mode="live",
                    lessons=MemoryShadowLessons(memory),
                    bind_pins=event_core.bind,
                )
            ).compile(saver)
            worker = MeetingWorker(engine, graph, on_done=runner.close_event, checkpoints=saver)

            trigger = CouncilTrigger(
                engine,
                pin=lambda: watcher.pin().version_ids(),
                council=settings.council,
                scanner_log=PgScannerLog(engine),
            )
            outbox = DecisionOutbox(
                engine=engine, redis=redis, maxlen=streams.maxlen.get(Stream.DECISIONS.value)
            )

            async def on_candidate(msg_id: str, candidate: Candidate) -> bool:
                watcher.apply_pending()
                await asyncio.to_thread(trigger.admit, candidate)
                return True

            async def watch_config() -> None:
                await watcher.poll_once()
                if Section.MODELS in watcher.pending:
                    applied = watcher.apply_pending()
                    if Section.MODELS in applied:
                        await health.check_all(watcher.pin().models())

            candidates = StreamConsumer(
                redis=redis,
                stream=Stream.CANDIDATES,
                group=TRIGGER_GROUP,
                consumer=CONSUMER,
                model=Candidate,
                handler=on_candidate,
                batch_size=streams.batch_size,
                block_ms=streams.block_ms,
                reclaim_idle_ms=streams.reclaim_idle_ms,
                start_id="$",
            )
            try:
                await health.check_all(watcher.pin().models())
            except LookupError:
                log.warning("models section has no active version; agent roles stay closed until one exists")
            register(scheduler, Scanner(pit, engine, redis, static=static))
            scheduler.start()
            owner = OwnerWorker(
                scope="llm",
                redis=redis,
                session_factory=sessions,
                private_key=load_private_key("llm"),
                checks=LLM_SECRET_CHECKS,
                consumer=CONSUMER,
                block_ms=streams.block_ms,
                reclaim_idle_ms=streams.reclaim_idle_ms,
                result_maxlen=streams.maxlen.get(Stream.SECRET_TEST_RESULT.value),
                model_test_runner=model_test_runner(llm, health),
            )
            tasks = [
                asyncio.create_task(candidates.run(stop), name="trigger"),
                asyncio.create_task(owner.run(stop), name="vault-owner"),
                _loop(stop, MEETING_POLL_S, worker.run_pending, "meetings"),
                _loop(stop, RELAY_INTERVAL_S, outbox.relay_once, "decision relay"),
                _loop(stop, CONFIG_POLL_S, watch_config, "config watcher"),
            ]
            log.info("council service started")
            died = await supervise(stop, tasks)
            if died:
                raise SystemExit(1)
    finally:
        stop.set()
        for task in tasks:
            task.cancel()
        if scheduler.running:
            scheduler.shutdown(wait=False)
        await fetcher.aclose()
        await redis.aclose()
        engine.dispose()
        log.info("council service stopped")


def main() -> None:
    configure_logging(SERVICE)
    asyncio.run(amain())


if __name__ == "__main__":
    main()
