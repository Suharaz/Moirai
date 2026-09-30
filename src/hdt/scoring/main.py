"""Scorer service entry point (`python -m hdt.scoring.main`, compose service `scorer`).

Bootstrap: HDT_PG_DSN (role `hdt_scorer`), HDT_REDIS_URL (ACL user `scorer`, consumes `config_changed`),
HDT_SCOPE_PRIVATE_KEY_FILE (reads the `llm` scope key for the reflection role), the read-only lake.
Every `scoring.yaml` `interval_s` the cycle of `hdt.scoring.service.Scorer` runs; the process exports
`hdt_agent_weight{agent,target_type,param}` on the metrics port.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

from redis.asyncio import Redis

from hdt.agents.llm_router import LlmRouter
from hdt.agents.model_test import RoleHealth
from hdt.core.config import CouncilFile, require_env_value, scanner_config, scoring_config, static_config
from hdt.core.logging import configure_logging
from hdt.core.service import every, install_stop_signals
from hdt.core.streams import connect_redis
from hdt.db.session import make_engine, make_session_factory
from hdt.lake.pit_query import PitQuery
from hdt.memory.store import open_postgres_store
from hdt.ops.metrics import serve_metrics
from hdt.scoring.service import Scorer
from hdt.settings.events import ConfigWatcher
from hdt.settings.schemas import ModelsSection, Section
from hdt.settings.versions import SectionNotConfiguredError

SERVICE = "scorer"
log = logging.getLogger(__name__)


async def amain() -> int:
    static = static_config()
    scoring = scoring_config()
    dsn = require_env_value("HDT_PG_DSN")
    engine = make_engine(dsn)
    sessions = make_session_factory(engine)
    redis: Redis = connect_redis(require_env_value("HDT_REDIS_URL"))
    data = static.settings.data
    pit = PitQuery(Path(data.staging_root), Path(data.lake_root))
    watcher = ConfigWatcher(
        redis=redis,
        session_factory=sessions,
        service=SERVICE,
        consumer=f"{SERVICE}-1",
        sections=(Section.MODELS, Section.COUNCIL),
        block_ms=min(static.settings.streams.block_ms, 1000),
        reclaim_idle_ms=static.settings.streams.reclaim_idle_ms,
    )

    def council() -> CouncilFile:
        try:
            return watcher.pin().council(static)
        except SectionNotConfiguredError:
            return static.council

    def models() -> ModelsSection | None:
        try:
            return watcher.pin().models()
        except SectionNotConfiguredError:
            return None

    router = LlmRouter.from_env("live", sessions)
    # Gates the reflection role on its structured-output self-test: a closed role raises
    # LlmRoleClosedError (an LlmUnavailableError), so reflection fails closed and retries next cycle.
    health = RoleHealth(router, sessions, service=SERVICE)

    async def check_reflection_role() -> None:
        """Never blocks the scoring cycle: a check that fails to run is logged and retried next cycle."""
        section = models()
        config = section.roles.get("reflection") if section is not None else None
        if config is None:
            return
        try:
            reason = await health.closed_reason("reflection", config)
        except Exception:
            log.exception("reflection role self-test failed to run")
            return
        if reason is not None:
            log.warning("reflection role closed by its self-test", extra={"reason": reason})

    stop = asyncio.Event()
    install_stop_signals(stop)
    try:
        await watcher.start()
        with open_postgres_store(dsn) as memory:
            scorer = Scorer(
                sessions=sessions,
                static=static,
                scanner=scanner_config(),
                scoring=scoring,
                pit=pit,
                memory=memory,
                council=council,
                models=models,
                llm=router,
            )

            async def poll_config() -> None:
                await watcher.poll_once()

            async def cycle() -> None:
                watcher.apply_pending()
                await check_reflection_role()
                await scorer.cycle()

            tasks = [
                asyncio.create_task(every(stop, 1.0, poll_config, "config watcher"), name="config"),
                asyncio.create_task(
                    every(stop, float(scoring.interval_s), cycle, "scoring cycle"), name="cycle"
                ),
            ]
            await stop.wait()
            await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        await router.aclose()
        await redis.aclose()
        engine.dispose()
    log.info("scorer stopped")
    return 0


def main() -> None:
    configure_logging(SERVICE)
    serve_metrics(SERVICE)
    sys.exit(asyncio.run(amain()))


if __name__ == "__main__":
    main()
