"""News worker (`python -m hdt.news.main`, compose service `news`, Postgres role `hdt_news`).

Loops:
- every `ingest.poll_s`: poll the enabled sources (`hdt.news.ingest`), then judge up to
  `ingest.process_batch` unjudged canonical items of the processing window (extractor -> quote check ->
  two judges -> code tier / `check_official` -> known events), one insert-only verdict each;
- every `attention.cadence_s`: attention signals of the universe coins (`news_attention`), then the
  assessment of the attention coins and of the coins with new evidence; a directional mode (fade hype,
  fade panic, ride) becomes a HOLLOW_HYPE `Candidate` on stream `candidates` (at most
  `candidates.max_per_day`, one per coin per `candidates.min_spacing_h`), recorded in `news_candidates`
  before the XADD and marked published after it;
- the config watcher (`config-news`, section `models`): the pinned extractor / judge models are applied
  between cycles only; every judging call is gated by the roles' structured-output self-test
  (`RoleHealth`: a closed role judges nothing, its items wait).
The models are OpenRouter slugs chosen on the console; while any of the three roles is not configured no
item is judged (they wait, the veto scan soft-vetoes bad-catalyst keyword hits meanwhile). An item that
cannot be processed gets its own `llm_failed` row (`hdt.news.verdict`) and never stops the cycle.
"""

from __future__ import annotations

import asyncio
import logging
import socket
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Final

import sqlalchemy as sa
from redis.asyncio import Redis
from sqlalchemy.orm import Session, sessionmaker

from hdt.agents.llm_router import LlmRouter
from hdt.agents.llm_types import StructuredLlm
from hdt.agents.model_test import RoleHealth
from hdt.contracts.candidate import Candidate
from hdt.contracts.common import CandidateSource, TargetType
from hdt.contracts.streams import Stream
from hdt.core.alerts import alert
from hdt.core.clock import ensure_utc, utcnow
from hdt.core.config import StaticConfig, require_env_value, scanner_config, static_config
from hdt.core.logging import configure_logging
from hdt.core.service import every, install_stop_signals
from hdt.core.streams import connect_redis, publish
from hdt.db.session import make_engine, make_session_factory
from hdt.lake.pit_query import PitQuery
from hdt.memory.episodic import EpisodicWriter
from hdt.memory.store import open_postgres_store
from hdt.news.assess import PgNewsAssessor
from hdt.news.attention import LakeAttention, article_window, attention_for
from hdt.news.coins import CoinDirectory
from hdt.news.config import NewsFile, news_config
from hdt.news.ingest import Ingestor
from hdt.news.market import LakeMarketView
from hdt.news.metrics import CANDIDATES_PUBLISHED, VERDICTS
from hdt.news.runtime import official_evidence, pinned_roles
from hdt.news.sources import SourceHttp
from hdt.news.store import (
    ATTENTION,
    CANDIDATES,
    StoredCandidate,
    attention_coins,
    candidates_since,
    insert_rows,
    mark_candidate_published,
    news_counts,
    pending_items,
    recently_judged_coins,
    unpublished_candidates,
)
from hdt.news.verdict import ItemJudge, KnownEventSink, RoleGate, VerdictJob, store_verdict
from hdt.ops.metrics import serve_metrics
from hdt.settings.events import ConfigWatcher
from hdt.settings.schemas import Section
from hdt.tools.ports import NewsAssessment
from hdt.vault.loader import SecretLoader

log = logging.getLogger(__name__)

SERVICE: Final[str] = "news"
CANDIDATE_MODES: Final[frozenset[str]] = frozenset({"fade_hype", "fade_panic", "ride"})
SECRET_NAME: Final[str] = "news_feed"  # noqa: S105 - vault item name, not a credential


def candidate_score(assessment: NewsAssessment) -> float:
    """Strength of the news lean: distance of the regime-rule probability from 0.5."""
    return round(abs(assessment.p_model - 0.5), 4)


def candidate_allowed(
    coin_id: int,
    as_of: datetime,
    recent: list[tuple[int, datetime, datetime | None]],
    config: NewsFile,
) -> bool:
    """Daily cap over every coin and minimum spacing per coin (`recent` covers the last 24 h)."""
    day = [r for r in recent if r[1] > as_of - timedelta(days=1)]
    if len(day) >= config.candidates.max_per_day:
        return False
    spacing = timedelta(hours=config.candidates.min_spacing_h)
    return not any(c == coin_id and as_of - at < spacing for c, at, _ in recent)


async def publish_candidate(
    engine: sa.Engine, redis: Redis, stored: StoredCandidate, *, maxlen: int | None
) -> None:
    """XADD a recorded HOLLOW_HYPE candidate, then mark its `news_candidates` row published.

    The `Candidate` is rebuilt from the recorded row alone (target type and label spec included), so a
    republish after a restart with another config sends the same payload."""
    candidate = Candidate(
        coin_id=stored.coin_id,
        as_of=stored.as_of,
        source=CandidateSource.HOLLOW_HYPE,
        score=stored.score,
        rule_version=stored.rule_version,
        target_type=stored.target_type,
        label_spec_version=stored.label_spec_version,
    )
    await publish(redis, Stream.CANDIDATES, candidate, maxlen=maxlen)
    with engine.begin() as conn:
        mark_candidate_published(conn, stored.coin_id, stored.as_of, utcnow())
    CANDIDATES_PUBLISHED.labels(mode=stored.mode).inc()
    log.info("hollow hype candidate", extra={"coin_id": stored.coin_id, "mode": stored.mode})


async def republish_unsent(engine: sa.Engine, redis: Redis, *, since: datetime, maxlen: int | None) -> int:
    """Publish the candidates recorded since `since` whose cycle stopped between the insert and the XADD
    (at least once); an older unsent one stays unpublished, the council would see a stale event."""
    with engine.connect() as conn:
        unsent = unpublished_candidates(conn, since)
    for stored in unsent:
        await publish_candidate(engine, redis, stored, maxlen=maxlen)
    return len(unsent)


class NewsWorker:
    def __init__(
        self,
        *,
        engine: sa.Engine,
        sessions: sessionmaker[Session],
        redis: Redis,
        pit: PitQuery,
        llm: StructuredLlm,
        watcher: ConfigWatcher,
        known_events: KnownEventSink | None,
        static: StaticConfig,
        config: NewsFile,
        health: RoleGate | None = None,
    ) -> None:
        self.engine = engine
        self.sessions = sessions
        self.redis = redis
        self.pit = pit
        self.llm = llm
        self.watcher = watcher
        self.known_events = known_events
        self.static = static
        self.config = config
        self.health = health
        self.sources = static.news_sources
        self.http = SourceHttp(
            timeout_s=config.ingest.request_timeout_s, max_bytes=config.ingest.max_response_bytes
        )
        loader = SecretLoader.from_env("data", sessions)
        self.ingestor = Ingestor(
            engine,
            self.http,
            config=config,
            sources=self.sources,
            api_key=lambda: str(loader.get(SECRET_NAME).values["api_key"]),
        )
        self.assessor = PgNewsAssessor(
            engine, LakeMarketView(pit, static=static), config=config, sources=self.sources
        )
        self._empty_parse_alerted: set[str] = set()

    # ------------------------------------------------------------------ ingest + judge

    async def ingest_cycle(self) -> None:
        now = utcnow()
        coins = await asyncio.to_thread(CoinDirectory.from_lake, self.pit, now)
        if not len(coins):
            log.warning("no point-in-time universe: news items cannot be mapped to coins yet")
            return
        counts = await self.ingestor.poll(coins)
        for result in counts.results:
            if not result.ok and result.error and "parsed to no articles" in result.error:
                self._alert_parse(result.source_key, result.error)
        await self.judge_cycle(coins)

    def _alert_parse(self, source: str, error: str) -> None:
        with self.sessions() as session, session.begin():
            alert(
                session,
                kind="integrity_error",
                severity="warning",
                title=f"news source {source} parsed to nothing",
                detail=error[:500],
                service=SERVICE,
                episode=f"news_parse:{source}",
            )

    async def judge_cycle(self, coins: CoinDirectory) -> None:
        self.watcher.apply_pending()
        roles = pinned_roles(self.watcher.pin())
        if roles is None:
            log.warning("news models are not configured: items wait unjudged")
            return
        judge = ItemJudge(
            self.llm,
            roles,
            config=self.config,
            sources=self.sources,
            known_events=self.known_events,
            health=self.health,
        )
        now = utcnow()
        since = now - timedelta(hours=self.config.ingest.process_lookback_h)
        with self.engine.connect() as conn:
            items = pending_items(conn, self.config.rule_version, since, self.config.ingest.process_batch)
            evidence = official_evidence(conn, self.pit, now, self.config)
        for item in items:
            refs = tuple(c for c in (coins.get(cid) for cid in item.coin_ids) if c is not None)
            if not refs:
                continue
            processed_at = utcnow()
            job = VerdictJob(item, refs, evidence, processed_at, "news")
            row = await judge.verdict(job)
            if row is None:
                break  # models unavailable: stop this cycle, retry the rest next time
            if store_verdict(self.engine, row, job, self.config.rule_version):
                VERDICTS.labels(processor="news", status=row["status"]).inc()

    # ------------------------------------------------------------------ attention + candidates

    async def attention_cycle(self) -> None:
        as_of = utcnow()
        coins = await asyncio.to_thread(CoinDirectory.from_lake, self.pit, as_of)
        if not len(coins):
            return
        cfg = self.config.attention
        lake = await asyncio.to_thread(LakeAttention.read, self.pit, as_of, cfg)
        with self.engine.connect() as conn:
            times = news_counts(conn, list(coins.coins), article_window(as_of, cfg), as_of)
        rows: list[dict[str, Any]] = []
        for coin_id in coins.coins:
            att = attention_for(coin_id, as_of, lake, times.get(coin_id, []), cfg)
            if att.attention or att.trending_rank is not None or att.news_count or att.new_listing:
                rows.append(att.row(self.config.rule_version, utcnow()))
        with self.engine.begin() as conn:
            insert_rows(conn, ATTENTION, rows)
        await self.candidate_cycle(as_of)

    async def candidate_cycle(self, as_of: datetime) -> None:
        cfg = self.config
        cadence = timedelta(seconds=2 * cfg.attention.cadence_s)
        with self.engine.connect() as conn:
            watched = set(attention_coins(conn, as_of, cadence))
            watched |= recently_judged_coins(conn, cfg.rule_version, as_of - cadence, as_of)
            recent = candidates_since(conn, as_of - timedelta(days=1))
        maxlen = self.static.settings.streams.maxlen.get(Stream.CANDIDATES.value)
        await republish_unsent(self.engine, self.redis, since=as_of - cadence, maxlen=maxlen)
        for coin_id in sorted(watched):
            assessment = await asyncio.to_thread(self.assessor.assess, coin_id, as_of)
            if assessment.mode not in CANDIDATE_MODES or not candidate_allowed(coin_id, as_of, recent, cfg):
                continue
            score = candidate_score(assessment)
            row = {
                "coin_id": coin_id,
                "as_of": ensure_utc(as_of),
                "mode": assessment.mode,
                "score": score,
                "rule_version": cfg.rule_version,
                "target_type": TargetType.RAW_12H.value,
                "label_spec_version": scanner_config().labels.label_spec_version,
                "evidence": assessment.model_dump(mode="json"),
                "published_at": None,
            }
            with self.engine.begin() as conn:
                if not insert_rows(conn, CANDIDATES, [row]):
                    continue
            await publish_candidate(self.engine, self.redis, StoredCandidate.from_row(row), maxlen=maxlen)
            recent.append((coin_id, ensure_utc(as_of), utcnow()))

    # ------------------------------------------------------------------ run

    async def run(self, stop: asyncio.Event) -> None:
        await self.watcher.start()
        tasks = [
            asyncio.create_task(every(stop, 1.0, self.watcher.poll_once, "config watcher"), name="config"),
            asyncio.create_task(
                every(stop, float(self.config.ingest.poll_s), self.ingest_cycle, "news ingest"), name="ingest"
            ),
            asyncio.create_task(
                every(stop, float(self.config.attention.cadence_s), self.attention_cycle, "news attention"),
                name="attention",
            ),
        ]
        try:
            await stop.wait()
        finally:
            stop.set()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self.http.aclose()
        log.info("news worker stopped")


async def amain() -> int:
    static = static_config()
    config = news_config()
    engine = make_engine()
    sessions = make_session_factory(engine)
    redis: Redis = connect_redis(require_env_value("HDT_REDIS_URL"))
    data = static.settings.data
    pit = PitQuery(Path(data.staging_root), Path(data.lake_root))
    serve_metrics(SERVICE)
    stop = asyncio.Event()
    install_stop_signals(stop)
    llm = LlmRouter.from_env("live", sessions)
    # The self-test verdicts of the news roles gate every judging call (a failed test closes the role).
    health = RoleHealth(llm, sessions, service=SERVICE)
    watcher = ConfigWatcher(
        redis=redis,
        session_factory=sessions,
        service=SERVICE,
        consumer=socket.gethostname(),
        sections=(Section.MODELS,),
    )
    try:
        with open_postgres_store(require_env_value("HDT_PG_DSN")) as memory:
            worker = NewsWorker(
                engine=engine,
                sessions=sessions,
                redis=redis,
                pit=pit,
                llm=llm,
                watcher=watcher,
                known_events=EpisodicWriter(memory).record_known_event,
                static=static,
                config=config,
                health=health,
            )
            log.info("news worker starting", extra={"rule_version": config.rule_version})
            await worker.run(stop)
    finally:
        await llm.aclose()
        await redis.aclose()
        engine.dispose()
    return 0


def main() -> None:
    configure_logging(SERVICE)
    sys.exit(asyncio.run(amain()))


if __name__ == "__main__":
    main()
