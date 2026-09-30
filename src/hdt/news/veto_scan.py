"""Independent veto scan (`python -m hdt.news.veto_scan`, compose service `veto-scan`, role `hdt_veto_scan`).

Every `risk.veto_scan_interval_s`, outside any agent budget and regardless of any gate:
1. judges itself up to `veto.judge_pending_max` unjudged canonical items of the window whose title or
   summary matches a bad-catalyst keyword (same extractor / judges / quote check as the news worker,
   processor `veto_scan`, no known-event write); items still unjudged count as soft vetoes, and so do
   keyword items whose verdict failed or that only the extractor saw (`unconfirmed_hit`). The news roles
   are gated by their structured-output self-test (`RoleHealth`): a closed role judges nothing;
2. re-runs `check_official` at scan time for every agreed bad catalyst (an announcement or on-chain record
   may have been recorded since the verdict);
3. publishes one `RiskFlags` per universe coin to stream `risk_flags` (risk treats a coin without a fresh
   scan as "no new LONG", so clear coins are published too) and logs it in `news_veto_scans`.
Rules: `hdt.news.veto`. Flags expire after `veto.expires_cycles` cycles; `scan_fresh_at` is aged by the
staleness of the required sources.
"""

from __future__ import annotations

import asyncio
import logging
import re
import socket
import sys
from collections import defaultdict
from datetime import timedelta
from pathlib import Path
from typing import Any, Final

import sqlalchemy as sa
from redis.asyncio import Redis
from sqlalchemy.orm import Session, sessionmaker

from hdt.agents.llm_router import LlmRouter
from hdt.agents.llm_types import StructuredLlm
from hdt.agents.model_test import RoleHealth
from hdt.contracts.streams import Stream
from hdt.core.alerts import alert
from hdt.core.clock import ensure_utc, utcnow
from hdt.core.config import StaticConfig, require_env_value, static_config
from hdt.core.logging import configure_logging
from hdt.core.service import every, install_stop_signals
from hdt.core.streams import connect_redis, publish
from hdt.db.session import make_engine, make_session_factory
from hdt.lake.pit_query import PitQuery
from hdt.lake.universe import load_universe
from hdt.news.classes import VETO_CLASSES
from hdt.news.coins import CoinDirectory
from hdt.news.config import NewsFile, news_config
from hdt.news.metrics import VERDICTS, VETO_FLAGS, VETO_SCAN_LAST
from hdt.news.runtime import official_evidence, pinned_roles
from hdt.news.store import (
    VETO_SCANS,
    StoredItem,
    StoredVerdict,
    insert_rows,
    source_state,
    unjudged_items,
    verdicts_since,
)
from hdt.news.verdict import ItemJudge, OfficialEvidence, RoleGate, VerdictJob, store_verdict
from hdt.news.veto import VetoVerdict, decide_veto, keyword_pattern, risk_flags, scan_fresh_at
from hdt.ops.metrics import serve_metrics
from hdt.settings.events import ConfigWatcher
from hdt.settings.schemas import Section

log = logging.getLogger(__name__)

SERVICE: Final[str] = "veto_scan"


UNCONFIRMED_STATUSES: Final[frozenset[str]] = frozenset({"llm_failed", "quote_failed"})


def unconfirmed_hit(item: StoredItem, verdict: StoredVerdict, keywords: re.Pattern[str]) -> bool:
    """A bad-catalyst keyword item that no judge pair cleared: its verdict failed (`llm_failed`,
    `quote_failed`) or only the extractor looked at it (`no_event` without judges). It stays a soft veto
    for the window: a broken quote or a steered extractor never turns "XYZ drained" into a clear."""
    if not keywords.search(f"{item.title}\n{item.summary or ''}"):
        return False
    if verdict.status in UNCONFIRMED_STATUSES:
        return True
    return verdict.status == "no_event" and "judges" not in verdict.detail


def veto_verdicts(
    pairs: list[tuple[StoredItem, StoredVerdict]],
    coins: CoinDirectory,
    evidence: OfficialEvidence,
    *,
    as_of: Any,
    config: NewsFile,
    static: StaticConfig,
    keywords: re.Pattern[str],
) -> dict[int, list[VetoVerdict]]:
    """Per coin, the verdicts the rules read; `official` re-checked for this coin at scan time."""
    out: dict[int, list[VetoVerdict]] = defaultdict(list)
    for item, verdict in pairs:
        hit = unconfirmed_hit(item, verdict, keywords)
        for coin_id in item.coin_ids:
            official = coin_id in set(verdict.detail.get("official_coins") or ())
            ref = coins.get(coin_id)
            if verdict.status == "agreed" and verdict.event_class in VETO_CLASSES and not official and ref:
                try:
                    official = evidence.check(
                        verdict.event_class,
                        ref,
                        as_of=as_of,
                        item=item,
                        sources=static.news_sources,
                        config=config,
                    ).official
                except Exception:
                    # One unreadable record must not stop the scan for every coin: not official, so the
                    # agreed bad catalyst still counts, as a soft veto.
                    log.exception(
                        "official re-check failed; treating the item as not official",
                        extra={"item_id": item.item_id, "coin_id": coin_id},
                    )
                    official = False
            out[coin_id].append(
                VetoVerdict(
                    item_id=item.item_id,
                    status=verdict.status,
                    event_class=verdict.event_class,
                    class_a=verdict.class_a,
                    class_b=verdict.class_b,
                    official=official,
                    unconfirmed_keyword_hit=hit,
                )
            )
    return out


class VetoScan:
    def __init__(
        self,
        *,
        engine: sa.Engine,
        sessions: sessionmaker[Session],
        redis: Redis,
        pit: PitQuery,
        llm: StructuredLlm,
        watcher: ConfigWatcher,
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
        self.static = static
        self.config = config
        self.health = health
        self.interval_s = static.risk.veto_scan_interval_s
        self.keywords = keyword_pattern(config.veto.prefilter_keywords)

    async def scan_once(self) -> int:
        as_of = utcnow()
        cfg = self.config
        universe = await asyncio.to_thread(load_universe, self.pit, as_of)
        if universe is None:
            log.warning("veto scan skipped: no point-in-time universe")
            return 0
        coins = await asyncio.to_thread(CoinDirectory.from_lake, self.pit, as_of)
        since = as_of - timedelta(hours=cfg.veto.window_h)
        with self.engine.connect() as conn:
            evidence = official_evidence(conn, self.pit, as_of, cfg)
            pending = [
                item
                for item in unjudged_items(conn, cfg.rule_version, since, as_of)
                if self.keywords.search(f"{item.title}\n{item.summary or ''}")
            ]
        try:
            await self._judge(pending, coins, evidence)
        except Exception:
            # Judging is best effort; publishing the flags is not. Items left unjudged are soft vetoes.
            log.exception("veto scan could not judge its pending items; publishing without them")
        as_of = utcnow()
        with self.engine.connect() as conn:
            pairs = verdicts_since(conn, cfg.rule_version, since, as_of)
            still = {
                item.item_id: item
                for item in unjudged_items(conn, cfg.rule_version, since, as_of)
                if self.keywords.search(f"{item.title}\n{item.summary or ''}")
            }
            health = source_state(conn)
            unlock_rows = evidence.unlocks
        by_coin = veto_verdicts(
            pairs, coins, evidence, as_of=as_of, config=cfg, static=self.static, keywords=self.keywords
        )
        unjudged: dict[int, list[str]] = defaultdict(list)
        for item in still.values():
            for coin_id in item.coin_ids:
                unjudged[coin_id].append(item.item_id)
        tiers = evidence.unlock_tiers
        fresh_at, stale = scan_fresh_at(
            as_of, {k: v[0] for k, v in health.items()}, cfg.veto, self.interval_s
        )
        if stale:
            log.warning("veto scan sources stale", extra={"sources": stale})
        rows: list[dict[str, Any]] = []
        published = 0
        maxlen = self.static.settings.streams.maxlen.get(Stream.RISK_FLAGS.value)
        for member in universe.members:
            coin_id = member.cmc_id
            decision = decide_veto(
                by_coin.get(coin_id, []),
                [u for u in unlock_rows if u.coin_id == coin_id],
                tiers,
                unjudged.get(coin_id, []),
                as_of=as_of,
                cfg=cfg.veto,
            )
            flags = risk_flags(
                coin_id, decision, as_of=as_of, fresh_at=fresh_at, cfg=cfg.veto, interval_s=self.interval_s
            )
            await publish(self.redis, Stream.RISK_FLAGS, flags, maxlen=maxlen)
            published += 1
            VETO_FLAGS.labels(kind=decision.kind).inc()
            rows.append(
                {
                    "coin_id": coin_id,
                    "as_of": ensure_utc(as_of),
                    "veto_long": flags.veto_long,
                    "veto_short": flags.veto_short,
                    "size_mult": flags.size_mult,
                    "evidence_ref": flags.evidence_ref,
                    "scan_fresh_at": flags.scan_fresh_at,
                    "expires_at": flags.expires_at,
                    "reasons": {**decision.reasons, "stale_sources": stale},
                    "rule_version": cfg.rule_version,
                }
            )
        with self.engine.begin() as conn:
            insert_rows(conn, VETO_SCANS, rows)
        VETO_SCAN_LAST.set(as_of.timestamp())
        log.info(
            "veto scan published",
            extra={"coins": published, "hard": sum(r["veto_long"] for r in rows), "stale": stale},
        )
        return published

    async def _judge(
        self, pending: list[StoredItem], coins: CoinDirectory, evidence: OfficialEvidence
    ) -> None:
        if not pending:
            return
        self.watcher.apply_pending()
        roles = pinned_roles(self.watcher.pin())
        if roles is None:
            log.warning("news models are not configured: bad-catalyst items stay unjudged (soft veto)")
            return
        judge = ItemJudge(
            self.llm, roles, config=self.config, sources=self.static.news_sources, health=self.health
        )
        for item in pending[: self.config.veto.judge_pending_max]:
            refs = tuple(c for c in (coins.get(cid) for cid in item.coin_ids) if c is not None)
            if not refs:
                continue
            job = VerdictJob(item, refs, evidence, utcnow(), "veto_scan")
            row = await judge.verdict(job)
            if row is None:
                return
            if store_verdict(self.engine, row, job, self.config.rule_version):
                VERDICTS.labels(processor="veto_scan", status=row["status"]).inc()

    async def guarded_scan(self) -> None:
        try:
            await self.scan_once()
        except Exception as exc:
            with self.sessions() as session, session.begin():
                alert(
                    session,
                    kind="integrity_error",
                    severity="critical",
                    title="veto scan cycle failed",
                    detail=f"{type(exc).__name__}: {exc}"[:500],
                    service=SERVICE,
                    episode="veto_scan_cycle",
                )
            raise

    async def run(self, stop: asyncio.Event) -> None:
        await self.watcher.start()
        tasks = [
            asyncio.create_task(every(stop, 1.0, self.watcher.poll_once, "config watcher"), name="config"),
            asyncio.create_task(
                every(stop, float(self.interval_s), self.guarded_scan, "veto scan"), name="scan"
            ),
        ]
        try:
            await stop.wait()
        finally:
            stop.set()
            await asyncio.gather(*tasks, return_exceptions=True)
        log.info("veto scan stopped")


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
    scan = VetoScan(
        engine=engine,
        sessions=sessions,
        redis=redis,
        pit=pit,
        llm=llm,
        watcher=watcher,
        static=static,
        config=config,
        health=health,
    )
    log.info("veto scan starting", extra={"interval_s": scan.interval_s})
    try:
        await scan.run(stop)
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
