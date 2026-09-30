"""The scorer cycle: resolve due labels, rescore the full history, audit claims, fit the stacker, run the
lesson A/B and Reflection. Each step is its own transaction; a failing step is logged and retried next
cycle without blocking the others.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta

import sqlalchemy as sa
from sqlalchemy import Connection
from sqlalchemy.orm import Session, sessionmaker

from hdt.agents.llm_types import StructuredLlm
from hdt.contracts.common import AgentName, TargetType
from hdt.core.alerts import alert
from hdt.core.alerts import resolve as resolve_alert
from hdt.core.clock import utcnow
from hdt.core.config import CouncilFile, ScannerFile, ScoringFile, StaticConfig
from hdt.db.session import transaction
from hdt.features.lake_io import LakeView
from hdt.lake.pit_query import PitQuery
from hdt.lake.universe import load_universe
from hdt.memory.episodic import EpisodicWriter, OutcomeRecord
from hdt.memory.lessons import LessonLog
from hdt.memory.store import MemoryConflictError, MemoryStore
from hdt.ops.metrics import AGENT_WEIGHT
from hdt.quant.labels import MarkBook
from hdt.scoring import claims_audit, reflection, store
from hdt.scoring.engine import EngineConfig, ScoringResult, rescore
from hdt.scoring.resolver import LabelOutcome, build_request, resolve
from hdt.scoring.stacking import fit_stacker
from hdt.settings.schemas import ModelsSection

log = logging.getLogger(__name__)
SERVICE = "scorer"
_RESCORE_LOCK = 0x68647453636F7265
"""Advisory lock key of the rescore transaction ("hdtScore")."""


_LABEL_EPISODE = "labels"
"""One `integrity_error` episode for every label-resolution failure: a systemic resolver error pages once."""
_LABEL_ALERT_IDS = 10
"""Event ids named in the label-failure alert detail (the rest are counted)."""


@dataclass(frozen=True)
class CycleReport:
    labels: int
    scored: int
    flagged: int
    stacked: int
    ab_recorded: int
    reflected: int


class Scorer:
    def __init__(
        self,
        *,
        sessions: sessionmaker[Session],
        static: StaticConfig,
        scanner: ScannerFile,
        scoring: ScoringFile,
        pit: PitQuery,
        memory: MemoryStore,
        council: Callable[[], CouncilFile],
        models: Callable[[], ModelsSection | None],
        llm: StructuredLlm | None,
        clock: Callable[[], datetime] = utcnow,
    ) -> None:
        self.sessions = sessions
        self.static = static
        self.scanner = scanner
        self.scoring = scoring
        self.view = LakeView(pit, clock=clock)
        self.marks = MarkBook(self.view)
        self.pit = pit
        self.lessons = LessonLog(memory)
        self.episodes = EpisodicWriter(memory)
        self.council = council
        self.models = models
        self.llm = llm
        self.clock = clock

    # ------------------------------------------------------------------ labels

    def resolve_due(self) -> int:
        """Resolve every due card and re-resolve recent `missing` / `error` labels, one savepoint per card:
        a card that raises is recorded as `error` (retried within `missing_retry_h`) and never blocks the
        other cards. The failures of a cycle raise one `integrity_error` alert, resolved by the first cycle
        without failures once no `error` label is left inside the retry window."""
        now = self.clock()
        council = self.council()
        cfg = self.scoring.resolver
        window = timedelta(hours=cfg.missing_retry_h)
        written = 0
        with transaction(self.sessions) as session:
            conn = session.connection()
            due = store.due_cards(
                conn,
                now=now,
                horizon_h=council.horizon_h,
                delay=timedelta(seconds=cfg.label_delay_s),
                limit=cfg.batch,
            )
            retry = store.retry_cards(
                conn,
                now=now,
                window=window,
                interval=timedelta(seconds=cfg.missing_retry_interval_s),
                limit=cfg.batch,
            )
            retried = {card.event_id for card in retry}
            failed: list[tuple[str, str]] = []
            for card in (*due, *retry):
                try:
                    with session.begin_nested():
                        outcome = self._resolve_card(conn, card, council.horizon_h, now)
                        if outcome.status == "pending":
                            if card.event_id in retried:
                                store.touch_label_attempt(conn, card.event_id, at=now)
                            continue
                        if outcome.status == "missing":
                            log.warning(
                                "label missing: recorded, not dropped",
                                extra={"event_id": card.event_id, "target_type": card.target_type},
                            )
                        store.write_label(conn, outcome, resolved_at=now)
                    written += 1
                except Exception as exc:
                    failed.append(
                        (card.event_id, self._label_failed(session, card, council.horizon_h, exc, now))
                    )
            if failed:
                ids = ", ".join(event_id for event_id, _ in failed[:_LABEL_ALERT_IDS])
                more = f" (+{len(failed) - _LABEL_ALERT_IDS} more)" if len(failed) > _LABEL_ALERT_IDS else ""
                alert(
                    session,
                    kind="integrity_error",
                    severity="critical",
                    title=f"label resolution failed for {len(failed)} card(s)",
                    detail=f"{ids}{more}: {failed[0][1]}",
                    service=SERVICE,
                    episode=_LABEL_EPISODE,
                )
            elif store.retryable_errors(conn, now=now, window=window) == 0:
                resolve_alert(
                    session, kind="integrity_error", account=None, service=SERVICE, episode=_LABEL_EPISODE
                )
        return written

    def _resolve_card(
        self, conn: Connection, card: store.DueCard, horizon_h: int, now: datetime
    ) -> LabelOutcome:
        cfg = self.scoring.resolver
        request = build_request(
            event_id=card.event_id,
            coin_id=card.coin_id,
            card_symbol=card.symbol,
            as_of=card.as_of,
            horizon_h=horizon_h,
            target_type=card.target_type,  # type: ignore[arg-type]
            label_spec_version=card.label_spec_version,
            packets=store.event_packets(conn, card.coin_id, card.as_of),
            universe=load_universe(self.pit, card.as_of),
            btc_cmc_symbol=self.static.indicators.reserves.btc_symbol,
        )
        return resolve(
            self.view,
            request,
            labels=self.scanner.labels,
            barrier=self.scoring.barrier,
            now=now,
            missing_after=timedelta(hours=cfg.missing_after_h),
            marks=self.marks.mark,
            kline_fetch_lag=timedelta(seconds=cfg.kline_fetch_lag_s),
        )

    @staticmethod
    def _label_failed(
        session: Session, card: store.DueCard, horizon_h: int, exc: Exception, now: datetime
    ) -> str:
        """Record the card as `error`; the reason for the cycle's single alert."""
        reason = f"{type(exc).__name__}: {exc}"[:500]
        log.error(
            "label resolution failed: card recorded as error",
            exc_info=exc,
            extra={"event_id": card.event_id, "target_type": card.target_type},
        )
        with session.begin_nested():
            store.write_label_error(
                session.connection(), card, horizon_h=horizon_h, error=reason, resolved_at=now
            )
        return reason

    # ------------------------------------------------------------------ weights, calibration, stacking

    def rescore(self) -> tuple[ScoringResult, int]:
        engine_cfg = EngineConfig.from_config(self.council(), self.scoring)
        with transaction(self.sessions) as session:
            conn = session.connection()
            # One rescore at a time: two scorer processes would race on the params version sequence.
            conn.execute(sa.select(sa.func.pg_advisory_xact_lock(_RESCORE_LOCK)))
            events = store.load_history(conn)
            flags = store.load_flags(conn)
            result = rescore(events, engine_cfg, flags)
            store.write_scored(conn, result.scored)
            store.write_weights(conn, result.weights)
            for key_result in result.keys.values():
                store.write_key_result(conn, key_result, now=self.clock())
            stacked = 0
            for key, samples in store.stack_samples(events).items():
                latest = store.latest_stack_through(conn, key)
                newest = max(s.as_of for s in samples)
                if latest is not None and newest <= latest:
                    continue
                fit = fit_stacker(engine_cfg.agents, samples, self.scoring.stacking, clip=engine_cfg.clip)
                if fit is not None:
                    store.write_stack_fit(conn, key, fit, engine_cfg.agents, now=self.clock())
                    stacked += 1
        self._export_gauges(result)
        return result, stacked

    @staticmethod
    def _export_gauges(result: ScoringResult) -> None:
        latest: dict[tuple[str, str], tuple[datetime, float, float, float]] = {}
        for row in result.weights:
            key = (row.target_type, row.agent)
            if key not in latest or row.as_of >= latest[key][0]:
                latest[key] = (row.as_of, row.w_capped, row.a, row.r)
        for (target, agent), (_, w, a, r) in latest.items():
            AGENT_WEIGHT.labels(agent, target, "w").set(w)
            AGENT_WEIGHT.labels(agent, target, "a").set(a)
            AGENT_WEIGHT.labels(agent, target, "r").set(r)

    # ------------------------------------------------------------------ episodic outcomes

    def record_outcomes(self) -> int:
        """Each scored agent forecast becomes an `OutcomeRecord` in the agent's episodic memory (once)."""
        written = 0
        with transaction(self.sessions) as session:
            conn = session.connection()
            pending = store.pending_outcomes(conn, limit=self.scoring.resolver.batch)
            for rows in pending.values():
                for row in rows:
                    record = OutcomeRecord.resolved(
                        as_of=row.as_of,
                        horizon_h=row.horizon_h,
                        event_id=row.event_id,
                        agent=AgentName(row.agent),
                        coin_id=row.coin_id,
                        target_type=TargetType(row.target_type),
                        label=row.y,
                        realized_return=row.realized_return,
                        hit=row.hit,
                        log_loss=row.log_loss,
                    )
                    try:
                        written += int(self.episodes.record_outcome(record))
                    except MemoryConflictError:
                        log.error(
                            "a different outcome is already in memory; kept the stored one",
                            extra={"event_id": row.event_id, "agent": row.agent},
                        )
            store.mark_outcomes_recorded(conn, list(pending), now=self.clock())
        return written

    # ------------------------------------------------------------------ claims audit

    def audit_claims(self) -> int:
        with transaction(self.sessions) as session:
            return len(claims_audit.audit(session, self.scoring.claims_audit, now=self.clock()))

    # ------------------------------------------------------------------ lessons

    def run_ab(self) -> int:
        with self.sessions() as session:
            return len(
                reflection.run_ab(
                    session.connection(),
                    self.lessons,
                    self.scoring.reflection,
                    now=self.clock(),
                    clip=self.scoring.loss_clip,
                )
            )

    async def reflect(self) -> int:
        params = self.scoring.reflection
        if not params.enabled or self.llm is None:
            return 0
        models = self.models()
        config = models.roles.get("reflection") if models is not None else None
        if config is None:
            log.warning("reflection role has no model configured: reflection skipped")
            return 0
        with self.sessions() as session:
            trades = reflection.closed_trades(session.connection())
        done = 0
        for trade in trades:
            status = await reflection.reflect(
                self.sessions,
                trade,
                llm=self.llm,
                lessons=self.lessons,
                config=config,
                params=params,
                now=self.clock(),
            )
            if status is None:
                break
            done += 1
        return done

    async def cycle(self) -> CycleReport:
        labels = self._step("label resolver", self.resolve_due)
        scored, stacked = 0, 0
        rescored = False
        try:
            result, stacked = self.rescore()
            scored = len(result.scored)
            rescored = True
        except Exception:
            log.exception("rescoring failed: episodic outcomes and reflection wait for the next cycle")
        if rescored:
            self._step("episodic outcomes", self.record_outcomes)
        flagged = self._step("claims audit", self.audit_claims)
        ab = self._step("lesson A/B", self.run_ab)
        reflected = 0
        if rescored:
            try:
                reflected = await self.reflect()
            except Exception:
                log.exception("reflection failed")
        report = CycleReport(labels, scored, flagged, stacked, ab, reflected)
        log.info("scoring cycle done", extra=report.__dict__)
        return report

    @staticmethod
    def _step(what: str, fn: Callable[[], int]) -> int:
        try:
            return fn()
        except Exception:
            log.exception("%s failed", what)
            return 0
