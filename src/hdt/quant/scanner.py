"""Scanner (red-team #23 / #24): which coins convene the council, on a fixed as_of grid.

Every `cadence_s` (after the CMC liquidation cycle closes, `run_delay_s` past the grid) the scanner
evaluates, point in time, for every coin of the LTX cross-section except BTC:
- LTX strict and loose thresholds (Design Contract section 2) with the contagion block;
- MIGRATION strict and loose thresholds on S_P, zM, G, BG;
and HELD for coins with an open position in the running account namespace (latest `AccountState` on the
`account_state` stream, read-only), re-emitted when the coin was not emitted for `held_reeval_min`.

Every evaluation is written to `scanner_log` (insert-only). Emission order under the daily budget
`max_candidates_per_day`: HELD always (never dropped), then strict candidates by score, then superset
(loose-only, only with `superset_enabled`); a candidate over budget is logged with `dropped_budget=true`.
BTC is never emitted.

Idempotency: one transaction per as_of holds an advisory lock, skips an as_of already logged and inserts
the log rows; the emitted candidates are XADDed only after that commit, so a consumer never sees a candidate
whose `scanner_log` row (and its `strict_pass`) is not visible yet. The log is the outbox: every XADD is
followed by an insert into `scanner_published`, and each run (a rerun of a logged as_of included) first
relays the emitted rows of the last two cadences that have no such row, rebuilt from the log itself. A crash
or Redis failure between commit and XADD is therefore retried; a crash between XADD and the mark publishes
twice, and the council trigger dedupes by the deterministic event id of (coin_id, as_of, source).

Relay: the newest slot first (the freshest candidate is worth the most to the council), and within one coin
and slot in rule priority (LTX, MIGRATION, HOLLOW_HYPE, then HELD), so a scored rule wins the council's
spacing over an unscored HELD re-evaluation. One failing row (XADD or mark) is logged, counted
(`hdt_scanner_relay_failures_total`) and retried next run; the rows after it are still relayed. The daily
budget and HELD recency count only delivered rows (with a `scanner_published` mark), so a candidate that
never reached the stream neither uses budget nor delays the next HELD re-evaluation.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

import sqlalchemy as sa
from apscheduler.job import Job
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from pydantic import ValidationError
from redis.asyncio import Redis
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from hdt.contracts.account import AccountState
from hdt.contracts.candidate import Candidate
from hdt.contracts.common import Account, CandidateSource, TargetType
from hdt.contracts.streams import Stream
from hdt.core.clock import ensure_utc, utcnow
from hdt.core.config import CmcRoutesFile, ScannerFile, StaticConfig, scanner_config, static_config
from hdt.core.ids import to_canonical
from hdt.core.streams import publish
from hdt.db.models.quant import ScannerLogRow, ScannerPublishedRow
from hdt.features.crowding import rules
from hdt.features.engine import FeatureEngine, Snapshot
from hdt.features.lake_io import LakeView
from hdt.lake.pit_query import PitQuery
from hdt.lake.universe import Universe, load_universe
from hdt.ops.metrics import SCANNER_RELAY_FAILURES
from hdt.risk.pinned import pinned_config
from hdt.settings.versions import pin_current

log = logging.getLogger(__name__)

EmitClass = Literal["held", "strict", "superset"]
_GRID_EPOCH = datetime(2000, 1, 1, tzinfo=UTC)
_LOCK_KEY = "hdt.scanner"
ACCOUNT_STATE_SCAN = 100
"""Newest `account_state` entries inspected for the running namespace (streams interleave namespaces)."""
RELAY_PRIORITY: dict[str, int] = {
    CandidateSource.LTX.value: 0,
    CandidateSource.MIGRATION.value: 1,
    CandidateSource.HOLLOW_HYPE.value: 2,
    CandidateSource.HELD.value: 3,
}
"""Relay order of the rules of one coin and slot (lower first); the scored rules go before HELD."""


def _delivered() -> sa.Exists:
    """The log row has a `scanner_published` mark (its candidate reached the stream)."""
    pub, log_ = ScannerPublishedRow, ScannerLogRow
    return sa.exists().where(
        pub.coin_id == log_.coin_id,
        pub.as_of == log_.as_of,
        pub.rule == log_.rule,
        pub.rule_version == log_.rule_version,
    )


@dataclass
class Evaluation:
    coin_id: int
    rule: CandidateSource
    side: str | None
    conditions: dict[str, Any]
    strict_pass: bool
    loose_pass: bool
    contagion_blocked: bool | None
    score: float | None
    target_type: TargetType
    emit_class: EmitClass | None
    emitted: bool = False
    dropped_budget: bool = False


@dataclass
class ScanResult:
    as_of: datetime
    evaluations: list[Evaluation] = field(default_factory=list)
    emitted: list[Candidate] = field(default_factory=list)
    skipped: bool = False
    """True when this as_of was already logged (rerun) or no universe exists."""
    republished: list[Candidate] = field(default_factory=list)
    """Candidates of earlier runs whose XADD had not been confirmed, relayed by this run."""


def grid_as_of(now: datetime, scanner: ScannerFile) -> datetime:
    """The scan slot of `now`: floor((now - delay) / cadence) * cadence + delay on a fixed epoch grid."""
    now = ensure_utc(now)
    delay = timedelta(seconds=scanner.run_delay_s)
    cadence = scanner.cadence_s
    elapsed = int((now - delay - _GRID_EPOCH).total_seconds())
    return _GRID_EPOCH + timedelta(seconds=elapsed - elapsed % cadence) + delay


def evaluate_rules(snap: Snapshot, scanner: ScannerFile) -> list[Evaluation]:
    """LTX and MIGRATION evaluations for the LTX cross-section without BTC (pure given the snapshot)."""
    out: list[Evaluation] = []
    cross = snap.cross
    min_abs = snap.static.indicators.funding.min_abs_per_h
    btc_id = snap.btc.cmc_id if snap.btc is not None else None
    for member in snap.universe.ltx:
        if member.cmc_id == btc_id:
            continue
        core = snap.core(member)
        view = rules(core, cross, scanner, min_abs)
        ltx = view.ltx
        blocked = view.blocked
        strict = ltx.strict_pass
        loose = ltx.loose_pass
        emit: EmitClass | None = None
        if blocked is False and strict:
            emit = "strict"
        elif blocked is False and loose and scanner.superset_enabled:
            emit = "superset"
        out.append(
            Evaluation(
                coin_id=member.cmc_id,
                rule=CandidateSource.LTX,
                side=ltx.side,
                conditions={
                    "values": view.ltx_inputs.values(),
                    "strict": ltx.strict,
                    "loose": ltx.loose,
                    "breadth": cross.breadth.of(ltx.side) if ltx.side else None,
                    "btc_flushed": cross.btc_flush.get(ltx.side) if ltx.side else None,
                },
                strict_pass=strict,
                loose_pass=loose,
                contagion_blocked=blocked,
                score=view.ltx_inputs.spike,
                target_type=TargetType.RESID_12H,
                emit_class=emit,
            )
        )
        m_strict, m_loose = view.migration, view.migration_loose
        m = core.migration
        m_emit: EmitClass | None = None
        if m_strict is not None and m_strict.passed:
            m_emit = "strict"
        elif m_loose is not None and m_loose.passed and scanner.superset_enabled:
            m_emit = "superset"
        out.append(
            Evaluation(
                coin_id=member.cmc_id,
                rule=CandidateSource.MIGRATION,
                side=m_strict.side if m_strict is not None else None,
                conditions={
                    "values": {
                        "s_p": m.s_p if m else None,
                        "zm": core.zm,
                        "g": m.g if m else None,
                        "bg": m.bg if m else None,
                    },
                    "strict": m_strict.conditions if m_strict else None,
                    "loose": m_loose.conditions if m_loose else None,
                    "outlier_venues": list(m.outlier_venues) if m else [],
                },
                strict_pass=bool(m_strict and m_strict.passed),
                loose_pass=bool(m_loose and m_loose.passed),
                contagion_blocked=None,
                score=core.zm,
                target_type=TargetType.RAW_12H,
                emit_class=m_emit,
            )
        )
    return out


def apply_budget(evaluations: Sequence[Evaluation], emitted_today: int, budget: int) -> None:
    """Mark `emitted` / `dropped_budget`: HELD always, then strict by score, then superset."""
    used = emitted_today
    for ev in evaluations:
        if ev.emit_class == "held":
            ev.emitted = True
            used += 1
    for klass in ("strict", "superset"):
        ranked = sorted(
            (ev for ev in evaluations if ev.emit_class == klass),
            key=lambda ev: (
                -(ev.score if ev.score is not None else float("-inf")),
                ev.coin_id,
                ev.rule.value,
            ),
        )
        for ev in ranked:
            if used < budget:
                ev.emitted = True
                used += 1
            else:
                ev.dropped_budget = True


def latest_account_state(
    entries: Sequence[tuple[Any, dict[Any, Any]]], account: Account
) -> AccountState | None:
    """Newest well-formed `AccountState` of `account` among XREVRANGE entries (newest first)."""
    for _, fields in entries:
        data = fields.get(b"data", fields.get("data"))
        if data is None:
            continue
        try:
            state = AccountState.model_validate_json(data)
        except (ValidationError, ValueError):
            continue
        if state.account == account:
            return state
    return None


class Scanner:
    def __init__(
        self,
        pit: PitQuery,
        engine: sa.Engine,
        redis: Redis,
        *,
        static: StaticConfig | None = None,
        scanner: ScannerFile | None = None,
        features: FeatureEngine | None = None,
        clock: Callable[[], datetime] = utcnow,
    ) -> None:
        self.pit = pit
        self.engine = engine
        self.redis = redis
        self.static = static or static_config()
        self.scanner = scanner or scanner_config()
        self.features = features or FeatureEngine(LakeView(pit, clock=clock), self.static, self.scanner)
        self.clock = clock

    async def run(self, now: datetime | None = None) -> ScanResult:
        as_of = grid_as_of(now or self.clock(), self.scanner)
        with Session(self.engine) as session:
            pin = pin_current(session)
        # The run mode as Risk and execution resolve it: a stored mode version outside the current hard
        # ceilings runs clipped (they raise its `config_invalid` alert) instead of failing every scan.
        pinned = pinned_config(pin, self.static)
        for problem in pinned.problems:
            log.warning(
                "scanner: stored %s version %d breaks the hard ceilings; running clipped",
                problem.section.value,
                problem.version_id,
            )
        account = pinned.mode
        routes: CmcRoutesFile = pin.cmc_routes(self.static)
        universe = load_universe(self.pit, as_of)
        if universe is None:
            log.warning("scanner: no universe recorded at %s; nothing evaluated", as_of.isoformat())
            return ScanResult(as_of, skipped=True)
        snap = self.features.snapshot(as_of, universe, routes)
        evaluations = evaluate_rules(snap, self.scanner)
        held = await self._held(account, universe)
        return await self._commit(as_of, universe, evaluations, held)

    async def _held(self, account: Account, universe: Universe) -> dict[int, str]:
        """coin_id -> signed position qty for coins held (hedge book excluded) in the namespace."""
        entries: Any = await self.redis.xrevrange(str(Stream.ACCOUNT_STATE), count=ACCOUNT_STATE_SCAN)
        state = latest_account_state(entries, account)
        if state is None:
            return {}
        by_symbol = {m.binance_symbol: m.cmc_id for m in universe.members}
        btc = self.static.indicators.reserves.btc_symbol
        btc_ids = {m.cmc_id for m in universe.members if m.cmc_symbol == btc}
        out: dict[int, str] = {}
        for pos in state.positions:
            if pos.qty == 0 or pos.is_hedge_book:
                continue
            coin_id = by_symbol.get(pos.symbol)
            if coin_id is None:
                log.warning("scanner: held symbol %s is not in the universe of %s", pos.symbol, universe.date)
                continue
            if coin_id not in btc_ids:
                out[coin_id] = str(pos.qty)
        return out

    async def _commit(
        self, as_of: datetime, universe: Universe, evaluations: list[Evaluation], held: dict[int, str]
    ) -> ScanResult:
        cfg = self.scanner
        row = ScannerLogRow
        result = ScanResult(as_of, evaluations)
        maxlen = self.static.settings.streams.maxlen.get(str(Stream.CANDIDATES))
        with self.engine.begin() as conn:
            conn.execute(sa.text("SELECT pg_advisory_xact_lock(hashtext(:k))"), {"k": _LOCK_KEY})
            done = conn.execute(
                sa.select(sa.func.count()).where(row.as_of == as_of, row.rule_version == cfg.rule_version)
            ).scalar_one()
            if done:
                result.skipped = True
            else:
                self._log_scan(conn, result, universe, held)
        new = {c.natural_key for c in result.emitted}
        result.republished = [c for c in await self._relay_unsent(as_of, maxlen) if c.natural_key not in new]
        return result

    def _log_scan(
        self, conn: sa.Connection, result: ScanResult, universe: Universe, held: dict[int, str]
    ) -> None:
        """Budget, emit and log one as_of inside the caller's locked transaction."""
        cfg = self.scanner
        row = ScannerLogRow
        as_of = result.as_of
        evaluations = result.evaluations
        evaluations.extend(self._held_evaluations(conn, as_of, held))
        day_start = as_of.replace(hour=0, minute=0, second=0, microsecond=0)
        emitted_today = conn.execute(
            sa.select(sa.func.count()).where(
                row.emitted.is_(True), row.as_of >= day_start, row.as_of < as_of, _delivered()
            )
        ).scalar_one()
        apply_budget(evaluations, int(emitted_today), cfg.max_candidates_per_day)
        result.emitted.extend(
            Candidate(
                coin_id=ev.coin_id,
                as_of=as_of,
                source=ev.rule,
                score=ev.score if ev.score is not None else 0.0,
                rule_version=cfg.rule_version,
                target_type=ev.target_type,
                label_spec_version=cfg.labels.label_spec_version,
            )
            for ev in evaluations
            if ev.emitted
        )
        now = utcnow()
        rows = [
            {
                "coin_id": ev.coin_id,
                "as_of": as_of,
                "rule": ev.rule.value,
                "rule_version": cfg.rule_version,
                "side": ev.side,
                "conditions": to_canonical(ev.conditions),
                "strict_pass": ev.strict_pass,
                "loose_pass": ev.loose_pass,
                "contagion_blocked": ev.contagion_blocked,
                "emitted": ev.emitted,
                "dropped_budget": ev.dropped_budget,
                "score": ev.score,
                "target_type": ev.target_type.value,
                "label_spec_version": cfg.labels.label_spec_version,
                "universe_date": universe.date,
                "feature_ver": self.features.feature_ver,
                "created_at": now,
            }
            for ev in evaluations
        ]
        if rows:
            conn.execute(insert(ScannerLogRow).values(rows).on_conflict_do_nothing())

    async def _relay_unsent(self, as_of: datetime, maxlen: int | None) -> list[Candidate]:
        """XADD every emitted log row of the last two cadences not yet in `scanner_published`: the newest
        slot first, then per coin in `RELAY_PRIORITY`. A failing row is counted and left for the next run.

        An older unsent row stays unsent: the council would skip it as stale anyway.
        """
        log_ = ScannerLogRow
        since = as_of - timedelta(seconds=2 * self.scanner.cadence_s)
        priority = sa.case(RELAY_PRIORITY, value=log_.rule, else_=len(RELAY_PRIORITY))
        with self.engine.connect() as conn:
            rows = conn.execute(
                sa.select(
                    log_.coin_id,
                    log_.as_of,
                    log_.rule,
                    log_.rule_version,
                    log_.score,
                    log_.target_type,
                    log_.label_spec_version,
                )
                .where(log_.emitted.is_(True), log_.as_of >= since, log_.as_of <= as_of, ~_delivered())
                .order_by(log_.as_of.desc(), log_.coin_id, priority)
            ).all()
        relayed: list[Candidate] = []
        for r in rows:
            try:
                candidate = Candidate(
                    coin_id=r.coin_id,
                    as_of=ensure_utc(r.as_of),
                    source=CandidateSource(r.rule),
                    score=r.score if r.score is not None else 0.0,
                    rule_version=r.rule_version,
                    target_type=TargetType(r.target_type),
                    label_spec_version=r.label_spec_version,
                )
                await publish(self.redis, Stream.CANDIDATES, candidate, maxlen=maxlen)
                with self.engine.begin() as conn:
                    conn.execute(
                        insert(ScannerPublishedRow)
                        .values(
                            coin_id=r.coin_id,
                            as_of=r.as_of,
                            rule=r.rule,
                            rule_version=r.rule_version,
                            published_at=utcnow(),
                        )
                        .on_conflict_do_nothing()
                    )
            except Exception:
                SCANNER_RELAY_FAILURES.inc()
                log.exception(
                    "scanner: relaying a candidate failed; retried next run",
                    extra={"coin_id": r.coin_id, "as_of": ensure_utc(r.as_of).isoformat(), "rule": r.rule},
                )
                continue
            relayed.append(candidate)
        return relayed

    def _held_evaluations(
        self, conn: sa.Connection, as_of: datetime, held: dict[int, str]
    ) -> list[Evaluation]:
        if not held:
            return []
        row = ScannerLogRow
        since = as_of - timedelta(minutes=self.scanner.held_reeval_min)
        recent: set[int] = set(
            conn.execute(
                sa.select(row.coin_id).where(
                    row.coin_id.in_(held), row.emitted.is_(True), row.as_of > since, _delivered()
                )
            ).scalars()
        )
        out = []
        for coin_id in sorted(held):
            due = coin_id not in recent
            last = conn.execute(
                sa.select(row.target_type)
                .where(row.coin_id == coin_id, row.emitted.is_(True), row.rule != CandidateSource.HELD.value)
                .order_by(row.as_of.desc())
                .limit(1)
            ).scalar_one_or_none()
            out.append(
                Evaluation(
                    coin_id=coin_id,
                    rule=CandidateSource.HELD,
                    side="LONG" if not held[coin_id].startswith("-") else "SHORT",
                    conditions={"position_qty": held[coin_id], "reeval_due": due},
                    strict_pass=True,
                    loose_pass=True,
                    contagion_blocked=None,
                    score=None,
                    target_type=TargetType(last) if last else TargetType.RAW_12H,
                    emit_class="held" if due else None,
                )
            )
        return out


def register(scheduler: AsyncIOScheduler, scanner: Scanner) -> Job:
    """Add the scanner job to the council service scheduler on the fixed as_of grid."""
    cfg = scanner.scanner
    trigger = IntervalTrigger(
        seconds=cfg.cadence_s, start_date=_GRID_EPOCH + timedelta(seconds=cfg.run_delay_s), timezone=UTC
    )
    return scheduler.add_job(
        scanner.run,
        trigger,
        id="quant_scanner",
        name="quant scanner",
        coalesce=True,
        max_instances=1,
        misfire_grace_time=max(1, cfg.cadence_s // 2),
        replace_existing=True,
    )
