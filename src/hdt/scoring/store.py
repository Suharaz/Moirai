"""Scorer database I/O (role `hdt_scorer`): due events, label rows, the labeled history and result upserts.

Every result write is an upsert that only touches rows whose values differ, so rescoring an unchanged
history writes nothing and rescoring a changed one converges to the same rows as a fresh run.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert

from hdt.contracts.packet import QuantPacket
from hdt.core.clock import utcnow
from hdt.db.models.decision import DecisionCardRow, DecisionForecastRow
from hdt.db.models.quant import QuantPacketRow
from hdt.db.models.scoring import (
    POOLED,
    CalibrationBinRow,
    CalibrationStatRow,
    ClaimsAuditFlagRow,
    ScoredForecastRow,
    ScoringLabelRow,
    ScoringParamsRow,
    StackingModelRow,
    WeightHistoryRow,
)
from hdt.quant.packet import load_packet
from hdt.scoring.coverage import FlagWindow
from hdt.scoring.engine import AgentRound1, KeyResult, ScoredRow, ScoringEvent, WeightRow
from hdt.scoring.resolver import LabelOutcome
from hdt.scoring.stacking import StackFit, StackSample

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class DueCard:
    event_id: str
    coin_id: int
    symbol: str
    as_of: datetime
    target_type: str
    label_spec_version: str
    first_due: datetime


def due_cards(
    conn: sa.Connection, *, now: datetime, horizon_h: int, delay: timedelta, limit: int
) -> list[DueCard]:
    """Scored cards (`unscored = false`) whose horizon has passed and that have no final label yet."""
    c = DecisionCardRow
    horizon = timedelta(hours=horizon_h)
    rows = conn.execute(
        sa.select(c.event_id, c.coin_id, c.symbol, c.as_of, c.target_type, c.label_spec_version)
        .where(
            c.unscored.is_(False),
            c.as_of <= now - horizon - delay,
            ~sa.exists().where(ScoringLabelRow.event_id == c.event_id),
        )
        .order_by(c.as_of, c.event_id)
        .limit(limit)
    ).all()
    return [
        DueCard(
            r.event_id, r.coin_id, r.symbol, r.as_of, r.target_type, r.label_spec_version, r.as_of + horizon
        )
        for r in rows
    ]


def retry_cards(
    conn: sa.Connection,
    *,
    now: datetime,
    window: timedelta,
    interval: timedelta,
    limit: int,
) -> list[DueCard]:
    """Cards whose label is `missing` or `error`, still inside the re-resolution window (horizon end within
    `window` of `now`) and not attempted for `interval`. The resolver reads marks point in time (records
    fetched by `t + kline_fetch_lag + max_mark_gap`), so a retry resolves an `error` card, or a `missing`
    card whose on-time records landed in the lake late; a backfill fetched after that bound is not read."""
    c, lab = DecisionCardRow, ScoringLabelRow
    horizon_end = lab.as_of + sa.func.make_interval(0, 0, 0, 0, lab.horizon_h)
    rows = conn.execute(
        sa.select(
            c.event_id,
            c.coin_id,
            c.symbol,
            c.as_of,
            c.target_type,
            c.label_spec_version,
            horizon_end.label("first_due"),
        )
        .join(lab, lab.event_id == c.event_id)
        .where(
            lab.status.in_(("missing", "error")),
            horizon_end >= now - window,
            lab.resolved_at <= now - interval,
        )
        .order_by(lab.resolved_at, c.as_of, c.event_id)
        .limit(limit)
    ).all()
    return [
        DueCard(r.event_id, r.coin_id, r.symbol, r.as_of, r.target_type, r.label_spec_version, r.first_due)
        for r in rows
    ]


def event_packets(conn: sa.Connection, coin_id: int, as_of: datetime) -> dict[str, QuantPacket]:
    """Every committed packet of (coin, as_of) by agent, hash re-verified."""
    rows = conn.execute(
        sa.select(QuantPacketRow.agent, QuantPacketRow.packet_sha256).where(
            QuantPacketRow.coin_id == coin_id, QuantPacketRow.as_of == as_of
        )
    ).all()
    packets: dict[str, QuantPacket] = {}
    for row in rows:
        packet = load_packet(conn, row.packet_sha256)
        if packet is not None:
            packets[row.agent] = packet
    return packets


def write_label(conn: sa.Connection, outcome: LabelOutcome, *, resolved_at: datetime) -> None:
    """Insert the label; a `missing` / `error` row is replaced (re-resolution), a `resolved` one never."""
    if outcome.status == "pending":
        raise ValueError("a pending label is not written")
    req = outcome.request
    values: dict[str, Any] = {
        "coin_id": req.coin_id,
        "symbol": req.symbol,
        "as_of": req.as_of,
        "target_type": req.target_type.value,
        "label_spec_version": req.label_spec_version,
        "horizon_h": req.horizon_h,
        "status": outcome.status,
        "y": outcome.y,
        "label_value": outcome.value,
        "coin_return": outcome.coin_return,
        "btc_return": outcome.btc_return,
        "beta_btc": req.beta_btc,
        "atr": req.atr_frac,
        "regime": req.regime,
        "barrier": outcome.barrier,
        "barrier_y": outcome.barrier_y,
        "resolved_at": resolved_at,
        "error": None,
    }
    _write_label_row(conn, req.event_id, values)


def write_label_error(
    conn: sa.Connection,
    card: DueCard,
    *,
    horizon_h: int,
    error: str,
    resolved_at: datetime,
) -> None:
    """Record a card the resolver could not process (terminal after the retry window, never silent)."""
    values: dict[str, Any] = {
        "coin_id": card.coin_id,
        "symbol": card.symbol,
        "as_of": card.as_of,
        "target_type": card.target_type,
        "label_spec_version": card.label_spec_version,
        "horizon_h": horizon_h,
        "status": "error",
        "y": None,
        "label_value": None,
        "coin_return": None,
        "btc_return": None,
        "beta_btc": None,
        "atr": None,
        "regime": None,
        "barrier": None,
        "barrier_y": None,
        "resolved_at": resolved_at,
        "error": error[:500] or "error",
    }
    _write_label_row(conn, card.event_id, values)


def _write_label_row(conn: sa.Connection, event_id: str, values: dict[str, Any]) -> None:
    lab = ScoringLabelRow.__table__
    stmt = insert(ScoringLabelRow).values(event_id=event_id, **values)
    conn.execute(
        stmt.on_conflict_do_update(
            index_elements=["event_id"],
            set_={name: stmt.excluded[name] for name in values},
            where=lab.c.status != "resolved",
        )
    )


def touch_label_attempt(conn: sa.Connection, event_id: str, *, at: datetime) -> None:
    """Stamp a re-resolution attempt of a `missing` / `error` label that ended pending, so `retry_cards`
    throttles it by `interval` like any other attempt."""
    lab = ScoringLabelRow
    conn.execute(
        sa.update(lab)
        .where(lab.event_id == event_id, lab.status.in_(("missing", "error")))
        .values(resolved_at=at)
    )


def retryable_errors(conn: sa.Connection, *, now: datetime, window: timedelta) -> int:
    """Labels still `error` inside the re-resolution window (horizon end within `window` of `now`)."""
    lab = ScoringLabelRow
    horizon_end = lab.as_of + sa.func.make_interval(0, 0, 0, 0, lab.horizon_h)
    return int(
        conn.execute(
            sa.select(sa.func.count()).where(lab.status == "error", horizon_end >= now - window)
        ).scalar_one()
    )


def _version(value: Any) -> int | None:
    """A registered agent version (>= 1); `0` (a replay of an unregistered configuration) is not one."""
    try:
        version = int(str(value))
    except (TypeError, ValueError):
        return None
    return version if version >= 1 else None


def _p(forecast: Mapping[str, Any], name: str) -> float | None:
    value = forecast.get(name)
    return float(value) if isinstance(value, int | float) and not isinstance(value, bool) else None


def load_history(conn: sa.Connection) -> list[ScoringEvent]:
    """Every resolved scored event with its round-1 forecasts and final-round revisions."""
    lab, card = ScoringLabelRow, DecisionCardRow
    cards = conn.execute(
        sa.select(
            lab.event_id,
            lab.coin_id,
            lab.as_of,
            lab.horizon_h,
            lab.target_type,
            lab.label_spec_version,
            lab.y,
            lab.barrier_y,
            lab.regime,
            card.p_pooled,
            card.rounds,
            card.agents,
        )
        .join(card, card.event_id == lab.event_id)
        .where(lab.status == "resolved")
        .order_by(lab.as_of, lab.event_id)
    ).all()
    if not cards:
        return []
    forecasts: dict[str, dict[str, dict[int, Mapping[str, Any]]]] = {}
    ids = [r.event_id for r in cards]
    for chunk_start in range(0, len(ids), 1000):
        chunk = ids[chunk_start : chunk_start + 1000]
        for row in conn.execute(
            sa.select(
                DecisionForecastRow.event_id,
                DecisionForecastRow.agent,
                DecisionForecastRow.round,
                DecisionForecastRow.forecast,
            ).where(DecisionForecastRow.event_id.in_(chunk))
        ):
            forecasts.setdefault(row.event_id, {}).setdefault(row.agent, {})[row.round] = row.forecast
    events: list[ScoringEvent] = []
    for r in cards:
        agents: list[AgentRound1] = []
        card_agents: Mapping[str, Any] = r.agents or {}
        for agent, rounds in sorted(forecasts.get(r.event_id, {}).items()):
            first = rounds.get(1)
            if first is None:
                continue
            version = _version(first.get("agent_version"))
            if version is None:
                meta = card_agents.get(agent)
                version = _version(meta.get("agent_version")) if isinstance(meta, Mapping) else None
            last_round = max(rounds)
            final = _p(rounds[last_round], "p_used") if last_round > 1 else None
            agents.append(
                AgentRound1(
                    agent=agent,
                    agent_version=version,
                    p_used=None if first.get("abstain") else _p(first, "p_used"),
                    p_model=_p(first, "p_model"),
                    p_final=final,
                )
            )
        events.append(
            ScoringEvent(
                event_id=r.event_id,
                coin_id=r.coin_id,
                as_of=r.as_of,
                horizon_h=r.horizon_h,
                target_type=r.target_type,
                label_spec_version=r.label_spec_version,
                y=int(r.y),
                regime=r.regime,
                p_pooled=float(r.p_pooled) if r.p_pooled is not None else None,
                debated=int(r.rounds) > 1,
                agents=tuple(agents),
                barrier_y=None if r.barrier_y is None else int(r.barrier_y),
            )
        )
    return events


def load_flags(conn: sa.Connection) -> list[FlagWindow]:
    f = ClaimsAuditFlagRow
    return [
        FlagWindow(r.agent, r.raised_at, r.reviewed_at)
        for r in conn.execute(sa.select(f.agent, f.raised_at, f.reviewed_at).order_by(f.agent, f.raised_at))
    ]


def _upsert(
    conn: sa.Connection, table: sa.Table, rows: Sequence[Mapping[str, Any]], keys: Sequence[str]
) -> None:
    """Insert rows; on key conflict update only when some value differs (no-op writes are skipped)."""
    if not rows:
        return
    columns = [c for c in rows[0] if c not in keys]
    for start in range(0, len(rows), 1000):
        stmt = insert(table).values(list(rows[start : start + 1000]))
        if columns:
            differs = sa.or_(*(table.c[c].is_distinct_from(stmt.excluded[c]) for c in columns))
            stmt = stmt.on_conflict_do_update(
                index_elements=list(keys), set_={c: stmt.excluded[c] for c in columns}, where=differs
            )
        else:
            stmt = stmt.on_conflict_do_nothing(index_elements=list(keys))
        conn.execute(stmt)


def write_scored(conn: sa.Connection, rows: Iterable[ScoredRow]) -> None:
    _upsert(
        conn,
        ScoredForecastRow.__table__,  # type: ignore[arg-type]
        [
            {
                "event_id": r.event_id,
                "agent": r.agent,
                "target_type": r.target_type,
                "label_spec_version": r.label_spec_version,
                "coin_id": r.coin_id,
                "as_of": r.as_of,
                "agent_version": r.agent_version,
                "p": r.p,
                "p_model": r.p_model,
                "label": r.label,
                "y": r.y,
                "hit": r.hit,
                "log_loss": r.log_loss,
                "regime": r.regime,
                "scored_at": r.scored_at,
            }
            for r in rows
        ],
        ("event_id", "agent"),
    )


def write_weights(conn: sa.Connection, rows: Iterable[WeightRow]) -> None:
    _upsert(
        conn,
        WeightHistoryRow.__table__,  # type: ignore[arg-type]
        [
            {
                "target_type": r.target_type,
                "label_spec_version": r.label_spec_version,
                "agent": r.agent,
                "as_of": r.as_of,
                "agent_version": r.agent_version,
                "w": r.w,
                "w_capped": r.w_capped,
                "a": r.a,
                "r": r.r,
                "coverage": r.coverage,
                "forecasts": r.forecasts,
                "events": r.events,
            }
            for r in rows
        ],
        ("target_type", "label_spec_version", "agent", "as_of"),
    )


def write_key_result(conn: sa.Connection, result: KeyResult, *, now: datetime | None = None) -> int:
    """Calibration rows at `computed_at` and a new `scoring_params` version only when the payload changed.
    Returns the served params_version."""
    target, spec = result.key
    bins: list[dict[str, Any]] = []
    stats: list[dict[str, Any]] = []
    for agent, chart in sorted(result.reliability.items()):
        stats.append(
            {
                "target_type": target,
                "label_spec_version": spec,
                "agent": agent,
                "computed_at": result.computed_at,
                "spiegelhalter_z": None
                if chart.spiegelhalter_z is None
                else round(chart.spiegelhalter_z, 12),
                "ece": round(chart.ece, 12),
                "n": chart.n,
            }
        )
        bins.extend(
            {
                "target_type": target,
                "label_spec_version": spec,
                "agent": agent,
                "computed_at": result.computed_at,
                "bin_index": b.index,
                "bin_lo": b.lo,
                "bin_hi": b.hi,
                "n": b.n,
                "mean_p": round(b.mean_p, 12),
                "observed_rate": round(b.observed_rate, 12),
            }
            for b in chart.bins
        )
    _upsert(
        conn,
        CalibrationStatRow.__table__,  # type: ignore[arg-type]
        stats,
        ("target_type", "label_spec_version", "agent", "computed_at"),
    )
    _upsert(
        conn,
        CalibrationBinRow.__table__,  # type: ignore[arg-type]
        bins,
        ("target_type", "label_spec_version", "agent", "computed_at", "bin_index"),
    )
    p = ScoringParamsRow
    latest = conn.execute(
        sa.select(p.params_version, p.payload_sha256)
        .where(p.target_type == target, p.label_spec_version == spec)
        .order_by(p.params_version.desc())
        .limit(1)
        .with_for_update()
    ).one_or_none()
    sha = result.payload_sha256
    if latest is not None and latest.payload_sha256 == sha:
        return int(latest.params_version)
    version = 1 if latest is None else int(latest.params_version) + 1
    conn.execute(
        insert(p).values(
            target_type=target,
            label_spec_version=spec,
            params_version=version,
            as_of=result.computed_at,
            payload=result.payload,
            payload_sha256=sha,
            created_at=now or utcnow(),
        )
    )
    return version


def stack_samples(events: Iterable[ScoringEvent]) -> dict[tuple[str, str], list[StackSample]]:
    """Independent outcomes with a triple-barrier label and a pooled p, per key."""
    out: dict[tuple[str, str], list[StackSample]] = {}
    for e in events:
        if e.barrier_y is None or e.p_pooled is None:
            continue
        out.setdefault(e.key, []).append(
            StackSample(
                event_id=e.event_id,
                as_of=e.as_of,
                horizon_h=e.horizon_h,
                p_by_agent={a.agent: a.p_used for a in e.agents if a.p_used is not None},
                regime=e.regime,
                p_pooled=e.p_pooled,
                barrier_y=e.barrier_y,
            )
        )
    return out


def latest_stack_through(conn: sa.Connection, key: tuple[str, str]) -> datetime | None:
    s = StackingModelRow
    return conn.execute(
        sa.select(sa.func.max(s.trained_through)).where(
            s.target_type == key[0], s.label_spec_version == key[1]
        )
    ).scalar_one_or_none()


def write_stack_fit(
    conn: sa.Connection,
    key: tuple[str, str],
    fit: StackFit,
    agents: Sequence[str],
    *,
    now: datetime | None = None,
) -> None:
    model, features = fit.stacker.to_stored() if fit.stacker is not None else (None, {"agents": list(agents)})
    conn.execute(
        insert(StackingModelRow)
        .values(
            target_type=key[0],
            label_spec_version=key[1],
            trained_through=fit.trained_through,
            n=fit.n,
            oos_n=fit.oos_n,
            oos_logloss_stack=fit.oos_logloss_stack,
            oos_logloss_pool=fit.oos_logloss_pool,
            enabled=fit.enabled,
            features=features,
            model=model,
            created_at=now or utcnow(),
        )
        .on_conflict_do_nothing(index_elements=["target_type", "label_spec_version", "trained_through"])
    )


@dataclass(frozen=True)
class PendingOutcome:
    event_id: str
    agent: str
    coin_id: int
    as_of: datetime
    horizon_h: int
    target_type: str
    y: int
    realized_return: float | None
    hit: bool
    log_loss: float


def pending_outcomes(conn: sa.Connection, *, limit: int) -> dict[str, list[PendingOutcome]]:
    """Scored per-agent forecasts of resolved, already rescored labels not yet written to episodic memory,
    by event (a label without scored rows waits for the next successful rescore)."""
    lab, s = ScoringLabelRow, ScoredForecastRow
    ids: list[str] = list(
        conn.execute(
            sa.select(lab.event_id)
            .where(
                lab.status == "resolved",
                lab.outcomes_recorded_at.is_(None),
                sa.exists().where(s.event_id == lab.event_id),
            )
            .order_by(lab.as_of, lab.event_id)
            .limit(limit)
        ).scalars()
    )
    if not ids:
        return {}
    rows = conn.execute(
        sa.select(
            s.event_id,
            s.agent,
            s.coin_id,
            s.as_of,
            lab.horizon_h,
            s.target_type,
            s.y,
            lab.label_value,
            s.hit,
            s.log_loss,
        )
        .join(lab, lab.event_id == s.event_id)
        .where(s.event_id.in_(ids), s.agent != POOLED)
        .order_by(s.as_of, s.event_id, s.agent)
    ).all()
    out: dict[str, list[PendingOutcome]] = {event_id: [] for event_id in ids}
    for r in rows:
        out[r.event_id].append(
            PendingOutcome(
                r.event_id,
                r.agent,
                r.coin_id,
                r.as_of,
                r.horizon_h,
                r.target_type,
                int(r.y),
                r.label_value,
                bool(r.hit),
                float(r.log_loss),
            )
        )
    return out


def mark_outcomes_recorded(conn: sa.Connection, event_ids: Sequence[str], *, now: datetime) -> None:
    if event_ids:
        conn.execute(
            sa.update(ScoringLabelRow)
            .where(ScoringLabelRow.event_id.in_(list(event_ids)))
            .values(outcomes_recorded_at=now)
        )
