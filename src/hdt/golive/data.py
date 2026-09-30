"""Postgres readers of phase 11 (role `hdt_scorer`; grants in migrations 0001, 0003, 0005, 0009, 0010, 0011).

- `load_splits`, `load_current_splits`, `load_finals`: the current calibration / evaluation split (the newest
  generation) and its final G4 evaluation per target type (`golive_eval_splits`, `golive_g4_finals`);
- `load_replay_events`: every scored (independent) decision card with a resolved label, its round-1 and
  final forecasts, the parameters in force (card `params` + the `scoring_params` calibration maps of its
  `params_version`), the council rules of its pinned `council` config version, its config and model pins
  and its scanner superset log row;
- `scored_cards`, `window_cards`, `unlabeled_cards`, `card_target_types`: the decision cards of an
  evaluation window;
- `entry_events`: events with a filled entry order in one account namespace;
- `daily_pnl`: realized PnL per UTC day and target type of one account: fills net of commissions plus
  funding cash flows, attributed through the position's opening event; the pooled BTC hedge book is shared
  between the target types in proportion to the notional x time of the alt positions it hedged;
- `account_equity`: the whole account's equity (unrealized PnL included) at the end of each UTC day.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import distinct_on

from hdt.core.clock import ensure_utc
from hdt.core.config import CouncilFile, StaticConfig
from hdt.db.models.decision import DecisionCardRow, DecisionForecastRow
from hdt.db.models.golive import GoLiveEvalSplitRow, GoLiveG4FinalRow
from hdt.db.models.ledger import CashFlowRow, EquitySnapshotRow, FillRow, OrderRow, PositionRow
from hdt.db.models.quant import ScannerLogRow
from hdt.db.models.scoring import ScoringLabelRow, ScoringParamsRow
from hdt.db.models.settings import ConfigVersionRow
from hdt.golive.ablation import AgentPath, CouncilRules, ReplayEvent
from hdt.scoring.calibration import IsotonicMap
from hdt.scoring.loss import DEFAULT_CLIP
from hdt.settings.schemas import Section, effective_council, parse_section
from hdt.settings.versions import UnknownVersionError

ENTRY_LEGS = ("entry", "entry_ioc")
FUNDING_KIND = "FUNDING_FEE"
HELD = "HELD"
_CHUNK = 1000


def _chunks(values: Sequence[str]) -> Iterable[Sequence[str]]:
    for start in range(0, len(values), _CHUNK):
        yield values[start : start + _CHUNK]


def _num(value: Any) -> float | None:
    return float(value) if isinstance(value, int | float) and not isinstance(value, bool) else None


def _floats(value: Any) -> dict[str, float]:
    if not isinstance(value, Mapping):
        return {}
    return {str(k): float(v) for k, v in value.items() if _num(v) is not None}


def _version_ids(value: Any) -> dict[str, int]:
    if not isinstance(value, Mapping):
        return {}
    return {str(k): int(v) for k, v in value.items() if isinstance(v, int) and not isinstance(v, bool)}


def _agent_pins(value: Any) -> dict[str, tuple[str | None, str | None]]:
    if not isinstance(value, Mapping):
        return {}
    out: dict[str, tuple[str | None, str | None]] = {}
    for agent, pin in value.items():
        if not isinstance(pin, Mapping):
            continue
        slug, version = pin.get("model_slug"), pin.get("agent_version")
        out[str(agent)] = (
            str(slug) if slug is not None else None,
            str(version) if version is not None else None,
        )
    return out


# --------------------------------------------------------------------------------------------- split


@dataclass(frozen=True)
class SplitRecord:
    target_type: str
    generation: int
    eval_start: datetime


def load_current_splits(conn: sa.Connection) -> dict[str, SplitRecord]:
    """The current split (newest generation) of each target type that has one."""
    s = GoLiveEvalSplitRow
    return {
        str(r.target_type): SplitRecord(str(r.target_type), int(r.generation), ensure_utc(r.eval_start))
        for r in conn.execute(
            sa.select(s.target_type, s.generation, s.eval_start)
            .ext(distinct_on(s.target_type))
            .order_by(s.target_type, s.generation.desc())
        )
    }


def load_splits(conn: sa.Connection) -> dict[str, datetime]:
    """`eval_start` of the current split of each target type."""
    return {t: r.eval_start for t, r in load_current_splits(conn).items()}


@dataclass(frozen=True)
class FinalRecord:
    target_type: str
    generation: int
    eval_start: datetime
    eval_end: datetime
    passed: bool
    gate_passed: bool
    evidence_ref: str
    evaluated_at: datetime

    def to_json(self) -> dict[str, Any]:
        return {
            "target_type": self.target_type,
            "generation": self.generation,
            "eval_start": self.eval_start.isoformat(),
            "eval_end": self.eval_end.isoformat(),
            "passed": self.passed,
            "gate_passed": self.gate_passed,
            "evidence_ref": self.evidence_ref,
            "evaluated_at": self.evaluated_at.isoformat(),
        }


def load_finals(conn: sa.Connection) -> dict[str, FinalRecord]:
    """The recorded final G4 evaluation of each target type's current split (a target type whose current
    split has no final yet is absent)."""
    f = GoLiveG4FinalRow
    current = load_current_splits(conn)
    return {
        str(r.target_type): FinalRecord(
            str(r.target_type),
            int(r.generation),
            ensure_utc(r.eval_start),
            ensure_utc(r.eval_end),
            bool(r.passed),
            bool(r.gate_passed),
            str(r.evidence_ref),
            ensure_utc(r.evaluated_at),
        )
        for r in conn.execute(
            sa.select(
                f.target_type,
                f.generation,
                f.eval_start,
                f.eval_end,
                f.passed,
                f.gate_passed,
                f.evidence_ref,
                f.evaluated_at,
            )
        )
        if r.target_type in current and current[r.target_type].generation == r.generation
    }


# ------------------------------------------------------------------------------------------- council pins


def pinned_councils(
    conn: sa.Connection, static: StaticConfig, version_ids: Iterable[int | None]
) -> dict[int | None, CouncilFile]:
    """The effective council of each pinned `council` config version (None: no version was active, the YAML
    default applies, exactly as the council ran). An id that is not a `council` version raises."""
    wanted = sorted({v for v in version_ids if v is not None})
    out: dict[int | None, CouncilFile] = {None: static.council}
    if not wanted:
        return out
    v = ConfigVersionRow
    found = {
        int(r.id): r for r in conn.execute(sa.select(v.id, v.section, v.payload_json).where(v.id.in_(wanted)))
    }
    for version_id in wanted:
        row = found.get(version_id)
        if row is None or row.section != Section.COUNCIL.value:
            raise UnknownVersionError(f"config version {version_id} is not a 'council' version")
        out[version_id] = effective_council(parse_section(Section.COUNCIL, row.payload_json), static)
    return out


# -------------------------------------------------------------------------------------- replay events


def _param_maps(
    conn: sa.Connection, keys: set[tuple[str, str, int]]
) -> dict[tuple[str, str, int], tuple[dict[str, IsotonicMap], tuple[float, float]]]:
    out: dict[tuple[str, str, int], tuple[dict[str, IsotonicMap], tuple[float, float]]] = {}
    p = ScoringParamsRow
    wanted = {k for k in keys if k[2] >= 1}
    for target, spec in sorted({(t, s) for t, s, _ in wanted}):
        versions = sorted(v for t, s, v in wanted if (t, s) == (target, spec))
        for row in conn.execute(
            sa.select(p.params_version, p.payload).where(
                p.target_type == target, p.label_spec_version == spec, p.params_version.in_(versions)
            )
        ):
            payload = row.payload or {}
            clip = payload.get("clip") or list(DEFAULT_CLIP)
            maps = {str(k): IsotonicMap.from_json(v) for k, v in (payload.get("calibration") or {}).items()}
            out[(target, spec, int(row.params_version))] = (maps, (float(clip[0]), float(clip[1])))
    return out


def load_replay_events(
    conn: sa.Connection,
    *,
    static: StaticConfig,
    start: datetime | None = None,
    end: datetime | None = None,
) -> list[ReplayEvent]:
    """Scored cards with a resolved label in `[start, end)`, in canonical `(as_of, event_id)` order.

    Each event carries the council rules of the card's pinned `council` config version (and that council's
    initial `r` / `a` for an agent the card's `params` do not list), never today's. A card whose
    `params_version` snapshot is missing from `scoring_params` is replayed with the identity calibration
    (version 0 is the identity by definition)."""
    lab, card = ScoringLabelRow, DecisionCardRow
    query = (
        sa.select(
            card.event_id,
            card.coin_id,
            card.as_of,
            card.source,
            card.target_type,
            card.label_spec_version,
            card.rounds,
            card.stop_reason,
            card.p_pooled,
            card.disagreement,
            card.params,
            card.config_version_ids,
            card.agents,
            lab.y,
            lab.label_value,
            lab.beta_btc,
        )
        .join(lab, lab.event_id == card.event_id)
        .where(lab.status == "resolved", card.unscored.is_(False))
        .order_by(card.as_of, card.event_id)
    )
    if start is not None:
        query = query.where(card.as_of >= ensure_utc(start))
    if end is not None:
        query = query.where(card.as_of < ensure_utc(end))
    cards = conn.execute(query).all()
    if not cards:
        return []
    ids = [r.event_id for r in cards]
    forecasts: dict[str, dict[str, dict[int, Mapping[str, Any]]]] = {}
    for chunk in _chunks(ids):
        f = DecisionForecastRow
        for row in conn.execute(
            sa.select(f.event_id, f.agent, f.round, f.forecast).where(f.event_id.in_(chunk))
        ):
            forecasts.setdefault(row.event_id, {}).setdefault(row.agent, {})[int(row.round)] = row.forecast
    keys = {
        (r.target_type, r.label_spec_version, int(_num((r.params or {}).get("params_version")) or 0))
        for r in cards
    }
    maps = _param_maps(conn, keys)
    pins = {r.event_id: _version_ids(r.config_version_ids) for r in cards}
    councils = pinned_councils(conn, static, (p.get(Section.COUNCIL.value) for p in pins.values()))
    rules = {version: CouncilRules.from_config(council) for version, council in councils.items()}
    scans = _scanner_rows(conn, [(int(r.coin_id), r.as_of, str(r.source)) for r in cards])
    out: list[ReplayEvent] = []
    for r in cards:
        params: Mapping[str, Any] = r.params or {}
        version = int(_num(params.get("params_version")) or 0)
        found = maps.get((r.target_type, r.label_spec_version, version))
        council_id = pins[r.event_id].get(Section.COUNCIL.value)
        council = councils[council_id]
        agents: list[AgentPath] = []
        for agent, rounds in sorted(forecasts.get(r.event_id, {}).items()):
            first = rounds.get(1)
            if first is None:
                continue
            last = rounds[max(rounds)]
            agents.append(
                AgentPath(
                    agent=agent,
                    p_round1=None if first.get("abstain") else _num(first.get("p_used")),
                    p_final=None if last.get("abstain") else _num(last.get("p_used")),
                    p_model_round1=_num(first.get("p_model")),
                    p_model_final=_num(last.get("p_model")),
                )
            )
        r_map = _floats(params.get("r"))
        a_map = _floats(params.get("a"))
        names = {a.agent for a in agents}
        scan = scans.get((int(r.coin_id), ensure_utc(r.as_of), str(r.source)))
        out.append(
            ReplayEvent(
                event_id=r.event_id,
                coin_id=int(r.coin_id),
                as_of=ensure_utc(r.as_of),
                source=str(r.source),
                target_type=str(r.target_type),
                label_spec_version=str(r.label_spec_version),
                y=int(r.y),
                rounds=int(r.rounds),
                stop_reason=str(r.stop_reason),
                p_pooled=_num(r.p_pooled),
                disagreement=_num(r.disagreement),
                method=params.get("method") if isinstance(params.get("method"), str) else None,
                params_version=version,
                weights=_floats(params.get("w")),
                r={a: r_map.get(a, council.weights.r_initial) for a in names},
                a={a: a_map.get(a, council.weights.a_initial) for a in names},
                b=_num(params.get("b")) or 0.0,
                maps=found[0] if found else {},
                cal_clip=found[1] if found else DEFAULT_CLIP,
                agents=tuple(agents),
                scan_score=scan.score if scan else None,
                scan_strict=scan.strict if scan else None,
                scan_side=scan.side if scan else None,
                label_value=_num(r.label_value),
                beta_btc=_num(r.beta_btc),
                rules=rules[council_id],
                config_version_ids=pins[r.event_id],
                agent_pins=_agent_pins(r.agents),
            )
        )
    return out


@dataclass(frozen=True)
class ScanRow:
    score: float | None
    strict: bool
    side: str | None
    emitted: bool


def _scanner_rows(
    conn: sa.Connection, keys: Sequence[tuple[int, datetime, str]]
) -> dict[tuple[int, datetime, str], ScanRow]:
    """The scanner superset log row of each (coin, as_of, rule); the emitted row wins when several rule
    versions logged the same slot."""
    wanted = [k for k in keys if k[2] in ("LTX", "MIGRATION")]
    if not wanted:
        return {}
    s = ScannerLogRow
    out: dict[tuple[int, datetime, str], ScanRow] = {}
    lo = min(ensure_utc(k[1]) for k in wanted)
    hi = max(ensure_utc(k[1]) for k in wanted)
    coins = sorted({k[0] for k in wanted})
    wanted_set = {(c, ensure_utc(t), rule) for c, t, rule in wanted}
    for row in conn.execute(
        sa.select(s.coin_id, s.as_of, s.rule, s.score, s.strict_pass, s.side, s.emitted)
        .where(s.as_of >= lo, s.as_of <= hi, s.coin_id.in_(coins), s.rule.in_(("LTX", "MIGRATION")))
        .order_by(s.as_of, s.coin_id, s.rule, s.rule_version)
    ):
        key = (int(row.coin_id), ensure_utc(row.as_of), str(row.rule))
        if key not in wanted_set:
            continue
        prev = out.get(key)
        if prev is None or (bool(row.emitted) and not prev.emitted):
            out[key] = ScanRow(_num(row.score), bool(row.strict_pass), row.side, bool(row.emitted))
    return out


# ------------------------------------------------------------------------------------ evaluation cards


def scored_cards(
    conn: sa.Connection, target_type: str, start: datetime, end: datetime
) -> list[tuple[str, datetime]]:
    """(event_id, as_of) of the independent forecasts (scored cards) of one target type in `[start, end)`,
    whatever their label, in `(as_of, event_id)` order."""
    c = DecisionCardRow
    return [
        (str(r.event_id), ensure_utc(r.as_of))
        for r in conn.execute(
            sa.select(c.event_id, c.as_of)
            .where(
                c.target_type == target_type,
                c.unscored.is_(False),
                c.source != HELD,
                c.as_of >= ensure_utc(start),
                c.as_of < ensure_utc(end),
            )
            .order_by(c.as_of, c.event_id)
        )
    ]


def unlabeled_cards(conn: sa.Connection, target_type: str, start: datetime, end: datetime) -> int:
    """Scored cards of the window without any `scoring_labels` row yet."""
    c, lab = DecisionCardRow, ScoringLabelRow
    return int(
        conn.execute(
            sa.select(sa.func.count())
            .select_from(c)
            .outerjoin(lab, lab.event_id == c.event_id)
            .where(
                lab.event_id.is_(None),
                c.target_type == target_type,
                c.unscored.is_(False),
                c.source != HELD,
                c.as_of >= ensure_utc(start),
                c.as_of < ensure_utc(end),
            )
        ).scalar_one()
    )


def window_cards(conn: sa.Connection, target_type: str, start: datetime, end: datetime) -> list[str]:
    """Every decision card of one target type in `[start, end)`: scored or not, any label (an unscored or
    unlabeled event can still open a position, and its PnL counts)."""
    c = DecisionCardRow
    return [
        str(e)
        for e in conn.execute(
            sa.select(c.event_id).where(
                c.target_type == target_type, c.as_of >= ensure_utc(start), c.as_of < ensure_utc(end)
            )
        ).scalars()
    ]


def card_target_types(conn: sa.Connection, *, since: datetime | None) -> set[str]:
    """Target types with at least one decision card at or after `since` (every card when None)."""
    c = DecisionCardRow
    query = sa.select(c.target_type).distinct()
    if since is not None:
        query = query.where(c.as_of >= ensure_utc(since))
    return {str(t) for t in conn.execute(query).scalars()}


# ---------------------------------------------------------------------------------------------- ledger


def entry_events(conn: sa.Connection, account: str, event_ids: Sequence[str]) -> set[str]:
    """Events with at least one entry order of `account` that filled (executed quantity > 0)."""
    out: set[str] = set()
    o = OrderRow
    for chunk in _chunks(list(event_ids)):
        out.update(
            str(e)
            for e in conn.execute(
                sa.select(o.event_id)
                .where(
                    o.account == account,
                    o.leg.in_(ENTRY_LEGS),
                    o.executed_qty > 0,
                    o.event_id.in_(chunk),
                )
                .distinct()
            ).scalars()
        )
    return out


@dataclass(frozen=True)
class DailyPnl:
    by_target: dict[str, dict[date, float]]
    """target_type -> UTC day -> realized PnL (USD) of positions opened by the given events."""
    before_start: float
    """Realized PnL of the whole account before `start` (sets the starting equity of the window)."""
    unattributed: float
    """PnL in the window of positions whose event is outside `events` (e.g. opened during the calibration
    set) and the hedge-book share of such positions; reported per target, counted by the account checks."""


@dataclass(frozen=True)
class _Position:
    event_id: str | None
    hedge: bool
    opened_at: datetime
    closed_at: datetime | None
    notional: float


def _hedge_shares(
    positions: Mapping[int, _Position], event_targets: Mapping[str, str], end: datetime
) -> dict[int, dict[str | None, float]]:
    """Per hedge-book position, the share of each target type (None: unattributed): the notional x time of
    the alt positions open while the hedge position was open."""
    alts = [p for p in positions.values() if not p.hedge]
    out: dict[int, dict[str | None, float]] = {}
    for pid, h in positions.items():
        if not h.hedge:
            continue
        h_end = h.closed_at or end
        weights: dict[str | None, float] = defaultdict(float)
        for p in alts:
            overlap = (min(p.closed_at or end, h_end) - max(p.opened_at, h.opened_at)).total_seconds()
            if overlap > 0 and p.notional > 0:
                target = event_targets.get(p.event_id) if p.event_id is not None else None
                weights[target] += p.notional * overlap
        total = sum(weights.values())
        out[pid] = {k: v / total for k, v in weights.items()} if total > 0 else {None: 1.0}
    return out


def daily_pnl(
    conn: sa.Connection,
    account: str,
    *,
    start: datetime,
    end: datetime,
    event_targets: Mapping[str, str],
) -> DailyPnl:
    """`event_targets` maps each counted event (every decision card of the window) to its target type."""
    start, end = ensure_utc(start), ensure_utc(end)
    p, f, c = PositionRow, FillRow, CashFlowRow
    positions = {
        int(r.position_id): _Position(
            r.event_id,
            bool(r.is_hedge_book),
            ensure_utc(r.opened_at),
            ensure_utc(r.closed_at) if r.closed_at is not None else None,
            abs(float(r.entry_price) * float(r.max_qty)),
        )
        for r in conn.execute(
            sa.select(
                p.position_id, p.event_id, p.is_hedge_book, p.opened_at, p.closed_at, p.entry_price, p.max_qty
            ).where(p.account == account)
        )
    }
    shares = _hedge_shares(positions, event_targets, end)

    def split_of(position_id: int | None, event_id: str | None) -> dict[str | None, float]:
        if position_id is not None and position_id in positions:
            pos = positions[position_id]
            if pos.hedge:
                return shares[position_id]
            event_id = pos.event_id
        return {event_targets.get(event_id) if event_id is not None else None: 1.0}

    by_target: dict[str, dict[date, float]] = defaultdict(lambda: defaultdict(float))
    before = unattributed = 0.0

    def book(value: float, at: datetime, position_id: int | None, event_id: str | None) -> None:
        nonlocal before, unattributed
        if at < start:
            before += value
            return
        for target, share in split_of(position_id, event_id).items():
            if target is None:
                unattributed += value * share
            else:
                by_target[target][at.date()] += value * share

    for row in conn.execute(
        sa.select(f.position_id, f.event_id, f.realized_pnl, f.fee_usd, f.filled_at).where(
            f.account == account, f.filled_at < end
        )
    ):
        value = float(row.realized_pnl) - float(row.fee_usd or 0)
        book(value, ensure_utc(row.filled_at), row.position_id, row.event_id)
    for flow in conn.execute(
        sa.select(c.position_id, c.amount_usd, c.occurred_at).where(
            c.account == account, c.occurred_at < end, c.kind == FUNDING_KIND
        )
    ):
        book(float(flow.amount_usd), ensure_utc(flow.occurred_at), flow.position_id, None)
    return DailyPnl({k: dict(v) for k, v in by_target.items()}, before, unattributed)


def account_equity(conn: sa.Connection, account: str, *, start: datetime, end: datetime) -> list[float]:
    """The account's equity (wallet plus unrealized PnL, `equity_snapshots`) at `start` and at the end of
    every UTC day of `[start, end)`: the last snapshot at or before each point. A day without a snapshot
    carries the previous value; points before the first snapshot are dropped."""
    start, end = ensure_utc(start), ensure_utc(end)
    e = EquitySnapshotRow
    baseline = conn.execute(
        sa.select(e.equity).where(e.account == account, e.ts <= start).order_by(e.ts.desc()).limit(1)
    ).scalar_one_or_none()
    day = sa.func.date_trunc("day", sa.func.timezone("UTC", e.ts))
    ranked = (
        sa.select(
            day.label("day"),
            e.equity,
            sa.func.row_number().over(partition_by=day, order_by=e.ts.desc()).label("rank"),
        )
        .where(e.account == account, e.ts > start, e.ts < end)
        .subquery()
    )
    closes = {
        r.day.date(): float(r.equity)
        for r in conn.execute(sa.select(ranked.c.day, ranked.c.equity).where(ranked.c.rank == 1))
    }
    out: list[float] = [float(baseline)] if baseline is not None else []
    current = out[0] if out else None
    d = start.date()
    last = (end - timedelta(microseconds=1)).date()
    while d <= last:
        if d in closes:
            current = closes[d]
        if current is not None:
            out.append(current)
        d += timedelta(days=1)
    return out
