"""Phase 11 weekly job and the final G4 evaluation: G4 on the evaluation set (interim every week, final once),
ablation replay and threshold calibration on the calibration set, paper vs live, and the weekly report
(Markdown + a Telegram summary).

Everything here reads Postgres as `hdt_scorer`. The report is sent through the existing ops alert outbox
(`hdt.ops.alerts.raise_alert`, kind `weekly_report`, severity info, one per ISO week): the telegram-bot
service delivers it like every other alert and closes it 24 h after delivery; no second Telegram sender
exists. The calibration proposals start from today's council (the active console `council` version, the
YAML default when none is active); every replayed event uses the council rules of its own pinned version.
Evaluation-set ablations exist only in the final evaluation (`hdt.golive.final`).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta
from typing import Any

import sqlalchemy as sa
from sqlalchemy.orm import Session

from hdt.core.clock import ensure_utc
from hdt.core.config import ScannerFile, ScoringFile, StaticConfig
from hdt.db.models.ledger import CashFlowRow, FillRow
from hdt.db.models.llm import LlmCallRow
from hdt.db.models.scoring import CalibrationStatRow, GateProgressRow, WeightHistoryRow
from hdt.golive.ablation import AblationResult, CouncilRules, ReplayEvent, run_ablation
from hdt.golive.calibrate import (
    Proposal,
    calibrate_d_threshold,
    calibrate_eta,
    calibrate_ltx_threshold,
    calibration_events,
    config_diff,
)
from hdt.golive.data import (
    FUNDING_KIND,
    account_equity,
    card_target_types,
    daily_pnl,
    entry_events,
    load_finals,
    load_replay_events,
    load_splits,
    scored_cards,
    unlabeled_cards,
    window_cards,
)
from hdt.golive.g4 import (
    PAPER,
    G4Result,
    G4Thresholds,
    TargetInputs,
    evaluate_account,
    evaluate_target,
    pinned_check,
)
from hdt.golive.live_compare import Comparison, Tolerances, measure
from hdt.golive.stats import BOOTSTRAP_RESAMPLES
from hdt.ops.alerts import raise_alert
from hdt.quant.gates import DECISION_HORIZON_H
from hdt.settings.versions import SectionNotConfiguredError, pin_current

SERVICE = "scorer"
DETAIL_MAX = 2000
CALIBRATION_SET = "calibration"


@dataclass(frozen=True)
class GoLiveContext:
    static: StaticConfig
    rules: CouncilRules
    """Today's council rules: the starting point of the calibration proposals (never used to replay)."""
    taker: float
    slippage_bp: float
    starting_equity: float
    eta: float
    alpha: float
    spike_min: float
    spike_floor: float
    zm_min: float
    label_retry_h: float
    """`resolver.missing_retry_h`: a missing label may still resolve until this long after its horizon."""
    resamples: int = BOOTSTRAP_RESAMPLES
    thresholds: G4Thresholds = field(default_factory=G4Thresholds)

    @classmethod
    def load(
        cls,
        session: Session,
        static: StaticConfig,
        scoring: ScoringFile,
        scanner: ScannerFile,
        *,
        resamples: int = BOOTSTRAP_RESAMPLES,
    ) -> GoLiveContext:
        try:
            council = pin_current(session).council(static)
        except SectionNotConfiguredError:
            council = static.council
        return cls(
            static=static,
            rules=CouncilRules.from_config(council),
            taker=static.risk.fees.taker,
            slippage_bp=static.risk.fees.slippage_bp,
            starting_equity=static.risk.paper_starting_equity_usd,
            eta=scoring.hedge.eta,
            alpha=scoring.hedge.fixed_share_alpha,
            spike_min=scanner.ltx.strict.spike_min,
            spike_floor=scanner.ltx.loose.spike_min,
            zm_min=scanner.migration.strict.zm_min,
            label_retry_h=scoring.resolver.missing_retry_h,
            resamples=resamples,
        )

    @property
    def strict_min(self) -> dict[str, float]:
        return {"LTX": self.spike_min, "MIGRATION": self.zm_min}


def day_floor(t: datetime) -> datetime:
    t = ensure_utc(t)
    return datetime.combine(t.date(), time(0), tzinfo=t.tzinfo)


# ------------------------------------------------------------------------------------------------- G4


@dataclass(frozen=True)
class Window:
    """The evaluation window of one target type: `[start, end)`, sealed at the UTC midnight after the day
    its N reached the threshold (then it never grows again); unsealed it ends at today's 00:00 UTC."""

    target_type: str
    start: datetime
    end: datetime
    sealed: bool
    entries: frozenset[str]


def evaluation_window(
    conn: sa.Connection, target_type: str, split: datetime, *, now: datetime, min_entries: int
) -> Window:
    horizon = max(day_floor(now), split)
    cards = scored_cards(conn, target_type, split, horizon)
    entered = entry_events(conn, PAPER, [e for e, _ in cards])
    end, sealed, count = horizon, False, 0
    for event_id, as_of in cards:
        if event_id in entered:
            count += 1
            if count >= min_entries:
                end, sealed = day_floor(as_of) + timedelta(days=1), True
                break
    return Window(target_type, split, end, sealed, frozenset(e for e, t in cards if t < end and e in entered))


def required_targets(
    conn: sa.Connection,
    splits: Mapping[str, datetime],
    history: Sequence[ReplayEvent],
    extra: Sequence[str] | None = None,
) -> tuple[str, ...]:
    """Every target type with a decision card since the earliest split (every card before any split), a
    scored event or a split. `extra` can only add target types, never narrow the set."""
    since = min(splits.values()) if splits else None
    found = card_target_types(conn, since=since) | {e.target_type for e in history} | set(splits)
    return tuple(sorted(found | set(extra or ())))


def evaluation_windows(
    conn: sa.Connection, ctx: GoLiveContext, *, now: datetime, required: Sequence[str]
) -> tuple[dict[str, Window], tuple[str, ...]]:
    """(window per required target type with a split, required target types without a split)."""
    splits = load_splits(conn)
    windows = {
        t: evaluation_window(conn, t, splits[t], now=now, min_entries=ctx.thresholds.min_entries)
        for t in required
        if t in splits
    }
    return windows, tuple(t for t in required if t not in splits)


def compute_g4(
    conn: sa.Connection,
    ctx: GoLiveContext,
    *,
    now: datetime,
    final: bool = False,
    required: Sequence[str] | None = None,
    events: Sequence[ReplayEvent] | None = None,
) -> tuple[G4Result, dict[str, AblationResult]]:
    """G4 over each target type's evaluation window (complete UTC days up to `now`, or the sealed window).

    Interim (`final=False`, the weekly job): no ablation on evaluation events, `overall` undecided. Final:
    the ablation replay of each window is part of the result; `hdt.golive.final` records it once."""
    horizon = day_floor(now)
    history = list(events) if events is not None else load_replay_events(conn, static=ctx.static, end=horizon)
    targets = required_targets(conn, load_splits(conn), history, required)
    windows, missing = evaluation_windows(conn, ctx, now=now, required=targets)
    results = {}
    ablations: dict[str, AblationResult] = {}
    for target, w in windows.items():
        mine = [e for e in history if e.target_type == target and w.start <= e.as_of < w.end]
        cards = window_cards(conn, target, w.start, w.end)
        pnl = daily_pnl(conn, PAPER, start=w.start, end=w.end, event_targets=dict.fromkeys(cards, target))
        ablation = None
        if final and mine:
            ablation = run_ablation(
                mine, ctx.rules, target_type=target, strict_min=ctx.strict_min, resamples=ctx.resamples
            )
            ablations[target] = ablation
        results[target] = evaluate_target(
            TargetInputs(
                target_type=target,
                eval_start=w.start,
                end=w.end,
                forecasts=[(e.p_pooled, e.y) for e in mine if e.p_pooled is not None],
                entries=len(w.entries),
                daily_pnl=pnl.by_target.get(target, {}),
                equity0=ctx.starting_equity + pnl.before_start,
                ablation=ablation,
                unattributed_pnl=pnl.unattributed,
                pinned=pinned_check(target, mine) if mine else None,
                final=final,
                sealed=w.sealed,
            ),
            ctx.thresholds,
        )
    account_window = None
    equity: list[float] = []
    if windows:
        start = min(w.start for w in windows.values())
        end = max(w.end for w in windows.values())
        if end > start:
            account_window = (start, end)
            equity = account_equity(conn, PAPER, start=start, end=end)
    return (
        G4Result(
            ensure_utc(now),
            targets,
            results,
            missing,
            final=final,
            account=evaluate_account(equity, ctx.thresholds),
            account_window=account_window,
        ),
        ablations,
    )


def final_blockers(conn: sa.Connection, ctx: GoLiveContext, *, now: datetime) -> list[str]:
    """Why the final evaluation cannot run yet (empty: it can). It needs a split and a sealed window for every
    required target type, a label for every scored card of each window, the label retry period over, and no
    final evaluation recorded for any of those splits."""
    history = load_replay_events(conn, static=ctx.static, end=day_floor(now))
    targets = required_targets(conn, load_splits(conn), history)
    if not targets:
        return ["no target type has a decision card or a split yet"]
    windows, missing = evaluation_windows(conn, ctx, now=now, required=targets)
    finals = load_finals(conn)
    out = [f"{t}: no calibration / evaluation split (python -m hdt.golive.split)" for t in missing]
    settle = timedelta(hours=DECISION_HORIZON_H + ctx.label_retry_h)
    for target, w in sorted(windows.items()):
        if target in finals:
            out.append(
                f"{target}: final evaluation already recorded at {finals[target].evaluated_at.isoformat()}"
                " (a new evaluation needs a new split: python -m hdt.golive.split set)"
            )
            continue
        if not w.sealed:
            out.append(
                f"{target}: {len(w.entries)} of {ctx.thresholds.min_entries} independent paper entries, "
                "window not sealed"
            )
            continue
        pending = unlabeled_cards(conn, target, w.start, w.end)
        if pending:
            out.append(f"{target}: {pending} scored cards of the window have no label yet")
        if ensure_utc(now) < w.end + settle:
            out.append(f"{target}: labels may still change until {(w.end + settle).isoformat()}")
    return out


# ---------------------------------------------------------------------------------- replay and calibrate


@dataclass(frozen=True)
class ReplayReport:
    ablations: dict[str, AblationResult]
    proposals: list[Proposal]
    notes: list[str]

    def to_json(self) -> dict[str, Any]:
        return {
            "set": CALIBRATION_SET,
            "ablations": {k: v.to_json() for k, v in sorted(self.ablations.items())},
            "proposals": [p.to_json() for p in self.proposals],
            "config_diff": config_diff(self.proposals),
            "notes": self.notes,
        }


def replay_report(
    conn: sa.Connection,
    ctx: GoLiveContext,
    *,
    now: datetime,
    events: Sequence[ReplayEvent] | None = None,
) -> ReplayReport:
    """Ablations and calibration proposals on the calibration set only (events before their target type's
    split); evaluation-set ablations belong to the final evaluation."""
    history = (
        list(events)
        if events is not None
        else load_replay_events(conn, static=ctx.static, end=ensure_utc(now))
    )
    splits = load_splits(conn)
    notes: list[str] = []
    chosen = calibration_events(history, splits)
    missing = sorted({e.target_type for e in history} - set(splits))
    if missing:
        notes.append(
            f"no calibration / evaluation split for {', '.join(missing)} (python -m hdt.golive.split)"
        )
    ablations = {
        t: run_ablation(chosen, ctx.rules, target_type=t, strict_min=ctx.strict_min, resamples=ctx.resamples)
        for t in sorted({e.target_type for e in chosen})
    }
    proposals = [
        calibrate_d_threshold(chosen, ctx.rules, taker=ctx.taker, slippage_bp=ctx.slippage_bp),
        calibrate_eta(chosen, agents=ctx.rules.agents, current=ctx.eta, alpha=ctx.alpha),
        calibrate_ltx_threshold(
            chosen, current=ctx.spike_min, floor=ctx.spike_floor, taker=ctx.taker, slippage_bp=ctx.slippage_bp
        ),
    ]
    if not chosen:
        notes.append("calibration set is empty: proposals keep every current value")
    return ReplayReport(ablations, proposals, notes)


# ------------------------------------------------------------------------------------- report sections


def gate_rows(conn: sa.Connection) -> list[dict[str, Any]]:
    g = GateProgressRow
    return [
        dict(r._mapping)
        for r in conn.execute(
            sa.select(g.gate, g.check_key, g.label, g.value, g.target, g.met, g.updated_at).order_by(
                g.gate, g.check_key
            )
        )
    ]


def weights_summary(conn: sa.Connection) -> list[dict[str, Any]]:
    """Newest weight row per (target_type, label_spec_version, agent)."""
    w = WeightHistoryRow
    latest = (
        sa.select(w.target_type, w.label_spec_version, w.agent, sa.func.max(w.as_of).label("as_of"))
        .group_by(w.target_type, w.label_spec_version, w.agent)
        .subquery()
    )
    rows = conn.execute(
        sa.select(w.target_type, w.label_spec_version, w.agent, w.as_of, w.w_capped, w.a, w.r, w.forecasts)
        .join(
            latest,
            sa.and_(
                latest.c.target_type == w.target_type,
                latest.c.label_spec_version == w.label_spec_version,
                latest.c.agent == w.agent,
                latest.c.as_of == w.as_of,
            ),
        )
        .order_by(w.target_type, w.label_spec_version, w.agent)
    )
    return [dict(r._mapping) for r in rows]


def calibration_summary(conn: sa.Connection) -> list[dict[str, Any]]:
    c = CalibrationStatRow
    latest = (
        sa.select(c.target_type, c.label_spec_version, c.agent, sa.func.max(c.computed_at).label("at"))
        .group_by(c.target_type, c.label_spec_version, c.agent)
        .subquery()
    )
    rows = conn.execute(
        sa.select(c.target_type, c.label_spec_version, c.agent, c.computed_at, c.spiegelhalter_z, c.ece, c.n)
        .join(
            latest,
            sa.and_(
                latest.c.target_type == c.target_type,
                latest.c.label_spec_version == c.label_spec_version,
                latest.c.agent == c.agent,
                latest.c.at == c.computed_at,
            ),
        )
        .order_by(c.target_type, c.label_spec_version, c.agent)
    )
    return [dict(r._mapping) for r in rows]


def costs_summary(conn: sa.Connection, *, since: datetime, until: datetime) -> dict[str, Any]:
    """LLM spend by pipeline and paper trading costs (commissions, funding) over `[since, until)`."""
    llm = LlmCallRow
    by_pipeline = {
        str(r.pipeline): {
            "calls": int(r.calls),
            "cost_usd": float(r.cost or 0.0),
            "unpriced": int(r.unpriced),
        }
        for r in conn.execute(
            sa.select(
                llm.pipeline,
                sa.func.count().label("calls"),
                sa.func.sum(llm.cost_usd).label("cost"),
                sa.func.count().filter(llm.cost_usd.is_(None)).label("unpriced"),
            )
            .where(llm.called_at >= since, llm.called_at < until)
            .group_by(llm.pipeline)
            .order_by(llm.pipeline)
        )
    }
    f, c = FillRow, CashFlowRow
    fees = conn.execute(
        sa.select(sa.func.coalesce(sa.func.sum(f.fee_usd), 0)).where(
            f.account == PAPER, f.filled_at >= since, f.filled_at < until
        )
    ).scalar_one()
    funding = conn.execute(
        sa.select(sa.func.coalesce(sa.func.sum(c.amount_usd), 0)).where(
            c.account == PAPER, c.kind == FUNDING_KIND, c.occurred_at >= since, c.occurred_at < until
        )
    ).scalar_one()
    return {
        "since": since.isoformat(),
        "until": until.isoformat(),
        "llm": by_pipeline,
        "llm_total_usd": sum(v["cost_usd"] for v in by_pipeline.values()),
        "paper_commissions_usd": float(fees or 0),
        "paper_funding_usd": float(funding or 0),
    }


def live_comparison(
    conn: sa.Connection, ctx: GoLiveContext, *, since: datetime | None = None
) -> Comparison | None:
    return measure(conn, Tolerances(slippage_bp=ctx.slippage_bp), since=since)


# ------------------------------------------------------------------------------------------- rendering


def _fmt(value: Any, digits: int = 4) -> str:
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def replay_markdown(replay: Mapping[str, Any]) -> list[str]:
    """Ablation tables, forward branches and the calibration proposals with their config diff."""
    lines = [f"## Ablations ({replay['set']} set, paired log-loss bootstrap, Holm)", ""]
    for target, ab in replay["ablations"].items():
        lines += [
            f"### {target} ({ab['n_events']} events, {ab['replay_mismatches']} replay mismatches)",
            "",
            "| test | n | mean diff | p Holm | retained | wins |",
            "|:--|--:|--:|--:|:--|:--|",
        ]
        for c in [*ab["components"], *ab["decisions"]]:
            lines.append(
                f"| {c['name']} | {c['n']} | {_fmt(c['mean_diff'], 5)} | {_fmt(c['p_holm'], 4)} | "
                f"{_fmt(c['retained'])} | {_fmt(c['wins'])} |"
            )
        branches = ", ".join(b["name"] for b in ab["forward_shadow_branches"])
        lines += ["", f"Forward shadow branches (never replayed): {branches}", ""]
    lines += ["## Calibration proposals (calibration set only, not applied)", ""]
    for p in replay["proposals"]:
        lines.append(
            f"- {p['file']} `{p['path']}`: current {p['current']}, "
            f"proposed {_fmt(p['proposed'])} ({p['reason']})"
        )
    diff = replay["config_diff"]
    lines += ["", "```diff", diff.rstrip("\n") or "(no change proposed)", "```", ""]
    return lines


def render_markdown(report: Mapping[str, Any]) -> str:
    lines = [f"# Weekly go-live report {report['week']}", "", f"Generated {report['generated_at']} UTC.", ""]
    g4 = report["g4"]
    finals = report.get("g4_finals") or []
    lines += [
        "## Gate G4 interim (final evaluation set so far, paper account)",
        "",
        "Interim figures never decide: the gate is decided once by the final evaluation "
        "(`python -m hdt.golive.final run`) on each sealed window.",
        "",
        f"Required target types: {', '.join(g4['required']) or 'none'}"
        + (f"; no split yet for {', '.join(g4['missing_split'])}" if g4["missing_split"] else ""),
        "",
    ]
    if finals:
        lines += ["Final evaluation recorded:", ""]
        lines += [
            f"- {f['target_type']}: {'passed' if f['passed'] else 'failed'} on "
            f"[{f['eval_start']}, {f['eval_end']}), gate {'passed' if f['gate_passed'] else 'failed'} "
            f"(evidence {f['evidence_ref']})"
            for f in finals
        ]
        lines.append("")
    else:
        lines += ["Final evaluation: not run yet.", ""]
    for target, tr in g4["targets"].items():
        window = f"[{tr['eval_start']}, {tr['eval_end']}), {'sealed' if tr['sealed'] else 'open'}"
        lines += [f"### {target} ({window})", "", "| check | value | target | met |", "|:--|--:|--:|:--|"]
        for c in tr["checks"]:
            lines.append(f"| {c['label']} | {_fmt(c['value'])} | {_fmt(c['target'])} | {_fmt(c['met'])} |")
        lines.append("")
    account = g4["account"]
    window = f"[{account['window'][0]}, {account['window'][1]})" if account["window"] else "no window"
    lines += [
        f"### Whole paper account ({window})",
        "",
        "| check | value | target | met |",
        "|:--|--:|--:|:--|",
    ]
    for c in account["checks"]:
        lines.append(f"| {c['label']} | {_fmt(c['value'])} | {_fmt(c['target'])} | {_fmt(c['met'])} |")
    lines.append("")
    lines += [
        "## All gate progress rows",
        "",
        "| gate | check | value | target | met |",
        "|:--|:--|--:|--:|:--|",
    ]
    for r in report["gates"]:
        lines.append(
            f"| {r['gate']} | {r['check_key']} | {_fmt(r['value'])} | "
            f"{_fmt(r['target'])} | {_fmt(r['met'])} |"
        )
    lines += [
        "",
        "## Weights (newest per agent)",
        "",
        "| target | agent | w | a | r | forecasts |",
        "|:--|:--|--:|--:|--:|--:|",
    ]
    for r in report["weights"]:
        lines.append(
            f"| {r['target_type']} | {r['agent']} | {_fmt(r['w_capped'], 3)} | {_fmt(r['a'], 3)} | "
            f"{_fmt(r['r'], 3)} | {r['forecasts']} |"
        )
    lines += [
        "",
        "## Calibration (newest per agent)",
        "",
        "| target | agent | Spiegelhalter Z | ECE | n |",
        "|:--|:--|--:|--:|--:|",
    ]
    for r in report["calibration"]:
        lines.append(
            f"| {r['target_type']} | {r['agent']} | {_fmt(r['spiegelhalter_z'], 3)} | "
            f"{_fmt(r['ece'], 4)} | {r['n']} |"
        )
    costs = report["costs"]
    lines += [
        "",
        f"## Costs ({costs['since']} to {costs['until']})",
        "",
        f"- LLM total: {costs['llm_total_usd']:.2f} USD",
        *(
            f"- {name}: {v['calls']} calls, {v['cost_usd']:.2f} USD ({v['unpriced']} unpriced)"
            for name, v in costs["llm"].items()
        ),
        f"- Paper commissions: {costs['paper_commissions_usd']:.2f} USD; paper funding: "
        f"{costs['paper_funding_usd']:.2f} USD (positive = received)",
        "",
    ]
    study = report.get("event_study")
    lines += ["## Event study on shadow data (12 h preregistered, net of fees, slippage and funding)", ""]
    if study is None:
        lines += ["Not run this week.", ""]
    else:
        lines += [
            "| source | target | N 12h | N min | decision 12h | mean 12h | CI 12h | replan |",
            "|:--|:--|--:|--:|:--|--:|:--|:--|",
        ]
        for g in study["groups"]:
            h12 = g["horizons"].get("12")
            ci = f"[{_fmt(h12['ci_low'], 5)}, {_fmt(h12['ci_high'], 5)}]" if h12 else "-"
            lines.append(
                f"| {g['source']} | {g['target_type']} | {h12['n'] if h12 else 0} | {_fmt(g['n_min'])} | "
                f"{g['decision_12h']} | {_fmt(h12['mean'], 5) if h12 else '-'} | {ci} | "
                f"{_fmt(g['replan_signal'])} |"
            )
        lines.append("")
    lines += replay_markdown(report["replay"])
    live = report.get("live")
    lines += ["## Paper vs live", ""]
    if live is None:
        lines += ["No active mode version: nothing to compare.", ""]
    else:
        lines += [
            f"Size step since {live['since']} (size multiplier {_fmt(live['size_multiplier'], 2)}): "
            f"**{live['verdict']}**",
            "",
            "| metric | n | min n | live | paper | limit | within |",
            "|:--|--:|--:|--:|--:|--:|:--|",
        ]
        for m in live["metrics"]:
            lines.append(
                f"| {m['key']} | {m['n']} | {m['min_n']} | {_fmt(m['live'])} | {_fmt(m['paper'])} | "
                f"{_fmt(m['limit'])} | {_fmt(m['within'])} |"
            )
        lines.append("")
    for note in report.get("notes", []):
        lines.append(f"- {note}")
    return "\n".join(lines).rstrip("\n") + "\n"


def telegram_detail(report: Mapping[str, Any]) -> str:
    """The alert detail: a plain-text digest of the report (the full Markdown stays on disk)."""
    g4 = report["g4"]
    finals = report.get("g4_finals") or []
    if finals:
        gate = "PASSED" if all(f["gate_passed"] for f in finals) else "FAILED"
        lines = [f"G4 final: {gate} ({', '.join(f['target_type'] for f in finals)})"]
    else:
        lines = ["G4 final: not run yet (interim below never decides)"]
    for target, tr in g4["targets"].items():
        failing = [c["key"] for c in tr["checks"] if c["met"] is False]
        state = "sealed" if tr["sealed"] else "open"
        verdict = "no failing check" if not failing else "failing " + ", ".join(failing)
        lines.append(f"{target} interim ({state}): {verdict}")
    failing_account = [c["key"] for c in g4["account"]["checks"] if c["met"] is False]
    lines.append(
        "account interim: "
        + ("no failing check" if not failing_account else "failing " + ", ".join(failing_account))
    )
    if g4["missing_split"]:
        lines.append(f"no split: {', '.join(g4['missing_split'])}")
    study = report.get("event_study")
    if study is not None:
        for g in study["groups"]:
            lines.append(
                f"event study {g['source']} {g['target_type']}: N={g['n_signals']} 12h {g['decision_12h']}"
            )
    for target, ab in report["replay"]["ablations"].items():
        losers = [c["name"] for c in ab["components"] if c["retained"] and c["wins"] is not True]
        lines.append(
            f"ablation {target}: "
            + ("every retained component wins" if not losers else "not winning " + ", ".join(losers))
        )
    changes = [p for p in report["replay"]["proposals"] if p["proposed"] is not None]
    lines.append(
        "proposals: "
        + ("; ".join(f"{p['path']} {p['current']} -> {p['proposed']}" for p in changes) or "none")
    )
    live = report.get("live")
    if live is not None:
        lines.append(f"paper vs live: {live['verdict']}")
    lines.append(f"costs: LLM {report['costs']['llm_total_usd']:.2f} USD")
    lines.append(f"report: {report.get('report_path', '-')}")
    text = "\n".join(lines)
    return text if len(text) <= DETAIL_MAX else text[: DETAIL_MAX - 3] + "..."


def send_report(session: Session, report: Mapping[str, Any]) -> str | None:
    """Queue the Telegram digest in the alert outbox (one per ISO week; a rerun the same week is a no-op
    while the week's alert is still open)."""
    return raise_alert(
        session,
        kind="weekly_report",
        severity="info",
        title=f"Weekly go-live report {report['week']}",
        detail=telegram_detail(report),
        dedupe_key=str(report["week"]),
        service=SERVICE,
    )


def iso_week(now: datetime) -> str:
    year, week, _ = ensure_utc(now).isocalendar()
    return f"{year}-W{week:02d}"


def week_window(now: datetime) -> tuple[datetime, datetime]:
    until = day_floor(now)
    return until - timedelta(days=7), until
