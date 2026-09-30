"""Gate G4 (Design Contract section 9, phase 11 Red Team Delta), computed per `target_type` on the final
evaluation set only (events at or after the `golive_eval_splits` instant of that target type), plus the
whole paper account:

- N: independent forecasts (scored cards) with a filled entry order of the `paper` account: >= 150;
- calibration of the pooled `p` over every independent forecast (NO_TRADE included): Spiegelhalter
  |Z| < 1.96 and the 95% CI of the calibration slope contains 1; ECE is reported only;
- daily realized PnL (every UTC day of the window, no-trade days as 0) of the positions opened by every
  decision card of the window, scored or not, with its share of the BTC hedge book: annualized Sharpe > 1.5
  and PSR(0) >= 0.95, max drawdown < 10%;
- `pinned`: the model slug and agent version of every agent and the `models` and `council` config versions
  stay the same over the window's scored cards (a change starts a new evaluation, never a mixed one), and
  every required target type's window ran with the same pins (`pins_agree`: a pass must be bound to one
  configuration, `hdt.golive.pins`);
- every retained component wins its paired-bootstrap log-loss ablation with Holm correction, replayed with
  each card's pinned council rules and without a single replay mismatch (`hdt.golive.ablation`);
- account: Sharpe, PSR and max drawdown of the whole paper account's end-of-day equity (unrealized PnL
  included, every position whatever its event) over the union of the windows.

The window of a target type is sealed at the UTC midnight after the day its N reached 150: the final
evaluation always reads the same window, whenever it runs. The gate passes when every required target type
passes every deciding check and the account checks pass. Required: every target type with a decision card
since the earliest split, or with a split; a caller can only add to that set.

Two stages. The weekly job computes an interim result: it upserts `gate_progress` (gate `G4`, keys
`<target_type>.<check>`, `account.<check>`, `pins_agree` and `overall`, whose `met` stays NULL), never over
the rows of a target type whose current split already has a final evaluation (nor the account, `pins_agree`
and `overall` rows once a final exists), and never writes `gate_flags`; the ablation is not computed on
evaluation events. The final evaluation (`python -m hdt.golive.final run`) runs once per split: `write_final`
inserts one `golive_g4_finals` row per required target type and split generation (a second run fails on the
primary key), with the pins the window ran with plus the static digests (`pins`), and the `gate_flags` row
(G4, passed, evidence_ref, decided_by) in one transaction, as `hdt_scorer`. The console live gate
(`hdt.configapi.routes.mode.read_live_gate`) reads the newest final evaluation, never a flag, and refuses
`live` once today's configuration differs from its pins.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from itertools import pairwise
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert

from hdt.core.ids import canonical_sha256
from hdt.db.models.golive import GoLiveG4FinalRow
from hdt.db.models.scoring import GateProgressRow
from hdt.db.models.settings import GateFlagRow
from hdt.golive.ablation import AblationResult, ReplayEvent
from hdt.golive.data import SplitRecord, load_current_splits
from hdt.golive.pins import PINNED_SECTIONS
from hdt.golive.stats import (
    calibration_slope,
    daily_series,
    max_drawdown,
    psr,
    returns_from_pnl,
    sharpe,
    spiegelhalter_z,
)
from hdt.scoring.calibration import reliability

GATE = "G4"
DECIDED_BY = "scorer:g4"
PAPER = "paper"
ACCOUNT = "account"
PINS_AGREE = "pins_agree"


@dataclass(frozen=True)
class G4Thresholds:
    min_entries: int = 150
    z_max: float = 1.96
    sharpe_min: float = 1.5
    psr_min: float = 0.95
    max_drawdown: float = 0.10


@dataclass(frozen=True)
class Check:
    key: str
    label: str
    value: float | None
    target: float | None
    met: bool | None
    """None: reported only (never decides)."""
    detail: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "label": self.label,
            "value": self.value,
            "target": self.target,
            "met": self.met,
            **({"detail": self.detail} if self.detail else {}),
        }


DECIDING = frozenset(
    {
        "n_entries",
        "spiegelhalter_z",
        "calibration_slope",
        "sharpe",
        "psr",
        "max_drawdown",
        "pinned",
        "ablation",
    }
)
ACCOUNT_DECIDING = frozenset({"sharpe", "psr", "max_drawdown"})


def pinned_check(target_type: str, events: Sequence[ReplayEvent]) -> Check:
    """The window's scored cards all ran with the same model slug and agent version per agent and the same
    `models` and `council` config versions. When they did, `detail["pins"]` holds those pins: `models` and
    `council` (config version id, None: no console version was active) and `agents` (agent ->
    `{model_slug, agent_version}`)."""
    sections: dict[str, set[int | None]] = defaultdict(set)
    agents: dict[str, set[tuple[str | None, str | None]]] = defaultdict(set)
    for ev in events:
        for section in PINNED_SECTIONS:
            sections[section].add(ev.config_version_ids.get(section))
        for agent, pin in ev.agent_pins.items():
            agents[agent].add(pin)
    changed = [f"config:{s}" for s in PINNED_SECTIONS if len(sections[s]) > 1] + [
        f"agent:{a}" for a in sorted(agents) if len(agents[a]) > 1
    ]
    if changed:
        detail: dict[str, Any] = {"changed": changed}
    elif not events:
        detail = {"reason": "no evaluation events"}
    else:
        pins: dict[str, Any] = {s: next(iter(sections[s])) for s in PINNED_SECTIONS}
        pins["agents"] = {
            a: dict(zip(("model_slug", "agent_version"), next(iter(agents[a])), strict=True))
            for a in sorted(agents)
        }
        detail = {"pins": pins}
    return Check(
        "pinned",
        f"{target_type}: model slugs, agent versions and models / council config pinned over the window",
        float(len(changed)),
        0.0,
        bool(events) and not changed,
        detail,
    )


@dataclass(frozen=True)
class TargetInputs:
    target_type: str
    eval_start: datetime | None
    end: datetime
    forecasts: Sequence[tuple[float, int]]
    """(pooled p, y) of every independent forecast of the evaluation set."""
    entries: int
    daily_pnl: Mapping[date, float]
    equity0: float
    ablation: AblationResult | None
    unattributed_pnl: float = 0.0
    pinned: Check | None = None
    """`pinned_check` of the window (None: no evaluation events, the check fails)."""
    final: bool = True
    """False for the weekly interim result: the ablation is not computed on evaluation events."""
    sealed: bool = False


@dataclass(frozen=True)
class TargetResult:
    target_type: str
    checks: list[Check]
    days: int
    eval_start: datetime | None = None
    eval_end: datetime | None = None
    sealed: bool = False

    @property
    def passed(self) -> bool:
        deciding = [c for c in self.checks if c.key in DECIDING]
        return len(deciding) == len(DECIDING) and all(c.met is True for c in deciding)

    @property
    def pins(self) -> dict[str, Any] | None:
        """The `models` / `council` config version ids and agent pins of the window (`pinned_check`); None
        when the window was not pinned (no evaluation events, or a pin changed inside it)."""
        pinned = next((c for c in self.checks if c.key == "pinned"), None)
        if pinned is None or pinned.met is not True:
            return None
        pins = pinned.detail.get("pins")
        return dict(pins) if isinstance(pins, Mapping) else None

    def to_json(self) -> dict[str, Any]:
        return {
            "target_type": self.target_type,
            "passed": self.passed,
            "days": self.days,
            "eval_start": self.eval_start.isoformat() if self.eval_start else None,
            "eval_end": self.eval_end.isoformat() if self.eval_end else None,
            "sealed": self.sealed,
            "checks": [c.to_json() for c in self.checks],
        }


DEFAULT_THRESHOLDS = G4Thresholds()


def _pnl_checks(
    prefix: str, returns: Sequence[float], equity: Sequence[float], th: G4Thresholds
) -> list[Check]:
    sr = sharpe(returns) if len(returns) >= 2 else None
    prob = psr(returns) if len(returns) >= 2 else None
    dd = max_drawdown(equity) if len(equity) >= 2 else None
    return [
        Check(
            "sharpe",
            f"{prefix}: annualized Sharpe of daily PnL (no-trade days included)",
            sr,
            th.sharpe_min,
            sr > th.sharpe_min if sr is not None else False,
            {"days": len(returns)},
        ),
        Check(
            "psr", f"{prefix}: PSR(0)", prob, th.psr_min, prob >= th.psr_min if prob is not None else False
        ),
        Check(
            "max_drawdown",
            f"{prefix}: max drawdown of the equity curve",
            dd,
            th.max_drawdown,
            dd < th.max_drawdown if dd is not None else False,
            {"start_equity": equity[0] if equity else None, "final_equity": equity[-1] if equity else None},
        ),
    ]


def evaluate_target(inputs: TargetInputs, th: G4Thresholds = DEFAULT_THRESHOLDS) -> TargetResult:
    t = inputs.target_type
    checks: list[Check] = []
    p = [pv for pv, _ in inputs.forecasts]
    y = [yv for _, yv in inputs.forecasts]
    checks.append(
        Check(
            "n_entries",
            f"{t}: independent paper entry orders on the evaluation set",
            float(inputs.entries),
            float(th.min_entries),
            inputs.entries >= th.min_entries,
        )
    )
    checks.append(Check("n_forecasts", f"{t}: independent forecasts scored", float(len(p)), None, None))
    z = spiegelhalter_z(p, y) if p else None
    checks.append(
        Check(
            "spiegelhalter_z",
            f"{t}: calibration |Spiegelhalter Z|",
            abs(z) if z is not None else None,
            th.z_max,
            abs(z) < th.z_max if z is not None else False,
            {"z": z},
        )
    )
    fit = calibration_slope(p, y)
    checks.append(
        Check(
            "calibration_slope",
            f"{t}: calibration slope (95% CI contains 1)",
            fit.slope if fit else None,
            1.0,
            fit.contains_one if fit else False,
            {"ci_low": fit.ci_low, "ci_high": fit.ci_high, "intercept": fit.intercept} if fit else {},
        )
    )
    ece = reliability(p, y, bins=10).ece if p else None
    checks.append(Check("ece", f"{t}: ECE (reported only)", ece, None, None))
    if inputs.eval_start is not None:
        last_day = (inputs.end - timedelta(microseconds=1)).date()
        series = daily_series(inputs.daily_pnl, inputs.eval_start.date(), last_day)
    else:
        series = []
    pnl = [v for _, v in series]
    if pnl and inputs.equity0 > 0:
        returns, equity = returns_from_pnl(pnl, inputs.equity0)
    else:
        returns, equity = [], []
    pnl_checks = _pnl_checks(f"{t} realized paper PnL", returns, equity, th)
    pnl_checks[0].detail["trading_days"] = sum(1 for v in pnl if v != 0)
    checks += pnl_checks
    checks.append(
        Check(
            "net_pnl",
            f"{t}: net realized paper PnL in the window (USD)",
            float(sum(pnl)),
            None,
            None,
            {"unattributed_usd": inputs.unattributed_pnl},
        )
    )
    checks.append(
        inputs.pinned
        if inputs.pinned is not None
        else Check("pinned", f"{t}: pinned models and config (no evaluation events)", None, 0.0, False)
    )
    if not inputs.final:
        checks.append(
            Check("ablation", f"{t}: ablation (computed by the final evaluation only)", None, None, None)
        )
    elif inputs.ablation is not None:
        retained = [c for c in inputs.ablation.components if c.retained]
        wins = sum(1 for c in retained if c.wins is True)
        checks.append(
            Check(
                "ablation",
                f"{t}: retained components winning the paired log-loss ablation (Holm), no replay mismatch",
                float(wins),
                float(len(retained)),
                inputs.ablation.retained_components_win,
                {
                    "losers": [c.name for c in retained if c.wins is not True],
                    "replay_mismatches": inputs.ablation.replay_mismatches,
                },
            )
        )
    else:
        checks.append(Check("ablation", f"{t}: ablation (no evaluation events)", None, None, False))
    return TargetResult(t, checks, len(pnl), inputs.eval_start, inputs.end, inputs.sealed)


def evaluate_account(equity: Sequence[float], th: G4Thresholds = DEFAULT_THRESHOLDS) -> list[Check]:
    """Sharpe, PSR and max drawdown of the whole paper account's end-of-day equity (`equity[0]`: start).
    A non-positive equity point leaves no return series (the Sharpe checks fail)."""
    positive = all(v > 0 for v in equity)
    returns = [b / a - 1.0 for a, b in pairwise(equity)] if positive else []
    return _pnl_checks("whole paper account (unrealized included)", returns, equity, th)


@dataclass(frozen=True)
class G4Result:
    computed_at: datetime
    required: tuple[str, ...]
    targets: dict[str, TargetResult]
    missing_split: tuple[str, ...]
    final: bool = False
    account: list[Check] = field(default_factory=list)
    account_window: tuple[datetime, datetime] | None = None

    @property
    def account_passed(self) -> bool:
        deciding = [c for c in self.account if c.key in ACCOUNT_DECIDING]
        return len(deciding) == len(ACCOUNT_DECIDING) and all(c.met is True for c in deciding)

    @property
    def pins_agree(self) -> bool:
        """Every pinned required target type's window ran with the same pins. Finals pinned to different
        versions could never match one active configuration, so they fail the gate (an unpinned window fails
        its own `pinned` check)."""
        pins = [self.targets[t].pins for t in self.required if t in self.targets]
        return len({canonical_sha256(p) for p in pins if p is not None}) <= 1

    @property
    def passed(self) -> bool:
        return (
            bool(self.required)
            and not self.missing_split
            and all(t in self.targets and self.targets[t].passed for t in self.required)
            and self.account_passed
            and self.pins_agree
        )

    def to_json(self) -> dict[str, Any]:
        return {
            "gate": GATE,
            "stage": "final" if self.final else "interim",
            "computed_at": self.computed_at.isoformat(),
            "passed": self.passed,
            "required": list(self.required),
            "missing_split": list(self.missing_split),
            PINS_AGREE: self.pins_agree,
            "account": {
                "window": [t.isoformat() for t in self.account_window] if self.account_window else None,
                "passed": self.account_passed,
                "checks": [c.to_json() for c in self.account],
            },
            "targets": {k: v.to_json() for k, v in sorted(self.targets.items())},
        }


def _row(result: G4Result, key: str, label: str, value: Any, target: Any, met: bool | None) -> dict[str, Any]:
    return {
        "gate": GATE,
        "check_key": key,
        "label": label,
        "value": value,
        "target": target,
        "met": met,
        "updated_at": result.computed_at,
    }


def progress_rows(result: G4Result) -> list[dict[str, Any]]:
    """`gate_progress` rows: one per check and target type, the account checks, `pins_agree`, plus `overall`
    (its `met` is NULL for an interim result: only the final evaluation decides)."""
    rows = [
        _row(result, f"{target}.{c.key}", c.label, c.value, c.target, c.met)
        for target, tr in sorted(result.targets.items())
        for c in tr.checks
    ]
    rows += [_row(result, f"{ACCOUNT}.{c.key}", c.label, c.value, c.target, c.met) for c in result.account]
    rows += [
        _row(result, f"{target}.split", f"{target}: calibration / evaluation split set", None, None, False)
        for target in result.missing_split
    ]
    rows.append(
        _row(
            result,
            PINS_AGREE,
            "every required target type's window ran with the same models, council and agent pins",
            None,
            None,
            result.pins_agree,
        )
    )
    passing = sum(1 for t in result.required if t in result.targets and result.targets[t].passed)
    label = (
        "G4 final evaluation: every required target type and the account pass"
        if result.final
        else "G4 interim (decided only by the final evaluation, python -m hdt.golive.final)"
    )
    rows.append(
        _row(
            result,
            "overall",
            label,
            float(passing),
            float(len(result.required)),
            result.passed if result.final else None,
        )
    )
    return rows


def write_progress(conn: sa.Connection, result: G4Result, *, decided: Collection[str] = ()) -> int:
    """Upsert the G4 rows of `gate_progress`; returns how many rows were written.

    `decided`: the target types whose current split already has a final evaluation (the weekly interim
    run). Their rows, and the account, `pins_agree` and `overall` rows the final wrote, are left as the final
    recorded them."""
    rows = progress_rows(result)
    if decided:
        skipped = {*decided, ACCOUNT}
        rows = [r for r in rows if "." in r["check_key"] and r["check_key"].split(".", 1)[0] not in skipped]
    if not rows:
        return 0
    stmt = insert(GateProgressRow)
    conn.execute(
        stmt.on_conflict_do_update(
            index_elements=["gate", "check_key"],
            set_={k: stmt.excluded[k] for k in ("label", "value", "target", "met", "updated_at")},
        ),
        rows,
    )
    return len(rows)


def final_rows(
    result: G4Result,
    evidence_ref: str,
    *,
    splits: Mapping[str, SplitRecord],
    static_pins: Mapping[str, str],
) -> list[dict[str, Any]]:
    """The `golive_g4_finals` rows of a final result: one per required target type on its current split
    generation (`splits`), with the window's `pins` plus `static_pins` (`hdt.golive.pins.static_digests`),
    SQL NULL when the window was not pinned."""
    if not result.final:
        raise ValueError("only a final G4 result is recorded")
    if result.missing_split or any(t not in result.targets for t in result.required):
        raise ValueError("every required target type needs a split and a result")
    if not result.required:
        raise ValueError("no required target type")
    rows = []
    for target in result.required:
        tr = result.targets[target]
        if tr.eval_start is None or tr.eval_end is None or not tr.sealed:
            raise ValueError(f"{target}: the evaluation window is not sealed")
        split = splits.get(target)
        if split is None or split.eval_start != tr.eval_start:
            raise ValueError(f"{target}: the evaluated window does not start at the current split")
        pins = tr.pins
        rows.append(
            {
                "target_type": target,
                "generation": split.generation,
                "eval_start": tr.eval_start,
                "eval_end": tr.eval_end,
                "passed": tr.passed,
                "gate_passed": result.passed,
                "evidence_ref": evidence_ref,
                "result": tr.to_json(),
                "pins": {**pins, "static": dict(static_pins)} if pins is not None else None,
            }
        )
    return rows


def write_final(
    conn: sa.Connection, result: G4Result, evidence_ref: str, *, static_pins: Mapping[str, str]
) -> None:
    """Record the final evaluation in the caller's transaction: the `final_rows` of the current splits, the
    G4 `gate_flags` row (the database accepts it only as the outcome of that newest final) and the
    `gate_progress` rows. A second final evaluation of a split raises `IntegrityError` (primary key) and
    writes nothing."""
    rows = final_rows(result, evidence_ref, splits=load_current_splits(conn), static_pins=static_pins)
    conn.execute(sa.insert(GoLiveG4FinalRow), rows)
    conn.execute(
        sa.insert(GateFlagRow).values(
            gate=GATE,
            passed=result.passed,
            evidence_ref=evidence_ref,
            decided_by=DECIDED_BY,
            decided_at=result.computed_at,
        )
    )
    write_progress(conn, result)
