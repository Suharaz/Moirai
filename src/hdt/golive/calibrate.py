"""Threshold calibration on the calibration set only (phase 11 step 3), pure; output is a proposal.

Every helper reads events strictly before the evaluation split (the caller filters them) and returns a
`Proposal`: the grid it searched, the chosen value (None: keep the current one) and why. Nothing here
writes a config file; `config_diff` renders the proposals as a diff for the owner to apply through the
console (runtime values come from pinned console config versions) and the YAML defaults.

- Manager `d_threshold` (`config/council.yaml` `manager.d_threshold`): among fallback-eligible events (no
  consensus under the configured threshold, debate over, pooled `p >= p_long` or `<= p_short`), the fallback
  trades `D < d`; the chosen `d` maximizes the lower bound of the UTC-day block bootstrap 95% CI of the mean
  net 12 h return (label return signed by the side of `p`, minus the round-trip taker fee and slippage; the
  beta hedge leg too for `RESID_12H`). Chosen only when that lower bound is above 0.
- LTX `spike_min` (`config/scanner.yaml` `ltx.strict.spike_min`): same objective over the LTX events with
  `SPIKE >= cut`, direction from the scanner row. A change needs a new scanner `rule_version`.
- Hedge `eta` (`config/scoring.yaml` `hedge.eta`): prequential log-loss of the round-1 log pool whose
  weights are learned online by the sleeping-experts Hedge (`hdt.scoring.hedge.HedgeState`, daily fixed
  share `alpha`), one Hedge per target type; the chosen `eta` minimizes the summed loss over target types.

Each grid point needs at least `min_n` events; the net-return objectives do not include funding (the lake
event study, `hdt.golive.event_study`, reports funding-netted returns).
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from hdt.council.aggregate import logit, sigmoid
from hdt.council.consensus import consensus, normalize
from hdt.golive.ablation import DEBATE_OVER, CouncilRules, ReplayEvent
from hdt.quant.gates import BootstrapResult, day_block_bootstrap, round_trip_cost
from hdt.scoring.hedge import HedgeState
from hdt.scoring.loss import DEFAULT_CLIP, log_loss

D_GRID: tuple[float, ...] = tuple(round(0.1 * k, 2) for k in range(2, 21))
SPIKE_MULTIPLES: tuple[float, ...] = (0.75, 1.0, 1.25, 1.5, 2.0, 2.5)
ETA_GRID: tuple[float, ...] = (0.02, 0.05, 0.08, 0.1, 0.13, 0.16, 0.2, 0.3, 0.5)
MIN_N = 30


@dataclass(frozen=True)
class Proposal:
    file: str
    path: str
    current: float
    proposed: float | None
    """None: keep the current value."""
    grid: list[dict[str, Any]]
    reason: str
    notes: list[str] = field(default_factory=list)

    def to_json(self) -> dict[str, Any]:
        return {
            "file": self.file,
            "path": self.path,
            "current": self.current,
            "proposed": self.proposed,
            "reason": self.reason,
            "grid": self.grid,
            "notes": self.notes,
        }


def net_return(ev: ReplayEvent, side: str, *, taker: float, slippage_bp: float) -> float | None:
    """12 h label return in the direction of `side`, minus the round-trip cost (hedge leg for RESID)."""
    if ev.label_value is None or side not in ("LONG", "SHORT"):
        return None
    hedge = ev.beta_btc if ev.target_type == "RESID_12H" else None
    if ev.target_type == "RESID_12H" and hedge is None:
        return None
    signed = ev.label_value if side == "LONG" else -ev.label_value
    return signed - round_trip_cost(taker, slippage_bp, hedge_beta=hedge)


def _bootstrap_row(value: float, nets: list[tuple[float, ReplayEvent]]) -> dict[str, Any]:
    result: BootstrapResult | None = day_block_bootstrap(
        [v for v, _ in nets], [e.as_of.date() for _, e in nets]
    )
    return {
        "value": value,
        "n": len(nets),
        "mean": result.mean if result else None,
        "ci_low": result.ci_low if result else None,
        "ci_high": result.ci_high if result else None,
    }


def _choose(grid: list[dict[str, Any]], min_n: int) -> dict[str, Any] | None:
    eligible = [g for g in grid if g["n"] >= min_n and g["ci_low"] is not None]
    if not eligible:
        return None
    best = max(eligible, key=lambda g: (g["ci_low"], -g["value"]))
    return best if best["ci_low"] > 0 else None


def _fallback_eligible(ev: ReplayEvent, rules: CouncilRules) -> str | None:
    """The fallback side of `ev` when the manager fallback could apply, else None."""
    p = ev.p_pooled
    if p is None or ev.disagreement is None or ev.stop_reason not in DEBATE_OVER:
        return None
    final = {a.agent: a.p_final for a in ev.agents if a.p_final is not None}
    result = consensus(
        dict(final),
        ev.weights,
        stance_long=rules.stance_long,
        stance_short=rules.stance_short,
        quorum=rules.quorum,
        supermajority=rules.supermajority,
        groups=rules.groups,
    )
    if result.reached:
        return None
    if p >= rules.p_long:
        return "LONG"
    if p <= rules.p_short:
        return "SHORT"
    return None


def calibrate_d_threshold(
    events: Sequence[ReplayEvent],
    rules: CouncilRules,
    *,
    taker: float,
    slippage_bp: float,
    grid: Iterable[float] = D_GRID,
    min_n: int = MIN_N,
) -> Proposal:
    eligible: list[tuple[float, float, ReplayEvent]] = []
    for ev in events:
        side = _fallback_eligible(ev, rules)
        if side is None:
            continue
        net = net_return(ev, side, taker=taker, slippage_bp=slippage_bp)
        if net is not None and ev.disagreement is not None:
            eligible.append((ev.disagreement, net, ev))
    rows = [_bootstrap_row(d, [(net, ev) for dis, net, ev in eligible if dis < d]) for d in sorted(set(grid))]
    best = _choose(rows, min_n)
    reason = (
        f"max CI low of fallback net return at D < {best['value']}"
        if best
        else f"no D with >= {min_n} fallback trades and a CI above 0 ({len(eligible)} eligible events)"
    )
    return Proposal(
        "config/council.yaml",
        "manager.d_threshold",
        rules.d_threshold,
        best["value"] if best and best["value"] != rules.d_threshold else None,
        rows,
        reason,
    )


def calibrate_ltx_threshold(
    events: Sequence[ReplayEvent],
    *,
    current: float,
    floor: float,
    taker: float,
    slippage_bp: float,
    min_n: int = MIN_N,
) -> Proposal:
    """`floor` is the loose `spike_min`: nothing below it was ever convened, so no cut below it is known."""
    ltx: list[tuple[float, float, ReplayEvent]] = []
    for ev in events:
        if ev.source != "LTX" or ev.scan_score is None or ev.scan_side is None:
            continue
        net = net_return(ev, ev.scan_side, taker=taker, slippage_bp=slippage_bp)
        if net is not None:
            ltx.append((ev.scan_score, net, ev))
    cuts = sorted({round(current * k, 6) for k in SPIKE_MULTIPLES if current * k >= floor} | {floor})
    rows = [_bootstrap_row(cut, [(net, ev) for score, net, ev in ltx if score >= cut]) for cut in cuts]
    best = _choose(rows, min_n)
    reason = (
        f"max CI low of LTX net return at SPIKE >= {best['value']}"
        if best
        else f"no SPIKE cut with >= {min_n} events and a CI above 0 ({len(ltx)} LTX events)"
    )
    return Proposal(
        "config/scanner.yaml",
        "ltx.strict.spike_min",
        current,
        best["value"] if best and best["value"] != current else None,
        rows,
        reason,
        ["a changed scanner value needs a new scanner rule_version"],
    )


def _prequential_loss(
    events: Sequence[ReplayEvent], agents: tuple[str, ...], eta: float, alpha: float
) -> float:
    """Summed log-loss of the round-1 log pool with online Hedge weights (predict, then update)."""
    state = HedgeState(agents=agents, eta=eta, alpha=alpha)
    total = 0.0
    for ev in sorted(events, key=lambda e: (e.as_of, e.event_id)):
        opinions = {a.agent: a.p_round1 for a in ev.agents if a.p_round1 is not None and a.agent in agents}
        if not opinions:
            continue
        state.advance_to(ev.as_of.date())
        w = normalize(state.weights(), sorted(opinions))
        p = sigmoid(sum(w[a] * logit(float(v)) for a, v in opinions.items()))
        total += log_loss(p, ev.y, DEFAULT_CLIP)
        state.update({a: log_loss(float(v), ev.y, DEFAULT_CLIP) for a, v in opinions.items()})
    return total


def calibrate_eta(
    events: Sequence[ReplayEvent],
    *,
    agents: tuple[str, ...],
    current: float,
    alpha: float,
    grid: Iterable[float] = ETA_GRID,
    min_n: int = MIN_N,
) -> Proposal:
    by_target: dict[str, list[ReplayEvent]] = {}
    for ev in events:
        by_target.setdefault(ev.target_type, []).append(ev)
    n = sum(len(v) for v in by_target.values())
    rows: list[dict[str, Any]] = []
    for eta in sorted(set(grid) | {current}):
        per_target = {t: _prequential_loss(evs, agents, eta, alpha) for t, evs in sorted(by_target.items())}
        total = sum(per_target.values())
        rows.append(
            {
                "value": eta,
                "n": n,
                "mean_log_loss": total / n if n else None,
                "per_target": {t: v / len(by_target[t]) for t, v in per_target.items()},
            }
        )
    if n < min_n:
        return Proposal(
            "config/scoring.yaml", "hedge.eta", current, None, rows, f"only {n} events (< {min_n})"
        )
    best = min(rows, key=lambda r: (r["mean_log_loss"], abs(r["value"] - current)))
    current_row = next(r for r in rows if r["value"] == current)
    notes = []
    if not math.isclose(best["mean_log_loss"], current_row["mean_log_loss"], rel_tol=0, abs_tol=1e-12):
        notes.append(
            f"mean log-loss {current_row['mean_log_loss']:.6f} at the current eta vs "
            f"{best['mean_log_loss']:.6f} at {best['value']}"
        )
    notes.append("re-run the synthetic oracle test (python -m hdt.scoring.oracle) before applying a new eta")
    return Proposal(
        "config/scoring.yaml",
        "hedge.eta",
        current,
        best["value"] if best["value"] != current else None,
        rows,
        "min prequential log-loss of the Hedge-weighted round-1 pool",
        notes,
    )


def config_diff(proposals: Sequence[Proposal]) -> str:
    """A unified-diff style proposal (never applied by code)."""
    lines: list[str] = []
    for p in proposals:
        if p.proposed is None:
            continue
        lines += [
            f"--- {p.file}",
            f"+++ {p.file} (proposed)",
            f"@@ {p.path} @@",
            f"-{p.path}: {p.current:g}",
            f"+{p.path}: {p.proposed:g}",
        ]
    return "\n".join(lines) + ("\n" if lines else "")


def proposals_json(proposals: Sequence[Proposal]) -> list[dict[str, Any]]:
    return [p.to_json() for p in proposals]


def calibration_events(events: Iterable[ReplayEvent], splits: Mapping[str, Any]) -> list[ReplayEvent]:
    """Events strictly before their target type's split; target types without a split contribute none."""
    out = []
    for ev in events:
        split = splits.get(ev.target_type)
        if split is not None and ev.as_of < split:
            out.append(ev)
    return out
