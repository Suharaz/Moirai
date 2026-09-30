"""Initial `p_model` coefficients (phase 03, Design Contract section 3), used by `scripts/fit_p_model.py`.

Samples: every independent candidate event of the recorded lake (loose thresholds, a superset of strict, both
rules; one per (coin, non-overlapping 12 h window)), with the agent's own feature block at the
event `as_of` (the same `Snapshot.agent_block` the live packet uses) and the 12 h label of the event's own
setup (Design Contract section 4): LTX events carry `RESID_12H` (residual after the beta hedge), Migration
events `RAW_12H` (mark return); y = 1 when it is > 0. Target types are never pooled: an agent file declares
one `target_type` and is fit only on the events of that target type.

Fit: L2 logistic regression on z-scores (mean / std of the training rows; a missing value is z = 0, the
fit-time mean, exactly as `PModel.logit` treats it). Walk-forward (`walk_forward_folds`): the distinct event
instants in time order are cut into `folds` blocks, so every row of one `as_of` lands in one block; each
block from the second on is predicted by a model fit on the earlier rows whose 12 h label had resolved when
the block starts (purge: `as_of + 12 h <= first as_of of the block`). A block with no such training row is
not scored. The reported out-of-sample log-loss is the row-weighted mean over the scored blocks. A feature
family with fewer than `min_family_samples` rows carrying any of its values is shrunk to 0 and listed in
`shrunk_families`. The result is a new version file for review; the active
`config/p_model/<agent>.<target>.yaml` is not edited.

The window is never chosen by the caller: `passed_g1` returns the `[start, end)` of the passed G1 study
recorded in `reports/g1/latest.json` and refuses unless the study's seed, rule, label and feature versions and
config pins (`hdt.quant.study.config_pins`) are the current ones.
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
from sklearn.linear_model import LogisticRegression

from hdt.contracts.common import AgentName, TargetType
from hdt.core.clock import ensure_utc
from hdt.core.config import PModelFile, PModelFitFile, ScannerFile, Standardization, StaticConfig
from hdt.features.engine import FeatureEngine, feature_version
from hdt.features.lake_io import LakeView
from hdt.lake.pit_query import PitQuery
from hdt.quant.gates import (
    BOOTSTRAP_RESAMPLES,
    BOOTSTRAP_SEED,
    DECISION_HORIZON_H,
    LATEST_FILE,
    G1Study,
    Trigger,
    g1_status,
    independent_events,
    parse_status,
)
from hdt.quant.labels import MarkBook, resolve_label
from hdt.quant.p_model import numeric
from hdt.quant.study import config_pins, scan_slots, target_of

_EPS = 1e-12


@dataclass(frozen=True)
class Sample:
    coin_id: int
    rule: str
    as_of: datetime
    features: dict[AgentName, dict[str, float | None]]
    labels: dict[TargetType, int | None]


def collect_samples(
    pit: PitQuery,
    static: StaticConfig,
    scanner: ScannerFile,
    start: datetime,
    end: datetime,
    specs: Sequence[PModelFile],
    *,
    progress: Callable[[datetime], None] | None = None,
) -> list[Sample]:
    """Independent loose-threshold events of `[start, end)`: every agent's features and the setup label."""
    feature_names: dict[AgentName, set[str]] = {}
    for spec in specs:
        feature_names.setdefault(AgentName(spec.agent), set()).update(spec.coefficients)
    view = LakeView(pit)
    engine = FeatureEngine(view, static, scanner, max_snapshots=2)
    marks = MarkBook(view)
    window = timedelta(hours=DECISION_HORIZON_H)
    end = ensure_utc(end)
    found: list[tuple[Trigger, Sample]] = []
    for slot in scan_slots(engine, pit, static, scanner, start, end):
        if progress is not None:
            progress(slot.as_of)
        snap = slot.snap
        for ev in slot.evaluations:
            if not ev.loose_pass or ev.side is None or slot.as_of + window > end:
                continue
            member = snap.member(ev.coin_id)
            if member is None:
                continue
            features: dict[AgentName, dict[str, float | None]] = {}
            for agent, names in feature_names.items():
                values = snap.agent_block(agent, member).values
                features[agent] = {name: numeric(values.get(name)) for name in names}
            beta = snap.beta(member) if ev.rule.value == "LTX" else None
            target = target_of(ev.rule.value)
            label = resolve_label(
                view,
                symbol=member.binance_symbol,
                btc_symbol=snap.btc.binance_symbol if snap.btc is not None else "",
                as_of=slot.as_of,
                horizon_h=DECISION_HORIZON_H,
                target_type=target,
                beta_btc=beta,
                params=scanner.labels,
                marks=marks.mark,
            )
            labels = {target: label.y}
            trig = Trigger(ev.coin_id, ev.rule.value, ev.side, slot.as_of)
            found.append((trig, Sample(ev.coin_id, ev.rule.value, slot.as_of, features, labels)))
    by_key = {(t.coin_id, t.rule, t.as_of): s for t, s in found}
    kept = independent_events([t for t, _ in found], window)
    return [by_key[(t.coin_id, t.rule, t.as_of)] for t in kept]


@dataclass(frozen=True)
class FitResult:
    spec: PModelFile
    rows: int
    active: tuple[str, ...]
    oos_rows: int


def _standardize(x: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Column mean / std over non-missing values (std 1 when undefined)."""
    mean = np.zeros(x.shape[1])
    std = np.ones(x.shape[1])
    for j in range(x.shape[1]):
        col = x[:, j][~np.isnan(x[:, j])]
        if col.size:
            mean[j] = float(col.mean())
        if col.size > 1 and float(col.std(ddof=1)) > _EPS:
            std[j] = float(col.std(ddof=1))
    return mean, std


def _z(x: np.ndarray, mean: np.ndarray, std: np.ndarray) -> np.ndarray:
    z = (x - mean) / std
    return np.where(np.isnan(z), 0.0, z)


def _fit(x: np.ndarray, y: np.ndarray, c: float) -> tuple[float, np.ndarray]:
    """Intercept and coefficients; a one-class sample gives the smoothed base rate and zero slopes."""
    if x.shape[1] == 0 or len(set(y.tolist())) < 2:
        rate = (float(y.sum()) + 0.5) / (len(y) + 1.0)
        return math.log(rate / (1.0 - rate)), np.zeros(x.shape[1])
    model = LogisticRegression(C=c, l1_ratio=0.0, solver="lbfgs", max_iter=1000)
    model.fit(x, y)
    return float(model.intercept_[0]), model.coef_[0].astype(float)


def _logloss(p: np.ndarray, y: np.ndarray) -> float:
    p = np.clip(p, 0.02, 0.98)
    return float(-(y * np.log(p) + (1 - y) * np.log(1 - p)).sum())


def next_version(current: PModelFile) -> str:
    """`<agent>-<target>-pm<N+1>` from the active file's `p_model_ver`."""
    prefix = f"{current.agent}-{current.target_type.lower()}-pm"
    match = re.fullmatch(rf"{re.escape(prefix)}(\d+)", current.p_model_ver)
    return f"{prefix}{int(match.group(1)) + 1 if match else 1}"


def walk_forward_folds(
    as_of: Sequence[datetime], folds: int, horizon: timedelta
) -> list[tuple[np.ndarray, np.ndarray]]:
    """(train, test) row indices of every scored walk-forward block of the rows' `as_of`.

    The distinct instants in time order are cut into `folds` blocks, so all rows of one instant share a block.
    Block k >= 2 is a test set; its training rows are the earlier rows whose label window
    `[as_of, as_of + horizon]` has closed by the block's first instant (purge). A block without a training
    row is left out; fewer distinct instants than `folds` gives no block.
    """
    instants = sorted(set(as_of))
    if len(instants) < folds:
        return []
    cuts = np.linspace(0, len(instants), folds + 1).astype(int)
    out: list[tuple[np.ndarray, np.ndarray]] = []
    for k in range(1, folds):
        first = instants[cuts[k]]
        after = instants[cuts[k + 1]] if k + 1 < folds else None
        test = [i for i, t in enumerate(as_of) if t >= first and (after is None or t < after)]
        train = [i for i, t in enumerate(as_of) if t + horizon <= first]
        if train:
            out.append((np.array(train, dtype=np.intp), np.array(test, dtype=np.intp)))
    return out


def fit_agent(
    agent: AgentName,
    current: PModelFile,
    samples: Sequence[Sample],
    params: PModelFitFile,
    *,
    source: str,
) -> FitResult:
    """Fit `current`'s agent and target type; `source` names the passed G1 report the window comes from."""
    c, folds, min_family_samples = params.l2_c, params.folds, params.min_family_samples
    target = TargetType(current.target_type)
    rows = sorted(
        (s for s in samples if target_of(s.rule) is target and s.labels.get(target) is not None),
        key=lambda s: (s.as_of, s.coin_id, s.rule),
    )
    names = sorted(current.coefficients)
    x_all = np.array(
        [[np.nan if (v := s.features[agent].get(n)) is None else v for n in names] for s in rows],
        dtype=np.float64,
    ).reshape(len(rows), len(names))
    y = np.array([s.labels[target] for s in rows], dtype=np.float64)
    shrunk = []
    for family, members in sorted(current.families.items()):
        cols = [names.index(n) for n in members]
        present = int((~np.isnan(x_all[:, cols])).any(axis=1).sum()) if rows else 0
        if present < min_family_samples:
            shrunk.append(family)
    shrunk_names = {n for f in shrunk for n in current.families[f]}
    active = tuple(n for n in names if n not in shrunk_names)
    idx = [names.index(n) for n in active]
    x = x_all[:, idx] if idx else np.zeros((len(rows), 0))

    loss, scored = 0.0, 0
    horizon = timedelta(hours=DECISION_HORIZON_H)
    for train, test in walk_forward_folds([s.as_of for s in rows], folds, horizon):
        mean, std = _standardize(x[train])
        b0, b = _fit(_z(x[train], mean, std), y[train], c)
        logits = b0 + _z(x[test], mean, std) @ b
        loss += _logloss(1.0 / (1.0 + np.exp(-logits)), y[test])
        scored += len(test)

    mean, std = _standardize(x)
    b0, b = _fit(_z(x, mean, std), y, c) if rows else (0.0, np.zeros(len(active)))
    coefficients = {n: 0.0 for n in names}
    standardization = {n: Standardization(mean=0.0, std=1.0) for n in names}
    for j, name in enumerate(active):
        coefficients[name] = float(b[j])
        standardization[name] = Standardization(mean=float(mean[j]), std=float(std[j]))
    fitted = PModelFile(
        schema_version=1,
        agent=current.agent,
        p_model_ver=next_version(current),
        target_type=current.target_type,
        label_spec_version=current.label_spec_version,
        fit_window_start=rows[0].as_of.date() if rows else None,
        fit_window_end=rows[-1].as_of.date() if rows else None,
        sample_count=len(rows),
        oos_logloss=loss / scored if scored else None,
        intercept=b0,
        coefficients=coefficients,
        standardization=standardization,
        families=current.families,
        shrunk_families=tuple(shrunk),
        note=(
            f"Fit {params.fit_version} (L2 C={c}) on the window of G1 report {source}: {len(rows)} "
            f"independent loose-threshold {target.value} events; "
            f"walk-forward {folds} time blocks purged by the {DECISION_HORIZON_H} h label horizon, "
            f"out-of-sample log-loss over {scored} rows. "
            + (
                f"Shrunk to 0 for lack of data (< {min_family_samples} rows): {', '.join(shrunk)}."
                if shrunk
                else "No family shrunk."
            )
        ),
    )
    return FitResult(fitted, len(rows), active, scored)


class FitRefusedError(RuntimeError):
    """Gate G1 has not passed on exactly the current study inputs: the fit may not run."""


def passed_g1(
    reports_root: Path, repo_root: Path, static: StaticConfig, scanner: ScannerFile
) -> tuple[G1Study, str]:
    """The study of the passed G1 decision in `reports_root/latest.json` and its report path.

    Refuses unless `latest.json` reads `passed` with a complete study record, the JSON report it names holds
    the same status (same window and pins), and the study's bootstrap seed and resamples, rule, label and
    feature versions and config pins equal the current ones. The fit window is the study's `[start, end)`.
    """
    status = g1_status(reports_root)
    if not status.passed or status.report_path is None:
        raise FitRefusedError(f"gate G1 is {status.state}: p_model is fit only after G1 passes")
    study = status.study
    if study is None:
        raise FitRefusedError(
            f"{LATEST_FILE} records no complete study inputs: rerun scripts/g1_event_study.py"
        )
    report_path = Path(status.report_path)
    report_file = report_path if report_path.is_absolute() else repo_root / report_path
    try:
        in_report = parse_status(json.loads(report_file.read_text(encoding="utf-8"))["status"])
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise FitRefusedError(f"G1 report {report_path} is unreadable: {exc}") from None
    if in_report != status:
        raise FitRefusedError(f"G1 report {report_path} does not hold the status of {LATEST_FILE}")
    current: dict[str, object] = {
        "bootstrap_seed": BOOTSTRAP_SEED,
        "bootstrap_resamples": BOOTSTRAP_RESAMPLES,
        "rule_version": scanner.rule_version,
        "label_spec_version": scanner.labels.label_spec_version,
        "feature_ver": feature_version(static, scanner),
    }
    recorded = study.to_json()
    stale = [key for key, value in current.items() if recorded[key] != value]
    pins = config_pins(static, scanner)
    stale += [
        f"config_pins.{name}"
        for name in sorted(pins.keys() | study.config_pins.keys())
        if pins.get(name) != study.config_pins.get(name)
    ]
    if stale:
        raise FitRefusedError(
            f"G1 study {report_path} was computed with other inputs than the current ones: {', '.join(stale)}"
        )
    return study, report_path.as_posix()
