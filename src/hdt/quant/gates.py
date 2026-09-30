"""Gate G1 (Design Contract section 9, `docs/g1-preregistration.md`): statistics, decision and status.

Pure statistics used by `scripts/g1_event_study.py`:
- `independent_events`: one event per (coin, non-overlapping 12 h window), the first trigger of either rule
  opening the window (later triggers of the same coin inside it are ignored; same instant: LTX first);
- `day_block_bootstrap`: mean with a UTC-day block bootstrap (fixed seed) and its percentile CI;
- `n_min`: `((1.96 + 0.84) x sigma / delta)^2`, with `delta = 2 x round-trip cost`;
- `decide`: `insufficient_power` when N < N_min, `failed` when the CI contains 0, else `passed`.

The preregistered question covers both triggers, so the overall gate state combines the LTX and Migration
decisions conservatively (`combine`): any `failed` fails the gate, then any `insufficient_data`, then any
`insufficient_power`; the gate passes only when every rule passes. Each rule's own state is reported.

`g1_status(reports_root)` reads `reports_root/latest.json`, written by the event study. Phases 05-08 stay
blocked until it reads `passed` (plan: G1 blocks them). The status carries the study it was computed on
(`G1Study`: window, seed, rule / label / feature versions, config pins), which `scripts/fit_p_model.py`
binds to.
"""

from __future__ import annotations

import json
import math
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Literal

import numpy as np

from hdt.core.clock import ensure_utc

G1State = Literal["insufficient_data", "insufficient_power", "passed", "failed"]
Z_ALPHA = 1.96
"""Two-sided alpha 0.05 (preregistered)."""
Z_POWER = 0.84
"""Power 0.8 (preregistered)."""
BOOTSTRAP_RESAMPLES = 10_000
BOOTSTRAP_SEED = 20260927
DECISION_HORIZON_H = 12
LATEST_FILE = "latest.json"


@dataclass(frozen=True)
class Trigger:
    coin_id: int
    rule: str
    side: str
    as_of: datetime


def independent_events(triggers: Iterable[Trigger], window: timedelta) -> list[Trigger]:
    """One event per (coin, non-overlapping `window`), whatever the rule: the first trigger opens the window
    and every later trigger of the coin inside it (either rule) is dropped. Triggers of one coin at the same
    instant are ordered by rule name (`LTX` before `MIGRATION`), so the choice is deterministic."""
    out: list[Trigger] = []
    open_until: dict[int, datetime] = {}
    for trig in sorted(triggers, key=lambda t: (ensure_utc(t.as_of), t.coin_id, t.rule)):
        at = ensure_utc(trig.as_of)
        until = open_until.get(trig.coin_id)
        if until is not None and at < until:
            continue
        open_until[trig.coin_id] = at + window
        out.append(trig)
    return out


@dataclass(frozen=True)
class BootstrapResult:
    n: int
    days: int
    mean: float
    ci_low: float
    ci_high: float


def day_block_bootstrap(
    values: Sequence[float],
    days: Sequence[date],
    *,
    resamples: int = BOOTSTRAP_RESAMPLES,
    seed: int = BOOTSTRAP_SEED,
    level: float = 0.95,
) -> BootstrapResult | None:
    """Mean of `values` and a percentile CI from resampling whole UTC days with replacement."""
    if len(values) != len(days):
        raise ValueError("values and days must have the same length")
    if not values:
        return None
    by_day: dict[date, list[float]] = defaultdict(list)
    for value, day in zip(values, days, strict=True):
        by_day[day].append(float(value))
    ordered = sorted(by_day)
    sums = np.array([sum(by_day[d]) for d in ordered], dtype=np.float64)
    counts = np.array([len(by_day[d]) for d in ordered], dtype=np.float64)
    rng = np.random.default_rng(seed)
    picks = rng.integers(0, len(ordered), size=(resamples, len(ordered)))
    means = sums[picks].sum(axis=1) / counts[picks].sum(axis=1)
    tail = (1.0 - level) / 2.0 * 100.0
    low, high = np.percentile(means, [tail, 100.0 - tail])
    return BootstrapResult(
        n=len(values),
        days=len(ordered),
        mean=float(sums.sum() / counts.sum()),
        ci_low=float(low),
        ci_high=float(high),
    )


def n_min(sigma: float, delta: float) -> int | None:
    """Events needed to detect `delta` with power 0.8 at two-sided 0.05 (None when undefined)."""
    if not math.isfinite(sigma) or sigma <= 0 or delta <= 0:
        return None
    return math.ceil(((Z_ALPHA + Z_POWER) * sigma / delta) ** 2)


def round_trip_cost(taker: float, slippage_bp: float, *, hedge_beta: float | None = None) -> float:
    """Taker fee twice plus slippage twice; a beta hedge adds the same for |beta| notional of BTC."""
    leg = 2.0 * taker + 2.0 * slippage_bp / 10_000.0
    return leg * (1.0 + abs(hedge_beta)) if hedge_beta is not None else leg


def decide(n: int, needed: int | None, ci: BootstrapResult | None) -> G1State:
    if n == 0 or needed is None or ci is None:
        return "insufficient_data"
    if n < needed:
        return "insufficient_power"
    if ci.ci_low <= 0.0 <= ci.ci_high:
        return "failed"
    return "passed" if ci.ci_low > 0.0 else "failed"


@dataclass(frozen=True)
class RuleResult:
    rule: str
    state: G1State
    n_events: int
    n_min: int | None
    sigma: float | None
    delta: float | None
    dropped: dict[str, int]
    horizons: dict[str, BootstrapResult | None]
    """`"12"` decides; 4 / 8 / 24 are reference only."""


@dataclass(frozen=True)
class G1Study:
    """What a G1 decision was computed on; the `p_model` fit runs only on exactly this."""

    start: datetime
    end: datetime
    bootstrap_seed: int
    bootstrap_resamples: int
    rule_version: str
    label_spec_version: str
    feature_ver: str
    config_pins: dict[str, str]
    """Config section -> sha256 of its canonical JSON (`hdt.quant.study.config_pins`)."""

    def to_json(self) -> dict[str, object]:
        out = asdict(self)
        out["start"], out["end"] = self.start.isoformat(), self.end.isoformat()
        return out

    @classmethod
    def from_json(cls, raw: object) -> G1Study | None:
        """None unless every field is present with its type: a partial record binds nothing."""
        if not isinstance(raw, dict):
            return None
        texts = ("start", "end", "rule_version", "label_spec_version", "feature_ver")
        ints = ("bootstrap_seed", "bootstrap_resamples")
        pins = raw.get("config_pins")
        if (
            not all(isinstance(raw.get(k), str) for k in texts)
            or not all(isinstance(raw.get(k), int) and not isinstance(raw.get(k), bool) for k in ints)
            or not isinstance(pins, dict)
            or not pins
            or not all(isinstance(k, str) and isinstance(v, str) for k, v in pins.items())
        ):
            return None
        try:
            start, end = (ensure_utc(datetime.fromisoformat(raw[k])) for k in ("start", "end"))
        except ValueError:
            return None
        return cls(
            start=start,
            end=end,
            bootstrap_seed=raw["bootstrap_seed"],
            bootstrap_resamples=raw["bootstrap_resamples"],
            rule_version=raw["rule_version"],
            label_spec_version=raw["label_spec_version"],
            feature_ver=raw["feature_ver"],
            config_pins=dict(sorted(pins.items())),
        )


@dataclass(frozen=True)
class G1Status:
    state: G1State
    decided_at: datetime | None
    n_events: int
    n_min: int | None
    ci_low: float | None
    ci_high: float | None
    report_path: str | None
    rules: dict[str, G1State] = field(default_factory=dict)
    study: G1Study | None = None

    @property
    def passed(self) -> bool:
        return self.state == "passed"

    def to_json(self) -> dict[str, object]:
        out = asdict(self)
        out["decided_at"] = self.decided_at.isoformat() if self.decided_at else None
        out["study"] = self.study.to_json() if self.study else None
        return out


NO_REPORT = G1Status("insufficient_data", None, 0, None, None, None, None)


_SEVERITY: tuple[G1State, ...] = ("failed", "insufficient_data", "insufficient_power", "passed")


def combine(states: Iterable[G1State]) -> G1State:
    """The gate state over every preregistered rule: the least favourable one (no rules: no data)."""
    found = set(states)
    if not found:
        return "insufficient_data"
    return next(state for state in _SEVERITY if state in found)


def status_from_rules(
    rules: dict[str, RuleResult], decided_at: datetime, report_path: str | None, *, study: G1Study
) -> G1Status:
    """The overall status; its N / N_min / CI are those of the rule deciding the combined state."""
    state = combine(result.state for result in rules.values())
    deciding = next((rules[name] for name in sorted(rules) if rules[name].state == state), None)
    decision = deciding.horizons.get(str(DECISION_HORIZON_H)) if deciding else None
    return G1Status(
        state=state,
        decided_at=ensure_utc(decided_at),
        n_events=deciding.n_events if deciding else 0,
        n_min=deciding.n_min if deciding else None,
        ci_low=decision.ci_low if decision else None,
        ci_high=decision.ci_high if decision else None,
        report_path=report_path,
        rules={name: result.state for name, result in sorted(rules.items())},
        study=study,
    )


def write_status(reports_root: Path, status: G1Status) -> Path:
    reports_root.mkdir(parents=True, exist_ok=True)
    path = reports_root / LATEST_FILE
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(status.to_json(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)
    return path


def g1_status(reports_root: Path) -> G1Status:
    """The latest G1 decision; no report (or an unreadable one) means `insufficient_data` (fail closed)."""
    path = reports_root / LATEST_FILE
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return NO_REPORT
    return parse_status(raw)


def parse_status(raw: object) -> G1Status:
    """A `G1Status.to_json` record; anything unrecognisable is `NO_REPORT` (fail closed)."""
    if not isinstance(raw, dict):
        return NO_REPORT
    state = raw.get("state")
    if state not in ("insufficient_data", "insufficient_power", "passed", "failed"):
        return NO_REPORT
    decided = raw.get("decided_at")
    rules = raw.get("rules")
    return G1Status(
        state=state,
        decided_at=ensure_utc(datetime.fromisoformat(decided)) if isinstance(decided, str) else None,
        n_events=int(raw.get("n_events") or 0),
        n_min=raw.get("n_min") if isinstance(raw.get("n_min"), int) else None,
        ci_low=_float(raw.get("ci_low")),
        ci_high=_float(raw.get("ci_high")),
        report_path=raw.get("report_path") if isinstance(raw.get("report_path"), str) else None,
        rules={str(k): v for k, v in rules.items()} if isinstance(rules, dict) else {},
        study=G1Study.from_json(raw.get("study")),
    )


def _float(value: object) -> float | None:
    return float(value) if isinstance(value, int | float) and not isinstance(value, bool) else None
