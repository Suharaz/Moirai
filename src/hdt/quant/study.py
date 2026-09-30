"""Gate G1 event study on the recorded lake (`docs/g1-preregistration.md`) for `scripts/g1_event_study.py`.

One pass over every scanner slot (`cadence_s` grid, `run_delay_s` offset) in `[start, end)`:
- the scanner rules are evaluated point in time exactly as the live scanner does (`evaluate_rules` on the
  feature snapshot of the slot and the universe built at or before it);
- strict triggers (the scanner's `emit_class == "strict"`, with a side) become candidate events; one event per
  (coin, non-overlapping 12 h window) over both rules (`gates.independent_events`);
- loose triggers are counted for the frequency report only;
- every slot and coin without a strict trigger of a rule adds that rule's gross 12 h metric to the non-event
  baseline, whose standard deviation is `sigma` (no event result is read to derive it).

Labels read the future of each slot by design (`hdt.quant.labels`); a label whose horizon ends after `end` is
missing (`immature`), never guessed. The report is a pure function of (lake, config files, start, end, seed):
`decided_at` is `end`, not the wall clock, so a rerun writes identical files.
"""

from __future__ import annotations

import math
from collections import Counter, defaultdict
from collections.abc import Callable, Iterator
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from statistics import median
from typing import Any

from hdt.contracts.common import TargetType
from hdt.core.clock import ensure_utc
from hdt.core.config import ScannerFile, StaticConfig
from hdt.core.ids import canonical_sha256
from hdt.features.engine import FeatureEngine, Snapshot
from hdt.features.lake_io import LakeView
from hdt.lake.pit_query import PitQuery
from hdt.lake.universe import UniverseMember, load_universe
from hdt.quant.gates import (
    BOOTSTRAP_RESAMPLES,
    BOOTSTRAP_SEED,
    DECISION_HORIZON_H,
    BootstrapResult,
    G1Study,
    RuleResult,
    Trigger,
    day_block_bootstrap,
    decide,
    independent_events,
    n_min,
    round_trip_cost,
)
from hdt.quant.labels import MarkBook, resolve_label
from hdt.quant.scanner import Evaluation, evaluate_rules, grid_as_of

RULES: tuple[str, ...] = ("LTX", "MIGRATION")


@dataclass
class _Running:
    """Welford mean / variance of the non-event baseline (every slot, every coin without a trigger)."""

    n: int = 0
    mean: float = 0.0
    m2: float = 0.0

    def add(self, value: float) -> None:
        self.n += 1
        d = value - self.mean
        self.mean += d / self.n
        self.m2 += d * (value - self.mean)

    @property
    def std(self) -> float | None:
        return math.sqrt(self.m2 / (self.n - 1)) if self.n > 1 else None


@dataclass(frozen=True)
class Slot:
    as_of: datetime
    snap: Snapshot
    evaluations: list[Evaluation]


def slot_grid(start: datetime, end: datetime, scanner: ScannerFile) -> Iterator[datetime]:
    """Every scanner slot in `[start, end)` on the live scanner's fixed grid."""
    step = timedelta(seconds=scanner.cadence_s)
    at = grid_as_of(ensure_utc(start), scanner)
    if at < ensure_utc(start):
        at += step
    while at < ensure_utc(end):
        yield at
        at += step


def scan_slots(
    engine: FeatureEngine,
    pit: PitQuery,
    static: StaticConfig,
    scanner: ScannerFile,
    start: datetime,
    end: datetime,
) -> Iterator[Slot]:
    """Point-in-time rule evaluations of every slot; slots without a recorded universe are skipped."""
    for as_of in slot_grid(start, end, scanner):
        universe = load_universe(pit, as_of)
        if universe is None:
            continue
        snap = engine.snapshot(as_of, universe, static.cmc_routes)
        yield Slot(as_of, snap, evaluate_rules(snap, scanner))


def target_of(rule: str) -> TargetType:
    return TargetType.RESID_12H if rule == "LTX" else TargetType.RAW_12H


def config_pins(static: StaticConfig, scanner: ScannerFile) -> dict[str, str]:
    """sha256 of the canonical JSON of every config value the study and the fit samples read.

    The scanner file (rules, SPIKE floor, contagion, label spec), the indicators, the CMC routes given to
    every snapshot, the fees of the event cost and the snapshot staleness limit. A version string can stay
    the same while a value changes; these hashes cannot.
    """
    sections: dict[str, object] = {
        "scanner": scanner,
        "indicators": static.indicators,
        "cmc_routes": static.cmc_routes,
        "fees": static.risk.fees,
        "data_stale_s": static.settings.data.stale_s,
    }
    return {name: canonical_sha256(value) for name, value in sections.items()}


@dataclass
class StudyInputs:
    start: datetime
    end: datetime
    taker: float
    slippage_bp: float
    max_mark_gap_s: int
    rule_version: str
    label_spec_version: str
    feature_ver: str
    config_pins: dict[str, str]

    def study(self) -> G1Study:
        return G1Study(
            start=self.start,
            end=self.end,
            bootstrap_seed=BOOTSTRAP_SEED,
            bootstrap_resamples=BOOTSTRAP_RESAMPLES,
            rule_version=self.rule_version,
            label_spec_version=self.label_spec_version,
            feature_ver=self.feature_ver,
            config_pins=self.config_pins,
        )


@dataclass
class EventRow:
    coin_id: int
    rule: str
    side: str
    as_of: datetime
    beta_btc: float | None
    cost: float
    net: dict[str, float | None]
    """Horizon (hours, as text) -> signed return minus cost; None when the label is missing."""
    missing: dict[str, str]


@dataclass
class StudyResult:
    inputs: StudyInputs
    slots: int
    slots_without_universe: int
    rules: dict[str, RuleResult]
    events: list[EventRow]
    frequency: dict[str, Any]
    baseline: dict[str, dict[str, float | int | None]]
    notes: list[str] = field(default_factory=list)

    def to_json(self) -> dict[str, Any]:
        def rule_json(r: RuleResult) -> dict[str, Any]:
            out = asdict(r)
            out["horizons"] = {h: (asdict(b) if b else None) for h, b in r.horizons.items()}
            return out

        return {
            "inputs": {
                **asdict(self.inputs),
                "start": self.inputs.start.isoformat(),
                "end": self.inputs.end.isoformat(),
                "bootstrap_resamples": BOOTSTRAP_RESAMPLES,
                "bootstrap_seed": BOOTSTRAP_SEED,
            },
            "slots": self.slots,
            "slots_without_universe": self.slots_without_universe,
            "rules": {name: rule_json(r) for name, r in sorted(self.rules.items())},
            "baseline": self.baseline,
            "frequency": self.frequency,
            "events": [
                {**asdict(e), "as_of": e.as_of.isoformat()}
                for e in sorted(self.events, key=lambda e: (e.as_of, e.coin_id, e.rule))
            ],
            "notes": self.notes,
        }


class EventStudy:
    def __init__(
        self,
        pit: PitQuery,
        static: StaticConfig,
        scanner: ScannerFile,
        *,
        view: LakeView | None = None,
        progress: Callable[[datetime], None] | None = None,
    ) -> None:
        self.pit = pit
        self.static = static
        self.scanner = scanner
        self.view = view or LakeView(pit)
        self.features = FeatureEngine(self.view, static, scanner, max_snapshots=2)
        self.marks = MarkBook(self.view)
        self.horizons = tuple(sorted({DECISION_HORIZON_H, *scanner.labels.reference_horizons_h}))
        self._progress = progress

    def _label(
        self,
        member: UniverseMember,
        btc: UniverseMember | None,
        as_of: datetime,
        h: int,
        rule: str,
        beta: float | None,
    ) -> float | None:
        label = resolve_label(
            self.view,
            symbol=member.binance_symbol,
            btc_symbol=btc.binance_symbol if btc is not None else "",
            as_of=as_of,
            horizon_h=h,
            target_type=target_of(rule),
            beta_btc=beta,
            params=self.scanner.labels,
            marks=self.marks.mark,
        )
        return label.value

    def run(self, start: datetime, end: datetime) -> StudyResult:
        start, end = ensure_utc(start), ensure_utc(end)
        fees = self.static.risk.fees
        inputs = StudyInputs(
            start=start,
            end=end,
            taker=fees.taker,
            slippage_bp=fees.slippage_bp,
            max_mark_gap_s=self.scanner.labels.max_mark_gap_s,
            rule_version=self.scanner.rule_version,
            label_spec_version=self.scanner.labels.label_spec_version,
            feature_ver=self.features.feature_ver,
            config_pins=config_pins(self.static, self.scanner),
        )
        window = timedelta(hours=DECISION_HORIZON_H)
        strict: list[tuple[Trigger, Snapshot]] = []
        loose: list[Trigger] = []
        baseline = {rule: _Running() for rule in RULES}
        baseline_missing: dict[str, Counter[str]] = {rule: Counter() for rule in RULES}
        ltx_betas: list[float] = []
        slots = 0
        grid = list(slot_grid(start, end, self.scanner))
        for slot in scan_slots(self.features, self.pit, self.static, self.scanner, start, end):
            slots += 1
            if self._progress is not None:
                self._progress(slot.as_of)
            snap = slot.snap
            for ev in slot.evaluations:
                rule = ev.rule.value
                if ev.loose_pass and ev.side is not None:
                    loose.append(Trigger(ev.coin_id, rule, ev.side, slot.as_of))
                if ev.emit_class == "strict" and ev.side is not None:
                    strict.append((Trigger(ev.coin_id, rule, ev.side, slot.as_of), snap))
                    continue
                member = snap.member(ev.coin_id)
                if member is None or slot.as_of + window > end:
                    baseline_missing[rule]["immature" if member else "not_in_universe"] += 1
                    continue
                beta = snap.beta(member) if rule == "LTX" else None
                if rule == "LTX" and beta is not None:
                    ltx_betas.append(abs(beta))
                value = self._label(member, snap.btc, slot.as_of, DECISION_HORIZON_H, rule, beta)
                if value is None:
                    baseline_missing[rule]["no_label"] += 1
                else:
                    baseline[rule].add(value)

        snaps = {(t.coin_id, t.rule, t.as_of): s for t, s in strict}
        events = independent_events([t for t, _ in strict], window)
        rows = [self._event_row(t, snaps[(t.coin_id, t.rule, t.as_of)], end) for t in events]
        typical_beta = median(ltx_betas) if ltx_betas else None
        results: dict[str, RuleResult] = {}
        baseline_json: dict[str, dict[str, float | int | None]] = {}
        for rule in RULES:
            sigma = baseline[rule].std
            if rule == "LTX":
                delta = (
                    2.0 * round_trip_cost(fees.taker, fees.slippage_bp, hedge_beta=typical_beta)
                    if typical_beta is not None
                    else None
                )
            else:
                delta = 2.0 * round_trip_cost(fees.taker, fees.slippage_bp)
            needed = n_min(sigma, delta) if sigma is not None and delta is not None else None
            mine = [r for r in rows if r.rule == rule]
            horizons: dict[str, BootstrapResult | None] = {}
            for h in self.horizons:
                key = str(h)
                valid = [(r.net[key], r.as_of.date()) for r in mine if r.net.get(key) is not None]
                horizons[key] = day_block_bootstrap(
                    [float(v) for v, _ in valid if v is not None], [d for _, d in valid]
                )
            decision = horizons[str(DECISION_HORIZON_H)]
            n = decision.n if decision else 0
            dropped = Counter(
                r.missing[str(DECISION_HORIZON_H)] for r in mine if str(DECISION_HORIZON_H) in r.missing
            )
            results[rule] = RuleResult(
                rule=rule,
                state=decide(n, needed, decision),
                n_events=n,
                n_min=needed,
                sigma=sigma,
                delta=delta,
                dropped=dict(sorted(dropped.items())),
                horizons=horizons,
            )
            baseline_json[rule] = {
                "n": baseline[rule].n,
                "mean": baseline[rule].mean if baseline[rule].n else None,
                "sigma": sigma,
                "median_abs_beta_btc": typical_beta if rule == "LTX" else None,
                **{f"missing_{k}": v for k, v in sorted(baseline_missing[rule].items())},
            }
        return StudyResult(
            inputs=inputs,
            slots=slots,
            slots_without_universe=len(grid) - slots,
            rules=results,
            events=rows,
            frequency=self._frequency(events, independent_events(loose, window), start, end),
            baseline=baseline_json,
        )

    def _event_row(self, trig: Trigger, snap: Snapshot, end: datetime) -> EventRow:
        member = snap.member(trig.coin_id)
        fees = self.static.risk.fees
        beta = snap.beta(member) if member is not None and trig.rule == "LTX" else None
        hedged = trig.rule == "LTX"
        cost = round_trip_cost(fees.taker, fees.slippage_bp, hedge_beta=beta if hedged else None)
        net: dict[str, float | None] = {}
        missing: dict[str, str] = {}
        for h in self.horizons:
            key = str(h)
            if member is None:
                net[key], missing[key] = None, "not_in_universe"
            elif hedged and beta is None:
                net[key], missing[key] = None, "no_beta"
            elif trig.as_of + timedelta(hours=h) > end:
                net[key], missing[key] = None, "immature"
            else:
                value = self._label(member, snap.btc, trig.as_of, h, trig.rule, beta)
                if value is None:
                    net[key], missing[key] = None, "no_mark"
                else:
                    net[key] = (value if trig.side == "LONG" else -value) - cost
        return EventRow(trig.coin_id, trig.rule, trig.side, trig.as_of, beta, cost, net, missing)

    @staticmethod
    def _frequency(
        strict: list[Trigger], loose: list[Trigger], start: datetime, end: datetime
    ) -> dict[str, Any]:
        days = max((end - start).total_seconds() / 86_400.0, 1e-9)

        def summary(triggers: list[Trigger]) -> dict[str, Any]:
            per_rule: dict[str, Any] = {}
            for rule in RULES:
                mine = [t for t in triggers if t.rule == rule]
                per_day: Counter[date] = Counter(t.as_of.date() for t in mine)
                per_coin: Counter[int] = Counter(t.coin_id for t in mine)
                per_rule[rule] = {
                    "events": len(mine),
                    "events_per_day": len(mine) / days,
                    "max_events_one_day": max(per_day.values(), default=0),
                    "coins": len(per_coin),
                    "per_coin": {str(k): v for k, v in sorted(per_coin.items())},
                }
            return per_rule

        combined: defaultdict[date, int] = defaultdict(int)
        for t in strict:
            combined[t.as_of.date()] += 1
        return {
            "days": days,
            "strict": summary(strict),
            "loose": summary(loose),
            "strict_events_per_day_all_rules": len(strict) / days,
            "strict_max_one_day_all_rules": max(combined.values(), default=0),
        }
