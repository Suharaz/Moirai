"""Gate G1 statistics and decision (docs/g1-preregistration.md)."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from itertools import pairwise
from pathlib import Path

import pytest

from hdt.core.config import scanner_config, static_config
from hdt.quant.gates import (
    BOOTSTRAP_RESAMPLES,
    BOOTSTRAP_SEED,
    BootstrapResult,
    G1Study,
    RuleResult,
    Trigger,
    combine,
    day_block_bootstrap,
    decide,
    g1_status,
    independent_events,
    n_min,
    round_trip_cost,
    status_from_rules,
    write_status,
)
from hdt.quant.study import config_pins, slot_grid

T0 = datetime(2026, 10, 1, tzinfo=UTC)
H12 = timedelta(hours=12)
STUDY = G1Study(
    start=T0,
    end=T0 + timedelta(days=28),
    bootstrap_seed=BOOTSTRAP_SEED,
    bootstrap_resamples=BOOTSTRAP_RESAMPLES,
    rule_version="s1",
    label_spec_version="lbl1",
    feature_ver="f1.s1",
    config_pins=config_pins(static_config(), scanner_config()),
)


def test_one_event_per_coin_window_across_rules() -> None:
    triggers = [
        Trigger(1, "MIGRATION", "LONG", T0),
        Trigger(1, "LTX", "SHORT", T0),  # same instant: LTX is kept, deterministically
        Trigger(1, "MIGRATION", "LONG", T0 + timedelta(hours=11, minutes=55)),  # inside the window
        Trigger(1, "LTX", "LONG", T0 + H12),  # the window is half-open: a new event
        Trigger(2, "MIGRATION", "LONG", T0 + timedelta(hours=1)),
    ]
    kept = independent_events(reversed(triggers), H12)
    assert [(t.coin_id, t.rule, t.as_of) for t in kept] == [
        (1, "LTX", T0),
        (2, "MIGRATION", T0 + timedelta(hours=1)),
        (1, "LTX", T0 + H12),
    ]


def test_block_bootstrap_is_deterministic_and_resamples_whole_days() -> None:
    days = [date(2026, 10, d) for d in (1, 1, 2, 3, 3, 3)]
    values = [0.01, 0.03, -0.01, 0.02, 0.02, 0.02]
    first = day_block_bootstrap(values, days, resamples=2000)
    assert first == day_block_bootstrap(values, days, resamples=2000)
    assert first is not None
    assert (first.n, first.days) == (6, 3)
    assert first.mean == pytest.approx(sum(values) / 6)
    assert first.ci_low <= first.mean <= first.ci_high
    # every day's mean lies inside [-0.01, 0.02]: no resample of whole days can leave that range
    assert first.ci_low >= -0.01 - 1e-12
    assert first.ci_high <= 0.02 + 1e-12
    assert day_block_bootstrap([], []) is None
    with pytest.raises(ValueError, match="same length"):
        day_block_bootstrap([1.0], [])


def test_n_min_and_costs_follow_the_preregistered_formula() -> None:
    assert n_min(0.02, 0.004) == 196  # ((1.96 + 0.84) * 5) ** 2 = 196
    assert n_min(0.0, 0.004) is None
    assert n_min(0.02, 0.0) is None
    assert round_trip_cost(0.0005, 2) == pytest.approx(0.0014)
    assert round_trip_cost(0.0005, 2, hedge_beta=-0.5) == pytest.approx(0.0021)


def ci(n: int, low: float, high: float) -> BootstrapResult:
    return BootstrapResult(n=n, days=n, mean=(low + high) / 2, ci_low=low, ci_high=high)


def test_decision_states() -> None:
    assert decide(0, 10, None) == "insufficient_data"
    assert decide(5, None, ci(5, 0.01, 0.02)) == "insufficient_data"
    assert decide(9, 10, ci(9, 0.01, 0.02)) == "insufficient_power"
    assert decide(10, 10, ci(10, -0.01, 0.02)) == "failed"
    assert decide(10, 10, ci(10, -0.03, -0.01)) == "failed"  # wrong direction after costs
    assert decide(10, 10, ci(10, 0.001, 0.02)) == "passed"


def rule(name: str, state: str, n: int = 10) -> RuleResult:
    return RuleResult(name, state, n, 10, 0.02, 0.004, {}, {"12": ci(n, 0.001, 0.02)})  # type: ignore[arg-type]


def test_gate_passes_only_when_every_rule_passes() -> None:
    assert combine(["passed", "passed"]) == "passed"
    assert combine(["passed", "insufficient_power"]) == "insufficient_power"
    assert combine(["insufficient_power", "insufficient_data"]) == "insufficient_data"
    assert combine(["insufficient_data", "failed", "passed"]) == "failed"
    assert combine([]) == "insufficient_data"
    status = status_from_rules(
        {"LTX": rule("LTX", "passed"), "MIGRATION": rule("MIGRATION", "insufficient_power", 3)},
        T0,
        "r.json",
        study=STUDY,
    )
    assert status.state == "insufficient_power"
    assert status.n_events == 3  # the numbers of the deciding rule
    assert status.rules == {"LTX": "passed", "MIGRATION": "insufficient_power"}


def test_status_file_round_trips_and_fails_closed(tmp_path: Path) -> None:
    assert g1_status(tmp_path).state == "insufficient_data"
    status = status_from_rules(
        {"LTX": rule("LTX", "passed"), "MIGRATION": rule("MIGRATION", "passed")}, T0, "x", study=STUDY
    )
    write_status(tmp_path, status)
    assert g1_status(tmp_path) == status
    assert g1_status(tmp_path).passed
    assert g1_status(tmp_path).study == STUDY
    raw = json.loads((tmp_path / "latest.json").read_text(encoding="utf-8"))
    del raw["study"]["config_pins"]
    (tmp_path / "latest.json").write_text(json.dumps(raw), encoding="utf-8")
    assert g1_status(tmp_path).study is None  # a partial study record binds nothing
    (tmp_path / "latest.json").write_text(json.dumps({"state": "PASSED"}), encoding="utf-8")
    assert not g1_status(tmp_path).passed
    (tmp_path / "latest.json").write_text("{broken", encoding="utf-8")
    assert g1_status(tmp_path).state == "insufficient_data"


def test_study_evaluates_every_scanner_slot() -> None:
    scanner = scanner_config()
    start = T0 + timedelta(seconds=1)
    slots = list(slot_grid(start, T0 + timedelta(days=1), scanner))
    assert len(slots) == 86_400 // scanner.cadence_s
    assert all(b - a == timedelta(seconds=scanner.cadence_s) for a, b in pairwise(slots))
    assert slots[0] >= start
    assert slots[0].timestamp() % scanner.cadence_s == scanner.run_delay_s
