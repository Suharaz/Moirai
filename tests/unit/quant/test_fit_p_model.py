"""`scripts/fit_p_model.py` core: setup labels, walk-forward, family shrinking, versioning, G1 binding."""

from __future__ import annotations

import json
import random
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from hdt.contracts.common import AgentName, TargetType
from hdt.core.config import (
    PModelFitFile,
    ScannerFile,
    StaticConfig,
    load_p_model,
    scanner_config,
    static_config,
)
from hdt.features.engine import feature_version
from hdt.quant.fit import FitRefusedError, Sample, fit_agent, next_version, passed_g1, walk_forward_folds
from hdt.quant.gates import (
    BOOTSTRAP_RESAMPLES,
    BOOTSTRAP_SEED,
    BootstrapResult,
    G1Study,
    RuleResult,
    status_from_rules,
    write_status,
)
from hdt.quant.p_model import PModel
from hdt.quant.study import config_pins

T0 = datetime(2026, 10, 1, tzinfo=UTC)
H12 = timedelta(hours=12)
PARAMS = PModelFitFile(schema_version=1, fit_version="fit-test", l2_c=1.0, folds=5, min_family_samples=50)


def samples(rule: str, n: int, *, informative: bool = True, seed: int = 7) -> list[Sample]:
    """Crowding samples whose label follows `spike` (when informative); `oi_hhi` is never recorded."""
    rng = random.Random(seed)
    target = TargetType.RESID_12H if rule == "LTX" else TargetType.RAW_12H
    out = []
    for i in range(n):
        spike = rng.gauss(3.0, 1.0)
        y = int(spike + rng.gauss(0, 0.5) > 3.0) if informative else rng.randint(0, 1)
        features: dict[str, float | None] = {
            name: rng.gauss(0, 1) for name in load_p_model("crowding", "RESID_12H").coefficients
        }
        features["spike"] = spike
        features["oi_hhi"] = None
        out.append(
            Sample(
                coin_id=1 + i % 7,
                rule=rule,
                as_of=T0 + timedelta(hours=12 * i),
                features={AgentName.CROWDING: features},
                labels={target: y},
            )
        )
    return out


def test_informative_feature_is_learned_and_missing_family_shrinks() -> None:
    spec = load_p_model("crowding", "RESID_12H")  # fit on LTX events only
    rows = samples("LTX", 400)
    result = fit_agent(AgentName.CROWDING, spec, rows, PARAMS, source="reports/g1/test.json")
    fitted = result.spec
    assert fitted.p_model_ver == next_version(spec) == "crowding-resid_12h-pm1"
    assert fitted.sample_count == 400
    assert result.oos_rows == 320  # blocks 2..5 of 5 are predicted out of sample
    assert fitted.oos_logloss is not None
    assert fitted.oos_logloss < 0.6  # well below log(2) = 0.693
    assert fitted.coefficients["spike"] > 1.0
    # oi_hhi has no values; its family still has the other four migration features, so nothing shrinks
    assert fitted.coefficients["oi_hhi"] == 0.0
    assert fitted.shrunk_families == ()
    model = PModel(fitted)
    assert model.predict({"spike": 5.0}) > 0.8
    assert model.predict({"spike": 1.0}) < 0.2
    assert model.predict({}) == pytest.approx(model.predict({"oi_hhi": None}))


def test_samples_of_another_target_type_are_never_pooled() -> None:
    spec = load_p_model("crowding", "RESID_12H")
    result = fit_agent(
        AgentName.CROWDING, spec, samples("MIGRATION", 400), PARAMS, source="reports/g1/test.json"
    )
    assert result.rows == 0  # Migration events carry RAW_12H labels; Crowding's file is RESID_12H
    assert set(result.spec.shrunk_families) == set(spec.families)
    assert all(value == 0.0 for value in result.spec.coefficients.values())
    assert result.spec.oos_logloss is None


def test_family_below_min_samples_is_shrunk_to_zero() -> None:
    spec = load_p_model("crowding", "RESID_12H")
    rows = samples("LTX", 40)
    result = fit_agent(AgentName.CROWDING, spec, rows, PARAMS, source="reports/g1/test.json")
    assert set(result.spec.shrunk_families) == {"ltx", "migration"}
    assert all(value == 0.0 for value in result.spec.coefficients.values())


def test_rows_of_one_instant_never_straddle_train_and_test() -> None:
    spec = load_p_model("crowding", "RESID_12H")
    # 7 instants 12 h apart with 3 coins each: 21 rows, which a cut over rows would split mid-instant.
    rows = [replace(s, as_of=T0 + H12 * (i // 3)) for i, s in enumerate(samples("LTX", 21))]
    result = fit_agent(AgentName.CROWDING, spec, rows, PARAMS, source="reports/g1/test.json")
    # Blocks over the 7 instants: [0], [1], [2, 3], [4], [5, 6]; blocks 2..5 hold 1 + 2 + 1 + 2 instants.
    assert result.oos_rows == 18


def test_walk_forward_blocks_are_whole_instants_purged_by_the_label_horizon() -> None:
    rng = random.Random(3)
    # 300 rows on a 5-minute grid over about a week: many instants carry several rows.
    as_of = sorted(T0 + timedelta(minutes=5 * rng.randrange(0, 2000)) for _ in range(300))
    assert len(set(as_of)) < len(as_of)
    folds = walk_forward_folds(as_of, 5, H12)
    assert len(folds) == 4
    tested: list[int] = []
    for train, test in folds:
        train_at = [as_of[i] for i in train]
        test_at = {as_of[i] for i in test}
        assert max(train_at) < min(test_at)
        assert max(train_at) + H12 <= min(test_at)  # no training label resolves inside the test block
        assert {i for i, t in enumerate(as_of) if t in test_at} == set(test.tolist())
        tested += test.tolist()
    assert len(tested) == len(set(tested))
    assert walk_forward_folds(as_of[:4], 5, H12) == []  # fewer distinct instants than blocks


def current_study(static: StaticConfig, scanner: ScannerFile) -> G1Study:
    return G1Study(
        start=T0,
        end=T0 + timedelta(days=28),
        bootstrap_seed=BOOTSTRAP_SEED,
        bootstrap_resamples=BOOTSTRAP_RESAMPLES,
        rule_version=scanner.rule_version,
        label_spec_version=scanner.labels.label_spec_version,
        feature_ver=feature_version(static, scanner),
        config_pins=config_pins(static, scanner),
    )


def write_g1(repo: Path, study: G1Study, state: str = "passed") -> Path:
    """A G1 report JSON and `latest.json` exactly as `scripts/g1_event_study.py` writes their status."""
    reports = repo / "reports" / "g1"
    reports.mkdir(parents=True, exist_ok=True)
    decision = BootstrapResult(n=200, days=28, mean=0.01, ci_low=0.002, ci_high=0.02)
    rules = {
        name: RuleResult(name, state, 200, 150, 0.02, 0.004, {}, {"12": decision})  # type: ignore[arg-type]
        for name in ("LTX", "MIGRATION")
    }
    report = "reports/g1/2026-10-29.json"
    status = status_from_rules(rules, study.end, report, study=study)
    (repo / report).write_text(json.dumps({"status": status.to_json()}), encoding="utf-8")
    write_status(reports, status)
    return reports


def test_fit_window_is_the_passed_study_with_the_current_config(tmp_path: Path) -> None:
    static, scanner = static_config(), scanner_config()
    study = current_study(static, scanner)
    reports = write_g1(tmp_path, study)
    assert passed_g1(reports, tmp_path, static, scanner) == (study, "reports/g1/2026-10-29.json")


def test_fit_refuses_a_study_computed_on_other_inputs(tmp_path: Path) -> None:
    static, scanner = static_config(), scanner_config()
    study = current_study(static, scanner)
    reports = write_g1(tmp_path, study)
    # A threshold edited without a new rule_version: every version string still matches.
    strict = scanner.ltx.strict.model_copy(update={"spike_min": scanner.ltx.strict.spike_min + 1.0})
    edited = scanner.model_copy(update={"ltx": scanner.ltx.model_copy(update={"strict": strict})})
    with pytest.raises(FitRefusedError, match=r"config_pins\.scanner"):
        passed_g1(reports, tmp_path, static, edited)
    fees = static.risk.fees.model_copy(update={"taker": static.risk.fees.taker * 2})
    pricier = static.model_copy(update={"risk": static.risk.model_copy(update={"fees": fees})})
    with pytest.raises(FitRefusedError, match=r"config_pins\.fees"):
        passed_g1(reports, tmp_path, pricier, scanner)
    write_g1(tmp_path, replace(study, bootstrap_seed=BOOTSTRAP_SEED + 1))
    with pytest.raises(FitRefusedError, match="bootstrap_seed"):
        passed_g1(reports, tmp_path, static, scanner)


def test_fit_refuses_without_a_bound_passed_study(tmp_path: Path) -> None:
    static, scanner = static_config(), scanner_config()
    study = current_study(static, scanner)
    reports = write_g1(tmp_path, study, state="insufficient_power")
    with pytest.raises(FitRefusedError, match="insufficient_power"):
        passed_g1(reports, tmp_path, static, scanner)
    write_g1(tmp_path, study)
    latest = reports / "latest.json"
    raw = json.loads(latest.read_text(encoding="utf-8"))
    # latest.json widened to another window than the report it names
    latest.write_text(
        json.dumps({**raw, "study": {**raw["study"], "start": (T0 - H12).isoformat()}}), encoding="utf-8"
    )
    with pytest.raises(FitRefusedError, match="does not hold the status"):
        passed_g1(reports, tmp_path, static, scanner)
    # a status file without the study record (written before the study was persisted)
    latest.write_text(json.dumps({k: v for k, v in raw.items() if k != "study"}), encoding="utf-8")
    with pytest.raises(FitRefusedError, match="no complete study"):
        passed_g1(reports, tmp_path, static, scanner)
