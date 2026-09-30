"""Phase 11 gate G4: pass and fail fixtures, no-trade days, split handling, the pinned-model check, the
whole-account checks and the gate_progress rows."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta

import pytest
from golive_events import agent_event

from hdt.golive.ablation import AblationResult, Comparison, ReplayEvent
from hdt.golive.data import SplitRecord
from hdt.golive.g4 import (
    DECIDING,
    Check,
    G4Result,
    TargetInputs,
    TargetResult,
    evaluate_account,
    evaluate_target,
    final_rows,
    pinned_check,
    progress_rows,
    write_progress,
)
from hdt.golive.stats import PairedBootstrap, returns_from_pnl, sharpe

START = datetime(2026, 10, 1, tzinfo=UTC)
END = START + timedelta(days=60)
EQUITY = 10_000.0
PINS = {"alpha": ("model-a", "1"), "beta": ("model-b", "1")}
VERSIONS = {"models": 3, "council": 5}
SPLITS = {t: SplitRecord(t, 2, START) for t in ("RAW_12H", "RESID_12H")}
STATIC = {"scanner": "s1", "scoring": "s2", "council": "s3"}


def _calibrated_forecasts() -> list[tuple[float, int]]:
    """100 at 0.2 with 20 hits, 100 at 0.8 with 80 hits: Z = 0 and slope 1."""
    return [(0.2, 1 if i < 20 else 0) for i in range(100)] + [(0.8, 1 if i < 80 else 0) for i in range(100)]


def _ablation(wins: bool, mismatches: int = 0) -> AblationResult:
    test = PairedBootstrap(
        n=200, days=60, mean=0.02 if wins else -0.01, ci_low=0.01, ci_high=0.03, p_value=0.001
    )
    comp = Comparison("debate", "debate", "full", "round1_pool", 200, 0.60, 0.62, test, 0.003, True)
    return AblationResult("RAW_12H", 200, mismatches, [comp], [])


def _pnl(values: dict[int, float]) -> dict[date, float]:
    return {(START + timedelta(days=k)).date(): v for k, v in values.items()}


def _steady_pnl() -> dict[date, float]:
    """Three-day cycle: a +40 day, a no-trade day, a -10 day."""
    return _pnl({k: (40.0 if k % 3 == 0 else -10.0) for k in range(60) if k % 3 != 1})


def _steady_equity() -> list[float]:
    series = [_steady_pnl().get((START + timedelta(days=k)).date(), 0.0) for k in range(60)]
    return returns_from_pnl(series, EQUITY)[1]


def _events(n: int = 5) -> list[ReplayEvent]:
    paths = {"alpha": (0.6, 0.7, 0.5), "beta": (0.5, 0.5, 0.5)}
    return [agent_event(i, i % 2, paths, config_version_ids=VERSIONS, agent_pins=PINS) for i in range(n)]


def _pinned() -> Check:
    return pinned_check("RAW_12H", _events())


def _inputs(**over: object) -> TargetInputs:
    base: dict[str, object] = {
        "target_type": "RAW_12H",
        "eval_start": START,
        "end": END,
        "forecasts": _calibrated_forecasts(),
        "entries": 150,
        "daily_pnl": _steady_pnl(),
        "equity0": EQUITY,
        "ablation": _ablation(True),
        "pinned": _pinned(),
        "sealed": True,
    }
    base.update(over)
    return TargetInputs(**base)  # type: ignore[arg-type]


def _check(result: TargetResult, key: str):  # type: ignore[no-untyped-def]
    return next(c for c in result.checks if c.key == key)


def _account_check(checks: list[Check], key: str) -> Check:
    return next(c for c in checks if c.key == key)


def test_passing_fixture_meets_every_deciding_check() -> None:
    result = evaluate_target(_inputs())
    assert {c.key for c in result.checks} >= DECIDING
    assert all(_check(result, k).met is True for k in DECIDING)
    assert result.passed
    assert _check(result, "spiegelhalter_z").value == pytest.approx(0.0, abs=1e-9)
    assert _check(result, "calibration_slope").value == pytest.approx(1.0, abs=1e-8)
    # no-trade days are part of the series: 60 days, 40 of them with PnL
    sharpe_check = _check(result, "sharpe")
    assert sharpe_check.detail == {"days": 60, "trading_days": 40}
    assert result.days == 60


def test_sharpe_counts_no_trade_days() -> None:
    # four trades in 60 days: Sharpe about 3.3 per trading day, about 0.97 once the idle days count
    pnl = _pnl({0: 30.0, 15: -20.0, 30: 30.0, 45: -20.0})
    result = evaluate_target(_inputs(daily_pnl=pnl))
    series = [pnl.get((START + timedelta(days=k)).date(), 0.0) for k in range(60)]
    expected = sharpe(returns_from_pnl(series, EQUITY)[0])
    trading_only = sharpe(returns_from_pnl([30.0, -20.0, 30.0, -20.0], EQUITY)[0])
    check = _check(result, "sharpe")
    assert check.value == pytest.approx(expected)
    assert trading_only is not None
    assert trading_only > 1.5
    assert check.met is False
    assert not result.passed


def test_fails_on_drawdown_entries_calibration_and_ablation() -> None:
    crash = _steady_pnl()
    crash[(START + timedelta(days=31)).date()] = -1_500.0
    result = evaluate_target(_inputs(daily_pnl=crash))
    assert _check(result, "max_drawdown").met is False
    assert not result.passed

    few = evaluate_target(_inputs(entries=149))
    assert _check(few, "n_entries").met is False
    assert not few.passed

    over = [(0.1, 1 if i < 30 else 0) for i in range(100)] + [(0.9, 1 if i < 70 else 0) for i in range(100)]
    miscal = evaluate_target(_inputs(forecasts=over))
    assert _check(miscal, "calibration_slope").met is False
    assert _check(miscal, "spiegelhalter_z").met is False
    assert not miscal.passed

    lost = evaluate_target(_inputs(ablation=_ablation(False)))
    assert _check(lost, "ablation").met is False
    assert not lost.passed
    none = evaluate_target(_inputs(ablation=None))
    assert _check(none, "ablation").met is False


def test_a_replay_mismatch_fails_the_ablation_even_when_every_component_wins() -> None:
    result = evaluate_target(_inputs(ablation=_ablation(True, mismatches=1)))
    ablation = _check(result, "ablation")
    assert ablation.met is False
    assert ablation.detail["replay_mismatches"] == 1
    assert not result.passed


def test_pinned_check_fails_when_a_model_slug_or_a_config_version_changes() -> None:
    assert _pinned().met is True
    events = _events()
    moved = [*events[:3], *(replace(e, agent_pins={**PINS, "alpha": ("model-c", "1")}) for e in events[3:])]
    slug = pinned_check("RAW_12H", moved)
    assert slug.met is False
    assert slug.detail == {"changed": ["agent:alpha"]}
    council = [*events[:2], *(replace(e, config_version_ids={**VERSIONS, "council": 6}) for e in events[2:])]
    assert pinned_check("RAW_12H", council).detail == {"changed": ["config:council"]}
    # an unrelated section may change (a risk edit does not change the forecaster)
    risk = [replace(e, config_version_ids={**VERSIONS, "risk": e.coin_id}) for e in events]
    assert pinned_check("RAW_12H", risk).met is True
    assert pinned_check("RAW_12H", []).met is False

    result = evaluate_target(_inputs(pinned=slug))
    assert _check(result, "pinned").met is False
    assert not result.passed
    assert not evaluate_target(_inputs(pinned=None)).passed


def test_a_pinned_window_records_its_config_and_agent_pins_in_the_final_row() -> None:
    """The live gate compares these pins with the active config: the final row must carry them."""
    pins = {
        "models": 3,
        "council": 5,
        "agents": {
            "alpha": {"model_slug": "model-a", "agent_version": "1"},
            "beta": {"model_slug": "model-b", "agent_version": "1"},
        },
    }
    assert _pinned().detail == {"pins": pins}
    ok = evaluate_target(_inputs())
    assert ok.pins == pins
    no_console = [replace(e, config_version_ids={}) for e in _events()]
    assert pinned_check("RAW_12H", no_console).detail["pins"]["models"] is None
    moved = [replace(e, config_version_ids={**VERSIONS, "models": 4 + e.coin_id % 2}) for e in _events()]
    assert evaluate_target(_inputs(pinned=pinned_check("RAW_12H", moved))).pins is None
    assert evaluate_target(_inputs(pinned=None)).pins is None

    result = G4Result(END, ("RAW_12H",), {"RAW_12H": ok}, (), final=True, account=evaluate_account([]))
    [final_row] = final_rows(result, "evidence", splits=SPLITS, static_pins=STATIC)
    assert final_row["pins"] == {**pins, "static": STATIC}
    assert final_row["generation"] == 2
    # a window that does not start at the current split is never recorded against it
    moved_split = {"RAW_12H": SplitRecord("RAW_12H", 3, START + timedelta(days=1))}
    with pytest.raises(ValueError, match="current split"):
        final_rows(result, "evidence", splits=moved_split, static_pins=STATIC)


def test_targets_pinned_to_different_versions_fail_the_gate() -> None:
    """Review M-3: finals pinned to different versions could never match one active config."""
    account = evaluate_account(_steady_equity())
    raw = evaluate_target(_inputs())
    resid = evaluate_target(_inputs(target_type="RESID_12H"))
    same = G4Result(END, ("RAW_12H", "RESID_12H"), {"RAW_12H": raw, "RESID_12H": resid}, (), True, account)
    assert same.pins_agree
    assert same.passed
    other = [replace(e, config_version_ids={**VERSIONS, "models": 4}) for e in _events()]
    moved = evaluate_target(_inputs(target_type="RESID_12H", pinned=pinned_check("RESID_12H", other)))
    assert moved.passed
    split = G4Result(END, ("RAW_12H", "RESID_12H"), {"RAW_12H": raw, "RESID_12H": moved}, (), True, account)
    assert not split.pins_agree
    assert not split.passed
    rows = {r["check_key"]: r for r in progress_rows(split)}
    assert rows["pins_agree"]["met"] is False
    assert rows["overall"]["met"] is False
    assert split.to_json()["pins_agree"] is False


def test_interim_progress_never_overwrites_a_decided_target() -> None:
    """Review M-2: the weekly interim run leaves the rows of a target type with a final, and the account,
    `pins_agree` and `overall` rows, as the final wrote them."""
    interim = G4Result(
        END,
        ("RAW_12H", "RESID_12H"),
        {
            "RAW_12H": evaluate_target(_inputs(final=False)),
            "RESID_12H": evaluate_target(_inputs(final=False)),
        },
        (),
        account=evaluate_account(_steady_equity()),
    )
    written: list[list[dict[str, object]]] = []

    class Recorder:
        def execute(self, statement: object, params: list[dict[str, object]] | None = None) -> None:
            written.append(params or [])

    assert write_progress(Recorder(), interim, decided={"RAW_12H"}) > 0  # type: ignore[arg-type]
    keys = {str(r["check_key"]) for r in written[0]}
    assert keys
    assert all(k.startswith("RESID_12H.") for k in keys)
    written.clear()
    assert write_progress(Recorder(), interim, decided={"RAW_12H", "RESID_12H"}) == 0  # type: ignore[arg-type]
    assert written == []
    written.clear()
    write_progress(Recorder(), interim)  # type: ignore[arg-type]
    assert {"overall", "pins_agree", "account.sharpe"} <= {str(r["check_key"]) for r in written[0]}


def test_no_pnl_or_no_forecasts_fail_instead_of_passing() -> None:
    empty = evaluate_target(_inputs(daily_pnl={}, forecasts=[]))
    # an idle window is 60 zero days: zero variance, so no Sharpe
    assert _check(empty, "sharpe").value is None
    assert _check(empty, "sharpe").met is False
    assert _check(empty, "spiegelhalter_z").met is False
    assert not empty.passed


def test_interim_result_never_decides_and_computes_no_ablation() -> None:
    interim = evaluate_target(_inputs(final=False, ablation=None))
    assert _check(interim, "ablation").met is None
    assert not interim.passed
    result = G4Result(END, ("RAW_12H",), {"RAW_12H": interim}, (), account=evaluate_account(_steady_equity()))
    assert progress_rows(result)[-1]["met"] is None
    with pytest.raises(ValueError, match="final"):
        final_rows(result, "evidence", splits=SPLITS, static_pins=STATIC)


def test_account_drawdown_counts_every_position() -> None:
    """The per-target PnL can look fine while the account falls: an unscored event losing 15% of the account
    fails the gate through the account check."""
    ok = evaluate_target(_inputs())
    equity = _steady_equity()
    passing = evaluate_account(equity)
    assert all(c.met is True for c in passing)
    assert G4Result(END, ("RAW_12H",), {"RAW_12H": ok}, (), final=True, account=passing).passed

    crashed = [*equity[:30], *(v - 0.15 * EQUITY for v in equity[30:])]
    account = evaluate_account(crashed)
    assert _account_check(account, "max_drawdown").met is False
    assert ok.passed
    assert not G4Result(END, ("RAW_12H",), {"RAW_12H": ok}, (), final=True, account=account).passed
    # no equity snapshots: the account checks fail instead of passing
    assert not G4Result(
        END, ("RAW_12H",), {"RAW_12H": ok}, (), final=True, account=evaluate_account([])
    ).passed


def test_result_needs_every_required_target_and_split() -> None:
    now = END
    ok = evaluate_target(_inputs())
    account = evaluate_account(_steady_equity())
    assert G4Result(now, ("RAW_12H",), {"RAW_12H": ok}, (), final=True, account=account).passed
    missing = G4Result(
        now, ("RAW_12H", "RESID_12H"), {"RAW_12H": ok}, ("RESID_12H",), final=True, account=account
    )
    assert not missing.passed
    assert not G4Result(now, (), {}, (), final=True, account=account).passed

    rows = {r["check_key"]: r for r in progress_rows(missing)}
    assert rows["RESID_12H.split"]["met"] is False
    assert rows["overall"]["value"] == 1.0
    assert rows["overall"]["target"] == 2.0
    assert rows["overall"]["met"] is False
    assert rows["RAW_12H.sharpe"]["met"] is True
    assert rows["account.max_drawdown"]["met"] is True
    assert all(r["gate"] == "G4" and r["updated_at"] == now for r in rows.values())
