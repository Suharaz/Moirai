"""Phase 11 G4 statistics against hand-computed values."""

from __future__ import annotations

import math
from datetime import date, timedelta

import pytest

from hdt.golive.stats import (
    calibration_slope,
    daily_series,
    holm,
    max_drawdown,
    paired_bootstrap,
    psr,
    returns_from_pnl,
    sharpe,
    spiegelhalter_z,
)

D0 = date(2026, 10, 1)


def _phi(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def test_spiegelhalter_z_known_value() -> None:
    # num = (0 - 0.2)(0.6) + (1 - 0.8)(-0.6) = -0.24; var = 2 x 0.36 x 0.16 = 0.1152
    assert spiegelhalter_z([0.2, 0.8], [0, 1]) == pytest.approx(-0.24 / math.sqrt(0.1152))


def test_spiegelhalter_z_zero_when_frequencies_match() -> None:
    p = [0.2] * 5 + [0.8] * 5
    y = [1, 0, 0, 0, 0, 1, 1, 1, 1, 0]
    assert spiegelhalter_z(p, y) == pytest.approx(0.0, abs=1e-12)


def test_calibration_slope_is_one_for_calibrated_groups() -> None:
    p = [0.2] * 10 + [0.8] * 10
    y = [1, 1] + [0] * 8 + [1] * 8 + [0, 0]
    fit = calibration_slope(p, y)
    assert fit is not None
    assert fit.slope == pytest.approx(1.0, abs=1e-8)
    assert fit.intercept == pytest.approx(0.0, abs=1e-8)
    assert fit.contains_one


def test_calibration_slope_detects_overconfidence() -> None:
    # outcome rates 0.3 / 0.7 at forecasts 0.1 / 0.9: slope = logit(0.7) / logit(0.9)
    p = [0.1] * 100 + [0.9] * 100
    y = [1] * 30 + [0] * 70 + [1] * 70 + [0] * 30
    fit = calibration_slope(p, y)
    assert fit is not None
    assert fit.slope == pytest.approx(math.log(0.7 / 0.3) / math.log(0.9 / 0.1), abs=1e-8)
    assert not fit.contains_one
    assert fit.ci_high < 1.0


def test_calibration_slope_undefined_cases() -> None:
    assert calibration_slope([0.3, 0.6], [0, 1]) is None
    assert calibration_slope([0.3, 0.6, 0.7], [1, 1, 1]) is None
    assert calibration_slope([0.4, 0.4, 0.4], [0, 1, 0]) is None


def test_daily_series_counts_no_trade_days() -> None:
    series = daily_series({D0: 5.0, D0 + timedelta(days=3): -2.0}, D0, D0 + timedelta(days=4))
    assert series == [
        (D0, 5.0),
        (D0 + timedelta(days=1), 0.0),
        (D0 + timedelta(days=2), 0.0),
        (D0 + timedelta(days=3), -2.0),
        (D0 + timedelta(days=4), 0.0),
    ]


def test_returns_from_pnl_compound_on_previous_equity() -> None:
    returns, equity = returns_from_pnl([100.0, -110.0], 1000.0)
    assert equity == [1000.0, 1100.0, 990.0]
    assert returns == pytest.approx([0.1, -0.1])
    with pytest.raises(ValueError, match="starting equity"):
        returns_from_pnl([1.0], 0.0)


def test_sharpe_and_psr_known_values() -> None:
    r = [0.01, -0.01, 0.02, 0.0]
    # mean 0.005, sample var 500e-6 / 3, skew 0, population kurtosis 1.64
    sr = 0.005 / math.sqrt(500e-6 / 3)
    assert sharpe(r) == pytest.approx(sr * math.sqrt(365))
    expected = _phi(sr * math.sqrt(3) / math.sqrt(1 + (1.64 - 1) / 4 * sr * sr))
    assert psr(r) == pytest.approx(expected)
    assert sharpe([0.01]) is None
    assert sharpe([0.01, 0.01]) is None


def test_max_drawdown_from_running_peak() -> None:
    assert max_drawdown([100, 120, 90, 130, 104]) == pytest.approx(0.25)
    assert max_drawdown([100, 101, 102]) == 0.0


def test_holm_adjustment() -> None:
    assert holm([0.01, 0.04, 0.03]) == pytest.approx([0.03, 0.06, 0.06])
    assert holm([0.5, 0.9]) == pytest.approx([1.0, 1.0])
    assert holm([]) == []


def test_paired_bootstrap_constant_differences() -> None:
    days = [D0, D0, D0 + timedelta(days=1), D0 + timedelta(days=2)]
    win = paired_bootstrap([0.1] * 4, days, resamples=999, seed=7)
    assert win is not None
    assert (win.n, win.days) == (4, 3)
    assert win.mean == pytest.approx(0.1)
    assert win.ci_low == pytest.approx(0.1)
    assert win.p_value == pytest.approx(1 / 1000)
    lose = paired_bootstrap([-0.1] * 4, days, resamples=999, seed=7)
    assert lose is not None
    assert lose.p_value == pytest.approx(1.0)
    assert paired_bootstrap([], []) is None


def test_paired_bootstrap_is_seeded() -> None:
    diffs = [0.3, -0.1, 0.2, -0.05, 0.1, 0.0]
    days = [D0 + timedelta(days=i // 2) for i in range(6)]
    assert paired_bootstrap(diffs, days, resamples=500, seed=3) == paired_bootstrap(
        diffs, days, resamples=500, seed=3
    )
