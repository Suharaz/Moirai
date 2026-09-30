"""Pure statistics of gate G4 and the ablations (Design Contract section 9, phase 11 Red Team Delta).

- `spiegelhalter_z`: the calibration Z of `hdt.scoring.calibration.reliability` (same formula as the
  console chart): `sum (y - p)(1 - 2p) / sqrt(sum (1 - 2p)^2 p (1 - p))`.
- `calibration_slope`: logistic recalibration `y ~ sigmoid(alpha + beta * logit(p))` fitted by Newton-Raphson;
  `beta` with its 95% Wald interval from the inverse Fisher information. A calibrated forecaster has
  `beta = 1` (and `alpha = 0`).
- `daily_series`: a daily PnL series over every UTC day of a window, days without a fill included as 0.
- `sharpe` / `psr`: annualized Sharpe of daily returns (`sqrt(365)`, crypto trades every day) and the
  probabilistic Sharpe ratio of Bailey and Lopez de Prado (2012) against a benchmark of 0, with the
  non-annualized Sharpe, the sample skewness and the (non-excess) kurtosis.
- `max_drawdown`: largest peak-to-trough fall of an equity curve as a fraction of the peak (the starting
  equity counts as the first peak).
- `paired_bootstrap`: mean of paired differences with a UTC-day block bootstrap (whole days resampled with
  replacement, fixed seed), its percentile CI and the one-sided p-value of `mean <= 0`.
- `holm`: Holm-Bonferroni step-down adjusted p-values (monotone, capped at 1).
"""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta

import numpy as np
from scipy.stats import norm

from hdt.scoring.calibration import reliability

Z_95 = 1.959963984540054
DAYS_PER_YEAR = 365
BOOTSTRAP_RESAMPLES = 10_000
BOOTSTRAP_SEED = 20260927
_P_EPS = 1e-6


def spiegelhalter_z(p: Sequence[float], y: Sequence[int]) -> float | None:
    """None when undefined (no forecasts, or every forecast is exactly 0, 0.5 or 1)."""
    return reliability(p, y, bins=10).spiegelhalter_z


@dataclass(frozen=True)
class SlopeFit:
    n: int
    intercept: float
    slope: float
    slope_se: float
    ci_low: float
    ci_high: float

    @property
    def contains_one(self) -> bool:
        return self.ci_low <= 1.0 <= self.ci_high


def calibration_slope(
    p: Sequence[float], y: Sequence[int], *, max_iter: int = 100, tol: float = 1e-10
) -> SlopeFit | None:
    """None when the fit is undefined: fewer than 3 forecasts, one class only, a constant forecast, or
    separable data (Newton does not converge)."""
    if len(p) != len(y):
        raise ValueError("p and y must have the same length")
    n = len(p)
    if n < 3 or len(set(y)) < 2:
        return None
    q = np.clip(np.asarray(p, dtype=np.float64), _P_EPS, 1.0 - _P_EPS)
    z = np.log(q / (1.0 - q))
    if float(np.ptp(z)) == 0.0:
        return None
    labels = np.asarray(y, dtype=np.float64)
    x = np.column_stack([np.ones(n), z])
    beta = np.array([0.0, 1.0])
    info = np.eye(2)
    for _ in range(max_iter):
        eta = x @ beta
        mu = 1.0 / (1.0 + np.exp(-eta))
        weight = mu * (1.0 - mu)
        info = x.T @ (x * weight[:, None])
        try:
            step = np.linalg.solve(info, x.T @ (labels - mu))
        except np.linalg.LinAlgError:
            return None
        beta = beta + step
        if not np.all(np.isfinite(beta)):
            return None
        if float(np.max(np.abs(step))) < tol:
            break
    else:
        return None
    mu = 1.0 / (1.0 + np.exp(-(x @ beta)))
    info = x.T @ (x * (mu * (1.0 - mu))[:, None])
    try:
        cov = np.linalg.inv(info)
    except np.linalg.LinAlgError:
        return None
    se = math.sqrt(float(cov[1, 1]))
    if not math.isfinite(se):
        return None
    slope = float(beta[1])
    return SlopeFit(n, float(beta[0]), slope, se, slope - Z_95 * se, slope + Z_95 * se)


def daily_series(pnl: Mapping[date, float], start: date, end: date) -> list[tuple[date, float]]:
    """Every UTC day in `[start, end]` with its PnL; days without trading are 0 (they count)."""
    if end < start:
        return []
    out: list[tuple[date, float]] = []
    day = start
    while day <= end:
        out.append((day, float(pnl.get(day, 0.0))))
        day += timedelta(days=1)
    return out


def returns_from_pnl(pnl: Sequence[float], equity0: float) -> tuple[list[float], list[float]]:
    """Daily returns `pnl_d / equity_{d-1}` and the equity curve (starting equity first)."""
    if equity0 <= 0:
        raise ValueError("starting equity must be positive")
    equity = [equity0]
    returns: list[float] = []
    for value in pnl:
        prev = equity[-1]
        returns.append(value / prev if prev > 0 else float("-inf"))
        equity.append(prev + value)
    return returns, equity


def _moments(returns: Sequence[float]) -> tuple[float, float, float, float] | None:
    """mean, sample std (ddof 1), population skewness and population (non-excess) kurtosis."""
    r = np.asarray(returns, dtype=np.float64)
    if r.size < 2 or not np.all(np.isfinite(r)):
        return None
    mean = float(r.mean())
    std = float(r.std(ddof=1))
    dev = r - mean
    m2 = float(np.mean(dev**2))
    if std == 0.0 or m2 == 0.0:
        return None
    skew = float(np.mean(dev**3)) / m2**1.5
    kurt = float(np.mean(dev**4)) / m2**2
    return mean, std, skew, kurt


def sharpe(returns: Sequence[float], *, periods_per_year: int = DAYS_PER_YEAR) -> float | None:
    """Annualized Sharpe (risk-free 0); None with fewer than 2 days or zero variance."""
    moments = _moments(returns)
    if moments is None:
        return None
    mean, std, _, _ = moments
    return mean / std * math.sqrt(periods_per_year)


def psr(returns: Sequence[float], *, benchmark: float = 0.0) -> float | None:
    """P(true Sharpe > benchmark), benchmark in the same (per-period, non-annualized) unit."""
    moments = _moments(returns)
    if moments is None:
        return None
    mean, std, skew, kurt = moments
    sr = mean / std
    n = len(returns)
    denom = 1.0 - skew * sr + (kurt - 1.0) / 4.0 * sr * sr
    if denom <= 0:
        return None
    return float(norm.cdf((sr - benchmark) * math.sqrt(n - 1) / math.sqrt(denom)))


def max_drawdown(equity: Sequence[float]) -> float:
    """Largest fall from a running peak as a fraction of that peak (0 for a non-falling curve)."""
    peak = -math.inf
    worst = 0.0
    for value in equity:
        peak = max(peak, value)
        if peak > 0:
            worst = max(worst, (peak - value) / peak)
    return worst


@dataclass(frozen=True)
class PairedBootstrap:
    n: int
    days: int
    mean: float
    ci_low: float
    ci_high: float
    p_value: float
    """One-sided: bootstrap share of resampled means <= 0, with the +1 correction."""


def paired_bootstrap(
    diffs: Sequence[float],
    days: Sequence[date],
    *,
    resamples: int = BOOTSTRAP_RESAMPLES,
    seed: int = BOOTSTRAP_SEED,
    level: float = 0.95,
) -> PairedBootstrap | None:
    """`diffs[i]` is `loss_variant - loss_full` of event i (positive: the full system is better)."""
    if len(diffs) != len(days):
        raise ValueError("diffs and days must have the same length")
    if not diffs:
        return None
    by_day: dict[date, list[float]] = defaultdict(list)
    for value, day in zip(diffs, days, strict=True):
        by_day[day].append(float(value))
    ordered = sorted(by_day)
    sums = np.array([sum(by_day[d]) for d in ordered], dtype=np.float64)
    counts = np.array([len(by_day[d]) for d in ordered], dtype=np.float64)
    rng = np.random.default_rng(seed)
    picks = rng.integers(0, len(ordered), size=(resamples, len(ordered)))
    means = sums[picks].sum(axis=1) / counts[picks].sum(axis=1)
    tail = (1.0 - level) / 2.0 * 100.0
    low, high = np.percentile(means, [tail, 100.0 - tail])
    p_value = (1.0 + float(np.sum(means <= 0.0))) / (resamples + 1.0)
    return PairedBootstrap(
        n=len(diffs),
        days=len(ordered),
        mean=float(sums.sum() / counts.sum()),
        ci_low=float(low),
        ci_high=float(high),
        p_value=p_value,
    )


def holm(p_values: Sequence[float]) -> list[float]:
    """Holm-Bonferroni adjusted p-values, in the input order."""
    m = len(p_values)
    order = sorted(range(m), key=lambda i: (p_values[i], i))
    adjusted = [0.0] * m
    running = 0.0
    for rank, index in enumerate(order):
        running = max(running, min(1.0, (m - rank) * p_values[index]))
        adjusted[index] = running
    return adjusted
