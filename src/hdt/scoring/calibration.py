"""Calibration (phase 08): isotonic maps per agent and for the pooled `p`, the pool intercept `b`, and the
reliability chart data (bins, ECE, Spiegelhalter Z) for the console and the public dashboard.

Fit window (out of sample for every decision made after `now`): events with
`now - window_days <= as_of` and `as_of + 2 x horizon <= now`: the label must be known (purge) and one more
horizon is left out (embargo = horizon). Fewer than `min_samples` events leave the identity map and b = 0.
Everything is fitted per `(target_type, label_spec_version)` by the caller; tables never mix them.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import numpy as np
from scipy.optimize import minimize_scalar
from sklearn.isotonic import IsotonicRegression

from hdt.scoring.loss import clip_p, logit


@dataclass(frozen=True)
class IsotonicMap:
    """Piecewise-linear monotone map from raw to calibrated probability (identity when empty)."""

    x: tuple[float, ...] = ()
    y: tuple[float, ...] = ()

    def __call__(self, p: float, clip: tuple[float, float]) -> float:
        if not self.x:
            return clip_p(p, clip)
        return clip_p(float(np.interp(p, self.x, self.y)), clip)

    def to_json(self) -> dict[str, list[float]]:
        return {"x": list(self.x), "y": list(self.y)}

    @classmethod
    def from_json(cls, data: Mapping[str, Any] | None) -> IsotonicMap:
        if not data:
            return cls()
        x = tuple(float(v) for v in data.get("x", ()))
        y = tuple(float(v) for v in data.get("y", ()))
        if len(x) != len(y):
            raise ValueError("calibration map x and y differ in length")
        return cls(x, y)


def in_window(as_of: datetime, now: datetime, horizon: timedelta, window: timedelta) -> bool:
    return now - window <= as_of and as_of + 2 * horizon <= now


def fit_isotonic(
    p: Sequence[float], y: Sequence[int], *, min_samples: int, clip: tuple[float, float]
) -> IsotonicMap:
    if len(p) < min_samples or len(set(y)) < 2:
        return IsotonicMap()
    model = IsotonicRegression(y_min=clip[0], y_max=clip[1], out_of_bounds="clip", increasing=True)
    model.fit(np.asarray(p, dtype=float), np.asarray(y, dtype=float))
    xs = [round(float(v), 10) for v in model.X_thresholds_]
    ys = [round(float(v), 10) for v in model.y_thresholds_]
    return IsotonicMap(tuple(xs), tuple(ys))


def fit_intercept(
    pooled_logits: Sequence[float],
    y: Sequence[int],
    *,
    bound: float,
    min_samples: int,
    clip: tuple[float, float],
) -> float:
    """b minimizing the log-loss of `sigmoid(b + sum w logit(cal p))` given each event's pooled sum."""
    if len(y) < min_samples:
        return 0.0
    z = np.asarray(pooled_logits, dtype=float)
    labels = np.asarray(y, dtype=float)
    lo, hi = clip

    def loss(b: float) -> float:
        p = np.clip(1.0 / (1.0 + np.exp(-(z + b))), lo, hi)
        return float(-np.mean(labels * np.log(p) + (1 - labels) * np.log(1 - p)))

    result = minimize_scalar(loss, bounds=(-bound, bound), method="bounded", options={"xatol": 1e-8})
    return round(float(result.x), 8)


def pooled_logit(
    p_by_agent: Mapping[str, float],
    weights: Mapping[str, float],
    maps: Mapping[str, IsotonicMap],
    clip: tuple[float, float],
) -> float:
    total = 0.0
    for agent, p in p_by_agent.items():
        cal = maps.get(agent, IsotonicMap())(p, clip)
        total += weights.get(agent, 0.0) * logit(cal)
    return total


@dataclass(frozen=True)
class ReliabilityBin:
    index: int
    lo: float
    hi: float
    n: int
    mean_p: float
    observed_rate: float


@dataclass(frozen=True)
class Reliability:
    bins: tuple[ReliabilityBin, ...]
    ece: float
    spiegelhalter_z: float | None
    n: int


def reliability(p: Sequence[float], y: Sequence[int], bins: int) -> Reliability:
    """Equal-width bins over [0, 1] (empty bins omitted), ECE and the Spiegelhalter Z statistic."""
    n = len(p)
    if n == 0:
        return Reliability((), 0.0, None, 0)
    acc: dict[int, list[tuple[float, int]]] = {}
    for prob, label in zip(p, y, strict=True):
        index = min(int(prob * bins), bins - 1)
        acc.setdefault(index, []).append((prob, label))
    out: list[ReliabilityBin] = []
    ece = 0.0
    for index in sorted(acc):
        items = acc[index]
        mean_p = sum(v for v, _ in items) / len(items)
        rate = sum(label for _, label in items) / len(items)
        ece += len(items) / n * abs(mean_p - rate)
        out.append(ReliabilityBin(index, index / bins, (index + 1) / bins, len(items), mean_p, rate))
    num = sum((label - prob) * (1 - 2 * prob) for prob, label in zip(p, y, strict=True))
    var = sum((1 - 2 * prob) ** 2 * prob * (1 - prob) for prob in p)
    z = num / math.sqrt(var) if var > 0 else None
    return Reliability(tuple(out), ece, z, n)


def reliability_json(
    target_type: str, label_spec_version: str, charts: Mapping[str, Reliability]
) -> dict[str, Any]:
    """The reliability chart export: per agent (and `pooled`) the bins, ECE, Spiegelhalter Z and n."""
    return {
        "target_type": target_type,
        "label_spec_version": label_spec_version,
        "agents": {
            agent: {
                "n": chart.n,
                "ece": chart.ece,
                "spiegelhalter_z": chart.spiegelhalter_z,
                "bins": [
                    {
                        "bin_index": b.index,
                        "bin_lo": b.lo,
                        "bin_hi": b.hi,
                        "n": b.n,
                        "mean_p": b.mean_p,
                        "observed_rate": b.observed_rate,
                    }
                    for b in chart.bins
                ],
            }
            for agent, chart in sorted(charts.items())
        },
    }
