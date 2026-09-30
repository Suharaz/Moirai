"""Log opinion pool (Design Contract section 3), pure.

For every agent with an opinion (abstaining agents are excluded):
- r-mixing of its debate path in logit space: `z_i = z_i^(1) + r_i * (z_i^final - z_i^(1))`,
  `p_i = sigmoid(z_i)` (`p_used` of round 1 and of the final round);
- pooled `logit(p) = b + sum_i w~_i * logit(cal_i(p_i))` with `w~` normalized over those agents;
- `p` clipped to `p_clip` ([0.02, 0.98]).
`D` (disagreement) is the w~-weighted standard deviation of the calibrated logits `logit(cal_i(p_i))`.
When the learned parameters carry a stacker (>= 300 independent outcomes), the stacker's probability
replaces the pool; `D` is computed the same way.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Mapping
from dataclasses import dataclass

from hdt.council.consensus import normalize
from hdt.council.ports import Stacker

_EPS = 1e-9


def logit(p: float) -> float:
    q = min(max(p, _EPS), 1.0 - _EPS)
    return math.log(q / (1.0 - q))


def sigmoid(z: float) -> float:
    if z >= 0:
        return 1.0 / (1.0 + math.exp(-z))
    e = math.exp(z)
    return e / (1.0 + e)


def clip(value: float, lo: float, hi: float) -> float:
    return min(max(value, lo), hi)


def mixed_p(p_round1: float, p_final: float, r: float) -> float:
    """`sigmoid(z1 + r * (z_final - z1))`: r=0 keeps round 1, r=1 takes the final round."""
    z1 = logit(p_round1)
    return sigmoid(z1 + r * (logit(p_final) - z1))


def disagreement(logits: Mapping[str, float], weights: Mapping[str, float]) -> float:
    """w-weighted standard deviation of the logits (0 when a single agent or every logit is equal)."""
    if not logits:
        return 0.0
    w = normalize(weights, sorted(logits))
    mean = sum(w[a] * logits[a] for a in logits)
    variance = sum(w[a] * (logits[a] - mean) ** 2 for a in logits)
    return math.sqrt(max(variance, 0.0))


@dataclass(frozen=True)
class PoolResult:
    p: float
    """Pooled probability after the clip."""
    p_unclipped: float
    disagreement: float
    weights: Mapping[str, float]
    mixed: Mapping[str, float]
    """p_i after r-mixing."""
    logits: Mapping[str, float]
    """logit(cal_i(p_i))."""
    method: str
    """`log_pool` or `stacker`."""


def aggregate(
    round1: Mapping[str, float],
    final: Mapping[str, float],
    *,
    weights: Mapping[str, float],
    r: Callable[[str], float],
    calibrate: Callable[[str, float], float],
    intercept_b: float,
    p_clip: tuple[float, float],
    stacker: Stacker | None = None,
    regime: str | None = None,
) -> PoolResult | None:
    """Pool the agents present in both `round1` and `final` (their `p_used`); None when nobody is."""
    present = sorted(a for a in final if a in round1)
    if not present:
        return None
    w = normalize(weights, present)
    mixed = {a: mixed_p(round1[a], final[a], clip(r(a), 0.0, 1.0)) for a in present}
    logits = {a: logit(clip(calibrate(a, mixed[a]), _EPS, 1.0 - _EPS)) for a in present}
    if stacker is not None:
        raw = float(stacker.predict(mixed, regime))
        method = "stacker"
    else:
        raw = sigmoid(intercept_b + sum(w[a] * logits[a] for a in present))
        method = "log_pool"
    lo, hi = p_clip
    return PoolResult(
        p=clip(raw, lo, hi),
        p_unclipped=raw,
        disagreement=disagreement(logits, w),
        weights=w,
        mixed=mixed,
        logits=logits,
        method=method,
    )
