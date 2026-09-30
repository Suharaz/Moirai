"""Log-loss on clipped probabilities (Design Contract section 4): every scored probability is clipped to
`[lo, hi]` (config `scoring.yaml` `loss_clip`, default [0.02, 0.98]) before the loss, so one confident
miss costs at most -ln(0.02) ~ 3.9 and cannot dominate a weight update."""

from __future__ import annotations

import math
from typing import Final

DEFAULT_CLIP: Final[tuple[float, float]] = (0.02, 0.98)


def clip_p(p: float, bounds: tuple[float, float] = DEFAULT_CLIP) -> float:
    lo, hi = bounds
    return min(max(p, lo), hi)


def log_loss(p: float, y: int, bounds: tuple[float, float] = DEFAULT_CLIP) -> float:
    """Binary log-loss of probability `p` (that the label is 1) for label `y` in {0, 1}."""
    if y not in (0, 1):
        raise ValueError(f"label must be 0 or 1, got {y!r}")
    q = clip_p(p, bounds)
    return -math.log(q if y == 1 else 1.0 - q)


def logit(p: float) -> float:
    return math.log(p / (1.0 - p))


def sigmoid(z: float) -> float:
    if z >= 0:
        return 1.0 / (1.0 + math.exp(-z))
    e = math.exp(z)
    return e / (1.0 + e)


def hit(p: float, y: int) -> bool:
    """The directional call of `p` matched the label; p == 0.5 makes no call and is never a hit."""
    return (p > 0.5 and y == 1) or (p < 0.5 and y == 0)
