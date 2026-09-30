"""`r_i`: trust in an agent's debate revisions (Design Contract sections 3 and 4).

Pooled input is `z = z_round1 + r * (z_final - z_round1)`. Per debated event where the agent gave both a
round-1 and a final forecast: `delta = loss(final) - loss(round 1)` and
`r <- clip(r - kappa_r * delta, 0, 1)`, so revisions that lowered the loss earn trust. Events without
debate do not move `r`. Starts at 0.5 and resets to 0.5 on a new `agent_version`.
"""

from __future__ import annotations

from hdt.scoring.loss import log_loss


def update_r(
    r: float, *, p_round1: float, p_final: float, y: int, kappa: float, clip: tuple[float, float]
) -> float:
    delta = log_loss(p_final, y, clip) - log_loss(p_round1, y, clip)
    return min(max(r - kappa * delta, 0.0), 1.0)
