"""`a_i`: trust in the LLM-adjusted part of an agent's forecast (Design Contract sections 3 and 4).

`a <- clip(a + kappa_a * (loss(p_model) - loss(p_used)), 0, 1)` per scored round-1 forecast: the LLM
adjustment earns trust when `p_used` beats the agent's own `p_model`, loses it otherwise. Starts at 1.0
(the LLM has the full +-0.5 logit margin) and resets to 1.0 when the agent gets a new `agent_version`.
"""

from __future__ import annotations

from hdt.scoring.loss import log_loss


def update_a(
    a: float, *, p_model: float, p_used: float, y: int, kappa: float, clip: tuple[float, float]
) -> float:
    delta = log_loss(p_model, y, clip) - log_loss(p_used, y, clip)
    return min(max(a + kappa * delta, 0.0), 1.0)
