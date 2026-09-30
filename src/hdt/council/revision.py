"""Bounds on an agent's forecast, re-enforced by the council whatever the runner returned (pure).

Round 1: `z_used = z_model + a_i * clip(z_llm - z_model, +-llm_logit_margin)`.
Revision (round >= 2), relative to the agent's effective forecast of the previous round:
- an abstaining revision changes nothing (the previous forecast stays in force);
- `z_llm` moves at most `logit_bound_round` (0.5) per round and stays within `logit_bound_total` (1.0) of
  `z_model` (clipped into both bounds), then `z_used = z_model + a_i * (z_llm - z_model)`;
- any change of `p` needs at least one NEW citation: a claim shared to this agent in this round that it
  had not cited before;
- a change of direction needs at least one cited new claim that is `hard` with a `direction_hint` of the
  new direction. The direction a revision is compared with is the agent's last side (`sign(p_used - 0.5)`
  of its latest effective forecast that was not exactly 0.5), so stopping on 0.5 on the way never splits
  one flip into two soft steps;
- on a violation the previous `p_model`, `p_used` and `candidate_id` are kept, and `p_llm` becomes the
  previous effective LLM probability (the one the bounds were computed from), so a later round is bounded
  exactly like this one (claims, citations and provenance of the submitted revision are still recorded).
`a_i` is clamped to [0, 1] (NaN counts as 0: no LLM adjustment) before it is used.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Set
from dataclasses import dataclass, field
from typing import Any, Final

from hdt.contracts.common import DirectionHint
from hdt.contracts.forecast import AgentForecast, Claim
from hdt.council.aggregate import clip, logit, sigmoid

_SAME: Final[float] = 1e-12

ABSTAINED: Final[str] = "abstained_in_debate"
NO_NEW_CITATION: Final[str] = "no_new_citation"
FLIP_WITHOUT_HARD: Final[str] = "flip_without_hard_evidence"
CLIPPED: Final[str] = "clipped_to_logit_bounds"


def _rebuild(forecast: AgentForecast, **update: Any) -> AgentForecast:
    return AgentForecast.model_validate({**forecast.model_dump(mode="python"), **update})


def clamp_a(a_i: float) -> float:
    """LLM trust in [0, 1]; a non-finite value gives 0 (the LLM cannot move `p_used`)."""
    return min(max(a_i, 0.0), 1.0) if math.isfinite(a_i) else 0.0


def enforce_round1(forecast: AgentForecast, *, a_i: float, margin: float) -> AgentForecast:
    """Recompute `p_used` of a round-1 forecast from `p_model`, `p_llm` and `a_i`."""
    if forecast.abstain or forecast.p_model is None or forecast.p_llm is None:
        return forecast
    z_model = logit(forecast.p_model)
    z_used = z_model + clamp_a(a_i) * clip(logit(forecast.p_llm) - z_model, -margin, margin)
    return _rebuild(forecast, p_used=sigmoid(z_used))


def effective_z_llm(forecast: AgentForecast, *, margin: float, bound_total: float) -> float:
    """The LLM logit the next revision is bounded against (round 1: within the margin of z_model)."""
    if forecast.p_model is None or forecast.p_llm is None:
        raise ValueError("an abstaining forecast has no LLM logit")
    z_model = logit(forecast.p_model)
    bound = margin if forecast.round == 1 else bound_total
    return z_model + clip(logit(forecast.p_llm) - z_model, -bound, bound)


def side_of(p: float) -> int:
    return 1 if p > 0.5 else -1 if p < 0.5 else 0


@dataclass(frozen=True)
class Revision:
    effective: AgentForecast
    accepted: bool
    violations: tuple[str, ...] = ()
    new_citations: tuple[str, ...] = ()
    notes: tuple[str, ...] = field(default=())

    def to_json(self) -> dict[str, Any]:
        return {
            "accepted": self.accepted,
            "violations": list(self.violations),
            "new_citations": list(self.new_citations),
            "notes": list(self.notes),
        }


def revise(
    previous: AgentForecast,
    submitted: AgentForecast,
    *,
    shown: Mapping[str, Claim],
    cited_before: Set[str],
    a_i: float,
    margin: float,
    bound_round: float,
    bound_total: float,
    last_side: int = 0,
) -> Revision:
    """Apply the revision rules; `shown` = shared claims this agent saw this round, by shared id;
    `last_side` = the side of the agent's latest effective forecast that was not exactly 0.5 (0: none),
    used when `previous` sits exactly on 0.5."""
    keep: dict[str, Any] = {
        "p_model": previous.p_model,
        "p_llm": previous.p_llm,
        "p_used": previous.p_used,
        "abstain": previous.abstain,
        "abstain_reason": previous.abstain_reason,
        "candidate_id": previous.candidate_id,
    }
    new_ids = tuple(
        dict.fromkeys(cid for cid in submitted.cited_claim_ids if cid in shown and cid not in cited_before)
    )
    if previous.abstain:
        return Revision(_rebuild(submitted, **keep), False, (ABSTAINED,), new_ids)
    if previous.p_model is None or previous.p_used is None:
        raise ValueError("a forecast that did not abstain must carry p_model and p_used")
    z_model = logit(previous.p_model)
    z_prev = effective_z_llm(previous, margin=margin, bound_total=bound_total)
    keep["p_llm"] = sigmoid(z_prev)
    if submitted.abstain or submitted.p_llm is None:
        return Revision(_rebuild(submitted, **keep), False, (ABSTAINED,), new_ids)
    lo = max(z_prev - bound_round, z_model - bound_total)
    hi = min(z_prev + bound_round, z_model + bound_total)
    if lo > hi:
        lo = hi = z_prev
    z_raw = logit(submitted.p_llm)
    z_llm = clip(z_raw, lo, hi)
    notes = (CLIPPED,) if abs(z_llm - z_raw) > _SAME else ()
    p_used = sigmoid(z_model + clamp_a(a_i) * (z_llm - z_model))
    candidate_id = submitted.candidate_id if submitted.candidate_id is not None else previous.candidate_id
    if abs(z_llm - z_prev) <= _SAME:
        unchanged = _rebuild(submitted, **{**keep, "candidate_id": candidate_id})
        return Revision(unchanged, True, (), new_ids, notes)
    if not new_ids:
        return Revision(_rebuild(submitted, **keep), False, (NO_NEW_CITATION,), new_ids, notes)
    old_side = side_of(previous.p_used) or last_side
    new_side = side_of(p_used)
    if old_side != 0 and new_side != 0 and old_side != new_side:
        wanted = DirectionHint.UP if new_side > 0 else DirectionHint.DOWN
        if not any(shown[cid].hard and shown[cid].direction_hint is wanted for cid in new_ids):
            return Revision(_rebuild(submitted, **keep), False, (FLIP_WITHOUT_HARD,), new_ids, notes)
    effective = _rebuild(
        submitted,
        p_model=previous.p_model,
        p_llm=sigmoid(z_llm),
        p_used=p_used,
        abstain=False,
        abstain_reason=None,
        candidate_id=candidate_id,
    )
    return Revision(effective, True, (), new_ids, notes)
