"""Code-side checks of one LLM forecast draft (phase 05 step 4, Design Contract section 3).

The LLM writes only an `AgentForecastDraft`. Everything that decides what the council may use is computed
or checked here, never taken from the LLM:

- `p_used = sigmoid(z_model + a_i * clip(z_llm - z_model, +-margin))`, with `margin` = `llm_logit_margin`
  in round 1 and `logit_bound_total` on revisions, and `a_i` clamped to [0, 1]. `p_llm` is kept as written
  (the council recomputes both bounds from it); a clipped adjustment is reported in `problems`.
- The stance follows from `p_used` (LONG `>= stance_long`, SHORT `<= stance_short`). `candidate_id` must be
  in the shared candidate set and on the stance side; otherwise it is dropped. The News agent and held-coin
  re-evaluations carry no candidate.
- Claims keep only resolvable references: `packet` -> a feature of the agent's own packet (a `features.`
  prefix is accepted and removed), `tool` -> the `result_id` of an ok tool result the agent received,
  `url` -> a news item id the agent saw in a tool result or its news assessment. Claims with a repeated id
  are dropped. Code-assigned fields (`verified`, `hard`, `tier`, `domain_url`) always start unset.
- `cited_claim_ids` keep only the agent's kept claims and, on revisions, the shared claim ids.
"""

from __future__ import annotations

import math
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Final

from hdt.contracts.candidate import CandidateSet
from hdt.contracts.common import AgentName, ClaimKind, Side
from hdt.contracts.forecast import AgentForecastDraft, Claim, ClaimDraft

PACKET_REF_PREFIX: Final[str] = "features."
AGENT_ABSTAINED: Final[str] = "agent_abstained"


def logit(p: float) -> float:
    if not 0.0 < p < 1.0:
        raise ValueError(f"probability {p} outside (0, 1)")
    return math.log(p / (1.0 - p))


def sigmoid(z: float) -> float:
    if z >= 0:
        return 1.0 / (1.0 + math.exp(-z))
    e = math.exp(z)
    return e / (1.0 + e)


def stance_of(p_used: float, stance_long: float, stance_short: float) -> Side | None:
    if p_used >= stance_long:
        return Side.LONG
    if p_used <= stance_short:
        return Side.SHORT
    return None


@dataclass(frozen=True)
class Bounds:
    """What the forecast is checked against; built by the runner from the request and the pinned config."""

    agent: AgentName
    round: int
    p_model: float
    a_i: float
    llm_logit_margin: float
    logit_bound_total: float
    stance_long: float
    stance_short: float
    candidate_set: CandidateSet | None
    packet_fields: frozenset[str] = frozenset()
    tool_result_ids: frozenset[str] = frozenset()
    news_item_ids: frozenset[str] = frozenset()
    shared_claim_ids: frozenset[str] = frozenset()

    @property
    def margin(self) -> float:
        return self.llm_logit_margin if self.round == 1 else self.logit_bound_total


@dataclass(frozen=True)
class Checked:
    """A draft after the code checks; `problems` lists every correction (logged and kept on the card)."""

    abstain: bool
    abstain_reason: str | None
    p_llm: float | None
    p_used: float | None
    stance: Side | None
    candidate_id: str | None
    claims: tuple[Claim, ...]
    cited_claim_ids: tuple[str, ...]
    reason: str
    problems: tuple[str, ...]


def used_probability(p_model: float, p_llm: float, a_i: float, margin: float) -> tuple[float, bool]:
    """`p_used` and whether the LLM adjustment had to be clipped."""
    z_model = logit(p_model)
    delta = logit(p_llm) - z_model
    clipped = min(max(delta, -margin), margin)
    # Same rule as `council.revision.clamp_a`: a non-finite trust (NaN or +/-inf) gives 0.
    a = min(max(a_i, 0.0), 1.0) if math.isfinite(a_i) else 0.0
    return sigmoid(z_model + a * clipped), clipped != delta


def check_claim(draft: ClaimDraft, bounds: Bounds) -> tuple[Claim | None, str | None]:
    """The code-side claim, or None with the reason it is not resolvable."""
    if draft.kind is ClaimKind.PACKET:
        ref = draft.ref.removeprefix(PACKET_REF_PREFIX)
        if ref not in bounds.packet_fields:
            return None, f"claim {draft.claim_id}: packet field {draft.ref!r} is not in the packet"
        draft = draft.model_copy(update={"ref": ref})
    elif draft.kind is ClaimKind.TOOL:
        if draft.ref not in bounds.tool_result_ids:
            return None, f"claim {draft.claim_id}: tool result {draft.ref!r} was not returned to the agent"
    elif draft.ref not in bounds.news_item_ids:
        return None, f"claim {draft.claim_id}: news item {draft.ref!r} was not seen by the agent"
    return Claim.from_draft(draft), None


def check_claims(drafts: Iterable[ClaimDraft], bounds: Bounds) -> tuple[tuple[Claim, ...], list[str]]:
    kept: list[Claim] = []
    problems: list[str] = []
    seen: set[str] = set()
    for draft in drafts:
        if draft.claim_id in seen:
            problems.append(f"claim {draft.claim_id}: repeated claim_id")
            continue
        seen.add(draft.claim_id)
        claim, problem = check_claim(draft, bounds)
        if claim is None:
            problems.append(problem or f"claim {draft.claim_id}: rejected")
            continue
        kept.append(claim)
    return tuple(kept), problems


def check_candidate(
    candidate_id: str | None, stance: Side | None, bounds: Bounds
) -> tuple[str | None, str | None]:
    """The kept candidate id, or None with the reason a named candidate was dropped."""
    if candidate_id is None:
        return None, None
    if bounds.agent is AgentName.NEWS:
        return None, f"candidate {candidate_id}: the News agent does not choose price levels"
    if bounds.candidate_set is None:
        return None, f"candidate {candidate_id}: no candidate set for this event (HOLD/EXIT only)"
    candidate = bounds.candidate_set.get(candidate_id)
    if candidate is None:
        return None, f"candidate {candidate_id}: not in the shared candidate set"
    if stance is None:
        return None, f"candidate {candidate_id}: the forecast has no stance"
    if candidate.side is not stance:
        return None, f"candidate {candidate_id}: {candidate.side.value} candidate for a {stance.value} stance"
    return candidate_id, None


def check_draft(draft: AgentForecastDraft, bounds: Bounds) -> Checked:
    claims, problems = check_claims(draft.claims, bounds)
    citable = {c.claim_id for c in claims} | (bounds.shared_claim_ids if bounds.round > 1 else frozenset())
    cited: list[str] = []
    for claim_id in draft.cited_claim_ids:
        if claim_id not in citable:
            problems.append(f"cited claim {claim_id}: unknown id")
        elif claim_id not in cited:
            cited.append(claim_id)
    if draft.abstain:
        if draft.candidate_id is not None:
            problems.append(f"candidate {draft.candidate_id}: dropped from an abstaining forecast")
        return Checked(
            abstain=True,
            abstain_reason=(draft.abstain_reason or "").strip() or AGENT_ABSTAINED,
            p_llm=None,
            p_used=None,
            stance=None,
            candidate_id=None,
            claims=claims,
            cited_claim_ids=tuple(cited),
            reason=draft.reason,
            problems=tuple(problems),
        )
    if draft.p_llm is None:  # excluded by AgentForecastDraft; kept for type narrowing
        raise ValueError("a non-abstaining draft needs p_llm")
    p_used, clipped = used_probability(bounds.p_model, draft.p_llm, bounds.a_i, bounds.margin)
    if clipped:
        problems.append(
            f"logit adjustment {logit(draft.p_llm) - logit(bounds.p_model):+.3f} clipped to "
            f"+-{bounds.margin:g}"
        )
    stance = stance_of(p_used, bounds.stance_long, bounds.stance_short)
    candidate_id, candidate_problem = check_candidate(draft.candidate_id, stance, bounds)
    if candidate_problem:
        problems.append(candidate_problem)
    return Checked(
        abstain=False,
        abstain_reason=None,
        p_llm=draft.p_llm,
        p_used=p_used,
        stance=stance,
        candidate_id=candidate_id,
        claims=claims,
        cited_claim_ids=tuple(cited),
        reason=draft.reason,
        problems=tuple(problems),
    )
