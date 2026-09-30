"""Consensus rule (Design Contract section 3), pure.

- Stance from `p_used`: LONG when `p >= stance_long` (0.55), SHORT when `p <= stance_short` (0.45), else
  NEUTRAL; abstaining agents have no stance and are dropped.
- Quorum: at least `quorum` (4) agents with an opinion.
- Weight rule: the winning side holds >= `supermajority` (2/3) of the total normalized weight `w~` of the
  agents with an opinion (neutral agents count in the total).
- Correlated-group rule: the winning side must also win the vote when every agent is one vote and each
  correlation group (`momentum` = {technical, micro}) counts as ONE vote (the side holding the larger weight
  inside the group; no vote when the group is tied or neutral). Winning means strictly more votes than the
  opposite side.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum

from hdt.contracts.common import Side


class Stance(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"
    NEUTRAL = "NEUTRAL"


def stance_of(p: float, *, stance_long: float, stance_short: float) -> Stance:
    if p >= stance_long:
        return Stance.LONG
    if p <= stance_short:
        return Stance.SHORT
    return Stance.NEUTRAL


@dataclass(frozen=True)
class Check:
    """One line of a decision-card check list (`consensus` / `manager_rule` JSON: {passed, text})."""

    passed: bool | None
    text: str

    def to_json(self) -> dict[str, bool | str | None]:
        return {"passed": self.passed, "text": self.text}


@dataclass(frozen=True)
class ConsensusResult:
    reached: bool
    side: Side | None
    """The side with the larger weight (None when tied or nobody takes a side)."""
    stances: Mapping[str, Stance]
    weights: Mapping[str, float]
    """Normalized w~ over the agents with an opinion."""
    side_weight: float
    votes_for: int
    votes_against: int
    checks: tuple[Check, ...] = field(default=())


def normalize(weights: Mapping[str, float], present: list[str]) -> dict[str, float]:
    """w~ over `present`: proportional to the given weights, uniform when they sum to zero."""
    raw = {agent: max(0.0, float(weights.get(agent, 0.0))) for agent in present}
    total = sum(raw.values())
    if total <= 0:
        return {agent: 1.0 / len(present) for agent in present} if present else {}
    return {agent: value / total for agent, value in raw.items()}


def _group_vote(members: list[str], stances: Mapping[str, Stance], w: Mapping[str, float]) -> Stance:
    long_w = sum(w[a] for a in members if stances[a] is Stance.LONG)
    short_w = sum(w[a] for a in members if stances[a] is Stance.SHORT)
    if long_w > short_w:
        return Stance.LONG
    if short_w > long_w:
        return Stance.SHORT
    return Stance.NEUTRAL


def group_votes(
    stances: Mapping[str, Stance],
    weights: Mapping[str, float],
    groups: Mapping[str, tuple[str, ...]],
) -> list[Stance]:
    """One vote per agent outside every group, one vote per group with present members."""
    grouped: set[str] = set()
    votes: list[Stance] = []
    for name in sorted(groups):
        members = [a for a in groups[name] if a in stances and a not in grouped]
        grouped.update(groups[name])
        if members:
            votes.append(_group_vote(members, stances, weights))
    votes.extend(stances[a] for a in sorted(stances) if a not in grouped)
    return votes


def consensus(
    p_used: Mapping[str, float | None],
    weights: Mapping[str, float],
    *,
    stance_long: float,
    stance_short: float,
    quorum: int,
    supermajority: float,
    groups: Mapping[str, tuple[str, ...]],
) -> ConsensusResult:
    """`p_used` maps every agent to its probability, or None when it abstained."""
    present = sorted(a for a, p in p_used.items() if p is not None)
    stances = {
        a: stance_of(p, stance_long=stance_long, stance_short=stance_short)
        for a in present
        if (p := p_used[a]) is not None
    }
    w = normalize(weights, present)
    long_w = sum(w[a] for a in present if stances[a] is Stance.LONG)
    short_w = sum(w[a] for a in present if stances[a] is Stance.SHORT)
    side: Side | None = None
    if long_w > short_w:
        side = Side.LONG
    elif short_w > long_w:
        side = Side.SHORT
    side_weight = max(long_w, short_w)
    checks = [Check(len(present) >= quorum, f"quorum: {len(present)} agents with an opinion (need {quorum})")]
    votes_for = votes_against = 0
    if side is None:
        checks.append(Check(False, f"no winning side (LONG w~ {long_w:.3f}, SHORT w~ {short_w:.3f})"))
    else:
        checks.append(
            Check(
                side_weight >= supermajority - 1e-12,
                f"{side.value} holds {side_weight:.3f} of w~ (need {supermajority:.3f})",
            )
        )
        wanted = Stance(side.value)
        other = Stance.SHORT if wanted is Stance.LONG else Stance.LONG
        votes = group_votes(stances, w, groups)
        votes_for = sum(1 for v in votes if v is wanted)
        votes_against = sum(1 for v in votes if v is other)
        checks.append(
            Check(
                votes_for > votes_against,
                f"correlated groups as one vote: {side.value} {votes_for} vs {other.value} {votes_against}",
            )
        )
    reached = side is not None and all(check.passed for check in checks)
    return ConsensusResult(
        reached=reached,
        side=side,
        stances=stances,
        weights=w,
        side_weight=side_weight,
        votes_for=votes_for,
        votes_against=votes_against,
        checks=tuple(checks),
    )
