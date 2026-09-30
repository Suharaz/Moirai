"""Manager rule, candidate choice and the position intent (Design Contract sections 3 and 5), pure.

Manager:
- consensus: the winning side, size `size_consensus` (1.0);
- no consensus once the debate is over (the last allowed round was played, the debate stopped early for
  lack of new claims, or debate is disabled): pooled `p >= p_long` (LONG) or `p <= p_short` (SHORT), with
  `sign(p - 0.5)` matching that direction and `D < d_threshold`, gives size `size_fallback` (0.5);
- otherwise NO_TRADE. Every direction, consensus included, must match `sign(p - 0.5)` of the pool.

`select_candidate`: w~ vote among the winning-side agents that chose a candidate of that side in the event's
shared set; ties go to the higher R:R, then to the entry closest to the mark, then to the smaller id.

`position_intent`: OPEN for a coin not held; for a held coin a same-direction decision is HOLD (never an
add) and an opposite one is EXIT; a held-coin re-evaluation emits HOLD or EXIT only.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

from hdt.contracts.candidate import CandidateSet, LevelCandidate
from hdt.contracts.common import Intent, Side
from hdt.council.consensus import Check


class Outcome(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"
    NO_TRADE = "NO_TRADE"


@dataclass(frozen=True)
class ManagerDecision:
    outcome: Outcome
    size: float
    rule: str
    """`consensus`, `fallback` or `no_trade`."""
    checks: tuple[Check, ...]

    @property
    def side(self) -> Side | None:
        return None if self.outcome is Outcome.NO_TRADE else Side(self.outcome.value)


def sign_matches(p: float, side: Side) -> bool:
    return p > 0.5 if side is Side.LONG else p < 0.5


def manage(
    agreed_side: Side | None,
    pooled_p: float | None,
    d: float | None,
    *,
    debate_over: bool,
    p_long: float,
    p_short: float,
    d_threshold: float,
    size_consensus: float,
    size_fallback: float,
) -> ManagerDecision:
    """`agreed_side`: the consensus side of the final round (None: no consensus)."""
    checks: list[Check] = [Check(agreed_side is not None, "consensus reached")]
    if pooled_p is None or d is None:
        checks.append(Check(False, "no agent with an opinion: nothing to pool"))
        return ManagerDecision(Outcome.NO_TRADE, 0.0, "no_trade", tuple(checks))
    if agreed_side is not None:
        ok = sign_matches(pooled_p, agreed_side)
        checks.append(Check(ok, f"pooled p {pooled_p:.3f} agrees with {agreed_side.value}"))
        if ok:
            return ManagerDecision(Outcome(agreed_side.value), size_consensus, "consensus", tuple(checks))
        return ManagerDecision(Outcome.NO_TRADE, 0.0, "no_trade", tuple(checks))
    checks.append(Check(debate_over, "debate over (fallback rule applies)"))
    side_of_p: Side | None = None
    if pooled_p >= p_long:
        side_of_p = Side.LONG
    elif pooled_p <= p_short:
        side_of_p = Side.SHORT
    checks.append(
        Check(side_of_p is not None, f"pooled p {pooled_p:.3f} >= {p_long:.2f} or <= {p_short:.2f}")
    )
    sign_ok = side_of_p is not None and sign_matches(pooled_p, side_of_p)
    checks.append(Check(sign_ok, "sign(p - 0.5) matches the direction"))
    d_ok = d < d_threshold
    checks.append(Check(d_ok, f"disagreement D {d:.3f} < {d_threshold:.3f}"))
    if debate_over and side_of_p is not None and sign_ok and d_ok:
        return ManagerDecision(Outcome(side_of_p.value), size_fallback, "fallback", tuple(checks))
    return ManagerDecision(Outcome.NO_TRADE, 0.0, "no_trade", tuple(checks))


def select_candidate(
    side: Side,
    choices: Mapping[str, str | None],
    weights: Mapping[str, float],
    candidate_set: CandidateSet,
    mark: Decimal | None,
) -> LevelCandidate | None:
    """`choices` maps each winning-side agent to its chosen candidate_id (None when it chose none)."""
    votes: dict[str, float] = {}
    for agent, candidate_id in choices.items():
        if candidate_id is None:
            continue
        candidate = candidate_set.get(candidate_id)
        if candidate is None or candidate.side is not side:
            continue
        votes[candidate_id] = votes.get(candidate_id, 0.0) + max(0.0, weights.get(agent, 0.0))
    if not votes:
        return None

    def rank(candidate_id: str) -> tuple[float, float, Decimal, str]:
        candidate = candidate_set.get(candidate_id)
        assert candidate is not None
        distance = abs(candidate.entry - mark) if mark is not None else Decimal(0)
        return (-round(votes[candidate_id], 12), -candidate.rr, distance, candidate_id)

    best = min(votes, key=rank)
    return candidate_set.get(best)


def position_intent(
    outcome: Outcome, held_side: Side | None, pooled_p: float | None, *, reevaluation: bool
) -> Intent | None:
    """The DecisionMsg intent, or None when nothing is sent.

    Coin not held: OPEN on a direction, nothing on NO_TRADE. Coin held: same direction HOLD, opposite EXIT.
    NO_TRADE on a held coin: a held-coin re-evaluation (source HELD) keeps the position while the pool still
    leans its way (HOLD) and exits otherwise; any other event sends nothing."""
    if held_side is None:
        return None if outcome is Outcome.NO_TRADE else Intent.OPEN
    if outcome is Outcome.NO_TRADE:
        if not reevaluation:
            return None
        if pooled_p is None:
            return Intent.HOLD
        return Intent.HOLD if sign_matches(pooled_p, held_side) else Intent.EXIT
    return Intent.HOLD if outcome.value == held_side.value else Intent.EXIT
