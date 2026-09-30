"""Manager (test-strategy 3.5): consensus size 1.0; after the debate p >= 0.60 / <= 0.40 with a matching
sign and D below the threshold gives 0.5; otherwise NO_TRADE. Candidate choice and the position intent."""

from __future__ import annotations

from decimal import Decimal

import pytest

from council_builders import AS_OF, LONG_A, LONG_B, SHORT_A, candidate_set
from hdt.contracts.candidate import CandidateSet, LevelCandidate
from hdt.contracts.common import Intent, Side
from hdt.council.manager import Outcome, manage, position_intent, select_candidate

RULE = {"p_long": 0.6, "p_short": 0.4, "d_threshold": 0.8, "size_consensus": 1.0, "size_fallback": 0.5}


def run(side: Side | None, p: float | None, d: float | None, *, debate_over: bool = True):  # type: ignore[no-untyped-def]
    return manage(side, p, d, debate_over=debate_over, **RULE)


def test_consensus_trades_full_size() -> None:
    decision = run(Side.LONG, 0.58, 1.5)
    assert (decision.outcome, decision.size, decision.rule) == (Outcome.LONG, 1.0, "consensus")


def test_consensus_against_the_pooled_sign_does_not_trade() -> None:
    decision = run(Side.LONG, 0.47, 0.2)
    assert decision.outcome is Outcome.NO_TRADE


@pytest.mark.parametrize(
    ("p", "d", "expected"),
    [
        (0.60, 0.3, Outcome.LONG),
        (0.40, 0.3, Outcome.SHORT),
        (0.59, 0.3, Outcome.NO_TRADE),
        (0.41, 0.3, Outcome.NO_TRADE),
        (0.65, 0.8, Outcome.NO_TRADE),
        (0.65, 0.79, Outcome.LONG),
    ],
)
def test_fallback_after_the_debate(p: float, d: float, expected: Outcome) -> None:
    decision = run(None, p, d)
    assert decision.outcome is expected
    assert decision.size == (0.5 if expected is not Outcome.NO_TRADE else 0.0)


def test_fallback_needs_the_debate_to_be_over() -> None:
    assert run(None, 0.7, 0.1, debate_over=False).outcome is Outcome.NO_TRADE


def test_nothing_to_pool_is_no_trade() -> None:
    assert run(None, None, None).outcome is Outcome.NO_TRADE


def test_select_candidate_by_weight_then_rr_then_mark() -> None:
    cs = candidate_set()
    # weight wins: A has more w~ behind it
    chosen = select_candidate(
        Side.LONG, {"x": LONG_A, "y": LONG_A, "z": LONG_B}, {"x": 0.2, "y": 0.2, "z": 0.3}, cs, None
    )
    assert chosen is not None
    assert chosen.candidate_id == LONG_A
    # tie on weight: higher R:R (B has rr 3.0)
    chosen = select_candidate(Side.LONG, {"x": LONG_A, "z": LONG_B}, {"x": 0.3, "z": 0.3}, cs, Decimal("100"))
    assert chosen is not None
    assert chosen.candidate_id == LONG_B
    # a vote for the other side's candidate or an unknown id does not count
    assert select_candidate(Side.LONG, {"x": SHORT_A, "y": None}, {"x": 1.0, "y": 1.0}, cs, None) is None


def test_select_candidate_tie_on_rr_goes_to_the_closest_entry() -> None:
    tick = Decimal("0.01")
    near = LevelCandidate(
        candidate_id="lc_DDDDDDDDDDDDDDDD",
        side=Side.LONG,
        entry=Decimal("100.00"),
        invalidation=Decimal("99.00"),
        tp1=Decimal("102.00"),
        rr=2.0,
        tick=tick,
    )
    far = LevelCandidate(
        candidate_id="lc_EEEEEEEEEEEEEEEE",
        side=Side.LONG,
        entry=Decimal("95.00"),
        invalidation=Decimal("94.00"),
        tp1=Decimal("97.00"),
        rr=2.0,
        tick=tick,
    )
    cs = CandidateSet(coin_id=1, as_of=AS_OF, levels_ver="v", candidates=(near, far))
    chosen = select_candidate(
        Side.LONG, {"x": near.candidate_id, "y": far.candidate_id}, {"x": 0.5, "y": 0.5}, cs, Decimal("96.00")
    )
    assert chosen == far


@pytest.mark.parametrize(
    ("outcome", "held", "p", "reevaluation", "expected"),
    [
        (Outcome.LONG, None, 0.7, False, Intent.OPEN),
        (Outcome.NO_TRADE, None, 0.5, False, None),
        (Outcome.LONG, Side.LONG, 0.7, False, Intent.HOLD),
        (Outcome.SHORT, Side.LONG, 0.3, False, Intent.EXIT),
        (Outcome.NO_TRADE, Side.LONG, 0.55, True, Intent.HOLD),
        (Outcome.NO_TRADE, Side.LONG, 0.45, True, Intent.EXIT),
        (Outcome.NO_TRADE, Side.SHORT, 0.45, True, Intent.HOLD),
        (Outcome.NO_TRADE, Side.LONG, 0.45, False, None),
    ],
)
def test_position_intent(
    outcome: Outcome, held: Side | None, p: float, reevaluation: bool, expected: Intent | None
) -> None:
    """A held coin never gets an OPEN (no adds): same direction HOLD, opposite EXIT; a held re-evaluation
    without a trade decision holds while the pool still leans the position's way."""
    assert position_intent(outcome, held, p, reevaluation=reevaluation) is expected
