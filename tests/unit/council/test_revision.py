"""Revision rules re-enforced by the council (pure): flips through 0.5, the keep path, `a_i` bounds."""

from __future__ import annotations

import math
from typing import Any

import pytest

from council_builders import AS_OF, COIN, LABEL_SPEC, claim, logit, sigmoid
from hdt.contracts.common import AgentName, ClaimKind, TargetType
from hdt.contracts.forecast import AgentForecast, Claim
from hdt.council.revision import FLIP_WITHOUT_HARD, NO_NEW_CITATION, enforce_round1, revise

BOUNDS: dict[str, Any] = {"margin": 0.5, "bound_round": 0.5, "bound_total": 1.0}


def forecast(
    round_: int, p_llm: float, p_used: float, *, p_model: float = 0.5, cited: tuple[str, ...] = ()
) -> AgentForecast:
    return AgentForecast(
        agent=AgentName.NEWS,
        agent_version="1",
        event_id="ev",
        round=round_,
        coin_id=COIN,
        as_of=AS_OF,
        target_type=TargetType.RESID_12H,
        label_spec_version=LABEL_SPEC,
        abstain=False,
        p_model=p_model,
        p_llm=p_llm,
        p_used=p_used,
        cited_claim_ids=cited,
    )


def soft(cid: str) -> dict[str, Claim]:
    return {cid: claim(cid, ClaimKind.URL, "n_1", "exchange notice", quote="maintenance window")}


def test_a_flip_through_exactly_one_half_needs_hard_evidence() -> None:
    """I1 repro: News (p_model 0.5) at 0.622 moves to 0.5 on a soft claim, then to 0.378 on another soft
    claim. Each step alone is not a flip by sign, but together they cross from LONG to SHORT."""
    round1 = enforce_round1(forecast(1, 0.9, 0.9), a_i=1.0, margin=0.5)
    assert round1.p_used == pytest.approx(sigmoid(0.5))
    r2 = revise(
        round1, forecast(2, 0.5, 0.5, cited=("s1",)), shown=soft("s1"), cited_before=set(), a_i=1.0, **BOUNDS
    )
    assert r2.accepted
    assert r2.effective.p_used == pytest.approx(0.5)
    r3 = revise(
        r2.effective,
        forecast(3, 0.2, 0.2, cited=("s2",)),
        shown=soft("s2"),
        cited_before={"s1"},
        a_i=1.0,
        last_side=1,
        **BOUNDS,
    )
    assert r3.violations == (FLIP_WITHOUT_HARD,)
    assert r3.effective.p_used == pytest.approx(0.5)


def test_leaving_one_half_on_the_same_side_as_before_is_not_a_flip() -> None:
    at_half = forecast(2, 0.5, 0.5)
    r3 = revise(
        at_half,
        forecast(3, 0.7, 0.7, cited=("s2",)),
        shown=soft("s2"),
        cited_before=set(),
        a_i=1.0,
        last_side=1,
        **BOUNDS,
    )
    assert r3.accepted
    assert r3.effective.p_used == pytest.approx(sigmoid(0.5))


def test_a_refused_revision_keeps_the_effective_llm_logit_not_the_raw_one() -> None:
    """M1 repro: round 1 p_llm 0.9 is bounded to z = +0.5; a refused round 2 kept the raw 0.9 under round 2,
    so round 3 was bounded against z = +1.0 and a request for 0.5 gave 0.622 instead of 0.5."""
    round1 = enforce_round1(forecast(1, 0.9, 0.9), a_i=1.0, margin=0.5)
    refused = revise(round1, forecast(2, 0.9, 0.9), shown={}, cited_before=set(), a_i=1.0, **BOUNDS)
    assert refused.violations == (NO_NEW_CITATION,)
    assert refused.effective.p_llm == pytest.approx(sigmoid(0.5))
    r3 = revise(
        refused.effective,
        forecast(3, 0.5, 0.5, cited=("s1",)),
        shown=soft("s1"),
        cited_before=set(),
        a_i=1.0,
        **BOUNDS,
    )
    assert r3.accepted
    assert r3.effective.p_used == pytest.approx(0.5)


@pytest.mark.parametrize(("a_i", "expected_z"), [(math.nan, 0.0), (2.0, 0.5), (-1.0, 0.0)])
def test_llm_trust_is_clamped_to_the_unit_interval(a_i: float, expected_z: float) -> None:
    """M15: a NaN or out-of-range `a_i` diverged from the runner (which clamps) or produced NaN."""
    z_model = logit(0.6)
    got = enforce_round1(forecast(1, 0.9, 0.9, p_model=0.6), a_i=a_i, margin=0.5)
    assert got.p_used == pytest.approx(sigmoid(z_model + expected_z))
    revised = revise(
        forecast(1, 0.9, 0.9, p_model=0.6),
        forecast(2, 0.95, 0.95, p_model=0.6, cited=("s1",)),
        shown=soft("s1"),
        cited_before=set(),
        a_i=a_i,
        **BOUNDS,
    )
    assert revised.effective.p_used == pytest.approx(sigmoid(z_model + (expected_z * 2)))
