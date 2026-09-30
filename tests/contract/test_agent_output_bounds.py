"""Agent output bounds (phase 05 Success Criterion 1, test-strategy 3.4 and 4): whatever the LLM writes,
no forecast leaves `validate` with a logit adjustment beyond the bound, a candidate outside the shared set
or on the wrong side, a claim pointing to a field that does not exist, or a self-declared verification."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from hdt.agents.validate import Bounds, check_draft, logit
from hdt.contracts.candidate import CandidateSet, LevelCandidate
from hdt.contracts.common import AgentName, Side
from hdt.contracts.forecast import AgentForecastDraft

AS_OF = datetime(2026, 9, 1, 12, tzinfo=UTC)
LONG_ID = "lc_AAAAAAAAAAAAAAAA"
SHORT_ID = "lc_BBBBBBBBBBBBBBBB"
SET = CandidateSet(
    coin_id=7,
    as_of=AS_OF,
    levels_ver="lv1",
    candidates=(
        LevelCandidate(
            candidate_id=LONG_ID,
            side=Side.LONG,
            entry=Decimal(10),
            invalidation=Decimal(9),
            tp1=Decimal(12),
            rr=2.0,
            tick=Decimal(1),
        ),
        LevelCandidate(
            candidate_id=SHORT_ID,
            side=Side.SHORT,
            entry=Decimal(10),
            invalidation=Decimal(11),
            tp1=Decimal(8),
            rr=2.0,
            tick=Decimal(1),
        ),
    ),
)


def bounds(
    *, p_model: float = 0.5, a_i: float = 1.0, round_: int = 1, agent: AgentName = AgentName.CROWDING
) -> Bounds:
    return Bounds(
        agent=agent,
        round=round_,
        p_model=p_model,
        a_i=a_i,
        llm_logit_margin=0.5,
        logit_bound_total=1.0,
        stance_long=0.55,
        stance_short=0.45,
        candidate_set=SET,
        packet_fields=frozenset({"skew_4h", "fdz"}),
        tool_result_ids=frozenset({"get_snapshot:0123456789abcdef01234567"}),
        news_item_ids=frozenset({"rss:9"}),
        shared_claim_ids=frozenset({"s_1"}),
    )


def llm_output(**fields: Any) -> AgentForecastDraft:
    """Parse raw LLM JSON exactly as the router does."""
    return AgentForecastDraft.model_validate_json(json.dumps({"abstain": False, **fields}))


probabilities = st.floats(min_value=0.001, max_value=0.999)


@given(
    p_model=st.floats(min_value=0.02, max_value=0.98),
    p_llm=probabilities,
    a_i=st.floats(0, 1),
    round_=st.integers(1, 3),
)
def test_no_output_leaves_validate_beyond_the_logit_bound(
    p_model: float, p_llm: float, a_i: float, round_: int
) -> None:
    checked = check_draft(llm_output(p_llm=p_llm), bounds(p_model=p_model, a_i=a_i, round_=round_))
    assert checked.p_used is not None
    margin = 0.5 if round_ == 1 else 1.0
    assert abs(logit(checked.p_used) - logit(p_model)) <= margin + 1e-9


def test_out_of_bound_adjustment_is_clipped_and_reported() -> None:
    checked = check_draft(llm_output(p_llm=0.95), bounds())
    assert checked.p_used == pytest.approx(1 / (1 + pow(2.718281828459045, -0.5)))
    assert checked.p_llm == 0.95
    assert any("clipped" in problem for problem in checked.problems)


@given(a_i=st.floats(-5, 5))
def test_a_i_outside_its_range_cannot_widen_the_bound(a_i: float) -> None:
    checked = check_draft(llm_output(p_llm=0.99), bounds(a_i=a_i))
    assert checked.p_used is not None
    assert abs(logit(checked.p_used)) <= 0.5 + 1e-9


@pytest.mark.parametrize("a_i", [float("nan"), float("inf"), float("-inf")], ids=["nan", "inf", "-inf"])
def test_a_non_finite_a_i_gives_no_llm_weight(a_i: float) -> None:
    """m9: `+inf` clamped to 1.0 here while the council's `clamp_a` gives 0.0."""
    checked = check_draft(llm_output(p_llm=0.99), bounds(p_model=0.4, a_i=a_i))
    assert checked.p_used == pytest.approx(0.4)


@pytest.mark.parametrize(
    ("p_llm", "candidate_id"),
    [
        (0.9, "lc_ZZZZZZZZZZZZZZZZ"),  # not in the shared set
        (0.9, SHORT_ID),  # LONG stance, SHORT candidate
        (0.1, LONG_ID),  # SHORT stance, LONG candidate
        (0.5, LONG_ID),  # no stance at all
    ],
)
def test_candidate_outside_the_set_or_off_side_is_blocked(p_llm: float, candidate_id: str) -> None:
    checked = check_draft(llm_output(p_llm=p_llm, candidate_id=candidate_id), bounds())
    assert checked.candidate_id is None


def test_same_side_candidate_of_the_set_is_kept() -> None:
    assert check_draft(llm_output(p_llm=0.9, candidate_id=LONG_ID), bounds()).candidate_id == LONG_ID
    assert check_draft(llm_output(p_llm=0.1, candidate_id=SHORT_ID), bounds()).candidate_id == SHORT_ID


def test_news_agent_and_held_coin_events_never_carry_a_candidate() -> None:
    news = check_draft(llm_output(p_llm=0.9, candidate_id=LONG_ID), bounds(agent=AgentName.NEWS))
    held = Bounds(**{**bounds().__dict__, "candidate_set": None})
    assert news.candidate_id is None
    assert check_draft(llm_output(p_llm=0.9, candidate_id=LONG_ID), held).candidate_id is None


def test_malformed_candidate_id_never_parses() -> None:
    with pytest.raises(ValueError, match="candidate_id"):
        llm_output(p_llm=0.9, candidate_id="c2")


def test_claims_pointing_to_missing_fields_results_or_items_are_blocked() -> None:
    claims = [
        {"claim_id": "a", "kind": "packet", "ref": "skew_4h", "statement": "s"},
        {"claim_id": "b", "kind": "packet", "ref": "features.fdz", "statement": "s"},
        {"claim_id": "c", "kind": "packet", "ref": "invented_field", "statement": "s"},
        {"claim_id": "d", "kind": "tool", "ref": "get_snapshot:0123456789abcdef01234567", "statement": "s"},
        {"claim_id": "e", "kind": "tool", "ref": "get_snapshot:ffffffffffffffffffffffff", "statement": "s"},
        {"claim_id": "f", "kind": "url", "ref": "rss:9", "statement": "s", "quote": "q"},
        {"claim_id": "g", "kind": "url", "ref": "rss:10", "statement": "s", "quote": "q"},
        {"claim_id": "a", "kind": "packet", "ref": "fdz", "statement": "duplicate id"},
    ]
    checked = check_draft(
        llm_output(p_llm=0.6, claims=claims, cited_claim_ids=["a", "c", "e", "s_1", "s_2"]), bounds(round_=2)
    )
    assert [(c.claim_id, c.ref) for c in checked.claims] == [
        ("a", "skew_4h"),
        ("b", "fdz"),
        ("d", "get_snapshot:0123456789abcdef01234567"),
        ("f", "rss:9"),
    ]
    assert checked.cited_claim_ids == ("a", "s_1")


def test_self_declared_verification_is_ignored() -> None:
    claim = {
        "claim_id": "a",
        "kind": "packet",
        "ref": "skew_4h",
        "statement": "s",
        "verified": True,
        "hard": True,
        "tier": "T0",
        "domain_url": "binance.com",
    }
    [kept] = check_draft(llm_output(p_llm=0.6, claims=[claim]), bounds()).claims
    assert not kept.verified
    assert not kept.hard
    assert kept.tier is None
    assert kept.domain_url is None


def test_abstaining_output_carries_no_probability_or_candidate() -> None:
    checked = check_draft(
        AgentForecastDraft.model_validate({"abstain": True, "p_llm": 0.9, "candidate_id": LONG_ID}), bounds()
    )
    assert checked.abstain
    assert checked.p_used is None
    assert checked.candidate_id is None
    assert checked.abstain_reason
