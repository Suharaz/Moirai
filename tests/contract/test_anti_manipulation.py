"""End-to-end anti-manipulation contract with fake agents (phase 06 success criterion, test-strategy 3.5).

A split round 1 (no consensus) opens the debate. In round 2 agents try to move the council without valid
evidence: a flip without citations, repeated claims, fabricated numbers, a fabricated quote on an attacker
page, a flip backed only by a correct but non-hard packet claim, and a flip backed by a hard claim of the
opposite direction. The council keeps each such agent's previous forecast and never shares the bad
claims; a flip backed by a hard claim of the new direction is the one that goes through.
"""

from __future__ import annotations

from typing import Any

import pytest

from council_builders import (
    AGENTS,
    ScriptedRunner,
    Sources,
    Turn,
    claim,
    run_meeting,
    services,
    sigmoid,
    stored_source,
)
from hdt.contracts.common import AgentName, ClaimKind, DirectionHint, Tier
from hdt.council.revision import FLIP_WITHOUT_HARD, NO_NEW_CITATION
from hdt.council.verifier import QUOTE_NOT_FOUND, REPEATED, VALUE_MISMATCH

C, T, M, F, N, X = (
    AgentName.CROWDING,
    AgentName.TECHNICAL,
    AgentName.MICRO,
    AgentName.FUNDAMENTAL,
    AgentName.NEWS,
    AgentName.MACRO,
)
P_MODEL = {C: 0.6, T: 0.45, M: 0.45, F: 0.6, N: 0.45, X: 0.5}
FEATURES = {
    C: {"ltx_strict_pass": True, "ltx_side": "LONG"},
    X: {"migration_strict_pass": True, "migration_side": "SHORT"},
}
ATTACKER_PAGE = "XYZ price analysis: traders expect volatility this week."
SOURCES = Sources([stored_source("n_att", ATTACKER_PAGE, tier=Tier.T3, official=False, event_class=None)])

HARD_UP = claim(
    "h_up", ClaimKind.PACKET, "ltx_strict_pass", "LTX strict trigger fired", hint=DirectionHint.UP
)
SOFT_UP = claim(
    "s_up", ClaimKind.PACKET, "rsi_14", "RSI is 61, momentum up", value=61.0, hint=DirectionHint.UP
)
HARD_DOWN = claim(
    "h_dn", ClaimKind.PACKET, "migration_strict_pass", "migration trigger fired", hint=DirectionHint.DOWN
)
FAKE_NUMBER = claim(
    "f_num", ClaimKind.PACKET, "funding_z", "funding z is -4.5", value=-4.5, hint=DirectionHint.UP
)
FAKE_QUOTE = claim(
    "f_quote",
    ClaimKind.URL,
    "n_att",
    "exchange confirms listing",
    quote="Binance confirms XYZ listing tomorrow",
)

ROUND1 = {
    (C, 1): Turn(0.62, claims=(HARD_UP, FAKE_QUOTE)),
    (T, 1): Turn(0.40),
    (M, 1): Turn(0.40),
    (F, 1): Turn(0.62, claims=(SOFT_UP,)),
    (N, 1): Turn(0.40),
    (X, 1): Turn(0.50, claims=(HARD_DOWN, FAKE_NUMBER)),
}


def meeting(round2: dict[tuple[AgentName, int], Turn]) -> Any:
    runner = ScriptedRunner(p_model=P_MODEL, turns={**ROUND1, **round2}, features=FEATURES)
    return runner, services(runner, sources=SOURCES)


def forecast_row(record: Any, agent: AgentName, round_: int) -> dict[str, Any]:
    return next(f for f in record.forecasts if f["agent"] == agent.value and f["round"] == round_)


def claim_row(record: Any, original_id: str, round_: int) -> dict[str, Any]:
    return next(c for c in record.claims if c["original_claim_id"] == original_id and c["round"] == round_)


ATTEMPTS = {
    # flip LONG -> SHORT with no citation at all (the News agent is never sent: it may see none of these
    # packet claims)
    (C, 2): Turn(0.40),
    # flip citing only a correct, verified, but non-hard packet claim
    (M, 2): Turn(0.62, cite_where=(("statement", "RSI is 61"),)),
    # flip citing a hard claim whose code direction is DOWN
    (T, 2): Turn(0.62, cite_where=(("statement", "migration trigger fired"),)),
    # repeats its own round-1 claim, and one of another agent
    (F, 2): Turn(
        0.62,
        claims=(
            SOFT_UP,
            claim(
                "rep", ClaimKind.PACKET, "ltx_strict_pass", "LTX strict trigger fired", hint=DirectionHint.UP
            ),
        ),
    ),
    (X, 2): Turn(0.50),
}


@pytest.fixture(scope="module")
def attempted() -> tuple[ScriptedRunner, Any]:
    import asyncio

    runner, svc = meeting(ATTEMPTS)
    return runner, asyncio.run(run_meeting(svc))


def test_split_round_one_goes_to_debate(attempted: tuple[ScriptedRunner, Any]) -> None:
    _, record = attempted
    assert record.card["rounds"] == 2
    assert record.card["stop_reason"] == "no_new_claims"
    assert not record.card["params"]["round1_consensus"]


def test_flip_without_claims_is_refused(attempted: tuple[ScriptedRunner, Any]) -> None:
    _, record = attempted
    row = forecast_row(record, C, 2)
    assert row["revision"]["violations"] == [NO_NEW_CITATION]
    assert row["forecast"]["p_used"] == pytest.approx(forecast_row(record, C, 1)["forecast"]["p_used"])
    assert row["stance"] == "LONG"
    assert not any(f["agent"] == N.value and f["round"] == 2 for f in record.forecasts)


def test_repeated_claims_are_rejected_and_not_shared_again(attempted: tuple[ScriptedRunner, Any]) -> None:
    _runner, record = attempted
    assert claim_row(record, "s_up", 2)["reject_reason"] == REPEATED
    assert claim_row(record, "rep", 2)["reject_reason"] == REPEATED
    assert not any(c["shared"] for c in record.claims if c["round"] == 2)


def test_fabricated_number_is_rejected_penalized_and_never_shown(
    attempted: tuple[ScriptedRunner, Any],
) -> None:
    runner, record = attempted
    row = claim_row(record, "f_num", 1)
    assert row["reject_reason"] == VALUE_MISMATCH
    assert row["penalty"]
    assert not row["shared"]
    shown = [c for claims in runner.shown.values() for c in claims]
    assert all("funding z is -4.5" not in c.statement for c in shown)


def test_fabricated_quote_on_an_attacker_page_is_rejected(attempted: tuple[ScriptedRunner, Any]) -> None:
    runner, record = attempted
    row = claim_row(record, "f_quote", 1)
    assert row["reject_reason"] == QUOTE_NOT_FOUND
    assert row["penalty"]
    assert all(c.kind is not ClaimKind.URL for claims in runner.shown.values() for c in claims)


def test_correct_but_non_hard_packet_claim_cannot_unlock_a_flip(
    attempted: tuple[ScriptedRunner, Any],
) -> None:
    _, record = attempted
    assert claim_row(record, "s_up", 1)["claim"]["verified"]
    assert not claim_row(record, "s_up", 1)["claim"]["hard"]
    row = forecast_row(record, M, 2)
    assert row["revision"]["violations"] == [FLIP_WITHOUT_HARD]
    assert row["stance"] == "SHORT"


def test_opposite_direction_hard_claim_cannot_unlock_a_flip(attempted: tuple[ScriptedRunner, Any]) -> None:
    _, record = attempted
    hard_down = claim_row(record, "h_dn", 1)["claim"]
    assert hard_down["hard"]
    assert hard_down["direction_hint"] == "down"
    row = forecast_row(record, T, 2)
    assert row["revision"]["violations"] == [FLIP_WITHOUT_HARD]
    assert row["stance"] == "SHORT"


def test_agents_never_see_reasons_or_sources(attempted: tuple[ScriptedRunner, Any]) -> None:
    runner, _ = attempted
    for claims in runner.shown.values():
        for c in claims:
            assert c.claim_id.startswith("sc_")
            assert "round 1" not in c.statement


def test_flip_backed_by_a_hard_claim_of_the_new_direction_is_accepted_within_bounds() -> None:
    import asyncio

    _runner, svc = meeting({(T, 2): Turn(0.62, cite_where=(("statement", "LTX strict trigger fired"),))})
    record = asyncio.run(run_meeting(svc))
    row = forecast_row(record, T, 2)
    assert row["revision"]["accepted"]
    # +0.5 logit per round at most: from logit(0.40) the LLM logit is clipped to logit(0.40) + 0.5
    z_model = -0.2006707
    z_prev = -0.4054651
    assert row["forecast"]["p_used"] == pytest.approx(sigmoid(z_model + (z_prev + 0.5 - z_model)), abs=1e-6)
    assert row["stance"] == "NEUTRAL"
    assert row["forecast"]["p_used"] > 0.5
    assert set(AGENTS) >= {AgentName(f["agent"]) for f in record.forecasts}
