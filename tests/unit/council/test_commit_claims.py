"""Blind commit (edit after commit detected) and anonymized claim sharing."""

from __future__ import annotations

import pytest

from council_builders import AS_OF, COIN, LABEL_SPEC, claim
from hdt.contracts.common import AgentName, ClaimKind, TargetType
from hdt.contracts.forecast import AgentForecast
from hdt.council.claims import news_safe, share, shown_to
from hdt.council.commit import RoundCommit, verify_commit


def forecast(agent: AgentName, p: float) -> AgentForecast:
    return AgentForecast(
        agent=agent,
        agent_version="1",
        event_id="ev",
        round=1,
        coin_id=COIN,
        as_of=AS_OF,
        target_type=TargetType.RESID_12H,
        label_spec_version=LABEL_SPEC,
        abstain=False,
        p_model=0.6,
        p_llm=p,
        p_used=p,
        reason="round one",
    )


def test_an_edit_after_the_round_one_commit_is_detected() -> None:
    forecasts = {"technical": forecast(AgentName.TECHNICAL, 0.62), "micro": forecast(AgentName.MICRO, 0.41)}
    commit = RoundCommit.of("ev", 1, forecasts)
    assert verify_commit(commit, forecasts) == []
    edited = {**forecasts, "micro": forecasts["micro"].model_copy(update={"p_used": 0.6, "p_llm": 0.6})}
    assert verify_commit(commit, edited) == ["micro"]
    reworded = {**forecasts, "technical": forecasts["technical"].model_copy(update={"reason": "hindsight"})}
    assert verify_commit(commit, reworded) == ["technical"]
    dropped = {"technical": forecasts["technical"]}
    assert verify_commit(commit, dropped) == ["micro"]


def test_commit_is_independent_of_insertion_order() -> None:
    a, b = forecast(AgentName.TECHNICAL, 0.62), forecast(AgentName.MICRO, 0.41)
    assert RoundCommit.of("ev", 1, {"technical": a, "micro": b}) == RoundCommit.of(
        "ev", 1, {"micro": b, "technical": a}
    )


def test_shared_claims_are_anonymized_truncated_and_deterministic() -> None:
    long_text = "funding is deeply negative " * 20
    c1 = claim("my-local-id", ClaimKind.PACKET, "funding_z", long_text, value=-1.8)
    c2 = claim("other", ClaimKind.PACKET, "rsi_14", "RSI high", value=61.0)
    items = [("technical", "a" * 64, c1), ("micro", "b" * 64, c2)]
    shared = share(items, event_id="ev", round_=1, max_chars=280)
    again = share(list(reversed(items)), event_id="ev", round_=1, max_chars=280)
    assert shared == again
    ids = {s.claim.claim_id for s in shared}
    assert "my-local-id" not in ids
    assert all(i.startswith("sc_") for i in ids)
    assert all(len(s.claim.statement) <= 280 for s in shared)
    # an agent is never shown its own claims, and a packet field name (which names the source agent's
    # family) is replaced by the shared id
    [seen] = shown_to("technical", shared)
    assert seen.statement == "RSI high"
    assert seen.ref == seen.claim_id
    assert "rsi_14" not in seen.model_dump_json()


@pytest.mark.parametrize(
    "statement",
    ["entry around 65,430", "entry 65.4k", "support near 65,400", "level 65432", "entry at 65.43K"],
)
def test_a_rounded_or_abbreviated_level_is_hidden_from_news(statement: str) -> None:
    """m10 repro: `65,430` and `65.4k` (a secret level of 65432.1) were shown to the News agent."""
    assert not news_safe(claim("u1", ClaimKind.URL, "n_1", statement), [65432.1])


@pytest.mark.parametrize(
    "statement", ["7 exchange listing headlines today", "volume up 40% in 2026", "about 100 posts"]
)
def test_unrelated_numbers_stay_visible_to_news(statement: str) -> None:
    assert news_safe(claim("u1", ClaimKind.URL, "n_1", statement), [65432.1, 0.62])
