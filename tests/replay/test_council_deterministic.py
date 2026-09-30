"""Deterministic replay (phase 06 success criterion): the same event run twice gives the same decision card
hash, whatever the wall clock; a missing cached LLM output fails the replay instead of changing it."""

from __future__ import annotations

from datetime import timedelta

import pytest
from langgraph.checkpoint.memory import InMemorySaver

from council_builders import (
    AS_OF,
    LONG_B,
    MemoryDecisionStore,
    ScriptedRunner,
    Turn,
    claim,
    event_input,
    run_meeting,
    services,
)
from hdt.agents.llm_types import LlmCacheMissError
from hdt.contracts.common import AgentName, ClaimKind, DirectionHint
from hdt.core.clock import ManualClock, use_clock
from hdt.council.graph import CouncilGraph
from hdt.council.ports import AgentRequest, AgentResult

C, T, M, F, N, X = (
    AgentName.CROWDING,
    AgentName.TECHNICAL,
    AgentName.MICRO,
    AgentName.FUNDAMENTAL,
    AgentName.NEWS,
    AgentName.MACRO,
)
P_MODEL = {C: 0.62, T: 0.45, M: 0.58, F: 0.6, N: 0.5, X: 0.52}
FEATURES = {C: {"ltx_strict_pass": True, "ltx_side": "LONG"}}
TURNS = {
    (C, 1): Turn(
        0.64,
        claims=(claim("a", ClaimKind.PACKET, "ltx_strict_pass", "LTX strict fired"),),
        candidate_id=LONG_B,
    ),
    (T, 1): Turn(0.42, claims=(claim("b", ClaimKind.PACKET, "rsi_14", "RSI 61", value=61.0),)),
    (M, 1): Turn(
        0.60,
        claims=(
            claim("c", ClaimKind.PACKET, "funding_z", "funding negative", value=-1.8, hint=DirectionHint.UP),
        ),
        candidate_id=LONG_B,
    ),
    (F, 1): Turn(0.61, candidate_id=LONG_B),
    (N, 1): Turn(None),
    (X, 1): Turn(0.50),
    (T, 2): Turn(0.60, cite_where=(("statement", "LTX strict fired"),)),
    (M, 2): Turn(0.62, cite_where=(("statement", "RSI 61"),), candidate_id=LONG_B),
    (F, 2): Turn(0.63, cite_where=(("statement", "LTX strict fired"),), candidate_id=LONG_B),
    (X, 2): Turn(0.58, cite_where=(("statement", "funding negative"),)),
    (C, 2): Turn(0.64),
}


async def replay_once(start_offset: timedelta) -> tuple[str, object]:
    runner = ScriptedRunner(p_model=P_MODEL, turns=TURNS, features=FEATURES)
    with use_clock(ManualClock(AS_OF + start_offset)):
        record = await run_meeting(services(runner))
    return record.card["card_sha256"], record


async def test_same_event_twice_gives_the_same_card_hash() -> None:
    first_hash, first = await replay_once(timedelta(seconds=5))
    second_hash, second = await replay_once(timedelta(days=13))
    assert first.card["rounds"] >= 2  # the debate (shuffle, shared ids, revisions) is part of the replay
    assert first_hash == second_hash
    assert first.forecasts == second.forecasts
    assert first.claims == second.claims
    assert first.decision == second.decision


async def test_changed_agent_output_changes_the_card_hash() -> None:
    base_hash, _ = await replay_once(timedelta(seconds=5))
    turns = {**TURNS, (X, 1): Turn(0.49)}
    runner = ScriptedRunner(p_model=P_MODEL, turns=turns, features=FEATURES)
    record = await run_meeting(services(runner))
    assert record.card["card_sha256"] != base_hash


class CacheMissRunner(ScriptedRunner):
    async def forecast(self, request: AgentRequest) -> AgentResult:
        if request.agent is M and request.round == 2:
            raise LlmCacheMissError("no cached generation for micro round 2")
        return await super().forecast(request)


async def test_cache_miss_fails_the_replay_without_a_card() -> None:
    runner = CacheMissRunner(p_model=P_MODEL, turns=TURNS, features=FEATURES)
    store = MemoryDecisionStore()
    app = CouncilGraph(services(runner, store=store)).compile(InMemorySaver())
    with pytest.raises(LlmCacheMissError):
        await app.ainvoke(event_input(), {"configurable": {"thread_id": "ev_test_1"}}, durability="sync")
    assert store.records == {}
