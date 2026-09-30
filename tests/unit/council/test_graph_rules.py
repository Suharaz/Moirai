"""Meeting-level rules with scripted agents: what the News agent is shown, the flip rule across rounds,
pinned scoring parameters and the round-1 commit re-checked before a card is emitted."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from typing import Any

import pytest
from langgraph.checkpoint.memory import InMemorySaver

from council_builders import (
    AS_OF,
    LONG_A,
    MemoryDecisionStore,
    Params,
    ScriptedRunner,
    Sources,
    Turn,
    UniformParams,
    candidate_set,
    claim,
    event_input,
    run_meeting,
    services,
    stored_source,
)
from hdt.contracts.common import AgentName, ClaimKind, Tier
from hdt.council.commit import CommitMismatchError
from hdt.council.graph import CouncilGraph, DecisionRecord
from hdt.council.revision import FLIP_WITHOUT_HARD
from hdt.tools.base import ToolResult

C, T, M, F, N, X = (
    AgentName.CROWDING,
    AgentName.TECHNICAL,
    AgentName.MICRO,
    AgentName.FUNDAMENTAL,
    AgentName.NEWS,
    AgentName.MACRO,
)
P_MODEL = {C: 0.6, T: 0.45, M: 0.45, F: 0.6, N: 0.5, X: 0.5}
SPLIT = {
    (C, 1): Turn(0.62),
    (T, 1): Turn(0.40),
    (M, 1): Turn(0.40),
    (F, 1): Turn(0.62),
    (N, 1): Turn(0.9),
    (X, 1): Turn(0.50),
}
NEWS_ID = "get_news:" + "a" * 24


def forecast_row(record: DecisionRecord, agent: AgentName, round_: int) -> dict[str, Any]:
    return next(f for f in record.forecasts if f["agent"] == agent.value and f["round"] == round_)


def test_news_is_shown_only_news_tool_claims_that_repeat_no_level_or_probability() -> None:
    """C2 repro: in round 2 the News agent was shown every other agent's claims, including packet fields
    and tool claims naming the candidate levels, with the source family readable in the `ref`."""
    headlines = ToolResult(
        tool="get_news", status="ok", result_id=NEWS_ID, data={"headline_count": 7.0, "level": 100.0}
    )
    shared = (
        claim("p1", ClaimKind.PACKET, "rsi_14", "RSI is 61, momentum up", value=61.0),
        claim("t1", ClaimKind.TOOL, NEWS_ID, "7 exchange listing headlines today", value=7.0),
        claim("t2", ClaimKind.TOOL, NEWS_ID, "support sits at 100.00 per the feed", value=7.0),
        claim("t3", ClaimKind.TOOL, NEWS_ID, "a level quoted in the news", value=100.0),
    )
    turns = {**SPLIT, (F, 1): Turn(0.62, claims=shared, tool_results=(headlines,))}
    runner = ScriptedRunner(p_model=P_MODEL, turns=turns)
    record = asyncio.run(run_meeting(services(runner)))
    assert all(c["reject_reason"] is None for c in record.claims if c["round"] == 1)
    news = runner.shown[(N, 2)]
    assert [c.statement for c in news] == ["7 exchange listing headlines today"]
    assert all(c.ref == c.claim_id for c in news)
    technical = runner.shown[(T, 2)]
    assert len(technical) == 4
    assert all(c.ref == c.claim_id and "rsi" not in c.ref and "get_news" not in c.ref for c in technical)
    levels = {float(x) for c in candidate_set().candidates for x in (c.entry, c.invalidation, c.tp1)}
    assert not any(isinstance(c.value, float) and c.value in levels for c in news)


URL_TEXT = "The exchange posted a maintenance window for the token bridge. Deposits resume after review."
SOURCES = Sources([stored_source("n_bridge", URL_TEXT, tier=Tier.T3, official=False, event_class=None)])


def test_news_cannot_flip_through_exactly_one_half_over_two_rounds() -> None:
    """I1 repro: News at 0.622 moved to 0.5 on one soft claim, then to 0.378 on another, both accepted."""
    note1 = claim(
        "u1", ClaimKind.URL, "n_bridge", "maintenance window posted", quote="posted a maintenance window"
    )
    note2 = claim("u2", ClaimKind.URL, "n_bridge", "deposits paused", quote="Deposits resume after review")
    turns = {
        **SPLIT,
        (C, 1): Turn(0.62, claims=(note1,)),
        (F, 2): Turn(0.62, claims=(note2,)),
        (N, 2): Turn(0.5, cite_where=(("statement", "maintenance window"),)),
        (N, 3): Turn(0.2, cite_where=(("statement", "deposits paused"),)),
    }
    runner = ScriptedRunner(p_model=P_MODEL, turns=turns)
    record = asyncio.run(run_meeting(services(runner, sources=SOURCES)))
    assert record.card["rounds"] == 3
    r2 = forecast_row(record, N, 2)
    assert r2["revision"]["accepted"]
    assert r2["forecast"]["p_used"] == pytest.approx(0.5)
    r3 = forecast_row(record, N, 3)
    assert r3["revision"]["violations"] == [FLIP_WITHOUT_HARD]
    assert r3["forecast"]["p_used"] == pytest.approx(0.5)


@dataclass
class Bumped(UniformParams):
    """A newer parameter version whose calibration differs."""

    def calibrate(self, agent: str, p: float) -> float:
        return min(0.98, p + 0.1)


class PublishingParams(Params):
    """Version 1 is the newest when the meeting starts; version 2 is published right after."""

    def __init__(self) -> None:
        super().__init__(UniformParams(params_version=1))
        self.versions = {1: self.params, 2: Bumped(params_version=2)}
        self.newest = 1

    def load(
        self,
        target_type: Any,
        label_spec_version: str,
        *,
        at: Any = None,
        params_version: Any = None,
        pins: Any = None,
    ) -> Any:
        self.calls.append((at, params_version))
        chosen = self.versions[params_version if params_version is not None else self.newest]
        self.newest = 2
        return chosen


def test_a_meeting_pools_with_the_parameter_version_it_pinned() -> None:
    """I3 repro: `commit_round1` and `aggregate` loaded the newest version again, so a version published
    mid-meeting changed the weights and calibration while the card still named the first version."""
    unanimous = {(a, 1): Turn(0.62) for a in AgentName}
    baseline = asyncio.run(run_meeting(services(ScriptedRunner(p_model=P_MODEL, turns=unanimous))))
    params = PublishingParams()
    record = asyncio.run(
        run_meeting(services(ScriptedRunner(p_model=P_MODEL, turns=unanimous), params=params))
    )
    assert baseline.card["p_pooled"] is not None
    assert record.card["p_pooled"] == pytest.approx(baseline.card["p_pooled"])
    assert record.card["params"]["params_version"] == 1
    assert params.calls[0] == (AS_OF, None)
    assert all(call == (AS_OF, 1) for call in params.calls[1:])


def test_every_later_load_reuses_the_pins_load_context_resolved() -> None:
    """m11: the stacking model, agent-version overlays and claims-audit flags were re-resolved by time at
    every load; a scorer commit spanning `as_of` could change them mid-meeting or on a resume."""
    pins = {
        "stacker_trained_through": "2026-03-01T00:00:00.000000Z",
        "live_versions": {"crowding": 3},
        "flagged": [],
    }
    params = Params(UniformParams(params_version=1, pins=pins))
    unanimous = {(a, 1): Turn(0.62) for a in AgentName}
    record = asyncio.run(
        run_meeting(services(ScriptedRunner(p_model=P_MODEL, turns=unanimous), params=params))
    )
    assert params.pins_seen[0] is None
    assert len(params.pins_seen) > 1
    assert all(seen == pins for seen in params.pins_seen[1:])
    assert record.card["params"]["pins"] == pins


def test_a_side_candidate_is_dropped_when_the_council_stance_is_not_that_side() -> None:
    """M12: the runner checked the candidate against its own `p_used` (total bound 1.0); the council's
    per-round bound leaves this revision NEUTRAL, so a LONG candidate cannot stay on the card."""
    features = {C: {"ltx_strict_pass": True, "ltx_side": "LONG"}}
    hard_up = claim("h_up", ClaimKind.PACKET, "ltx_strict_pass", "LTX strict trigger fired")
    turns = {
        **SPLIT,
        (N, 1): Turn(0.40),
        (C, 1): Turn(0.62, claims=(hard_up,)),
        (T, 2): Turn(0.62, cite_where=(("statement", "LTX strict"),), candidate_id=LONG_A),
    }
    runner = ScriptedRunner(p_model=P_MODEL, turns=turns, features=features)
    record = asyncio.run(run_meeting(services(runner)))
    row = forecast_row(record, T, 2)
    assert row["revision"]["accepted"]
    assert row["stance"] == "NEUTRAL"
    assert row["forecast"]["candidate_id"] is None


def test_a_card_whose_round_one_differs_from_the_blind_commit_is_not_emitted() -> None:
    """M6: nothing re-checked the card's round-1 forecasts against the commit before emit."""
    runner = ScriptedRunner(p_model=P_MODEL, turns=SPLIT)
    graph = CouncilGraph(services(runner))
    app = graph.compile(InMemorySaver())
    config = {"configurable": {"thread_id": "ev_test_1"}}
    asyncio.run(app.ainvoke(event_input(), config, durability="sync"))
    state = dict(asyncio.run(app.aget_state(config)).values)
    history = dict(state["history"])
    row = dict(history["1:technical"])
    row["forecast"] = {**row["forecast"], "p_used": 0.7}
    history["1:technical"] = row
    tampered = {**state, "history": history}
    fresh = CouncilGraph(replace(graph.s, store=MemoryDecisionStore()))
    with pytest.raises(CommitMismatchError):
        asyncio.run(fresh.emit_decision(tampered))  # type: ignore[arg-type]
    asyncio.run(fresh.emit_decision(state))  # type: ignore[arg-type]
    assert isinstance(fresh.s.store, MemoryDecisionStore)
    assert "ev_test_1" in fresh.s.store.records


def test_a_resumed_round_gets_the_earlier_rounds_full_tool_log_errors_included() -> None:
    """N1 (council side): the checkpoint kept only `ok` tool results, so a resumed round 2 rebuilt a
    session whose prompt said "none" where the uninterrupted one showed round 1's error result."""
    ok = ToolResult(tool="get_news", status="ok", result_id=NEWS_ID, data={"headline_count": 7.0})
    failed = ToolResult(
        tool="get_snapshot",
        status="error",
        message="tool get_snapshot is not available to the technical agent",
    )
    shared = (claim("t1", ClaimKind.TOOL, NEWS_ID, "7 exchange listing headlines today", value=7.0),)
    turns = {
        **SPLIT,
        (F, 1): Turn(0.62, claims=shared, tool_results=(ok,)),
        (T, 1): Turn(0.40, tool_results=(ok,), tool_log=(ok, failed)),
    }
    runner = ScriptedRunner(p_model=P_MODEL, turns=turns)
    asyncio.run(run_meeting(services(runner)))
    round2 = next(r for r in runner.requests if r.agent is T and r.round == 2)
    assert [e.tool_log for e in round2.earlier] == [(ok, failed)]
    assert round2.earlier[0].tool_results == (ok,)
