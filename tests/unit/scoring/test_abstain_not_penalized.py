"""Abstention (sleeping experts): an agent that abstains is neither rewarded nor punished."""

from __future__ import annotations

from datetime import date

import pytest
from tests.unit.scoring.scoring_events import EventFactory

from hdt.scoring.engine import EngineConfig, rescore
from hdt.scoring.hedge import HedgeState


def test_sleeper_keeps_its_share_while_the_awake_agents_are_rescored() -> None:
    hedge = HedgeState(("a", "b", "sleeper"), eta=0.5, alpha=0.0)
    hedge.advance_to(date(2026, 1, 1))
    hedge.update({"a": 0.3, "b": 0.9, "sleeper": 0.4})
    before = hedge.weights()
    for _ in range(30):
        hedge.update({"a": 0.2, "b": 1.4})
    after = hedge.weights()
    assert after["sleeper"] == pytest.approx(before["sleeper"], rel=1e-12)
    assert after["a"] > before["a"]
    assert after["b"] < before["b"]


def test_always_abstaining_agent_keeps_uniform_weight_and_is_never_scored(
    make_events: EventFactory, engine_cfg: EngineConfig
) -> None:
    result = rescore(make_events(300, abstain=("news",)), engine_cfg)
    assert not [row for row in result.scored if row.agent == "news"]
    last = {row.agent: row for row in result.weights if row.as_of == max(w.as_of for w in result.weights)}
    assert last["news"].w == pytest.approx(1 / 6, rel=1e-9)
    assert last["news"].coverage == 0.0
    assert last["news"].a == engine_cfg.a_initial
    assert last["news"].r == engine_cfg.r_initial
    assert last["crowding"].w > last["news"].w > last["fundamental"].w


def test_partial_abstention_costs_nothing_against_the_pool(
    make_events: EventFactory, engine_cfg: EngineConfig
) -> None:
    full = rescore(make_events(300, seed=4), engine_cfg)
    sparse = rescore(make_events(300, seed=4, abstain_rate=0.3), engine_cfg)
    last = max(w.as_of for w in full.weights)
    w_full = {r.agent: r.w for r in full.weights if r.as_of == last}
    w_sparse = {r.agent: r.w for r in sparse.weights if r.as_of == last}
    cov = {r.agent: r.coverage for r in sparse.weights if r.as_of == last}
    assert all(0.55 < c < 0.85 for c in cov.values())
    assert w_sparse["crowding"] == max(w_sparse.values())
    assert w_full["crowding"] == max(w_full.values())
