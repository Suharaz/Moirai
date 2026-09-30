"""Phase 11 numeric ablations: which components win the paired log-loss test."""

from __future__ import annotations

import math
from dataclasses import replace

import pytest
from golive_events import RULES, agent_event, informative_history

from hdt.golive.ablation import (
    FORWARD_SHADOW_BRANCHES,
    AblationResult,
    Comparison,
    acted_probability,
    cut_thresholds,
    run_ablation,
    variant_probabilities,
)
from hdt.golive.stats import PairedBootstrap


def _component(result: AblationResult, name: str) -> Comparison:
    return next(c for c in result.components if c.name == name)


def test_variants_replay_the_log_pool() -> None:
    ev = agent_event(0, 1, {"alpha": (0.6, 0.8, 0.5), "beta": (0.5, 0.5, 0.5), "gamma": (0.4, 0.4, 0.5)})
    probs = variant_probabilities(ev, RULES)

    def logit(p: float) -> float:
        return math.log(p / (1 - p))

    def sigmoid(z: float) -> float:
        return 1 / (1 + math.exp(-z))

    # r = 0.5 mixes alpha's round 1 and final in logit space
    alpha_mixed = (logit(0.6) + logit(0.8)) / 2
    assert probs["full"] == pytest.approx(sigmoid((alpha_mixed + 0 + logit(0.4)) / 3))
    assert probs["round1_pool"] == pytest.approx(sigmoid((logit(0.6) + logit(0.4)) / 3))
    assert probs["r1"] == pytest.approx(sigmoid((logit(0.8) + logit(0.4)) / 3))
    assert probs["a0"] == pytest.approx(0.5)
    assert probs["drop:gamma"] == pytest.approx(sigmoid(logit(0.6) / 2))


def test_components_win_only_when_they_help() -> None:
    result = run_ablation(informative_history(), RULES, target_type="RAW_12H", resamples=2000)
    assert result.n_events == 80
    assert result.replay_mismatches == 0
    assert _component(result, "debate").wins is True
    assert _component(result, "llm_adjustment").wins is True
    assert _component(result, "agent:alpha").wins is True
    # removing the wrong-leaning agent improves the pool: it does not win
    beta = _component(result, "agent:beta")
    assert beta.test is not None
    assert beta.test.mean < 0
    assert beta.wins is False
    assert all(c.retained for c in result.components)
    assert result.retained_components_win is False
    assert [b["name"] for b in result.to_json()["forward_shadow_branches"]] == [
        name for name, _ in FORWARD_SHADOW_BRANCHES
    ]


def test_holm_is_applied_within_the_component_family() -> None:
    result = run_ablation(informative_history(), RULES, target_type="RAW_12H", resamples=2000)
    tested = [c for c in result.components if c.test is not None]
    for c in tested:
        assert c.p_holm is not None
        assert c.test is not None
        assert c.p_holm >= c.test.p_value
    smallest = min(tested, key=lambda c: c.test.p_value)  # type: ignore[union-attr]
    assert smallest.p_holm == pytest.approx(min(1.0, len(tested) * smallest.test.p_value))  # type: ignore[union-attr]


def test_holm_family_is_the_retained_components_only() -> None:
    # one round everywhere: the debate is tested (final and round-1 paths differ) but not retained
    events = [replace(ev, rounds=1) for ev in informative_history()]
    result = run_ablation(events, RULES, target_type="RAW_12H", resamples=2000)
    debate = _component(result, "debate")
    assert debate.test is not None
    assert debate.retained is False
    assert debate.p_holm is None
    family = [c for c in result.components if c.retained and c.test is not None]
    assert len(family) == 4
    smallest = min(family, key=lambda c: c.test.p_value)  # type: ignore[union-attr]
    assert smallest.p_holm == pytest.approx(min(1.0, len(family) * smallest.test.p_value))  # type: ignore[union-attr]


def test_each_event_replays_with_its_pinned_council_rules() -> None:
    events = informative_history(20)
    today = replace(RULES, p_clip=(0.45, 0.55))
    # today's rules do not reproduce the stored pools: every clipped event is a replay mismatch
    assert run_ablation(events, today, target_type="RAW_12H", resamples=200).replay_mismatches > 0
    pinned = [replace(ev, rules=RULES) for ev in events]
    result = run_ablation(pinned, today, target_type="RAW_12H", resamples=200)
    assert result.replay_mismatches == 0


def test_an_agent_removed_since_the_window_still_gets_its_drop_test() -> None:
    """Review M-12: the agent components follow the roster the window ran with, not today's."""
    events = [replace(ev, rules=RULES) for ev in informative_history(20)]
    today = replace(RULES, agents=("alpha", "beta"))
    result = run_ablation(events, today, target_type="RAW_12H", resamples=200)
    gamma = _component(result, "agent:gamma")
    assert gamma.retained is True
    assert gamma.test is not None
    assert [c.name for c in result.components if c.name.startswith("agent:")] == [
        f"agent:{a}" for a in RULES.agents
    ]


def test_a_replay_mismatch_fails_the_ablation() -> None:
    events = informative_history(20)
    assert run_ablation(events, RULES, target_type="RAW_12H", resamples=200).replay_mismatches == 0
    tampered = [replace(events[0], p_pooled=0.5), *events[1:]]
    assert run_ablation(tampered, RULES, target_type="RAW_12H", resamples=200).replay_mismatches == 1

    test = PairedBootstrap(n=200, days=60, mean=0.02, ci_low=0.01, ci_high=0.03, p_value=0.001)
    wins = Comparison("debate", "debate", "full", "round1_pool", 200, 0.60, 0.62, test, 0.003, True)
    assert wins.wins is True
    assert AblationResult("RAW_12H", 200, 0, [wins], []).retained_components_win is True
    assert AblationResult("RAW_12H", 200, 1, [wins], []).retained_components_win is False


def test_untested_retained_component_fails_the_gate() -> None:
    # r = 0 everywhere: the debate cannot have changed the pool and is not retained
    events = [
        agent_event(i, i % 2, {"alpha": (0.7 if i % 2 else 0.3, 0.9 if i % 2 else 0.1, 0.5)}, r=0.0)
        for i in range(20)
    ]
    result = run_ablation(events, RULES, target_type="RAW_12H", resamples=500)
    assert _component(result, "debate").retained is False
    # beta and gamma never gave an opinion: not retained, and nothing can be dropped from a pool of one
    assert _component(result, "agent:beta").retained is False
    alpha = _component(result, "agent:alpha")
    assert alpha.retained is True
    assert alpha.n == 0
    assert alpha.wins is None
    assert result.retained_components_win is False


def test_other_target_types_are_ignored() -> None:
    raw = informative_history(10)
    resid = informative_history(6, target_type="RESID_12H")
    assert run_ablation(raw + resid, RULES, target_type="RESID_12H", resamples=200).n_events == 6


def test_decision_variants_use_no_trade_as_half() -> None:
    # no consensus (long, short, neutral); pooled p about 0.61 >= p_long, so only the fallback can trade
    paths = {"alpha": (0.9, 0.9, 0.5), "beta": (0.3, 0.3, 0.5), "gamma": (0.5, 0.5, 0.5)}
    wide = agent_event(0, 1, paths, disagreement=0.9)
    assert wide.p_pooled is not None
    assert wide.p_pooled > RULES.p_long
    assert acted_probability(wide, RULES, supermajority=2 / 3) == 0.5
    narrow = agent_event(0, 1, paths, disagreement=0.3)
    assert acted_probability(narrow, RULES, supermajority=2 / 3) == narrow.p_pooled
    strong = agent_event(1, 1, {"alpha": (0.8, 0.8, 0.5), "beta": (0.7, 0.7, 0.5), "gamma": (0.7, 0.7, 0.5)})
    assert acted_probability(strong, RULES, supermajority=2 / 3) == strong.p_pooled


def test_score_cuts_replace_events_below_the_cut() -> None:
    assert cut_thresholds({"LTX": 4.0}) == {"LTX": [4.0, 5.0, 6.0, 8.0]}
    events = [
        agent_event(
            i,
            1,
            {"alpha": (0.8, 0.8, 0.5), "beta": (0.7, 0.7, 0.5), "gamma": (0.7, 0.7, 0.5)},
            scan_score=4.5 if i % 2 else 9.0,
            scan_strict=True,
        )
        for i in range(12)
    ]
    result = run_ablation(events, RULES, target_type="RAW_12H", strict_min={"LTX": 4.0}, resamples=200)
    names = {c.name: c for c in result.decisions}
    # every event is a winning trade: cutting at 5 replaces half of them by 0.5, so the variant loses
    # (loss_variant - loss_full > 0)
    cut5 = names["cut:LTX>=5"]
    assert cut5.n == 12
    assert cut5.test is not None
    assert cut5.test.mean > 0
    assert names["cut:LTX>=4"].test is not None
    assert names["cut:LTX>=4"].test.mean == pytest.approx(0.0)
    assert all(c.component is None and not c.retained for c in result.decisions)
