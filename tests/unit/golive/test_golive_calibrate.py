"""Phase 11 threshold calibration: proposals from the calibration set only, rendered as a config diff."""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta

import pytest
from golive_events import RULES, T0, agent_event, informative_history

from hdt.golive.calibrate import (
    Proposal,
    calibrate_d_threshold,
    calibrate_eta,
    calibrate_ltx_threshold,
    calibration_events,
    config_diff,
    net_return,
)

TAKER = 0.0005
SLIP = 2.0
COST = 2 * TAKER + 2 * SLIP / 10_000  # 0.0014


def test_net_return_signs_and_costs() -> None:
    ev = agent_event(0, 1, {"alpha": (0.6, 0.6, 0.5)}, label_value=0.01)
    assert net_return(ev, "LONG", taker=TAKER, slippage_bp=SLIP) == pytest.approx(0.01 - COST)
    assert net_return(ev, "SHORT", taker=TAKER, slippage_bp=SLIP) == pytest.approx(-0.01 - COST)
    resid = agent_event(
        0, 1, {"alpha": (0.6, 0.6, 0.5)}, target_type="RESID_12H", label_value=0.01, beta_btc=-1.5
    )
    assert net_return(resid, "LONG", taker=TAKER, slippage_bp=SLIP) == pytest.approx(0.01 - COST * 2.5)
    no_beta = replace(resid, beta_btc=None)
    assert net_return(no_beta, "LONG", taker=TAKER, slippage_bp=SLIP) is None


def _fallback_events() -> list:  # type: ignore[type-arg]
    """No consensus (one long, one short, one neutral) and pooled p >= p_long: the fallback applies.
    Low-disagreement trades win 1%, high-disagreement trades lose 1%."""
    out = []
    paths = {"alpha": (0.9, 0.9, 0.5), "beta": (0.3, 0.3, 0.5), "gamma": (0.5, 0.5, 0.5)}
    for i in range(80):
        d = 0.3 if i % 2 == 0 else 1.1
        out.append(
            agent_event(
                i,
                1,
                paths,
                disagreement=d,
                label_value=0.01 if d < 1.0 else -0.01,
                as_of=T0 + timedelta(hours=8 * i),
            )
        )
    return out


def test_d_threshold_proposal_excludes_losing_disagreement() -> None:
    prop = calibrate_d_threshold(_fallback_events(), RULES, taker=TAKER, slippage_bp=SLIP)
    by_value = {row["value"]: row for row in prop.grid}
    assert by_value[0.4]["n"] == 40
    assert by_value[0.4]["mean"] == pytest.approx(0.01 - COST)
    assert by_value[1.2]["n"] == 80
    assert by_value[1.2]["mean"] == pytest.approx(-COST)
    # every d in (0.3, 1.1] keeps only the winners; ties go to the smallest d
    assert prop.proposed == 0.4
    assert prop.current == RULES.d_threshold
    assert prop.path == "manager.d_threshold"


def test_d_threshold_keeps_current_when_nothing_is_eligible() -> None:
    prop = calibrate_d_threshold(_fallback_events()[:10], RULES, taker=TAKER, slippage_bp=SLIP)
    assert prop.proposed is None
    assert "no D with >= 30" in prop.reason


def test_ltx_threshold_raises_the_cut_above_noise() -> None:
    events = []
    for i in range(120):
        score = 4.5 if i % 3 == 0 else 6.0
        events.append(
            agent_event(
                i,
                1,
                {"alpha": (0.6, 0.6, 0.5)},
                scan_score=score,
                scan_side="LONG",
                label_value=-0.01 if score < 5 else 0.01,
                as_of=T0 + timedelta(hours=6 * i),
            )
        )
    prop = calibrate_ltx_threshold(events, current=4.0, floor=2.5, taker=TAKER, slippage_bp=SLIP)
    cuts = [row["value"] for row in prop.grid]
    assert cuts[0] == 2.5
    assert 4.0 in cuts
    # the noise sits at 4.5, above the current cut: 4.5 < cut <= 6.0 keeps only winners, the smallest wins
    assert prop.proposed == 5.0
    assert prop.notes == ["a changed scanner value needs a new scanner rule_version"]


def test_eta_proposal_is_deterministic_and_bounded_to_the_grid() -> None:
    history = informative_history(60)
    first = calibrate_eta(history, agents=RULES.agents, current=0.13, alpha=0.01)
    second = calibrate_eta(history, agents=RULES.agents, current=0.13, alpha=0.01)
    assert first == second
    losses = {row["value"]: row["mean_log_loss"] for row in first.grid}
    best = min(losses, key=lambda v: (losses[v], abs(v - 0.13)))
    assert first.proposed == (None if best == 0.13 else best)
    few = calibrate_eta(history[:10], agents=RULES.agents, current=0.13, alpha=0.01)
    assert few.proposed is None
    assert "only 10 events" in few.reason


def test_calibration_events_stop_at_the_split() -> None:
    history = informative_history(10) + informative_history(4, target_type="RESID_12H")
    split = T0 + timedelta(hours=12 * 5)
    chosen = calibration_events(history, {"RAW_12H": split})
    assert [e.event_id for e in chosen] == [f"ev{i:04d}" for i in range(5)]
    assert all(e.target_type == "RAW_12H" for e in chosen)


def test_config_diff_lists_only_changes() -> None:
    keep = Proposal("config/scoring.yaml", "hedge.eta", 0.13, None, [], "keep")
    change = Proposal("config/council.yaml", "manager.d_threshold", 0.8, 0.6, [], "better")
    assert config_diff([keep, change]) == (
        "--- config/council.yaml\n"
        "+++ config/council.yaml (proposed)\n"
        "@@ manager.d_threshold @@\n"
        "-manager.d_threshold: 0.8\n"
        "+manager.d_threshold: 0.6\n"
    )
    assert config_diff([keep]) == ""
