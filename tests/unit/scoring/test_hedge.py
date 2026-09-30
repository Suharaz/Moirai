"""Hedge weights: the synthetic-oracle acceptance test, fixed share, and the served-weight bounds."""

from __future__ import annotations

from datetime import date

import pytest

from hdt.core.config import scoring_config, static_config
from hdt.scoring.hedge import HedgeState, apply_caps
from hdt.scoring.oracle import NOISE_LIMIT, ORACLE_TARGET, ORACLE_WITHIN, evaluate
from hdt.scoring.regime_weights import HierarchicalHedge, effective_weights


def test_oracle_reaches_target_weight_within_150_forecasts_on_100_seeds() -> None:
    hedge = scoring_config().hedge
    runs = evaluate(hedge.eta, hedge.fixed_share_alpha, range(100))
    failed = [r for r in runs if not r.passed]
    assert not failed, [(r.seed, r.reached_at, round(r.max_noise_w, 4)) for r in failed]
    assert max(r.reached_at or 0 for r in runs) <= ORACLE_WITHIN
    assert max(r.max_noise_w for r in runs) <= NOISE_LIMIT
    assert min(r.final_oracle_w for r in runs) >= ORACLE_TARGET


def test_better_agent_gains_weight_and_fixed_share_pulls_back_toward_uniform() -> None:
    hedge = HedgeState(("good", "bad"), eta=0.5, alpha=0.1)
    hedge.advance_to(date(2026, 1, 1))
    for _ in range(20):
        hedge.update({"good": 0.3, "bad": 1.2})
    peak = hedge.weights()["good"]
    assert peak > 0.99
    hedge.advance_to(date(2026, 1, 11))
    relaxed = hedge.weights()["good"]
    assert 0.5 < relaxed < peak
    assert relaxed == pytest.approx(0.5 + (peak - 0.5) * 0.9**10, rel=1e-9)


def test_caps_hold_floor_ceiling_and_correlated_group_ceiling() -> None:
    raw = {
        "crowding": 0.70,
        "micro": 0.20,
        "technical": 0.05,
        "news": 0.03,
        "macro": 0.01,
        "fundamental": 0.01,
    }
    capped = apply_caps(
        raw,
        floor=0.05,
        ceilings=dict.fromkeys(raw, 0.40),
        groups=[("crowding", "micro")],
        group_ceiling=0.55,
    )
    assert sum(capped.values()) == pytest.approx(1.0)
    assert all(0.05 - 1e-9 <= w <= 0.40 + 1e-9 for w in capped.values())
    assert capped["crowding"] + capped["micro"] <= 0.55 + 1e-9
    assert capped["crowding"] == pytest.approx(0.40)
    order = sorted(("technical", "news", "macro", "fundamental"), key=lambda a: -capped[a])
    assert order[0] == "technical"


def test_version_cap_and_floor_bind_on_the_served_council_weights() -> None:
    council = static_config().council
    w = council.weights
    raw = {
        "crowding": 0.9,
        "technical": 0.02,
        "micro": 0.02,
        "fundamental": 0.02,
        "news": 0.02,
        "macro": 0.02,
    }
    ceilings = dict.fromkeys(raw, w.ceiling)
    ceilings["crowding"] = w.new_version_cap
    capped = apply_caps(raw, floor=w.floor, ceilings=ceilings, groups=[], group_ceiling=1.0)
    assert capped["crowding"] == pytest.approx(w.new_version_cap)
    assert min(capped.values()) >= w.floor - 1e-9


def test_regime_cell_shrinks_to_global_until_it_has_evidence() -> None:
    hh = HierarchicalHedge(("a", "b"), eta=0.5, alpha=0.0, shrink_n=50)
    day = date(2026, 1, 1)
    for _ in range(5):
        hh.update({"a": 0.2, "b": 1.0}, "trend_high_vol", day)
    for _ in range(200):
        hh.update({"a": 1.0, "b": 0.2}, "sideways_low_vol", day)
    snap = hh.snapshot()
    cells = {name: (c["log_w"], c["n"]) for name, c in snap["cells"].items()}
    thin = effective_weights(snap["global"], cells, "trend_high_vol", 50)
    thick = effective_weights(snap["global"], cells, "sideways_low_vol", 50)
    unknown = effective_weights(snap["global"], cells, None, 50)
    assert thick["b"] > 0.99
    assert thin["b"] > 0.5
    assert unknown == pytest.approx(hh.weights(None))
