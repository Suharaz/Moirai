"""Calibration (isotonic, intercept, reliability) and the stacker's out-of-sample gate."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import numpy as np
import pytest

from hdt.core.config import scoring_config
from hdt.scoring.calibration import IsotonicMap, fit_intercept, fit_isotonic, in_window, reliability
from hdt.scoring.loss import logit
from hdt.scoring.stacking import LightGbmStacker, StackSample, fit_stacker

CLIP = (0.02, 0.98)
START = datetime(2026, 1, 1, tzinfo=UTC)


def test_isotonic_map_corrects_an_overconfident_agent_and_round_trips() -> None:
    rng = np.random.default_rng(0)
    p = rng.uniform(0.05, 0.95, 4000)
    truth = 0.5 + (p - 0.5) * 0.4
    y = (rng.random(4000) < truth).astype(int)
    fitted = fit_isotonic(list(p), list(y), min_samples=100, clip=CLIP)
    assert fitted(0.9, CLIP) == pytest.approx(0.66, abs=0.06)
    assert fitted(0.1, CLIP) == pytest.approx(0.34, abs=0.06)
    assert IsotonicMap.from_json(fitted.to_json()) == fitted
    assert fit_isotonic(list(p[:50]), list(y[:50]), min_samples=100, clip=CLIP)(0.9, CLIP) == 0.9


def test_window_excludes_labels_inside_the_purge_and_embargo() -> None:
    now = START + timedelta(days=100)
    horizon, window = timedelta(hours=12), timedelta(days=90)
    assert in_window(now - timedelta(hours=24), now, horizon, window)
    assert not in_window(now - timedelta(hours=23), now, horizon, window)
    assert not in_window(now - timedelta(days=91), now, horizon, window)


def test_intercept_recovers_a_bias_within_its_bound() -> None:
    rng = np.random.default_rng(1)
    z = rng.normal(0.0, 1.0, 3000)
    y = (rng.random(3000) < 1 / (1 + np.exp(-(z + 0.4)))).astype(int)
    assert fit_intercept(list(z), list(y), bound=1.0, min_samples=100, clip=CLIP) == pytest.approx(
        0.4, abs=0.1
    )
    y_high = np.ones(3000, dtype=int)
    assert fit_intercept(list(z), list(y_high), bound=1.0, min_samples=100, clip=CLIP) == pytest.approx(
        1.0, abs=1e-3
    )
    assert fit_intercept(list(z[:10]), list(y[:10]), bound=1.0, min_samples=100, clip=CLIP) == 0.0


def test_reliability_bins_ece_and_spiegelhalter() -> None:
    rng = np.random.default_rng(2)
    p = rng.uniform(0.0, 1.0, 5000)
    calibrated = reliability(list(p), list((rng.random(5000) < p).astype(int)), 10)
    biased = reliability(list(p), list((rng.random(5000) < p * 0.6).astype(int)), 10)
    assert calibrated.n == 5000
    assert sum(b.n for b in calibrated.bins) == 5000
    assert calibrated.ece < 0.03
    assert calibrated.spiegelhalter_z is not None
    assert abs(calibrated.spiegelhalter_z) < 3
    assert biased.ece > 0.1


def _samples(n: int, *, informative: bool, seed: int = 3) -> list[StackSample]:
    rng = np.random.default_rng(seed)
    out: list[StackSample] = []
    for i in range(n):
        regime = ("trend_high_vol", "sideways_low_vol")[i % 2]
        if informative:
            # The crowding signal is only right in trending regimes: a pool of logits cannot express that.
            y = int(rng.random() < 0.5)
            crowding = (0.8 if y else 0.2) if regime == "trend_high_vol" else float(rng.choice([0.2, 0.8]))
            p = {"crowding": crowding, "technical": float(rng.uniform(0.4, 0.6))}
            pooled = 1 / (1 + np.exp(-(0.5 * logit(crowding) + 0.5 * logit(p["technical"]))))
        else:
            # The pool already carries the calibrated truth; the agents' inputs are noise.
            pooled = float(rng.uniform(0.15, 0.85))
            y = int(rng.random() < pooled)
            p = {"crowding": float(rng.uniform(0.2, 0.8)), "technical": float(rng.uniform(0.2, 0.8))}
        out.append(
            StackSample(
                event_id=f"s{i:05d}",
                as_of=START + timedelta(hours=12 * i),
                horizon_h=12,
                p_by_agent=p,
                regime=regime,
                p_pooled=float(pooled),
                barrier_y=y,
            )
        )
    return out


def test_stacker_is_enabled_only_when_it_beats_the_pool_out_of_sample() -> None:
    params = scoring_config().stacking
    assert (
        fit_stacker(("crowding", "technical"), _samples(params.min_outcomes - 1, informative=True), params)
        is None
    )
    good = fit_stacker(("crowding", "technical"), _samples(600, informative=True), params)
    assert good is not None
    assert good.enabled
    assert good.oos_logloss_stack < good.oos_logloss_pool
    assert good.stacker is not None
    stored_model, features = good.stacker.to_stored()
    reloaded = LightGbmStacker.from_stored(stored_model, features)
    probe = {"crowding": 0.8, "technical": 0.5}
    assert reloaded.predict(probe, "trend_high_vol") == pytest.approx(
        good.stacker.predict(probe, "trend_high_vol")
    )
    noise = fit_stacker(("crowding", "technical"), _samples(600, informative=False), params)
    assert noise is not None
    assert not noise.enabled
    assert noise.stacker is None
