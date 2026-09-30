"""Aggregate (test-strategy 3.5): log opinion pool with r-mixing, clip [0.02, 0.98], abstainers excluded."""

from __future__ import annotations

import math

import pytest

from hdt.council.aggregate import aggregate, disagreement, logit, sigmoid

CLIP = (0.02, 0.98)


def pool(round1: dict[str, float], final: dict[str, float], weights: dict[str, float], **kw: object):  # type: ignore[no-untyped-def]
    args = {"r": lambda _a: 0.5, "calibrate": lambda _a, p: p, "intercept_b": 0.0, "p_clip": CLIP} | kw
    return aggregate(round1, final, weights=weights, **args)  # type: ignore[arg-type]


def test_log_pool_matches_the_formula() -> None:
    round1 = {"a": 0.6, "b": 0.7, "c": 0.4}
    final = {"a": 0.65, "b": 0.7, "c": 0.45}
    weights = {"a": 2.0, "b": 1.0, "c": 1.0}
    result = pool(round1, final, weights, intercept_b=0.1)
    assert result is not None
    w = {"a": 0.5, "b": 0.25, "c": 0.25}
    mixed = {k: sigmoid(logit(round1[k]) + 0.5 * (logit(final[k]) - logit(round1[k]))) for k in round1}
    expected = sigmoid(0.1 + sum(w[k] * logit(mixed[k]) for k in w))
    assert result.p == pytest.approx(expected)
    assert result.method == "log_pool"


@pytest.mark.parametrize(("r", "expected"), [(0.0, 0.6), (1.0, 0.7)])
def test_r_mixing_moves_between_round_one_and_final(r: float, expected: float) -> None:
    result = pool({"a": 0.6}, {"a": 0.7}, {"a": 1.0}, r=lambda _a: r)
    assert result is not None
    assert result.p == pytest.approx(expected)


def test_pooled_probability_is_clipped() -> None:
    result = pool({"a": 0.999, "b": 0.999}, {"a": 0.999, "b": 0.999}, {"a": 1, "b": 1})
    assert result is not None
    assert result.p == pytest.approx(0.98)
    assert result.p_unclipped > 0.98
    low = pool({"a": 0.001}, {"a": 0.001}, {"a": 1})
    assert low is not None
    assert low.p == pytest.approx(0.02)


def test_abstainers_are_excluded_and_weights_renormalized() -> None:
    """An agent missing from the final round (it abstained) takes no part; w~ is renormalized."""
    with_abstainer = pool({"a": 0.7, "b": 0.3}, {"a": 0.7}, {"a": 1.0, "b": 5.0})
    alone = pool({"a": 0.7}, {"a": 0.7}, {"a": 1.0})
    assert with_abstainer is not None
    assert alone is not None
    assert with_abstainer.p == pytest.approx(alone.p)
    assert dict(with_abstainer.weights) == {"a": 1.0}


def test_nobody_with_an_opinion_gives_no_pool() -> None:
    assert pool({}, {}, {}) is None


def test_disagreement_value_range() -> None:
    """D is the w~-weighted std of the logits: 0 when unanimous, half the spread for two equal camps,
    and at most half the logit width of the clip range for any probabilities inside it."""
    assert disagreement({"a": logit(0.6), "b": logit(0.6)}, {"a": 1, "b": 1}) == pytest.approx(0.0)
    two_camps = disagreement({"a": logit(0.7), "b": logit(0.3)}, {"a": 1, "b": 1})
    assert two_camps == pytest.approx(logit(0.7))
    widest = disagreement({"a": logit(0.98), "b": logit(0.02)}, {"a": 1, "b": 1})
    assert widest == pytest.approx(logit(0.98))
    many = disagreement(
        {k: logit(p) for k, p in zip("abcdef", (0.98, 0.02, 0.5, 0.7, 0.3, 0.9), strict=True)}, {}
    )
    assert 0.0 <= many <= logit(0.98)


def test_default_disagreement_threshold_separates_mild_from_split_councils() -> None:
    """With the council default d_threshold 0.8: a 65/35 split (D 0.62) passes, a 70/30 split (D 0.85)
    does not."""
    mild = disagreement({"a": logit(0.65), "b": logit(0.35)}, {"a": 1, "b": 1})
    split = disagreement({"a": logit(0.7), "b": logit(0.3)}, {"a": 1, "b": 1})
    assert mild < 0.8 < split
    assert math.isclose(mild, logit(0.65))
