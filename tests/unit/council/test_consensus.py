"""Consensus tables (test-strategy 3.5): quorum 4, 2/3 of w-tilde, the momentum group as one vote."""

from __future__ import annotations

import pytest

from hdt.contracts.common import Side
from hdt.council.consensus import Stance, consensus

MOMENTUM = {"momentum": ("technical", "micro")}
RULES = {"stance_long": 0.55, "stance_short": 0.45, "quorum": 4, "supermajority": 2 / 3, "groups": MOMENTUM}
ALL = ("crowding", "technical", "micro", "fundamental", "news", "macro")


def uniform(agents: tuple[str, ...] = ALL) -> dict[str, float]:
    return {a: 1.0 for a in agents}


def test_unanimous_long_reaches_consensus() -> None:
    result = consensus({a: 0.62 for a in ALL}, uniform(), **RULES)
    assert result.reached
    assert result.side is Side.LONG
    assert result.side_weight == pytest.approx(1.0)


def test_three_weak_long_vs_two_strong_short_is_no_consensus() -> None:
    """SHORT holds >= 2/3 of w~, but by agent count it is 2 SHORT votes against 2 LONG votes (the momentum
    pair votes once), so the vote check fails."""
    p = {"technical": 0.6, "micro": 0.6, "crowding": 0.6, "fundamental": 0.35, "macro": 0.35, "news": None}
    w = {"technical": 0.1, "micro": 0.1, "crowding": 0.1, "fundamental": 0.35, "macro": 0.35}
    result = consensus(p, w, **RULES)
    assert result.side is Side.SHORT
    assert result.side_weight >= 2 / 3
    assert (result.votes_for, result.votes_against) == (2, 2)
    assert not result.reached


def test_momentum_group_counts_as_one_vote() -> None:
    """Without the group the LONG side would win 3 votes to 2; grouped, it is 2 to 2."""
    p = {"technical": 0.7, "micro": 0.7, "crowding": 0.6, "fundamental": 0.4, "macro": 0.4, "news": None}
    w = {"technical": 0.3, "micro": 0.3, "crowding": 0.1, "fundamental": 0.15, "macro": 0.15}
    grouped = consensus(p, w, **RULES)
    ungrouped = consensus(p, w, **{**RULES, "groups": {}})
    assert grouped.side is Side.LONG
    assert grouped.side_weight == pytest.approx(0.7)
    assert not grouped.reached
    assert ungrouped.reached


def test_quorum_counts_only_agents_with_an_opinion() -> None:
    p = {"crowding": 0.7, "technical": 0.7, "micro": 0.7, "fundamental": None, "news": None, "macro": None}
    result = consensus(p, uniform(), **RULES)
    assert not result.reached
    assert result.side is Side.LONG


def test_side_needs_two_thirds_of_weight() -> None:
    p = {"crowding": 0.6, "technical": 0.6, "fundamental": 0.6, "news": 0.6, "macro": 0.4, "micro": 0.4}
    just_below = {"crowding": 1, "technical": 1, "fundamental": 1, "news": 0.95, "macro": 1, "micro": 1}
    at_two_thirds = {**just_below, "news": 1.0}
    assert not consensus(p, just_below, **RULES).reached
    assert consensus(p, at_two_thirds, **RULES).reached


def test_neutral_agents_dilute_the_supermajority() -> None:
    """A neutral stance (0.45 < p < 0.55) keeps its weight in w~ but supports no side."""
    p = {"crowding": 0.6, "technical": 0.6, "micro": 0.5, "fundamental": 0.6, "news": 0.5, "macro": 0.6}
    result = consensus(p, uniform(), **RULES)
    assert result.stances["micro"] is Stance.NEUTRAL
    assert result.side_weight == pytest.approx(4 / 6)
    assert result.reached


def test_tie_has_no_side() -> None:
    p = {"crowding": 0.6, "technical": 0.6, "micro": 0.4, "fundamental": 0.4, "news": 0.6, "macro": 0.4}
    result = consensus(p, uniform(), **RULES)
    assert result.side is None
    assert not result.reached
