"""Hierarchical weights: a global Hedge plus one Hedge per regime cell, shrunk toward the global table.

Each regime cell (`trend_high_vol`, `trend_low_vol`, `sideways_high_vol`, `sideways_low_vol`, from the
macro packet feature `regime`) runs its own sleeping-experts Hedge on the events of that cell only. The
served log weight of agent `i` in cell `c` is

    log w_i = log w_global_i + lambda_c * (log w_cell_i - log(1 / N)),   lambda_c = n_c / (n_c + k)

so a cell with few scored events (`n_c` small) is the global table, and a well-sampled cell tilts it by
what the cell learned relative to its uniform start. `k` is `scoring.yaml` `hedge.regime_shrink_n`.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Final

from hdt.contracts.packet import QuantPacket
from hdt.scoring.hedge import HedgeState, apply_caps, normalize

REGIME_CELLS: Final[tuple[str, ...]] = (
    "trend_high_vol",
    "trend_low_vol",
    "sideways_high_vol",
    "sideways_low_vol",
)
REGIME_FEATURE: Final[str] = "regime"


def regime_of(packets: Mapping[str, QuantPacket]) -> str | None:
    """The regime cell of an event from its committed packets (macro first); None when unknown."""
    ordered = sorted(packets.items(), key=lambda item: (item[0] != "macro", item[0]))
    for _, packet in ordered:
        value = packet.features.get(REGIME_FEATURE)
        if isinstance(value, str) and value in REGIME_CELLS:
            return value
    return None


@dataclass
class HierarchicalHedge:
    agents: tuple[str, ...]
    eta: float
    alpha: float
    shrink_n: float
    global_state: HedgeState = field(init=False)
    cells: dict[str, HedgeState] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.global_state = HedgeState(self.agents, self.eta, self.alpha)

    def advance_to(self, day: date) -> None:
        self.global_state.advance_to(day)
        for state in self.cells.values():
            state.advance_to(day)

    def update(self, losses: Mapping[str, float], regime: str | None, day: date) -> float | None:
        self.advance_to(day)
        pool = self.global_state.update(losses)
        if regime is not None and losses:
            cell = self.cells.get(regime)
            if cell is None:
                cell = HedgeState(self.agents, self.eta, self.alpha)
                cell.advance_to(day)
                self.cells[regime] = cell
            cell.update(losses)
        return pool

    def weights(self, regime: str | None = None) -> dict[str, float]:
        """Normalized weights over every agent for `regime` (the global table when None or unseen)."""
        return effective_weights(
            self.global_state.log_w,
            {name: (state.log_w, state.updates) for name, state in self.cells.items()},
            regime,
            self.shrink_n,
        )

    def snapshot(self) -> dict[str, Any]:
        return {
            "global": dict(sorted(self.global_state.log_w.items())),
            "cells": {
                name: {"log_w": dict(sorted(state.log_w.items())), "n": state.updates}
                for name, state in sorted(self.cells.items())
            },
            "shrink_n": self.shrink_n,
        }


def effective_weights(
    global_log_w: Mapping[str, float],
    cells: Mapping[str, tuple[Mapping[str, float], int]],
    regime: str | None,
    shrink_n: float,
    present: Iterable[str] | None = None,
) -> dict[str, float]:
    """Hierarchical weights restricted to `present` (every agent when None), normalized to 1."""
    agents = list(global_log_w) if present is None else [a for a in present if a in global_log_w]
    if not agents:
        return {}
    n_agents = len(global_log_w)
    log_w = {a: global_log_w[a] for a in agents}
    cell = cells.get(regime) if regime is not None else None
    if cell is not None:
        cell_log_w, n = cell
        lam = n / (n + shrink_n)
        uniform = -math.log(n_agents)
        for a in agents:
            log_w[a] += lam * (cell_log_w.get(a, uniform) - uniform)
    top = max(log_w.values())
    return normalize({a: math.exp(v - top) for a, v in log_w.items()})


def served_weights(
    global_log_w: Mapping[str, float],
    cells: Mapping[str, tuple[Mapping[str, float], int]],
    *,
    regime: str | None,
    shrink_n: float,
    present: Iterable[str],
    floor: float,
    ceilings: Mapping[str, float],
    groups: Iterable[Iterable[str]],
    group_ceiling: float,
) -> dict[str, float]:
    """The pooling weights `w~` of the present (non-abstaining) agents: hierarchical weights restricted to
    them and renormalized, then the floor, the per-agent ceilings and the correlated-group ceiling."""
    raw = effective_weights(global_log_w, cells, regime, shrink_n, present)
    return apply_caps(raw, floor=floor, ceilings=ceilings, groups=groups, group_ceiling=group_ceiling)
