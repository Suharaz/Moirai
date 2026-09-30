"""Hedge with sleeping experts and daily fixed share (Design Contract section 4, phase 08 Red Team Delta).

- Update: `w_i <- w_i * exp(-eta * loss_i)` for every agent, then renormalized.
- Sleeping experts: an agent that abstains is assigned the pool loss, the Hedge mix loss of the awake
  agents `-1/eta * ln(sum_awake w_i exp(-eta loss_i) / sum_awake w_i)` (Freund, Schapire, Singer and
  Warmuth 1997). With that loss the total weight of the sleepers and the awake set move by the same factor,
  so a sleeper's share of the total is exactly unchanged: abstaining is neither penalized nor rewarded.
- Fixed share, applied once per UTC day (never per event): `w <- (1 - alpha) w + alpha / N`; `d` elapsed
  days apply `(1 - alpha)^d` at once, so the result does not depend on how days are batched.
- Log weights are kept (numerically stable); the floor, ceilings and group ceiling are applied only when
  weights are served (`apply_caps`), never to the learning state.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date

_EPS = 1e-12


def _logsumexp(values: Iterable[float]) -> float:
    items = list(values)
    top = max(items)
    return top + math.log(sum(math.exp(v - top) for v in items))


def normalize(weights: Mapping[str, float]) -> dict[str, float]:
    total = sum(weights.values())
    if total <= 0:
        n = len(weights)
        return dict.fromkeys(weights, 1.0 / n) if n else {}
    return {k: v / total for k, v in weights.items()}


@dataclass
class HedgeState:
    """Sleeping-experts Hedge over a fixed set of agents (uniform start)."""

    agents: tuple[str, ...]
    eta: float
    alpha: float
    log_w: dict[str, float] = field(default_factory=dict)
    day: date | None = None
    updates: int = 0

    def __post_init__(self) -> None:
        if not self.agents:
            raise ValueError("Hedge needs at least one agent")
        if self.eta <= 0 or not 0 <= self.alpha < 1:
            raise ValueError("eta must be > 0 and alpha in [0, 1)")
        if not self.log_w:
            self.log_w = dict.fromkeys(self.agents, -math.log(len(self.agents)))

    def weights(self) -> dict[str, float]:
        norm = _logsumexp(self.log_w.values())
        return {a: math.exp(self.log_w[a] - norm) for a in self.agents}

    def advance_to(self, day: date) -> None:
        """Apply the fixed share of every UTC day elapsed since the last update."""
        if self.day is None:
            self.day = day
            return
        days = (day - self.day).days
        if days <= 0:
            return
        self.day = day
        if self.alpha == 0:
            return
        keep = (1.0 - self.alpha) ** days
        n = len(self.agents)
        w = self.weights()
        self.log_w = {a: math.log(keep * w[a] + (1.0 - keep) / n) for a in self.agents}

    def pool_loss(self, losses: Mapping[str, float]) -> float:
        """The Hedge mix loss of the awake agents (the loss assigned to sleepers)."""
        awake = [a for a in self.agents if a in losses]
        if not awake:
            raise ValueError("pool loss needs at least one awake agent")
        base = _logsumexp(self.log_w[a] for a in awake)
        mixed = _logsumexp(self.log_w[a] - self.eta * losses[a] for a in awake)
        return (base - mixed) / self.eta

    def update(self, losses: Mapping[str, float]) -> float | None:
        """One event: `losses` holds the awake agents only. Returns the pool loss (None when all slept)."""
        unknown = set(losses) - set(self.agents)
        if unknown:
            raise ValueError(f"unknown agents {sorted(unknown)}")
        if not losses:
            return None
        pool = self.pool_loss(losses)
        stepped = {a: self.log_w[a] - self.eta * losses.get(a, pool) for a in self.agents}
        norm = _logsumexp(stepped.values())
        self.log_w = {a: v - norm for a, v in stepped.items()}
        self.updates += 1
        return pool


def _fill(
    base: Mapping[str, float], total: float, caps: Mapping[str, float], floor: float
) -> dict[str, float]:
    """Split `total` over `base`'s agents in proportion to `base`, none above its cap nor below
    min(floor, cap); the sum is below `total` only when the caps cannot hold it."""
    out: dict[str, float] = {}
    for _ in range(2 * len(base) + 2):
        free = [a for a in base if a not in out]
        if not free:
            break
        room = max(total - sum(out.values()), 0.0)
        mass = sum(base[a] for a in free)
        share = {a: room * (base[a] / mass if mass > _EPS else 1.0 / len(free)) for a in free}
        over = [a for a in free if share[a] > caps[a] + _EPS]
        if over:
            out.update({a: caps[a] for a in over})
            continue
        under = [a for a in free if share[a] < min(floor, caps[a]) - _EPS]
        if under:
            out.update({a: min(floor, caps[a]) for a in under})
            continue
        out.update(share)
        break
    return out


def apply_caps(
    raw: Mapping[str, float],
    *,
    floor: float,
    ceilings: Mapping[str, float],
    groups: Iterable[Iterable[str]],
    group_ceiling: float,
) -> dict[str, float]:
    """Normalize `raw` over its agents, then enforce per-agent ceilings, the correlated-group ceiling and the
    floor by water filling: an agent (or a group) at a bound is fixed there and the rest of the mass is
    redistributed over the free agents in proportion to `raw`.

    When the ceilings cannot hold a total of 1 (for example two present agents capped at 0.40), every agent
    sits at its ceiling and the weights sum to less than 1: the pool then shrinks toward the intercept
    instead of letting a capped agent carry more than its ceiling.
    """
    agents = sorted(raw)
    if not agents:
        return {}
    base = normalize({a: max(raw[a], 0.0) for a in agents})
    caps = {a: min(ceilings.get(a, 1.0), 1.0) for a in agents}
    member_groups = [tuple(m for m in g if m in base) for g in groups]
    member_groups = [g for g in member_groups if len(g) > 1]
    fixed: dict[str, float] = {}
    settled: set[int] = set()
    for _ in range(4 * len(agents) + 4):
        free = [a for a in agents if a not in fixed]
        budget = 1.0 - sum(fixed.values())
        weights = dict(fixed)
        mass = sum(base[a] for a in free)
        for a in free:
            weights[a] = budget * (base[a] / mass if mass > _EPS else 1.0 / len(free))
        if not free:
            return weights
        over = [a for a in free if weights[a] > caps[a] + _EPS]
        if over:
            for a in over:
                fixed[a] = caps[a]
            continue
        capped_group = False
        for index, group in enumerate(member_groups):
            if index in settled or sum(weights[m] for m in group) <= group_ceiling + _EPS:
                continue
            # The whole group (members already at a bound included) shares the group ceiling by water filling
            # in proportion to `raw` under the member ceilings; the group is then settled for good.
            fixed.update(_fill({m: base[m] for m in group}, group_ceiling, caps, floor))
            settled.add(index)
            capped_group = True
        if capped_group:
            continue
        under = [a for a in free if weights[a] < floor - _EPS]
        if under:
            for a in under:
                fixed[a] = min(floor, caps[a])
            continue
        return weights
    raise RuntimeError("weight caps did not converge")
