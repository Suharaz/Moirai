"""Synthetic oracle test that chooses Hedge `eta` and the daily fixed share `alpha` (phase 08 Red Team Delta).

Six agents forecast a fair coin (`y ~ Bernoulli(0.5)`), `events_per_day` independent forecasts per UTC day:
- the oracle is informative and calibrated: it calls the right direction with probability
  `oracle_accuracy` and always says `p = oracle_accuracy` toward its call;
- five noise agents say `p ~ Uniform(0.5 - noise_spread, 0.5 + noise_spread)`, independent of `y`;
- every agent abstains on each event with probability `abstain_rate` (sleeping experts).

Criterion (Design Contract section 4): the oracle's global Hedge weight reaches >= 0.30 within <= 150
independent forecasts, and no noise agent ever exceeds 1/6 + 0.03, on every seed. `python -m
hdt.scoring.oracle` prints the grid; the chosen pair lives in `config/scoring.yaml` (`hedge`) and
`tests/unit/scoring/test_hedge.py` re-checks it.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Final

import numpy as np

from hdt.scoring.hedge import HedgeState
from hdt.scoring.loss import log_loss

AGENTS: Final[tuple[str, ...]] = ("oracle", "noise_1", "noise_2", "noise_3", "noise_4", "noise_5")
ORACLE_TARGET: Final[float] = 0.30
ORACLE_WITHIN: Final[int] = 150
NOISE_LIMIT: Final[float] = 1 / 6 + 0.03


@dataclass(frozen=True)
class OracleSetup:
    oracle_accuracy: float = 0.75
    noise_spread: float = 0.05
    abstain_rate: float = 0.10
    events_per_day: int = 8
    events: int = 400


DEFAULT_SETUP = OracleSetup()


@dataclass(frozen=True)
class OracleRun:
    seed: int
    reached_at: int | None
    """1-based count of scored forecasts when the oracle first had w >= 0.30 (None: never)."""
    max_noise_w: float
    final_oracle_w: float

    @property
    def passed(self) -> bool:
        return (
            self.reached_at is not None
            and self.reached_at <= ORACLE_WITHIN
            and self.max_noise_w <= NOISE_LIMIT
        )


def simulate(eta: float, alpha: float, seed: int, setup: OracleSetup = DEFAULT_SETUP) -> OracleRun:
    rng = np.random.default_rng(seed)
    hedge = HedgeState(AGENTS, eta, alpha)
    start = date(2026, 1, 1)
    reached: int | None = None
    max_noise = 0.0
    for n in range(1, setup.events + 1):
        y = int(rng.random() < 0.5)
        right = rng.random() < setup.oracle_accuracy
        call = y if right else 1 - y
        forecasts = {"oracle": setup.oracle_accuracy if call == 1 else 1 - setup.oracle_accuracy}
        for agent in AGENTS[1:]:
            forecasts[agent] = float(0.5 + rng.uniform(-setup.noise_spread, setup.noise_spread))
        awake = {a: log_loss(p, y) for a, p in forecasts.items() if rng.random() >= setup.abstain_rate}
        hedge.advance_to(start + timedelta(days=(n - 1) // setup.events_per_day))
        hedge.update(awake)
        weights = hedge.weights()
        if reached is None and weights["oracle"] >= ORACLE_TARGET:
            reached = n
        max_noise = max(max_noise, *(weights[a] for a in AGENTS[1:]))
    return OracleRun(seed, reached, max_noise, hedge.weights()["oracle"])


def evaluate(eta: float, alpha: float, seeds: range, setup: OracleSetup = DEFAULT_SETUP) -> list[OracleRun]:
    return [simulate(eta, alpha, seed, setup) for seed in seeds]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--seeds", type=int, default=100)
    args = parser.parse_args()
    seeds = range(args.seeds)
    print("eta    alpha   pass  worst_reached  worst_noise")
    for eta in (0.05, 0.08, 0.10, 0.12, 0.13, 0.15, 0.18, 0.20):
        for alpha in (0.0, 0.005, 0.01, 0.02, 0.03):
            runs = evaluate(eta, alpha, seeds)
            reached = [r.reached_at if r.reached_at is not None else 10**6 for r in runs]
            print(
                f"{eta:<6} {alpha:<7} {sum(r.passed for r in runs):>2}/{len(runs)}"
                f"  {max(reached):>13}  {max(r.max_noise_w for r in runs):.4f}"
            )


if __name__ == "__main__":
    main()
