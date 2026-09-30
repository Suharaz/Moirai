"""LightGBM stacking over the agents' round-1 probabilities (phase 08, Design Contract section 4).

Target: the triple-barrier label (`scoring_labels.barrier_y`). Features: `logit(p_used)` per agent (NaN when
the agent abstained, which LightGBM routes natively) and the regime cell one-hot. Trees are at most
`max_depth` (<= 3) deep. The stacker is fitted only with at least `min_outcomes` (>= 300) independent
outcomes and is enabled only when its purged walk-forward out-of-sample log-loss beats the log pool's
(`p_pooled` of the same events, same target); otherwise the council keeps the log opinion pool.

Walk-forward: events sorted by `as_of` are cut into `folds + 1` contiguous blocks; fold k tests block k and
trains on events with `as_of + 2 x horizon <= start of block k` (purge the overlapping labels, embargo one
horizon). Training is single-threaded and seeded, so a refit on the same history gives the same model.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import lightgbm as lgb
import numpy as np

from hdt.core.config import StackingParams
from hdt.scoring.loss import DEFAULT_CLIP, clip_p, log_loss, logit
from hdt.scoring.regime_weights import REGIME_CELLS

_MIN_TRAIN = 50


def _features(agents: Sequence[str], p_by_agent: Mapping[str, float], regime: str | None) -> list[float]:
    row = [logit(clip_p(p_by_agent[a])) if a in p_by_agent else math.nan for a in agents]
    row.extend(1.0 if regime == cell else 0.0 for cell in REGIME_CELLS)
    return row


class LightGbmStacker:
    """Council `Stacker`: P(triple-barrier label = 1) from the present agents' probabilities and regime."""

    def __init__(self, booster: lgb.Booster, agents: Sequence[str]) -> None:
        self._booster = booster
        self.agents = tuple(agents)

    def predict(self, p_by_agent: Mapping[str, float], regime: str | None) -> float:
        x = np.asarray([_features(self.agents, p_by_agent, regime)], dtype=float)
        return clip_p(float(np.asarray(self._booster.predict(x))[0]))

    def to_stored(self) -> tuple[str, dict[str, Any]]:
        return self._booster.model_to_string(), {"agents": list(self.agents), "regimes": list(REGIME_CELLS)}

    @classmethod
    def from_stored(cls, model: str | None, features: Mapping[str, Any]) -> LightGbmStacker:
        if not model:
            raise ValueError("an enabled stacking model must carry its model text")
        if list(features.get("regimes", [])) != list(REGIME_CELLS):
            raise ValueError("stored stacker was trained on another regime encoding")
        return cls(lgb.Booster(model_str=model), [str(a) for a in features["agents"]])


@dataclass(frozen=True)
class StackSample:
    event_id: str
    as_of: datetime
    horizon_h: int
    p_by_agent: Mapping[str, float]
    regime: str | None
    p_pooled: float
    barrier_y: int


@dataclass(frozen=True)
class StackFit:
    trained_through: datetime
    n: int
    oos_n: int
    oos_logloss_stack: float
    oos_logloss_pool: float
    enabled: bool
    stacker: LightGbmStacker | None


def _train(agents: Sequence[str], samples: Sequence[StackSample], params: StackingParams) -> lgb.Booster:
    x = np.asarray([_features(agents, s.p_by_agent, s.regime) for s in samples], dtype=float)
    y = np.asarray([s.barrier_y for s in samples], dtype=float)
    config = {
        "objective": "binary",
        "max_depth": params.max_depth,
        "num_leaves": 2**params.max_depth,
        "learning_rate": params.learning_rate,
        "min_data_in_leaf": params.min_child_samples,
        "seed": params.seed,
        "deterministic": True,
        "force_row_wise": True,
        "num_threads": 1,
        "verbosity": -1,
    }
    return lgb.train(config, lgb.Dataset(x, label=y), num_boost_round=params.n_estimators)


def fit_stacker(
    agents: Sequence[str],
    samples: Sequence[StackSample],
    params: StackingParams,
    *,
    clip: tuple[float, float] = DEFAULT_CLIP,
) -> StackFit | None:
    """None below `min_outcomes`; otherwise the walk-forward comparison and, when it wins, the stacker."""
    ordered = sorted(samples, key=lambda s: (s.as_of, s.event_id))
    if len(ordered) < params.min_outcomes:
        return None
    blocks = np.array_split(np.arange(len(ordered)), params.folds + 1)
    stack_losses: list[float] = []
    pool_losses: list[float] = []
    for k in range(1, len(blocks)):
        test = [ordered[i] for i in blocks[k]]
        if not test:
            continue
        start = test[0].as_of
        train = [s for s in ordered if s.as_of + timedelta(hours=2 * s.horizon_h) <= start]
        if len(train) < _MIN_TRAIN or len({s.barrier_y for s in train}) < 2:
            continue
        model = LightGbmStacker(_train(agents, train, params), agents)
        for s in test:
            stack_losses.append(log_loss(model.predict(s.p_by_agent, s.regime), s.barrier_y, clip))
            pool_losses.append(log_loss(s.p_pooled, s.barrier_y, clip))
    oos_stack = float(np.mean(stack_losses)) if stack_losses else math.inf
    oos_pool = float(np.mean(pool_losses)) if pool_losses else math.inf
    wins = bool(stack_losses) and oos_stack < oos_pool
    enabled = params.enabled and wins and len({s.barrier_y for s in ordered}) == 2
    stacker = LightGbmStacker(_train(agents, ordered, params), agents) if enabled else None
    return StackFit(
        trained_through=ordered[-1].as_of,
        n=len(ordered),
        oos_n=len(stack_losses),
        oos_logloss_stack=round(oos_stack, 12) if stack_losses else 0.0,
        oos_logloss_pool=round(oos_pool, 12) if pool_losses else 0.0,
        enabled=enabled,
        stacker=stacker,
    )
