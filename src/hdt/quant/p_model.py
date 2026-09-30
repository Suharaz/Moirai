"""Per-agent baseline probability (red-team #22), Design Contract section 3.

`p_model(agent, x) = clip(sigmoid(b0 + sum_k b_k * z_k), 0.02, 0.98)` with `z_k = (x_k - mean_k) / std_k`
from the agent's own `config/p_model/<agent>.<target_type>.yaml` (point-in-time standardization fixed at
fit time). Labels are never pooled across target types (Design Contract section 4): each agent has one model
per target type and a packet is always scored by the model of its candidate's target type.
Booleans count as 1 / 0; a missing, non-numeric or non-finite input contributes z = 0 (the fit-time mean),
so absent data pulls the forecast toward the base rate instead of inventing a value. News is not computed
here (phase 07 regime rules, adapted in phase 05).
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from functools import cache
from pathlib import Path

from hdt.contracts.common import AgentName, TargetType
from hdt.contracts.packet import FeatureValue
from hdt.core.config import CONFIG_DIR, PModelFile, load_p_model

P_MIN = 0.02
P_MAX = 0.98
"""Clip bounds of the Design Contract formula (not tunable thresholds)."""

MODEL_AGENTS = (
    AgentName.CROWDING,
    AgentName.TECHNICAL,
    AgentName.MICRO,
    AgentName.FUNDAMENTAL,
    AgentName.MACRO,
)


def sigmoid(z: float) -> float:
    if z >= 0:
        return 1.0 / (1.0 + math.exp(-z))
    e = math.exp(z)
    return e / (1.0 + e)


def numeric(value: FeatureValue) -> float | None:
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, int | float) and math.isfinite(value):
        return float(value)
    return None


@dataclass(frozen=True)
class PModel:
    spec: PModelFile

    @property
    def version(self) -> str:
        return self.spec.p_model_ver

    def logit(self, features: Mapping[str, FeatureValue]) -> float:
        z = self.spec.intercept
        for name in sorted(self.spec.coefficients):
            coef = self.spec.coefficients[name]
            if coef == 0.0:
                continue
            x = numeric(features.get(name))
            if x is None:
                continue
            norm = self.spec.standardization[name]
            z += coef * (x - norm.mean) / norm.std
        return z

    def predict(self, features: Mapping[str, FeatureValue]) -> float:
        return min(max(sigmoid(self.logit(features)), P_MIN), P_MAX)


@cache
def _load(agent: AgentName, target_type: TargetType, config_dir: Path) -> PModel:
    return PModel(load_p_model(agent.value, target_type.value, config_dir))


def p_model_for(agent: AgentName, target_type: TargetType, config_dir: Path = CONFIG_DIR) -> PModel:
    """The agent's active coefficients for `target_type` (process-wide cache; files are versioned)."""
    if agent not in MODEL_AGENTS:
        raise ValueError(f"no quant p_model for agent {agent.value}")
    return _load(agent, TargetType(target_type), config_dir)
