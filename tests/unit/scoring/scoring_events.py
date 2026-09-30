"""Synthetic scored-event histories for the scoring engine tests."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta

import numpy as np
import pytest

from hdt.core.config import scoring_config, static_config
from hdt.scoring.engine import AgentRound1, EngineConfig, ScoringEvent

START = datetime(2026, 3, 1, tzinfo=UTC)
AGENTS = ("crowding", "technical", "micro", "fundamental", "news", "macro")
SKILL = {"crowding": 0.72, "technical": 0.55, "micro": 0.6, "fundamental": 0.5, "news": 0.52, "macro": 0.5}

EventFactory = Callable[..., list[ScoringEvent]]


@pytest.fixture
def engine_cfg() -> EngineConfig:
    return EngineConfig.from_config(static_config().council, scoring_config())


def _events(
    n: int,
    *,
    seed: int = 1,
    target_type: str = "RESID_12H",
    abstain: Sequence[str] = (),
    abstain_rate: float = 0.0,
    versions: dict[str, Callable[[int], int]] | None = None,
    start: datetime = START,
    prefix: str = "ev",
) -> list[ScoringEvent]:
    rng = np.random.default_rng(seed)
    out: list[ScoringEvent] = []
    for i in range(n):
        y = int(rng.random() < 0.5)
        agents: list[AgentRound1] = []
        for agent in AGENTS:
            right = rng.random() < SKILL[agent]
            p = float(np.clip(rng.normal(0.66, 0.05), 0.51, 0.9))
            p_used = p if right == bool(y) else 1.0 - p
            p_model = float(np.clip(0.5 + (p_used - 0.5) * 0.5, 0.05, 0.95))
            sleeps = agent in abstain or rng.random() < abstain_rate
            version = versions[agent](i) if versions and agent in versions else 1
            agents.append(
                AgentRound1(
                    agent=agent,
                    agent_version=version,
                    p_used=None if sleeps else round(p_used, 6),
                    p_model=round(p_model, 6),
                    p_final=None if sleeps else round(0.5 + (p_used - 0.5) * 0.8, 6),
                )
            )
        out.append(
            ScoringEvent(
                event_id=f"{prefix}-{i:05d}",
                coin_id=100 + i % 7,
                as_of=start + timedelta(hours=3 * i),
                horizon_h=12,
                target_type=target_type,
                label_spec_version="lbl1",
                y=y,
                regime=("trend_high_vol", "sideways_low_vol")[i % 2],
                p_pooled=round(float(rng.uniform(0.3, 0.7)), 6),
                debated=i % 3 == 0,
                agents=tuple(agents),
                barrier_y=y,
            )
        )
    return out


@pytest.fixture
def make_events() -> EventFactory:
    return _events
