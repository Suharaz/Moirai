"""Replay event builders for the phase 11 unit tests (three agents, identity calibration maps)."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, datetime, timedelta

from hdt.golive.ablation import AgentPath, CouncilRules, ReplayEvent, variant_probabilities

AGENTS = ("alpha", "beta", "gamma")
T0 = datetime(2026, 10, 1, 1, tzinfo=UTC)

RULES = CouncilRules(
    agents=AGENTS,
    p_clip=(0.02, 0.98),
    stance_long=0.55,
    stance_short=0.45,
    quorum=2,
    supermajority=2 / 3,
    groups={},
    p_long=0.60,
    p_short=0.40,
    d_threshold=0.8,
    size_consensus=1.0,
    size_fallback=0.5,
)


def agent_event(
    i: int,
    y: int,
    paths: Mapping[str, tuple[float | None, float | None, float | None]],
    *,
    as_of: datetime | None = None,
    target_type: str = "RAW_12H",
    source: str = "LTX",
    disagreement: float = 0.3,
    stop_reason: str = "max_rounds",
    r: float = 0.5,
    a: float = 1.0,
    **extra: object,
) -> ReplayEvent:
    """`paths[agent] = (p_round1, p_final, p_model)`; the stored `p_pooled` is the replayed full pool."""
    agents = tuple(AgentPath(name, r1, fin, model, model) for name, (r1, fin, model) in sorted(paths.items()))
    ev = ReplayEvent(
        event_id=f"ev{i:04d}",
        coin_id=100 + i % 7,
        as_of=as_of or T0 + timedelta(hours=12 * i),
        source=source,
        target_type=target_type,
        label_spec_version="v1",
        y=y,
        rounds=2,
        stop_reason=stop_reason,
        p_pooled=None,
        disagreement=disagreement,
        method="log_pool",
        params_version=1,
        weights=dict.fromkeys(AGENTS, 1.0),
        r=dict.fromkeys(AGENTS, r),
        a=dict.fromkeys(AGENTS, a),
        b=0.0,
        maps={},
        cal_clip=(0.02, 0.98),
        agents=agents,
        **extra,  # type: ignore[arg-type]
    )
    return replace(ev, p_pooled=variant_probabilities(ev, RULES)["full"])


def informative_history(n: int = 80, *, target_type: str = "RAW_12H") -> list[ReplayEvent]:
    """`alpha` is informative and sharpened by the debate, `beta` leans the wrong way, `gamma` is weak;
    `p_model` is 0.5 for everyone (the LLM adjustment carries the information)."""
    out = []
    for i in range(n):
        y = 1 if i % 2 == 0 else 0
        s = 1.0 if y else -1.0
        out.append(
            agent_event(
                i,
                y,
                {
                    "alpha": (0.5 + 0.15 * s, 0.5 + 0.30 * s, 0.5),
                    "beta": (0.5 - 0.05 * s, 0.5 - 0.05 * s, 0.5),
                    "gamma": (0.5 + 0.05 * s, 0.5 + 0.10 * s, 0.5),
                },
                target_type=target_type,
            )
        )
    return out
