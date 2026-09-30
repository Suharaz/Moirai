"""Phase 11 ablation replay is a pure function of the stored rows: identical output on every run and for
any input order, and the replayed full pool equals the stored pooled p."""

from __future__ import annotations

import json
import random
from dataclasses import replace
from datetime import UTC, datetime, timedelta

from hdt.golive.ablation import AgentPath, CouncilRules, ReplayEvent, run_ablation, variant_probabilities
from hdt.scoring.calibration import IsotonicMap

AGENTS = ("crowding", "flow", "macro", "micro", "news", "technical")
RULES = CouncilRules(
    agents=AGENTS,
    p_clip=(0.02, 0.98),
    stance_long=0.55,
    stance_short=0.45,
    quorum=4,
    supermajority=2 / 3,
    groups={"momentum": ("technical", "micro")},
    p_long=0.60,
    p_short=0.40,
    d_threshold=0.8,
    size_consensus=1.0,
    size_fallback=0.5,
)
T0 = datetime(2026, 10, 1, tzinfo=UTC)
MAP = IsotonicMap.from_json({"x": [0.0, 0.5, 1.0], "y": [0.05, 0.45, 0.95]})


def _history(seed: int, n: int = 120) -> list[ReplayEvent]:
    rng = random.Random(seed)
    out = []
    for i in range(n):
        y = rng.randint(0, 1)
        agents = []
        for name in AGENTS:
            if rng.random() < 0.1:
                agents.append(AgentPath(name, None, None, None, None))
                continue
            model = min(0.95, max(0.05, 0.5 + (0.1 if y else -0.1) + rng.gauss(0, 0.1)))
            r1 = min(0.97, max(0.03, model + rng.gauss(0, 0.05)))
            fin = min(0.97, max(0.03, r1 + (0.05 if y else -0.05) + rng.gauss(0, 0.05)))
            agents.append(AgentPath(name, r1, fin, model, fin - 0.01))
        ev = ReplayEvent(
            event_id=f"r{i:04d}",
            coin_id=rng.randint(1, 30),
            as_of=T0 + timedelta(hours=5 * i + rng.randint(0, 4)),
            source=rng.choice(("LTX", "MIGRATION", "HOLLOW_HYPE")),
            target_type="RESID_12H" if rng.random() < 0.5 else "RAW_12H",
            label_spec_version="v1",
            y=y,
            rounds=rng.choice((1, 2, 3)),
            stop_reason=rng.choice(("max_rounds", "no_new_claims", "consensus")),
            p_pooled=None,
            disagreement=rng.uniform(0.1, 1.5),
            method="log_pool",
            params_version=3,
            weights={a: rng.uniform(0.5, 1.5) for a in AGENTS},
            r={a: rng.uniform(0.2, 0.8) for a in AGENTS},
            a=dict.fromkeys(AGENTS, 1.0),
            b=rng.uniform(-0.1, 0.1),
            maps={"flow": MAP},
            cal_clip=(0.02, 0.98),
            agents=tuple(agents),
            scan_score=rng.uniform(2.5, 9.0),
            scan_strict=rng.random() < 0.6,
            scan_side="LONG",
            label_value=rng.gauss(0, 0.02),
            beta_btc=1.0,
        )
        out.append(replace(ev, p_pooled=variant_probabilities(ev, RULES)["full"]))
    return out


def _run(events: list[ReplayEvent], target: str) -> str:
    result = run_ablation(
        events, RULES, target_type=target, strict_min={"LTX": 4.0, "MIGRATION": 2.0}, resamples=1000
    )
    return json.dumps(result.to_json(), sort_keys=True)


def test_replay_is_identical_across_runs_and_input_order() -> None:
    events = _history(11)
    shuffled = list(events)
    random.Random(5).shuffle(shuffled)
    for target in ("RAW_12H", "RESID_12H"):
        first = _run(events, target)
        assert first == _run(_history(11), target)
        assert first == _run(shuffled, target)


def test_replayed_full_pool_matches_the_stored_pool() -> None:
    events = _history(12)
    for target in ("RAW_12H", "RESID_12H"):
        assert json.loads(_run(events, target))["replay_mismatches"] == 0
    drifted = [
        replace(e, p_pooled=min(0.98, e.p_pooled + 0.01)) if e.p_pooled is not None else e for e in events
    ]
    raw = [e for e in drifted if e.target_type == "RAW_12H"]
    assert json.loads(_run(drifted, "RAW_12H"))["replay_mismatches"] == len(raw)
