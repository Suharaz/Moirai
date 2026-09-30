"""Phase 03 success criterion: 60 coins x 5 agents < 2 s on a dev machine (excluding Postgres writes).

Warm means a running process: the lake hours are already parsed (the `LakeView` memo of closed hours) and the
p_model files are loaded. Each timed pass still builds a new `FeatureEngine` snapshot of every coin, then the
shared candidate set and one packet per (coin, agent), exactly the `quant_core` path without the database
insert. The best of three passes is compared so a briefly busy machine does not fail the test.
"""

from __future__ import annotations

import time
from datetime import date

import pytest

from fixtures.synthetic_lake import FAR_FUTURE, MAIN_DAY, A, build_perf
from hdt.contracts.common import TargetType
from hdt.contracts.packet import QuantPacket
from hdt.core.config import CONFIG_DIR, scanner_config, static_config
from hdt.features.engine import FeatureEngine
from hdt.features.lake_io import LakeView
from hdt.features.levels import LevelRules
from hdt.lake.universe import Universe, load_universe
from hdt.quant.p_model import MODEL_AGENTS, p_model_for
from hdt.quant.packet import build_packet

TRADABLE_COINS = 60
BUDGET_S = 2.0
CONFIG_IDS = {"cmc_routes": 1, "mode": 1, "risk": 1}


def _packets(view: LakeView, universe: Universe, day: date) -> list[QuantPacket]:
    static, scanner = static_config(), scanner_config()
    rules = LevelRules(static.risk.min_rr, static.risk.max_entry_distance_atr)
    features = FeatureEngine(view, static, scanner)
    snap = features.snapshot(A, universe, static.cmc_routes)
    out = []
    for member in universe.members:
        if member.cmc_symbol == "BTC":
            continue
        candidate_set = snap.candidate_set(member, rules)
        for agent in MODEL_AGENTS:
            block = snap.agent_block(agent, member)
            model = p_model_for(agent, TargetType.RESID_12H, CONFIG_DIR)
            out.append(
                build_packet(
                    agent=agent,
                    coin_id=member.cmc_id,
                    as_of=A,
                    block=block,
                    p_model=model.predict(block.values),
                    p_model_ver=model.version,
                    candidate_set_sha256=candidate_set.candidate_set_sha256,
                    universe_date=day,
                    config_version_ids=CONFIG_IDS,
                    feature_ver=features.feature_ver,
                    target_type=TargetType.RESID_12H,
                    label_spec_version=scanner.labels.label_spec_version,
                )
            )
    return out


def test_60_coins_x_5_agents_under_2_seconds_warm(tmp_path_factory: pytest.TempPathFactory) -> None:
    lake = build_perf(tmp_path_factory.mktemp("perf_lake"), TRADABLE_COINS + 1)
    view = LakeView(lake.pit, clock=lambda: FAR_FUTURE)
    universe = load_universe(lake.pit, A)
    assert universe is not None
    cold = _packets(view, universe, MAIN_DAY)
    assert len(cold) == TRADABLE_COINS * len(MODEL_AGENTS)
    # Real packets, not empty shells: every coin has candidates and its crowding features.
    assert all(p.features["mark_price"] is not None for p in cold)
    assert all(p.features["spike_floor_usd"] is not None for p in cold if p.agent.value == "crowding")
    timings = []
    for _ in range(3):
        start = time.perf_counter()
        warm = _packets(view, universe, MAIN_DAY)
        timings.append(time.perf_counter() - start)
        assert [p.packet_sha256 for p in warm] == [p.packet_sha256 for p in cold]
    assert min(timings) < BUDGET_S, f"warm passes took {[round(t, 3) for t in timings]} s"
