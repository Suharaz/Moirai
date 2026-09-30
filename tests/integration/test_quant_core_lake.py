"""`QuantCore` end to end on the synthetic lake with Postgres (phase 03 success criteria).

Determinism across processes (two independent instances), one shared candidate set per (coin, as_of), the
full cache key (never cross-served), storage before return readable by `hdt_risk`, the universe refusals, and
p_model chosen by the pins' target type. The core writes as `hdt_council` on a migrated database.
"""

from __future__ import annotations

import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy as sa
import yaml
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import Session

from fixtures.quant_db import QuantDb
from fixtures.synthetic_lake import (
    A_BLOCK,
    BTC,
    ETH,
    FAR_FUTURE,
    MAIN_COINS,
    MAIN_DAY,
    XTRG,
    A,
    SyntheticLake,
)
from hdt.contracts.common import Account, AgentName, TargetType
from hdt.contracts.packet import QuantPacket
from hdt.core.config import CONFIG_DIR, scanner_config, static_config
from hdt.quant.cache import PacketCache
from hdt.quant.p_model import MODEL_AGENTS, P_MAX, P_MIN, p_model_for, sigmoid
from hdt.quant.packet import load_candidate_set, load_packet
from hdt.quant.quant_core import CoinNotInUniverseError, QuantCore, QuantPins, UnsupportedAgentError
from hdt.settings import store
from hdt.settings.schemas import ModeSection, Section

pytestmark = [pytest.mark.pg, pytest.mark.integration]

TRADABLE = tuple(c for c in MAIN_COINS if c.symbol != BTC.symbol)
UNKNOWN_COIN = 999_999


def _pins(db: QuantDb, target_type: TargetType = TargetType.RESID_12H, **overrides: Any) -> QuantPins:
    fields: dict[str, Any] = {
        "config_version_ids": db.version_ids,
        "universe_date": MAIN_DAY,
        "target_type": target_type,
        "label_spec_version": scanner_config().labels.label_spec_version,
    }
    fields.update(overrides)
    return QuantPins(**fields)


def _core(lake: SyntheticLake, engine: sa.Engine, **kwargs: Any) -> QuantCore:
    return QuantCore(lake.pit, engine, clock=lambda: FAR_FUTURE, **kwargs)


def _p_model_dir(tmp_path: Path, edits: dict[str, dict[str, Any]]) -> Path:
    """A config dir whose `p_model/` copies the repository files, with `edits[file stem]` applied."""
    target = tmp_path / "config"
    shutil.copytree(CONFIG_DIR / "p_model", target / "p_model")
    for stem, update in edits.items():
        path = target / "p_model" / f"{stem}.yaml"
        spec = yaml.safe_load(path.read_text(encoding="utf-8"))
        spec.update(update)
        path.write_text(yaml.safe_dump(spec, sort_keys=False), encoding="utf-8")
    return target


def _stored_packet_count(db: QuantDb, coin_ids: tuple[int, ...]) -> int:
    with db.admin.connect() as conn:
        query = sa.text("SELECT count(*) FROM quant_packets WHERE coin_id = ANY(:ids)")
        return int(conn.execute(query, {"ids": list(coin_ids)}).scalar_one())


def test_two_instances_agree_and_all_agents_share_one_candidate_set(
    main_lake: SyntheticLake, quant_db: QuantDb
) -> None:
    pins = _pins(quant_db)
    runs: list[dict[tuple[int, datetime, AgentName], QuantPacket]] = []
    for _ in range(2):  # two independent instances: own lake view, feature engine, cache and pin memo
        core = _core(main_lake, quant_db.council)
        runs.append(
            {
                (coin.cmc_id, as_of, agent): core(agent, coin.cmc_id, as_of, pins)
                for as_of in (A, A_BLOCK)
                for coin in TRADABLE
                for agent in MODEL_AGENTS
            }
        )
    first, second = runs
    assert first.keys() == second.keys()
    for key, packet in first.items():
        assert second[key].packet_sha256 == packet.packet_sha256, key
        assert second[key].candidate_set_sha256 == packet.candidate_set_sha256, key
    reference = _core(main_lake, quant_db.council)
    with quant_db.risk.connect() as conn:
        for as_of in (A, A_BLOCK):
            for coin in TRADABLE:
                shas = {first[(coin.cmc_id, as_of, agent)].candidate_set_sha256 for agent in MODEL_AGENTS}
                assert len(shas) == 1, (coin.symbol, as_of)
                (sha,) = shas
                assert reference.candidate_set(coin.cmc_id, as_of, pins).candidate_set_sha256 == sha
                stored = load_candidate_set(conn, sha)
                assert stored is not None
                assert (stored.coin_id, stored.as_of) == (coin.cmc_id, as_of)
    # Different agents see different packets (own feature family), never one packet copied to all.
    xtrg = {first[(XTRG.cmc_id, A, agent)].packet_sha256 for agent in MODEL_AGENTS}
    assert len(xtrg) == len(MODEL_AGENTS)
    assert first[(XTRG.cmc_id, A, AgentName.CROWDING)].features["spike"] is not None


def test_cache_key_changes_on_every_component_and_never_cross_serves(
    main_lake: SyntheticLake, quant_db: QuantDb, tmp_path: Path
) -> None:
    cache = PacketCache()
    pins = _pins(quant_db)
    base = _core(main_lake, quant_db.council, cache=cache)
    original = base(AgentName.CROWDING, XTRG.cmc_id, A, pins)
    assert base(AgentName.CROWDING, XTRG.cmc_id, A, pins) is original  # the shared cache is live

    other_agent = base(AgentName.TECHNICAL, XTRG.cmc_id, A, pins)
    assert other_agent.agent is AgentName.TECHNICAL

    # Same coefficients, new version string: only the key differs, so a stale hit would look right.
    new_ver = "crowding-resid_12h-pm1"
    pm_dir = _p_model_dir(tmp_path, {"crowding.resid_12h": {"p_model_ver": new_ver}})
    other_model = _core(main_lake, quant_db.council, cache=cache, p_model_dir=pm_dir)(
        AgentName.CROWDING, XTRG.cmc_id, A, pins
    )
    assert other_model.p_model_ver == new_ver
    assert other_model.p_model == original.p_model

    static = static_config()
    new_feature_ver = static.indicators.feature_ver + "-test"
    static_b = static.model_copy(
        update={"indicators": static.indicators.model_copy(update={"feature_ver": new_feature_ver})}
    )
    other_features = _core(main_lake, quant_db.council, cache=cache, static=static_b)(
        AgentName.CROWDING, XTRG.cmc_id, A, pins
    )
    assert other_features.feature_ver.startswith(new_feature_ver + ".")
    assert dict(other_features.features) == dict(original.features)

    with Session(quant_db.admin) as session, session.begin():
        active = store.get_active(session, Section.MODE)
        assert active is not None
        mode = active.model()
        assert isinstance(mode, ModeSection)
        changed = store.create_version(
            session,
            Section.MODE,
            ModeSection(mode=Account.TESTNET, size_multiplier=0.5),  # paper is pinned at 1.0
            author="test",
            reason="new config version for the cache key test",
            parent_id=active.id,
        )
    new_ids = {**quant_db.version_ids, Section.MODE.value: changed.id}
    other_config = base(AgentName.CROWDING, XTRG.cmc_id, A, _pins(quant_db, config_version_ids=new_ids))
    assert dict(other_config.config_version_ids) == new_ids

    variants = [original, other_agent, other_model, other_features, other_config]
    assert len({p.packet_sha256 for p in variants}) == len(variants)
    # Every variant is now cached side by side; each key still gets exactly its own packet.
    assert base(AgentName.CROWDING, XTRG.cmc_id, A, pins) is original
    assert base(AgentName.TECHNICAL, XTRG.cmc_id, A, pins) is other_agent
    assert base(AgentName.CROWDING, XTRG.cmc_id, A, _pins(quant_db, config_version_ids=new_ids)) is (
        other_config
    )
    assert (
        _core(main_lake, quant_db.council, cache=cache, p_model_dir=pm_dir)(
            AgentName.CROWDING, XTRG.cmc_id, A, pins
        ).packet_sha256
        == other_model.packet_sha256
    )
    assert (
        _core(main_lake, quant_db.council, cache=cache, static=static_b)(
            AgentName.CROWDING, XTRG.cmc_id, A, pins
        ).packet_sha256
        == other_features.packet_sha256
    )


def test_packet_is_stored_before_return_and_readable_by_hdt_risk(
    main_lake: SyntheticLake, quant_db: QuantDb
) -> None:
    pins = _pins(quant_db)
    packet = _core(main_lake, quant_db.council)(AgentName.MACRO, ETH.cmc_id, A, pins)
    with quant_db.risk.connect() as conn:  # a separate session: the insert is committed at return
        assert load_packet(conn, packet.packet_sha256) == packet
        candidate_set = load_candidate_set(conn, packet.candidate_set_sha256)
        assert candidate_set is not None
        assert candidate_set.coin_id == ETH.cmc_id
    # A core that cannot write never returns (nor caches) the packet it could not store.
    cache = PacketCache()
    reader_core = _core(main_lake, quant_db.risk, cache=cache)
    with pytest.raises(ProgrammingError, match="permission denied"):
        reader_core(AgentName.MICRO, ETH.cmc_id, A, pins)
    assert len(cache) == 0


def test_refuses_btc_unknown_coins_missing_universe_and_news(
    main_lake: SyntheticLake, quant_db: QuantDb
) -> None:
    core = _core(main_lake, quant_db.council)
    pins = _pins(quant_db)
    with pytest.raises(CoinNotInUniverseError, match="BTC"):
        core(AgentName.CROWDING, BTC.cmc_id, A, pins)
    with pytest.raises(CoinNotInUniverseError, match="not in the universe"):
        core(AgentName.CROWDING, UNKNOWN_COIN, A, pins)
    with pytest.raises(CoinNotInUniverseError, match="no universe"):  # built at 09:00, not yet recorded
        core(AgentName.CROWDING, XTRG.cmc_id, datetime.combine(MAIN_DAY, A.timetz()).replace(hour=8), pins)
    with pytest.raises(CoinNotInUniverseError, match="no universe"):
        core(AgentName.CROWDING, XTRG.cmc_id, A, _pins(quant_db, universe_date=MAIN_DAY - timedelta(days=1)))
    with pytest.raises(CoinNotInUniverseError):
        core.candidate_set(BTC.cmc_id, A, pins)
    with pytest.raises(UnsupportedAgentError):
        core(AgentName.NEWS, XTRG.cmc_id, A, pins)
    assert _stored_packet_count(quant_db, (BTC.cmc_id, UNKNOWN_COIN)) == 0


def test_p_model_comes_from_the_model_of_the_pinned_target_type(
    main_lake: SyntheticLake, quant_db: QuantDb, tmp_path: Path
) -> None:
    raw_spec = {"p_model_ver": "crowding-raw_12h-t1", "intercept": -0.4, "coefficients_spike": 0.3}
    resid_spec = {"p_model_ver": "crowding-resid_12h-t1", "intercept": 0.2, "coefficients_spike": -0.25}
    edits = {}
    for stem, spec in (("crowding.raw_12h", raw_spec), ("crowding.resid_12h", resid_spec)):
        base = yaml.safe_load((CONFIG_DIR / "p_model" / f"{stem}.yaml").read_text(encoding="utf-8"))
        edits[stem] = {
            "p_model_ver": spec["p_model_ver"],
            "intercept": spec["intercept"],
            "coefficients": {**base["coefficients"], "spike": spec["coefficients_spike"]},
            "shrunk_families": ["migration"],  # the ltx family (spike) is fitted
        }
    pm_dir = _p_model_dir(tmp_path, edits)
    core = _core(main_lake, quant_db.council, p_model_dir=pm_dir)
    raw = core(AgentName.CROWDING, XTRG.cmc_id, A, _pins(quant_db, TargetType.RAW_12H))
    resid = core(AgentName.CROWDING, XTRG.cmc_id, A, _pins(quant_db, TargetType.RESID_12H))
    spike = raw.features["spike"]
    assert isinstance(spike, float)
    assert spike > 0
    assert dict(raw.features) == dict(resid.features)
    assert raw.candidate_set_sha256 == resid.candidate_set_sha256
    cases = ((raw, TargetType.RAW_12H, raw_spec), (resid, TargetType.RESID_12H, resid_spec))
    for packet, target, spec in cases:
        assert packet.target_type is target
        assert packet.p_model_ver == spec["p_model_ver"]
        assert packet.p_model_ver == p_model_for(AgentName.CROWDING, target, pm_dir).version
        expected = sigmoid(spec["intercept"] + spec["coefficients_spike"] * spike)
        assert packet.p_model == pytest.approx(min(max(expected, P_MIN), P_MAX), rel=1e-12)
    assert raw.p_model != resid.p_model
    # Agents whose files are unchanged keep the repository model of each target type.
    technical = core(AgentName.TECHNICAL, XTRG.cmc_id, A, _pins(quant_db, TargetType.RAW_12H))
    assert technical.p_model_ver == p_model_for(AgentName.TECHNICAL, TargetType.RAW_12H).version
