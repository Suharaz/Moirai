"""p_model defaults, packet determinism and the full-key packet cache."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from hdt.contracts.common import AgentName, DataQualityFlag, TargetType
from hdt.features.common import FeatureBlock
from hdt.quant.cache import CacheKey, PacketCache, config_hash
from hdt.quant.labels import Label, simple_return
from hdt.quant.p_model import MODEL_AGENTS, P_MAX, P_MIN, p_model_for
from hdt.quant.packet import build_packet

AS_OF = datetime(2026, 9, 1, 12, 5, 30, tzinfo=UTC)
SHA = "a" * 64


def _packet(agent: AgentName, features: dict[str, float | None], config_ids: dict[str, int] | None = None):  # type: ignore[no-untyped-def]
    block = FeatureBlock()
    block.update(features)
    if features.get("atr_1h") is None:
        block.flag(DataQualityFlag.MISSING_ROUTE, "atr_1h")
    model = p_model_for(agent, TargetType.RESID_12H)
    return build_packet(
        agent=agent,
        coin_id=1027,
        as_of=AS_OF,
        block=block,
        p_model=model.predict(block.values),
        p_model_ver=model.version,
        candidate_set_sha256=SHA,
        universe_date=date(2026, 9, 1),
        config_version_ids=config_ids or {"risk": 3, "cmc": 2},
        feature_ver="f1.s1",
        target_type=TargetType.RESID_12H,
        label_spec_version="lbl1",
    )


@pytest.mark.parametrize("agent", sorted(MODEL_AGENTS, key=lambda a: a.value))
def test_unfitted_models_return_the_uninformed_rate_and_stay_in_bounds(agent: AgentName) -> None:
    for target in TargetType:
        model = p_model_for(agent, target)
        assert model.spec.target_type == target.value
        assert model.predict({}) == pytest.approx(0.5)
        assert P_MIN <= model.predict({"atr_1h": 1e9}) <= P_MAX
    assert p_model_for(agent, TargetType.RAW_12H).version != p_model_for(agent, TargetType.RESID_12H).version


def test_packet_hash_is_deterministic_and_content_sensitive() -> None:
    first = _packet(AgentName.CROWDING, {"atr_1h": 2.5, "mark_price": 100.0})
    again = _packet(AgentName.CROWDING, {"mark_price": 100.0, "atr_1h": 2.5})
    changed = _packet(AgentName.CROWDING, {"atr_1h": 2.5000001, "mark_price": 100.0})
    assert first.packet_sha256 == again.packet_sha256
    assert first.packet_sha256 != changed.packet_sha256
    missing = _packet(AgentName.CROWDING, {"atr_1h": None})
    assert missing.data_quality[DataQualityFlag.MISSING_ROUTE] == ("atr_1h",)


def test_cache_never_serves_another_agent_or_config() -> None:
    cache = PacketCache()
    crowding = _packet(AgentName.CROWDING, {"atr_1h": 2.5})
    key = cache.put(crowding)
    assert cache.get(key) is crowding
    other_agent = CacheKey(AgentName.TECHNICAL, *list(key.__dict__.values())[1:])
    assert cache.get(other_agent) is None
    repinned = _packet(AgentName.CROWDING, {"atr_1h": 2.5}, {"risk": 4, "cmc": 2})
    assert CacheKey.of(repinned) != key
    assert config_hash({"risk": 3}, date(2026, 9, 1)) != config_hash({"risk": 3}, date(2026, 9, 2))


def test_labels_sign_and_missing_marks() -> None:
    assert simple_return(100.0, 110.0) == pytest.approx(0.1)
    assert simple_return(None, 110.0) is None
    resid = Label(TargetType.RESID_12H, 12, 0.05, 0.04, 1.5, 0.05 - 1.5 * 0.04)
    assert resid.y == 0
    assert resid.signed("SHORT") == pytest.approx(0.01)
    assert Label(TargetType.RAW_12H, 12, None, None, None, None).y is None
