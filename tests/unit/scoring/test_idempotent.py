"""Rescoring is a pure function of the labeled history: deterministic, order-free, online, per target type."""

from __future__ import annotations

import math
import random
from dataclasses import replace
from datetime import timedelta

import pytest
from tests.unit.scoring.scoring_events import AGENTS, EventFactory

from hdt.core.config import static_config
from hdt.scoring.coverage import FlagWindow
from hdt.scoring.engine import EngineConfig, rescore
from hdt.scoring.llm_trust import update_a
from hdt.scoring.params import PoolParamsView


def test_rescoring_twice_or_shuffled_gives_identical_rows_and_params(
    make_events: EventFactory, engine_cfg: EngineConfig
) -> None:
    events = make_events(200)
    first = rescore(events, engine_cfg)
    shuffled = list(events)
    random.Random(3).shuffle(shuffled)
    second = rescore(shuffled, engine_cfg)
    assert first.scored == second.scored
    assert first.weights == second.weights
    assert {k: v.payload_sha256 for k, v in first.keys.items()} == {
        k: v.payload_sha256 for k, v in second.keys.items()
    }


def test_target_types_never_share_weights(make_events: EventFactory, engine_cfg: EngineConfig) -> None:
    resid = make_events(150, seed=2)
    raw = make_events(150, seed=9, target_type="RAW_12H", prefix="raw")
    alone = rescore(resid, engine_cfg)
    mixed = rescore(resid + raw, engine_cfg)
    key = ("RESID_12H", "lbl1")
    assert mixed.keys[key].payload_sha256 == alone.keys[key].payload_sha256
    assert set(mixed.keys) == {key, ("RAW_12H", "lbl1")}
    assert [w for w in mixed.weights if w.target_type == "RESID_12H"] == list(alone.weights)


def test_past_weights_do_not_change_when_later_labels_arrive(
    make_events: EventFactory, engine_cfg: EngineConfig
) -> None:
    events = make_events(240)
    early = rescore(events[:120], engine_cfg)
    later = rescore(events, engine_cfg)
    cutoff = events[119].as_of
    assert [w for w in later.weights if w.as_of <= cutoff] == list(early.weights)
    assert [s for s in later.scored if s.as_of <= cutoff] == list(early.scored)


def test_pooled_row_and_log_loss_are_recorded_per_event(
    make_events: EventFactory, engine_cfg: EngineConfig
) -> None:
    events = make_events(20)
    result = rescore(events, engine_cfg)
    pooled = [s for s in result.scored if s.agent == "pooled"]
    assert len(pooled) == 20
    assert all(s.scored_at == s.as_of + timedelta(hours=12) for s in pooled)
    worst = max(result.scored, key=lambda s: s.log_loss)
    assert worst.hit is False
    assert worst.log_loss <= -math.log(engine_cfg.clip[0]) + 1e-9


def test_version_bump_resets_trust_and_caps_the_new_version(
    make_events: EventFactory, engine_cfg: EngineConfig
) -> None:
    bump_at = 150
    events = make_events(200, versions={"crowding": lambda i: 1 if i < bump_at else 2})
    result = rescore(events, engine_cfg)
    rows = [w for w in result.weights if w.agent == "crowding"]
    before = next(w for w in rows if w.as_of == events[bump_at - 1].as_of)
    after = next(w for w in rows if w.as_of == events[bump_at].as_of)
    assert before.agent_version == 1
    assert after.agent_version == 2
    assert after.forecasts == 1
    assert before.w_capped > engine_cfg.new_version_cap
    assert after.w_capped == pytest.approx(engine_cfg.new_version_cap)
    item = next(i for i in events[bump_at].agents if i.agent == "crowding")
    assert item.p_used is not None
    assert item.p_model is not None
    reset_then_updated = update_a(
        engine_cfg.a_initial,
        p_model=item.p_model,
        p_used=item.p_used,
        y=events[bump_at].y,
        kappa=engine_cfg.kappa_a,
        clip=engine_cfg.clip,
    )
    assert after.a == pytest.approx(reset_then_updated, abs=1e-11)
    assert before.a != pytest.approx(engine_cfg.a_initial, abs=1e-3)


def test_claims_flag_lowers_the_ceiling_only_while_open(
    make_events: EventFactory, engine_cfg: EngineConfig
) -> None:
    events = make_events(200)
    flag_start, flag_end = events[100].as_of, events[150].as_of
    result = rescore(events, engine_cfg, [FlagWindow("crowding", flag_start, flag_end)])
    rows = {w.as_of: w for w in result.weights if w.agent == "crowding"}
    flagged = [w.w_capped for t, w in rows.items() if flag_start <= t < flag_end]
    reviewed = [w.w_capped for t, w in rows.items() if t >= flag_end]
    assert max(flagged) <= engine_cfg.flagged_ceiling + 1e-9
    assert max(reviewed) > engine_cfg.flagged_ceiling


def test_served_params_reproduce_the_scored_weights(
    make_events: EventFactory, engine_cfg: EngineConfig
) -> None:
    events = make_events(200)
    result = rescore(events, engine_cfg)
    key = result.keys[("RESID_12H", "lbl1")]
    view = PoolParamsView.from_payload(1, key.payload, static_config().council, None)
    served = view.weights("trend_high_vol", AGENTS)
    assert sum(served.values()) == pytest.approx(1.0)
    assert served["crowding"] == max(served.values())
    assert view.a("crowding") == key.payload["a"]["crowding"]
    first = replace(events[0], event_id="zzz")
    assert rescore([first], engine_cfg).keys[("RESID_12H", "lbl1")].payload["events"] == 1
