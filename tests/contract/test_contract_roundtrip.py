"""Producer -> stream/Postgres encoding -> consumer parse, for every v1 contract, plus contract invariants."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest
import sqlalchemy as sa
from pydantic import BaseModel, ValidationError
from sqlalchemy import Engine

from hdt.contracts import (
    Account,
    AccountState,
    AgentForecast,
    AgentForecastDraft,
    Candidate,
    CandidateSet,
    CandidateSource,
    Claim,
    ClaimKind,
    DataQualityFlag,
    DecisionMsg,
    DirectionHint,
    Intent,
    Leg,
    LevelCandidate,
    OpenAlgoOrderState,
    OrderIntent,
    OrderSide,
    OrderType,
    PositionState,
    QuantPacket,
    RiskFlags,
    Side,
    TargetType,
    TimeInForce,
    ToolCallRecord,
)
from hdt.core.ids import b32_digest
from hdt.core.streams import encode

T0 = datetime(2026, 9, 27, 10, 0, tzinfo=UTC)


def _cid(tag: str) -> str:
    return "lc_" + b32_digest(tag, 16)


def _long() -> LevelCandidate:
    return LevelCandidate(
        candidate_id=_cid("a"),
        side=Side.LONG,
        entry=Decimal("100.0"),
        invalidation=Decimal("98"),
        tp1=Decimal("103"),
        rr=1.5,
        tick=Decimal("0.1"),
    )


def _short() -> LevelCandidate:
    return LevelCandidate(
        candidate_id=_cid("b"),
        side=Side.SHORT,
        entry=Decimal("100"),
        invalidation=Decimal("102"),
        tp1=Decimal("96.5"),
        rr=1.75,
        tick=Decimal("0.5"),
    )


def _packet() -> QuantPacket:
    cset = CandidateSet(
        coin_id=5,
        as_of=T0,
        levels_ver="l1",
        candidates=tuple(sorted((_long(), _short()), key=lambda c: c.candidate_id)),
    )
    return QuantPacket.build(
        agent="crowding",
        coin_id=5,
        as_of=T0,
        features={"spike": 4.2, "regime": "trend", "skew4": None},
        p_model=0.61,
        candidate_set_sha256=cset.candidate_set_sha256,
        data_quality={DataQualityFlag.SPIKE_FLOOR_APPLIED: ("spike",)},
        universe_date=date(2026, 9, 27),
        config_version_ids={"council": 3, "risk": 2},
        feature_ver="f1",
        p_model_ver="pm1",
        target_type=TargetType.RESID_12H,
        label_spec_version="lbl1",
    )


def _roundtrip[M: BaseModel](model: M) -> M:
    return type(model).model_validate_json(encode(model)["data"])


def _samples() -> list[BaseModel]:
    claim = Claim(
        claim_id="c1",
        kind=ClaimKind.PACKET,
        ref="spike",
        statement="SPIKE above 4",
        value=4.2,
        direction_hint=DirectionHint.UP,
    )
    return [
        Candidate(
            coin_id=5,
            as_of=T0,
            source=CandidateSource.LTX,
            score=2.5,
            rule_version="ltx1",
            target_type=TargetType.RESID_12H,
            label_spec_version="lbl1",
        ),
        _long(),
        _packet(),
        claim,
        AgentForecast(
            agent="crowding",
            agent_version="crowding@1",
            event_id="evt1",
            round=1,
            coin_id=5,
            as_of=T0,
            target_type=TargetType.RESID_12H,
            label_spec_version="lbl1",
            p_model=0.61,
            p_llm=0.66,
            p_used=0.66,
            abstain=False,
            candidate_id=_cid("a"),
            claims=(claim,),
            packet_sha256=_packet().packet_sha256,
            prompt_hash="b" * 64,
            model_slug="vendor/model-a",
        ),
        RiskFlags(
            coin_id=5,
            as_of=T0,
            veto_long=True,
            veto_short=False,
            size_mult=0.5,
            expires_at=T0 + timedelta(minutes=10),
            scan_fresh_at=T0,
        ),
        DecisionMsg(
            event_id="evt1",
            coin_id=5,
            as_of=T0,
            intent=Intent.OPEN,
            side=Side.LONG,
            p=0.66,
            p_side=0.66,
            manager_size=1.0,
            candidate_id=_cid("a"),
            packet_sha256=_packet().packet_sha256,
            config_version_ids={"council": 3},
            target_type=TargetType.RESID_12H,
            label_spec_version="lbl1",
        ),
        OrderIntent(
            intent_id="i1",
            account=Account.PAPER,
            event_id="evt1",
            leg=Leg.ENTRY,
            seq=0,
            symbol="SOLUSDT",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            qty=Decimal("1.5"),
            price=Decimal("100.0"),
            reduce_only=False,
            tif=TimeInForce.GTX,
            leverage=2,
            client_id="ABCDEFGHIJKLMNOPQRST-entry-0",
            created_at=T0,
            key_id="risk-2026-09",
            signature="sig",
        ),
        AccountState(
            account=Account.PAPER,
            equity=Decimal("10000"),
            available=Decimal("9000"),
            day_start_equity=Decimal("10050"),
            positions=(
                PositionState(
                    symbol="SOLUSDT",
                    qty=Decimal("-1.5"),
                    entry_price=Decimal("100"),
                    mark_price=Decimal("99"),
                    unrealized_pnl=Decimal("1.5"),
                    leverage=2,
                    liquidation_price=Decimal("140"),
                    margin_type="ISOLATED",
                ),
            ),
            open_orders=(),
            open_algo_orders=(
                OpenAlgoOrderState(
                    symbol="SOLUSDT",
                    client_algo_id="X-sl-0",
                    side=OrderSide.BUY,
                    order_type=OrderType.STOP_MARKET,
                    trigger_price=Decimal("102"),
                    qty=Decimal("1.5"),
                    reduce_only=True,
                    close_position=False,
                    status="NEW",
                ),
            ),
            ts=T0,
        ),
    ]


@pytest.mark.parametrize("model", _samples(), ids=lambda m: type(m).__name__)
def test_every_contract_survives_the_wire(model: BaseModel) -> None:
    assert _roundtrip(model) == model


def test_packet_tampering_is_detected_on_parse() -> None:
    packet = _packet()
    raw = packet.model_dump(mode="json")
    raw["p_model"] = 0.9
    with pytest.raises(ValidationError, match="packet_sha256"):
        QuantPacket.model_validate(raw)


def test_packet_hash_is_deterministic() -> None:
    assert _packet().packet_sha256 == _packet().packet_sha256


def test_decision_ignores_stray_numbers_and_rejects_unknown_versions() -> None:
    msg = _samples()[6]
    raw = msg.model_dump(mode="json") | {"price": "1", "qty": "999", "leverage": 20}
    parsed = DecisionMsg.model_validate(raw)
    assert not hasattr(parsed, "price")
    assert not hasattr(parsed, "leverage")
    with pytest.raises(ValidationError):
        DecisionMsg.model_validate(raw | {"schema_version": 2})


def test_open_decision_requires_candidate_and_packet() -> None:
    raw = _samples()[6].model_dump(mode="json")
    with pytest.raises(ValidationError):
        DecisionMsg.model_validate(raw | {"candidate_id": None})


def test_decision_expires_at_is_optional_and_a_v1_message_carrying_it_still_parses() -> None:
    """Owner decision 2026-09-28: the council emits no `expires_at`; a v1 message with one stays valid."""
    raw = _samples()[6].model_dump(mode="json")
    assert raw["expires_at"] is None
    old = DecisionMsg.model_validate(raw | {"expires_at": (T0 + timedelta(seconds=120)).isoformat()})
    assert old.expires_at == T0 + timedelta(seconds=120)
    with pytest.raises(ValidationError, match="expires_at"):
        DecisionMsg.model_validate(raw | {"expires_at": T0.isoformat()})


def test_llm_cannot_self_declare_code_assigned_claim_fields() -> None:
    claim = Claim.from_llm(
        {
            "claim_id": "x",
            "kind": "url",
            "ref": "item1",
            "statement": "listing",
            "verified": True,
            "hard": True,
            "tier": "T0",
            "domain_url": "binance.com",
        }
    )
    assert (claim.verified, claim.hard, claim.tier, claim.domain_url) == (False, False, None, None)


def test_abstaining_forecast_cannot_carry_a_candidate() -> None:
    with pytest.raises(ValidationError):
        AgentForecast(
            agent="news",
            agent_version="news@1",
            event_id="e",
            round=1,
            coin_id=5,
            as_of=T0,
            target_type=TargetType.RAW_12H,
            label_spec_version="lbl1",
            abstain=True,
            abstain_reason="stale",
            candidate_id=_cid("a"),
        )


def test_level_candidate_rejects_stop_on_the_wrong_side_and_off_tick() -> None:
    with pytest.raises(ValidationError, match="LONG"):
        LevelCandidate(
            candidate_id=_cid("z"),
            side=Side.LONG,
            entry=Decimal("100"),
            invalidation=Decimal("101"),
            tp1=Decimal("103"),
            rr=3.0,
            tick=Decimal("0.1"),
        )
    with pytest.raises(ValidationError, match="tick"):
        LevelCandidate(
            candidate_id=_cid("z"),
            side=Side.LONG,
            entry=Decimal("100.05"),
            invalidation=Decimal("98"),
            tp1=Decimal("103.1"),
            rr=1.5,
            tick=Decimal("0.1"),
        )


def test_order_intent_signature_covers_account() -> None:
    intent = _samples()[7]
    assert isinstance(intent, OrderIntent)
    moved = intent.model_copy(update={"account": Account.LIVE})
    assert intent.signing_bytes() != moved.signing_bytes()
    assert b"signature" not in intent.signing_bytes()


def test_llm_forecast_draft_drops_code_assigned_fields_in_nested_claims() -> None:
    draft = AgentForecastDraft.model_validate(
        {
            "p_llm": 0.7,
            "abstain": False,
            "p_used": 0.99,
            "packet_sha256": "c" * 64,
            "claims": [
                {
                    "claim_id": "c1",
                    "kind": "url",
                    "ref": "item1",
                    "statement": "exchange listing",
                    "verified": True,
                    "hard": True,
                    "tier": "T0",
                }
            ],
        }
    )
    assert "p_used" not in draft.model_dump()
    claim = Claim.from_draft(draft.claims[0])
    assert (claim.verified, claim.hard, claim.tier) == (False, False, None)


@pytest.mark.parametrize("bad", [float("nan"), 7.0, 0.0, 1.0])
def test_probabilities_are_range_checked_even_when_abstaining(bad: float) -> None:
    with pytest.raises(ValidationError):
        AgentForecast(
            agent="news",
            agent_version="news@1",
            event_id="e",
            round=1,
            coin_id=5,
            as_of=T0,
            target_type=TargetType.RAW_12H,
            label_spec_version="lbl1",
            abstain=True,
            abstain_reason="no data",
            p_model=bad,
        )


def test_ids_and_scores_are_validated() -> None:
    raw = _samples()[6].model_dump(mode="json")
    with pytest.raises(ValidationError):
        DecisionMsg.model_validate(raw | {"candidate_id": "BUY 100 BTC NOW"})
    with pytest.raises(ValidationError):
        Candidate.model_validate(_samples()[0].model_dump(mode="json") | {"score": float("nan")})


def test_level_candidate_enforces_minimum_reward_to_risk() -> None:
    with pytest.raises(ValidationError, match="rr"):
        LevelCandidate(
            candidate_id=_cid("r"),
            side=Side.LONG,
            entry=Decimal("100"),
            invalidation=Decimal("98"),
            tp1=Decimal("102.4"),
            rr=1.2,
            tick=Decimal("0.1"),
        )


def _intent(**changes: object) -> OrderIntent:
    base = _samples()[7]
    assert isinstance(base, OrderIntent)
    return OrderIntent.model_validate(base.model_dump() | changes)


def test_order_intent_leg_rules_and_leverage_ceiling() -> None:
    with pytest.raises(ValidationError):
        _intent(leverage=20)
    with pytest.raises(ValidationError, match="opens exposure"):
        _intent(reduce_only=True)
    stop = {
        "leg": Leg.SL,
        "side": OrderSide.SELL,
        "order_type": OrderType.STOP_MARKET,
        "price": None,
        "tif": None,
        "trigger_price": Decimal("95"),
    }
    with pytest.raises(ValidationError, match="must be reduce_only"):
        _intent(**stop, reduce_only=False)
    with pytest.raises(ValidationError, match="mutually exclusive"):
        _intent(**stop, qty=None, close_position=True, reduce_only=True)
    assert _intent(**stop, reduce_only=True).leg is Leg.SL


def test_order_intent_repr_never_shows_the_signature() -> None:
    intent = _intent(signature="q83vEjRWeJq83vEjRWeJq83vEjRWeJq83vEjRWeJ==")
    assert "q83vEjRWeJ" not in repr(intent)
    assert "q83vEjRWeJ" not in str(intent)


def test_packet_content_cannot_change_without_a_matching_hash() -> None:
    packet = _packet()
    with pytest.raises(TypeError):
        packet.features["spike"] = 99  # type: ignore[index]
    with pytest.raises(ValidationError, match="packet_sha256"):
        packet.model_copy(update={"p_model": 0.99})


def test_forecast_commit_bytes_ignore_tool_latency() -> None:
    forecast = _samples()[4]
    assert isinstance(forecast, AgentForecast)
    call = ToolCallRecord(tool="get_ohlcv", input_sha256="d" * 64, status="ok", latency_ms=12)
    live = forecast.model_copy(update={"tool_calls": (call,)})
    replay = forecast.model_copy(update={"tool_calls": (call.model_copy(update={"latency_ms": 900}),)})
    assert live.commit_bytes() == replay.commit_bytes()
    assert live.canonical_bytes() != replay.canonical_bytes()


@pytest.mark.pg
def test_packet_hash_survives_a_postgres_jsonb_roundtrip(pg_engine: Engine) -> None:
    packet = QuantPacket.build(
        **_packet().body().model_dump()
        | {"features": {"supply_base_units": 4.2e17, "oi_base_units": 1e22, "spike": 4.25, "flat": 2.0}}
    )
    with pg_engine.connect() as conn:
        stored = conn.execute(
            sa.text("SELECT CAST(:p AS jsonb)::text"), {"p": packet.model_dump_json()}
        ).scalar_one()
    reloaded = QuantPacket.model_validate_json(stored)
    assert reloaded.packet_sha256 == packet.packet_sha256
