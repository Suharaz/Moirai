"""The Risk gate (phase 09 section 1): no decision expiry, packet integrity, candidate, sign, intent by
position, BTC, market re-check, ceilings and the signed leg plan.

Success criterion (gate part): a decision with a wrong packet hash yields 0 orders; a held coin receiving a
same-direction decision yields `HOLD` and 0 new orders; an OPEN decided long after its candidate is judged
only by the current mark against its levels (owner decision 2026-09-28).
"""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

import pytest

from hdt.contracts.common import Account, Intent, Leg, OrderSide, OrderType, Side, TimeInForce
from hdt.execution.client_ids import client_id
from hdt.risk.gate import GateInputs, HeldPosition, evaluate, intent_id_for
from hdt.risk.limits import Exposure
from risk_builders import (
    CID_LONG,
    CID_LONG_FAR,
    CID_SHORT,
    COIN_ID,
    NOW,
    SYMBOL,
    book,
    candidate,
    candidate_set,
    decision,
    gate_config,
    gate_inputs,
    market,
    packet,
    risk_file,
    symbol_filters,
    with_decision,
)

HELD_LONG = {SYMBOL: HeldPosition(Side.LONG, Decimal("9"))}


def test_approved_open_publishes_the_stop_plan_before_the_entry() -> None:
    res = evaluate(gate_inputs(), gate_config(), NOW)

    assert res.verdict == "approved"
    assert res.effective_intent is Intent.OPEN
    assert [i.leg for i in res.intents] == [Leg.SL, Leg.TP1, Leg.TRAIL, Leg.ENTRY_IOC, Leg.ENTRY]
    by_leg = {i.leg: i for i in res.intents}
    sl, tp1, trail = by_leg[Leg.SL], by_leg[Leg.TP1], by_leg[Leg.TRAIL]
    ioc, entry = by_leg[Leg.ENTRY_IOC], by_leg[Leg.ENTRY]
    # size 9.000 at 3x (see risk_builders), prices from the candidate only
    assert entry.qty == Decimal("9.000")
    assert entry.price == Decimal("100.00")
    assert entry.tif is TimeInForce.GTX
    assert entry.leverage == 3
    assert entry.side is OrderSide.BUY
    assert ioc.tif is TimeInForce.IOC
    assert ioc.price == Decimal("100.10")  # entry + 0.1% slippage, tick-floored
    assert sl.order_type is OrderType.STOP_MARKET
    assert sl.trigger_price == Decimal("98.00")
    assert sl.qty == Decimal("9.000")
    assert tp1.order_type is OrderType.TAKE_PROFIT_MARKET
    assert tp1.trigger_price == Decimal("104.00")
    assert tp1.qty == Decimal("4.500")
    assert trail.qty == Decimal("4.500")
    assert trail.price == Decimal("1.50")  # 1x ATR trailing distance
    assert all(i.reduce_only for i in (sl, tp1, trail))
    assert all(i.side is OrderSide.SELL for i in (sl, tp1, trail))
    assert sl.expires_at == NOW + timedelta(hours=12)  # time stop at the horizon
    assert (entry.expires_at, ioc.expires_at) == (None, None)  # a decision has no time limit
    # the step 5b bound (1.5 ATR of 1.50) signed on both entry legs, re-checked by execution before sending
    assert (entry.max_entry_distance, ioc.max_entry_distance) == (Decimal("2.25"), Decimal("2.25"))
    assert (sl.max_entry_distance, tp1.max_entry_distance, trail.max_entry_distance) == (None, None, None)
    # deterministic ids: a Risk retry after a crash re-signs the same intent and client ids
    assert entry.intent_id == intent_id_for(Account.PAPER, "evt-0001", Leg.ENTRY, 0)
    assert entry.client_id == client_id("evt-0001", Leg.ENTRY, 0)
    assert res.entry_client_id == entry.client_id
    assert res.stop_client_algo_id == sl.client_id
    assert all(
        i.account is Account.PAPER and i.key_id == "risk-test" and not i.signature for i in res.intents
    )
    assert res.exposure is not None
    assert res.exposure.risk_usd == Decimal("18.000")  # 9 x 2.00 stop distance


def test_short_open_mirrors_the_plan() -> None:
    pkt = packet(candidate_set())
    d = decision(side=Side.SHORT, p=0.35, candidate_id=CID_SHORT, packet_sha256=pkt.packet_sha256)
    res = evaluate(gate_inputs(d, pkt=pkt), gate_config(), NOW)
    assert res.verdict == "approved"
    by_leg = {i.leg: i for i in res.intents}
    assert by_leg[Leg.ENTRY].side is OrderSide.SELL
    assert by_leg[Leg.SL].side is OrderSide.BUY
    assert by_leg[Leg.SL].trigger_price == Decimal("102.00")
    assert by_leg[Leg.ENTRY_IOC].price == Decimal("99.90")
    assert by_leg[Leg.ENTRY].qty == Decimal("9.000")


LATE_AS_OF = NOW - timedelta(minutes=30)
"""A candidate whose debate took 30 minutes: the decision reaches Risk at NOW."""


def _late_open(mark: str, *, atr: float = 1.5, expires_at: timedelta | None = None) -> GateInputs:
    """A LONG OPEN on a candidate of LATE_AS_OF (entry 100.00, stop 99.00, TP1 101.50) at mark `mark`."""
    cs = candidate_set(candidate(stop="99.00", tp1="101.50"), as_of=LATE_AS_OF)
    pkt = packet(cs, as_of=LATE_AS_OF, features={"atr_1h": atr})
    d = decision(
        as_of=LATE_AS_OF,
        packet_sha256=pkt.packet_sha256,
        expires_at=None if expires_at is None else LATE_AS_OF + expires_at,
    )
    return gate_inputs(d, cs=cs, pkt=pkt, mkt=market(mark=mark))


def test_an_open_decided_30_minutes_after_its_candidate_is_approved_while_the_levels_hold() -> None:
    res = evaluate(_late_open("100.40"), gate_config(), NOW)
    assert (res.verdict, res.reason) == ("approved", None)
    assert [i.leg for i in res.intents] == [Leg.SL, Leg.TP1, Leg.TRAIL, Leg.ENTRY_IOC, Leg.ENTRY]
    # a v1 message carrying an old `expires_at` is judged the same way: the field is ignored
    v1 = evaluate(_late_open("100.40", expires_at=timedelta(seconds=120)), gate_config(), NOW)
    assert (v1.verdict, v1.reason) == ("approved", None)


@pytest.mark.parametrize(
    ("mark", "atr", "reason"),
    [
        ("98.90", 1.5, "stop_crossed"),  # the mark is below the LONG stop 99.00
        ("101.60", 1.5, "tp_crossed"),  # above the TP1 101.50, yet within 1.5 ATR of the entry
        ("100.90", 0.5, "entry_distance"),  # between the levels, but 1.8 ATR from the entry
    ],
)
def test_an_open_decided_30_minutes_late_is_refused_once_the_market_invalidated_it(
    mark: str, atr: float, reason: str
) -> None:
    res = evaluate(_late_open(mark, atr=atr), gate_config(), NOW)
    assert (res.verdict, res.reason, res.intents) == ("rejected", reason, ())
    assert res.checks[-1].reason == reason
    assert mark in res.checks[-1].text


def test_wrong_packet_hash_is_an_integrity_error_with_a_critical_alert() -> None:
    # The consumer found no packet for the hash, or re-hashing the stored bytes failed.
    missing = gate_inputs(use_packet=False)
    res = evaluate(missing, gate_config(), NOW)
    assert (res.verdict, res.reason, res.intents) == ("rejected", "integrity", ())
    assert res.alert is not None
    assert (res.alert.kind, res.alert.severity) == ("integrity_error", "critical")
    tampered = gate_inputs(packet_error="sha256 of the stored packet does not match")
    res = evaluate(tampered, gate_config(), NOW)
    assert (res.reason, res.intents) == ("integrity", ())


def test_packet_of_another_coin_is_an_integrity_error() -> None:
    cs = candidate_set(coin_id=999)
    pkt = packet(cs, coin_id=999)
    d = decision(packet_sha256=pkt.packet_sha256, coin_id=COIN_ID)
    assert evaluate(gate_inputs(d, cs=cs, pkt=pkt), gate_config(), NOW).reason == "integrity"


def test_candidate_set_not_matching_the_packet_is_an_integrity_error() -> None:
    inp = gate_inputs()
    other = candidate_set(candidate(Side.LONG, entry="100.50", stop="98.00", tp1="104.50"))
    res = evaluate(gate_inputs(inp.decision, pkt=inp.packet, cs=other), gate_config(), NOW)
    assert res.reason == "integrity"


def test_unknown_candidate_is_rejected() -> None:
    pkt = packet(candidate_set())
    d = decision(candidate_id=CID_LONG_FAR, packet_sha256=pkt.packet_sha256)
    assert evaluate(gate_inputs(d, pkt=pkt), gate_config(), NOW).reason == "candidate_unknown"


def test_candidate_side_must_match_the_decision_side() -> None:
    pkt = packet(candidate_set())
    d = decision(side=Side.LONG, p=0.65, candidate_id=CID_SHORT, packet_sha256=pkt.packet_sha256)
    res = evaluate(gate_inputs(d, pkt=pkt), gate_config(), NOW)
    assert (res.reason, res.intents) == ("candidate_side", ())


@pytest.mark.parametrize(
    ("side", "p", "p_side"),
    [
        (Side.LONG, 0.40, 0.60),  # message claims 0.60 but p says 0.40
        (Side.LONG, 0.40, None),  # consistent but on the wrong side of 0.5
        (Side.SHORT, 0.70, None),
        (Side.SHORT, 0.30, 0.65),  # p_side must be recomputed, not trusted
    ],
)
def test_sign_mismatch_is_rejected(side: Side, p: float, p_side: float | None) -> None:
    pkt = packet(candidate_set())
    cid = CID_LONG if side is Side.LONG else CID_SHORT
    d = decision(side=side, p=p, p_side=p_side, candidate_id=cid, packet_sha256=pkt.packet_sha256)
    res = evaluate(gate_inputs(d, pkt=pkt), gate_config(), NOW)
    assert (res.reason, res.intents) == ("sign_mismatch", ())


def test_held_coin_same_direction_open_is_hold_with_no_order() -> None:
    res = evaluate(gate_inputs(bk=book(held=HELD_LONG)), gate_config(), NOW)
    assert res.verdict == "hold"
    assert res.effective_intent is Intent.HOLD
    assert res.intents == ()


def test_held_coin_hold_decision_changes_nothing() -> None:
    d = decision(intent=Intent.HOLD)
    res = evaluate(gate_inputs(d, bk=book(held=HELD_LONG)), gate_config(), NOW)
    assert (res.verdict, res.intents) == ("hold", ())


def test_open_against_the_held_side_exits_without_reversal() -> None:
    pkt = packet(candidate_set())
    d = decision(side=Side.SHORT, p=0.35, candidate_id=CID_SHORT, packet_sha256=pkt.packet_sha256)
    res = evaluate(gate_inputs(d, pkt=pkt, bk=book(held=HELD_LONG)), gate_config(), NOW)
    assert res.verdict == "approved"
    assert res.effective_intent is Intent.EXIT
    assert len(res.intents) == 1
    (leg,) = res.intents
    assert (leg.leg, leg.side, leg.order_type, leg.qty, leg.reduce_only) == (
        Leg.EXIT,
        OrderSide.SELL,
        OrderType.MARKET,
        Decimal("9"),
        True,
    )
    assert res.entry_intent_id == leg.intent_id


def test_exit_or_hold_without_a_position_is_ignored() -> None:
    for intent in (Intent.EXIT, Intent.HOLD):
        res = evaluate(gate_inputs(decision(intent=intent)), gate_config(), NOW)
        assert (res.verdict, res.reason, res.intents) == ("ignored", "no_position", ())


def test_btc_is_reserved_for_the_hedge_book() -> None:
    btc = market(symbol="BTCUSDT", is_btc=True, filters=symbol_filters("BTCUSDT"))
    res = evaluate(gate_inputs(mkt=btc), gate_config(), NOW)
    assert (res.reason, res.intents) == ("btc_reserved", ())
    book_held = book(held={"BTCUSDT": HeldPosition(Side.SHORT, Decimal("1"))})
    res = evaluate(gate_inputs(decision(intent=Intent.EXIT), mkt=btc, bk=book_held), gate_config(), NOW)
    assert (res.reason, res.intents) == ("btc_reserved", ())


def test_coin_without_a_symbol_is_rejected() -> None:
    assert evaluate(gate_inputs(mkt=market(symbol=None)), gate_config(), NOW).reason == "symbol_unknown"


def test_candidate_is_rechecked_against_the_current_market() -> None:
    cases = {
        "symbol_not_trading": gate_inputs(mkt=market(filters=symbol_filters(status="BREAK"))),
        "tick": gate_inputs(mkt=market(filters=symbol_filters(tick="0.3"))),
        "entry_distance": gate_inputs(mkt=market(mark="103.00")),  # 3.00 / ATR 1.5 = 2 ATR
        "missing_data": gate_inputs(mkt=market(mark=None)),
    }
    for reason, inp in cases.items():
        assert evaluate(inp, gate_config(), NOW).reason == reason, reason
    strict = gate_config(risk=risk_file(min_rr=2.5))  # candidate R:R is 2.0
    assert evaluate(gate_inputs(), strict, NOW).reason == "rr"


def test_missing_notional_cap_inputs_reject() -> None:
    cs = candidate_set()
    pkt = packet(cs, features={"open_interest_usd": None})
    d = decision(packet_sha256=pkt.packet_sha256)
    assert evaluate(gate_inputs(d, cs=cs, pkt=pkt), gate_config(), NOW).reason == "missing_data"


def test_portfolio_ceiling_rejects_with_its_reason_and_no_order() -> None:
    full = tuple(Exposure(f"C{i}USDT", Side.SHORT, Decimal("1")) for i in range(4))
    res = evaluate(gate_inputs(bk=book(exposures=full)), gate_config(), NOW)
    assert (res.verdict, res.reason, res.intents) == ("rejected", "max_positions", ())
    assert res.sizing is not None  # the breakdown is still recorded for the decision card


def test_live_uses_the_pinned_size_multiplier() -> None:
    res = evaluate(gate_inputs(), gate_config(Account.LIVE), NOW)
    assert res.sizing is not None
    assert res.sizing["mode_size_multiplier"] == 0.25
    assert all(i.account is Account.LIVE for i in res.intents)


def test_hedge_beta_and_btc_quantity_are_recorded() -> None:
    res = evaluate(gate_inputs(), gate_config(), NOW)
    assert res.hedge_beta == 1.2
    assert res.sizing is not None
    assert res.sizing["hedge_qty"] == pytest.approx(1.2 * 900 / 60000)


def test_a_new_decision_uses_the_stored_candidate_levels_not_the_mark() -> None:
    near = gate_inputs(mkt=market(mark="100.10"))
    far = with_decision(gate_inputs(mkt=market(mark="101.40")), near.decision)
    a = {i.leg: i for i in evaluate(near, gate_config(), NOW).intents}
    b = {i.leg: i for i in evaluate(far, gate_config(), NOW).intents}
    for leg in (Leg.ENTRY, Leg.ENTRY_IOC, Leg.SL, Leg.TP1):
        assert (a[leg].price, a[leg].trigger_price) == (b[leg].price, b[leg].trigger_price)


def test_packet_other_than_the_decision_names_is_an_integrity_error() -> None:
    # A self-consistent packet (its own hash verifies) stored under the hash the decision names.
    other = packet(candidate_set(), features={"atr_1h": 1.6})
    d = decision(packet_sha256=packet(candidate_set()).packet_sha256)
    assert other.packet_sha256 != d.packet_sha256
    res = evaluate(gate_inputs(d, pkt=other), gate_config(), NOW)
    assert (res.verdict, res.reason, res.intents) == ("rejected", "integrity", ())
    assert res.alert is not None
    assert (res.alert.kind, res.alert.severity) == ("integrity_error", "critical")


@pytest.mark.parametrize(
    ("side", "p", "cid", "mark"),
    [
        (Side.LONG, 0.65, CID_LONG, "98.00"),  # LONG stop 98.00: the mark is at the stop
        (Side.LONG, 0.65, CID_LONG, "97.90"),
        (Side.SHORT, 0.35, CID_SHORT, "102.00"),  # SHORT stop 102.00
        (Side.SHORT, 0.35, CID_SHORT, "102.10"),
    ],
)
def test_open_is_rejected_when_the_mark_already_crossed_the_invalidation(
    side: Side, p: float, cid: str, mark: str
) -> None:
    # Every mark here is within 1.5 ATR of the entry, so only the crossed stop can reject it.
    pkt = packet(candidate_set())
    d = decision(side=side, p=p, candidate_id=cid, packet_sha256=pkt.packet_sha256)
    res = evaluate(gate_inputs(d, pkt=pkt, mkt=market(mark=mark)), gate_config(), NOW)
    assert (res.verdict, res.reason, res.intents) == ("rejected", "stop_crossed", ())


@pytest.mark.parametrize(
    ("side", "p", "mark"),
    [
        (Side.LONG, 0.65, "101.50"),  # LONG TP1 101.50: the mark is at the take-profit
        (Side.LONG, 0.65, "101.60"),
        (Side.SHORT, 0.35, "98.50"),  # SHORT TP1 98.50
        (Side.SHORT, 0.35, "98.40"),
    ],
)
def test_open_is_rejected_when_the_mark_already_reached_the_take_profit(
    side: Side, p: float, mark: str
) -> None:
    # R:R 1.5 candidates whose TP1 lies within 1.5 ATR of the entry: only the reached TP can reject them.
    long = candidate(Side.LONG, stop="99.00", tp1="101.50")
    short = candidate(Side.SHORT, stop="101.00", tp1="98.50")
    cs = candidate_set(long, short)
    pkt = packet(cs)
    cid = long.candidate_id if side is Side.LONG else short.candidate_id
    d = decision(side=side, p=p, candidate_id=cid, packet_sha256=pkt.packet_sha256)
    res = evaluate(gate_inputs(d, cs=cs, pkt=pkt, mkt=market(mark=mark)), gate_config(), NOW)
    assert (res.verdict, res.reason, res.intents) == ("rejected", "tp_crossed", ())


def test_namespace_the_mode_no_longer_enables_opens_nothing_but_still_exits() -> None:
    res = evaluate(gate_inputs(), gate_config(opens_enabled=False), NOW)
    assert (res.verdict, res.reason, res.intents) == ("rejected", "mode_disabled", ())

    exit_inputs = gate_inputs(decision(intent=Intent.EXIT), bk=book(held=HELD_LONG))
    winding = evaluate(exit_inputs, gate_config(opens_enabled=False), NOW)
    enabled = evaluate(exit_inputs, gate_config(), NOW)
    assert winding.intents
    assert winding.verdict == enabled.verdict
    assert [i.leg for i in winding.intents] == [i.leg for i in enabled.intents]


def test_a_loosened_runtime_config_is_clipped_to_the_hard_ceilings() -> None:
    # `model_copy` skips validation, like a config that slipped past the schema: the gate clips it.
    loose = risk_file(account_state_max_age_s=3600, max_entry_distance_atr=5.0)
    stale = evaluate(gate_inputs(bk=book(state_age_s=40.0)), gate_config(risk=loose), NOW)
    assert stale.reason == "account_state_stale"

    far = evaluate(gate_inputs(mkt=market(mark="102.40")), gate_config(risk=loose), NOW)  # 1.6 ATR
    assert far.reason == "entry_distance"


def test_projected_hedge_risk_counts_on_the_opposite_side() -> None:
    # LONG 900 USD notional x beta 1.2 / BTC 60 000 = 0.018 BTC SHORT hedge; its stop is 3 x 1 000 BTC ATR
    # away, so the hedge adds 54 USD of SHORT risk. SHORT already carries 100 USD; the budget is 150 USD.
    shorts = (Exposure("ADAUSDT", Side.SHORT, Decimal("100")),)
    pkt = packet(candidate_set(), features={"btc_atr_1h": 1000.0})
    res = evaluate(gate_inputs(pkt=pkt, bk=book(exposures=shorts)), gate_config(), NOW)
    assert (res.verdict, res.reason, res.intents) == ("rejected", "same_direction_risk", ())
    assert res.sizing is not None
    assert res.sizing["hedge_risk_projected"] == pytest.approx(54.0)

    # Without the projection the same book approves; with a LONG book the hedge only shrinks it.
    assert evaluate(gate_inputs(bk=book(exposures=shorts)), gate_config(), NOW).verdict == "approved"
    shrinking = evaluate(
        gate_inputs(pkt=pkt, bk=book(exposures=shorts, hedge_qty="0.05")), gate_config(), NOW
    )
    assert shrinking.verdict == "approved"
