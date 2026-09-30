"""Contract: no price, quantity, size or leverage of an `OrderIntent` can come from LLM output.

A `DecisionMsg` only carries `candidate_id` (plus the calibrated `p` and the bounded `manager_size`
multiplier). Stray numeric fields an LLM might add are ignored at parse time and never reach an order;
every price is taken from the stored candidate (or derived from it and the packet ATR by config), every
quantity from the deterministic sizing chain, and the risk to the stop never exceeds the equity budget.
"""

from __future__ import annotations

from decimal import ROUND_FLOOR, Decimal

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from hdt.contracts.common import Leg, Side
from hdt.contracts.decision import DecisionMsg
from hdt.contracts.order import OrderIntent
from hdt.risk.gate import evaluate
from risk_builders import (
    CID_LONG,
    CID_LONG_FAR,
    CID_SHORT,
    NOW,
    candidate,
    candidate_set,
    decision,
    gate_config,
    gate_inputs,
    packet,
    risk_file,
)

NUMERIC_FIELDS = {
    "price",
    "qty",
    "quantity",
    "size",
    "notional",
    "leverage",
    "entry",
    "entry_price",
    "stop",
    "stop_price",
    "invalidation",
    "tp",
    "tp1",
    "take_profit",
    "trigger_price",
    "risk_usd",
}
STRAY = {
    "price": 1.0,
    "entry": 55.5,
    "stop": 1.0,
    "tp1": 999.0,
    "qty": 1_000_000,
    "size": 50.0,
    "notional": 1e9,
    "leverage": 125,
    "trigger_price": 2.0,
    "risk_usd": 1e7,
}
PRICE_LEGS = (Leg.ENTRY, Leg.ENTRY_IOC, Leg.SL, Leg.TP1, Leg.TRAIL)


def _order_numbers(intents: tuple[OrderIntent, ...]) -> list[tuple[Leg, object, object, object, object]]:
    return [(i.leg, i.price, i.trigger_price, i.qty, i.leverage) for i in intents]


def test_decision_schema_has_no_price_size_or_leverage_field() -> None:
    assert NUMERIC_FIELDS.isdisjoint(DecisionMsg.model_fields)


def test_stray_numbers_in_a_decision_are_ignored_at_parse() -> None:
    d = decision(extra=STRAY)
    dumped = d.model_dump()
    assert NUMERIC_FIELDS.isdisjoint(dumped)
    assert d == decision()


def test_stray_numbers_never_change_the_orders() -> None:
    clean = evaluate(gate_inputs(), gate_config(), NOW)
    polluted = evaluate(gate_inputs(decision(extra=STRAY)), gate_config(), NOW)
    assert clean.verdict == polluted.verdict == "approved"
    assert _order_numbers(clean.intents) == _order_numbers(polluted.intents)
    for intent in polluted.intents:
        assert intent.leverage in (None, 1, 2, 3)
        assert intent.price not in (Decimal("1.0"), Decimal("55.5"))
        assert intent.qty != Decimal(1_000_000)


def test_every_price_comes_from_the_chosen_candidate() -> None:
    near = candidate(Side.LONG, entry="100.00", stop="98.00", tp1="104.00", candidate_id=CID_LONG)
    far = candidate(Side.LONG, entry="99.00", stop="96.00", tp1="105.00", candidate_id=CID_LONG_FAR)
    cs = candidate_set(near, far, candidate(Side.SHORT, candidate_id=CID_SHORT))
    pkt = packet(cs)
    atr = Decimal("1.50")  # packet atr_1h x trail_atr_mult 1.0
    for chosen in (near, far):
        d = decision(candidate_id=chosen.candidate_id, packet_sha256=pkt.packet_sha256, extra=STRAY)
        res = evaluate(gate_inputs(d, cs=cs, pkt=pkt), gate_config(), NOW)
        assert res.verdict == "approved"
        legs = {i.leg: i for i in res.intents}
        assert legs[Leg.ENTRY].price == chosen.entry
        ioc = (chosen.entry * Decimal("1.001")).quantize(Decimal("0.01"), rounding=ROUND_FLOOR)
        assert legs[Leg.ENTRY_IOC].price == ioc  # config slippage over the candidate entry
        assert legs[Leg.SL].trigger_price == chosen.invalidation
        assert legs[Leg.TP1].trigger_price == chosen.tp1
        assert legs[Leg.TRAIL].trigger_price == chosen.tp1
        assert legs[Leg.TRAIL].price == atr
        allowed = {chosen.entry, chosen.invalidation, chosen.tp1, legs[Leg.ENTRY_IOC].price, atr}
        for leg in PRICE_LEGS:
            for number in (legs[leg].price, legs[leg].trigger_price):
                assert number is None or number in allowed


@settings(max_examples=200, deadline=None)
@given(
    p=st.floats(min_value=0.51, max_value=0.99),
    manager_size=st.floats(min_value=0.01, max_value=1.0),
    stray=st.dictionaries(
        st.sampled_from(sorted(NUMERIC_FIELDS)),
        st.one_of(st.floats(allow_nan=False, allow_infinity=False), st.integers(), st.text(max_size=8)),
        max_size=6,
    ),
)
def test_quantity_stays_within_the_equity_risk_budget_whatever_the_message(
    p: float, manager_size: float, stray: dict[str, object]
) -> None:
    d = decision(p=p, manager_size=manager_size, extra=stray)
    res = evaluate(gate_inputs(d), gate_config(), NOW)
    risk = risk_file()
    if res.verdict != "approved":
        assert res.intents == ()
        return
    legs = {i.leg: i for i in res.intents}
    entry = legs[Leg.ENTRY]
    assert entry.qty is not None
    stop_distance = abs(Decimal("100.00") - Decimal("98.00"))
    budget = Decimal("10000") * Decimal(repr(risk.risk_pct))
    assert entry.qty * stop_distance <= budget
    assert entry.qty * Decimal("100.00") <= Decimal("10000") * Decimal(repr(risk.max_margin_pct)) * 3
    assert entry.leverage is not None
    assert 1 <= entry.leverage <= 3
    assert legs[Leg.SL].qty == entry.qty
    assert legs[Leg.ENTRY_IOC].qty == entry.qty


@pytest.mark.parametrize("field", ["qty", "price", "leverage"])
def test_a_decision_cannot_smuggle_order_fields_into_the_signed_intent(field: str) -> None:
    res = evaluate(gate_inputs(decision(extra={field: 42})), gate_config(), NOW)
    baseline = evaluate(gate_inputs(), gate_config(), NOW)
    assert [getattr(i, field) for i in res.intents] == [getattr(i, field) for i in baseline.intents]
