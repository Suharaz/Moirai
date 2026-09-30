"""Execution's own hard limits (phase 09 section 5), applied even to a validly signed intent.

Success criterion (limits part): a signed intent that exceeds Execution's own ceiling is rejected.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from hdt.contracts.common import Account, Leg, OrderSide, Side
from hdt.contracts.order import OrderIntent
from hdt.execution.limits import (
    LimitContext,
    check_intent,
    check_protective_geometry,
    increases_exposure,
)
from intent_builders import exit_intent, hedge_intents, open_plan

NOW = datetime(2026, 9, 28, 12, 0, tzinfo=UTC)
CTX = LimitContext(
    equity=Decimal("10000"),
    daily_notional=Decimal(0),
    allowlist=frozenset({"SOLUSDT", "ETHUSDT"}),
    held_symbols=frozenset(),
    sl_received=True,
    entries_for_event=0,
    blocked_reason=None,
    vetoed_sides=frozenset(),
    hedge_book_qty=Decimal(0),
)


def _plan(symbol: str = "SOLUSDT", qty: str = "9", side: Side = Side.LONG) -> dict[Leg, OrderIntent]:
    long = side is Side.LONG
    legs = open_plan(
        account=Account.PAPER,
        event_id="evt-lim",
        symbol=symbol,
        side=side,
        qty=Decimal(qty),
        entry=Decimal("100.00"),
        stop=Decimal("98.00") if long else Decimal("102.00"),
        tp1=Decimal("104.00") if long else Decimal("96.00"),
        ioc_price=Decimal("100.10") if long else Decimal("99.90"),
        now=NOW,
    )
    return {i.leg: i for i in legs}


def test_signed_entry_within_the_ceilings_is_accepted() -> None:
    res = check_intent(_plan()[Leg.ENTRY], CTX, None)
    assert res.ok
    assert res.notional == Decimal("900.00")


def test_signed_entry_above_the_per_order_ceiling_is_rejected() -> None:
    res = check_intent(_plan(qty="10.01")[Leg.ENTRY], CTX, None)  # 1001 > 10% of 10 000
    assert not res.ok
    assert "execution cap" in (res.reason or "")


def test_signed_entry_above_the_daily_ceiling_is_rejected() -> None:
    res = check_intent(_plan()[Leg.ENTRY], replace(CTX, daily_notional=Decimal("29500")), None)
    assert not res.ok
    assert "daily opening notional" in (res.reason or "")


def test_leverage_above_three_is_rejected_even_if_signed() -> None:
    entry = _plan()[Leg.ENTRY].model_copy(update={"leverage": 5})  # bypasses the contract validator
    assert check_intent(entry, CTX, None).reason == "leverage 5 above 3"


def test_protective_leg_that_is_not_reduce_only_is_rejected() -> None:
    sl = _plan()[Leg.SL].model_copy(update={"reduce_only": False})
    assert "reduce-only" in (check_intent(sl, CTX, None).reason or "")


def test_btc_is_only_for_the_hedge_book() -> None:
    entry = _plan(symbol="BTCUSDT", qty="0.001")[Leg.ENTRY]
    assert check_intent(entry, CTX, None).reason == "BTC is reserved for the hedge book"
    book_stop, hedge = hedge_intents(
        account=Account.PAPER, seq=1, delta=Decimal("-0.01"), stop=Decimal("61000"), now=NOW
    )
    assert check_intent(hedge, CTX, Decimal("60000")).ok
    assert check_intent(book_stop, CTX, Decimal("60000")).ok
    alt_hedge = hedge.model_copy(update={"symbol": "SOLUSDT"})
    assert "BTCUSDT only" in (check_intent(alt_hedge, CTX, Decimal("100")).reason or "")


def test_symbol_outside_the_universe_is_refused_except_to_close_a_held_position() -> None:
    plan = _plan(symbol="DOGEUSDT")
    assert "tradable universe" in (check_intent(plan[Leg.ENTRY], CTX, None).reason or "")
    held = replace(CTX, held_symbols=frozenset({"DOGEUSDT"}))
    assert check_intent(plan[Leg.SL], held, None).ok
    close = exit_intent(
        account=Account.PAPER,
        event_id="evt-x",
        symbol="DOGEUSDT",
        held_side=Side.LONG,
        qty=Decimal(1),
        now=NOW,
    )
    assert check_intent(close, held, None).ok
    assert "tradable universe" in (check_intent(plan[Leg.ENTRY], held, None).reason or "")


def test_entry_before_its_sl_leg_is_refused() -> None:
    res = check_intent(_plan()[Leg.ENTRY], replace(CTX, sl_received=False), None)
    assert res.reason == "entry before the sl leg of its event"


def test_one_entry_per_event() -> None:
    res = check_intent(_plan()[Leg.ENTRY], replace(CTX, entries_for_event=1), None)
    assert "already accepted" in (res.reason or "")


def test_opening_is_blocked_while_resyncing_or_killed_but_closing_is_not() -> None:
    blocked = replace(CTX, blocked_reason="namespace is re-syncing")
    plan = _plan()
    assert check_intent(plan[Leg.ENTRY], blocked, None).reason == "namespace is re-syncing"
    assert check_intent(plan[Leg.ENTRY_IOC], blocked, None).reason == "namespace is re-syncing"
    for leg in (Leg.SL, Leg.TP1, Leg.TRAIL):
        assert check_intent(plan[leg], blocked, None).ok


def test_entry_on_a_vetoed_side_is_blocked_even_if_signed() -> None:
    vetoed = replace(CTX, vetoed_sides=frozenset({OrderSide.BUY}))
    assert "vetoed" in (check_intent(_plan()[Leg.ENTRY], vetoed, None).reason or "")
    assert check_intent(_plan(side=Side.SHORT)[Leg.ENTRY], vetoed, None).ok


def test_unknown_equity_blocks_exposure() -> None:
    res = check_intent(_plan()[Leg.ENTRY], replace(CTX, equity=None), None)
    assert res.reason == "equity unknown: no exposure-increasing order"


def test_hedge_growth_is_capped_and_hedge_reduction_always_passes() -> None:
    _, grow = hedge_intents(
        account=Account.PAPER, seq=2, delta=Decimal("-0.2"), stop=Decimal("61000"), now=NOW
    )
    assert "execution cap" in (check_intent(grow, CTX, Decimal("60000")).reason or "")  # 12 000 > 7 500
    book = replace(CTX, hedge_book_qty=Decimal("-0.3"), blocked_reason="namespace killed (manual)")
    _, shrink = hedge_intents(
        account=Account.PAPER, seq=3, delta=Decimal("0.2"), stop=Decimal("61000"), now=NOW
    )
    assert not increases_exposure(shrink, Decimal("-0.3"))
    assert check_intent(shrink, book, Decimal("60000")).ok
    assert increases_exposure(grow, Decimal("-0.3"))


def test_hedge_that_flips_the_book_opens_the_new_side_and_is_valued_at_it() -> None:
    _, flip = hedge_intents(  # short 0.3 -> long 0.1: a smaller book, but a new side
        account=Account.PAPER, seq=4, delta=Decimal("0.4"), stop=Decimal("59000"), now=NOW
    )
    assert increases_exposure(flip, Decimal("-0.3"))
    book = replace(CTX, hedge_book_qty=Decimal("-0.3"))
    res = check_intent(flip, book, Decimal("60000"))
    assert res.ok
    assert res.notional == Decimal("6000.0")  # the new long 0.1, not the 0.4 traded
    blocked = replace(book, blocked_reason="namespace killed (manual)")
    assert check_intent(flip, blocked, Decimal("60000")).reason == "namespace killed (manual)"
    capped = replace(book, daily_notional=Decimal("24500"))  # 24 500 + 6 000 > 30 000
    assert "daily opening notional" in (check_intent(flip, capped, Decimal("60000")).reason or "")


@pytest.mark.parametrize("side", [Side.LONG, Side.SHORT])
def test_protective_geometry(side: Side) -> None:
    plan = _plan(side=side)
    entry = plan[Leg.ENTRY]
    assert check_protective_geometry(entry, plan[Leg.SL]) is None
    assert check_protective_geometry(entry, plan[Leg.TP1]) is None
    swapped_sl = plan[Leg.SL].model_copy(update={"trigger_price": plan[Leg.TP1].trigger_price})
    assert "wrong side" in (check_protective_geometry(entry, swapped_sl) or "")
    swapped_tp = plan[Leg.TP1].model_copy(update={"trigger_price": plan[Leg.SL].trigger_price})
    assert "wrong side" in (check_protective_geometry(entry, swapped_tp) or "")
    same_side = plan[Leg.SL].model_copy(update={"side": entry.side})
    assert "entry side" in (check_protective_geometry(entry, same_side) or "")
