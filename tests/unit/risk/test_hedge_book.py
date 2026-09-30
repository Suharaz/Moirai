"""Pooled BTC hedge book math (phase 09 section 7): net target, rebalance threshold, one book stop, risk.

Success criterion (pure part): two alt LONGs produce one net BTC target (a single short book), not one
hedge per alt. The exchange side (one BTC position, one STOP, clean reconcile) is covered by
`tests/integration/test_hedge_book_exec.py`.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from hdt.contracts.common import Account, Side
from hdt.risk.hedge_book import (
    AltLeg,
    Rebalance,
    book_risk,
    book_side,
    book_stop,
    cap_growth,
    hedge_event_id,
    plan_rebalance,
    target_notional,
    target_qty,
)
from risk_builders import risk_file, symbol_filters

HEDGE = risk_file().hedge  # threshold 20% of target, 1x minNotional, stop 3x ATR
BTC = symbol_filters("BTCUSDT", tick="0.1", step="0.001", min_qty="0.001", min_notional="100")
MARK = Decimal("50000")
TWO_LONGS = [AltLeg("AAAUSDT", Decimal("1000"), 1.2), AltLeg("BBBUSDT", Decimal("500"), 0.8)]


def test_two_alt_longs_net_into_one_short_btc_target() -> None:
    assert target_notional(TWO_LONGS) == Decimal("-1600.0")
    assert target_qty(TWO_LONGS, MARK, BTC) == Decimal("-0.032")
    assert book_side(target_qty(TWO_LONGS, MARK, BTC)) is Side.SHORT


def test_opposite_alts_offset_inside_the_book() -> None:
    legs = [*TWO_LONGS, AltLeg("CCCUSDT", Decimal("-700"), 1.0)]
    assert target_qty(legs, MARK, BTC) == Decimal("-0.018")
    balanced = [AltLeg("AAAUSDT", Decimal("1000"), 1.0), AltLeg("CCCUSDT", Decimal("-1000"), 1.0)]
    assert target_qty(balanced, MARK, BTC) == 0
    assert book_side(Decimal(0)) is None


def test_target_rounds_toward_zero_to_the_lot() -> None:
    assert target_qty([AltLeg("AAAUSDT", Decimal("1635"), 1.0)], MARK, BTC) == Decimal("-0.032")
    assert target_qty([AltLeg("AAAUSDT", Decimal("-1635"), 1.0)], MARK, BTC) == Decimal("0.032")
    with pytest.raises(ValueError, match="mark"):
        target_qty(TWO_LONGS, Decimal(0), BTC)


def test_first_rebalance_opens_the_whole_target() -> None:
    plan = plan_rebalance(Decimal("-0.032"), Decimal(0), MARK, BTC, HEDGE)
    assert plan.delta == Decimal("-0.032")
    assert plan.needed
    assert not plan.reduces


def test_drift_within_the_threshold_trades_nothing() -> None:
    # threshold = max(20% x 0.032, 1 x 100 USD / 50 000, minQty) = 0.0064
    plan = plan_rebalance(Decimal("-0.032"), Decimal("-0.027"), MARK, BTC, HEDGE)
    assert plan.delta == 0
    assert not plan.needed


def test_drift_above_the_threshold_rebalances_and_shrinking_is_reduce_only() -> None:
    plan = plan_rebalance(Decimal("-0.020"), Decimal("-0.032"), MARK, BTC, HEDGE)
    assert plan.delta == Decimal("0.012")
    assert plan.reduces
    grow = plan_rebalance(Decimal("-0.045"), Decimal("-0.032"), MARK, BTC, HEDGE)
    assert grow.delta == Decimal("-0.013")
    assert not grow.reduces


def test_no_alt_exposure_left_closes_the_book() -> None:
    plan = plan_rebalance(Decimal(0), Decimal("-0.032"), MARK, BTC, HEDGE)
    assert plan.delta == Decimal("0.032")
    assert plan.reduces


def test_min_notional_floor_of_the_threshold() -> None:
    # a tiny book: 20% of 0.004 = 0.0008 is below 100 USD / 50 000 = 0.002
    assert plan_rebalance(Decimal("-0.004"), Decimal("-0.0025"), MARK, BTC, HEDGE).delta == 0
    assert plan_rebalance(Decimal("-0.005"), Decimal("-0.0025"), MARK, BTC, HEDGE).delta == Decimal("-0.002")


def test_one_book_stop_beyond_the_mark_by_the_atr_multiple() -> None:
    atr = Decimal("333.33")  # 3x = 999.99
    assert book_stop(Decimal("-0.032"), MARK, atr, HEDGE, BTC) == Decimal("51000.0")  # short: ceil
    assert book_stop(Decimal("0.032"), MARK, atr, HEDGE, BTC) == Decimal("49000.0")  # long: floor
    with pytest.raises(ValueError, match="empty book"):
        book_stop(Decimal(0), MARK, atr, HEDGE, BTC)
    with pytest.raises(ValueError, match="ATR"):
        book_stop(Decimal("-0.032"), MARK, Decimal(0), HEDGE, BTC)


def test_book_risk_is_the_distance_to_its_stop() -> None:
    assert book_risk(Decimal("-0.032"), Decimal("51200"), MARK) == Decimal("38.400")
    assert book_risk(Decimal("0.032"), Decimal("48800"), MARK) == Decimal("38.400")
    assert book_risk(Decimal("0.032"), Decimal("50500"), MARK) == 0  # stop already in profit
    assert book_risk(Decimal("-0.032"), None, MARK, leverage=2) == Decimal("800")  # margin at risk
    assert book_risk(Decimal(0), None, MARK) == 0


def test_hedge_event_ids_are_per_account_and_sequence() -> None:
    assert hedge_event_id(Account.PAPER, 7) == "hedge:paper:7"
    assert hedge_event_id(Account.LIVE, 7) != hedge_event_id(Account.PAPER, 7)


STOP_DISTANCE = Decimal("1000")  # 3 x 333.33 BTC ATR, tick-rounded


def test_growth_is_capped_to_the_same_direction_room() -> None:
    plan = plan_rebalance(Decimal("-0.032"), Decimal(0), MARK, BTC, HEDGE)
    # 20 USD of room at 1 000 USD per BTC to the stop: the book may reach 0.020 BTC.
    capped = cap_growth(plan, Decimal("20"), STOP_DISTANCE, MARK, BTC)
    assert capped.delta == Decimal("-0.020")
    assert "capped" in capped.reason
    assert cap_growth(plan, Decimal("32"), STOP_DISTANCE, MARK, BTC) == plan  # exactly the budget


def test_capped_growth_below_the_exchange_minimum_is_skipped() -> None:
    plan = plan_rebalance(Decimal("-0.032"), Decimal(0), MARK, BTC, HEDGE)
    # room for 0.001 BTC = 50 USD notional, below the 100 USD minNotional
    skipped = cap_growth(plan, Decimal("1.5"), STOP_DISTANCE, MARK, BTC)
    assert not skipped.needed
    assert cap_growth(plan, Decimal("-5"), STOP_DISTANCE, MARK, BTC).delta == 0  # no room at all


def test_a_book_already_over_the_cap_is_never_grown_nor_cut_by_the_cap() -> None:
    plan = plan_rebalance(Decimal("-0.040"), Decimal("-0.030"), MARK, BTC, HEDGE)
    assert plan.delta == Decimal("-0.010")
    held = cap_growth(plan, Decimal("20"), STOP_DISTANCE, MARK, BTC)
    assert held == Rebalance(plan.target_qty, plan.current_qty, Decimal(0), held.reason)


def test_a_flip_without_room_on_the_new_side_only_closes_the_old_book() -> None:
    plan = plan_rebalance(Decimal("-0.040"), Decimal("0.010"), MARK, BTC, HEDGE)
    closed = cap_growth(plan, Decimal(0), STOP_DISTANCE, MARK, BTC)
    assert closed.delta == Decimal("-0.010")
    assert closed.reduces
    partial = cap_growth(plan, Decimal("20"), STOP_DISTANCE, MARK, BTC)
    assert partial.delta == Decimal("-0.030")  # +0.010 -> -0.020
