"""Paper venue fill rules (phase 09 section 10) and a hand-computed PnL sample (fees + funding).

Success criterion: the paper engine reproduces the PnL of a hand-computed sample order (fees + funding)
with an error < 0.1%; a post-only order touching the price fills when the estimated queue reaches 0 and
does not fill while queue remains.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from hdt.contracts.common import OrderSide, OrderType, TimeInForce
from hdt.execution.adapter_base import (
    CODE_GTX_WOULD_TAKE,
    CODE_REDUCE_ONLY_REJECTED,
    CODE_WOULD_TRIGGER_IMMEDIATELY,
    AlgoRequest,
    OrderEvent,
    OrderRejectedError,
    OrderRequest,
)
from hdt.execution.paper_venue import Book, PaperVenue

SYM = "SOLUSDT"
T0 = 1_790_000_000_000  # ms


def _venue(slippage_bp: str = "2") -> PaperVenue:
    return PaperVenue(
        maker_fee=Decimal("0.0002"),
        taker_fee=Decimal("0.0005"),
        slippage_bp=Decimal(slippage_bp),
        wallet=Decimal("10000"),
    )


def _book(bids: list[tuple[str, str]], asks: list[tuple[str, str]], *, diff: bool = True) -> Book:
    """A depth book; `diff` = maintained from 100 ms diffs (else only the 1 s depth20 snapshot)."""
    book = Book()
    book.apply_snapshot(bids, asks, u=10, ts_ms=T0)
    if diff:
        assert book.apply_diff(11, 11, 10, [], [], ts_ms=T0 + 1)
    return book


def _gtx(side: OrderSide, qty: str, price: str, cid: str = "entry-1") -> OrderRequest:
    return OrderRequest(
        symbol=SYM,
        side=side,
        order_type=OrderType.LIMIT,
        qty=Decimal(qty),
        client_id=cid,
        price=Decimal(price),
        tif=TimeInForce.GTX,
    )


def _market(side: OrderSide, qty: str, cid: str, reduce_only: bool = True) -> OrderRequest:
    return OrderRequest(
        symbol=SYM,
        side=side,
        order_type=OrderType.MARKET,
        qty=Decimal(qty),
        client_id=cid,
        reduce_only=reduce_only,
    )


def _fills(events: list[object]) -> list[tuple[Decimal, Decimal, bool | None]]:
    out = []
    for e in events:
        if isinstance(e, OrderEvent) and e.fill is not None:
            out.append((e.fill.price, e.fill.qty, e.fill.maker))
    return out


# ------------------------------------------------------------------ hand-computed PnL sample


def test_paper_pnl_matches_a_hand_computed_sample_within_0_1_percent() -> None:
    v = _venue()
    v.books[SYM] = _book([("100.00", "5"), ("99.90", "20")], [("100.10", "8"), ("100.20", "30")])
    v.place_order(_gtx(OrderSide.BUY, "10", "100.00"), T0 + 10)
    # queue ahead 5: 3 + 4 sold into the bid -> 2 fill us; then a trade through 99.95 fills the other 8
    v.on_trade(SYM, Decimal("100.00"), Decimal("3"), True, T0 + 20)
    v.on_trade(SYM, Decimal("100.00"), Decimal("4"), True, T0 + 30)
    v.on_trade(SYM, Decimal("99.95"), Decimal("1"), True, T0 + 40)
    assert v.positions[SYM].qty == Decimal("10")
    # funding: rate 0.0001 published before the funding time, settled at mark 101.00
    funding_ms = T0 + 3_600_000
    v.on_mark(SYM, Decimal("100.50"), T0 + 50, rate=Decimal("0.0001"), next_funding_ms=funding_ms)
    v.on_mark(SYM, Decimal("101.00"), funding_ms)
    # close: MARKET sell walks 4 @ 105.00 and 6 @ 104.90, each 2 bp worse
    v.books[SYM] = _book([("105.00", "4"), ("104.90", "20")], [("105.10", "10")])
    v.place_order(_market(OrderSide.SELL, "10", "exit-1"), funding_ms + 1000)

    fill_1, fill_2 = 105.00 * (1 - 0.0002), 104.90 * (1 - 0.0002)  # 104.979, 104.87902
    realized = (fill_1 - 100.0) * 4 + (fill_2 - 100.0) * 6
    fees = 100.0 * 10 * 0.0002 + (fill_1 * 4 + fill_2 * 6) * 0.0005
    funding = -10 * 101.00 * 0.0001
    expected = realized - fees + funding  # 48.36452494
    actual = float(v.wallet - Decimal("10000"))
    assert expected == pytest.approx(48.36452494, abs=1e-8)
    assert abs(actual - expected) / abs(expected) < 0.001
    assert SYM not in v.positions
    assert [f.kind for f in v.flows] == ["FUNDING_FEE"]
    assert v.flows[0].amount == Decimal("-0.1010000")
    trades = v.trades
    assert [t.maker for t in trades] == [True, True, False, False]
    assert sum(t.realized_pnl for t in trades) == pytest.approx(Decimal(repr(realized)), abs=Decimal("1e-9"))


def test_short_position_receives_positive_funding_and_the_last_rate_before_the_timestamp_applies() -> None:
    v = _venue(slippage_bp="0")
    v.books[SYM] = _book([("99.90", "50")], [("100.00", "50")])
    v.place_order(_market(OrderSide.SELL, "5", "open-s", reduce_only=False), T0 + 10)
    funding_ms = T0 + 8 * 3_600_000
    v.on_mark(SYM, Decimal("100"), T0 + 20, rate=Decimal("0.0001"), next_funding_ms=funding_ms)
    v.on_mark(SYM, Decimal("100"), T0 + 30, rate=Decimal("0.0003"), next_funding_ms=funding_ms)
    before = v.wallet
    v.on_mark(SYM, Decimal("100"), funding_ms + 5)
    assert v.wallet - before == Decimal("5") * Decimal("100") * Decimal("0.0003")  # +0.15 paid to shorts
    v.on_mark(SYM, Decimal("100"), funding_ms + 10)  # the same timestamp never settles twice
    assert len(v.flows) == 1


# ------------------------------------------------------------------ post-only queue rules


def test_post_only_does_not_fill_while_estimated_queue_remains() -> None:
    v = _venue()
    v.books[SYM] = _book([("100.00", "5")], [("100.10", "8")])
    snap, _ = v.place_order(_gtx(OrderSide.BUY, "10", "100.00"), T0 + 10)
    assert snap.status == "NEW"
    assert v.orders["entry-1"].queue_ahead == Decimal("5")
    events = v.on_trade(SYM, Decimal("100.00"), Decimal("4.999"), True, T0 + 20)
    assert _fills(events) == []
    assert v.orders["entry-1"].executed == 0
    assert v.orders["entry-1"].queue_ahead == Decimal("0.001")


def test_post_only_fills_the_overflow_once_the_queue_reaches_zero() -> None:
    v = _venue()
    v.books[SYM] = _book([("100.00", "5")], [("100.10", "8")])
    v.place_order(_gtx(OrderSide.BUY, "10", "100.00"), T0 + 10)
    assert _fills(v.on_trade(SYM, Decimal("100.00"), Decimal("5"), True, T0 + 20)) == []  # queue exactly 0
    events = v.on_trade(SYM, Decimal("100.00"), Decimal("3"), True, T0 + 30)
    assert _fills(events) == [(Decimal("100.00"), Decimal("3"), True)]
    assert v.orders["entry-1"].status == "PARTIALLY_FILLED"
    assert events[-1].fill_model == "queue"  # type: ignore[union-attr]
    events = v.on_trade(SYM, Decimal("100.00"), Decimal("50"), True, T0 + 40)
    assert _fills(events) == [(Decimal("100.00"), Decimal("7"), True)]  # never more than the order
    assert v.orders["entry-1"].status == "FILLED"


def test_trades_on_the_other_aggressor_side_or_before_the_order_do_not_consume_the_queue() -> None:
    v = _venue()
    v.books[SYM] = _book([("100.00", "5")], [("100.10", "8")])
    v.place_order(_gtx(OrderSide.BUY, "10", "100.00"), T0 + 10)
    v.on_trade(SYM, Decimal("100.00"), Decimal("20"), False, T0 + 20)  # a buyer lifting: not our side
    v.on_trade(SYM, Decimal("100.00"), Decimal("20"), True, T0 + 10)  # printed before we were placed
    v.on_trade(SYM, Decimal("100.05"), Decimal("20"), True, T0 + 30)  # above our bid
    assert v.orders["entry-1"].queue_ahead == Decimal("5")
    assert v.orders["entry-1"].executed == 0


def test_trade_through_the_level_fills_the_rest_at_the_limit() -> None:
    v = _venue()
    v.books[SYM] = _book([("100.00", "5")], [("100.10", "8")])
    v.place_order(_gtx(OrderSide.BUY, "10", "100.00"), T0 + 10)
    events = v.on_trade(SYM, Decimal("99.99"), Decimal("0.5"), True, T0 + 20)
    assert _fills(events) == [(Decimal("100.00"), Decimal("10"), True)]


def test_opposite_best_reaching_the_level_fills() -> None:
    v = _venue()
    v.books[SYM] = _book([("100.00", "5")], [("100.10", "8")])
    v.place_order(_gtx(OrderSide.BUY, "10", "100.00"), T0 + 10)
    v.books[SYM] = _book([("99.90", "5")], [("100.00", "8")])
    events = v.on_book(SYM, T0 + 20)
    assert _fills(events) == [(Decimal("100.00"), Decimal("10"), True)]


def test_queue_never_exceeds_the_displayed_level() -> None:
    v = _venue()
    v.books[SYM] = _book([("100.00", "5")], [("100.10", "8")])
    v.place_order(_gtx(OrderSide.BUY, "10", "100.00"), T0 + 10)
    v.books[SYM] = _book([("100.00", "2")], [("100.10", "8")])  # orders ahead of us were cancelled
    v.on_book(SYM, T0 + 20)
    assert v.orders["entry-1"].queue_ahead == Decimal("2")
    v.books[SYM] = _book([("100.00", "9")], [("100.10", "8")])  # orders joined behind us
    v.on_book(SYM, T0 + 30)
    assert v.orders["entry-1"].queue_ahead == Decimal("2")
    assert _fills(v.on_trade(SYM, Decimal("100.00"), Decimal("3"), True, T0 + 40)) == [
        (Decimal("100.00"), Decimal("1"), True)
    ]


def test_book_that_does_not_show_the_price_never_lowers_the_queue() -> None:
    # review cycle 1 H11 (b): a level showing 5000 went to queue 0 after a book update that omitted it,
    # and a 1-lot trade then filled the order
    v = _venue()
    v.books[SYM] = _book([("99.50", "10"), ("99.00", "5000")], [("99.60", "8")])
    v.place_order(_gtx(OrderSide.BUY, "10", "99.00"), T0 + 10)
    assert v.orders["entry-1"].queue_ahead == Decimal("5000")
    v.books[SYM] = _book([("100.00", "10"), ("99.90", "20")], [("100.10", "8")])  # 99.00 not shown
    v.on_book(SYM, T0 + 20)
    assert v.orders["entry-1"].queue_ahead == Decimal("5000")
    assert _fills(v.on_trade(SYM, Decimal("99.00"), Decimal("1"), True, T0 + 30)) == []
    assert v.orders["entry-1"].queue_ahead == Decimal("4999")


def test_queue_at_a_price_beyond_the_displayed_book_is_unknown_until_a_book_shows_it() -> None:
    v = _venue()
    v.books[SYM] = _book([("100.00", "5"), ("99.90", "20")], [("100.10", "8")])
    v.place_order(_gtx(OrderSide.BUY, "10", "99.00"), T0 + 10)  # below the deepest bid shown
    assert v.orders["entry-1"].queue_ahead is None
    restored = _venue()
    restored.load(v.to_dict())
    assert restored.orders["entry-1"].queue_ahead is None  # a restart keeps it unknown, not 0
    assert _fills(v.on_trade(SYM, Decimal("99.00"), Decimal("50"), True, T0 + 20)) == []
    v.books[SYM] = _book([("99.20", "5"), ("99.00", "7")], [("99.30", "8")])
    v.on_book(SYM, T0 + 30)
    assert v.orders["entry-1"].queue_ahead == Decimal("7")  # the first book showing the level sets it
    v.books[SYM] = _book([("99.10", "5"), ("98.90", "3")], [("99.20", "8")])  # shown and empty
    v.on_book(SYM, T0 + 40)
    assert v.orders["entry-1"].queue_ahead == 0
    assert _fills(v.on_trade(SYM, Decimal("99.00"), Decimal("2"), True, T0 + 50)) == [
        (Decimal("99.00"), Decimal("2"), True)
    ]


def test_sell_post_only_mirrors_the_rules() -> None:
    v = _venue()
    v.books[SYM] = _book([("99.90", "8")], [("100.00", "4")])
    v.place_order(_gtx(OrderSide.SELL, "6", "100.00", cid="short-1"), T0 + 10)
    assert _fills(v.on_trade(SYM, Decimal("100.00"), Decimal("4"), True, T0 + 20)) == []  # sellers
    assert _fills(v.on_trade(SYM, Decimal("100.00"), Decimal("5"), False, T0 + 30)) == [
        (Decimal("100.00"), Decimal("1"), True)
    ]


def test_post_only_that_would_take_is_rejected() -> None:
    v = _venue()
    v.books[SYM] = _book([("100.00", "5")], [("100.10", "8")])
    with pytest.raises(OrderRejectedError) as err:
        v.place_order(_gtx(OrderSide.BUY, "1", "100.10"), T0)
    assert err.value.code == CODE_GTX_WOULD_TAKE
    with pytest.raises(OrderRejectedError) as err:
        v.place_order(_gtx(OrderSide.SELL, "1", "100.00", cid="s"), T0)
    assert err.value.code == CODE_GTX_WOULD_TAKE


# ------------------------------------------------------------------ taker fills, stops, degraded model


def test_ioc_walks_only_within_its_limit_and_expires_the_rest() -> None:
    v = _venue(slippage_bp="0")
    v.books[SYM] = _book([("99.90", "5")], [("100.10", "4"), ("100.20", "30")])
    ioc = OrderRequest(
        symbol=SYM,
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        qty=Decimal("10"),
        client_id="ioc-1",
        price=Decimal("100.10"),
        tif=TimeInForce.IOC,
    )
    _, events = v.place_order(ioc, T0 + 10)
    assert _fills(events) == [(Decimal("100.10"), Decimal("4"), False)]
    assert v.orders["ioc-1"].status == "EXPIRED"
    assert v.orders["ioc-1"].executed == Decimal("4")


@pytest.mark.parametrize(
    ("side", "book_side", "limit", "taken"),
    [
        (OrderSide.BUY, [("100.05", "4"), ("100.10", "30")], "100.10", "100.05"),
        (OrderSide.SELL, [("99.95", "4"), ("99.90", "30")], "99.90", "99.95"),
    ],
)
def test_ioc_never_fills_beyond_its_limit_after_slippage(
    side: OrderSide, book_side: list[tuple[str, str]], limit: str, taken: str
) -> None:
    # review cycle 1 H11 (a): a BUY IOC limited at 100.10 took the 100.10 level at 100.120020 (+2 bp)
    v = _venue()
    buy = side is OrderSide.BUY
    v.books[SYM] = _book([] if buy else book_side, book_side if buy else [])
    ioc = OrderRequest(
        symbol=SYM,
        side=side,
        order_type=OrderType.LIMIT,
        qty=Decimal("10"),
        client_id="ioc-1",
        price=Decimal(limit),
        tif=TimeInForce.IOC,
    )
    _, events = v.place_order(ioc, T0 + 10)
    slipped = Decimal(taken) * (Decimal("1.0002") if buy else Decimal("0.9998"))
    assert _fills(events) == [(slipped, Decimal("4"), False)]  # the limit level slips past the limit
    assert v.orders["ioc-1"].status == "EXPIRED"


def test_stop_triggers_on_mark_and_walks_the_book_with_slippage() -> None:
    v = _venue()
    v.books[SYM] = _book([("99.90", "50")], [("100.00", "50")])
    v.place_order(_market(OrderSide.BUY, "10", "open-l", reduce_only=False), T0)
    v.on_mark(SYM, Decimal("100"), T0 + 1)
    stop = AlgoRequest(
        symbol=SYM,
        side=OrderSide.SELL,
        order_type=OrderType.STOP_MARKET,
        trigger_price=Decimal("95.00"),
        client_algo_id="sl-1",
        qty=Decimal("10"),
    )
    v.place_algo(stop, T0 + 2)
    v.books[SYM] = _book([("94.80", "6"), ("94.70", "20")], [("94.90", "5")])
    assert v.on_mark(SYM, Decimal("95.10"), T0 + 3) == []
    events = v.on_mark(SYM, Decimal("94.95"), T0 + 4)
    assert _fills(events) == [
        (Decimal("94.80") * Decimal("0.9998"), Decimal("6"), False),
        (Decimal("94.70") * Decimal("0.9998"), Decimal("4"), False),
    ]
    assert v.algos["sl-1"].status == "FINISHED"
    assert SYM not in v.positions
    assert all(e.fill_model == "queue" for e in events if isinstance(e, OrderEvent) and e.fill)


@pytest.mark.parametrize(
    ("mark", "remainder_at"),
    [
        ("94.95", "94.80"),  # the mark is better than the bid walked: the remainder gets the walked level
        ("94.60", "94.60"),  # the mark is worse: the remainder goes beyond the walked level
    ],
)
def test_stop_beyond_the_displayed_book_fills_no_better_than_the_levels_walked(
    mark: str, remainder_at: str
) -> None:
    # review cycle 1 H11 (c): the remainder beyond the visible book filled at the mark
    v = _venue()
    v.books[SYM] = _book([("99.90", "50")], [("100.00", "50")])
    v.place_order(_market(OrderSide.BUY, "10", "open-l", reduce_only=False), T0)
    v.on_mark(SYM, Decimal("100"), T0 + 1)
    stop = AlgoRequest(
        symbol=SYM,
        side=OrderSide.SELL,
        order_type=OrderType.STOP_MARKET,
        trigger_price=Decimal("95.00"),
        client_algo_id="sl-1",
        qty=Decimal("10"),
    )
    v.place_algo(stop, T0 + 2)
    v.books[SYM] = _book([("94.80", "6")], [("94.90", "5")])  # 6 of the 10 displayed
    events = v.on_mark(SYM, Decimal(mark), T0 + 3)
    slip = Decimal("0.9998")
    assert _fills(events) == [
        (Decimal("94.80") * slip, Decimal("6"), False),
        (Decimal(remainder_at) * slip, Decimal("4"), False),
    ]
    fills = [e for e in events if isinstance(e, OrderEvent) and e.fill]
    assert [e.fill_model for e in fills] == ["queue", "degraded"]
    assert SYM not in v.positions


def test_without_depth_diffs_fills_are_flagged_degraded() -> None:
    v = _venue()
    v.books[SYM] = _book([("99.90", "50")], [("100.00", "50")], diff=False)  # depth20 snapshot only
    _, events = v.place_order(_market(OrderSide.BUY, "2", "open-d", reduce_only=False), T0)
    assert {e.fill_model for e in events if isinstance(e, OrderEvent) and e.fill} == {"degraded"}
    assert v.orders["open-d"].fill_model == "degraded"


def test_market_without_any_book_fills_at_mark_plus_slippage_degraded() -> None:
    v = _venue()
    v.on_mark(SYM, Decimal("100"), T0)
    _, events = v.place_order(_market(OrderSide.BUY, "2", "open-m", reduce_only=False), T0 + 1)
    assert _fills(events) == [(Decimal("100") * Decimal("1.0002"), Decimal("2"), False)]
    assert events[0].fill_model == "degraded"  # type: ignore[union-attr]


def test_stop_already_crossed_is_rejected_like_binance() -> None:
    v = _venue()
    v.on_mark(SYM, Decimal("94"), T0)
    stop = AlgoRequest(
        symbol=SYM,
        side=OrderSide.SELL,
        order_type=OrderType.STOP_MARKET,
        trigger_price=Decimal("95.00"),
        client_algo_id="sl-x",
        qty=Decimal("1"),
    )
    with pytest.raises(OrderRejectedError) as err:
        v.place_algo(stop, T0 + 1)
    assert err.value.code == CODE_WOULD_TRIGGER_IMMEDIATELY


def test_reduce_only_with_nothing_to_reduce_is_rejected_and_caps_at_the_position() -> None:
    v = _venue(slippage_bp="0")
    v.books[SYM] = _book([("99.90", "50")], [("100.00", "50")])
    with pytest.raises(OrderRejectedError) as err:
        v.place_order(_market(OrderSide.SELL, "1", "ro-1"), T0)
    assert err.value.code == CODE_REDUCE_ONLY_REJECTED
    v.place_order(_market(OrderSide.BUY, "3", "open-r", reduce_only=False), T0 + 1)
    snap, _ = v.place_order(_market(OrderSide.SELL, "10", "ro-2"), T0 + 2)
    assert snap.executed_qty == Decimal("3")  # never flips the position
    assert SYM not in v.positions


def test_close_position_stop_closes_the_whole_book() -> None:
    v = _venue(slippage_bp="0")
    v.books["BTCUSDT"] = Book()
    v.on_mark("BTCUSDT", Decimal("60000"), T0)
    order = OrderRequest(
        symbol="BTCUSDT",
        side=OrderSide.SELL,
        order_type=OrderType.MARKET,
        qty=Decimal("0.032"),
        client_id="hedge-1",
    )
    v.place_order(order, T0 + 1)
    stop = AlgoRequest(
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.STOP_MARKET,
        trigger_price=Decimal("61200"),
        client_algo_id="book-sl",
        close_position=True,
        reduce_only=False,
    )
    v.place_algo(stop, T0 + 2)
    v.on_mark("BTCUSDT", Decimal("61250"), T0 + 3)
    assert "BTCUSDT" not in v.positions
    assert v.algos["book-sl"].status == "FINISHED"


def test_state_round_trips_through_persistence() -> None:
    v = _venue()
    v.books[SYM] = _book([("100.00", "5")], [("100.10", "8")])
    v.place_order(_gtx(OrderSide.BUY, "10", "100.00"), T0 + 10)
    v.on_trade(SYM, Decimal("100.00"), Decimal("7"), True, T0 + 20)
    restored = _venue()
    restored.load(v.to_dict())
    assert restored.orders["entry-1"].queue_ahead == v.orders["entry-1"].queue_ahead == 0
    assert restored.orders["entry-1"].executed == Decimal("2")
    assert restored.wallet == v.wallet
    assert restored.positions[SYM].qty == Decimal("2")
