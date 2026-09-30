"""Binance adapter and client against recorded-shape payloads (phase 09 section 11, no network).

REST responses and user-stream frames in the exchange's own shapes map to the adapter's snapshots and
events; the error codes execution branches on (-2011 / -2013 unknown order, -1021 clock drift, 418 IP ban,
429 rate limit, 5xx unknown state) end where the order manager expects them.
"""

from __future__ import annotations

import hashlib
import hmac
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import httpx
import pytest

from binance_shapes import (
    ACCOUNT_V3,
    ALGO_CLOSE_POSITION,
    ALGO_NEW,
    ALGO_TRIGGERED,
    API_KEY,
    API_SECRET,
    INCOME,
    ORDER_FILLED,
    ORDER_NEW,
    POSITION_RISK,
    T0,
    USER_TRADES,
    WS_ACCOUNT_UPDATE,
    WS_ACCOUNT_UPDATE_NO_USDT,
    WS_ALGO_NEW,
    WS_ALGO_REJECTED,
    WS_ALGO_TRIGGERED,
    WS_LISTEN_KEY_EXPIRED,
    WS_ORDER_CANCELED_NO_FEE,
    WS_ORDER_NEW,
    WS_ORDER_PARTIAL,
    WS_TRADE_LITE,
    BinanceRoutes,
    error,
)
from hdt.contracts.common import Account, OrderSide, OrderType, TimeInForce
from hdt.execution import binance_client
from hdt.execution.adapter_base import (
    AlgoEvent,
    AlgoRequest,
    ExchangeError,
    IpBannedError,
    OrderEvent,
    OrderRejectedError,
    OrderRequest,
    PositionModeError,
    RateLimitedError,
    UnknownOrderStateError,
)
from hdt.execution.binance_adapter import INCOME_OVERLAP_MS, BinanceAdapter, parse_algo, parse_order
from hdt.execution.user_stream import AccountUpdate, StreamRestartError, parse_user_event

ETH = "ETHUSDT"


def at(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=UTC)


class Clock:
    """Wall and monotonic time of the client module (Retry-After windows, time sync)."""

    def __init__(self) -> None:
        self.now = T0 / 1000

    def time(self) -> float:
        return self.now

    def monotonic(self) -> float:
        return self.now


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Clock:
    fake = Clock()
    monkeypatch.setattr(binance_client, "time", fake)
    return fake


@pytest.fixture
def routes() -> BinanceRoutes:
    return BinanceRoutes()


@pytest.fixture
async def adapter(routes: BinanceRoutes) -> Any:
    adapter = BinanceAdapter(Account.TESTNET, routes.client())
    yield adapter
    await adapter.aclose()


# ---------------------------------------------------------------------------- REST mapping


def test_order_result_maps_to_a_snapshot() -> None:
    new = parse_order(ORDER_NEW)
    assert (new.symbol, new.client_id, new.order_id) == (ETH, "hdt_ev1_entry_0", 8389765519)
    assert (new.side, new.order_type, new.tif, new.status) == (OrderSide.BUY, "LIMIT", "GTX", "NEW")
    assert (new.qty, new.executed_qty, new.price) == (Decimal("0.440"), Decimal(0), Decimal("2500.10"))
    assert new.avg_price is None  # "0.00" means no fill yet
    assert new.reduce_only is False
    assert new.updated_at == at(T0)
    assert new.is_open

    filled = parse_order(ORDER_FILLED)
    assert (filled.status, filled.executed_qty, filled.avg_price) == (
        "FILLED",
        Decimal("0.440"),
        Decimal("2500.55"),
    )
    assert filled.updated_at == at(T0 + 7)
    assert not filled.is_open


def test_algo_responses_map_trigger_quantity_and_the_triggered_order() -> None:
    new = parse_algo(ALGO_NEW)
    assert (new.client_algo_id, new.algo_id, new.order_type, new.status) == (
        "hdt_ev1_sl_0",
        2146760,
        "STOP_MARKET",
        "NEW",
    )
    assert (new.side, new.trigger_price, new.qty) == (OrderSide.SELL, Decimal("2450.00"), Decimal("0.440"))
    assert (new.reduce_only, new.close_position, new.triggered_order_id) == (True, False, None)
    assert new.is_working

    triggered = parse_algo(ALGO_TRIGGERED)
    assert (triggered.status, triggered.triggered_order_id) == ("TRIGGERED", 8389766001)
    assert triggered.updated_at == at(T0 + 60_001)
    assert not triggered.is_working

    book_stop = parse_algo(ALGO_CLOSE_POSITION)
    assert (book_stop.qty, book_stop.close_position, book_stop.triggered_order_id) == (None, True, None)


async def test_positions_skip_flat_symbols_and_derive_leverage_and_margin_type(
    adapter: BinanceAdapter, routes: BinanceRoutes
) -> None:
    routes.queue("GET", "/fapi/v3/positionRisk", POSITION_RISK)
    eth, btc = await adapter.positions()
    assert (eth.symbol, eth.qty, eth.entry_price, eth.mark_price) == (
        ETH,
        Decimal("0.440"),
        Decimal("2500.55"),
        Decimal("2510.00000000"),
    )
    assert (eth.leverage, eth.margin_type, eth.liquidation_price) == (2, "CROSSED", Decimal("1260.43"))
    assert eth.unrealized_pnl == Decimal("4.15800000")
    assert (btc.symbol, btc.qty, btc.leverage, btc.margin_type) == (
        "BTCUSDT",
        Decimal("-0.010"),
        3,
        "ISOLATED",
    )
    assert btc.liquidation_price is None  # "0": no liquidation price


async def test_balance_reads_the_usdt_asset_not_the_multi_asset_totals(
    adapter: BinanceAdapter, routes: BinanceRoutes
) -> None:
    routes.queue("GET", "/fapi/v3/account", ACCOUNT_V3)
    balance = await adapter.balance()
    assert balance.wallet_balance == Decimal("5000.12000000")
    assert balance.unrealized_pnl == Decimal("5.15800000")
    assert balance.available == Decimal("4216.74466667")
    assert balance.equity == Decimal("5005.27800000")

    routes.queue("GET", "/fapi/v3/account", {**ACCOUNT_V3, "assets": []})
    totals = await adapter.balance()
    assert (totals.wallet_balance, totals.available) == (Decimal("15123.45000000"), Decimal("14340.07466667"))


async def test_user_trades_attribute_unknown_orders_by_asking_the_exchange(
    adapter: BinanceAdapter, routes: BinanceRoutes
) -> None:
    routes.queue("GET", "/fapi/v1/userTrades", USER_TRADES)
    routes.queue(
        "GET", "/fapi/v1/order", {**ORDER_FILLED, "orderId": 8389766001, "clientOrderId": "autoclose-x1"}
    )
    fills = await adapter.user_trades(ETH, 698759, None, known={8389765520: "hdt_ev1_entry_ioc_0"})

    assert routes.calls("GET", "/fapi/v1/userTrades")[0].params["fromId"] == "698759"
    (lookup,) = routes.calls("GET", "/fapi/v1/order")
    assert lookup.params["orderId"] == "8389766001"  # only the order the ledger does not know
    assert [(f.trade_id, f.client_id) for f in fills] == [
        (698759, "hdt_ev1_entry_ioc_0"),
        (698760, "hdt_ev1_entry_ioc_0"),
        (698801, "autoclose-x1"),
    ]
    first, bnb_fee, stop = fills
    assert (first.side, first.price, first.qty, first.fee, first.fee_asset) == (
        OrderSide.BUY,
        Decimal("2500.00"),
        Decimal("0.220"),
        Decimal("0.27500000"),
        "USDT",
    )
    assert (bnb_fee.fee, bnb_fee.fee_asset) == (Decimal("0.00012000"), "BNB")
    assert (stop.side, stop.realized_pnl, stop.maker, stop.time) == (
        OrderSide.SELL,
        Decimal("-22.08200000"),
        False,
        at(T0 + 60_002),
    )


async def test_cash_flows_skip_trade_income_and_page_by_time(
    adapter: BinanceAdapter, routes: BinanceRoutes
) -> None:
    page = [
        {**INCOME[3], "tranId": 1_000_000 + i, "time": T0 + i, "income": "-0.01000000"} for i in range(1000)
    ]
    routes.queue("GET", "/fapi/v1/income", page, INCOME)
    flows = await adapter.cash_flows(at(T0 - 7_200_000))

    first, second = routes.calls("GET", "/fapi/v1/income")
    assert first.params["startTime"] == str(T0 - 7_200_000)
    assert second.params["startTime"] == str(T0 + 999 + 1)
    assert (
        len(flows) == 1000 + 2
    )  # the page, then TRANSFER and FUNDING_FEE (COMMISSION, REALIZED_PNL skipped)
    transfer, funding = flows[-2:]
    assert (transfer.kind, transfer.symbol, transfer.amount) == ("TRANSFER", None, Decimal("1000.00000000"))
    assert transfer.flow_id == "TRANSFER:9689322392:"
    assert (funding.kind, funding.symbol, funding.amount, funding.time) == (
        "FUNDING_FEE",
        ETH,
        Decimal("-0.11040000"),
        at(T0 + 28_800_000),
    )


def _income(kind: str, tran_id: int, time_ms: int) -> dict[str, Any]:
    return {**INCOME[3], "incomeType": kind, "tranId": tran_id, "time": time_ms}


async def test_cash_flows_resume_behind_the_newest_income_row_once_the_flows_are_recorded(
    adapter: BinanceAdapter, routes: BinanceRoutes
) -> None:
    day = 86_400_000
    funding = _income("FUNDING_FEE", 1, T0)
    trades = [_income("REALIZED_PNL", 2, T0 + day), _income("COMMISSION", 3, T0 + day + 5)]
    routes.queue("GET", "/fapi/v1/income", [funding, *trades], [funding, *trades], [])

    (flow,) = await adapter.cash_flows(at(T0 - day))
    assert flow.time == at(T0)
    # The caller has not recorded that flow yet (it passes an earlier start again): scan from its start.
    assert len(await adapter.cash_flows(at(T0 - day))) == 1
    # Recorded: the next start is the flow itself; a day of trade income after it is not paged again.
    assert await adapter.cash_flows(at(T0)) == []

    starts = [c.params["startTime"] for c in routes.calls("GET", "/fapi/v1/income")]
    assert starts == [str(T0 - day), str(T0 - day), str(T0 + day + 5 - INCOME_OVERLAP_MS)]


async def test_failed_income_page_keeps_the_scan_start(
    adapter: BinanceAdapter, routes: BinanceRoutes
) -> None:
    page = [_income("REALIZED_PNL", 10 + i, T0 + i) for i in range(1000)]
    routes.queue("GET", "/fapi/v1/income", page, error(400, -1100, "bad parameter"), [])
    with pytest.raises(ExchangeError):
        await adapter.cash_flows(at(T0))
    await adapter.cash_flows(at(T0))
    starts = [c.params["startTime"] for c in routes.calls("GET", "/fapi/v1/income")]
    assert starts == [str(T0), str(T0 + 1000), str(T0)]


@pytest.mark.parametrize(
    "body", [[ALGO_NEW, ALGO_CLOSE_POSITION], {"orders": [ALGO_NEW, ALGO_CLOSE_POSITION]}]
)
async def test_open_algo_orders_accept_a_list_or_an_orders_object(
    adapter: BinanceAdapter, routes: BinanceRoutes, body: Any
) -> None:
    routes.queue("GET", "/fapi/v1/openAlgoOrders", body)
    algos = await adapter.open_algo_orders()
    assert [(a.client_algo_id, a.close_position) for a in algos] == [
        ("hdt_ev1_sl_0", False),
        ("hdt_hb1_sl_0", True),
    ]


async def test_orders_are_sent_signed_with_exchange_formatted_fields(
    adapter: BinanceAdapter, routes: BinanceRoutes
) -> None:
    routes.queue("POST", "/fapi/v1/order", ORDER_NEW)
    routes.queue("POST", "/fapi/v1/algoOrder", ALGO_CLOSE_POSITION, ALGO_NEW)
    await adapter.place_order(
        OrderRequest(
            symbol=ETH,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            qty=Decimal("0.4400"),
            client_id="hdt_ev1_entry_0",
            price=Decimal("2500.10"),
            tif=TimeInForce.GTX,
        )
    )
    await adapter.place_algo(
        AlgoRequest(
            symbol="BTCUSDT",
            side=OrderSide.BUY,
            order_type=OrderType.STOP_MARKET,
            trigger_price=Decimal("70000.0"),
            client_algo_id="hdt_hb1_sl_0",
            qty=None,
            close_position=True,
        )
    )
    await adapter.place_algo(
        AlgoRequest(
            symbol=ETH,
            side=OrderSide.SELL,
            order_type=OrderType.STOP_MARKET,
            trigger_price=Decimal("2450.00"),
            client_algo_id="hdt_ev1_sl_0",
            qty=Decimal("0.440"),
            reduce_only=True,
        )
    )
    (order,) = routes.calls("POST", "/fapi/v1/order")
    assert {k: order.params[k] for k in ("symbol", "side", "type", "quantity", "price", "timeInForce")} == {
        "symbol": ETH,
        "side": "BUY",
        "type": "LIMIT",
        "quantity": "0.44",
        "price": "2500.1",
        "timeInForce": "GTX",
    }
    assert (order.params["newClientOrderId"], order.params["newOrderRespType"]) == (
        "hdt_ev1_entry_0",
        "RESULT",
    )
    assert "reduceOnly" not in order.params
    assert order.headers["x-mbx-apikey"] == API_KEY
    book_stop, stop = routes.calls("POST", "/fapi/v1/algoOrder")
    assert (book_stop.params["closePosition"], book_stop.params["algoType"]) == ("true", "CONDITIONAL")
    assert "quantity" not in book_stop.params
    assert "reduceOnly" not in book_stop.params
    assert (stop.params["quantity"], stop.params["reduceOnly"], stop.params["workingType"]) == (
        "0.44",
        "true",
        "MARK_PRICE",
    )
    for sent in (order, book_stop, stop):
        query = "&".join(f"{k}={v}" for k, v in sent.params.items() if k != "signature")
        expected = hmac.new(API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()
        assert sent.params["signature"] == expected


async def test_hedge_mode_account_is_refused(adapter: BinanceAdapter, routes: BinanceRoutes) -> None:
    routes.queue("GET", "/fapi/v1/positionSide/dual", {"dualSidePosition": False}, {"dualSidePosition": True})
    await adapter.ensure_one_way()
    with pytest.raises(PositionModeError):
        await adapter.ensure_one_way()


async def test_prepare_symbol_tolerates_an_unchanged_margin_type_once_per_leverage(
    adapter: BinanceAdapter, routes: BinanceRoutes
) -> None:
    routes.always("POST", "/fapi/v1/marginType", error(400, -4046, "No need to change margin type."))
    routes.always("POST", "/fapi/v1/leverage", {"leverage": 2, "maxNotionalValue": "1000000", "symbol": ETH})
    await adapter.prepare_symbol(ETH, 2)
    await adapter.prepare_symbol(ETH, 2)
    assert len(routes.calls("POST", "/fapi/v1/leverage")) == 1
    routes.queue(
        "POST",
        "/fapi/v1/marginType",
        error(400, -4047, "Margin type cannot be changed if there exists position."),
    )
    with pytest.raises(OrderRejectedError):
        await adapter.prepare_symbol(ETH, 3)


# ---------------------------------------------------------------------------- error codes


async def test_cancel_of_an_already_final_order_reads_the_truth(
    adapter: BinanceAdapter, routes: BinanceRoutes
) -> None:
    routes.queue("DELETE", "/fapi/v1/order", error(400, -2011, "Unknown order sent."))
    routes.queue("GET", "/fapi/v1/order", ORDER_FILLED)
    snap = await adapter.cancel_order(ETH, "hdt_ev1_entry_ioc_0")
    assert snap is not None
    assert (snap.status, snap.executed_qty) == ("FILLED", Decimal("0.440"))
    assert routes.calls("GET", "/fapi/v1/order")[0].params["origClientOrderId"] == "hdt_ev1_entry_ioc_0"


async def test_order_that_never_existed_is_none(adapter: BinanceAdapter, routes: BinanceRoutes) -> None:
    routes.queue("DELETE", "/fapi/v1/order", error(400, -2011, "Unknown order sent."))
    routes.queue("GET", "/fapi/v1/order", error(400, -2013, "Order does not exist."), error(400, -2013, "x"))
    assert await adapter.cancel_order(ETH, "hdt_ev9_entry_0") is None
    assert await adapter.query_order(ETH, "hdt_ev9_entry_0") is None


async def test_other_cancel_rejections_are_raised(adapter: BinanceAdapter, routes: BinanceRoutes) -> None:
    routes.queue("DELETE", "/fapi/v1/order", error(400, -1102, "Mandatory parameter was not sent."))
    with pytest.raises(OrderRejectedError) as raised:
        await adapter.cancel_order(ETH, "hdt_ev1_entry_0")
    assert raised.value.code == -1102


@pytest.mark.parametrize(
    "absent",
    [
        error(400, -2013, "Order does not exist."),
        error(400, -2011, "Unknown order sent."),
        error(404, -1, ""),
    ],
)
async def test_cancel_algo_of_a_gone_conditional_order_reads_the_truth(
    adapter: BinanceAdapter, routes: BinanceRoutes, absent: httpx.Response
) -> None:
    routes.queue(
        "DELETE", "/fapi/v1/algoOrder", error(400, -2011, "Unknown order sent."), error(400, -2011, "x")
    )
    routes.queue("GET", "/fapi/v1/algoOrder", ALGO_TRIGGERED, absent)
    triggered = await adapter.cancel_algo("hdt_ev1_sl_0")
    assert triggered is not None
    assert (triggered.status, triggered.triggered_order_id) == (
        "TRIGGERED",
        8389766001,
    )
    assert await adapter.cancel_algo("hdt_ev1_sl_0") is None


async def test_clock_drift_resyncs_time_and_retries_once(
    adapter: BinanceAdapter, routes: BinanceRoutes, clock: Clock
) -> None:
    drift = error(400, -1021, "Timestamp for this request is outside of the recvWindow.")
    routes.queue("GET", "/fapi/v1/time", {"serverTime": T0}, {"serverTime": T0 + 7000})
    routes.queue("POST", "/fapi/v1/order", drift, ORDER_NEW)
    request = OrderRequest(
        symbol=ETH,
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        qty=Decimal("0.44"),
        client_id="hdt_ev1_entry_0",
        price=Decimal("2500.10"),
        tif=TimeInForce.GTX,
    )
    snap = await adapter.place_order(request)
    assert snap.client_id == "hdt_ev1_entry_0"
    first, second = routes.calls("POST", "/fapi/v1/order")
    assert int(first.params["timestamp"]) == T0
    assert int(second.params["timestamp"]) == T0 + 7000  # the re-synced server offset
    assert len(routes.calls("GET", "/fapi/v1/time")) == 2

    routes.queue("POST", "/fapi/v1/order", drift, drift, ORDER_NEW)
    with pytest.raises(OrderRejectedError) as raised:
        await adapter.place_order(request)
    assert raised.value.code == -1021
    assert len(routes.calls("POST", "/fapi/v1/order")) == 4  # one retry, never a third attempt


async def test_rate_limit_backs_off_until_retry_after(
    adapter: BinanceAdapter, routes: BinanceRoutes, clock: Clock
) -> None:
    routes.always("GET", "/fapi/v1/openOrders", [ORDER_NEW])
    routes.queue(
        "GET",
        "/fapi/v3/positionRisk",
        error(429, -1003, "Too many requests; current limit is 2400 per minute.", {"Retry-After": "7"}),
    )
    with pytest.raises(RateLimitedError) as raised:
        await adapter.positions()
    assert raised.value.retry_after_s == 7.0
    sent = len(routes.sent)

    clock.now += 6
    with pytest.raises(RateLimitedError):
        await adapter.open_orders()
    assert len(routes.sent) == sent  # nothing reaches the exchange during the back-off

    clock.now += 1.5
    assert [o.client_id for o in await adapter.open_orders()] == ["hdt_ev1_entry_0"]


async def test_ip_ban_stops_every_request_until_it_expires(
    adapter: BinanceAdapter, routes: BinanceRoutes, clock: Clock
) -> None:
    routes.always("GET", "/fapi/v1/openOrders", [])
    routes.queue(
        "POST",
        "/fapi/v1/order",
        error(418, -1003, "Way too many requests; IP banned.", {"Retry-After": "300"}),
    )
    request = OrderRequest(
        symbol=ETH, side=OrderSide.SELL, order_type=OrderType.MARKET, qty=Decimal("0.44"), client_id="c1"
    )
    with pytest.raises(IpBannedError) as raised:
        await adapter.place_order(request)
    assert raised.value.http_status == 418
    assert not isinstance(raised.value, OrderRejectedError | UnknownOrderStateError)
    sent = len(routes.sent)

    clock.now += 299
    for call in (adapter.open_orders, adapter.exchange_filters, lambda: adapter.place_order(request)):
        with pytest.raises(IpBannedError):
            await call()
    assert len(routes.sent) == sent

    clock.now += 2
    assert await adapter.open_orders() == []


@pytest.mark.parametrize(
    ("reply", "expected"),
    [
        (
            error(503, -1000, "Unknown error, please check your request or try again later."),
            UnknownOrderStateError,
        ),
        (error(502, -1000, "Bad gateway"), UnknownOrderStateError),
        (httpx.ReadTimeout("slow"), UnknownOrderStateError),
        (error(400, -2019, "Margin is insufficient."), OrderRejectedError),
        (error(503, -1008, "Server is currently overloaded, please try again."), ExchangeError),
    ],
)
async def test_mutating_failures_say_whether_the_order_may_exist(
    adapter: BinanceAdapter, routes: BinanceRoutes, reply: Any, expected: type[Exception]
) -> None:
    def respond(_request: httpx.Request) -> httpx.Response:
        if isinstance(reply, Exception):
            raise reply
        assert isinstance(reply, httpx.Response)
        return reply

    routes.queue("POST", "/fapi/v1/order", respond)
    request = OrderRequest(
        symbol=ETH, side=OrderSide.SELL, order_type=OrderType.MARKET, qty=Decimal("0.44"), client_id="c1"
    )
    with pytest.raises(ExchangeError) as raised:
        await adapter.place_order(request)
    assert type(raised.value) is expected


# ---------------------------------------------------------------------------- user data stream frames


def test_order_update_frames_map_to_order_events_with_their_fill() -> None:
    new = parse_user_event(WS_ORDER_NEW)
    assert isinstance(new, OrderEvent)
    assert new.fill is None
    assert (new.order.client_id, new.order.order_id, new.order.status, new.order.tif) == (
        "hdt_ev1_entry_0",
        8389765519,
        "NEW",
        "GTX",
    )
    assert (new.order.qty, new.order.price, new.order.avg_price) == (
        Decimal("0.440"),
        Decimal("2500.10"),
        None,
    )

    partial = parse_user_event(WS_ORDER_PARTIAL)
    assert isinstance(partial, OrderEvent)
    assert partial.fill is not None
    assert (partial.order.status, partial.order.executed_qty) == ("PARTIALLY_FILLED", Decimal("0.150"))
    fill = partial.fill
    assert (fill.trade_id, fill.order_id, fill.client_id, fill.side) == (
        4011001,
        8389765519,
        "hdt_ev1_entry_0",
        OrderSide.BUY,
    )
    assert (fill.price, fill.qty, fill.fee, fill.fee_asset) == (
        Decimal("2500.10"),
        Decimal("0.150"),
        Decimal("0.07500300"),
        "USDT",
    )
    assert (fill.maker, fill.realized_pnl, fill.time) == (True, Decimal(0), at(T0 + 3000))

    canceled = parse_user_event(WS_ORDER_CANCELED_NO_FEE)
    assert isinstance(canceled, OrderEvent)
    assert canceled.fill is None
    assert (canceled.order.status, canceled.order.executed_qty) == ("CANCELED", Decimal("0.150"))


def test_algo_update_frames_agree_with_the_rest_view() -> None:
    new = parse_user_event(WS_ALGO_NEW)
    triggered = parse_user_event(WS_ALGO_TRIGGERED)
    rejected = parse_user_event(WS_ALGO_REJECTED)
    assert isinstance(new, AlgoEvent)
    assert isinstance(triggered, AlgoEvent)
    assert isinstance(rejected, AlgoEvent)
    assert new.algo.triggered_order_id is None  # "ai": "" before the trigger
    assert rejected.algo.status == "REJECTED"
    rest = parse_algo(ALGO_TRIGGERED)
    ws = triggered.algo
    fields = ("symbol", "client_algo_id", "algo_id", "side", "order_type", "status", "trigger_price", "qty")
    assert [getattr(ws, f) for f in fields] == [getattr(rest, f) for f in fields]
    assert (ws.triggered_order_id, ws.reduce_only, ws.close_position) == (8389766001, True, False)
    assert ws.updated_at == at(T0 + 60_000)


def test_account_update_carries_the_usdt_wallet_only() -> None:
    assert parse_user_event(WS_ACCOUNT_UPDATE) == AccountUpdate(Decimal("4999.84499700"))
    assert parse_user_event(WS_ACCOUNT_UPDATE_NO_USDT) == AccountUpdate(None)


def test_expired_listen_key_restarts_the_stream_and_other_frames_are_ignored() -> None:
    with pytest.raises(StreamRestartError):
        parse_user_event(WS_LISTEN_KEY_EXPIRED)
    assert parse_user_event(WS_TRADE_LITE) is None
    assert parse_user_event(["not", "an", "event"]) is None
