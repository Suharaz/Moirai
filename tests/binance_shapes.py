"""Binance USD-M payloads in the shapes the exchange sends them, and an httpx transport that serves them.

Field sets follow the Binance USD-M REST and user data stream documentation (order `RESULT` responses,
the algo service `algoOrder` / `openAlgoOrders`, `positionRisk` v3, `account` v3, `userTrades`, `income`,
`ORDER_TRADE_UPDATE`, `ALGO_UPDATE`, `ACCOUNT_UPDATE`), including the fields execution ignores, so a
mapping test fails if the adapter starts depending on an absent field or misreads a present one.

`BinanceRoutes` is an `httpx.MockTransport` handler: queue responses per `(method, path)` and read back
every request (with its decoded query) afterwards. `/fapi/v1/time` answers the transport's own clock
unless a response is queued for it.
"""

from __future__ import annotations

import json
import time
from collections import defaultdict, deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from urllib.parse import parse_qsl

import httpx

from hdt.execution.adapter_base import AlgoEvent, OrderEvent
from hdt.execution.binance_client import BinanceClient
from hdt.execution.filters import SymbolFilters
from hdt.execution.paper_venue import VenueEvent

BASE_URL = "https://demo-fapi.test"
API_KEY = "k" * 64
API_SECRET = "s" * 64
T0 = 1_790_000_000_000  # ms, 2026-09-21


def _filters(tick: str, step: str, min_qty: str, max_qty: str, min_notional: str) -> list[dict[str, Any]]:
    return [
        {
            "filterType": "PRICE_FILTER",
            "minPrice": tick,
            "maxPrice": "1000000",
            "tickSize": tick,
        },
        {"filterType": "LOT_SIZE", "stepSize": step, "minQty": min_qty, "maxQty": max_qty},
        {"filterType": "MARKET_LOT_SIZE", "stepSize": step, "minQty": min_qty, "maxQty": max_qty},
        {"filterType": "MAX_NUM_ORDERS", "limit": 200},
        {"filterType": "MAX_NUM_ALGO_ORDERS", "limit": 10},
        {"filterType": "MIN_NOTIONAL", "notional": min_notional},
        {"filterType": "PERCENT_PRICE", "multiplierUp": "1.0500", "multiplierDown": "0.9500"},
    ]


def _symbol(symbol: str, status: str, filters: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "pair": symbol,
        "contractType": "PERPETUAL",
        "status": status,
        "baseAsset": symbol.removesuffix("USDT"),
        "quoteAsset": "USDT",
        "marginAsset": "USDT",
        "pricePrecision": 2,
        "quantityPrecision": 3,
        "orderTypes": ["LIMIT", "MARKET", "STOP", "STOP_MARKET", "TAKE_PROFIT", "TAKE_PROFIT_MARKET"],
        "timeInForce": ["GTC", "IOC", "FOK", "GTX", "GTD"],
        "filters": filters,
    }


def exchange_info(filters: Mapping[str, SymbolFilters]) -> dict[str, Any]:
    """`GET /fapi/v1/exchangeInfo` listing exactly `filters` (the fake exchange's symbols)."""
    return {
        "timezone": "UTC",
        "serverTime": T0,
        "symbols": [
            _symbol(
                f.symbol,
                f.status,
                _filters(
                    format(f.tick_size, "f"),
                    format(f.step_size, "f"),
                    format(f.min_qty, "f"),
                    format(f.max_qty, "f"),
                    format(f.min_notional, "f"),
                ),
            )
            for f in filters.values()
        ],
    }


EXCHANGE_INFO: dict[str, Any] = {
    "timezone": "UTC",
    "serverTime": T0,
    "futuresType": "U_MARGINED",
    "rateLimits": [
        {"rateLimitType": "REQUEST_WEIGHT", "interval": "MINUTE", "intervalNum": 1, "limit": 2400},
        {"rateLimitType": "ORDERS", "interval": "MINUTE", "intervalNum": 1, "limit": 1200},
        {"rateLimitType": "ORDERS", "interval": "SECOND", "intervalNum": 10, "limit": 300},
    ],
    "assets": [{"asset": "USDT", "marginAvailable": True, "autoAssetExchange": "-10000"}],
    "symbols": [
        _symbol("ETHUSDT", "TRADING", _filters("0.01", "0.001", "0.001", "10000", "20")),
        _symbol("SOLUSDT", "TRADING", _filters("0.01", "1", "1", "100000", "5")),
        _symbol("BTCUSDT", "TRADING", _filters("0.1", "0.001", "0.001", "1000", "100")),
        _symbol("OLDUSDT", "SETTLING", _filters("0.0001", "1", "1", "100000", "5")),
    ],
}

ORDER_NEW: dict[str, Any] = {  # POST /fapi/v1/order, newOrderRespType=RESULT, resting GTX
    "orderId": 8389765519,
    "symbol": "ETHUSDT",
    "status": "NEW",
    "clientOrderId": "hdt_ev1_entry_0",
    "price": "2500.10",
    "avgPrice": "0.00",
    "origQty": "0.440",
    "executedQty": "0.000",
    "cumQty": "0.000",
    "cumQuote": "0.00000",
    "timeInForce": "GTX",
    "type": "LIMIT",
    "reduceOnly": False,
    "closePosition": False,
    "side": "BUY",
    "positionSide": "BOTH",
    "stopPrice": "0.00",
    "workingType": "CONTRACT_PRICE",
    "priceProtect": False,
    "origType": "LIMIT",
    "priceMatch": "NONE",
    "selfTradePreventionMode": "EXPIRE_MAKER",
    "goodTillDate": 0,
    "updateTime": T0,
}

ORDER_FILLED: dict[str, Any] = {  # GET /fapi/v1/order of an IOC that filled at two prices
    **ORDER_NEW,
    "orderId": 8389765520,
    "clientOrderId": "hdt_ev1_entry_ioc_0",
    "status": "FILLED",
    "price": "2505.00",
    "avgPrice": "2500.55",
    "executedQty": "0.440",
    "cumQty": "0.440",
    "cumQuote": "1100.24200",
    "timeInForce": "IOC",
    "time": T0 + 5,
    "updateTime": T0 + 7,
}

ALGO_NEW: dict[str, Any] = {  # POST /fapi/v1/algoOrder, CONDITIONAL STOP_MARKET with a quantity
    "algoId": 2146760,
    "clientAlgoId": "hdt_ev1_sl_0",
    "algoType": "CONDITIONAL",
    "orderType": "STOP_MARKET",
    "symbol": "ETHUSDT",
    "side": "SELL",
    "positionSide": "BOTH",
    "timeInForce": "GTC",
    "quantity": "0.440",
    "algoStatus": "NEW",
    "triggerPrice": "2450.00",
    "price": "0.00",
    "icebergQuantity": None,
    "selfTradePreventionMode": "EXPIRE_MAKER",
    "workingType": "MARK_PRICE",
    "priceMatch": "NONE",
    "closePosition": False,
    "priceProtect": False,
    "reduceOnly": True,
    "activatePrice": "",
    "callbackRate": "",
    "createTime": T0 + 10,
    "updateTime": T0 + 10,
    "triggerTime": 0,
    "goodTillDate": 0,
}

ALGO_TRIGGERED: dict[str, Any] = {  # GET /fapi/v1/algoOrder after the trigger
    **ALGO_NEW,
    "algoStatus": "TRIGGERED",
    "actualOrderId": "8389766001",
    "actualPrice": "2449.80",
    "actualQty": "0.440",
    "triggerTime": T0 + 60_000,
    "updateTime": T0 + 60_001,
}

ALGO_CLOSE_POSITION: dict[str, Any] = {  # the hedge book's closePosition stop (no quantity)
    **ALGO_NEW,
    "algoId": 2146761,
    "clientAlgoId": "hdt_hb1_sl_0",
    "symbol": "BTCUSDT",
    "side": "BUY",
    "quantity": "",
    "closePosition": True,
    "reduceOnly": False,
    "triggerPrice": "70000.0",
    "actualOrderId": "",
}

POSITION_RISK: list[dict[str, Any]] = [  # GET /fapi/v3/positionRisk
    {
        "symbol": "ETHUSDT",
        "positionSide": "BOTH",
        "positionAmt": "0.440",
        "entryPrice": "2500.55",
        "breakEvenPrice": "2501.80",
        "markPrice": "2510.00000000",
        "unRealizedProfit": "4.15800000",
        "liquidationPrice": "1260.43",
        "isolatedMargin": "0",
        "notional": "1104.40000000",
        "marginAsset": "USDT",
        "isolatedWallet": "0",
        "initialMargin": "552.20000000",
        "maintMargin": "4.41760000",
        "positionInitialMargin": "552.20000000",
        "openOrderInitialMargin": "0",
        "adl": 1,
        "bidNotional": "0",
        "askNotional": "0",
        "updateTime": T0 + 20,
    },
    {
        "symbol": "BTCUSDT",
        "positionSide": "BOTH",
        "positionAmt": "-0.010",
        "entryPrice": "71000.0",
        "breakEvenPrice": "70970.0",
        "markPrice": "70900.00000000",
        "unRealizedProfit": "1.00000000",
        "liquidationPrice": "0",
        "isolatedMargin": "236.33333333",
        "notional": "-709.00000000",
        "marginAsset": "USDT",
        "isolatedWallet": "236.00000000",
        "initialMargin": "236.33333333",
        "maintMargin": "2.83600000",
        "positionInitialMargin": "236.33333333",
        "openOrderInitialMargin": "0",
        "adl": 2,
        "bidNotional": "0",
        "askNotional": "0",
        "updateTime": T0 + 21,
    },
    {
        "symbol": "SOLUSDT",
        "positionSide": "BOTH",
        "positionAmt": "0",
        "entryPrice": "0.0",
        "breakEvenPrice": "0.0",
        "markPrice": "150.00000000",
        "unRealizedProfit": "0.00000000",
        "liquidationPrice": "0",
        "isolatedMargin": "0",
        "notional": "0",
        "marginAsset": "USDT",
        "isolatedWallet": "0",
        "initialMargin": "0",
        "maintMargin": "0",
        "positionInitialMargin": "0",
        "openOrderInitialMargin": "0",
        "adl": 0,
        "bidNotional": "0",
        "askNotional": "0",
        "updateTime": 0,
    },
]

ACCOUNT_V3: dict[str, Any] = {  # GET /fapi/v3/account (multi-asset wallet, USDT is the margin asset)
    "totalInitialMargin": "788.53333333",
    "totalMaintMargin": "7.25360000",
    "totalWalletBalance": "15123.45000000",
    "totalUnrealizedProfit": "5.15800000",
    "totalMarginBalance": "15128.60800000",
    "totalPositionInitialMargin": "788.53333333",
    "totalOpenOrderInitialMargin": "0.00000000",
    "totalCrossWalletBalance": "14887.45000000",
    "totalCrossUnPnl": "4.15800000",
    "availableBalance": "14340.07466667",
    "maxWithdrawAmount": "14340.07466667",
    "assets": [
        {
            "asset": "USDT",
            "walletBalance": "5000.12000000",
            "unrealizedProfit": "5.15800000",
            "marginBalance": "5005.27800000",
            "maintMargin": "7.25360000",
            "initialMargin": "788.53333333",
            "positionInitialMargin": "788.53333333",
            "openOrderInitialMargin": "0.00000000",
            "crossWalletBalance": "4764.12000000",
            "crossUnPnl": "4.15800000",
            "availableBalance": "4216.74466667",
            "maxWithdrawAmount": "4216.74466667",
            "updateTime": T0 + 30,
        },
        {
            "asset": "USDC",
            "walletBalance": "10123.33000000",
            "unrealizedProfit": "0.00000000",
            "marginBalance": "10123.33000000",
            "maintMargin": "0.00000000",
            "initialMargin": "0.00000000",
            "positionInitialMargin": "0.00000000",
            "openOrderInitialMargin": "0.00000000",
            "crossWalletBalance": "10123.33000000",
            "crossUnPnl": "0.00000000",
            "availableBalance": "10123.33000000",
            "maxWithdrawAmount": "10123.33000000",
            "updateTime": T0 + 30,
        },
    ],
    "positions": [
        {
            "symbol": "ETHUSDT",
            "positionSide": "BOTH",
            "positionAmt": "0.440",
            "unrealizedProfit": "4.15800000",
            "isolatedMargin": "0",
            "notional": "1104.40000000",
            "isolatedWallet": "0",
            "initialMargin": "552.20000000",
            "maintMargin": "4.41760000",
            "updateTime": T0 + 20,
        }
    ],
}

USER_TRADES: list[dict[str, Any]] = [  # GET /fapi/v1/userTrades?symbol=ETHUSDT
    {
        "buyer": True,
        "commission": "0.27500000",
        "commissionAsset": "USDT",
        "id": 698759,
        "maker": False,
        "orderId": 8389765520,
        "price": "2500.00",
        "qty": "0.220",
        "quoteQty": "550.00000",
        "realizedPnl": "0",
        "side": "BUY",
        "positionSide": "BOTH",
        "symbol": "ETHUSDT",
        "time": T0 + 5,
    },
    {
        "buyer": True,
        "commission": "0.00012000",
        "commissionAsset": "BNB",
        "id": 698760,
        "maker": False,
        "orderId": 8389765520,
        "price": "2501.10",
        "qty": "0.220",
        "quoteQty": "550.24200",
        "realizedPnl": "0",
        "side": "BUY",
        "positionSide": "BOTH",
        "symbol": "ETHUSDT",
        "time": T0 + 6,
    },
    {  # the STOP's market order: execution never sent it, so its client id comes from GET /fapi/v1/order
        "buyer": False,
        "commission": "0.53900000",
        "commissionAsset": "USDT",
        "id": 698801,
        "maker": False,
        "orderId": 8389766001,
        "price": "2449.80",
        "qty": "0.440",
        "quoteQty": "1077.91200",
        "realizedPnl": "-22.08200000",
        "side": "SELL",
        "positionSide": "BOTH",
        "symbol": "ETHUSDT",
        "time": T0 + 60_002,
    },
]

INCOME: list[dict[str, Any]] = [  # GET /fapi/v1/income
    {
        "symbol": "",
        "incomeType": "TRANSFER",
        "income": "1000.00000000",
        "asset": "USDT",
        "info": "TRANSFER",
        "time": T0 - 3_600_000,
        "tranId": 9689322392,
        "tradeId": "",
    },
    {
        "symbol": "ETHUSDT",
        "incomeType": "COMMISSION",
        "income": "-0.27500000",
        "asset": "USDT",
        "info": "COMMISSION",
        "time": T0 + 5,
        "tranId": 9689322393,
        "tradeId": "698759",
    },
    {
        "symbol": "ETHUSDT",
        "incomeType": "REALIZED_PNL",
        "income": "-22.08200000",
        "asset": "USDT",
        "info": "REALIZED_PNL",
        "time": T0 + 60_002,
        "tranId": 9689322394,
        "tradeId": "698801",
    },
    {
        "symbol": "ETHUSDT",
        "incomeType": "FUNDING_FEE",
        "income": "-0.11040000",
        "asset": "USDT",
        "info": "FUNDING_FEE",
        "time": T0 + 28_800_000,
        "tranId": 9689322395,
        "tradeId": "",
    },
]

# ---------------------------------------------------------------------------- user data stream frames

WS_ORDER_NEW: dict[str, Any] = {
    "e": "ORDER_TRADE_UPDATE",
    "E": T0 + 1,
    "T": T0,
    "o": {
        "s": "ETHUSDT",
        "c": "hdt_ev1_entry_0",
        "S": "BUY",
        "o": "LIMIT",
        "f": "GTX",
        "q": "0.440",
        "p": "2500.10",
        "ap": "0",
        "sp": "0",
        "x": "NEW",
        "X": "NEW",
        "i": 8389765519,
        "l": "0",
        "z": "0",
        "L": "0",
        "n": "0",
        "N": "USDT",
        "T": T0,
        "t": 0,
        "b": "1100.04400",
        "a": "0",
        "m": False,
        "R": False,
        "wt": "CONTRACT_PRICE",
        "ot": "LIMIT",
        "ps": "BOTH",
        "cp": False,
        "rp": "0",
        "pP": False,
        "si": 0,
        "ss": 0,
        "V": "EXPIRE_MAKER",
        "pm": "NONE",
        "gtd": 0,
    },
}

WS_ORDER_PARTIAL: dict[str, Any] = {  # a maker partial fill of that order, commission in USDT
    "e": "ORDER_TRADE_UPDATE",
    "E": T0 + 3001,
    "T": T0 + 3000,
    "o": {
        **WS_ORDER_NEW["o"],
        "x": "TRADE",
        "X": "PARTIALLY_FILLED",
        "ap": "2500.10",
        "l": "0.150",
        "z": "0.150",
        "L": "2500.10",
        "n": "0.07500300",
        "N": "USDT",
        "T": T0 + 3000,
        "t": 4011001,
        "m": True,
        "rp": "0",
    },
}

WS_ORDER_CANCELED_NO_FEE: dict[str, Any] = {  # a cancel frame: no trade, `N` and `n` absent
    "e": "ORDER_TRADE_UPDATE",
    "E": T0 + 5001,
    "T": T0 + 5000,
    "o": {
        key: value
        for key, value in {
            **WS_ORDER_NEW["o"],
            "x": "CANCELED",
            "X": "CANCELED",
            "ap": "2500.10",
            "z": "0.150",
            "T": T0 + 5000,
        }.items()
        if key not in ("N", "n")
    },
}

WS_ALGO_NEW: dict[str, Any] = {
    "e": "ALGO_UPDATE",
    "T": T0 + 10,
    "E": T0 + 11,
    "o": {
        "caid": "hdt_ev1_sl_0",
        "aid": 2146760,
        "at": "CONDITIONAL",
        "o": "STOP_MARKET",
        "s": "ETHUSDT",
        "S": "SELL",
        "ps": "BOTH",
        "f": "GTC",
        "q": "0.440",
        "X": "NEW",
        "ai": "",
        "ap": "0.00000",
        "aq": "0.00000",
        "act": "0",
        "tp": "2450.00",
        "p": "0",
        "V": "EXPIRE_MAKER",
        "wt": "MARK_PRICE",
        "pm": "NONE",
        "cp": False,
        "pP": False,
        "R": True,
        "tt": 0,
        "gtd": 0,
    },
}

WS_ALGO_TRIGGERED: dict[str, Any] = {
    "e": "ALGO_UPDATE",
    "T": T0 + 60_000,
    "E": T0 + 60_001,
    "o": {**WS_ALGO_NEW["o"], "X": "TRIGGERED", "ai": "8389766001", "tt": T0 + 60_000},
}

WS_ALGO_REJECTED: dict[str, Any] = {
    "e": "ALGO_UPDATE",
    "T": T0 + 12,
    "E": T0 + 13,
    "o": {**WS_ALGO_NEW["o"], "X": "REJECTED", "rm": "Reduce Only reject"},
}

WS_ACCOUNT_UPDATE: dict[str, Any] = {
    "e": "ACCOUNT_UPDATE",
    "E": T0 + 3002,
    "T": T0 + 3000,
    "a": {
        "m": "ORDER",
        "B": [
            {"a": "BNB", "wb": "1.00000000", "cw": "1.00000000", "bc": "0"},
            {"a": "USDT", "wb": "4999.84499700", "cw": "4999.84499700", "bc": "0"},
        ],
        "P": [
            {
                "s": "ETHUSDT",
                "pa": "0.150",
                "ep": "2500.10",
                "bep": "2500.35",
                "cr": "0",
                "up": "0",
                "mt": "cross",
                "iw": "0",
                "ps": "BOTH",
            }
        ],
    },
}

WS_ACCOUNT_UPDATE_NO_USDT: dict[str, Any] = {
    "e": "ACCOUNT_UPDATE",
    "E": T0 + 4000,
    "T": T0 + 4000,
    "a": {"m": "FUNDING_FEE", "B": [{"a": "BNB", "wb": "0.99000000", "cw": "0.99", "bc": "0"}], "P": []},
}

WS_LISTEN_KEY_EXPIRED: dict[str, Any] = {"e": "listenKeyExpired", "E": T0 + 3_600_000, "listenKey": "L" * 60}
WS_TRADE_LITE: dict[str, Any] = {
    "e": "TRADE_LITE",
    "E": T0 + 3001,
    "T": T0 + 3000,
    "s": "ETHUSDT",
    "q": "0.150",
    "p": "2500.10",
    "m": True,
    "c": "hdt_ev1_entry_0",
    "S": "BUY",
    "L": "2500.10",
    "l": "0.150",
    "t": 4011001,
    "i": 8389765519,
}


def _ms(at: datetime) -> int:
    return int(at.timestamp() * 1000)


def ws_frame(event: VenueEvent) -> dict[str, Any]:
    """The full user-stream frame Binance sends for one exchange event (every documented field)."""
    if isinstance(event, OrderEvent):
        o, f = event.order, event.fill
        at = _ms(o.updated_at)
        body: dict[str, Any] = {
            **WS_ORDER_NEW["o"],
            "s": o.symbol,
            "c": o.client_id,
            "S": o.side.value,
            "o": o.order_type,
            "ot": o.order_type,
            "f": o.tif or "GTC",
            "q": str(o.qty),
            "p": str(o.price or 0),
            "ap": str(o.avg_price or 0),
            "x": o.status if o.status in ("NEW", "CANCELED", "EXPIRED") else "NEW",
            "X": o.status,
            "i": o.order_id,
            "z": str(o.executed_qty),
            "l": "0",
            "L": "0",
            "n": "0",
            "N": "USDT",
            "t": 0,
            "R": o.reduce_only,
            "T": at,
            "m": False,
            "rp": "0",
        }
        if f is not None:
            at = _ms(f.time)
            body.update(
                x="TRADE",
                t=f.trade_id,
                L=str(f.price),
                l=str(f.qty),
                n=str(f.fee),
                N=f.fee_asset,
                m=bool(f.maker),
                rp=str(f.realized_pnl),
                T=at,
            )
        return {"e": "ORDER_TRADE_UPDATE", "E": at + 1, "T": at, "o": body}
    assert isinstance(event, AlgoEvent)
    a = event.algo
    at = _ms(a.updated_at)
    return {
        "e": "ALGO_UPDATE",
        "T": at,
        "E": at + 1,
        "o": {
            **WS_ALGO_NEW["o"],
            "caid": a.client_algo_id,
            "aid": a.algo_id,
            "o": a.order_type,
            "s": a.symbol,
            "S": a.side.value,
            "q": "" if a.qty is None else str(a.qty),
            "X": a.status,
            "ai": str(a.triggered_order_id) if a.triggered_order_id else "",
            "tp": str(a.trigger_price),
            "cp": a.close_position,
            "R": a.reduce_only,
        },
    }


# ---------------------------------------------------------------------------- transport


@dataclass
class Sent:
    method: str
    path: str
    params: dict[str, str]
    headers: httpx.Headers


def error(status: int, code: int, msg: str, headers: Mapping[str, str] | None = None) -> httpx.Response:
    return httpx.Response(status, json={"code": code, "msg": msg}, headers=headers)


Reply = httpx.Response | dict[str, Any] | list[Any] | Callable[[httpx.Request], httpx.Response]


@dataclass
class BinanceRoutes:
    """`httpx.MockTransport` handler: queued replies per `(method, path)`, every request recorded."""

    server_offset_ms: int = 0
    replies: dict[tuple[str, str], deque[Reply]] = field(default_factory=lambda: defaultdict(deque))
    sticky: dict[tuple[str, str], Reply] = field(default_factory=dict)
    sent: list[Sent] = field(default_factory=list)

    def queue(self, method: str, path: str, *replies: Reply) -> None:
        self.replies[(method, path)].extend(replies)

    def always(self, method: str, path: str, reply: Reply) -> None:
        self.sticky[(method, path)] = reply

    def calls(self, method: str | None = None, path: str | None = None) -> list[Sent]:
        return [
            s
            for s in self.sent
            if (method is None or s.method == method) and (path is None or s.path == path)
        ]

    def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        params = dict(parse_qsl(request.url.query.decode(), keep_blank_values=True))
        self.sent.append(Sent(request.method, path, params, request.headers))
        key = (request.method, path)
        if self.replies.get(key):
            reply = self.replies[key].popleft()
        elif key in self.sticky:
            reply = self.sticky[key]
        elif key == ("GET", "/fapi/v1/time"):
            reply = {"serverTime": int(time.time() * 1000) + self.server_offset_ms}
        else:
            return error(404, -5000, f"no reply queued for {request.method} {path}")
        if callable(reply):
            return reply(request)
        if isinstance(reply, httpx.Response):
            return reply
        return httpx.Response(200, content=json.dumps(reply).encode(), headers={"x-mbx-used-weight-1m": "5"})

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self)

    def client(self, **kwargs: Any) -> BinanceClient:
        return BinanceClient(
            base_url=BASE_URL, api_key=API_KEY, api_secret=API_SECRET, transport=self.transport(), **kwargs
        )
