"""Binance USD-M adapter (live `fapi.binance.com` or demo `demo-fapi.binance.com`).

Regular orders: `POST /fapi/v1/order` (LIMIT GTX / IOC, MARKET reduceOnly), `newClientOrderId`.
Conditional orders: `POST /fapi/v1/algoOrder` (`algoType=CONDITIONAL`, `triggerPrice`, `clientAlgoId`);
since 2025-12-09 `/fapi/v1/order` rejects STOP_MARKET / TAKE_PROFIT_MARKET with -4120. Untriggered algo
orders cannot be modified: callers place the new one first, then cancel the old one.
Reads: `GET /fapi/v3/positionRisk`, `/fapi/v3/account`, `/fapi/v1/openOrders`, `/fapi/v1/openAlgoOrders`,
`/fapi/v1/userTrades`, `/fapi/v1/income`. Startup check: `GET /fapi/v1/positionSide/dual` must be One-way.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Final

from hdt.contracts.common import Account, OrderSide, OrderType
from hdt.core.clock import utcnow
from hdt.execution.adapter_base import (
    AlgoRequest,
    AlgoSnapshot,
    Balance,
    CashFlow,
    ExchangeAdapter,
    ExchangeError,
    ExchangePosition,
    OrderRejectedError,
    OrderRequest,
    OrderSnapshot,
    PositionModeError,
    TradeFill,
)
from hdt.execution.binance_client import BinanceClient
from hdt.execution.filters import SymbolFilters, parse_exchange_filters, parse_rate_limits
from hdt.settings.ceilings import LEVERAGE_MAX, MARGIN_TYPE

log = logging.getLogger(__name__)

MARGIN_ASSET: Final[str] = "USDT"
NO_CHANGE_MARGIN_TYPE: Final[int] = -4046
ORDER_NOT_FOUND: Final[frozenset[int]] = frozenset({-2013, -2011})
ALGO_NOT_FOUND: Final[frozenset[int]] = frozenset({-2011, -2013, -4126, -4127})
TRADE_INCOME: Final[frozenset[str]] = frozenset({"REALIZED_PNL", "COMMISSION"})
INCOME_OVERLAP_MS: Final[int] = 10 * 60 * 1000
"""The next income scan starts this far behind the newest row already read (any type): a row the exchange
lists a little late is still picked up, and the flow ids deduplicate the overlap."""
PER_SYMBOL_MARK_MAX: Final[int] = 5


def ms_to_dt(value: Any) -> datetime:
    try:
        return datetime.fromtimestamp(int(value) / 1000, tz=UTC)
    except (TypeError, ValueError):
        return utcnow()


def dec(value: Any, default: str = "0") -> Decimal:
    if value is None or value == "":
        return Decimal(default)
    return Decimal(str(value))


def opt_dec(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    result = Decimal(str(value))
    return result if result != 0 else None


def parse_order(data: dict[str, Any]) -> OrderSnapshot:
    avg = opt_dec(data.get("avgPrice"))
    return OrderSnapshot(
        symbol=str(data["symbol"]),
        client_id=str(data.get("clientOrderId", "")),
        order_id=int(data["orderId"]) if data.get("orderId") is not None else None,
        side=OrderSide(str(data["side"])),
        order_type=str(data.get("type") or data.get("origType") or ""),
        tif=str(data["timeInForce"]) if data.get("timeInForce") else None,
        status=str(data["status"]),
        qty=dec(data.get("origQty")),
        executed_qty=dec(data.get("executedQty")),
        avg_price=avg,
        price=opt_dec(data.get("price")),
        reduce_only=bool(data.get("reduceOnly", False)),
        updated_at=ms_to_dt(data.get("updateTime") or data.get("time")),
    )


def parse_algo(data: dict[str, Any]) -> AlgoSnapshot:
    actual = data.get("actualOrderId")
    return AlgoSnapshot(
        symbol=str(data["symbol"]),
        client_algo_id=str(data.get("clientAlgoId", "")),
        algo_id=int(data["algoId"]) if data.get("algoId") is not None else None,
        side=OrderSide(str(data["side"])),
        order_type=str(data.get("orderType") or data.get("type") or ""),
        status=str(data.get("algoStatus") or data.get("status") or ""),
        trigger_price=dec(data.get("triggerPrice")),
        qty=opt_dec(data.get("quantity")),
        close_position=bool(data.get("closePosition", False)),
        reduce_only=bool(data.get("reduceOnly", False)),
        triggered_order_id=int(str(actual)) if actual not in (None, "", 0, "0") else None,
        updated_at=ms_to_dt(data.get("updateTime") or data.get("createTime")),
    )


def parse_trade(data: dict[str, Any], client_ids: dict[int, str]) -> TradeFill:
    order_id = int(data["orderId"])
    return TradeFill(
        symbol=str(data["symbol"]),
        trade_id=int(data["id"]),
        order_id=order_id,
        client_id=client_ids.get(order_id, ""),
        side=OrderSide(str(data["side"])),
        price=dec(data["price"]),
        qty=dec(data["qty"]),
        fee=dec(data.get("commission")),
        fee_asset=str(data.get("commissionAsset", MARGIN_ASSET)),
        maker=bool(data.get("maker")) if data.get("maker") is not None else None,
        realized_pnl=dec(data.get("realizedPnl")),
        time=ms_to_dt(data.get("time")),
    )


def parse_position(data: dict[str, Any], margin_types: dict[str, str]) -> ExchangePosition | None:
    qty = dec(data.get("positionAmt"))
    if qty == 0:
        return None
    symbol = str(data["symbol"])
    notional = abs(dec(data.get("notional")))
    initial = dec(data.get("initialMargin"))
    isolated_margin = dec(data.get("isolatedMargin"))
    margin = isolated_margin if isolated_margin > 0 else initial
    leverage = round(notional / margin) if margin > 0 else 1
    margin_type = margin_types.get(symbol) or ("ISOLATED" if isolated_margin > 0 else "CROSSED")
    return ExchangePosition(
        symbol=symbol,
        qty=qty,
        entry_price=dec(data.get("entryPrice")),
        mark_price=dec(data.get("markPrice")),
        unrealized_pnl=dec(data.get("unRealizedProfit")),
        leverage=max(1, min(leverage, 125)),
        liquidation_price=opt_dec(data.get("liquidationPrice")),
        margin_type="ISOLATED" if margin_type.upper().startswith("ISOLATED") else "CROSSED",
    )


class BinanceAdapter(ExchangeAdapter):
    fill_model = "exchange"

    def __init__(self, account: Account, client: BinanceClient) -> None:
        if account is Account.PAPER:
            raise ValueError("the paper namespace uses PaperAdapter")
        self.account = account
        self.client = client
        self._prepared: dict[str, int] = {}
        self._filters: dict[str, SymbolFilters] = {}
        self._margin_types: dict[str, str] = {}
        # (newest cash flow returned, newest income row of any type read) by the last complete income scan, ms
        self._income_scan: tuple[int | None, int | None] | None = None

    async def aclose(self) -> None:
        await self.client.aclose()

    def banned_until(self) -> float | None:
        return self.client.banned_until()

    async def exchange_filters(self) -> dict[str, SymbolFilters]:
        data = await self.client.public("GET", "/fapi/v1/exchangeInfo")
        self.client.rate.limits.update(parse_rate_limits(data))
        self._filters = parse_exchange_filters(data)
        return self._filters

    async def ensure_one_way(self) -> None:
        data = await self.client.signed("GET", "/fapi/v1/positionSide/dual")
        if bool(data.get("dualSidePosition")):
            raise PositionModeError(
                f"{self.account.value} account is in Hedge Mode; switch to One-way (reduceOnly needs it)"
            )

    async def prepare_symbol(self, symbol: str, leverage: int) -> None:
        if not 1 <= leverage <= LEVERAGE_MAX:
            raise ValueError(f"leverage {leverage} outside [1, {LEVERAGE_MAX}]")
        if self._prepared.get(symbol) == leverage:
            return
        try:
            await self.client.signed(
                "POST", "/fapi/v1/marginType", {"symbol": symbol, "marginType": MARGIN_TYPE}
            )
        except OrderRejectedError as exc:
            if exc.code != NO_CHANGE_MARGIN_TYPE:
                raise
        await self.client.signed("POST", "/fapi/v1/leverage", {"symbol": symbol, "leverage": leverage})
        self._prepared[symbol] = leverage
        self._margin_types[symbol] = MARGIN_TYPE

    async def place_order(self, request: OrderRequest) -> OrderSnapshot:
        params: dict[str, Any] = {
            "symbol": request.symbol,
            "side": request.side.value,
            "type": request.order_type.value,
            "quantity": _plain(request.qty),
            "newClientOrderId": request.client_id,
            "newOrderRespType": "RESULT",
        }
        if request.order_type is OrderType.LIMIT:
            if request.price is None or request.tif is None:
                raise ValueError("LIMIT needs price and tif")
            params["price"] = _plain(request.price)
            params["timeInForce"] = request.tif.value
        if request.reduce_only:
            params["reduceOnly"] = True
        return parse_order(await self.client.signed("POST", "/fapi/v1/order", params))

    async def query_order(self, symbol: str, client_id: str) -> OrderSnapshot | None:
        try:
            data = await self.client.signed(
                "GET", "/fapi/v1/order", {"symbol": symbol, "origClientOrderId": client_id}
            )
        except ExchangeError as exc:
            if exc.code in ORDER_NOT_FOUND:
                return None
            raise
        return parse_order(data)

    async def cancel_order(self, symbol: str, client_id: str) -> OrderSnapshot | None:
        try:
            data = await self.client.signed(
                "DELETE", "/fapi/v1/order", {"symbol": symbol, "origClientOrderId": client_id}
            )
        except OrderRejectedError as exc:
            if exc.code in ORDER_NOT_FOUND:
                # Already final (filled / cancelled / expired) or never created: read the truth.
                return await self.query_order(symbol, client_id)
            raise
        return parse_order(data)

    async def place_algo(self, request: AlgoRequest) -> AlgoSnapshot:
        params: dict[str, Any] = {
            "algoType": "CONDITIONAL",
            "symbol": request.symbol,
            "side": request.side.value,
            "type": request.order_type.value,
            "triggerPrice": _plain(request.trigger_price),
            "workingType": request.working_type,
            "clientAlgoId": request.client_algo_id,
            "newOrderRespType": "RESULT",
        }
        if request.close_position:
            params["closePosition"] = True
        else:
            if request.qty is None:
                raise ValueError("algo order needs qty or close_position")
            params["quantity"] = _plain(request.qty)
            if request.reduce_only:
                params["reduceOnly"] = True
        return parse_algo(await self.client.signed("POST", "/fapi/v1/algoOrder", params))

    async def query_algo(self, client_algo_id: str) -> AlgoSnapshot | None:
        try:
            data = await self.client.signed("GET", "/fapi/v1/algoOrder", {"clientAlgoId": client_algo_id})
        except ExchangeError as exc:
            if exc.code in ALGO_NOT_FOUND or exc.http_status == 404:
                return None
            raise
        if not isinstance(data, dict) or not data.get("symbol"):
            return None
        return parse_algo(data)

    async def cancel_algo(self, client_algo_id: str) -> AlgoSnapshot | None:
        try:
            await self.client.signed("DELETE", "/fapi/v1/algoOrder", {"clientAlgoId": client_algo_id})
        except OrderRejectedError as exc:
            if exc.code not in ALGO_NOT_FOUND:
                raise
        return await self.query_algo(client_algo_id)

    async def cancel_all_algos(self, symbol: str) -> None:
        await self.client.signed("DELETE", "/fapi/v1/algoOpenOrders", {"symbol": symbol})

    async def open_orders(self) -> list[OrderSnapshot]:
        data = await self.client.signed("GET", "/fapi/v1/openOrders")
        return [parse_order(item) for item in data]

    async def open_algo_orders(self) -> list[AlgoSnapshot]:
        data = await self.client.signed("GET", "/fapi/v1/openAlgoOrders")
        items = data.get("orders", data) if isinstance(data, dict) else data
        return [parse_algo(item) for item in items]

    async def positions(self) -> list[ExchangePosition]:
        data = await self.client.signed("GET", "/fapi/v3/positionRisk")
        out = [parse_position(item, self._margin_types) for item in data]
        return [p for p in out if p is not None]

    async def user_trades(
        self, symbol: str, from_id: int | None, start: datetime | None, known: Mapping[int, str] | None = None
    ) -> list[TradeFill]:
        params: dict[str, Any] = {"symbol": symbol, "limit": 1000}
        if from_id is not None:
            params["fromId"] = from_id
        elif start is not None:
            params["startTime"] = int(start.timestamp() * 1000)
        data = await self.client.signed("GET", "/fapi/v1/userTrades", params)
        client_ids = dict(known or {})
        for order_id in sorted({int(item["orderId"]) for item in data} - set(client_ids)):
            order = await self.client.signed("GET", "/fapi/v1/order", {"symbol": symbol, "orderId": order_id})
            client_ids[order_id] = str(order.get("clientOrderId", ""))
        return [parse_trade(item, client_ids) for item in data]

    async def balance(self) -> Balance:
        data = await self.client.signed("GET", "/fapi/v3/account")
        wallet = unrealized = available = Decimal(0)
        found = False
        for asset in data.get("assets", []):
            if asset.get("asset") == MARGIN_ASSET:
                wallet = dec(asset.get("walletBalance"))
                unrealized = dec(asset.get("unrealizedProfit"))
                available = dec(asset.get("availableBalance"))
                found = True
        if not found:
            wallet = dec(data.get("totalWalletBalance"))
            unrealized = dec(data.get("totalUnrealizedProfit"))
            available = dec(data.get("availableBalance"))
        return Balance(
            wallet_balance=wallet, unrealized_pnl=unrealized, equity=wallet + unrealized, available=available
        )

    async def cash_flows(self, start: datetime) -> list[CashFlow]:
        """Income other than realized PnL and commissions (those come with the fills) from `start` on.

        Once the caller recorded what the last complete scan returned (`start` reaches its newest cash
        flow), the scan starts no earlier than `INCOME_OVERLAP_MS` behind the newest income row of any type
        read, so the trade income since the last cash flow is not paged again on every cycle."""
        out: list[CashFlow] = []
        cursor = int(start.timestamp() * 1000)
        if self._income_scan is not None:
            flow_ms, seen_ms = self._income_scan
            if seen_ms is not None and (flow_ms is None or cursor >= flow_ms):
                cursor = max(cursor, seen_ms - INCOME_OVERLAP_MS)
        newest_flow: int | None = None
        newest_row = self._income_scan[1] if self._income_scan is not None else None
        while True:
            data = await self.client.signed("GET", "/fapi/v1/income", {"startTime": cursor, "limit": 1000})
            for item in data:
                time_ms = int(item["time"])
                newest_row = time_ms if newest_row is None else max(newest_row, time_ms)
                kind = str(item.get("incomeType", ""))
                if kind in TRADE_INCOME:
                    continue  # realized PnL and commissions come with the fills
                newest_flow = time_ms if newest_flow is None else max(newest_flow, time_ms)
                out.append(
                    CashFlow(
                        flow_id=f"{kind}:{item.get('tranId')}:{item.get('symbol') or ''}",
                        kind=kind,
                        symbol=str(item["symbol"]) if item.get("symbol") else None,
                        amount=dec(item.get("income")),
                        asset=str(item.get("asset", MARGIN_ASSET)),
                        time=ms_to_dt(time_ms),
                    )
                )
            if len(data) < 1000:
                self._income_scan = (newest_flow, newest_row)  # only a complete scan moves the cursor
                return out
            cursor = int(data[-1]["time"]) + 1

    async def mark_prices(self, symbols: Sequence[str]) -> dict[str, Decimal]:
        """Public `GET /fapi/v1/premiumIndex`: per symbol (weight 1) for a few, else all at once (10)."""
        wanted = sorted(set(symbols))
        if not wanted:
            return {}
        items: list[Any]
        if len(wanted) <= PER_SYMBOL_MARK_MAX:
            items = [await self.client.public("GET", "/fapi/v1/premiumIndex", {"symbol": s}) for s in wanted]
        else:
            data = await self.client.public("GET", "/fapi/v1/premiumIndex")
            items = list(data) if isinstance(data, list) else []
        out: dict[str, Decimal] = {}
        for item in items:
            if isinstance(item, dict) and item.get("symbol") in wanted and item.get("markPrice"):
                price = dec(item["markPrice"])
                if price > 0:
                    out[str(item["symbol"])] = price
        return out


def _plain(value: Decimal) -> str:
    return format(value.normalize(), "f")
