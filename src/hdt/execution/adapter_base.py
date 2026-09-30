"""Exchange adapter interface shared by the Binance adapter (demo-fapi / fapi) and the paper engine.

Everything above the adapter (order manager, stop guard, reconciler, kill switch) is exchange-agnostic
and works with these normalized snapshots. Errors are classified so callers never guess:
- `OrderRejectedError`: a definitive rejection (the order does not exist on the exchange);
- `UnknownOrderStateError`: HTTP 503 "Unknown error", timeouts, lost responses; the order MAY exist, so the
  caller verifies by client id before any retry;
- `RateLimitedError` (429, back off) and `IpBannedError` (418, stop the namespace and alert).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Literal, Protocol

from hdt.contracts.common import Account, OrderSide, OrderType, TimeInForce
from hdt.execution.filters import SymbolFilters

OPEN_ORDER_STATUSES = frozenset({"NEW", "PARTIALLY_FILLED"})
FINAL_ORDER_STATUSES = frozenset({"FILLED", "CANCELED", "EXPIRED", "REJECTED", "EXPIRED_IN_MATCH"})
WORKING_ALGO_STATUSES = frozenset({"NEW", "WORKING"})
FINAL_ALGO_STATUSES = frozenset({"TRIGGERED", "FINISHED", "CANCELED", "REJECTED", "EXPIRED"})
FillModel = Literal["exchange", "queue", "degraded"]

# Binance error codes the callers branch on (the paper engine raises the same codes).
CODE_WOULD_TRIGGER_IMMEDIATELY = -2021  # a conditional order whose trigger is already crossed
CODE_REDUCE_ONLY_REJECTED = -2022  # reduceOnly order with nothing to reduce
CODE_DUPLICATE_CLIENT_ID = -4116  # clientOrderId / clientAlgoId already used
CODE_GTX_WOULD_TAKE = -5022  # post-only (GTX) order would execute immediately as taker


class ExchangeError(Exception):
    def __init__(self, message: str, *, code: int | None = None, http_status: int | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.http_status = http_status


class OrderRejectedError(ExchangeError):
    """Definitive rejection: nothing was created on the exchange."""


class UnknownOrderStateError(ExchangeError):
    """The request may or may not have been executed (503, timeout): verify before retrying."""


class RateLimitedError(ExchangeError):
    """HTTP 429: back off (`retry_after_s`)."""

    def __init__(self, message: str, *, retry_after_s: float, code: int | None = None) -> None:
        super().__init__(message, code=code, http_status=429)
        self.retry_after_s = retry_after_s


class IpBannedError(ExchangeError):
    """HTTP 418: the IP is banned; stop the namespace and alert (`banned_until`: ban end, epoch seconds)."""

    def __init__(
        self,
        message: str,
        *,
        code: int | None = None,
        http_status: int | None = 418,
        banned_until: float | None = None,
    ) -> None:
        super().__init__(message, code=code, http_status=http_status)
        self.banned_until = banned_until


class PositionModeError(ExchangeError):
    """The account is in Hedge Mode (reduceOnly is unusable): execution refuses to run."""


@dataclass(frozen=True)
class OrderRequest:
    symbol: str
    side: OrderSide
    order_type: OrderType  # LIMIT or MARKET
    qty: Decimal
    client_id: str
    price: Decimal | None = None
    tif: TimeInForce | None = None
    reduce_only: bool = False


@dataclass(frozen=True)
class AlgoRequest:
    symbol: str
    side: OrderSide
    order_type: OrderType  # STOP_MARKET or TAKE_PROFIT_MARKET
    trigger_price: Decimal
    client_algo_id: str
    qty: Decimal | None = None
    close_position: bool = False
    reduce_only: bool = True
    working_type: Literal["MARK_PRICE", "CONTRACT_PRICE"] = "MARK_PRICE"


@dataclass(frozen=True)
class OrderSnapshot:
    symbol: str
    client_id: str
    order_id: int | None
    side: OrderSide
    order_type: str
    tif: str | None
    status: str
    qty: Decimal
    executed_qty: Decimal
    avg_price: Decimal | None
    price: Decimal | None
    reduce_only: bool
    updated_at: datetime

    @property
    def is_open(self) -> bool:
        return self.status in OPEN_ORDER_STATUSES


@dataclass(frozen=True)
class AlgoSnapshot:
    symbol: str
    client_algo_id: str
    algo_id: int | None
    side: OrderSide
    order_type: str
    status: str
    trigger_price: Decimal
    qty: Decimal | None
    close_position: bool
    reduce_only: bool
    triggered_order_id: int | None
    updated_at: datetime

    @property
    def is_working(self) -> bool:
        return self.status in WORKING_ALGO_STATUSES


@dataclass(frozen=True)
class TradeFill:
    symbol: str
    trade_id: int
    order_id: int | None
    client_id: str
    side: OrderSide
    price: Decimal
    qty: Decimal
    fee: Decimal
    fee_asset: str
    maker: bool | None
    realized_pnl: Decimal
    time: datetime


@dataclass(frozen=True)
class OrderEvent:
    """A regular order changed (ORDER_TRADE_UPDATE, REST query or paper engine), with its fill if any.

    `fill_model` is set by the paper engine (`queue` or `degraded`); exchange events leave it None.
    """

    order: OrderSnapshot
    fill: TradeFill | None = None
    fill_model: FillModel | None = None


@dataclass(frozen=True)
class AlgoEvent:
    """A conditional order changed (ALGO_UPDATE, REST query or paper engine)."""

    algo: AlgoSnapshot


class ExchangeEventSink(Protocol):
    """Receiver of exchange push events: the Binance user data stream or the paper engine feed it.

    Events of one type arrive in exchange order. The sink makes them durable (ledger) before returning.
    """

    async def on_order_event(self, event: OrderEvent) -> None: ...

    async def on_algo_event(self, event: AlgoEvent) -> None: ...

    async def on_account_update(self, wallet_balance: Decimal | None) -> None:
        """Balances or positions changed (ACCOUNT_UPDATE, paper funding / fills); USDT wallet if known."""
        ...


@dataclass(frozen=True)
class CashFlow:
    """A non-trade wallet change: funding, transfer, insurance, etc. (`amount` in the margin asset)."""

    flow_id: str
    kind: str
    symbol: str | None
    amount: Decimal
    asset: str
    time: datetime


@dataclass(frozen=True)
class ExchangePosition:
    symbol: str
    qty: Decimal  # signed, positive = long
    entry_price: Decimal
    mark_price: Decimal
    unrealized_pnl: Decimal
    leverage: int
    liquidation_price: Decimal | None
    margin_type: Literal["ISOLATED", "CROSSED"]


@dataclass(frozen=True)
class Balance:
    wallet_balance: Decimal  # margin asset (USDT) wallet
    unrealized_pnl: Decimal
    equity: Decimal  # margin balance = wallet + unrealized
    available: Decimal


class ExchangeAdapter(ABC):
    """One namespace's exchange. Implementations: `BinanceAdapter`, `PaperAdapter`."""

    account: Account
    fill_model: FillModel = "exchange"

    def banned_until(self) -> float | None:
        """Epoch seconds until which the exchange bans this namespace's IP (HTTP 418); None if not banned."""
        return None

    @abstractmethod
    async def exchange_filters(self) -> dict[str, SymbolFilters]: ...

    @abstractmethod
    async def ensure_one_way(self) -> None:
        """Raise `PositionModeError` when the account is in Hedge Mode."""

    @abstractmethod
    async def prepare_symbol(self, symbol: str, leverage: int) -> None:
        """Set ISOLATED margin and the leverage before the first order on `symbol`."""

    @abstractmethod
    async def place_order(self, request: OrderRequest) -> OrderSnapshot: ...

    @abstractmethod
    async def query_order(self, symbol: str, client_id: str) -> OrderSnapshot | None: ...

    @abstractmethod
    async def cancel_order(self, symbol: str, client_id: str) -> OrderSnapshot | None:
        """Cancel; returns the final snapshot (None when the exchange does not know the order)."""

    @abstractmethod
    async def place_algo(self, request: AlgoRequest) -> AlgoSnapshot: ...

    @abstractmethod
    async def query_algo(self, client_algo_id: str) -> AlgoSnapshot | None: ...

    @abstractmethod
    async def cancel_algo(self, client_algo_id: str) -> AlgoSnapshot | None: ...

    @abstractmethod
    async def cancel_all_algos(self, symbol: str) -> None:
        """`DELETE /fapi/v1/algoOpenOrders`: only used after the symbol's position is closed."""

    @abstractmethod
    async def open_orders(self) -> list[OrderSnapshot]: ...

    @abstractmethod
    async def open_algo_orders(self) -> list[AlgoSnapshot]: ...

    @abstractmethod
    async def positions(self) -> list[ExchangePosition]:
        """Non-zero positions, net per symbol."""

    @abstractmethod
    async def balance(self) -> Balance: ...

    @abstractmethod
    async def user_trades(
        self, symbol: str, from_id: int | None, start: datetime | None, known: Mapping[int, str] | None = None
    ) -> list[TradeFill]:
        """Fills from `from_id` (or `start`); `known` maps exchange order ids to ledger client ids."""

    @abstractmethod
    async def cash_flows(self, start: datetime) -> list[CashFlow]:
        """Funding and other non-trade wallet changes since `start`."""

    @abstractmethod
    async def mark_prices(self, symbols: Sequence[str]) -> dict[str, Decimal]:
        """Current mark price per symbol (symbols without a mark are omitted)."""

    async def aclose(self) -> None:  # noqa: B027 - optional hook
        """Release network resources."""
