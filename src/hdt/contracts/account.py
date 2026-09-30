"""`AccountState` (execution -> stream `account_state` -> risk, scanner, console, public-publisher).

Emitted only while the namespace is synced (never during RESYNCING). Quantities are net per symbol and
signed (positive = long). The public publisher only forwards allowlisted fields.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Literal

from pydantic import Field

from hdt.contracts.common import Account, ContractModel, OrderSide, OrderType, TimeInForce, UtcDatetime


class PositionState(ContractModel):
    symbol: str
    qty: Decimal = Field(description="signed net quantity, positive = long")
    entry_price: Decimal
    mark_price: Decimal
    unrealized_pnl: Decimal
    leverage: int = Field(ge=1)
    liquidation_price: Decimal | None = None
    margin_type: Literal["ISOLATED", "CROSSED"]
    is_hedge_book: bool = False


class OpenOrderState(ContractModel):
    symbol: str
    client_id: str
    side: OrderSide
    order_type: OrderType
    price: Decimal | None
    qty: Decimal
    executed_qty: Decimal
    reduce_only: bool
    tif: TimeInForce | None
    status: str


class OpenAlgoOrderState(ContractModel):
    symbol: str
    client_algo_id: str
    side: OrderSide
    order_type: OrderType
    trigger_price: Decimal
    qty: Decimal | None
    reduce_only: bool
    close_position: bool
    status: str


class AccountState(ContractModel):
    schema_version: Literal[1] = 1
    account: Account
    equity: Decimal
    available: Decimal
    day_start_equity: Decimal = Field(description="equity at 00:00 UTC, for the daily loss kill")
    positions: tuple[PositionState, ...]
    open_orders: tuple[OpenOrderState, ...]
    open_algo_orders: tuple[OpenAlgoOrderState, ...]
    ts: UtcDatetime

    def position(self, symbol: str) -> PositionState | None:
        for pos in self.positions:
            if pos.symbol == symbol and pos.qty != 0:
                return pos
        return None
