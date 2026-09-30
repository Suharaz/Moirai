"""`OrderIntent` (risk -> stream `orders` -> execution), signed by Risk with Ed25519.

The signature covers the canonical JSON of every field except `signature` (so `account` is signed and a
paper intent cannot run on live). Signing and verification live in the risk/execution services.

`max_entry_distance` (entry legs only, optional, added 2026-09-28): the largest entry-to-mark distance in
price units Risk accepted for this OPEN; execution re-checks the mark against it, the stop and TP1 right
before sending the entry (`hdt.core.entry_guard`), since a decision has no time limit.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Literal, Self

from pydantic import Field, NonNegativeInt, model_validator

from hdt.contracts.common import (
    Account,
    ContractModel,
    Leg,
    OrderSide,
    OrderType,
    TimeInForce,
    UtcDatetime,
)
from hdt.core.ids import canonical_json
from hdt.settings.ceilings import LEVERAGE_MAX

SYMBOL_PATTERN = r"^[A-Z0-9]{2,30}$"  # the only symbols an order (hence the universe) can carry
CLIENT_ID_PATTERN = r"^[\.A-Z\:/a-z0-9_-]{1,36}$"
OPENING_LEGS = frozenset({Leg.ENTRY, Leg.ENTRY_IOC})
PROTECTIVE_LEGS = frozenset({Leg.SL, Leg.TP1, Leg.TRAIL, Leg.EXIT})


class OrderIntent(ContractModel):
    schema_version: Literal[1] = 1
    intent_id: str = Field(min_length=1, max_length=128)
    account: Account
    event_id: str = Field(min_length=1)
    leg: Leg
    seq: NonNegativeInt
    symbol: str = Field(pattern=SYMBOL_PATTERN)
    side: OrderSide
    order_type: OrderType
    qty: Decimal | None = Field(default=None, gt=0)
    price: Decimal | None = Field(default=None, gt=0)
    trigger_price: Decimal | None = Field(default=None, gt=0)
    reduce_only: bool
    close_position: bool = False
    tif: TimeInForce | None = None
    leverage: int | None = Field(default=None, ge=1, le=LEVERAGE_MAX)
    max_entry_distance: Decimal | None = Field(default=None, gt=0)
    client_id: str = Field(pattern=CLIENT_ID_PATTERN)
    expires_at: UtcDatetime | None = None
    created_at: UtcDatetime
    key_id: str = Field(min_length=1)
    signature: str = Field(default="", repr=False)

    @model_validator(mode="after")
    def _shape(self) -> Self:
        t = self.order_type
        if t is OrderType.LIMIT and (self.price is None or self.qty is None or self.tif is None):
            raise ValueError("LIMIT needs price, qty and tif")
        if t in (OrderType.STOP_MARKET, OrderType.TAKE_PROFIT_MARKET):
            if self.trigger_price is None:
                raise ValueError(f"{t} needs trigger_price")
            if self.close_position == (self.qty is not None):
                raise ValueError(f"{t} needs exactly one of qty or close_position")
        if t is OrderType.MARKET and self.qty is None:
            raise ValueError("MARKET needs qty")
        if self.close_position and t not in (OrderType.STOP_MARKET, OrderType.TAKE_PROFIT_MARKET):
            raise ValueError("close_position is only valid for STOP_MARKET / TAKE_PROFIT_MARKET")
        if self.close_position and self.reduce_only:
            raise ValueError("close_position and reduce_only are mutually exclusive on Binance")
        if self.leg in OPENING_LEGS and (self.reduce_only or self.close_position):
            raise ValueError(f"leg {self.leg} opens exposure and cannot be reduce_only/close_position")
        if self.leg in PROTECTIVE_LEGS and not (self.reduce_only or self.close_position):
            raise ValueError(f"leg {self.leg} must be reduce_only or close_position")
        if self.max_entry_distance is not None and self.leg not in OPENING_LEGS:
            raise ValueError("max_entry_distance is only valid on entry legs")
        return self

    def signing_bytes(self) -> bytes:
        """Canonical bytes covered by the Ed25519 signature."""
        return canonical_json(self.model_dump(mode="python", exclude={"signature"}))
