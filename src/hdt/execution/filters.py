"""Exchange trading rules from `GET /fapi/v1/exchangeInfo`: tick, lot, minNotional and rate limits.

Quantities are always rounded DOWN to the lot step (never up past a signed size); prices are rounded to the
tick in the direction that keeps an order passive or a stop conservative, as the caller asks.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Any, Final

DEFAULT_RATE_LIMITS: Final[dict[str, int]] = {"weight_1m": 2400, "orders_1m": 1200, "orders_10s": 300}


class FilterError(ValueError):
    """exchangeInfo does not describe the symbol (or describes it incompletely)."""


@dataclass(frozen=True)
class SymbolFilters:
    symbol: str
    status: str
    tick_size: Decimal
    step_size: Decimal
    min_qty: Decimal
    max_qty: Decimal
    market_step_size: Decimal
    market_min_qty: Decimal
    market_max_qty: Decimal
    min_notional: Decimal

    @property
    def trading(self) -> bool:
        return self.status == "TRADING"

    def floor_qty(self, qty: Decimal, *, market: bool = False) -> Decimal:
        step = self.market_step_size if market else self.step_size
        if qty <= 0:
            return Decimal(0)
        return (qty / step).to_integral_value(rounding=ROUND_FLOOR) * step

    def floor_price(self, price: Decimal) -> Decimal:
        return (price / self.tick_size).to_integral_value(rounding=ROUND_FLOOR) * self.tick_size

    def ceil_price(self, price: Decimal) -> Decimal:
        return (price / self.tick_size).to_integral_value(rounding=ROUND_CEILING) * self.tick_size

    def price_aligned(self, price: Decimal) -> bool:
        return (price / self.tick_size) == (price / self.tick_size).to_integral_value()

    def qty_aligned(self, qty: Decimal, *, market: bool = False) -> bool:
        step = self.market_step_size if market else self.step_size
        return (qty / step) == (qty / step).to_integral_value()

    def qty_problem(self, qty: Decimal, price: Decimal, *, market: bool = False) -> str | None:
        """Why `qty` at `price` cannot be sent (None when it can)."""
        lo = self.market_min_qty if market else self.min_qty
        hi = self.market_max_qty if market else self.max_qty
        if qty <= 0:
            return "qty must be positive"
        if not self.qty_aligned(qty, market=market):
            return f"qty {qty} is not a multiple of the lot step"
        if qty < lo:
            return f"qty {qty} is below minQty {lo}"
        if qty > hi:
            return f"qty {qty} is above maxQty {hi}"
        if qty * price < self.min_notional:
            return f"notional {qty * price} is below minNotional {self.min_notional}"
        return None


def _dec(value: Any, what: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (ArithmeticError, ValueError) as exc:
        raise FilterError(f"{what} is not a number: {value!r}") from exc
    if not result.is_finite():
        raise FilterError(f"{what} is not finite")
    return result


def parse_symbol_filters(item: Mapping[str, Any]) -> SymbolFilters:
    symbol = str(item.get("symbol", ""))
    filters = {f.get("filterType"): f for f in item.get("filters", []) if isinstance(f, Mapping)}
    try:
        price, lot, market, notional = (
            filters["PRICE_FILTER"],
            filters["LOT_SIZE"],
            filters.get("MARKET_LOT_SIZE", filters["LOT_SIZE"]),
            filters["MIN_NOTIONAL"],
        )
    except KeyError as exc:
        raise FilterError(f"{symbol}: exchangeInfo misses filter {exc.args[0]}") from None
    result = SymbolFilters(
        symbol=symbol,
        status=str(item.get("status", "")),
        tick_size=_dec(price.get("tickSize"), f"{symbol} tickSize"),
        step_size=_dec(lot.get("stepSize"), f"{symbol} stepSize"),
        min_qty=_dec(lot.get("minQty"), f"{symbol} minQty"),
        max_qty=_dec(lot.get("maxQty"), f"{symbol} maxQty"),
        market_step_size=_dec(market.get("stepSize"), f"{symbol} market stepSize"),
        market_min_qty=_dec(market.get("minQty"), f"{symbol} market minQty"),
        market_max_qty=_dec(market.get("maxQty"), f"{symbol} market maxQty"),
        min_notional=_dec(notional.get("notional"), f"{symbol} minNotional"),
    )
    if result.tick_size <= 0 or result.step_size <= 0 or result.market_step_size <= 0:
        raise FilterError(f"{symbol}: tick and step sizes must be positive")
    return result


def parse_exchange_filters(data: Any) -> dict[str, SymbolFilters]:
    """Every symbol with complete filters; incomplete entries are skipped (they cannot be traded)."""
    out: dict[str, SymbolFilters] = {}
    for item in data.get("symbols", []) if isinstance(data, Mapping) else []:
        if not isinstance(item, Mapping):
            continue
        try:
            filters = parse_symbol_filters(item)
        except FilterError:
            continue
        out[filters.symbol] = filters
    return out


def parse_rate_limits(data: Any) -> dict[str, int]:
    """`weight_1m`, `orders_1m`, `orders_10s` from exchangeInfo `rateLimits` (defaults when absent)."""
    limits = dict(DEFAULT_RATE_LIMITS)
    for item in data.get("rateLimits", []) if isinstance(data, Mapping) else []:
        if not isinstance(item, Mapping):
            continue
        kind, unit, num, limit = (
            item.get("rateLimitType"),
            item.get("interval"),
            item.get("intervalNum"),
            item.get("limit"),
        )
        if not isinstance(limit, int) or not isinstance(num, int):
            continue
        if kind == "REQUEST_WEIGHT" and unit == "MINUTE" and num == 1:
            limits["weight_1m"] = limit
        elif kind == "ORDERS" and unit == "MINUTE" and num == 1:
            limits["orders_1m"] = limit
        elif kind == "ORDERS" and unit == "SECOND" and num == 10:
            limits["orders_10s"] = limit
    return limits
