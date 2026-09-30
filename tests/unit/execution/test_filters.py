"""exchangeInfo filters: tick rounding in the passive / conservative direction, lot rounding down only."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from hdt.execution.filters import (
    DEFAULT_RATE_LIMITS,
    FilterError,
    parse_exchange_filters,
    parse_rate_limits,
    parse_symbol_filters,
)

SOL: dict[str, Any] = {
    "symbol": "SOLUSDT",
    "status": "TRADING",
    "filters": [
        {"filterType": "PRICE_FILTER", "tickSize": "0.0100", "minPrice": "0.4200", "maxPrice": "6857"},
        {"filterType": "LOT_SIZE", "stepSize": "0.01", "minQty": "0.01", "maxQty": "1000000"},
        {"filterType": "MARKET_LOT_SIZE", "stepSize": "0.01", "minQty": "0.01", "maxQty": "5000"},
        {"filterType": "MIN_NOTIONAL", "notional": "5"},
    ],
}


def test_parse_symbol_filters_from_exchange_info() -> None:
    f = parse_symbol_filters(SOL)
    assert f.trading
    assert (f.tick_size, f.step_size, f.min_qty, f.min_notional) == (
        Decimal("0.0100"),
        Decimal("0.01"),
        Decimal("0.01"),
        Decimal("5"),
    )
    assert f.market_max_qty == Decimal("5000")


def test_quantity_rounds_down_only() -> None:
    f = parse_symbol_filters(SOL)
    assert f.floor_qty(Decimal("1.239")) == Decimal("1.23")
    assert f.floor_qty(Decimal("0.009")) == 0
    assert f.floor_qty(Decimal("-1")) == 0
    assert f.floor_qty(Decimal("7")) == Decimal("7")


def test_price_rounding_directions_and_alignment() -> None:
    f = parse_symbol_filters(SOL)
    assert f.floor_price(Decimal("100.129")) == Decimal("100.12")
    assert f.ceil_price(Decimal("100.121")) == Decimal("100.13")
    assert f.ceil_price(Decimal("100.12")) == Decimal("100.12")
    assert f.price_aligned(Decimal("100.12"))
    assert not f.price_aligned(Decimal("100.125"))


def test_qty_problem_reasons() -> None:
    f = parse_symbol_filters(SOL)
    assert f.qty_problem(Decimal("1"), Decimal("10")) is None
    assert "positive" in (f.qty_problem(Decimal(0), Decimal("10")) or "")
    assert "lot step" in (f.qty_problem(Decimal("1.005"), Decimal("10")) or "")
    assert "minNotional" in (f.qty_problem(Decimal("0.4"), Decimal("10")) or "")
    assert "maxQty" in (f.qty_problem(Decimal("6000"), Decimal("10"), market=True) or "")
    assert f.qty_problem(Decimal("6000"), Decimal("10")) is None


def test_incomplete_symbols_are_skipped_and_single_parse_raises() -> None:
    broken = {"symbol": "XUSDT", "status": "TRADING", "filters": [{"filterType": "PRICE_FILTER"}]}
    assert set(parse_exchange_filters({"symbols": [SOL, broken, "junk"]})) == {"SOLUSDT"}
    with pytest.raises(FilterError, match="LOT_SIZE"):
        parse_symbol_filters(broken)
    zero_tick = {**SOL, "filters": [{**SOL["filters"][0], "tickSize": "0"}, *SOL["filters"][1:]]}
    with pytest.raises(FilterError, match="positive"):
        parse_symbol_filters(zero_tick)


def test_rate_limits_read_live_with_defaults() -> None:
    info = {
        "rateLimits": [
            {"rateLimitType": "REQUEST_WEIGHT", "interval": "MINUTE", "intervalNum": 1, "limit": 2400},
            {"rateLimitType": "ORDERS", "interval": "MINUTE", "intervalNum": 1, "limit": 1100},
            {"rateLimitType": "ORDERS", "interval": "SECOND", "intervalNum": 10, "limit": 250},
        ]
    }
    assert parse_rate_limits(info) == {"weight_1m": 2400, "orders_1m": 1100, "orders_10s": 250}
    assert parse_rate_limits({}) == DEFAULT_RATE_LIMITS
