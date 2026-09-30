"""Testnet lifecycle driver (phase 09 section 9): `python -m hdt.execution.testnet_driver`.

It never sees a council decision and never holds a Binance key. Prices come from the demo exchange's
public endpoints (`bookTicker`, `premiumIndex`, `exchangeInfo` on demo-fapi); intents are signed with the
driver's own Ed25519 key (HDT_TESTNET_DRIVER_SIGNING_KEY[_FILE]), which execution's keyring accepts for
`account=testnet` only; they go to stream `orders` and the lifecycle is observed on stream
`account_state` (HDT_REDIS_URL, ACL user `testnet_driver`: XADD `orders`, XREVRANGE `account_state`).

One lifecycle (LONG, one event `driver:<run>:<n>`), published in Risk's order `sl`, `tp1`, `entry_ioc`,
`entry`:
1. the symbol must be flat with no open order or conditional order;
2. `entry` is a post-only (GTX) BUY at the best ask, so it would take: the exchange rejects it (-5022) and
   execution falls back to the signed `entry_ioc` (limit = ask + `ioc_slippage`);
3. the fill shows as a position; the STOP covering it must show within `stop_timeout_s`. Without a
   `StopProbe` (the CLI: the driver holds no Binance key) STOP latency is the time between the first
   AccountState showing the position and the first one showing it covered (execution's clock; AccountState
   is published on every change, at most once a second, so this is an observation latency only). With a
   probe (the acceptance test, which holds the demo account's key) STOP latency is exchange-anchored: from
   the exchange time of the lifecycle's first entry fill (`userTrades` `time`, the `T` of its
   ORDER_TRADE_UPDATE) to the `createTime` of the covering STOP listed in `openAlgoOrders`;
4. a signed `exit` (MARKET reduce-only) closes it; the symbol must end flat with no working conditional
   order (execution cancels the STOP and TP of a closed position).
The run stops at the first failed lifecycle (the criterion is N consecutive lifecycles). Exit code 0 when
every lifecycle passed and the p99 STOP latency is within `--max-stop-latency-s`, 1 otherwise, 2 when the
driver key or the Redis URL is missing (or on a usage error).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import sys
import uuid
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import ROUND_CEILING, Decimal
from typing import Any, Final, Protocol

import httpx
from pydantic import ValidationError
from redis.asyncio import Redis
from redis.exceptions import RedisError

from hdt.contracts.account import AccountState
from hdt.contracts.common import Account, Leg, OrderSide, OrderType, TimeInForce
from hdt.contracts.order import OrderIntent
from hdt.contracts.streams import Stream
from hdt.core.clock import utcnow
from hdt.core.config import read_env_value, static_config
from hdt.core.logging import configure_logging
from hdt.core.streams import connect_redis, publish
from hdt.execution.adapter_base import ExchangeAdapter, ExchangeError
from hdt.execution.binance_adapter import BinanceAdapter, ms_to_dt, opt_dec
from hdt.execution.client_ids import client_id, event_prefix, parse_client_id
from hdt.execution.filters import FilterError, SymbolFilters, parse_exchange_filters
from hdt.execution.stop_guard import covers
from hdt.risk.gate import intent_id_for
from hdt.risk.signer import DRIVER_KEY_ENV, IntentSigner, SigningKeyError

log = logging.getLogger(__name__)

ACCOUNT: Final[Account] = Account.TESTNET
EVENT_PREFIX: Final[str] = "driver:"
DEFAULT_SYMBOL: Final[str] = "ETHUSDT"
ON_EXCHANGE: Final[frozenset[str]] = frozenset({"NEW"})  # ledger status of an acknowledged conditional
STATE_SCAN: Final[int] = 50
PLACEHOLDER_KEY_ID: Final[str] = "unsigned"  # replaced by the signer's key id
EXIT_OK: Final[int] = 0
EXIT_FAILED: Final[int] = 1
EXIT_USAGE: Final[int] = 2
ENTRY_LEGS: Final[frozenset[Leg]] = frozenset({Leg.ENTRY, Leg.ENTRY_IOC})
OPEN_ALGO_PATH: Final[str] = "/fapi/v1/openAlgoOrders"


class MarketDataError(RuntimeError):
    """The demo exchange's public data is unavailable or incomplete."""


# ---------------------------------------------------------------------------- demo market data


@dataclass(frozen=True)
class Quote:
    mark: Decimal
    bid: Decimal
    ask: Decimal


class DemoMarket:
    """Public market data of the demo exchange (no key, no signed endpoint)."""

    def __init__(self, http: httpx.AsyncClient, base_url: str) -> None:
        self.http = http
        self.base_url = base_url.rstrip("/")
        self._filters: dict[str, SymbolFilters] = {}

    async def _get(self, path: str, params: dict[str, str] | None = None) -> Any:
        try:
            response = await self.http.get(f"{self.base_url}{path}", params=params)
            response.raise_for_status()
            return response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise MarketDataError(f"GET {path} failed: {type(exc).__name__}") from exc

    async def filters(self, symbol: str) -> SymbolFilters:
        if symbol not in self._filters:
            self._filters = parse_exchange_filters(await self._get("/fapi/v1/exchangeInfo"))
        found = self._filters.get(symbol)
        if found is None or not found.trading:
            raise MarketDataError(f"{symbol} is not trading on the demo exchange")
        return found

    async def quote(self, symbol: str) -> Quote:
        book = await self._get("/fapi/v1/ticker/bookTicker", {"symbol": symbol})
        premium = await self._get("/fapi/v1/premiumIndex", {"symbol": symbol})
        try:
            quote = Quote(
                mark=Decimal(str(premium["markPrice"])),
                bid=Decimal(str(book["bidPrice"])),
                ask=Decimal(str(book["askPrice"])),
            )
        except (KeyError, TypeError, ArithmeticError) as exc:
            raise MarketDataError(f"incomplete quote for {symbol}") from exc
        if quote.bid <= 0 or quote.ask < quote.bid or quote.mark <= 0:
            raise MarketDataError(f"unusable quote for {symbol}: {quote}")
        return quote


# ---------------------------------------------------------------------------- sinks and observers


class IntentSink(Protocol):
    async def send(self, intents: Sequence[OrderIntent]) -> None: ...


class RedisOrdersSink:
    """XADD signed intents to stream `orders`, in the given order."""

    def __init__(self, redis: Redis, maxlen: int | None = None) -> None:
        self.redis = redis
        self.maxlen = maxlen

    async def send(self, intents: Sequence[OrderIntent]) -> None:
        for intent in intents:
            await publish(self.redis, Stream.ORDERS, intent, maxlen=self.maxlen)


@dataclass(frozen=True)
class Observation:
    at: datetime
    positions: dict[str, Decimal]  # signed qty of every non-zero position
    covered: frozenset[str]  # symbols whose position has a covering STOP on the exchange
    algo_symbols: frozenset[str]  # symbols with a working conditional order
    order_symbols: frozenset[str]  # symbols with an open regular order

    def qty(self, symbol: str) -> Decimal:
        return self.positions.get(symbol, Decimal(0))

    def flat(self, symbol: str) -> bool:
        return self.qty(symbol) == 0 and symbol not in self.algo_symbols and symbol not in self.order_symbols


class Observer(Protocol):
    async def observe(self) -> Observation | None: ...


def observation_from_state(state: AccountState) -> Observation:
    positions = {p.symbol: p.qty for p in state.positions if p.qty != 0}
    algos = [a for a in state.open_algo_orders if a.status in ON_EXCHANGE]
    covered = frozenset(
        symbol
        for symbol, qty in positions.items()
        if any(
            a.symbol == symbol and covers(a.side.value, a.order_type.value, a.qty, a.close_position, qty)
            for a in algos
        )
    )
    return Observation(
        at=state.ts,
        positions=positions,
        covered=covered,
        algo_symbols=frozenset(a.symbol for a in state.open_algo_orders),
        order_symbols=frozenset(o.symbol for o in state.open_orders),
    )


class AccountStateObserver:
    """The newest testnet `AccountState` on stream `account_state` (published by execution)."""

    def __init__(self, redis: Redis, account: Account = ACCOUNT, scan: int = STATE_SCAN) -> None:
        self.redis = redis
        self.account = account
        self.scan = scan

    async def observe(self) -> Observation | None:
        entries: Any = await self.redis.xrevrange(Stream.ACCOUNT_STATE.value, count=self.scan)
        for _msg_id, fields in entries or []:
            data = fields.get(b"data", fields.get("data"))
            if data is None:
                continue
            try:
                state = AccountState.model_validate_json(data)
            except (ValidationError, ValueError):
                continue
            if state.account is self.account:
                return observation_from_state(state)
        return None


class AdapterObserver:
    """The exchange's own view through an adapter (in-process tests and diagnostics)."""

    def __init__(self, adapter: ExchangeAdapter, clock: Callable[[], datetime] = utcnow) -> None:
        self.adapter = adapter
        self.clock = clock

    async def observe(self) -> Observation | None:
        positions = {p.symbol: p.qty for p in await self.adapter.positions() if p.qty != 0}
        algos = [a for a in await self.adapter.open_algo_orders() if a.is_working]
        orders = await self.adapter.open_orders()
        covered = frozenset(
            symbol
            for symbol, qty in positions.items()
            if any(
                a.symbol == symbol and covers(a.side.value, a.order_type, a.qty, a.close_position, qty)
                for a in algos
            )
        )
        return Observation(
            at=self.clock(),
            positions=positions,
            covered=covered,
            algo_symbols=frozenset(a.symbol for a in algos),
            order_symbols=frozenset(o.symbol for o in orders),
        )


@dataclass(frozen=True)
class ExchangeStop:
    """One lifecycle's covering STOP as the exchange lists it, and the entry fill it covers."""

    fill_at: datetime  # exchange time of the lifecycle's first entry fill (`userTrades` `time`)
    stop_at: datetime  # exchange `createTime` of the covering STOP listed in `openAlgoOrders`
    client_algo_id: str
    algo_id: int | None  # the id the exchange acknowledged the STOP with

    def as_dict(self) -> dict[str, Any]:
        return {
            "fill_at": self.fill_at.isoformat(),
            "stop_at": self.stop_at.isoformat(),
            "client_algo_id": self.client_algo_id,
            "algo_id": self.algo_id,
        }


class StopProbe(Protocol):
    async def covering_stop(self, symbol: str, event_id: str, position: Decimal) -> ExchangeStop | None: ...


def _is_leg(value: object, event_id: str, legs: frozenset[Leg]) -> bool:
    """True for an exchange client id generated for one of `legs` of `event_id` (any attempt)."""
    parsed = parse_client_id(str(value or ""))
    return parsed is not None and parsed.leg in legs and parsed.prefix == event_prefix(event_id)


class BinanceStopProbe:
    """Exchange-anchored STOP latency on the demo account, read-only (signed GETs with the account's key).

    `covering_stop` answers None until `openAlgoOrders` lists a working STOP of the lifecycle's event that
    covers `position`; then it returns that STOP's `createTime` and the exchange time of the event's first
    entry fill (`userTrades` from `since`, exchange clock). Only the acceptance test builds it: the driver
    itself never holds a Binance key.
    """

    def __init__(self, adapter: BinanceAdapter, since: datetime) -> None:
        self.adapter = adapter
        self.since = since
        self._client_ids: dict[int, str] = {}

    async def covering_stop(self, symbol: str, event_id: str, position: Decimal) -> ExchangeStop | None:
        data = await self.adapter.client.signed("GET", OPEN_ALGO_PATH, {"symbol": symbol})
        items = data.get("orders", data) if isinstance(data, dict) else data
        stops = [
            a
            for a in items
            if a.get("symbol") == symbol
            and _is_leg(a.get("clientAlgoId"), event_id, frozenset({Leg.SL}))
            and str(a.get("algoStatus") or a.get("status") or "") in ON_EXCHANGE
            and a.get("createTime")
            and covers(
                str(a["side"]),
                str(a.get("orderType") or a.get("type") or ""),
                opt_dec(a.get("quantity")),
                bool(a.get("closePosition", False)),
                position,
            )
        ]
        if not stops:
            return None
        stop = min(stops, key=lambda a: int(a["createTime"]))
        trades = await self.adapter.user_trades(symbol, None, self.since, self._client_ids)
        self._client_ids.update({t.order_id: t.client_id for t in trades if t.order_id is not None})
        entry_fills = [t.time for t in trades if _is_leg(t.client_id, event_id, ENTRY_LEGS)]
        if not entry_fills:
            raise ExchangeError(f"{symbol} has a STOP of {event_id} but no entry fill in userTrades")
        return ExchangeStop(
            fill_at=min(entry_fills),
            stop_at=ms_to_dt(int(stop["createTime"])),
            client_algo_id=str(stop["clientAlgoId"]),
            algo_id=int(stop["algoId"]) if stop.get("algoId") is not None else None,
        )


# ---------------------------------------------------------------------------- intents


@dataclass(frozen=True)
class DriverParams:
    symbol: str = DEFAULT_SYMBOL
    leverage: int = 2
    stop_pct: Decimal = Decimal("0.02")  # STOP trigger below the bid
    tp_pct: Decimal = Decimal("0.02")  # TP trigger above the ask
    ioc_slippage: Decimal = Decimal("0.002")  # IOC limit above the ask
    tp_fraction: Decimal = Decimal("0.5")
    notional_mult: Decimal = Decimal("2.2")  # of the symbol's min notional (each TP half stays above it)
    entry_ttl: timedelta = timedelta(seconds=60)
    time_stop: timedelta = timedelta(minutes=15)  # execution closes the position if the driver dies


def _ceil_step(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_CEILING) * step


def entry_qty(filters: SymbolFilters, price: Decimal, notional_mult: Decimal) -> Decimal:
    """Smallest step-aligned qty whose notional is `notional_mult` x the symbol's minimum."""
    qty = max(filters.min_qty, _ceil_step(filters.min_notional * notional_mult / price, filters.step_size))
    problem = filters.qty_problem(qty, price)
    if problem is not None:
        raise MarketDataError(f"cannot size {filters.symbol}: {problem}")
    return qty


def _intent(event_id: str, leg: Leg, symbol: str, now: datetime, **fields: Any) -> OrderIntent:
    return OrderIntent(
        intent_id=intent_id_for(ACCOUNT, event_id, leg, 0),
        account=ACCOUNT,
        event_id=event_id,
        leg=leg,
        seq=0,
        symbol=symbol,
        client_id=client_id(event_id, leg, 0),
        created_at=now,
        key_id=PLACEHOLDER_KEY_ID,
        **fields,
    )


def open_plan(
    event_id: str, filters: SymbolFilters, quote: Quote, params: DriverParams, now: datetime
) -> list[OrderIntent]:
    """Unsigned LONG plan in publish order: `sl`, `tp1` (when the half is tradable), `entry_ioc`, `entry`."""
    symbol = filters.symbol
    qty = entry_qty(filters, quote.ask, params.notional_mult)
    stop = filters.floor_price(quote.bid * (1 - params.stop_pct))
    take = filters.ceil_price(quote.ask * (1 + params.tp_pct))
    ioc_price = filters.floor_price(quote.ask * (1 + params.ioc_slippage))
    plan = [
        _intent(
            event_id,
            Leg.SL,
            symbol,
            now,
            side=OrderSide.SELL,
            order_type=OrderType.STOP_MARKET,
            qty=qty,
            trigger_price=stop,
            reduce_only=True,
            expires_at=now + params.time_stop,
        )
    ]
    tp_qty = filters.floor_qty(qty * params.tp_fraction)
    if tp_qty >= filters.min_qty and filters.qty_problem(tp_qty, take) is None:
        plan.append(
            _intent(
                event_id,
                Leg.TP1,
                symbol,
                now,
                side=OrderSide.SELL,
                order_type=OrderType.TAKE_PROFIT_MARKET,
                qty=tp_qty,
                trigger_price=take,
                reduce_only=True,
                expires_at=now + params.time_stop,
            )
        )
    for leg, tif, price in (
        (Leg.ENTRY_IOC, TimeInForce.IOC, ioc_price),
        (Leg.ENTRY, TimeInForce.GTX, quote.ask),
    ):
        plan.append(
            _intent(
                event_id,
                leg,
                symbol,
                now,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                qty=qty,
                price=price,
                reduce_only=False,
                tif=tif,
                leverage=params.leverage,
                expires_at=now + params.entry_ttl,
            )
        )
    return plan


def exit_intent(event_id: str, symbol: str, position_qty: Decimal, now: datetime) -> OrderIntent:
    """Unsigned MARKET reduce-only close of the whole position."""
    return _intent(
        event_id,
        Leg.EXIT,
        symbol,
        now,
        side=OrderSide.SELL if position_qty > 0 else OrderSide.BUY,
        order_type=OrderType.MARKET,
        qty=abs(position_qty),
        reduce_only=True,
    )


# ---------------------------------------------------------------------------- lifecycles


@dataclass(frozen=True)
class LifecycleResult:
    index: int
    event_id: str
    symbol: str
    ok: bool
    error: str | None
    qty: Decimal | None
    fill_seen_at: datetime | None
    stop_seen_at: datetime | None
    stop_latency_s: float | None
    closed: bool
    exchange_stop: ExchangeStop | None = None  # set when a `StopProbe` measured the latency

    def as_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "event_id": self.event_id,
            "symbol": self.symbol,
            "ok": self.ok,
            "error": self.error,
            "qty": None if self.qty is None else str(self.qty),
            "fill_seen_at": None if self.fill_seen_at is None else self.fill_seen_at.isoformat(),
            "stop_seen_at": None if self.stop_seen_at is None else self.stop_seen_at.isoformat(),
            "stop_latency_s": self.stop_latency_s,
            "closed": self.closed,
            "exchange_stop": None if self.exchange_stop is None else self.exchange_stop.as_dict(),
        }


@dataclass(frozen=True)
class DriverSummary:
    n: int
    ok: int
    p50_s: float | None
    p99_s: float | None
    max_s: float | None

    def as_dict(self) -> dict[str, Any]:
        return {"n": self.n, "ok": self.ok, "p50_s": self.p50_s, "p99_s": self.p99_s, "max_s": self.max_s}


def _nearest_rank(sorted_values: Sequence[float], q: float) -> float:
    return sorted_values[max(0, math.ceil(q * len(sorted_values)) - 1)]


def summarize(results: Sequence[LifecycleResult]) -> DriverSummary:
    """STOP latency percentiles (nearest rank) over the lifecycles that measured one."""
    latencies = sorted(r.stop_latency_s for r in results if r.stop_latency_s is not None)
    return DriverSummary(
        n=len(results),
        ok=sum(1 for r in results if r.ok),
        p50_s=_nearest_rank(latencies, 0.50) if latencies else None,
        p99_s=_nearest_rank(latencies, 0.99) if latencies else None,
        max_s=latencies[-1] if latencies else None,
    )


async def _poll[T](
    fetch: Callable[[], Awaitable[T | None]], accept: Callable[[T], bool], *, timeout_s: float, poll_s: float
) -> T | None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    while True:
        value = await fetch()
        if value is not None and accept(value):
            return value
        if loop.time() >= deadline:
            return None
        await asyncio.sleep(poll_s)


async def wait_for(
    observer: Observer, predicate: Callable[[Observation], bool], *, timeout_s: float, poll_s: float
) -> Observation | None:
    """Poll `observer` until `predicate` holds (the matching observation) or `timeout_s` passes (None)."""
    return await _poll(observer.observe, predicate, timeout_s=timeout_s, poll_s=poll_s)


@dataclass(frozen=True)
class _Timeouts:
    poll_s: float
    fill_s: float
    stop_s: float
    close_s: float


async def _close(
    event_id: str,
    symbol: str,
    qty: Decimal,
    *,
    sink: IntentSink,
    observer: Observer,
    signer: IntentSigner,
    timeouts: _Timeouts,
    clock: Callable[[], datetime],
) -> bool:
    await sink.send([signer.sign(exit_intent(event_id, symbol, qty, clock()))])
    done = await wait_for(
        observer, lambda o: o.flat(symbol), timeout_s=timeouts.close_s, poll_s=timeouts.poll_s
    )
    return done is not None


async def run_lifecycle(
    index: int,
    event_id: str,
    *,
    market: DemoMarket,
    sink: IntentSink,
    observer: Observer,
    signer: IntentSigner,
    params: DriverParams,
    timeouts: _Timeouts,
    clock: Callable[[], datetime],
    probe: StopProbe | None = None,
) -> LifecycleResult:
    symbol = params.symbol

    def result(
        error: str | None,
        *,
        qty: Decimal | None = None,
        fill: datetime | None = None,
        stop: datetime | None = None,
        closed: bool = False,
        exchange_stop: ExchangeStop | None = None,
    ) -> LifecycleResult:
        latency = (stop - fill).total_seconds() if fill is not None and stop is not None else None
        return LifecycleResult(
            index, event_id, symbol, error is None, error, qty, fill, stop, latency, closed, exchange_stop
        )

    before = await observer.observe()
    if before is None:
        return result("no testnet AccountState on stream account_state (is execution running testnet?)")
    if not before.flat(symbol):
        return result(
            f"{symbol} is not flat before the lifecycle (position, order or conditional order open)"
        )
    filters = await market.filters(symbol)
    quote = await market.quote(symbol)
    plan = [signer.sign(i) for i in open_plan(event_id, filters, quote, params, clock())]
    await sink.send(plan)
    filled = await wait_for(
        observer, lambda o: o.qty(symbol) != 0, timeout_s=timeouts.fill_s, poll_s=timeouts.poll_s
    )
    if filled is None:
        return result(f"no fill within {timeouts.fill_s:g} s")
    qty = filled.qty(symbol)
    error: str | None = None
    fill_at: datetime | None = None
    stop_at: datetime | None = None
    exchange_stop: ExchangeStop | None = None
    if probe is None:
        fill_at = filled.at
        protected = (
            filled
            if symbol in filled.covered
            else await wait_for(
                observer,
                lambda o: symbol in o.covered or o.qty(symbol) == 0,
                timeout_s=timeouts.stop_s,
                poll_s=timeouts.poll_s,
            )
        )
        if protected is None:
            error = f"position without a covering STOP for {timeouts.stop_s:g} s"
        elif symbol not in protected.covered:
            error = "position closed before a covering STOP was seen"
        else:
            stop_at = protected.at
    else:
        listed = probe
        try:
            exchange_stop = await _poll(
                lambda: listed.covering_stop(symbol, event_id, qty),
                lambda _stop: True,
                timeout_s=timeouts.stop_s,
                poll_s=timeouts.poll_s,
            )
        except ExchangeError as exc:  # the position is open: it is still closed below
            error = f"STOP probe failed: {type(exc).__name__}: {exc}"
        else:
            if exchange_stop is None:
                error = f"no covering STOP listed in openAlgoOrders for {timeouts.stop_s:g} s"
            else:
                fill_at, stop_at = exchange_stop.fill_at, exchange_stop.stop_at
    closed = await _close(
        event_id, symbol, qty, sink=sink, observer=observer, signer=signer, timeouts=timeouts, clock=clock
    )
    if error is None and not closed:
        error = f"not flat with no working order within {timeouts.close_s:g} s after the exit"
    return result(error, qty=qty, fill=fill_at, stop=stop_at, closed=closed, exchange_stop=exchange_stop)


async def run_lifecycles(
    *,
    market: DemoMarket,
    sink: IntentSink,
    observer: Observer,
    signer: IntentSigner,
    params: DriverParams | None = None,
    n: int = 20,
    poll_s: float = 0.5,
    fill_timeout_s: float = 30.0,
    stop_timeout_s: float = 10.0,
    close_timeout_s: float = 30.0,
    run_id: str | None = None,
    clock: Callable[[], datetime] = utcnow,
    probe: StopProbe | None = None,
) -> list[LifecycleResult]:
    """Run up to `n` lifecycles, stopping at the first failure (STOP latency from `probe` when given)."""
    params = params or DriverParams()
    run = run_id or uuid.uuid4().hex[:12]
    timeouts = _Timeouts(poll_s, fill_timeout_s, stop_timeout_s, close_timeout_s)
    results: list[LifecycleResult] = []
    for index in range(n):
        event_id = f"{EVENT_PREFIX}{run}:{index}"
        try:
            outcome = await run_lifecycle(
                index,
                event_id,
                market=market,
                sink=sink,
                observer=observer,
                signer=signer,
                params=params,
                timeouts=timeouts,
                clock=clock,
                probe=probe,
            )
        except (MarketDataError, FilterError, RedisError, OSError) as exc:
            outcome = LifecycleResult(
                index,
                event_id,
                params.symbol,
                False,
                f"{type(exc).__name__}: {exc}",
                None,
                None,
                None,
                None,
                False,
            )
        results.append(outcome)
        log.info("lifecycle done", extra=outcome.as_dict())
        if not outcome.ok:
            break
    return results


# ---------------------------------------------------------------------------- CLI


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m hdt.execution.testnet_driver",
        description="Synthetic order lifecycles on the Binance demo exchange (testnet driver key).",
    )
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("-n", "--lifecycles", type=int, default=20)
    parser.add_argument("--leverage", type=int, default=2, choices=[1, 2, 3])
    parser.add_argument("--max-stop-latency-s", type=float, default=2.0)
    parser.add_argument("--poll-s", type=float, default=0.5)
    parser.add_argument("--fill-timeout-s", type=float, default=30.0)
    parser.add_argument("--stop-timeout-s", type=float, default=10.0)
    parser.add_argument("--close-timeout-s", type=float, default=30.0)
    return parser


async def _amain(args: argparse.Namespace, signer: IntentSigner, redis_url: str) -> int:
    static = static_config()
    redis = connect_redis(redis_url)
    http = httpx.AsyncClient(timeout=static.binance.request_timeout_s)
    try:
        results = await run_lifecycles(
            market=DemoMarket(http, static.binance.demo.rest),
            sink=RedisOrdersSink(redis, static.settings.streams.maxlen.get(Stream.ORDERS.value)),
            observer=AccountStateObserver(redis),
            signer=signer,
            params=DriverParams(symbol=args.symbol, leverage=args.leverage),
            n=args.lifecycles,
            poll_s=args.poll_s,
            fill_timeout_s=args.fill_timeout_s,
            stop_timeout_s=args.stop_timeout_s,
            close_timeout_s=args.close_timeout_s,
        )
    finally:
        await http.aclose()
        await redis.aclose()
    summary = summarize(results)
    for r in results:
        print(json.dumps(r.as_dict()))
    print(json.dumps({"summary": summary.as_dict(), "max_stop_latency_s": args.max_stop_latency_s}))
    passed = (
        len(results) == args.lifecycles
        and all(r.ok for r in results)
        and summary.p99_s is not None
        and summary.p99_s <= args.max_stop_latency_s
    )
    return EXIT_OK if passed else EXIT_FAILED


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:  # --help (0) or a usage error (2)
        return int(exc.code or 0) if isinstance(exc.code, int) else EXIT_USAGE
    if (
        args.lifecycles < 1
        or min(args.poll_s, args.fill_timeout_s, args.stop_timeout_s, args.close_timeout_s) <= 0
    ):
        print("--lifecycles and every timeout must be positive", file=sys.stderr)
        return EXIT_USAGE
    try:
        signer = IntentSigner.from_env(DRIVER_KEY_ENV)
    except SigningKeyError as exc:
        print(f"testnet driver cannot start: {exc}", file=sys.stderr)
        return EXIT_USAGE
    redis_url = read_env_value("HDT_REDIS_URL")
    if redis_url is None:
        print(
            "testnet driver cannot start: HDT_REDIS_URL (or HDT_REDIS_URL_FILE) is not set", file=sys.stderr
        )
        return EXIT_USAGE
    return asyncio.run(_amain(args, signer, redis_url))


if __name__ == "__main__":
    configure_logging("testnet-driver")
    sys.exit(main())
