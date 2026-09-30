"""Testnet lifecycle driver against execution over the fake exchange (phase 09 section 9, offline).

The driver's intents go through the real intake of a `testnet` namespace (driver key), the fake exchange
plays the demo exchange (the post-only entry at the ask would take: -5022, then the signed IOC), and the
driver observes the namespace as it does on the demo account: the AccountState stream. The STOP latency is
the time between the first observation showing the position and the first showing it covered.
"""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal

import httpx
import pytest
from redis.asyncio import Redis
from sqlalchemy import Engine

from binance_shapes import BinanceRoutes, error, exchange_info
from fake_exchange import (
    FILTERS,
    SOL,
    Harness,
    ledger_sessions,
    leg_of_call,
    make_harness,
    open_position,
    working,
)
from hdt.contracts.common import Account, Leg
from hdt.contracts.order import OrderIntent
from hdt.execution import actions
from hdt.execution.account_state import AccountStatePublisher
from hdt.execution.testnet_driver import (
    AccountStateObserver,
    AdapterObserver,
    DemoMarket,
    DriverParams,
    LifecycleResult,
    Observation,
    Observer,
    run_lifecycles,
    summarize,
)
from intent_builders import DRIVER_SIGNER, KEYRING

pytestmark = [pytest.mark.pg, pytest.mark.redis, pytest.mark.integration]

DEMO = "https://demo-fapi.test"
PARAMS = DriverParams(symbol=SOL)


@pytest.fixture(autouse=True)
def _no_retry_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(actions, "CANCEL_CONFIRM_BACKOFF_S", 0.0)
    monkeypatch.setattr(actions, "UNKNOWN_VERIFY_DELAY_S", 0.0)


@pytest.fixture
async def h(pg_engine: Engine) -> Harness:
    harness = make_harness(ledger_sessions(pg_engine), Account.TESTNET, KEYRING)
    await harness.start()
    return harness


def _demo_routes(h: Harness) -> BinanceRoutes:
    """Public demo-fapi endpoints serving the fake exchange's own book and mark."""

    def book(request: httpx.Request) -> httpx.Response:
        levels = h.exchange.venue.books[request.url.params["symbol"]]
        return httpx.Response(
            200, json={"bidPrice": str(levels.best_bid()), "askPrice": str(levels.best_ask())}
        )

    def premium(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, json={"markPrice": str(h.exchange.venue.mark(request.url.params["symbol"]))}
        )

    routes = BinanceRoutes()
    routes.always("GET", "/fapi/v1/exchangeInfo", exchange_info(FILTERS))
    routes.always("GET", "/fapi/v1/ticker/bookTicker", book)
    routes.always("GET", "/fapi/v1/premiumIndex", premium)
    return routes


class ExecutionSink:
    """Stream `orders` as execution's `exec-testnet` consumer handles it, then one AccountState."""

    def __init__(self, h: Harness, publisher: AccountStatePublisher | None) -> None:
        self.h = h
        self.publisher = publisher
        self.batches: list[list[Leg]] = []

    async def send(self, intents: Sequence[OrderIntent]) -> None:
        self.batches.append([i.leg for i in intents])
        statuses = await self.h.send(intents)
        assert statuses == ["accepted"] * len(intents), statuses
        await self.h.pump()
        if self.publisher is not None:
            await self.publisher.publish_once()


class LaggingObserver:
    """The exchange's view, with the covering STOP shown `lags[k]` polls after lifecycle k's position
    (AccountState lag); every poll advances the namespace clock by `step_s`."""

    def __init__(self, inner: Observer, h: Harness, lags: Sequence[int], step_s: float) -> None:
        self.inner = inner
        self.h = h
        self.lags = list(lags)
        self.step_s = step_s
        self.lifecycle = -1
        self.seen = 0

    async def observe(self) -> Observation | None:
        self.h.clock.advance(self.step_s)
        obs = await self.inner.observe()
        assert obs is not None
        if obs.qty(SOL) == 0:
            self.seen = 0
            return obs
        if self.seen == 0:
            self.lifecycle += 1
        self.seen += 1
        if self.seen <= self.lags[self.lifecycle]:
            return Observation(obs.at, obs.positions, frozenset(), obs.algo_symbols, obs.order_symbols)
        return obs


async def _run(
    h: Harness, observer: Observer, sink: ExecutionSink, n: int, *, stop_timeout_s: float = 5.0
) -> list[LifecycleResult]:
    async with httpx.AsyncClient(transport=_demo_routes(h).transport()) as http:
        return await run_lifecycles(
            market=DemoMarket(http, DEMO),
            sink=sink,
            observer=observer,
            signer=DRIVER_SIGNER,
            params=PARAMS,
            n=n,
            poll_s=0.001,
            fill_timeout_s=5.0,
            stop_timeout_s=stop_timeout_s,
            close_timeout_s=5.0,
            run_id="t",
            clock=h.clock,
        )


async def test_n_lifecycles_through_execution_observed_on_the_account_state_stream(
    h: Harness, redis_client: Redis
) -> None:
    publisher = AccountStatePublisher(h.ns, redis_client, h.kill, interval_s=5.0, maxlen=None)
    await publisher.publish_once()  # execution publishes on start
    sink = ExecutionSink(h, publisher)
    results = await _run(h, AccountStateObserver(redis_client), sink, 5)

    assert [(r.index, r.event_id, r.ok, r.error, r.closed) for r in results] == [
        (i, f"driver:t:{i}", True, None, True) for i in range(5)
    ]
    assert {r.qty for r in results} == {Decimal("0.110")}  # 2.2 x min notional 5 at the ask 100.10
    # Execution places the STOP in the same step as the fill: no AccountState ever shows it uncovered.
    assert [r.stop_latency_s for r in results] == [0.0] * 5
    summary = summarize(results)
    assert (summary.n, summary.ok, summary.p50_s, summary.p99_s, summary.max_s) == (5, 5, 0.0, 0.0, 0.0)

    assert sink.batches == [[Leg.SL, Leg.TP1, Leg.ENTRY_IOC, Leg.ENTRY], [Leg.EXIT]] * 5
    placed = [leg_of_call(c) for c in h.exchange.mutating_calls("place_order")]
    assert placed == [Leg.ENTRY, Leg.ENTRY_IOC, Leg.EXIT] * 5  # GTX at the ask rejected (-5022), IOC
    algos = [leg_of_call(c) for c in h.exchange.mutating_calls("place_algo")]
    assert algos == [Leg.SL, Leg.TP1] * 5
    position = h.exchange.venue.positions.get(SOL)
    assert position is None or position.qty == 0
    assert working(h, symbol=SOL) == []
    report = await h.reconcile()
    assert report.clean


async def test_stop_latency_is_the_time_from_the_fill_to_the_first_covered_observation(h: Harness) -> None:
    observer = LaggingObserver(AdapterObserver(h.exchange, h.clock), h, lags=[1, 3, 2], step_s=0.25)
    results = await _run(h, observer, ExecutionSink(h, None), 3)
    assert [r.ok for r in results] == [True] * 3
    assert [r.stop_latency_s for r in results] == [0.25, 0.75, 0.5]
    for r in results:
        assert r.fill_seen_at is not None
        assert r.stop_seen_at is not None
        assert (r.stop_seen_at - r.fill_seen_at).total_seconds() == r.stop_latency_s
    summary = summarize(results)
    assert (summary.p50_s, summary.p99_s, summary.max_s) == (0.5, 0.75, 0.75)


async def test_a_stop_never_seen_fails_the_lifecycle_which_is_still_closed_and_ends_the_run(
    h: Harness,
) -> None:
    observer = LaggingObserver(AdapterObserver(h.exchange, h.clock), h, lags=[10**9] * 3, step_s=0.0)
    sink = ExecutionSink(h, None)
    (result,) = await _run(h, observer, sink, 3, stop_timeout_s=0.05)
    assert not result.ok
    assert result.error == "position without a covering STOP for 0.05 s"
    assert result.closed  # the driver closed it anyway
    assert (result.stop_seen_at, result.stop_latency_s) == (None, None)
    assert sink.batches == [[Leg.SL, Leg.TP1, Leg.ENTRY_IOC, Leg.ENTRY], [Leg.EXIT]]
    position = h.exchange.venue.positions.get(SOL)
    assert position is None or position.qty == 0
    assert working(h, symbol=SOL) == []


async def test_a_symbol_that_is_not_flat_is_left_alone(h: Harness) -> None:
    await open_position(h, symbol=SOL)  # someone else's SOLUSDT position with its STOP
    sink = ExecutionSink(h, None)
    (result,) = await _run(h, AdapterObserver(h.exchange, h.clock), sink, 3)
    assert not result.ok
    assert result.error is not None
    assert "not flat before the lifecycle" in result.error
    assert sink.batches == []
    assert h.exchange.venue.positions[SOL].qty == Decimal("9")


async def test_no_testnet_account_state_fails_before_sending(h: Harness, redis_client: Redis) -> None:
    testnet = AccountStatePublisher(h.ns, redis_client, h.kill, interval_s=5.0, maxlen=None)
    await testnet.publish_once()  # a testnet state exists, but the observer looks for live
    sink = ExecutionSink(h, None)
    (result,) = await _run(h, AccountStateObserver(redis_client, Account.LIVE), sink, 2)
    assert result.error == "no testnet AccountState on stream account_state (is execution running testnet?)"
    assert sink.batches == []


async def test_demo_market_data_failure_ends_the_run_without_sending(h: Harness) -> None:
    routes = _demo_routes(h)
    routes.always(
        "GET", "/fapi/v1/ticker/bookTicker", error(503, -1001, "Internal error; unable to process.")
    )
    sink = ExecutionSink(h, None)
    async with httpx.AsyncClient(transport=routes.transport()) as http:
        results = await run_lifecycles(
            market=DemoMarket(http, DEMO),
            sink=sink,
            observer=AdapterObserver(h.exchange, h.clock),
            signer=DRIVER_SIGNER,
            params=PARAMS,
            n=3,
            poll_s=0.001,
            run_id="t",
            clock=h.clock,
        )
    (result,) = results
    assert result.error == "MarketDataError: GET /fapi/v1/ticker/bookTicker failed: HTTPStatusError"
    assert sink.batches == []
