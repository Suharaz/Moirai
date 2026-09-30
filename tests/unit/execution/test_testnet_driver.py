"""Testnet driver: STOP latency percentiles and its exchange-anchored measurement, the CLI's exit codes,
demo market data validation."""

from __future__ import annotations

import functools
import json
import random
from collections.abc import Sequence
from datetime import timedelta
from decimal import Decimal
from typing import Any

import httpx
import pytest

from binance_shapes import ALGO_NEW, EXCHANGE_INFO, T0, BinanceRoutes
from hdt.contracts.common import Account, Leg, OrderType
from hdt.contracts.order import OrderIntent
from hdt.core.clock import utcnow
from hdt.core.config import read_env_value
from hdt.execution import testnet_driver
from hdt.execution.binance_adapter import BinanceAdapter, ms_to_dt
from hdt.execution.client_ids import client_id
from hdt.execution.testnet_driver import (
    EXIT_FAILED,
    EXIT_OK,
    EXIT_USAGE,
    OPEN_ALGO_PATH,
    BinanceStopProbe,
    DemoMarket,
    LifecycleResult,
    MarketDataError,
    Observation,
    main,
    run_lifecycles,
    summarize,
)
from hdt.risk import signer as signer_module
from hdt.risk.signer import DRIVER_KEY_ENV
from intent_builders import DRIVER_LINE, DRIVER_SIGNER

DEMO = "https://demo-fapi.test"
REDIS_ENV = "HDT_REDIS_URL"


def _result(index: int, latency: float | None, *, ok: bool = True) -> LifecycleResult:
    fill = utcnow()
    stop = None if latency is None else fill + timedelta(seconds=latency)
    return LifecycleResult(
        index,
        f"driver:t:{index}",
        "ETHUSDT",
        ok,
        None if ok else "boom",
        Decimal("0.01"),
        fill,
        stop,
        latency,
        True,
    )


# ---------------------------------------------------------------------------- percentiles


def test_twenty_lifecycles_nearest_rank_p50_p99_and_max() -> None:
    latencies = [round(0.05 * k, 2) for k in range(1, 21)]
    random.Random(7).shuffle(latencies)
    summary = summarize([_result(i, v) for i, v in enumerate(latencies)])
    assert (summary.n, summary.ok) == (20, 20)
    assert (summary.p50_s, summary.p99_s, summary.max_s) == (0.5, 1.0, 1.0)


def test_p99_of_a_hundred_lifecycles_is_the_99th_value_not_the_max() -> None:
    summary = summarize([_result(i, float(i + 1)) for i in range(100)])
    assert (summary.p50_s, summary.p99_s, summary.max_s) == (50.0, 99.0, 100.0)


def test_lifecycles_without_a_measured_stop_count_but_have_no_latency() -> None:
    summary = summarize([_result(0, 0.3), _result(1, None, ok=False)])
    assert (summary.n, summary.ok, summary.p50_s, summary.p99_s, summary.max_s) == (2, 1, 0.3, 0.3, 0.3)
    empty = summarize([_result(0, None, ok=False)])
    assert (empty.p50_s, empty.p99_s, empty.max_s) == (None, None, None)


# ---------------------------------------------------------------------------- CLI exit codes


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    """Only the process environment counts (not the developer's `.env`)."""
    env_only = functools.partial(read_env_value, dotenv=None)
    monkeypatch.setattr(testnet_driver, "read_env_value", env_only)
    monkeypatch.setattr(signer_module, "read_env_value", env_only)
    for name in (DRIVER_KEY_ENV, REDIS_ENV):
        monkeypatch.delenv(name, raising=False)
        monkeypatch.delenv(f"{name}_FILE", raising=False)
    return monkeypatch


class Runs:
    """Stands in for `run_lifecycles`, recording what the CLI asked for."""

    def __init__(self, results: Sequence[LifecycleResult]) -> None:
        self.results = list(results)
        self.kwargs: dict[str, Any] = {}

    async def __call__(self, **kwargs: Any) -> list[LifecycleResult]:
        self.kwargs = kwargs
        return self.results


def _ready(env: pytest.MonkeyPatch, results: Sequence[LifecycleResult]) -> Runs:
    env.setenv(DRIVER_KEY_ENV, DRIVER_LINE)
    env.setenv(REDIS_ENV, "redis://127.0.0.1:6399/0")  # never connected: the lifecycles are stood in
    runs = Runs(results)
    env.setattr(testnet_driver, "run_lifecycles", runs)
    return runs


def test_help_is_0_and_usage_errors_are_2(
    env: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["--help"]) == EXIT_OK
    assert main(["--no-such-flag"]) == EXIT_USAGE
    assert main(["--leverage", "5"]) == EXIT_USAGE  # above the 3x ceiling
    assert main(["-n", "0"]) == EXIT_USAGE
    assert main(["--stop-timeout-s", "0"]) == EXIT_USAGE
    capsys.readouterr()


def test_missing_driver_key_or_redis_url_is_2(
    env: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main([]) == EXIT_USAGE
    assert DRIVER_KEY_ENV in capsys.readouterr().err
    env.setenv(DRIVER_KEY_ENV, DRIVER_LINE)
    assert main([]) == EXIT_USAGE
    assert REDIS_ENV in capsys.readouterr().err


def test_all_lifecycles_ok_within_the_latency_bound_is_0(
    env: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    runs = _ready(env, [_result(0, 0.4), _result(1, 1.9), _result(2, 0.8)])
    code = main(
        ["-n", "3", "--symbol", "SOLUSDT", "--leverage", "3", "--poll-s", "0.2", "--stop-timeout-s", "4"]
    )
    assert code == EXIT_OK
    assert runs.kwargs["n"] == 3
    assert (runs.kwargs["params"].symbol, runs.kwargs["params"].leverage) == ("SOLUSDT", 3)
    assert (runs.kwargs["poll_s"], runs.kwargs["stop_timeout_s"]) == (0.2, 4.0)
    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert [line["index"] for line in lines[:-1]] == [0, 1, 2]
    assert lines[-1] == {
        "summary": {"n": 3, "ok": 3, "p50_s": 0.8, "p99_s": 1.9, "max_s": 1.9},
        "max_stop_latency_s": 2.0,
    }


@pytest.mark.parametrize(
    ("results", "argv", "code"),
    [
        ([_result(0, 0.4), _result(1, 2.5), _result(2, 0.8)], ["-n", "3"], EXIT_FAILED),  # p99 over 2 s
        (
            [_result(0, 0.4), _result(1, 2.5), _result(2, 0.8)],
            ["-n", "3", "--max-stop-latency-s", "3"],
            EXIT_OK,
        ),
        ([_result(0, 0.4), _result(1, None, ok=False)], ["-n", "3"], EXIT_FAILED),  # stopped at a failure
        ([_result(0, 0.4), _result(1, 0.5)], ["-n", "3"], EXIT_FAILED),  # fewer lifecycles than asked
    ],
)
def test_a_failed_lifecycle_or_a_slow_stop_is_1(
    env: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    results: list[LifecycleResult],
    argv: list[str],
    code: int,
) -> None:
    _ready(env, results)
    assert main(argv) == code
    capsys.readouterr()


# ---------------------------------------------------------------------------- demo market data


async def _market(routes: BinanceRoutes) -> tuple[DemoMarket, httpx.AsyncClient]:
    http = httpx.AsyncClient(transport=routes.transport())
    return DemoMarket(http, DEMO + "/"), http


async def test_filters_are_read_once_and_a_symbol_not_trading_is_refused() -> None:
    routes = BinanceRoutes()
    routes.always("GET", "/fapi/v1/exchangeInfo", EXCHANGE_INFO)
    market, http = await _market(routes)
    async with http:
        eth = await market.filters("ETHUSDT")
        assert (eth.tick_size, eth.step_size, eth.min_notional) == (
            Decimal("0.01"),
            Decimal("0.001"),
            Decimal("20"),
        )
        await market.filters("SOLUSDT")
        with pytest.raises(MarketDataError, match="OLDUSDT is not trading"):
            await market.filters("OLDUSDT")
    assert len(routes.calls("GET", "/fapi/v1/exchangeInfo")) == 1  # every listed symbol from one read


@pytest.mark.parametrize(
    ("book", "premium", "match"),
    [
        ({"bidPrice": "2500.00", "askPrice": "2499.99"}, {"markPrice": "2500"}, "unusable quote"),
        ({"bidPrice": "0", "askPrice": "2500.01"}, {"markPrice": "2500"}, "unusable quote"),
        ({"bidPrice": "2500.00", "askPrice": "2500.01"}, {"markPrice": "0"}, "unusable quote"),
        ({"bidPrice": "2500.00"}, {"markPrice": "2500"}, "incomplete quote"),
        ({"bidPrice": "2500.00", "askPrice": "x"}, {"markPrice": "2500"}, "incomplete quote"),
    ],
)
async def test_an_unusable_quote_is_refused(
    book: dict[str, str], premium: dict[str, str], match: str
) -> None:
    routes = BinanceRoutes()
    routes.always("GET", "/fapi/v1/ticker/bookTicker", book)
    routes.always("GET", "/fapi/v1/premiumIndex", premium)
    market, http = await _market(routes)
    async with http:
        with pytest.raises(MarketDataError, match=match):
            await market.quote("ETHUSDT")
    (sent,) = routes.calls("GET", "/fapi/v1/ticker/bookTicker")
    assert sent.params == {"symbol": "ETHUSDT"}
    assert "x-mbx-apikey" not in sent.headers  # public endpoints only: the driver holds no Binance key


# ---------------------------------------------------------------------------- exchange-anchored STOP latency

ETH = "ETHUSDT"
EVENT = "driver:t:0"  # the first lifecycle of run "t"
OTHER_EVENT = "driver:x:0"
ENTRY_ORDER_ID = 101
OTHER_ORDER_ID = 99


class DemoAccount:
    """The demo account as the driver and the probe see it. AccountState-style observations show the fill
    and its covering STOP in the same snapshot (a 1 Hz view that cannot tell them apart), while the exchange
    lists the STOP `stop_after_ms` after the fill."""

    def __init__(self, stop_after_ms: int, *, stop_covers: bool = True) -> None:
        self.stop_after_ms = stop_after_ms
        self.stop_covers = stop_covers
        self.intents: list[OrderIntent] = []

    async def send(self, intents: Sequence[OrderIntent]) -> None:
        self.intents.extend(intents)

    def _qty(self) -> Decimal | None:
        legs = {i.leg: i for i in self.intents}
        entry = legs.get(Leg.ENTRY_IOC)
        return None if entry is None or Leg.EXIT in legs else entry.qty

    async def observe(self) -> Observation:
        qty = self._qty()
        held = frozenset() if qty is None else frozenset({ETH})
        return Observation(utcnow(), {} if qty is None else {ETH: qty}, held, held, frozenset())

    def open_algos(self, _request: httpx.Request) -> httpx.Response:
        qty = self._qty()
        assert qty is not None
        stop_qty = qty if self.stop_covers else qty / 2

        def algo(event: str, leg: Leg, algo_id: int, created: int, **fields: Any) -> dict[str, Any]:
            return {
                **ALGO_NEW,
                "algoId": algo_id,
                "clientAlgoId": client_id(event, leg, 1 if event == EVENT else 0),
                "symbol": ETH,
                "quantity": str(qty),
                "createTime": created,
                "updateTime": created + 50_000,  # a later amendment must not move the listing time
                **fields,
            }

        return httpx.Response(
            200,
            json=[
                algo(OTHER_EVENT, Leg.SL, 1, T0 - 5_000),  # another lifecycle's STOP
                algo(EVENT, Leg.TP1, 2, T0 + 100, orderType=OrderType.TAKE_PROFIT_MARKET.value),
                algo(EVENT, Leg.SL, 3, T0 + self.stop_after_ms, quantity=str(stop_qty)),
            ],
        )


def _trade(order_id: int, trade_id: int, at_ms: int) -> dict[str, Any]:
    return {
        "symbol": ETH,
        "id": trade_id,
        "orderId": order_id,
        "side": "BUY",
        "price": "2500.00",
        "qty": "0.010",
        "commission": "0.01",
        "commissionAsset": "USDT",
        "realizedPnl": "0",
        "maker": False,
        "time": at_ms,
    }


def _exchange(account: DemoAccount) -> BinanceRoutes:
    ids = {
        ENTRY_ORDER_ID: client_id(EVENT, Leg.ENTRY_IOC, 0),
        OTHER_ORDER_ID: client_id(OTHER_EVENT, Leg.ENTRY_IOC, 0),
    }
    routes = BinanceRoutes()
    routes.always("GET", "/fapi/v1/exchangeInfo", EXCHANGE_INFO)
    routes.always("GET", "/fapi/v1/ticker/bookTicker", {"bidPrice": "2500.00", "askPrice": "2500.01"})
    routes.always("GET", "/fapi/v1/premiumIndex", {"markPrice": "2500.00"})
    routes.queue("GET", OPEN_ALGO_PATH, [])  # the first read: execution has not placed the STOP yet
    routes.always("GET", OPEN_ALGO_PATH, account.open_algos)
    routes.always(
        "GET",
        "/fapi/v1/userTrades",
        [
            _trade(OTHER_ORDER_ID, 1, T0 - 10_000),
            _trade(ENTRY_ORDER_ID, 2, T0),
            _trade(ENTRY_ORDER_ID, 3, T0 + 3),
        ],
    )
    routes.always(
        "GET",
        "/fapi/v1/order",
        lambda request: httpx.Response(
            200, json={"clientOrderId": ids[int(request.url.params["orderId"])], "symbol": ETH}
        ),
    )
    return routes


async def _lifecycle(account: DemoAccount) -> tuple[LifecycleResult, BinanceRoutes]:
    routes = _exchange(account)
    adapter = BinanceAdapter(Account.TESTNET, routes.client())
    probe = BinanceStopProbe(adapter, since=ms_to_dt(T0 - 60_000))
    async with httpx.AsyncClient(transport=routes.transport()) as http:
        (result,) = await run_lifecycles(
            market=DemoMarket(http, DEMO),
            sink=account,
            observer=account,
            signer=DRIVER_SIGNER,
            n=1,
            poll_s=0.001,
            stop_timeout_s=1.0,
            fill_timeout_s=1.0,
            close_timeout_s=1.0,
            run_id="t",
            probe=probe,
        )
    await adapter.aclose()
    return result, routes


async def test_stop_latency_runs_from_the_exchange_fill_to_the_listed_stop_not_the_snapshots() -> None:
    result, routes = await _lifecycle(DemoAccount(stop_after_ms=2_500))
    assert (result.ok, result.closed) == (True, True)
    assert result.exchange_stop is not None
    assert result.exchange_stop.client_algo_id == client_id(EVENT, Leg.SL, 1)
    assert result.exchange_stop.algo_id == 3
    assert (result.fill_seen_at, result.stop_seen_at) == (ms_to_dt(T0), ms_to_dt(T0 + 2_500))
    assert result.stop_latency_s == 2.5  # the snapshots showed fill and STOP together (0 s)
    assert summarize([result]).p99_s == 2.5  # over the 2 s bound
    assert len(routes.calls("GET", OPEN_ALGO_PATH)) >= 2  # polled until the STOP was listed


async def test_a_stop_that_does_not_cover_the_position_fails_the_lifecycle_which_is_still_closed() -> None:
    account = DemoAccount(stop_after_ms=300, stop_covers=False)
    result, _routes = await _lifecycle(account)
    assert result.error == "no covering STOP listed in openAlgoOrders for 1 s"
    assert (result.closed, result.stop_latency_s, result.exchange_stop) == (True, None, None)
    assert account.intents[-1].leg is Leg.EXIT
