"""STOP invariant (phase 09 section 5): every open position always has a covering STOP on the exchange.

Runs the execution namespace (ledger on the dev Postgres) against the in-process exchange.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy import Engine

from alert_logs import raised as _raised
from fake_exchange import SOL, Harness, ledger_sessions, leg_of_call, make_harness, open_position, working
from hdt.contracts.common import Account, Leg, OrderSide, Side
from hdt.contracts.order import OrderIntent
from hdt.execution import actions, stop_guard
from hdt.execution.adapter_base import (
    AlgoEvent,
    AlgoRequest,
    AlgoSnapshot,
    ExchangeError,
    IpBannedError,
    OrderRejectedError,
    OrderRequest,
    OrderSnapshot,
    RateLimitedError,
)
from hdt.execution.client_ids import parse_client_id
from hdt.execution.order_manager import EXIT_RETRY_S
from intent_builders import KEYRING, exit_intent
from risk_builders import COIN_ID, flags

pytestmark = pytest.mark.pg


@pytest.fixture(autouse=True)
def _no_retry_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(actions, "CANCEL_CONFIRM_BACKOFF_S", 0.0)
    monkeypatch.setattr(actions, "UNKNOWN_VERIFY_DELAY_S", 0.0)
    monkeypatch.setattr(stop_guard, "RATE_LIMIT_WAIT_MAX_S", 0.0)


@pytest.fixture
async def h(pg_engine: Engine) -> Harness:
    harness = make_harness(ledger_sessions(pg_engine), Account.PAPER, KEYRING)
    await harness.start()
    return harness


def _alerts(caplog: pytest.LogCaptureFixture, kind: str) -> list[logging.LogRecord]:
    return [r for r in caplog.records if _raised(r, kind)]


async def test_fill_puts_a_covering_stop_on_the_exchange_before_the_take_profit(h: Harness) -> None:
    await open_position(h)
    assert h.exchange.venue.positions[SOL].qty == Decimal("9")
    (stop,) = working(h, Leg.SL)
    assert stop.order_type == "STOP_MARKET"
    assert stop.side is OrderSide.SELL
    assert stop.qty == Decimal("9")
    assert stop.trigger_price == Decimal("98.00")
    assert stop.reduce_only
    (tp,) = working(h, Leg.TP1)
    assert tp.qty == Decimal("4.500")
    assert tp.trigger_price == Decimal("104.00")
    algo_calls = h.exchange.mutating_calls("place_algo")
    assert algo_calls == [f"place_algo:{stop.client_algo_id}", f"place_algo:{tp.client_algo_id}"]
    pos = h.ledger.position(SOL)
    assert pos is not None
    assert (pos.stop_price, pos.tp1_price, pos.trail_distance) == (
        Decimal("98.00"),
        Decimal("104.00"),
        Decimal("1.50"),
    )


async def test_stop_follows_partial_fills_new_stop_first_then_the_old_one_cancelled(h: Harness) -> None:
    await open_position(h, fill="3")
    (first,) = working(h, Leg.SL)
    assert first.qty == Decimal("3")
    h.exchange.trade(SOL, "100.00", "6", buyer_maker=True)
    await h.pump()
    (stop,) = working(h, Leg.SL)
    assert stop.qty == Decimal("9")
    assert stop.client_algo_id != first.client_algo_id
    calls = h.exchange.calls
    assert calls.index(f"place_algo:{stop.client_algo_id}") < calls.index(
        f"cancel_algo:{first.client_algo_id}"
    )
    (tp,) = working(h, Leg.TP1)
    assert tp.qty == Decimal("4.500")


async def test_stop_cancelled_on_the_exchange_is_restored_from_the_event(h: Harness) -> None:
    await open_position(h)
    (lost,) = working(h, Leg.SL)
    h.exchange.external_cancel_algo(lost.client_algo_id)
    await h.pump()
    (stop,) = working(h, Leg.SL)
    assert stop.client_algo_id != lost.client_algo_id
    assert (stop.qty, stop.trigger_price) == (Decimal("9"), Decimal("98.00"))


async def test_stop_lost_while_the_stream_is_down_is_restored_by_the_reconciler(h: Harness) -> None:
    await open_position(h)
    (lost,) = working(h, Leg.SL)
    h.exchange.stream_up = False
    h.exchange.external_cancel_algo(lost.client_algo_id)
    h.exchange.stream_up = True
    report = await h.reconcile()
    (stop,) = working(h, Leg.SL)
    assert stop.client_algo_id != lost.client_algo_id
    assert stop.qty == Decimal("9")
    assert report.clean


async def test_stop_the_exchange_forgot_is_replaced_by_the_invariant_check(h: Harness) -> None:
    await open_position(h)
    (lost,) = working(h, Leg.SL)
    del h.exchange.venue.algos[lost.client_algo_id]  # the ledger still believes it is working
    report = await h.reconcile()
    assert not report.stop_invariant_ok
    (stop,) = working(h, Leg.SL)
    assert (stop.qty, stop.trigger_price) == (Decimal("9"), Decimal("98.00"))
    row = h.ledger.algo(lost.client_algo_id)
    assert row is not None
    assert row.status == "CANCELED"
    assert (await h.reconcile()).stop_invariant_ok


async def test_two_failed_stop_placements_close_the_position_reduce_only(
    h: Harness, caplog: pytest.LogCaptureFixture
) -> None:
    h.exchange.reject_algos = 2
    await open_position(h)
    legs = [(c.split(":")[0], leg_of_call(c)) for c in h.exchange.calls]
    # stop_place_max_attempts = 2, then the reduce-only close goes out before anything else
    assert legs[:4] == [
        ("place_order", Leg.ENTRY),
        ("place_algo", Leg.SL),
        ("place_algo", Leg.SL),
        ("place_order", Leg.EXIT),
    ]
    assert SOL not in h.exchange.venue.positions
    assert h.ledger.position(SOL) is None
    assert working(h) == []
    alerts = _alerts(caplog, "stop_invariant")
    assert alerts
    assert {r.levelno for r in alerts} == {logging.CRITICAL}


async def test_stop_already_crossed_closes_at_once_without_retrying(h: Harness) -> None:
    await open_position(h, mark_before_fill="97.95")
    legs = [(c.split(":")[0], leg_of_call(c)) for c in h.exchange.calls]
    assert legs[:3] == [("place_order", Leg.ENTRY), ("place_algo", Leg.SL), ("place_order", Leg.EXIT)]
    assert SOL not in h.exchange.venue.positions
    assert h.ledger.position(SOL) is None
    assert working(h) == []


async def test_trailing_stop_moves_only_in_the_favourable_direction(h: Harness) -> None:
    await open_position(h)
    h.exchange.set_mark(SOL, "103.00")
    await h.tick(1)
    assert working(h, Leg.TRAIL) == []  # not activated below TP1
    h.exchange.set_book(SOL, [("104.40", "50")], [("104.50", "50")])
    h.exchange.set_mark(SOL, "104.50")  # TP1 triggers: half the position is taken
    await h.pump()
    assert h.exchange.venue.positions[SOL].qty == Decimal("4.500")
    await h.tick(1)
    (trail,) = working(h, Leg.TRAIL)
    assert (trail.trigger_price, trail.qty) == (Decimal("103.00"), Decimal("4.500"))
    assert working(h, Leg.SL) == []  # superseded, cancelled after the new stop was placed
    h.exchange.set_mark(SOL, "103.60")
    await h.tick(1)
    assert [a.client_algo_id for a in working(h, Leg.TRAIL)] == [trail.client_algo_id]
    h.exchange.set_book(SOL, [("105.40", "50")], [("105.50", "50")])
    h.exchange.set_mark(SOL, "105.50")
    await h.tick(1)
    (higher,) = working(h, Leg.TRAIL)
    assert higher.trigger_price == Decimal("104.00")
    h.exchange.set_book(SOL, [("103.80", "50")], [("103.90", "50")])
    h.exchange.set_mark(SOL, "103.90")  # the trailed stop triggers
    await h.pump()
    assert SOL not in h.exchange.venue.positions
    assert h.ledger.position(SOL) is None
    assert working(h) == []


async def test_time_stop_closes_the_position_and_its_orders(h: Harness) -> None:
    await open_position(h)
    await h.tick(timedelta(hours=12).total_seconds() + 1)
    await h.pump()
    assert SOL not in h.exchange.venue.positions
    assert working(h) == []


async def test_hard_veto_on_a_held_coin_exits_the_position(h: Harness) -> None:
    await open_position(h)
    h.ns.coin_symbol = lambda coin: SOL if coin == COIN_ID else None
    h.ns.flags.offer(flags(veto_long=True, now=h.clock()))
    await h.tick(1)
    await h.pump()
    assert SOL not in h.exchange.venue.positions
    closed = h.ledger.open_positions()
    assert closed == []


async def test_stop_placement_that_was_not_executed_counts_toward_the_reduce_only_close(
    h: Harness, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = h.exchange.place_algo
    refused: list[str] = []

    async def not_executed(request: AlgoRequest) -> AlgoSnapshot:
        if len(refused) < 2:
            refused.append(request.client_algo_id)
            raise ExchangeError("POST /fapi/v1/algoOrder -> HTTP 503: Service Unavailable", http_status=503)
        return await original(request)

    monkeypatch.setattr(h.exchange, "place_algo", not_executed)
    await open_position(h)
    assert len(refused) == 2  # stop_place_max_attempts = 2, then the reduce-only close
    assert [leg_of_call(c) for c in h.exchange.mutating_calls("place_order")] == [Leg.ENTRY, Leg.EXIT]
    assert SOL not in h.exchange.venue.positions
    assert h.ledger.position(SOL) is None
    for cid in refused:
        row = h.ledger.algo(cid)
        assert row is not None
        assert row.status == "REJECTED"
    assert {r.levelno for r in _alerts(caplog, "stop_invariant")} == {logging.CRITICAL}


class Latch:
    """The real client's 429 / 418 latch (`binance_client`): once the exchange refused one request, every
    later request is refused locally, nothing sent, until the window ends."""

    MUTATING = ("place_order", "place_algo", "cancel_order", "cancel_algo")

    def __init__(self, error: Callable[[float], ExchangeError], window_s: float) -> None:
        self.error = error
        self.window_s = window_s
        self.until: float | None = None
        self.refused: list[str] = []

    def arm(self, h: Harness, monkeypatch: pytest.MonkeyPatch, *, first: str) -> None:
        for name in self.MUTATING:
            monkeypatch.setattr(h.exchange, name, self._guard(name, getattr(h.exchange, name), name == first))

    def release(self) -> None:
        self.until = 0.0

    def _guard(self, name: str, send: Callable[..., Awaitable[Any]], trips: bool) -> Callable[..., Any]:
        async def call(*args: Any) -> Any:
            now = time.monotonic()
            if self.until is None and trips:
                self.until = now + self.window_s  # the exchange's own 429 / 418 starts the window
            if self.until is not None and now < self.until:
                self.refused.append(name)
                raise self.error(self.until - now)
            return await send(*args)

        return call


def _rate_limited(left: float) -> ExchangeError:
    return RateLimitedError(f"not sent: rate limited for {left:.2f} s more", retry_after_s=left)


def _banned(left: float) -> ExchangeError:
    return IpBannedError(f"not sent: IP banned for {left:.0f} s more")


async def test_rate_limited_stop_waits_out_the_window_before_the_second_attempt(
    h: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(stop_guard, "RATE_LIMIT_WAIT_MAX_S", 1.0)
    await open_position(h, fill=None)
    latch = Latch(_rate_limited, window_s=0.05)
    latch.arm(h, monkeypatch, first="place_algo")
    h.exchange.trade(SOL, "100.00", "14", buyer_maker=True)  # queue 5 ahead, then the 9 fill
    await h.pump()
    assert latch.refused == ["place_algo"]  # the second attempt went out after the window
    (stop,) = working(h, Leg.SL)
    assert (stop.qty, stop.trigger_price) == (Decimal("9"), Decimal("98.00"))
    assert _exit_orders(h) == []
    assert h.exchange.venue.positions[SOL].qty == Decimal("9")


@pytest.mark.parametrize(("error", "window_s"), [(_rate_limited, 30.0), (_banned, 120.0)], ids=["429", "418"])
async def test_latched_client_leaves_the_fill_unprotected_until_the_next_reconcile(
    h: Harness,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    error: Callable[[float], ExchangeError],
    window_s: float,
) -> None:
    await open_position(h, fill=None)
    latch = Latch(error, window_s)
    latch.arm(h, monkeypatch, first="place_algo")
    h.exchange.trade(SOL, "100.00", "14", buyer_maker=True)
    await h.pump()  # the fill handler returns: neither the two STOP attempts nor the close raise
    assert latch.refused == ["place_algo", "place_algo", "place_order"]
    assert h.exchange.venue.positions[SOL].qty == Decimal("9")
    assert working(h) == []
    alerts = _alerts(caplog, "stop_invariant")
    assert {r.levelno for r in alerts} == {logging.CRITICAL}
    assert any("close not sent" in r.getMessage() for r in alerts)
    latch.release()
    report = await h.reconcile()
    assert not report.stop_invariant_ok
    (stop,) = working(h, Leg.SL)
    assert (stop.qty, stop.trigger_price) == (Decimal("9"), Decimal("98.00"))
    assert h.exchange.venue.positions[SOL].qty == Decimal("9")


async def test_a_fill_a_429_left_unprotected_gets_its_stop_when_the_window_ends(
    h: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Review cycle 3 m-a: the STOP used to wait for the next reconcile cycle, not for the 429 window."""
    await open_position(h, fill=None)
    latch = Latch(_rate_limited, window_s=30.0)
    latch.arm(h, monkeypatch, first="place_algo")
    h.exchange.trade(SOL, "100.00", "14", buyer_maker=True)
    await h.pump()  # both STOP attempts and the close are refused
    assert working(h) == []
    latch.release()  # the ledger clock, not the latch, decides when the guard contacts the exchange again
    await h.tick(29.0)
    assert working(h, Leg.SL) == []  # still inside the window: nothing sent
    await h.tick(1.5)
    (stop,) = working(h, Leg.SL)
    assert (stop.qty, stop.trigger_price) == (Decimal("9"), Decimal("98.00"))
    assert h.exchange.venue.positions[SOL].qty == Decimal("9")


async def test_pending_stop_row_does_not_count_as_covering(h: Harness) -> None:
    await open_position(h)
    (lost,) = working(h, Leg.SL)
    del h.exchange.venue.algos[lost.client_algo_id]  # the request never landed (crash before the send)
    h.ledger.mark_algo(lost.client_algo_id, "PENDING")
    async with h.ns.lock:
        assert await h.guard.protect(SOL)
    (stop,) = working(h, Leg.SL)
    assert stop.client_algo_id != lost.client_algo_id
    assert (stop.qty, stop.trigger_price) == (Decimal("9"), Decimal("98.00"))
    row = h.ledger.algo(lost.client_algo_id)
    assert row is not None
    assert row.status == "CANCELED"


async def test_tp1_fill_replaces_the_stop_for_the_remaining_quantity(h: Harness) -> None:
    await open_position(h)
    (first,) = working(h, Leg.SL)
    h.exchange.set_book(SOL, [("104.40", "50")], [("104.50", "50")])
    h.exchange.set_mark(SOL, "104.50")  # TP1 triggers: half the position is taken
    await h.pump()
    assert h.exchange.venue.positions[SOL].qty == Decimal("4.500")
    (stop,) = working(h, Leg.SL)
    assert (stop.qty, stop.trigger_price) == (Decimal("4.500"), Decimal("98.00"))
    calls = h.exchange.calls
    assert calls.index(f"place_algo:{stop.client_algo_id}") < calls.index(
        f"cancel_algo:{first.client_algo_id}"
    )
    assert working(h, Leg.TP1) == []


def _exit_orders(h: Harness) -> list[str]:
    return [c for c in h.exchange.mutating_calls("place_order") if leg_of_call(c) is Leg.EXIT]


@pytest.mark.parametrize(
    "error",
    [
        OrderRejectedError("Limit price can't be lower than the allowed range", code=-4131),
        RateLimitedError("POST /fapi/v1/order -> HTTP 429: too many requests", retry_after_s=5.0),
    ],
    ids=["rejected", "429"],
)
async def test_time_stop_close_that_was_not_placed_is_retried(
    h: Harness, monkeypatch: pytest.MonkeyPatch, error: ExchangeError
) -> None:
    await open_position(h)
    original = h.exchange.place_order
    rejected: list[str] = []

    async def reject_first_close(request: OrderRequest) -> OrderSnapshot:
        parsed = parse_client_id(request.client_id)
        if parsed is not None and parsed.leg is Leg.EXIT and not rejected:
            rejected.append(request.client_id)
            raise error
        return await original(request)

    monkeypatch.setattr(h.exchange, "place_order", reject_first_close)
    await h.tick(timedelta(hours=12).total_seconds() + 1)
    assert len(rejected) == 1
    pos = h.ledger.position(SOL)
    assert pos is not None
    assert not pos.exit_in_progress  # still managed (trailing, stop) while the close waits
    await h.tick(1)
    assert _exit_orders(h) == []  # not retried before the delay
    await h.tick(EXIT_RETRY_S)
    await h.pump()
    assert len(_exit_orders(h)) == 1
    assert SOL not in h.exchange.venue.positions
    assert h.ledger.position(SOL) is None
    assert working(h) == []


async def test_stop_that_triggered_without_executing_hands_the_position_back_and_retries_the_close(
    h: Harness,
) -> None:
    await open_position(h)
    (stop,) = working(h, Leg.SL)
    await h.manager.on_algo_event(AlgoEvent(replace(stop, status="TRIGGERED")))
    pos = h.ledger.position(SOL)
    assert pos is not None
    assert pos.exit_in_progress
    await h.manager.on_algo_event(AlgoEvent(replace(stop, status="EXPIRED")))  # its order never executed
    pos = h.ledger.position(SOL)
    assert pos is not None
    assert not pos.exit_in_progress
    (fresh,) = [a for a in working(h, Leg.SL) if a.client_algo_id != stop.client_algo_id]
    assert (fresh.qty, fresh.trigger_price) == (Decimal("9"), Decimal("98.00"))
    await h.tick(1)
    assert _exit_orders(h) == []  # managed again; the close the stop did not make waits for its retry
    await h.tick(EXIT_RETRY_S)
    await h.pump()
    assert len(_exit_orders(h)) == 1
    assert SOL not in h.exchange.venue.positions
    assert h.ledger.position(SOL) is None
    assert working(h) == []


async def test_stop_finished_without_executing_is_closed_again(h: Harness) -> None:
    await open_position(h)
    (stop,) = working(h, Leg.SL)
    # The algo reports FINISHED but its triggered order expired: no fill, no EXPIRED algo event.
    await h.manager.on_algo_event(AlgoEvent(replace(stop, status="FINISHED")))
    pos = h.ledger.position(SOL)
    assert pos is not None
    assert pos.exit_in_progress
    await h.tick(1)
    assert _exit_orders(h) == []  # the stop's own order may still be executing
    await h.tick(EXIT_RETRY_S)
    await h.pump()
    assert len(_exit_orders(h)) == 1
    assert SOL not in h.exchange.venue.positions
    assert h.ledger.position(SOL) is None
    assert working(h) == []


async def test_signed_exit_smaller_than_the_position_closes_all_of_it(h: Harness) -> None:
    await open_position(h)
    signed = exit_intent(
        account=Account.PAPER,
        event_id="evt-exit",
        symbol=SOL,
        held_side=Side.LONG,
        qty=Decimal("4"),  # Risk saw 4 before the rest of the entry filled
        now=h.clock(),
    )
    assert await h.send([signed]) == ["accepted"]
    await h.pump()
    assert len(_exit_orders(h)) == 2  # the signed 4, then the remaining 5
    assert SOL not in h.exchange.venue.positions
    assert h.ledger.position(SOL) is None
    assert working(h) == []


async def test_signed_exit_refused_by_the_rate_limit_is_retried(
    h: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    await open_position(h)
    original = h.exchange.place_order
    refused: list[str] = []

    async def rate_limited(request: OrderRequest) -> OrderSnapshot:
        parsed = parse_client_id(request.client_id)
        if parsed is not None and parsed.leg is Leg.EXIT and len(refused) < 2:
            refused.append(request.client_id)
            raise RateLimitedError("POST /fapi/v1/order not sent: rate limited", retry_after_s=5.0)
        return await original(request)

    monkeypatch.setattr(h.exchange, "place_order", rate_limited)
    signed = exit_intent(
        account=Account.PAPER,
        event_id="evt-exit",
        symbol=SOL,
        held_side=Side.LONG,
        qty=Decimal("9"),
        now=h.clock(),
    )
    assert await h.send([signed]) == ["accepted"]
    assert len(refused) == 2  # the signed exit, then the reduce-only close of the position
    pos = h.ledger.position(SOL)
    assert pos is not None
    assert not pos.exit_in_progress  # still managed while the close waits
    await h.tick(EXIT_RETRY_S)
    await h.pump()
    assert SOL not in h.exchange.venue.positions
    assert h.ledger.position(SOL) is None
    assert working(h) == []


async def _trail_at_103(h: Harness) -> tuple[list[OrderIntent], AlgoSnapshot]:
    """Open 9, TP1 takes half, then the trailing stop is placed at 103.00 for the remaining 4.5."""
    plan = await open_position(h)
    h.exchange.set_book(SOL, [("104.40", "50")], [("104.50", "50")])
    h.exchange.set_mark(SOL, "104.50")
    await h.pump()
    await h.tick(1)
    (trail,) = working(h, Leg.TRAIL)
    assert (trail.trigger_price, trail.qty) == (Decimal("103.00"), Decimal("4.500"))
    return plan, trail


async def test_failed_trailing_move_keeps_the_covering_stop(
    h: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, trail = await _trail_at_103(h)
    original = h.exchange.place_algo
    refused: list[str] = []

    async def unavailable(request: AlgoRequest) -> AlgoSnapshot:
        if len(refused) < 2:
            refused.append(request.client_algo_id)
            raise ExchangeError("POST /fapi/v1/algoOrder -> HTTP 503: Service Unavailable", http_status=503)
        return await original(request)

    monkeypatch.setattr(h.exchange, "place_algo", unavailable)
    h.exchange.set_book(SOL, [("105.40", "50")], [("105.50", "50")])
    h.exchange.set_mark(SOL, "105.50")
    await h.tick(1)
    await h.tick(1)
    assert len(refused) == 2  # one attempt per move; a failed move never closes the position
    assert _exit_orders(h) == []
    assert [a.client_algo_id for a in working(h, Leg.TRAIL)] == [trail.client_algo_id]
    assert h.exchange.venue.positions[SOL].qty == Decimal("4.500")
    pos = h.ledger.position(SOL)
    assert pos is not None
    assert pos.stop_price == Decimal("103.00")
    await h.tick(1)
    (higher,) = working(h, Leg.TRAIL)
    assert higher.trigger_price == Decimal("104.00")


async def test_re_driven_stop_leg_never_moves_the_trailed_stop_back(h: Harness) -> None:
    plan, trail = await _trail_at_103(h)
    sl = next(i for i in plan if i.leg is Leg.SL)
    placements = len(h.exchange.mutating_calls("place_algo"))
    async with h.ns.lock:
        await h.manager.act(sl)  # a re-delivered `sl` leg reaching the order manager again
    pos = h.ledger.position(SOL)
    assert pos is not None
    assert (pos.stop_price, pos.initial_stop_price) == (Decimal("103.00"), Decimal("98.00"))
    assert len(h.exchange.mutating_calls("place_algo")) == placements
    assert [a.client_algo_id for a in working(h, Leg.TRAIL)] == [trail.client_algo_id]
    assert working(h, Leg.SL) == []
