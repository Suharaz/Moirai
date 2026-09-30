"""Network unplugged for 60 s, then back (phase 09 success criterion).

The reconciler (run by the re-sync on reconnect) restores the correct state and no orphan order is left:
every order and conditional order on the exchange is known to the ledger, every position has a STOP,
and a redelivered intent never sends a second order.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from sqlalchemy import Engine

from fake_exchange import (
    ETH,
    SOL,
    Harness,
    ledger_sessions,
    make_harness,
    open_position,
    seed_market,
    working,
)
from hdt.contracts.common import Account, Side
from hdt.contracts.order import OrderIntent
from hdt.execution import actions
from hdt.execution.adapter_base import ExchangeError
from intent_builders import KEYRING, open_plan

pytestmark = [pytest.mark.pg, pytest.mark.integration]


@pytest.fixture(autouse=True)
def _no_retry_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(actions, "CANCEL_CONFIRM_BACKOFF_S", 0.0)
    monkeypatch.setattr(actions, "UNKNOWN_VERIFY_DELAY_S", 0.0)


@pytest.fixture
async def h(pg_engine: Engine) -> Harness:
    harness = make_harness(ledger_sessions(pg_engine), Account.PAPER, KEYRING)
    seed_market(harness.exchange, ETH)
    await harness.start()
    await open_position(harness)  # SOLUSDT 9 with its STOP and TP
    return harness


def _eth_plan(h: Harness) -> list[OrderIntent]:
    return open_plan(
        account=Account.PAPER,
        event_id="evt-2",
        symbol=ETH,
        side=Side.LONG,
        qty=Decimal("9"),
        entry=Decimal("100.00"),
        stop=Decimal("98.00"),
        tp1=Decimal("104.00"),
        ioc_price=Decimal("100.10"),
        now=h.clock(),
    )


async def _unplugged_for_60s_then_back(h: Harness) -> None:
    h.resync.begin()  # the user stream noticed the drop
    h.clock.advance(60)
    h.exchange.rest_up = True
    h.exchange.stream_up = True
    report = await h.resync.run()
    assert report.clean
    assert report.stop_invariant_ok


async def _assert_no_orphans(h: Harness) -> None:
    for order in await h.exchange.open_orders():
        assert h.ledger.order(order.client_id) is not None, order.client_id
    for algo in await h.exchange.open_algo_orders():
        assert h.ledger.algo(algo.client_algo_id) is not None, algo.client_algo_id
    for pos in await h.exchange.positions():
        stops = [a for a in working(h, symbol=pos.symbol) if a.order_type == "STOP_MARKET"]
        assert any(a.close_position or (a.qty or 0) >= abs(pos.qty) for a in stops), pos.symbol
        ledger = h.ledger.position(pos.symbol)
        assert ledger is not None
        assert ledger.qty == abs(pos.qty)
    kill = h.ledger.kill_state()
    assert kill is None or kill.state == "running"


async def test_entry_that_landed_just_before_the_drop_is_adopted_not_duplicated(h: Harness) -> None:
    plan = _eth_plan(h)
    entry = plan[-1]
    assert await h.send(plan[:-1]) == ["accepted"] * 4
    h.exchange.cut_after_landing = True
    with pytest.raises(ExchangeError):
        await h.send([entry])  # the consumer does not ack: the message will be redelivered
    assert h.exchange.venue.orders[entry.client_id].status == "NEW"
    h.exchange.trade(ETH, "100.00", "9", buyer_maker=True)  # 4 of 9 fill during the outage, unseen

    await _unplugged_for_60s_then_back(h)

    row = h.ledger.order(entry.client_id)
    assert row is not None
    assert row.status == "PARTIALLY_FILLED"
    eth = h.ledger.position(ETH)
    assert eth is not None
    assert eth.qty == Decimal("4")
    await _assert_no_orphans(h)

    assert await h.send([entry]) == ["duplicate"]  # redelivered after the outage
    assert h.exchange.mutating_calls(f"place_order:{entry.client_id}") == [f"place_order:{entry.client_id}"]
    await _assert_no_orphans(h)


async def test_entry_that_never_reached_the_exchange_is_sent_once_on_redelivery(h: Harness) -> None:
    plan = _eth_plan(h)
    entry = plan[-1]
    assert await h.send(plan[:-1]) == ["accepted"] * 4
    h.exchange.rest_up = False
    h.exchange.stream_up = False
    marks = h.exchange.mark_prices

    async def mark_only(symbols: object) -> object:  # the mark read still answers, the order call fails
        h.exchange.rest_up = True
        try:
            return await marks(symbols)  # type: ignore[arg-type]
        finally:
            h.exchange.rest_up = False

    h.exchange.mark_prices = mark_only  # type: ignore[method-assign,assignment]
    with pytest.raises(ExchangeError):
        await h.send([entry])  # accepted, then the exchange call failed: the consumer does not ack
    h.exchange.mark_prices = marks  # type: ignore[method-assign]
    assert entry.client_id not in h.exchange.venue.orders

    await _unplugged_for_60s_then_back(h)
    await _assert_no_orphans(h)
    assert h.ledger.open_orders(ETH) == []

    assert await h.send([entry]) == ["duplicate"]  # redelivered: the accepted intent is re-driven once
    assert await h.send([entry]) == ["duplicate"]
    assert h.exchange.mutating_calls(f"place_order:{entry.client_id}") == [f"place_order:{entry.client_id}"]
    assert h.exchange.venue.orders[entry.client_id].status == "NEW"
    await _assert_no_orphans(h)
    assert h.exchange.venue.positions[SOL].qty == Decimal("9")
