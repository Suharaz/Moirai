"""Entry watchdog (phase 09 section 5): cancel, confirm, then IOC for the real shortfall only.

The IOC quantity comes from the exchange's `executedQty` in the confirmed cancel, never from the ledger
(fill events of a lost user stream may still be missing there), so the position never exceeds the signed
entry quantity. Every send of an entry (a redelivered one included) re-checks the limits first.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from decimal import Decimal

import pytest
import sqlalchemy as sa
from sqlalchemy import Engine

from alert_logs import raised as _raised
from fake_exchange import (
    ETH,
    SOL,
    Harness,
    ledger_sessions,
    leg_of_call,
    make_harness,
    open_position,
    seed_market,
    working,
)
from hdt.contracts.common import Account, Leg, Side
from hdt.db.models.ledger import ProcessedIntentRow
from hdt.execution import actions
from hdt.execution.adapter_base import ExchangeError, OrderRequest, OrderSnapshot
from intent_builders import KEYRING, open_plan
from risk_builders import COIN_ID, flags

pytestmark = pytest.mark.pg

WAIT_S = 200  # beyond post_only_wait_s (180) on the namespace clock


@pytest.fixture(autouse=True)
def _no_retry_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(actions, "CANCEL_CONFIRM_BACKOFF_S", 0.0)
    monkeypatch.setattr(actions, "UNKNOWN_VERIFY_DELAY_S", 0.0)


@pytest.fixture
async def h(pg_engine: Engine) -> Harness:
    harness = make_harness(ledger_sessions(pg_engine), Account.PAPER, KEYRING)
    await harness.start()
    return harness


def _ioc_orders(h: Harness) -> list[str]:
    return [
        c.split(":", 1)[1]
        for c in h.exchange.mutating_calls("place_order")
        if leg_of_call(c) is Leg.ENTRY_IOC
    ]


def _status(h: Harness, intent_id: str) -> str:
    with h.ns.sessions() as s:
        row = s.scalars(
            sa.select(ProcessedIntentRow).where(
                ProcessedIntentRow.account == h.ns.name, ProcessedIntentRow.intent_id == intent_id
            )
        ).one()
        return row.status


async def test_ioc_is_the_shortfall_of_the_exchange_executed_qty_not_the_ledger(h: Harness) -> None:
    plan = await open_position(h, fill=None)
    entry = plan[-1]
    h.exchange.stream_up = False  # the partial fill is never pushed
    h.exchange.trade(SOL, "100.00", str(Decimal(5) + Decimal(6)), buyer_maker=True)
    h.exchange.stream_up = True
    assert h.ledger.filled_qty(entry.client_id) == 0
    await h.tick(WAIT_S)
    (ioc_cid,) = _ioc_orders(h)
    calls = h.exchange.calls
    assert calls.index(f"cancel_order:{entry.client_id}") < calls.index(f"place_order:{ioc_cid}")
    ioc = h.exchange.venue.orders[ioc_cid]
    assert ioc.qty == Decimal("3")  # 9 signed - 6 executed on the exchange
    assert ioc.price == Decimal("100.10")
    assert ioc.tif == "IOC"
    await h.pump()
    assert h.exchange.venue.positions[SOL].qty == Decimal("9")
    pos = h.ledger.position(SOL)
    assert pos is not None
    assert pos.qty == Decimal("9")
    (stop,) = working(h, Leg.SL)
    assert stop.qty == Decimal("9")


async def test_no_ioc_while_the_cancel_is_not_confirmed(h: Harness) -> None:
    plan = await open_position(h, fill="4")
    entry = plan[-1]
    h.exchange.stuck_cancels = True
    await h.tick(WAIT_S)
    assert h.exchange.mutating_calls("cancel_order") == [f"cancel_order:{entry.client_id}"] * 3
    assert _ioc_orders(h) == []
    assert h.exchange.venue.orders[entry.client_id].status == "PARTIALLY_FILLED"
    h.exchange.stuck_cancels = False
    await h.tick(1)  # the watchdog tries again on its next pass
    (ioc_cid,) = _ioc_orders(h)
    assert h.exchange.venue.orders[ioc_cid].qty == Decimal("5")
    await h.pump()
    assert h.exchange.venue.positions[SOL].qty == Decimal("9")


async def test_post_only_that_would_take_goes_straight_to_a_single_ioc(h: Harness) -> None:
    h.exchange.set_book(SOL, [("99.90", "50")], [("100.00", "4"), ("100.10", "3"), ("100.20", "50")])
    plan = await open_position(h, fill=None)
    entry = plan[-1]
    assert h.exchange.venue.orders.get(entry.client_id) is None  # rejected -5022, nothing rests
    (ioc_cid,) = _ioc_orders(h)
    assert h.exchange.venue.orders[ioc_cid].qty == Decimal("9")
    assert h.exchange.venue.positions[SOL].qty == Decimal("7")  # 4 + 3 within the 100.10 limit
    assert await h.send([entry]) == ["duplicate"]
    await h.tick(WAIT_S)
    assert _ioc_orders(h) == [ioc_cid]
    assert h.exchange.venue.positions[SOL].qty == Decimal("7")
    (stop,) = working(h, Leg.SL)
    assert stop.qty == Decimal("7")


async def test_expired_entry_is_cancelled_without_an_ioc(h: Harness) -> None:
    plan = await open_position(h, fill="2", ttl=timedelta(seconds=120))
    await h.tick(130)  # past expires_at, before the post-only wait
    assert h.exchange.venue.orders[plan[-1].client_id].status == "CANCELED"
    assert _ioc_orders(h) == []
    assert h.exchange.venue.positions[SOL].qty == Decimal("2")
    (stop,) = working(h, Leg.SL)
    assert stop.qty == Decimal("2")


async def test_an_entry_without_expiry_rests_the_post_only_wait_then_falls_back_to_ioc(h: Harness) -> None:
    """Risk signs entries without `expires_at` (owner decision 2026-09-28): the entry must still be retired,
    by `post_only_wait_s` (180 s), never earlier by a decision TTL and never left resting for good."""
    plan = await open_position(h, fill="2")
    entry = plan[-1]
    assert entry.expires_at is None
    await h.tick(150)  # past the former 120 s decision TTL, before the post-only wait
    assert h.exchange.venue.orders[entry.client_id].status == "PARTIALLY_FILLED"
    assert _ioc_orders(h) == []
    await h.tick(WAIT_S - 150)
    assert h.exchange.venue.orders[entry.client_id].status == "CANCELED"
    (ioc_cid,) = _ioc_orders(h)
    assert h.exchange.venue.orders[ioc_cid].qty == Decimal("7")  # 9 signed - 2 executed


async def test_ioc_is_refused_while_the_namespace_resyncs(h: Harness) -> None:
    plan = await open_position(h, fill="2")
    h.resync.begin()
    await h.tick(WAIT_S)
    assert h.exchange.venue.orders[plan[-1].client_id].status == "CANCELED"
    assert _ioc_orders(h) == []
    ioc_intent = next(i for i in plan if i.leg is Leg.ENTRY_IOC)
    assert _status(h, ioc_intent.intent_id) == "void"
    assert h.exchange.venue.positions[SOL].qty == Decimal("2")


async def test_shortfall_below_the_exchange_minimum_sends_no_ioc(h: Harness) -> None:
    plan = await open_position(h, fill="8.96")  # 0.04 x 100.10 = 4.004 < minNotional 5
    await h.tick(WAIT_S)
    assert _ioc_orders(h) == []
    ioc_intent = next(i for i in plan if i.leg is Leg.ENTRY_IOC)
    assert _status(h, ioc_intent.intent_id) == "done"
    assert h.exchange.venue.positions[SOL].qty == Decimal("8.96")


async def test_filled_entry_is_left_alone(h: Harness) -> None:
    await open_position(h)
    await h.tick(WAIT_S)
    assert h.exchange.mutating_calls("cancel_order") == []
    assert _ioc_orders(h) == []


def _cap_alerts(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [r.getMessage() for r in caplog.records if _raised(r, "intent_signature")]


# Daily opening-notional cap: 3 x equity 10000 = 30000. Entry 9 x 100.00 = 900, IOC 9 x 100.10 = 900.90.
CAP = Decimal("30000")


async def test_ioc_replacing_a_post_only_that_would_take_counts_once_up_to_the_cap(h: Harness) -> None:
    today = h.clock().date()
    already = CAP - Decimal("900.90")  # the IOC fills the day exactly
    h.ledger.add_daily_notional(today, already)
    h.exchange.set_book(SOL, [("99.90", "50")], [("100.00", "4"), ("100.10", "3"), ("100.20", "50")])
    await open_position(h, fill=None)
    assert len(_ioc_orders(h)) == 1
    assert h.exchange.venue.positions[SOL].qty == Decimal("7")
    # The rejected entry's 900 is replaced by the IOC's 900.90, not counted on top of it.
    assert h.ledger.daily_notional(today) == CAP


@pytest.mark.parametrize(
    ("already", "first_ioc_sent"), [(Decimal("28199.50"), False), (Decimal("27000"), True)]
)
async def test_deferred_ioc_rechecks_the_daily_cap_after_other_entries_used_it(
    h: Harness, caplog: pytest.LogCaptureFixture, already: Decimal, first_ioc_sent: bool
) -> None:
    today = h.clock().date()
    h.ledger.add_daily_notional(today, already)
    first = await open_position(h, fill=None)  # rests; its IOC passed intake with the headroom of then
    seed_market(h.exchange, ETH)
    other = open_plan(
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
    second = await h.send(other)  # another event's entry uses the headroom meanwhile
    assert second[-1] == "accepted"
    assert h.ledger.daily_notional(today) == already + Decimal("1800")
    await h.tick(WAIT_S)  # watchdog: both entries cancelled, each IOC re-checked before it is sent

    first_ioc = next(i for i in first if i.leg is Leg.ENTRY_IOC)
    sent = [leg_of_call(c) for c in h.exchange.mutating_calls("place_order")]
    assert h.exchange.venue.orders[first[-1].client_id].status == "CANCELED"
    assert h.ledger.daily_notional(today) <= CAP
    if first_ioc_sent:
        assert sent.count(Leg.ENTRY_IOC) == 2
        assert _cap_alerts(caplog) == []
    else:
        # 28199.50 + 900 (second entry) + 900.90 (first IOC, replacing its entry's 900) > 30000
        assert h.exchange.venue.orders.get(first_ioc.client_id) is None
        assert _status(h, first_ioc.intent_id) == "void"
        assert any("entry_ioc" in m and "not sent" in m for m in _cap_alerts(caplog))
        assert [r.levelno for r in caplog.records if "not sent" in r.getMessage()] == [logging.WARNING]
        assert SOL not in h.exchange.venue.positions


async def test_redelivered_entry_that_never_left_rechecks_the_veto_before_it_is_sent(
    h: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    plan = open_plan(
        account=Account.PAPER,
        event_id="evt-1",
        symbol=SOL,
        side=Side.LONG,
        qty=Decimal("9"),
        entry=Decimal("100.00"),
        stop=Decimal("98.00"),
        tp1=Decimal("104.00"),
        ioc_price=Decimal("100.10"),
        now=h.clock(),
    )
    entry = plan[-1]
    assert await h.send(plan[:-1]) == ["accepted"] * 4
    original = h.exchange.place_order

    async def unavailable(request: OrderRequest) -> OrderSnapshot:
        raise ExchangeError("POST /fapi/v1/order -> HTTP 503: Service Unavailable", http_status=503)

    monkeypatch.setattr(h.exchange, "place_order", unavailable)
    with pytest.raises(ExchangeError):
        await h.send([entry])  # accepted and recorded PENDING, the request failed: not acked
    monkeypatch.setattr(h.exchange, "place_order", original)
    row = h.ledger.order(entry.client_id)
    assert row is not None
    assert row.status == "PENDING"
    today = h.clock().date()
    counted = h.ledger.daily_notional(today)
    h.ns.coin_symbol = lambda coin: SOL if coin == COIN_ID else None
    h.ns.flags.offer(flags(veto_long=True, now=h.clock()))  # the coin is vetoed before the redelivery
    assert await h.send([entry]) == ["duplicate"]
    assert entry.client_id not in h.exchange.venue.orders
    assert _status(h, entry.intent_id) == "void"
    assert h.ledger.daily_notional(today) == counted
    assert SOL not in h.exchange.venue.positions
