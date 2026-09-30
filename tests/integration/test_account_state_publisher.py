"""AccountState publisher (phase 09 section 5, step 10) over a namespace built like the service builds it.

- nothing is published while the namespace is not synced (Risk's staleness veto covers a lost stream);
- a published state mirrors the exchange (equity, positions) and the ledger (open orders, stops);
- cadence: at once on a change (at most one per `MIN_SPACING_S`), else every `interval_s`;
- the daily loss: a warning at half the limit (once a day), the kill at the limit, stops kept; the warning
  of a UTC day resolves once the day has rolled, so the next day's warning pages again (review cycle 2, N12).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable
from decimal import Decimal
from typing import Any

import pytest
import sqlalchemy as sa
from redis.asyncio import Redis
from sqlalchemy import Engine

from alert_logs import raised as _raised
from fake_exchange import (
    FILTERS,
    SOL,
    FakeClock,
    FakeExchange,
    Harness,
    ledger_sessions,
    open_position,
    seed_market,
    working,
)
from hdt.contracts.account import AccountState
from hdt.contracts.common import Account, Leg, OrderType
from hdt.contracts.streams import Stream
from hdt.db.models.ledger import EquitySnapshotRow
from hdt.db.models.ops import AlertRow
from hdt.execution import account_state, actions
from hdt.execution.account_state import AccountStatePublisher
from hdt.execution.main import NamespaceRuntime, build_namespace
from intent_builders import KEYRING
from risk_builders import risk_file

pytestmark = [pytest.mark.pg, pytest.mark.redis, pytest.mark.integration]


@pytest.fixture(autouse=True)
def _no_retry_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(actions, "CANCEL_CONFIRM_BACKOFF_S", 0.0)
    monkeypatch.setattr(actions, "UNKNOWN_VERIFY_DELAY_S", 0.0)


def _runtime(pg_engine: Engine, redis: Redis) -> tuple[NamespaceRuntime, Harness]:
    clock = FakeClock()
    exchange = FakeExchange(Account.PAPER, clock, FILTERS)
    rt = build_namespace(
        Account.PAPER,
        exchange,
        sessions=ledger_sessions(pg_engine),
        risk=risk_file,
        keyring=KEYRING,
        redis=redis,
        allowlist=lambda: frozenset({SOL}),
        clock=clock,
    )
    seed_market(exchange)
    h = Harness(rt.ns, exchange, clock, rt.guard, rt.manager, rt.kill, rt.reconciler, rt.resync, rt.intake)
    return rt, h


async def _published(redis: Redis) -> list[AccountState]:
    entries: Any = await redis.xrange(str(Stream.ACCOUNT_STATE))
    return [AccountState.model_validate_json(fields[b"data"]) for _id, fields in entries]


def _snapshots(h: Harness) -> int:
    with h.ns.sessions() as s:
        return int(s.scalar(sa.select(sa.func.count()).select_from(EquitySnapshotRow)) or 0)


def test_build_namespace_refuses_an_adapter_of_another_account(pg_engine: Engine) -> None:
    exchange = FakeExchange(Account.TESTNET, FakeClock(), FILTERS)
    with pytest.raises(ValueError, match="cannot serve"):
        build_namespace(
            Account.PAPER,
            exchange,
            sessions=ledger_sessions(pg_engine),
            risk=risk_file,
            keyring=KEYRING,
            redis=None,
        )


async def test_state_is_published_only_while_synced_and_mirrors_exchange_and_ledger(
    pg_engine: Engine, redis_client: Redis
) -> None:
    rt, h = _runtime(pg_engine, redis_client)
    assert rt.publisher.interval_s == float(risk_file().account_state_interval_s)
    assert await rt.publisher.publish_once() is None  # before the first re-sync
    assert await _published(redis_client) == []

    await h.start()
    await open_position(h)  # SOLUSDT 9 long, STOP 98.00, TP1 104.00
    state = await rt.publisher.publish_once()
    assert state is not None
    (streamed,) = await _published(redis_client)
    assert streamed == state
    balance = h.exchange.venue.balance()
    assert (state.account, state.equity, state.available) == (
        Account.PAPER,
        balance.equity,
        balance.available,
    )
    (pos,) = state.positions
    assert (pos.symbol, pos.qty, pos.is_hedge_book) == (SOL, Decimal("9"), False)
    stops = [a for a in state.open_algo_orders if a.order_type is OrderType.STOP_MARKET]
    (stop,) = working(h, Leg.SL)
    assert [(a.client_algo_id, a.qty, a.trigger_price) for a in stops] == [
        (stop.client_algo_id, Decimal("9"), Decimal("98.00"))
    ]
    assert {a.order_type for a in state.open_algo_orders} == {
        OrderType.STOP_MARKET,
        OrderType.TAKE_PROFIT_MARKET,
    }
    assert state.day_start_equity == state.equity  # the first equity of the UTC day is the anchor
    assert state.ts == h.clock()

    await rt.publisher.publish_once()
    assert _snapshots(h) == 1  # at most one equity snapshot a minute
    h.clock.advance(61)
    await rt.publisher.publish_once()
    assert _snapshots(h) == 2

    h.resync.begin()  # the user stream is lost: RESYNCING
    assert await rt.publisher.publish_once() is None
    assert len(await _published(redis_client)) == 3


async def _until(predicate: Callable[[], bool], timeout: float) -> float:
    loop = asyncio.get_running_loop()
    start = loop.time()
    while not predicate():
        assert loop.time() - start < timeout, "condition not reached in time"
        await asyncio.sleep(0.01)
    return loop.time() - start


async def test_cadence_is_on_change_or_every_interval_and_silent_while_resyncing(
    pg_engine: Engine, redis_client: Redis, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(account_state, "MIN_SPACING_S", 0.1)
    rt, h = _runtime(pg_engine, redis_client)
    await h.start()
    publisher = AccountStatePublisher(rt.ns, None, rt.kill, interval_s=1.0, maxlen=None)
    count = 0
    publish_once = publisher.publish_once

    async def counted() -> AccountState | None:
        nonlocal count
        state = await publish_once()
        count += state is not None
        return state

    publisher.publish_once = counted
    stop = asyncio.Event()
    task = asyncio.create_task(publisher.run(stop))
    try:
        await _until(lambda: count == 1, 2.0)  # at once on start
        await asyncio.sleep(0.5)
        assert count == 1  # nothing changed, interval not over

        rt.ns.notify()  # a fill, an order update, a kill
        assert await _until(lambda: count == 2, 0.5) < 0.4  # well before the 1 s interval
        await asyncio.sleep(0.5)
        assert count == 2
        assert await _until(lambda: count == 3, 1.5) > 0.2  # the interval, without any change

        h.resync.begin()
        seen = count
        rt.ns.notify()
        await asyncio.sleep(1.5)  # a change and a whole interval: still nothing while re-syncing
        assert count == seen
        await h.resync.run()
        await _until(lambda: count > seen, 1.5)
    finally:
        stop.set()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=2.0)
    assert task.done()


def _alert_kinds(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [r.alert_kind for r in caplog.records if _raised(r)]


async def test_daily_loss_warns_at_minus_one_and_a_half_once_and_kills_at_the_limit_keeping_the_stop(
    pg_engine: Engine, redis_client: Redis, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING)
    rt, h = _runtime(pg_engine, redis_client)
    await h.start()
    await open_position(h)
    first = await rt.publisher.publish_once()  # anchors the day
    assert first is not None
    day_start = first.day_start_equity
    unrealized = h.exchange.venue.balance().unrealized_pnl

    h.exchange.venue.wallet = day_start * Decimal("0.988") - unrealized  # -1.2%: not yet the -1.5% level
    await rt.publisher.publish_once()
    assert _alert_kinds(caplog) == []
    h.exchange.venue.wallet = day_start * Decimal("0.984") - unrealized  # -1.6%: past -1.5% (P10 M1)
    await rt.publisher.publish_once()
    await rt.publisher.publish_once()
    assert _alert_kinds(caplog) == ["daily_loss_warning"]  # once a day
    kill = h.ledger.kill_state()
    assert kill is None or kill.state != "killed"

    h.exchange.venue.wallet = day_start * Decimal("0.98") - unrealized  # -2.0%: the limit
    state = await rt.publisher.publish_once()
    assert state is not None  # still published: Risk sees the equity that killed the namespace
    assert state.day_start_equity == day_start
    kill = h.ledger.kill_state()
    assert kill is not None
    assert (kill.state, kill.cause) == ("killed", "daily_loss")
    (stop,) = working(h, Leg.SL)
    assert stop.qty == Decimal("9")  # the kill never removes a STOP
    assert working(h, Leg.TP1) == []
    assert "kill_switch" in _alert_kinds(caplog)


def _warning_open(h: Harness, day: str) -> bool | None:
    """Is the latest `daily_loss_warning` of `day` open (None: never raised); the outbox is run-wide."""
    with h.ns.sessions() as s:
        latest = s.execute(
            sa.select(AlertRow.resolved_at)
            .where(AlertRow.kind == "daily_loss_warning", AlertRow.dedupe_key == f"{h.ns.name}:{day}")
            .order_by(AlertRow.raised_at.desc())
            .limit(1)
        ).one_or_none()
    return None if latest is None else latest[0] is None


async def test_the_daily_loss_warning_of_a_past_day_resolves_after_the_day_rolls(
    pg_engine: Engine, redis_client: Redis, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING)
    rt, h = _runtime(pg_engine, redis_client)
    await h.start()
    await open_position(h)
    first = await rt.publisher.publish_once()
    assert first is not None
    unrealized = h.exchange.venue.balance().unrealized_pnl
    h.exchange.venue.wallet = first.day_start_equity * Decimal("0.984") - unrealized  # -1.6%: warned
    await rt.publisher.publish_once()
    day1 = h.clock().date().isoformat()
    assert _warning_open(h, day1) is True

    h.clock.advance(24 * 3600)  # a new UTC day re-anchors the day-start equity
    await rt.publisher.publish_once()
    day2 = h.clock().date().isoformat()
    assert _warning_open(h, day1) is False
    assert _warning_open(h, day2) is None  # no loss on the new day yet
    assert _alert_kinds(caplog) == ["daily_loss_warning"]
