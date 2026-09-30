"""Kill switch (phase 09 section 7): entries and TPs go, every position keeps (or gets) its STOP.

Success criterion (kill part): after a kill every position still has a STOP on the exchange
(`openAlgoOrders`) and no entry or TP is left open.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
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
from hdt.contracts.common import Account, Leg, OrderSide, OrderType, Side, TimeInForce
from hdt.core.clock import utcnow
from hdt.db.models.ledger import KillStateRow
from hdt.db.models.ops import AlertRow
from hdt.db.session import transaction
from hdt.execution import actions, kill_switch
from hdt.execution.actions import Placed
from hdt.execution.adapter_base import ExchangeError, OrderRequest, OrderSnapshot
from hdt.execution.client_ids import client_id
from hdt.execution.kill_switch import KillSwitch, cli_command_id, resume_allowed
from hdt.execution.ledger import Ledger
from intent_builders import KEYRING, exit_intent, open_plan

pytestmark = pytest.mark.pg

T = datetime(2026, 9, 28, 12, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _no_retry_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(actions, "CANCEL_CONFIRM_BACKOFF_S", 0.0)
    monkeypatch.setattr(actions, "UNKNOWN_VERIFY_DELAY_S", 0.0)


@pytest.fixture
async def h(pg_engine: Engine) -> Harness:
    harness = make_harness(ledger_sessions(pg_engine), Account.PAPER, KEYRING)
    seed_market(harness.exchange, ETH)
    await harness.start()
    return harness


def _eth_plan(h: Harness, event_id: str) -> list:
    return open_plan(
        account=Account.PAPER,
        event_id=event_id,
        symbol=ETH,
        side=Side.LONG,
        qty=Decimal("5"),
        entry=Decimal("100.00"),
        stop=Decimal("98.00"),
        tp1=Decimal("104.00"),
        ioc_price=Decimal("100.10"),
        now=h.clock(),
    )


async def _engage(h: Harness, *, flatten: bool = False, cause: str = "manual"):
    async with h.ns.lock:
        return await h.kill.engage(cause, "operator test", flatten=flatten)  # type: ignore[arg-type]


async def test_kill_keeps_every_stop_and_removes_entries_and_take_profits(
    h: Harness, caplog: pytest.LogCaptureFixture
) -> None:
    await open_position(h)
    (stop,) = working(h, Leg.SL)
    (tp,) = working(h, Leg.TP1)
    eth = _eth_plan(h, "evt-2")
    assert await h.send(eth) == ["accepted"] * 5
    eth_entry = eth[-1].client_id
    assert h.exchange.venue.orders[eth_entry].status == "NEW"

    report = await _engage(h)

    assert report.safe
    assert report.cancelled_tps == 1
    assert report.cancelled_entries >= 1
    assert f"cancel_algo:{stop.client_algo_id}" not in h.exchange.calls
    assert [a.client_algo_id for a in working(h, Leg.SL)] == [stop.client_algo_id]
    assert working(h, Leg.TP1) == []
    assert h.exchange.venue.algos[tp.client_algo_id].status == "CANCELED"
    assert h.exchange.venue.orders[eth_entry].status == "CANCELED"
    assert await h.exchange.open_orders() == []
    kill = h.ledger.kill_state()
    assert kill is not None
    assert (kill.state, kill.cause) == ("killed", "manual")
    assert [r.levelno for r in caplog.records if _raised(r, "kill_switch")] == [logging.CRITICAL]


async def test_kill_places_the_stop_a_position_was_missing(h: Harness) -> None:
    await open_position(h)
    (lost,) = working(h, Leg.SL)
    del h.exchange.venue.algos[lost.client_algo_id]
    report = await _engage(h)
    assert report.stops_placed == [SOL]
    assert report.safe
    (stop,) = working(h, Leg.SL)
    assert (stop.qty, stop.trigger_price) == (Decimal("9"), Decimal("98.00"))


async def test_kill_cancels_our_entries_the_ledger_does_not_know(h: Harness) -> None:
    stray = client_id("evt-lost", Leg.ENTRY, 0)
    request = OrderRequest(
        symbol=ETH,
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        qty=Decimal("1"),
        client_id=stray,
        price=Decimal("99.80"),
        tif=TimeInForce.GTX,
    )
    await h.exchange.place_order(request)
    report = await _engage(h)
    assert h.exchange.venue.orders[stray].status == "CANCELED"
    assert report.open_entries_left == 0


async def test_after_a_kill_entries_are_refused_but_protection_and_exits_still_work(h: Harness) -> None:
    await open_position(h)
    await _engage(h)
    placed_before = len(h.exchange.mutating_calls("place_order"))
    statuses = await h.send(_eth_plan(h, "evt-3"))
    assert statuses == [
        "accepted",
        "accepted",
        "accepted",
        "rejected",
        "rejected",
    ]  # sl, tp1, trail | entries
    assert len(h.exchange.mutating_calls("place_order")) == placed_before
    close = exit_intent(
        account=Account.PAPER,
        event_id="evt-1",
        symbol=SOL,
        held_side=Side.LONG,
        qty=Decimal(9),
        now=h.clock(),
    )
    assert await h.send([close]) == ["accepted"]
    await h.pump()
    assert SOL not in h.exchange.venue.positions
    assert working(h) == []


async def test_flatten_closes_every_position_and_leaves_nothing_working(h: Harness) -> None:
    await open_position(h)
    report = await _engage(h, flatten=True)
    await h.pump()
    assert report.closed == [SOL]
    assert report.safe
    assert h.exchange.venue.positions == {}
    assert working(h) == []
    exits = [c for c in h.exchange.mutating_calls("place_order") if leg_of_call(c) is Leg.EXIT]
    assert len(exits) == 1


async def test_take_profit_is_not_put_back_while_killed(h: Harness) -> None:
    await open_position(h)
    await _engage(h)
    (stop,) = working(h, Leg.SL)
    h.exchange.external_cancel_algo(stop.client_algo_id)  # the stop vanishes: the guard restores it
    await h.pump()
    (restored,) = working(h, Leg.SL)
    assert restored.client_algo_id != stop.client_algo_id
    assert working(h, Leg.TP1) == []


async def test_pause_never_downgrades_a_kill_and_resume_needs_a_clean_reconcile(h: Harness) -> None:
    await open_position(h)
    await _engage(h, cause="reconcile")
    h.kill.pause("operator pause")
    kill = h.ledger.kill_state()
    assert kill is not None
    assert kill.state == "killed"
    refused = h.kill.resume()
    assert not refused.allowed
    report = await h.reconcile()
    assert report.clean
    allowed = h.kill.resume()
    assert allowed.allowed
    kill = h.ledger.kill_state()
    assert kill is not None
    assert kill.state == "running"
    assert h.ns.blocked_reason() is None


def _kill_from_outside(h: Harness, *, flatten: bool = False, cause: str | None = "manual") -> str:
    """What `hdt-kill` does with Redis and Telegram down: one kill_state row straight into Postgres."""
    command_id = cli_command_id(flatten=flatten)
    Ledger(h.ns.sessions, h.ns.account).set_kill_state(
        "killed", cause=cause, reason="cli", command_id=command_id
    )
    return command_id


async def test_kill_written_straight_to_postgres_is_enforced_once(h: Harness) -> None:
    await open_position(h)
    (stop,) = working(h, Leg.SL)
    eth = _eth_plan(h, "evt-2")
    await h.send(eth)
    _kill_from_outside(h)
    report = await h.kill.enforce()
    assert report is not None
    assert report.safe
    assert report.cancelled_tps == 1
    assert h.exchange.venue.orders[eth[-1].client_id].status == "CANCELED"
    assert [a.client_algo_id for a in working(h, Leg.SL)] == [stop.client_algo_id]
    assert working(h, Leg.TP1) == []
    calls = len(h.exchange.calls)
    assert await h.kill.enforce() is None  # already engaged by this process
    assert len(h.exchange.calls) == calls


async def test_flatten_requested_by_the_cli_command_id_closes_positions(h: Harness) -> None:
    await open_position(h)
    _kill_from_outside(h, flatten=True)
    report = await h.kill.enforce()
    await h.pump()
    assert report is not None
    assert report.closed == [SOL]
    assert h.exchange.venue.positions == {}
    assert working(h) == []


async def test_a_restarted_process_re_verifies_a_standing_kill_without_touching_the_stop(h: Harness) -> None:
    await open_position(h)
    await _engage(h)
    (stop,) = working(h, Leg.SL)
    restarted = KillSwitch(h.ns, h.manager, h.guard)
    report = await restarted.enforce()
    assert report is not None
    assert report.safe
    assert report.stops_placed == []
    assert f"cancel_algo:{stop.client_algo_id}" not in h.exchange.calls
    assert [a.client_algo_id for a in working(h, Leg.SL)] == [stop.client_algo_id]


async def test_kill_row_without_a_cause_is_enforced_as_manual(h: Harness) -> None:
    _kill_from_outside(h, cause=None)
    report = await h.kill.enforce()
    assert report is not None
    assert report.cause == "manual"


async def test_a_manual_kill_on_top_of_a_daily_loss_kill_keeps_the_same_day_resume_block(
    h: Harness,
) -> None:
    """Review cycle 1 H4: `hdt-kill` (or /kill) after a daily-loss kill must not unlock a same-day resume."""
    await open_position(h)
    await _engage(h, cause="daily_loss")
    assert not h.kill.resume().allowed
    _kill_from_outside(h)  # hdt-kill writes cause `manual`
    report = await h.kill.enforce()
    assert report is not None
    assert report.cause == "daily_loss"
    manual = await _engage(h, cause="manual")  # a Telegram /kill on top
    assert manual.cause == "daily_loss"
    decision = h.kill.resume()
    assert not decision.allowed
    assert "daily loss" in decision.reason
    kill = h.ledger.kill_state()
    assert kill is not None
    assert (kill.state, kill.cause) == ("killed", "daily_loss")


def test_a_stricter_kill_cause_replaces_a_weaker_one_and_restarts_since(h: Harness) -> None:
    first = h.ledger.set_kill_state("killed", cause="manual", reason="ops", command_id="a")
    stricter = h.ledger.set_kill_state("killed", cause="reconcile", reason="mismatch", command_id="b")
    assert (stricter.cause, stricter.reason, stricter.command_id) == ("reconcile", "mismatch", "b")
    assert first.since is not None
    assert stricter.since is not None
    assert stricter.since > first.since
    weaker = h.ledger.set_kill_state("killed", cause="rate_limit", reason="429", command_id="c")
    assert (weaker.cause, weaker.reason, weaker.since, weaker.command_id) == (
        "reconcile",
        "mismatch",
        stricter.since,
        "c",
    )


async def test_a_kill_sequence_that_fails_part_way_is_retried_until_safe(
    h: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Review cycle 1 M3: an exchange error inside the sequence must not mark the kill as engaged."""
    monkeypatch.setattr(kill_switch, "RETRY_MIN_S", 0.0)
    h.kill._retry_s = 0.0
    await open_position(h)
    eth = _eth_plan(h, "evt-2")
    await h.send(eth)
    open_orders = h.exchange.open_orders
    failures = [ExchangeError("HTTP 503", code=-1001, http_status=503)]

    async def flaky_open_orders() -> list[OrderSnapshot]:
        if failures:
            raise failures.pop()
        return await open_orders()

    monkeypatch.setattr(h.exchange, "open_orders", flaky_open_orders)
    _kill_from_outside(h)
    with pytest.raises(ExchangeError):
        await h.kill.enforce()
    report = await h.kill.enforce()  # re-run, not treated as engaged
    assert report is not None
    assert report.safe
    assert h.exchange.venue.orders[eth[-1].client_id].status == "CANCELED"
    assert await h.kill.enforce() is None


def _open_kill_alerts(h: Harness) -> int:
    with h.ns.sessions() as s:
        return int(
            s.scalar(
                sa.select(sa.func.count())
                .select_from(AlertRow)
                .where(
                    AlertRow.kind == "kill_switch",
                    AlertRow.dedupe_key == h.ns.name,
                    AlertRow.resolved_at.is_(None),
                    AlertRow.is_test.is_(False),
                )
            )
            or 0
        )


async def test_an_accepted_resume_resolves_the_kill_alert_so_the_next_kill_pages_again(h: Harness) -> None:
    await _engage(h)
    assert _open_kill_alerts(h) == 1
    assert h.kill.resume().allowed
    assert _open_kill_alerts(h) == 0
    await _engage(h)
    assert _open_kill_alerts(h) == 1


@pytest.mark.parametrize(
    ("state", "cause", "since", "now", "clean", "ran_at", "allowed"),
    [
        ("running", None, None, T, None, None, True),
        ("killed", "manual", T, T, None, None, True),
        ("killed", "daily_loss", T, T + timedelta(hours=11), None, None, False),
        ("killed", "daily_loss", T, T + timedelta(hours=12, minutes=1), None, None, True),
        ("killed", "reconcile", T, T + timedelta(minutes=5), None, None, False),
        ("killed", "reconcile", T, T + timedelta(minutes=5), False, T + timedelta(minutes=1), False),
        ("killed", "reconcile", T, T + timedelta(minutes=5), True, T - timedelta(minutes=1), False),
        ("killed", "reconcile", T, T + timedelta(minutes=5), True, T + timedelta(minutes=1), True),
        ("paused", "manual", T, T, None, None, True),
        # review cycle 2 N2: the latest dirty reconcile blocks whatever the stored cause
        ("killed", "manual", T, T + timedelta(minutes=5), False, T + timedelta(minutes=1), False),
        ("killed", "daily_loss", T, T + timedelta(days=1), False, T + timedelta(minutes=1), False),
        ("killed", "daily_loss", T, T + timedelta(days=1), True, T + timedelta(minutes=1), True),
        ("paused", "manual", T, T, False, T, False),
    ],
)
def test_resume_rules(
    state: str,
    cause: str | None,
    since: datetime | None,
    now: datetime,
    clean: bool | None,
    ran_at: datetime | None,
    allowed: bool,
) -> None:
    assert resume_allowed(state, cause, since, now, clean, ran_at).allowed is allowed


def _dirty_reconcile(h: Harness) -> None:
    h.ledger.insert_reconcile_run(
        clean=False, stop_invariant_ok=True, detail="position mismatch", mismatches=[{"symbol": SOL}]
    )


def _clean_reconcile(h: Harness) -> None:
    h.ledger.insert_reconcile_run(clean=True, stop_invariant_ok=True, detail=None, mismatches=[])


async def test_a_daily_loss_kill_on_top_of_a_reconcile_kill_keeps_the_dirty_reconcile_block(
    h: Harness,
) -> None:
    """Review cycle 2 N2 path A: the daily-loss cause replaces `reconcile`; the next UTC day the resume must
    still wait for a clean reconcile."""
    await open_position(h)
    _dirty_reconcile(h)
    await _engage(h, cause="reconcile")
    await _engage(h, cause="daily_loss")
    kill = h.ledger.kill_state()
    assert kill is not None
    assert (kill.state, kill.cause) == ("killed", "daily_loss")
    h.clock.advance(24 * 3600)  # the next UTC day: the daily-loss rule alone would allow it
    refused = h.kill.resume()
    assert not refused.allowed
    assert "reconcile" in refused.reason
    _clean_reconcile(h)
    assert h.kill.resume().allowed
    kill = h.ledger.kill_state()
    assert kill is not None
    assert kill.state == "running"


async def test_a_reconcile_mismatch_after_a_daily_loss_kill_blocks_the_next_day_resume(h: Harness) -> None:
    """N2, the other order: the reconcile kill on top keeps the stricter daily-loss cause, the dirty
    reconcile still blocks."""
    await open_position(h)
    await _engage(h, cause="daily_loss")
    _dirty_reconcile(h)
    await _engage(h, cause="reconcile")
    kill = h.ledger.kill_state()
    assert kill is not None
    assert kill.cause == "daily_loss"
    h.clock.advance(24 * 3600)
    assert not h.kill.resume().allowed
    _clean_reconcile(h)
    assert h.kill.resume().allowed


async def test_a_manual_kill_is_not_resumed_while_the_latest_reconcile_is_dirty(h: Harness) -> None:
    """N2 path B: a mismatch found on a namespace already killed by hand."""
    await _engage(h, cause="manual")
    _dirty_reconcile(h)
    assert not h.kill.resume().allowed
    _clean_reconcile(h)
    assert h.kill.resume().allowed


def _move_since(h: Harness, since: datetime) -> None:
    with transaction(h.ns.sessions) as s:
        s.execute(sa.update(KillStateRow).where(KillStateRow.account == h.ns.name).values(since=since))


async def test_re_engaging_a_standing_daily_loss_kill_keeps_its_since(h: Harness) -> None:
    """Review cycle 2 I2: a restart's `enforce`, a CLI kill on top and a retry re-engage the stored cause;
    none of them may push the daily-loss resume to a later UTC day."""
    await open_position(h)
    await _engage(h, cause="daily_loss")
    yesterday = utcnow() - timedelta(days=1)
    _move_since(h, yesterday)
    restarted = KillSwitch(h.ns, h.manager, h.guard)
    report = await restarted.enforce()
    assert report is not None
    assert report.cause == "daily_loss"
    _kill_from_outside(h)  # hdt-kill on top: a new command id, enforced again
    again = await restarted.enforce()
    assert again is not None
    kill = h.ledger.kill_state()
    assert kill is not None
    assert (kill.cause, kill.since) == ("daily_loss", yesterday)
    assert restarted.resume().allowed


async def test_a_new_daily_loss_detection_restarts_since(h: Harness) -> None:
    await _engage(h, cause="daily_loss")
    yesterday = utcnow() - timedelta(days=1)
    _move_since(h, yesterday)
    await _engage(h, cause="daily_loss")  # breached again on a new day
    kill = h.ledger.kill_state()
    assert kill is not None
    assert kill.since is not None
    assert kill.since > yesterday
    assert not h.kill.resume().allowed


async def test_a_flatten_whose_market_close_expires_is_not_safe_and_keeps_the_exits_armed(
    h: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Review cycle 2 m2: an EXPIRED MARKET closed nothing, so the flatten is neither counted nor SAFE."""
    await open_position(h)

    async def expired_close(_ns: object, symbol: str, qty: Decimal, *, event_id: str) -> Placed:
        side = OrderSide.SELL if qty > 0 else OrderSide.BUY
        return Placed(
            OrderSnapshot(
                symbol=symbol,
                client_id=client_id(event_id, Leg.EXIT, 9),
                order_id=1,
                side=side,
                order_type=OrderType.MARKET.value,
                tif=None,
                status="EXPIRED",
                qty=abs(qty),
                executed_qty=Decimal(0),
                avg_price=None,
                price=None,
                reduce_only=True,
                updated_at=h.clock(),
            )
        )

    monkeypatch.setattr(kill_switch, "close_market", expired_close)
    report = await _engage(h, flatten=True)
    assert report.closed == []
    assert report.not_flat == [SOL]
    assert not report.safe
    assert report.as_dict()["safe"] is False
    pos = h.ledger.position(SOL)
    assert pos is not None
    assert (pos.exit_in_progress, pos.exit_reason) == (False, None)
    (stop,) = working(h, Leg.SL)  # still protected
    assert stop.qty == Decimal("9")


def test_a_requested_flatten_is_safe_only_once_flat() -> None:
    report = kill_switch.KillReport("paper", "manual", flatten=True, not_flat=[SOL])
    assert not report.safe
    assert kill_switch.KillReport("paper", "manual", flatten=False, not_flat=[SOL]).safe
    flat = kill_switch.check_safety(kill_switch.KillReport("paper", "manual", flatten=True), [], [], [])
    assert flat.safe
