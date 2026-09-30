"""Reconciler (phase 09 section 5): exchange truth vs the ledger every cycle.

Missed fills are caught up (not a mismatch); anything still different afterwards (position, unknown order,
unexplained wallet change) is recorded as a dirty `reconcile_runs` row. Seen again by the next cycle it is a
real mismatch: Critical alert, kill state `reconcile` for this namespace (review cycle 3, m9). The STOP
invariant is repaired at once. Liquidation closer than 1.5x the stop distance tightens the stop.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

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
from hdt.contracts.common import Account, Leg, OrderSide, OrderType, TimeInForce
from hdt.db.models.ledger import ReconcileRunRow
from hdt.db.models.ops import AlertRow
from hdt.execution import actions, reconciler
from hdt.execution.adapter_base import AlgoRequest, AlgoSnapshot, CashFlow, ExchangeError, OrderRequest
from hdt.execution.paper_venue import VPosition
from hdt.execution.reconciler import ReconcileReport
from intent_builders import KEYRING

pytestmark = pytest.mark.pg


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


def _runs(h: Harness) -> list[ReconcileRunRow]:
    with h.ns.sessions() as s:
        return list(s.scalars(sa.select(ReconcileRunRow).order_by(ReconcileRunRow.ran_at)))


def _killed_by_reconcile(h: Harness) -> bool:
    kill = h.ledger.kill_state()
    return kill is not None and (kill.state, kill.cause) == ("killed", "reconcile")


def _alert_kinds(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [r.alert_kind for r in caplog.records if _raised(r)]  # type: ignore[attr-defined]


async def _seen_twice(h: Harness) -> ReconcileReport:
    """Two consecutive cycles: the first sighting is recorded without a kill, the second confirms it."""
    first = await h.reconcile()
    assert not first.clean
    assert not first.confirmed
    assert not _killed_by_reconcile(h)
    assert "seen once" in (_runs(h)[-1].detail or "")
    second = await h.reconcile()
    assert second.confirmed
    return second


async def test_a_mismatch_seen_once_is_recorded_and_rechecked_without_a_kill(
    h: Harness, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    await open_position(h)
    runner = h.resync.reconciler
    h.exchange.venue.positions[SOL].qty = Decimal("7")  # e.g. positionRisk ahead of userTrades
    waits: list[float] = []
    sleep = asyncio.sleep

    async def recording_sleep(delay: float, *args: Any, **kwargs: Any) -> Any:
        waits.append(delay)
        return await sleep(0, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(asyncio, "sleep", recording_sleep)
        report = await h.reconcile()
    assert [w for w in waits if w > 0] == []  # never waits while holding the namespace lock (m-b)
    assert (report.clean, report.confirmed) == (False, False)
    assert not _runs(h)[-1].clean  # recorded: a resume stays refused until a clean run
    assert runner.interval_s(30.0) == reconciler.MISMATCH_RECHECK_S  # re-checked soon, not in 30 s
    assert "reconcile_mismatch" not in _alert_kinds(caplog)
    assert not _killed_by_reconcile(h)
    h.exchange.venue.positions[SOL].qty = Decimal("9")  # the lagging listing caught up
    assert (await h.reconcile()).clean
    assert not _killed_by_reconcile(h)
    assert runner.interval_s(30.0) == 30.0


async def test_a_different_mismatch_on_the_next_cycle_is_a_new_first_sighting(h: Harness) -> None:
    await open_position(h)
    h.exchange.venue.positions[SOL].qty = Decimal("7")
    assert not (await h.reconcile()).confirmed
    h.exchange.venue.positions[SOL].qty = Decimal("9")
    h.exchange.venue.wallet -= Decimal("25")
    report = await h.reconcile()
    assert [m["kind"] for m in report.mismatches] == ["wallet"]
    assert not report.confirmed
    assert not _killed_by_reconcile(h)


async def test_a_fill_the_trade_list_shows_one_cycle_late_never_kills(
    h: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Review cycle 3 m9: positionRisk can list a fill before userTrades does; the next cycle catches up."""
    await open_position(h, fill=None)
    h.exchange.stream_up = False
    h.exchange.trade(SOL, "100.00", "14", buyer_maker=True)  # the entry fills; the push is lost
    h.exchange.stream_up = True
    user_trades = h.exchange.user_trades
    lagging = True

    async def lagging_trades(*args: Any, **kwargs: Any) -> Any:
        return [] if lagging else await user_trades(*args, **kwargs)

    monkeypatch.setattr(h.exchange, "user_trades", lagging_trades)
    first = await h.reconcile()
    assert [m["kind"] for m in first.mismatches] == ["position"]
    assert not _killed_by_reconcile(h)
    lagging = False
    assert (await h.reconcile()).clean  # the listed fill is caught up: the difference is gone
    assert not _killed_by_reconcile(h)


async def test_first_cycle_anchors_the_wallet_and_records_a_clean_run(h: Harness) -> None:
    acct = h.ledger.exec_account()
    assert acct.wallet_anchor == Decimal("10000")
    assert acct.sync_state == "synced"
    (run,) = _runs(h)
    assert run.clean
    assert run.stop_invariant_ok


async def test_fills_the_stream_missed_are_caught_up_not_reported(h: Harness) -> None:
    await open_position(h, fill=None)
    h.exchange.stream_up = False
    h.exchange.trade(SOL, "100.00", "14", buyer_maker=True)
    h.exchange.stream_up = True
    assert h.ledger.position(SOL) is None
    report = await h.reconcile()
    assert report.clean
    pos = h.ledger.position(SOL)
    assert pos is not None
    assert pos.qty == Decimal("9")
    (stop,) = working(h, Leg.SL)
    assert stop.qty == Decimal("9")
    assert not _killed_by_reconcile(h)


async def test_position_changed_without_a_trade_kills_the_namespace(
    h: Harness, caplog: pytest.LogCaptureFixture
) -> None:
    await open_position(h)
    h.exchange.venue.positions[SOL].qty = Decimal("7")  # e.g. auto-deleveraging we cannot see as a fill
    report = await _seen_twice(h)
    assert report.mismatches[0] == {"kind": "position", "symbol": SOL, "exchange": "7", "ledger": "9"}
    assert _killed_by_reconcile(h)
    run = _runs(h)[-1]
    assert not run.clean
    assert "position" in (run.detail or "")
    assert "reconcile_mismatch" in _alert_kinds(caplog)
    assert "kill_switch" in _alert_kinds(caplog)
    (stop,) = working(h, Leg.SL)  # the kill keeps the stop: it still covers the smaller position
    assert stop.qty == Decimal("9")


@pytest.mark.parametrize("first", ["manual", "rate_limit"])
async def test_mismatch_on_a_killed_namespace_makes_resume_need_a_clean_reconcile(
    h: Harness, first: str
) -> None:
    await open_position(h)
    async with h.ns.lock:
        await h.kill.engage(first, "operator")  # type: ignore[arg-type]
    h.exchange.venue.positions[SOL].qty = Decimal("7")
    await _seen_twice(h)
    assert _killed_by_reconcile(h)
    assert not h.kill.resume().allowed
    h.exchange.venue.positions[SOL].qty = Decimal("9")
    assert (await h.reconcile()).clean
    assert h.kill.resume().allowed


async def test_mismatch_never_lifts_a_stricter_kill(h: Harness) -> None:
    await open_position(h)
    async with h.ns.lock:
        await h.kill.engage("daily_loss", "daily loss limit")
    h.exchange.venue.positions[SOL].qty = Decimal("7")
    await _seen_twice(h)
    kill = h.ledger.kill_state()
    assert kill is not None
    assert (kill.state, kill.cause, kill.reason) == ("killed", "daily_loss", "daily loss limit")


async def test_foreign_resting_order_is_a_mismatch(h: Harness) -> None:
    manual = OrderRequest(
        symbol=ETH,
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        qty=Decimal("1"),
        client_id="web_manual_order_1",
        price=Decimal("99.80"),
        tif=TimeInForce.GTX,
    )
    await h.exchange.place_order(manual)
    report = await _seen_twice(h)
    assert [m["kind"] for m in report.mismatches] == ["foreign_order"]
    assert _killed_by_reconcile(h)


async def test_order_the_exchange_lost_is_a_mismatch(h: Harness) -> None:
    plan = await open_position(h, fill=None)
    del h.exchange.venue.orders[plan[-1].client_id]
    report = await _seen_twice(h)
    assert [m["kind"] for m in report.mismatches] == ["order_missing"]
    assert _killed_by_reconcile(h)


async def test_pending_order_that_never_reached_the_exchange_is_closed_after_the_grace(h: Harness) -> None:
    request = OrderRequest(
        symbol=SOL,
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        qty=Decimal("1"),
        client_id="PNAJ7EZNFSEOS7YMC5TW-entry-7",
        price=Decimal("99.00"),
        tif=TimeInForce.GTX,
    )
    h.ledger.record_order_pending(
        request, event_id="evt-crash", leg=Leg.ENTRY, intent_id=None, expires_at=None, fill_model=None
    )
    assert (await h.reconcile()).clean  # within the grace: the request may still be in flight
    row = h.ledger.order(request.client_id)
    assert row is not None
    assert row.status == "PENDING"
    h.clock.advance(timedelta(seconds=90).total_seconds())
    assert (await h.reconcile()).clean
    row = h.ledger.order(request.client_id)
    assert row is not None
    assert row.status == "REJECTED"


async def test_funding_is_booked_and_an_unexplained_wallet_change_is_a_mismatch(h: Harness) -> None:
    await open_position(h)
    venue = h.exchange.venue
    now_ms = int(h.clock().timestamp() * 1000)
    venue.on_mark(SOL, Decimal("100"), now_ms + 1, rate=Decimal("0.003"), next_funding_ms=now_ms + 2)
    venue.on_mark(SOL, Decimal("100"), now_ms + 2)
    assert venue.flows[0].amount == Decimal("-2.700")  # above the 1 USD wallet tolerance
    assert (await h.reconcile()).clean
    venue.wallet -= Decimal("25")  # money left the account without any flow the API reports
    report = await _seen_twice(h)
    assert [m["kind"] for m in report.mismatches] == ["wallet"]
    assert _killed_by_reconcile(h)


async def test_position_unknown_to_the_ledger_is_closed_at_once_and_kills(h: Harness) -> None:
    h.exchange.venue.positions[ETH] = VPosition(ETH, Decimal("3"), Decimal("100"))
    report = await h.reconcile()
    assert not report.stop_invariant_ok
    assert [m["kind"] for m in report.mismatches] == ["position"]
    assert ETH not in h.exchange.venue.positions  # closed reduce-only at the first sighting (no stop level)
    assert not _killed_by_reconcile(h)
    await h.pump()
    assert (await h.reconcile()).confirmed  # the ledger never knew that position: still differs
    assert _killed_by_reconcile(h)


async def test_liquidation_closer_than_1_5x_the_stop_distance_tightens_the_stop(
    h: Harness, caplog: pytest.LogCaptureFixture
) -> None:
    await open_position(h)
    h.exchange.liquidation[SOL] = Decimal("97.50")  # 2.50 away, stop 2.00 away: needs >= 3.00
    report = await h.reconcile()
    assert report.clean
    (stop,) = working(h, Leg.TRAIL)  # tighten() places the new level as a trail-leg stop
    assert stop.trigger_price == Decimal("98.34")  # 100 - 2.50 / 1.5, rounded toward the entry
    assert stop.qty == Decimal("9")
    assert working(h, Leg.SL) == []
    assert "liquidation_distance" in _alert_kinds(caplog)
    assert [r.levelno for r in caplog.records if _raised(r, "liquidation_distance")] == [logging.WARNING]


async def test_failed_liquidation_tighten_keeps_the_stop_and_the_position(
    h: Harness, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    await open_position(h)
    (stop,) = working(h, Leg.SL)

    async def unavailable(request: AlgoRequest) -> AlgoSnapshot:
        raise ExchangeError("POST /fapi/v1/algoOrder -> HTTP 503: Service Unavailable", http_status=503)

    monkeypatch.setattr(h.exchange, "place_algo", unavailable)
    h.exchange.liquidation[SOL] = Decimal("97.50")
    assert (await h.reconcile()).clean
    assert [a.client_algo_id for a in working(h, Leg.SL)] == [stop.client_algo_id]
    assert working(h, Leg.TRAIL) == []
    (alert,) = [r for r in caplog.records if _raised(r, "liquidation_distance")]
    assert "stop not tightened" in alert.getMessage()
    assert h.exchange.venue.positions[SOL].qty == Decimal("9")
    assert [c for c in h.exchange.mutating_calls("place_order") if leg_of_call(c) is Leg.EXIT] == []


async def test_liquidation_warning_is_raised_for_each_symbol(h: Harness) -> None:
    await open_position(h)
    await open_position(h, event_id="evt-2", symbol=ETH)
    h.exchange.liquidation[SOL] = Decimal("97.50")
    h.exchange.liquidation[ETH] = Decimal("97.50")
    assert (await h.reconcile()).clean
    with h.ns.sessions() as s:
        keys = s.scalars(
            sa.select(AlertRow.dedupe_key).where(
                AlertRow.kind == "liquidation_distance",
                AlertRow.resolved_at.is_(None),
                AlertRow.is_test.is_(False),
            )
        ).all()
    day = h.clock().date().isoformat()
    assert sorted(keys) == [f"{h.ns.name}:{ETH}:{day}", f"{h.ns.name}:{SOL}:{day}"]


@pytest.mark.parametrize("read", ["positions", "open_algo_orders", "balance"])
async def test_stop_out_between_two_exchange_reads_is_caught_up_not_a_mismatch(
    h: Harness, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch, read: str
) -> None:
    await open_position(h)
    original: Callable[[], Awaitable[Any]] = getattr(h.exchange, read)
    fired = False

    async def read_then_stop_out() -> Any:
        nonlocal fired
        result = await original()
        if not fired:  # the position's stop fires right after the reconciler's first such read
            fired = True
            h.exchange.set_book(SOL, [("97.90", "50")], [("98.00", "50")])
            h.exchange.set_mark(SOL, "97.90")
        return result

    monkeypatch.setattr(h.exchange, read, read_then_stop_out)
    report = await h.reconcile()
    assert fired
    assert SOL not in h.exchange.venue.positions  # stopped out on the exchange mid-cycle
    assert report.clean, report.mismatches
    assert report.stop_invariant_ok
    assert not _killed_by_reconcile(h)
    assert [c for c in h.exchange.mutating_calls("place_order") if leg_of_call(c) is Leg.EXIT] == []
    assert not [r for r in caplog.records if _raised(r, "stop_invariant")]
    await h.pump()  # the stream delivers the stop-out once the cycle released the namespace
    assert h.ledger.position(SOL) is None
    assert (await h.reconcile()).clean
    assert not _killed_by_reconcile(h)


async def test_income_is_fetched_from_the_last_recorded_flow(
    h: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    await open_position(h)
    venue = h.exchange.venue
    now_ms = int(h.clock().timestamp() * 1000)
    venue.on_mark(SOL, Decimal("100"), now_ms + 1, rate=Decimal("0.0001"), next_funding_ms=now_ms + 2)
    venue.on_mark(SOL, Decimal("100"), now_ms + 2)
    (funding,) = venue.flows
    starts: list[datetime] = []
    original = h.exchange.cash_flows

    async def recording(start: datetime) -> list[CashFlow]:
        starts.append(start)
        return await original(start)

    monkeypatch.setattr(h.exchange, "cash_flows", recording)
    assert (await h.reconcile()).clean
    assert (await h.reconcile()).clean
    assert starts == [h.ledger.exec_account().anchor_at, funding.time]


def _open_alerts(h: Harness, kind: str) -> int:
    with h.ns.sessions() as s:
        return int(
            s.scalar(
                sa.select(sa.func.count())
                .select_from(AlertRow)
                .where(
                    AlertRow.kind == kind,
                    AlertRow.dedupe_key == h.ns.name,
                    AlertRow.resolved_at.is_(None),
                    AlertRow.is_test.is_(False),
                )
            )
            or 0
        )


async def test_clean_run_resolves_the_open_mismatch_alert(h: Harness) -> None:
    await open_position(h)
    h.exchange.venue.positions[SOL].qty = Decimal("7")
    await _seen_twice(h)
    assert _open_alerts(h, "reconcile_mismatch") == 1
    h.exchange.venue.positions[SOL].qty = Decimal("9")
    assert (await h.reconcile()).clean
    assert _open_alerts(h, "reconcile_mismatch") == 0


def _open_severities(h: Harness, kind: str) -> dict[str, str]:
    """dedupe key -> severity of this namespace's open alerts of `kind`."""
    with h.ns.sessions() as s:
        rows = s.execute(
            sa.select(AlertRow.dedupe_key, AlertRow.severity).where(
                AlertRow.kind == kind,
                AlertRow.account == h.ns.name,
                AlertRow.resolved_at.is_(None),
                AlertRow.is_test.is_(False),
            )
        ).all()
    return {key: severity for key, severity in rows}


async def test_unconfirmed_entry_cancel_warning_never_absorbs_the_reconcile_critical(h: Harness) -> None:
    await open_position(h, fill="4")
    h.exchange.stuck_cancels = True
    await h.tick(200)  # the post-only wait is over: the cancel never confirms
    symbol_key = f"{h.ns.name}:{SOL}"
    assert _open_severities(h, "reconcile_mismatch") == {symbol_key: "warning"}
    h.exchange.venue.positions[SOL].qty = Decimal("3")
    await _seen_twice(h)
    assert _open_severities(h, "reconcile_mismatch") == {symbol_key: "warning", h.ns.name: "critical"}


async def test_confirmed_entry_cancel_resolves_its_warning(h: Harness) -> None:
    await open_position(h, fill="4")
    h.exchange.stuck_cancels = True
    await h.tick(200)
    h.exchange.stuck_cancels = False
    await h.tick(1)  # the watchdog's next pass confirms the cancel
    assert _open_severities(h, "reconcile_mismatch") == {}
