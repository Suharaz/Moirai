"""Execution's price guard right before an entry goes out (owner decision 2026-09-28: no time limits).

An OPEN has no expiry, so an entry can wait in `orders` for as long as execution is down. When it is finally
handled, the current mark is re-checked with Risk's rule (`hdt.core.entry_guard`): beyond the stop, beyond
TP1 or too far from the entry, or no mark at all, and nothing is sent; otherwise it is sent however late.
"""

from __future__ import annotations

import logging
from decimal import Decimal

import pytest
import sqlalchemy as sa
from sqlalchemy import Engine

from alert_logs import raised
from fake_exchange import SOL, Harness, ledger_sessions, leg_of_call, make_harness
from hdt.contracts.common import Account, Leg, Side
from hdt.contracts.order import OrderIntent
from hdt.db.models.ledger import ProcessedIntentRow
from hdt.execution import actions
from hdt.ops.metrics import ENTRIES_REFUSED
from intent_builders import KEYRING, open_plan

pytestmark = pytest.mark.pg


@pytest.fixture(autouse=True)
def _no_retry_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(actions, "CANCEL_CONFIRM_BACKOFF_S", 0.0)
    monkeypatch.setattr(actions, "UNKNOWN_VERIFY_DELAY_S", 0.0)


@pytest.fixture
async def h(pg_engine: Engine) -> Harness:
    harness = make_harness(ledger_sessions(pg_engine), Account.PAPER, KEYRING)
    await harness.start()
    return harness


def _plan(h: Harness, event_id: str, **kw: object) -> list[OrderIntent]:
    """LONG entry 100.00, stop 98.00, TP1 104.00, IOC 100.10, signed at the current clock."""
    return open_plan(
        account=h.ns.account,
        event_id=event_id,
        symbol=SOL,
        side=Side.LONG,
        qty=Decimal("9"),
        entry=Decimal("100.00"),
        stop=Decimal("98.00"),
        tp1=Decimal("104.00"),
        ioc_price=Decimal("100.10"),
        now=h.clock(),
        **kw,  # type: ignore[arg-type]
    )


def _entries_sent(h: Harness) -> list[Leg | None]:
    return [
        leg_of_call(c)
        for c in h.exchange.mutating_calls("place_order")
        if leg_of_call(c) in (Leg.ENTRY, Leg.ENTRY_IOC)
    ]


def _statuses(h: Harness, plan: list[OrderIntent]) -> dict[str, tuple[str, str | None]]:
    """Status and reason of each leg of `plan`, by leg name."""
    legs = {i.intent_id: i.leg.value for i in plan}
    with h.ns.sessions() as s:
        rows = s.execute(
            sa.select(
                ProcessedIntentRow.intent_id, ProcessedIntentRow.status, ProcessedIntentRow.reason
            ).where(ProcessedIntentRow.account == h.ns.name, ProcessedIntentRow.intent_id.in_(legs))
        ).all()
    return {legs[r.intent_id]: (r.status, r.reason) for r in rows}


def _refused(reason: str) -> float:
    return ENTRIES_REFUSED.labels(account="paper", reason=reason)._value.get()


async def test_an_entry_handled_after_downtime_is_not_sent_once_the_mark_crossed_its_stop(
    h: Harness, caplog: pytest.LogCaptureFixture
) -> None:
    plan = _plan(h, "evt-down")  # Risk approved and signed it; execution was down meanwhile
    h.clock.advance(3 * 3600)
    h.exchange.set_mark(SOL, "97.50")  # below the 98.00 stop of the LONG
    before = _refused("stop_crossed")
    with caplog.at_level(logging.WARNING):
        assert await h.send(plan) == ["accepted"] * len(plan)
        await h.tick(200)  # past post_only_wait_s: no watchdog IOC either
    assert _entries_sent(h) == []
    statuses = _statuses(h, plan)
    assert statuses["entry"][0] == "void"
    assert (statuses["entry"][1] or "").startswith("stop_crossed: ")
    assert statuses["entry_ioc"][0] == "void"
    assert _refused("stop_crossed") == before + 1
    assert len([r for r in caplog.records if raised(r, "entry_refused")]) == 1


@pytest.mark.parametrize(
    ("mark", "distance", "reason"),
    [("104.00", None, "tp_crossed"), ("101.00", Decimal("0.50"), "entry_distance")],
)
async def test_an_entry_is_not_sent_when_the_mark_broke_another_level(
    h: Harness, mark: str, distance: Decimal | None, reason: str
) -> None:
    plan = _plan(h, f"evt-{reason}", max_entry_distance=distance)
    h.exchange.set_mark(SOL, mark)
    assert await h.send(plan) == ["accepted"] * len(plan)
    assert _entries_sent(h) == []
    assert (_statuses(h, plan)["entry"][1] or "").startswith(f"{reason}: ")


async def test_an_entry_handled_hours_late_is_sent_while_its_levels_hold(h: Harness) -> None:
    plan = _plan(h, "evt-late", max_entry_distance=Decimal("1.50"))
    h.clock.advance(3 * 3600)
    h.exchange.set_mark(SOL, "100.40")  # between the stop and TP1, 0.40 from the entry
    assert await h.send(plan) == ["accepted"] * len(plan)
    assert _entries_sent(h) == [Leg.ENTRY]
    assert _statuses(h, plan)["entry"][0] != "void"


async def test_an_entry_is_not_sent_blind_without_a_mark(h: Harness) -> None:
    plan = _plan(h, "evt-blind")
    h.exchange.venue.marks.pop(SOL)
    before = _refused("no_mark")
    assert await h.send(plan) == ["accepted"] * len(plan)
    assert _entries_sent(h) == []
    assert (_statuses(h, plan)["entry"][1] or "").startswith("no_mark: ")
    assert _refused("no_mark") == before + 1


async def test_the_watchdog_ioc_is_not_sent_once_the_mark_crossed_the_stop(h: Harness) -> None:
    plan = _plan(h, "evt-ioc")
    assert await h.send(plan) == ["accepted"] * len(plan)
    assert _entries_sent(h) == [Leg.ENTRY]  # rests post-only at 100.00
    h.exchange.set_mark(SOL, "97.90")
    await h.tick(200)  # post-only wait over: the entry is cancelled, its IOC re-checked
    assert _entries_sent(h) == [Leg.ENTRY]
    assert h.exchange.venue.orders[plan[-1].client_id].status == "CANCELED"
    assert (_statuses(h, plan)["entry_ioc"][1] or "").startswith("stop_crossed: ")
