"""Namespace isolation (phase 09 sections 5 and 8): paper, testnet and live share nothing but Postgres.

Success criterion: a reconcile mismatch (or a lost user stream) in `testnet` stops only `testnet`; `paper`
keeps its kill state, keeps trading and reconciles clean.
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
from hdt.contracts.common import Account, Leg, Side
from hdt.execution import actions
from intent_builders import KEYRING, open_plan

pytestmark = [pytest.mark.pg, pytest.mark.integration]


@pytest.fixture(autouse=True)
def _no_retry_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(actions, "CANCEL_CONFIRM_BACKOFF_S", 0.0)
    monkeypatch.setattr(actions, "UNKNOWN_VERIFY_DELAY_S", 0.0)


def _running(h: Harness) -> bool:
    kill = h.ledger.kill_state()
    return kill is None or kill.state == "running"


async def test_testnet_mismatch_and_stream_loss_do_not_touch_paper(pg_engine: Engine) -> None:
    sessions = ledger_sessions(pg_engine)
    paper = make_harness(sessions, Account.PAPER, KEYRING)
    testnet = make_harness(sessions, Account.TESTNET, KEYRING)  # signed by the testnet driver key
    seed_market(paper.exchange, ETH)
    for h in (paper, testnet):
        await h.start()
        await open_position(h)  # the same event id and client ids in both namespaces

    testnet.exchange.venue.positions[SOL].qty = Decimal("5")  # testnet position moves without a fill
    assert not (await testnet.reconcile()).clean  # first sighting: recorded, re-checked next cycle
    assert (await testnet.reconcile()).confirmed
    testnet_kill = testnet.ledger.kill_state()
    assert testnet_kill is not None
    assert (testnet_kill.state, testnet_kill.cause) == ("killed", "reconcile")
    assert working(testnet, Leg.TP1) == []
    testnet.resync.begin()  # and its user stream drops
    assert testnet.ledger.exec_account().sync_state == "resyncing"

    assert _running(paper)
    assert paper.ns.synced
    assert paper.ledger.exec_account().sync_state == "synced"
    assert paper.ns.blocked_reason() is None
    assert len(working(paper, Leg.SL)) == 1
    assert len(working(paper, Leg.TP1)) == 1  # the testnet kill cancelled only its own TP

    await open_position(paper, event_id="evt-2", symbol=ETH)  # paper keeps opening
    assert paper.exchange.venue.positions[ETH].qty == Decimal("9")
    assert (await paper.reconcile()).clean
    assert _running(paper)

    placed = len(testnet.exchange.mutating_calls("place_order"))
    plan = open_plan(
        account=Account.TESTNET,
        event_id="evt-3",
        symbol=ETH,
        side=Side.LONG,
        qty=Decimal("5"),
        entry=Decimal("100.00"),
        stop=Decimal("98.00"),
        tp1=Decimal("104.00"),
        now=testnet.clock(),
    )
    statuses = await testnet.send(plan)
    assert statuses[-2:] == ["rejected", "rejected"]  # entry_ioc, entry
    assert len(testnet.exchange.mutating_calls("place_order")) == placed
