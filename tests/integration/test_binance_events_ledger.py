"""Binance user-stream frames -> adapter events -> ledger (phase 09 section 5).

Every exchange event of a full lifecycle (entry partially then fully filled, STOP resized, STOP triggered,
position closed) travels as the complete JSON frame Binance sends, is parsed by the user stream's parser
and dispatched to the order manager. The ledger must then hold exactly the exchange's fills, orders and
conditional orders, and reconcile clean.
"""

from __future__ import annotations

import json
import logging
from decimal import Decimal
from typing import Any

import pytest
import sqlalchemy as sa
from sqlalchemy import Engine

from binance_shapes import ws_frame
from fake_exchange import SOL, Harness, ledger_sessions, make_harness, working
from hdt.contracts.common import Account, Leg, Side
from hdt.db.models.ledger import FillRow
from hdt.execution import actions
from hdt.execution.adapter_base import OrderEvent
from hdt.execution.user_stream import dispatch, parse_user_event
from intent_builders import KEYRING, open_plan

pytestmark = [pytest.mark.pg, pytest.mark.integration]


@pytest.fixture(autouse=True)
def _no_retry_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(actions, "CANCEL_CONFIRM_BACKOFF_S", 0.0)
    monkeypatch.setattr(actions, "UNKNOWN_VERIFY_DELAY_S", 0.0)


async def stream(h: Harness) -> list[str]:
    """Deliver pending exchange events as Binance frames; returns the frame types sent."""
    sent: list[str] = []
    while h.exchange.pushes:
        event = h.exchange.pushes.pop(0)
        frames: list[dict[str, Any]] = [ws_frame(event)]
        if isinstance(event, OrderEvent) and event.fill is not None:
            wallet = str(h.exchange.venue.wallet)
            frames.append(
                {
                    "e": "ACCOUNT_UPDATE",
                    "E": frames[0]["E"],
                    "T": frames[0]["T"],
                    "a": {"m": "ORDER", "B": [{"a": "USDT", "wb": wallet, "cw": wallet, "bc": "0"}], "P": []},
                }
            )
        for frame in frames:
            parsed = parse_user_event(json.loads(json.dumps(frame)))
            assert parsed is not None, frame["e"]
            await dispatch(h.manager, parsed)
            sent.append(frame["e"])
    return sent


def _ledger_fills(h: Harness) -> list[tuple[Any, ...]]:
    with h.ns.sessions() as s:
        rows = s.scalars(
            sa.select(FillRow).where(FillRow.account == h.ns.account.value).order_by(FillRow.trade_id)
        ).all()
        return [(r.trade_id, r.side, r.price, r.qty, r.fee_usd, r.realized_pnl, r.leg) for r in rows]


async def test_a_full_lifecycle_streamed_as_binance_frames_lands_in_the_ledger(
    pg_engine: Engine, caplog: pytest.LogCaptureFixture
) -> None:
    h = make_harness(ledger_sessions(pg_engine), Account.TESTNET, KEYRING)
    await h.start()
    await stream(h)
    plan = open_plan(
        account=Account.TESTNET,
        event_id="evt-ws",
        symbol=SOL,
        side=Side.LONG,
        qty=Decimal("9"),
        entry=Decimal("100.00"),
        stop=Decimal("98.00"),
        tp1=Decimal("104.00"),
        ioc_price=Decimal("100.10"),
        now=h.clock(),
    )
    assert await h.send(plan) == ["accepted"] * len(plan)
    entry = plan[-1]
    kinds = await stream(h)
    assert kinds == ["ORDER_TRADE_UPDATE"]  # the resting entry; protective legs wait for a fill

    queue = h.exchange.venue.orders[entry.client_id].queue_ahead
    h.exchange.trade(SOL, "100.00", str(queue + 4), buyer_maker=True)
    kinds = await stream(h)
    assert "ALGO_UPDATE" in kinds  # the STOP covering the first 4
    row = h.ledger.order(entry.client_id)
    assert row is not None
    assert (row.status, row.executed_qty) == ("PARTIALLY_FILLED", Decimal("4"))
    assert [a.qty for a in working(h, Leg.SL)] == [Decimal("4")]

    h.exchange.trade(SOL, "100.00", "5", buyer_maker=True)
    await stream(h)
    await stream(h)  # the resize's own frames
    row = h.ledger.order(entry.client_id)
    assert row is not None
    assert (row.status, row.executed_qty) == ("FILLED", Decimal("9"))
    (stop,) = working(h, Leg.SL)
    assert stop.qty == Decimal("9")
    ledger_stop = h.ledger.algo(stop.client_algo_id)
    assert ledger_stop is not None
    assert (ledger_stop.status, ledger_stop.algo_id) == ("NEW", stop.algo_id)
    position = h.ledger.position(SOL)
    assert position is not None
    assert (position.qty, position.entry_price) == (Decimal("9"), Decimal("100.00"))

    orders_before_trigger = len(h.exchange.mutating_calls("place_order"))
    h.exchange.set_mark(SOL, "97.90")  # the STOP triggers; its market order is the exchange's own
    await stream(h)
    await stream(h)
    triggered = h.exchange.venue.algos[stop.client_algo_id].snapshot()
    assert triggered.triggered_order_id is not None
    ledger_stop = h.ledger.algo(stop.client_algo_id)
    assert ledger_stop is not None
    assert ledger_stop.triggered_order_id == triggered.triggered_order_id
    assert ledger_stop.status not in ("NEW", "PENDING")
    assert h.ledger.position(SOL) is None
    assert h.exchange.venue.positions == {}

    exchange_fills = sorted(
        (t.trade_id, t.side.value, t.price, t.qty, t.fee, t.realized_pnl) for t in h.exchange.venue.trades
    )
    ledger_fills = _ledger_fills(h)
    assert [f[:6] for f in ledger_fills] == exchange_fills
    assert [f[6] for f in ledger_fills] == ["entry", "entry", "sl", "sl"]  # the STOP walked two levels
    # The half-applied STOP order is never mistaken for an unprotected position.
    assert len(h.exchange.mutating_calls("place_order")) == orders_before_trigger
    assert not [r for r in caplog.records if r.levelno >= logging.CRITICAL]
    assert h.ledger.expected_wallet() == h.exchange.venue.wallet
    report = await h.reconcile()
    assert report.clean
    assert working(h) == []
