"""Hedge book execution (phase 09 section 6): one net BTC position with one book STOP, reconciler clean.

The first rebalance runs the real chain: `HedgeRebalancer` (two alt LONGs) -> `risk_intents` outbox ->
stream `orders` -> execution intake, with the user stream delivering the hedge fill as soon as it happens
(before the next intent is read), as on the exchange.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

import pytest
from redis.asyncio import Redis
from sqlalchemy import Engine

from fake_exchange import (
    BTC,
    ETH,
    FILTERS,
    SOL,
    FakeClock,
    Harness,
    ledger_sessions,
    leg_of_call,
    make_harness,
    working,
)
from hdt.contracts.common import Account, Leg, OrderSide
from hdt.contracts.order import OrderIntent
from hdt.contracts.streams import Stream
from hdt.core.streams import RawPayload
from hdt.execution import actions
from hdt.execution.filters import SymbolFilters
from hdt.quant.packet import store_candidate_set, store_packet
from hdt.risk.consumer import AccountStateBook, IntentOutbox, Mark, RiskRuntime
from hdt.risk.hedger import HedgeRebalancer
from intent_builders import KEYRING, RISK_SIGNER, hedge_intents
from risk_builders import (
    COIN_ID,
    FEATURES,
    NOW,
    account_state,
    default_candidate_set,
    packet,
    position,
    risk_file,
)

pytestmark = [pytest.mark.pg, pytest.mark.integration]


@dataclass(frozen=True)
class Member:
    cmc_id: int


class BtcMarket:
    """The lake view the rebalancer reads: BTC filters and mark, universe membership of the alts."""

    def filters(self, symbol: str, now: datetime) -> SymbolFilters | None:
        return FILTERS.get(symbol)

    def marks(self, symbols: set[str], now: datetime) -> dict[str, Mark]:
        return {BTC: Mark(Decimal("50000"), 1.0)} if BTC in symbols else {}

    def member_by_symbol(self, symbol: str, now: datetime) -> Member | None:
        return {SOL: Member(COIN_ID), ETH: Member(COIN_ID + 1)}.get(symbol)


@pytest.fixture(autouse=True)
def _no_retry_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(actions, "CANCEL_CONFIRM_BACKOFF_S", 0.0)
    monkeypatch.setattr(actions, "UNKNOWN_VERIFY_DELAY_S", 0.0)


@pytest.fixture
async def h(pg_engine: Engine) -> Harness:
    harness = make_harness(ledger_sessions(pg_engine), Account.PAPER, KEYRING, clock=FakeClock(NOW))
    harness.exchange.set_mark(BTC, "50000")
    harness.exchange.set_book(BTC, [("49990.0", "5")], [("50010.0", "5")])
    await harness.start()
    return harness


async def _execute(h: Harness, intents: list[OrderIntent]) -> None:
    for n, intent in enumerate(intents):
        assert await h.intake.handle(f"5-{n}", RawPayload(json.loads(intent.model_dump_json())))
        await h.pump()  # the user stream delivers the fill before the next intent is read


async def _rebalance(h: Harness, seq: int, delta: str, stop: str, *, reduce_only: bool = False) -> None:
    intents = hedge_intents(
        account=Account.PAPER,
        seq=seq,
        delta=Decimal(delta),
        stop=Decimal(stop),
        now=h.clock(),
        reduce_only=reduce_only,
    )
    await _execute(h, [i for i in intents if not (reduce_only and i.leg is Leg.SL)])


async def test_first_rebalance_from_the_hedger_opens_one_btc_position_with_one_stop(
    h: Harness, pg_engine: Engine, redis_client: Redis
) -> None:
    with pg_engine.begin() as conn:
        cs = default_candidate_set()
        store_candidate_set(conn, cs)
        # newer than any packet other tests stored for the coin (the hedger reads the newest one)
        store_packet(
            conn, packet(cs, features={**FEATURES, "btc_atr_1h": 333.33}, as_of=NOW + timedelta(minutes=5))
        )
    states = AccountStateBook()
    states.offer(account_state(positions=(position(SOL, "8"), position(ETH, "8"))))  # 2 x 800 USD long
    clock = FakeClock(NOW)
    rebalancer = HedgeRebalancer(
        account=Account.PAPER,
        sessions=h.ns.sessions,
        signer=RISK_SIGNER,
        states=states,
        market=BtcMarket(),  # type: ignore[arg-type]
        outbox=IntentOutbox(account=Account.PAPER, sessions=h.ns.sessions, redis=redis_client, clock=clock),
        runtime=lambda: RiskRuntime(
            risk=risk_file(), stale_s=180.0, config_version_ids={}, mode=Account.PAPER
        ),
        clock=clock,
    )
    row = await rebalancer.run_once()
    assert row is not None
    assert row.target_qty == Decimal("-0.032")
    assert row.stop_price == Decimal("51000.0")
    published = [
        OrderIntent.model_validate_json(fields[b"data"])
        for _id, fields in await redis_client.xrange(str(Stream.ORDERS))
    ]
    assert {i.leg for i in published} == {Leg.HEDGE, Leg.SL}

    await _execute(h, published)

    assert h.exchange.venue.positions[BTC].qty == Decimal("-0.032")
    (stop,) = working(h, symbol=BTC)
    assert stop.close_position
    assert stop.side is OrderSide.BUY
    assert stop.trigger_price == Decimal("51000.0")
    pos = h.ledger.position(BTC)
    assert pos is not None
    assert pos.is_hedge_book
    report = await h.reconcile()
    assert report.clean
    assert report.stop_invariant_ok


async def test_growing_the_book_keeps_one_position_and_moves_to_the_new_stop(h: Harness) -> None:
    await _rebalance(h, 1, "-0.032", "51000.0")
    (first,) = working(h, symbol=BTC)
    await _rebalance(h, 2, "-0.008", "50800.0")
    assert h.exchange.venue.positions[BTC].qty == Decimal("-0.040")
    (stop,) = working(h, symbol=BTC)
    assert stop.client_algo_id != first.client_algo_id
    assert stop.trigger_price == Decimal("50800.0")
    calls = h.exchange.calls
    assert calls.index(f"place_algo:{stop.client_algo_id}") < calls.index(
        f"cancel_algo:{first.client_algo_id}"
    )
    assert (await h.reconcile()).clean


async def _two_rebalances(h: Harness) -> tuple[OrderIntent, OrderIntent]:
    """Book short 0.032 stopped at 51000, then grown by 0.008 and moved to 50800: (SL1, SL2)."""
    first = hedge_intents(
        account=Account.PAPER, seq=1, delta=Decimal("-0.032"), stop=Decimal("51000.0"), now=h.clock()
    )
    await _execute(h, first)
    second = hedge_intents(
        account=Account.PAPER, seq=2, delta=Decimal("-0.008"), stop=Decimal("50800.0"), now=h.clock()
    )
    await _execute(h, second)
    sl1, sl2 = (next(i for i in batch if i.leg is Leg.SL) for batch in (first, second))
    return sl1, sl2


def _book_stop(h: Harness) -> tuple[str, Decimal]:
    (stop,) = working(h, symbol=BTC)
    pos = h.ledger.position(BTC)
    assert pos is not None
    assert pos.stop_price == stop.trigger_price
    return stop.client_algo_id, stop.trigger_price


async def test_re_delivered_older_book_stop_leaves_the_newer_one_working(h: Harness) -> None:
    sl1, sl2 = await _two_rebalances(h)
    before = _book_stop(h)
    assert before[1] == Decimal("50800.0")
    for n, sl in enumerate((sl1, sl2)):  # the stream re-delivers both after a consumer restart
        assert await h.intake.handle(f"6-{n}", RawPayload(json.loads(sl.model_dump_json())))
        await h.pump()
    assert _book_stop(h) == before
    assert (await h.reconcile()).clean


async def test_older_book_stop_reaching_the_order_manager_again_is_done_without_placing(h: Harness) -> None:
    sl1, sl2 = await _two_rebalances(h)
    before = _book_stop(h)
    placements = len(h.exchange.mutating_calls("place_algo"))
    async with h.ns.lock:
        await h.manager.act(sl1)
    await h.pump()
    assert _book_stop(h) == before
    assert len(h.exchange.mutating_calls("place_algo")) == placements
    for sl in (sl1, sl2):  # SL2 finished once its stop worked; SL1 is superseded, never placed again
        row = h.ledger.intent_row(sl.intent_id)
        assert row is not None
        assert row.status == "done"


async def test_an_older_book_stop_delivered_after_a_newer_one_never_replaces_it(h: Harness) -> None:
    """Review cycle 3 #1: the book's newest stop is the one of the highest hedge seq, whatever order the
    stream delivers them in (a message left pending is reclaimed 60 s later, after newer rebalances)."""
    await _rebalance(h, 1, "-0.032", "51000.0")
    sl1, hedge1 = hedge_intents(
        account=Account.PAPER, seq=2, delta=Decimal("-0.008"), stop=Decimal("50900.0"), now=h.clock()
    )
    await _execute(h, [hedge1])  # SL1 left pending (a transient DB error, a crash before the record)
    third = hedge_intents(
        account=Account.PAPER, seq=3, delta=Decimal("-0.004"), stop=Decimal("50800.0"), now=h.clock()
    )
    await _execute(h, third)
    sl2 = third[0]
    assert _book_stop(h) == (sl2.client_id, Decimal("50800.0"))
    await _execute(h, [sl1])  # reclaimed and delivered last
    assert h.exchange.venue.positions[BTC].qty == Decimal("-0.044")
    assert _book_stop(h) == (sl2.client_id, Decimal("50800.0"))
    row = h.ledger.intent_row(sl1.intent_id)
    assert row is not None
    assert row.status == "done"  # superseded, never placed
    assert f"place_algo:{sl1.client_id}" not in h.exchange.calls
    assert (await h.reconcile()).clean


async def test_closing_the_book_leaves_nothing_behind(h: Harness) -> None:
    await _rebalance(h, 1, "-0.032", "51000.0")
    await _rebalance(h, 2, "0.032", "1", reduce_only=True)
    assert BTC not in h.exchange.venue.positions
    assert working(h, symbol=BTC) == []
    assert h.ledger.position(BTC) is None
    assert (await h.reconcile()).clean


async def test_flipping_the_book_keeps_the_short_stop_until_the_long_exists(h: Harness) -> None:
    await _rebalance(h, 1, "-0.032", "51000.0")
    (short_stop,) = working(h, symbol=BTC)
    await _rebalance(h, 2, "0.052", "49000.0")  # short 0.032 -> long 0.020
    assert h.exchange.venue.positions[BTC].qty == Decimal("0.020")
    (long_stop,) = working(h, symbol=BTC)
    assert long_stop.close_position
    assert long_stop.side is OrderSide.SELL
    assert long_stop.trigger_price == Decimal("49000.0")
    calls = h.exchange.calls
    hedges = [i for i, c in enumerate(calls) if c.startswith("place_order:") and leg_of_call(c) is Leg.HEDGE]
    assert len(hedges) == 2
    assert calls.index(f"cancel_algo:{short_stop.client_algo_id}") > hedges[1]  # covered until the flip
    assert calls.index(f"place_algo:{long_stop.client_algo_id}") < calls.index(
        f"cancel_algo:{short_stop.client_algo_id}"
    )
    pos = h.ledger.position(BTC)
    assert pos is not None
    assert pos.is_hedge_book
    report = await h.reconcile()
    assert report.clean
    assert report.stop_invariant_ok


async def test_refused_flip_leaves_the_short_book_with_its_stop(h: Harness) -> None:
    await _rebalance(h, 1, "-0.032", "51000.0")
    (short_stop,) = working(h, symbol=BTC)
    book_stop, flip = hedge_intents(
        account=Account.PAPER, seq=2, delta=Decimal("0.052"), stop=Decimal("49000.0"), now=h.clock()
    )
    assert await h.send([book_stop]) == ["accepted"]
    h.resync.begin()  # the namespace stops opening before the flip arrives
    assert await h.send([flip]) == ["rejected"]
    assert h.exchange.venue.positions[BTC].qty == Decimal("-0.032")
    assert working(h, symbol=BTC) == [short_stop]
    assert h.exchange.mutating_calls("cancel_algo") == []
