"""The hedge book respects the same-direction risk ceiling (phase 09 section 2, review H9).

The rebalancer caps a growing book so the book's side (the book to its stop + that side's alt exposure)
stays within `same_direction_risk_max x equity`, and skips growth the cap leaves below the BTC minimum.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

import pytest
from redis.asyncio import Redis
from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

from fake_exchange import BTC, ETH, FILTERS, SOL, FakeClock, ledger_sessions
from hdt.contracts.common import Account, Leg
from hdt.contracts.order import OrderIntent
from hdt.contracts.streams import Stream
from hdt.execution.filters import SymbolFilters
from hdt.quant.packet import store_candidate_set, store_packet
from hdt.risk.consumer import AccountStateBook, IntentOutbox, Mark, RiskRuntime
from hdt.risk.hedger import HedgeRebalancer
from intent_builders import RISK_SIGNER
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

pytestmark = [pytest.mark.pg, pytest.mark.redis, pytest.mark.integration]


@dataclass(frozen=True)
class Member:
    cmc_id: int


class BtcMarket:
    """BTC filters and a 50 000 mark, universe membership of the alts."""

    def filters(self, symbol: str, now: datetime) -> SymbolFilters | None:
        return FILTERS.get(symbol)

    def marks(self, symbols: set[str], now: datetime) -> dict[str, Mark]:
        return {BTC: Mark(Decimal("50000"), 1.0)} if BTC in symbols else {}

    def member_by_symbol(self, symbol: str, now: datetime) -> Member | None:
        return {SOL: Member(COIN_ID), ETH: Member(COIN_ID + 1)}.get(symbol)


@pytest.fixture
def sessions(pg_engine: Engine) -> sessionmaker[Session]:
    factory = ledger_sessions(pg_engine)
    with pg_engine.begin() as conn:
        cs = default_candidate_set()
        store_candidate_set(conn, cs)
        # BTC ATR 333.33: the book stop sits 1 000 USD per BTC away (3x ATR, tick-rounded)
        store_packet(
            conn, packet(cs, features={**FEATURES, "btc_atr_1h": 333.33}, as_of=NOW + timedelta(minutes=5))
        )
    return factory


def _rebalancer(sessions: sessionmaker[Session], redis: Redis, equity: str) -> HedgeRebalancer:
    states = AccountStateBook()
    # 2 x 800 USD long x beta 1.2 -> a -0.032 BTC target, 32 USD of SHORT risk to its stop
    states.offer(account_state(equity=equity, positions=(position(SOL, "8"), position(ETH, "8"))))
    clock = FakeClock(NOW)
    return HedgeRebalancer(
        account=Account.PAPER,
        sessions=sessions,
        signer=RISK_SIGNER,
        states=states,
        market=BtcMarket(),  # type: ignore[arg-type]
        outbox=IntentOutbox(account=Account.PAPER, sessions=sessions, redis=redis, clock=clock),
        runtime=lambda: RiskRuntime(
            risk=risk_file(), stale_s=180.0, config_version_ids={}, mode=Account.PAPER
        ),
        clock=clock,
    )


async def _published(redis: Redis) -> list[OrderIntent]:
    return [OrderIntent.model_validate_json(f[b"data"]) for _id, f in await redis.xrange(str(Stream.ORDERS))]


async def test_growth_is_capped_to_the_same_direction_budget(
    sessions: sessionmaker[Session], redis_client: Redis
) -> None:
    # Equity 2 000: the 1.5% budget is 30 USD, room for 0.030 BTC at 1 000 USD per BTC.
    row = await _rebalancer(sessions, redis_client, "2000").run_once()
    assert row is not None
    assert row.target_qty == Decimal("-0.032")
    (hedge,) = [i for i in await _published(redis_client) if i.leg is Leg.HEDGE]
    assert hedge.qty == Decimal("0.030")


async def test_growth_below_the_minimum_after_the_cap_is_skipped(
    sessions: sessionmaker[Session], redis_client: Redis, caplog: pytest.LogCaptureFixture
) -> None:
    # Equity 100: 1.5 USD of room = 0.001 BTC = 50 USD notional, below the 100 USD minNotional.
    with caplog.at_level(logging.WARNING, logger="hdt.risk.hedger"):
        assert await _rebalancer(sessions, redis_client, "100").run_once() is None
    assert any(r.getMessage() == "hedge book growth capped" for r in caplog.records)
    assert await _published(redis_client) == []


async def test_a_book_within_the_budget_is_not_capped(
    sessions: sessionmaker[Session], redis_client: Redis
) -> None:
    await _rebalancer(sessions, redis_client, "10000").run_once()
    (hedge,) = [i for i in await _published(redis_client) if i.leg is Leg.HEDGE]
    assert hedge.qty == Decimal("0.032")
