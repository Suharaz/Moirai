"""Phase 09 acceptance on the Binance demo account: 20 consecutive driver lifecycles, STOP p99 <= 2 s.

Runs only with the demo account's keys (HDT_TESTNET_API_KEY / HDT_TESTNET_API_SECRET, environment or dev
`.env`); skipped otherwise. The account must be in One-way mode and flat on the driver's symbol.

Production path end to end: the key is sealed into the test database's vault (scope `exec`), the execution
service attaches its `testnet` namespace (One-way check, first re-sync over the private user stream,
`orders` consumer, AccountState publisher, reconciler, timers), and the driver talks to it only through
Redis (signed intents on `orders`, observations on `account_state`) with public demo market data.

STOP latency is exchange-anchored, not taken from the 1 Hz AccountState snapshots: a read-only probe with
the demo key (`BinanceStopProbe`) measures from the exchange time of each lifecycle's first entry fill
(`userTrades`) to the `createTime` of the covering STOP listed in `openAlgoOrders`. Each measurement is
cross-checked with the ledger: the fill time equals the first `fills.filled_at` of the event's entry legs,
and the listed STOP is the ledger's `sl` algo order with the `algoId` execution's placement acknowledged.
"""

from __future__ import annotations

import asyncio
import json
from datetime import timedelta
from pathlib import Path

import httpx
import pytest
import sqlalchemy as sa
from nacl.public import PrivateKey
from redis.asyncio import Redis
from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

from fake_exchange import ledger_sessions
from hdt.contracts.common import Account, Leg
from hdt.core.clock import utcnow
from hdt.core.config import read_env_value, static_config
from hdt.db.models.ledger import AlgoOrderRow, FillRow
from hdt.db.models.settings import SecretRow
from hdt.execution.binance_adapter import BinanceAdapter
from hdt.execution.binance_client import BinanceClient
from hdt.execution.key_check import binance_rest
from hdt.execution.main import ExecutionService
from hdt.execution.namespaces import BINANCE_SECRET
from hdt.execution.testnet_driver import (
    ENTRY_LEGS,
    AccountStateObserver,
    BinanceStopProbe,
    DemoMarket,
    LifecycleResult,
    RedisOrdersSink,
    run_lifecycles,
    summarize,
)
from hdt.lake.pit_query import PitQuery
from hdt.vault.loader import SecretLoader
from hdt.vault.sealed import fingerprint, last4, seal
from intent_builders import DRIVER_SIGNER, KEYRING

API_KEY = read_env_value("HDT_TESTNET_API_KEY")
API_SECRET = read_env_value("HDT_TESTNET_API_SECRET")
LIFECYCLES = 20
MAX_STOP_LATENCY_S = 2.0
SYNC_TIMEOUT_S = 60.0

pytestmark = [
    pytest.mark.testnet,
    pytest.mark.pg,
    pytest.mark.redis,
    pytest.mark.integration,
    pytest.mark.skipif(
        API_KEY is None or API_SECRET is None,
        reason="demo account keys HDT_TESTNET_API_KEY / HDT_TESTNET_API_SECRET are not set",
    ),
]


async def _until(what: str, predicate: object, timeout_s: float) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    while True:
        value = predicate() if callable(predicate) else predicate
        if asyncio.iscoroutine(value):
            value = await value
        if value:
            return
        assert loop.time() < deadline, f"{what} not reached within {timeout_s:g} s"
        await asyncio.sleep(0.5)


async def test_twenty_driver_lifecycles_on_the_demo_account(
    pg_engine: Engine, redis_client: Redis, tmp_path: Path
) -> None:
    assert API_KEY is not None
    assert API_SECRET is not None
    static = static_config()
    sessions = ledger_sessions(pg_engine)
    owner_key = PrivateKey.generate()
    plaintext = json.dumps({"api_key": API_KEY, "api_secret": API_SECRET}).encode()
    with sessions.begin() as s:
        s.execute(sa.delete(SecretRow).where(SecretRow.scope == "exec"))
        s.add(
            SecretRow(
                scope="exec",
                name=BINANCE_SECRET[Account.TESTNET],
                version=1,
                sealed_blob=seal(owner_key.public_key, plaintext),
                last4=last4(API_KEY),
                fingerprint=fingerprint(plaintext),
                status="active",
                reason=None,
                check_details={},
                last_request_id=None,
                checked_at=utcnow(),
                created_by="testnet-acceptance",
                created_at=utcnow(),
            )
        )
    service = ExecutionService(
        static=static,
        sessions=sessions,
        redis=redis_client,
        keyring=KEYRING,
        pit=PitQuery(tmp_path / "staging", tmp_path / "lake"),
        vault=SecretLoader("exec", sessions, owner_key),
        owner_key=owner_key,
    )
    await service.attach(Account.TESTNET)
    assert Account.TESTNET in service.attached, f"not attached (refused: {service.refused}), see the log"
    observer = AccountStateObserver(redis_client)
    try:
        ns = service.attached[Account.TESTNET].rt.ns
        await _until("first re-sync over the user stream", lambda: ns.synced, SYNC_TIMEOUT_S)
        await _until("first testnet AccountState", observer.observe, SYNC_TIMEOUT_S)
        probe_adapter = BinanceAdapter(
            Account.TESTNET,
            BinanceClient(
                base_url=binance_rest(Account.TESTNET, static.binance),
                api_key=API_KEY,
                api_secret=API_SECRET,
                recv_window_ms=static.binance.recv_window_ms,
                timeout_s=static.binance.request_timeout_s,
            ),
        )
        try:
            offset_ms = await probe_adapter.client.sync_time()
            probe = BinanceStopProbe(probe_adapter, since=utcnow() + timedelta(milliseconds=offset_ms))
            async with httpx.AsyncClient(timeout=static.binance.request_timeout_s) as http:
                results = await run_lifecycles(
                    market=DemoMarket(http, static.binance.demo.rest),
                    sink=RedisOrdersSink(redis_client),
                    observer=observer,
                    signer=DRIVER_SIGNER,
                    n=LIFECYCLES,
                    probe=probe,
                )
        finally:
            await probe_adapter.aclose()
    finally:
        await service.detach(Account.TESTNET)

    assert [r.error for r in results] == [None] * LIFECYCLES
    assert all(r.closed for r in results)
    for r in results:
        _cross_check_with_the_ledger(sessions, r)
    summary = summarize(results)
    assert summary.p99_s is not None
    assert summary.p99_s <= MAX_STOP_LATENCY_S, summary.as_dict()


def _cross_check_with_the_ledger(sessions: sessionmaker[Session], r: LifecycleResult) -> None:
    """The exchange's fill time and listed STOP are the ones execution booked and had acknowledged."""
    stop = r.exchange_stop
    assert stop is not None, r.as_dict()
    assert stop.stop_at >= stop.fill_at, r.as_dict()  # the STOP is placed after the fill it covers
    assert r.stop_latency_s == (stop.stop_at - stop.fill_at).total_seconds()
    with sessions() as s:
        first_fill = s.scalar(
            sa.select(sa.func.min(FillRow.filled_at)).where(
                FillRow.account == Account.TESTNET.value,
                FillRow.event_id == r.event_id,
                FillRow.leg.in_([leg.value for leg in ENTRY_LEGS]),
            )
        )
        algo = s.get(AlgoOrderRow, (Account.TESTNET.value, stop.client_algo_id))
    assert first_fill == stop.fill_at, r.as_dict()
    assert algo is not None, r.as_dict()
    assert (algo.event_id, algo.leg) == (r.event_id, Leg.SL.value)
    assert algo.algo_id is not None
    assert algo.algo_id == stop.algo_id, r.as_dict()  # the acknowledged STOP is the one listed
