"""Risk alerts page once per occurrence, not once per account forever (review cycle 2, C3-R).

- `integrity_error`: one alert per decision (episode = event id, else the stream id);
- `account_state_stale`: one alert per namespace, resolved by the next fresh AccountState so the next stale
  episode pages again;
- `price_crosscheck`: one alert per (account, symbol, UTC date).
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
import sqlalchemy as sa
from redis.asyncio import Redis
from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

import hdt.risk.main as risk_main
from fake_exchange import FakeClock, ledger_sessions
from hdt.contracts.common import Account
from hdt.core.config import static_config
from hdt.core.streams import RawPayload
from hdt.db.models.ops import AlertRow
from hdt.db.session import transaction
from hdt.lake.pit_query import PitQuery
from hdt.ops.alerts import resolve_alert
from hdt.quant.packet import store_candidate_set, store_packet
from hdt.risk.consumer import AccountStateBook, DecisionConsumer, RiskRuntime
from hdt.risk.gate import MarketInputs
from hdt.risk.main import RiskService
from hdt.risk.veto import FlagsBook
from intent_builders import RISK_SIGNER
from risk_builders import (
    NOW,
    account_state,
    decision,
    default_candidate_set,
    default_packet,
    flags,
    market,
    position,
    risk_file,
)

pytestmark = [pytest.mark.pg, pytest.mark.redis, pytest.mark.integration]


class FixedMarket:
    def __init__(self, inputs: MarketInputs) -> None:
        self.inputs = inputs

    def market(self, coin_id: int, now: datetime) -> MarketInputs:
        return self.inputs


@pytest.fixture
def sessions(pg_engine: Engine) -> sessionmaker[Session]:
    factory = ledger_sessions(pg_engine)
    with pg_engine.begin() as conn:
        store_candidate_set(conn, default_candidate_set())
        store_packet(conn, default_packet())
    with transaction(factory) as s:  # alerts left open by earlier tests of the session
        for kind in ("integrity_error", "account_state_stale", "price_crosscheck"):
            for key in _open_keys(s, kind):
                resolve_alert(s, kind=kind, dedupe_key=key)
    return factory


def _open_keys(s: Session, kind: str) -> list[str]:
    return list(
        s.scalars(
            sa.select(AlertRow.dedupe_key).where(
                AlertRow.kind == kind, AlertRow.resolved_at.is_(None), AlertRow.is_test.is_(False)
            )
        )
    )


def _open(sessions: sessionmaker[Session], kind: str) -> list[str]:
    with sessions() as s:
        return sorted(_open_keys(s, kind))


def _consumer(
    sessions: sessionmaker[Session], redis: Redis, states: AccountStateBook, now: datetime
) -> DecisionConsumer:
    book = FlagsBook.empty()
    book.offer(flags(now=now))
    return DecisionConsumer(
        account=Account.PAPER,
        sessions=sessions,
        redis=redis,
        signer=RISK_SIGNER,
        market=FixedMarket(market()),
        flags=book,
        states=states,
        runtime=lambda: RiskRuntime(risk_file(), 180.0, {"risk": 1}, Account.PAPER),
        clock=FakeClock(now),
    )


def _service(sessions: sessionmaker[Session], redis: Redis, root: Path) -> RiskService:
    return RiskService(
        static=static_config(), sessions=sessions, redis=redis, signer=RISK_SIGNER, pit=PitQuery(root, root)
    )


async def test_every_integrity_rejection_pages_on_its_own(
    sessions: sessionmaker[Session], redis_client: Redis
) -> None:
    consumer = _consumer(sessions, redis_client, AccountStateBook(), NOW)
    consumer.states.offer(account_state())
    first, second = (f"evt-{uuid.uuid4().hex[:8]}" for _ in range(2))
    for event_id in (first, second):
        d = decision(event_id=event_id, packet_sha256="ab" * 32)  # no packet stored under this hash
        assert await consumer.handle("1-1", RawPayload(d.model_dump(mode="json")))
    malformed = {**decision(event_id="evt-malformed").model_dump(mode="json"), "schema_version": 99}
    assert await consumer.handle("1-2", RawPayload(malformed))
    assert _open(sessions, "integrity_error") == sorted(
        [f"paper:{first}", f"paper:{second}", "paper:evt-malformed"]
    )


async def test_a_fresh_account_state_resolves_the_stale_alert_so_the_next_one_pages(
    sessions: sessionmaker[Session], redis_client: Redis, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    svc = _service(sessions, redis_client, tmp_path)
    clock = [NOW]
    monkeypatch.setattr(risk_main, "utcnow", lambda: clock[0])

    async def state_at(ts: datetime) -> None:
        clock[0] = ts + timedelta(seconds=2)
        assert await svc.on_state("1-0", account_state(ts=ts))

    def stale_decision(at: datetime) -> None:
        consumer = _consumer(sessions, redis_client, svc.states, at)  # the last state is 60 s old
        d = decision(event_id=f"evt-{uuid.uuid4().hex[:8]}", as_of=at)
        result = consumer.decide("1-1", d, market(), consumer.runtime(), at)
        assert result is not None
        assert result.reason == "account_state_stale"

    await state_at(NOW)
    stale_decision(NOW + timedelta(seconds=60))
    assert _open(sessions, "account_state_stale") == ["paper"]
    await state_at(NOW + timedelta(seconds=61))
    assert _open(sessions, "account_state_stale") == []
    stale_decision(NOW + timedelta(seconds=121))
    assert _open(sessions, "account_state_stale") == ["paper"]  # the next stale episode pages again


async def test_price_crosscheck_pages_per_symbol_and_utc_date(
    sessions: sessionmaker[Session], redis_client: Redis, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    svc = _service(sessions, redis_client, tmp_path)
    clock = [NOW]
    monkeypatch.setattr(risk_main, "utcnow", lambda: clock[0])
    members = {
        "SOLUSDT": SimpleNamespace(cmc_id=1, multiplier=1),
        "ETHUSDT": SimpleNamespace(cmc_id=2, multiplier=1),
    }
    monkeypatch.setattr(risk_main, "cmc_prices", lambda _pit, _now: {1: Decimal("150"), 2: Decimal("150")})
    monkeypatch.setattr(svc.market, "member_by_symbol", lambda symbol, _now: members.get(symbol))
    svc.namespaces[Account.PAPER] = None  # only the keys are read
    svc.states.offer(account_state(positions=(position("SOLUSDT"), position("ETHUSDT"))))
    assert await svc.crosscheck_once() == 2
    day = NOW.date().isoformat()
    assert _open(sessions, "price_crosscheck") == [f"paper:ETHUSDT:{day}", f"paper:SOLUSDT:{day}"]
    clock[0] = NOW + timedelta(days=1)  # the gap persists into the next UTC day: it pages again
    assert await svc.crosscheck_once() == 2
    assert len(_open(sessions, "price_crosscheck")) == 4
