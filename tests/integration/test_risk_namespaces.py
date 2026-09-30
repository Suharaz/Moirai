"""Risk namespaces follow the run mode, but never abandon exposure (phase 09 section 1, review H7).

A namespace the run mode no longer enables stays attached while its ledger holds an open position or a
working order: its EXIT/HOLD decisions, veto exits and hedge book keep running and every OPEN is rejected
(`mode_disabled`). It detaches once flat, and a restart re-attaches a still exposed namespace. A re-attach
after a long detach handles every decision it missed: a decision has no time limit (owner decision
2026-09-28). A stored config version outside the hard ceilings runs clipped with one critical alert (review
m5); a stored version carrying a retired key runs without one.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
import sqlalchemy as sa
from redis.asyncio import Redis
from sqlalchemy import Engine, update
from sqlalchemy.orm import Session, sessionmaker

from alert_logs import raised
from fake_exchange import ledger_sessions
from hdt.contracts.common import Account
from hdt.contracts.streams import Stream
from hdt.core.clock import utcnow
from hdt.core.config import static_config
from hdt.core.streams import encode
from hdt.db.models.ledger import PositionRow
from hdt.db.models.ops import AlertRow
from hdt.db.models.risk import ProcessedDecisionRow
from hdt.lake.pit_query import PitQuery
from hdt.risk.consumer import RiskRuntime
from hdt.risk.main import RiskService
from hdt.settings.ceilings import LEVERAGE_MAX
from hdt.settings.schemas import BINANCE_FIELDS, BINANCE_RISK_FIELDS, RISK_FIELDS, Section
from hdt.settings.versions import ConfigVersion
from intent_builders import RISK_SIGNER
from risk_builders import decision, risk_file

pytestmark = [pytest.mark.pg, pytest.mark.redis, pytest.mark.integration]


@pytest.fixture
def sessions(pg_engine: Engine) -> sessionmaker[Session]:
    return ledger_sessions(pg_engine)


def _service(sessions: sessionmaker[Session], redis: Redis, root: Path, mode: list[Account]) -> RiskService:
    svc = RiskService(
        static=static_config(), sessions=sessions, redis=redis, signer=RISK_SIGNER, pit=PitQuery(root, root)
    )
    svc.block_ms = 10  # detach waits for the blocking read to return
    svc.mode = lambda: mode[0]  # type: ignore[method-assign]
    return svc


@pytest.fixture
async def services(
    sessions: sessionmaker[Session], redis_client: Redis, tmp_path: Path
) -> AsyncIterator[list[RiskService]]:
    started: list[RiskService] = []
    yield started
    for svc in started:
        for account in list(svc.namespaces):
            await svc.detach(account)


def _open_position(sessions: sessionmaker[Session], account: Account) -> int:
    now = utcnow()
    with sessions.begin() as s:
        row = PositionRow(
            account=account.value,
            event_id="evt-live",
            symbol="SOLUSDT",
            side="LONG",
            qty=Decimal("9"),
            max_qty=Decimal("9"),
            entry_price=Decimal("100"),
            unrealized_pnl=Decimal(0),
            leverage=3,
            stop_price=Decimal("98"),
            is_hedge_book=False,
            opened_at=now,
            exit_notional=Decimal(0),
            exit_qty=Decimal(0),
            realized_pnl=Decimal(0),
            fees_funding_usd=Decimal(0),
            exit_in_progress=False,
            tp1_done=False,
            updated_at=now,
        )
        s.add(row)
        s.flush()
        return row.position_id


def _close(sessions: sessionmaker[Session], position_id: int) -> None:
    with sessions.begin() as s:
        s.execute(
            update(PositionRow).where(PositionRow.position_id == position_id).values(closed_at=utcnow())
        )


async def test_a_namespace_with_exposure_stays_attached_until_flat(
    sessions: sessionmaker[Session], redis_client: Redis, tmp_path: Path, services: list[RiskService]
) -> None:
    mode = [Account.LIVE]
    svc = _service(sessions, redis_client, tmp_path, mode)
    services.append(svc)
    await svc.sync_namespaces()
    assert set(svc.namespaces) == {Account.PAPER, Account.LIVE}

    position_id = _open_position(sessions, Account.LIVE)
    mode[0] = Account.PAPER  # the operator leaves live while a live position is open
    await svc.sync_namespaces()
    assert set(svc.namespaces) == {Account.PAPER, Account.LIVE}
    assert svc.winding_down == {Account.LIVE}

    _close(sessions, position_id)
    await svc.sync_namespaces()
    assert set(svc.namespaces) == {Account.PAPER}
    assert svc.winding_down == frozenset()


async def test_a_restart_reattaches_an_exposed_namespace_the_mode_no_longer_enables(
    sessions: sessionmaker[Session], redis_client: Redis, tmp_path: Path, services: list[RiskService]
) -> None:
    _open_position(sessions, Account.LIVE)
    svc = _service(sessions, redis_client, tmp_path, [Account.PAPER])
    services.append(svc)
    await svc.sync_namespaces()
    assert set(svc.namespaces) == {Account.PAPER, Account.LIVE}


def test_the_runtime_opens_only_namespaces_the_mode_enables() -> None:
    paper = RiskRuntime(risk_file(), 180.0, {}, Account.PAPER)
    live = RiskRuntime(risk_file(), 180.0, {}, Account.LIVE)
    assert (paper.opens(Account.PAPER), paper.opens(Account.LIVE)) == (True, False)
    assert (live.opens(Account.PAPER), live.opens(Account.LIVE)) == (True, True)
    testnet = RiskRuntime(risk_file(), 180.0, {}, Account.TESTNET)
    assert [testnet.opens(a) for a in (Account.PAPER, Account.TESTNET, Account.LIVE)] == [False, True, False]


async def test_testnet_mode_sends_decisions_to_testnet_only_and_winds_paper_down(
    sessions: sessionmaker[Session], redis_client: Redis, tmp_path: Path, services: list[RiskService]
) -> None:
    mode = [Account.PAPER]
    svc = _service(sessions, redis_client, tmp_path, mode)
    services.append(svc)
    position_id = _open_position(sessions, Account.PAPER)
    await svc.sync_namespaces()
    assert set(svc.namespaces) == {Account.PAPER}

    mode[0] = Account.TESTNET  # council decisions move to the demo account; the paper position winds down
    await svc.sync_namespaces()
    assert set(svc.namespaces) == {Account.PAPER, Account.TESTNET}
    assert svc.winding_down == {Account.PAPER}

    _close(sessions, position_id)
    await svc.sync_namespaces()
    assert set(svc.namespaces) == {Account.TESTNET}
    assert svc.winding_down == frozenset()


def _processed(sessions: sessionmaker[Session], event_id: str) -> int:
    with sessions() as s:
        q = (
            sa.select(sa.func.count())
            .select_from(ProcessedDecisionRow)
            .where(ProcessedDecisionRow.event_id == event_id)
        )
        return int(s.scalar(q) or 0)


async def test_a_reattach_after_a_long_detach_handles_every_missed_decision(
    sessions: sessionmaker[Session], redis_client: Redis, tmp_path: Path, services: list[RiskService]
) -> None:
    svc = _service(sessions, redis_client, tmp_path, [Account.PAPER])
    services.append(svc)
    await svc.attach(Account.PAPER)
    await svc.detach(Account.PAPER)  # e.g. weeks on another mode: the group stays where it stopped
    seconds, micros = await redis_client.time()
    now_ms = int(seconds) * 1000 + int(micros) // 1000
    late = utcnow() - timedelta(hours=1)  # no decision TTL: neither is skipped for its age
    stream = str(Stream.DECISIONS)
    await redis_client.xadd(
        stream, encode(decision(event_id="evt-old", as_of=late)), id=f"{now_ms - 600_000}-0"
    )
    await redis_client.xadd(
        stream, encode(decision(event_id="evt-young", as_of=late)), id=f"{now_ms - 1_000}-0"
    )
    await svc.attach(Account.PAPER)
    for _ in range(100):
        if _processed(sessions, "evt-young"):
            break
        await asyncio.sleep(0.05)
    assert (_processed(sessions, "evt-old"), _processed(sessions, "evt-young")) == (1, 1)


def _version(section: Section, version_id: int, payload: dict[str, object]) -> ConfigVersion:
    return ConfigVersion(
        id=version_id,
        section=section,
        payload=payload,
        payload_sha256="0" * 64,
        schema_version=1,
        author="test",
        reason="saved before the ceilings were tightened",
        created_at=datetime(2026, 9, 1, tzinfo=UTC),
        parent_id=None,
    )


async def test_a_stored_version_outside_the_ceilings_runs_clipped_with_one_critical_alert(
    sessions: sessionmaker[Session], redis_client: Redis, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    static = static_config()
    svc = RiskService(
        static=static,
        sessions=sessions,
        redis=redis_client,
        signer=RISK_SIGNER,
        pit=PitQuery(tmp_path, tmp_path),
    )
    risk_id = 1_000_000 + uuid.uuid4().int % 1_000_000
    svc.watcher.current.update(
        {
            Section.RISK: _version(
                Section.RISK, risk_id, {**static.risk.model_dump(include=set(RISK_FIELDS)), "leverage_max": 5}
            ),
            Section.BINANCE: _version(
                Section.BINANCE,
                risk_id + 1,
                {
                    **static.binance.model_dump(include=set(BINANCE_FIELDS)),
                    **static.risk.model_dump(include=set(BINANCE_RISK_FIELDS)),
                },
            ),
            Section.MODE: _version(Section.MODE, risk_id + 2, {"mode": "live", "size_multiplier": 0.25}),
        }
    )
    with caplog.at_level(logging.WARNING):
        runtimes = [svc.runtime() for _ in range(3)]
        assert svc.mode() is Account.LIVE
    assert {rt.risk.leverage_max for rt in runtimes} == {LEVERAGE_MAX}
    assert len([r for r in caplog.records if raised(r, "config_invalid")]) == 1
    with sessions() as s:
        severity = s.scalars(
            sa.select(AlertRow.severity).where(
                AlertRow.kind == "config_invalid",
                AlertRow.dedupe_key == f":risk:{risk_id}",
                AlertRow.resolved_at.is_(None),
            )
        ).all()
    assert severity == ["critical"]


async def test_a_stored_risk_version_with_the_retired_decision_ttl_runs_without_an_alert(
    sessions: sessionmaker[Session], redis_client: Redis, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Config versions are immutable: one saved before 2026-09-28 still carries `decision_ttl_s`. It is
    dropped on load (no `config_invalid`), while every other field of the version is still enforced."""
    static = static_config()
    svc = RiskService(
        static=static,
        sessions=sessions,
        redis=redis_client,
        signer=RISK_SIGNER,
        pit=PitQuery(tmp_path, tmp_path),
    )
    risk_id = 1_000_000 + uuid.uuid4().int % 1_000_000
    stored = {**static.risk.model_dump(include=set(RISK_FIELDS)), "decision_ttl_s": 120, "min_rr": 2.5}
    binance = {
        **static.binance.model_dump(include=set(BINANCE_FIELDS)),
        **static.risk.model_dump(include=set(BINANCE_RISK_FIELDS)),
    }
    svc.watcher.current.update(
        {
            Section.RISK: _version(Section.RISK, risk_id, stored),
            Section.BINANCE: _version(Section.BINANCE, risk_id + 1, binance),
            Section.MODE: _version(Section.MODE, risk_id + 2, {"mode": "paper", "size_multiplier": 1.0}),
        }
    )
    with caplog.at_level(logging.WARNING):
        runtime = svc.runtime()
    assert runtime.risk.min_rr == 2.5  # the stored version is the one in force, not the static fallback
    assert not hasattr(runtime.risk, "decision_ttl_s")
    assert [r for r in caplog.records if raised(r, "config_invalid")] == []
    with sessions() as s:
        alerts = s.scalars(sa.select(AlertRow.kind).where(AlertRow.dedupe_key == f":risk:{risk_id}")).all()
    assert alerts == []

    # every other field is still enforced: the same version with a broken ceiling is clipped and alerts
    svc.watcher.current[Section.RISK] = _version(Section.RISK, risk_id + 3, {**stored, "leverage_max": 5})
    with caplog.at_level(logging.WARNING):
        clipped = svc.runtime()
    assert clipped.risk.leverage_max == LEVERAGE_MAX
    assert len([r for r in caplog.records if raised(r, "config_invalid")]) == 1
