"""Execution service namespaces (phase 09 section 5, steps 1-2 and the vault key flow).

- `paper` always runs; the pinned run mode adds its own namespace, a mode change forgets its refusal;
- a Binance namespace without a validated vault key, or on a Hedge Mode account, is refused with a critical
  `namespace_error` alert and never attached; the refusal does not touch `paper`;
- the owner key test (`OwnerWorker`, scope `exec`) decides the key: a refused key stays out of the vault, so
  the live namespace stays detached; a key validated later gives the refused namespace a new chance;
- an IP ban stops the namespace with a critical `ip_banned` alert, resolved when it attaches again;
- a stored risk or mode version outside the current hard ceilings runs clipped with one critical
  `config_invalid` alert instead of stopping the service (review cycle 2, m5).
"""

from __future__ import annotations

import functools
import json
import logging
import secrets
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy as sa
from nacl.public import PrivateKey
from redis.asyncio import Redis
from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

from alert_logs import raised as _raised
from binance_shapes import BinanceRoutes, error
from fake_exchange import ledger_sessions
from hdt.contracts.common import Account
from hdt.core.clock import utcnow
from hdt.core.config import static_config
from hdt.db.models.ledger import ExecAccountRow
from hdt.db.models.ops import AlertRow
from hdt.db.models.settings import SecretRow
from hdt.execution import venues
from hdt.execution.adapter_base import IpBannedError
from hdt.execution.binance_client import BinanceClient
from hdt.execution.key_check import ACCOUNT_PATH, check_binance_key, exec_secret_checks
from hdt.execution.ledger import Ledger
from hdt.execution.main import ExecutionService
from hdt.execution.namespaces import enabled_accounts
from hdt.execution.paper import PaperAdapter
from hdt.lake.pit_query import PitQuery
from hdt.settings.ceilings import LEVERAGE_MAX, RISK_PCT_MAX
from hdt.settings.schemas import BINANCE_FIELDS, BINANCE_RISK_FIELDS, RISK_FIELDS, Section
from hdt.settings.versions import ConfigVersion
from hdt.vault.loader import SecretLoader
from hdt.vault.owner import CheckOutcome, OwnerWorker, SecretTestRequest
from hdt.vault.scopes import BinanceKeySecret
from hdt.vault.sealed import fingerprint, generate_keypair, last4, private_key_from_b64, seal
from intent_builders import KEYRING

pytestmark = [pytest.mark.pg, pytest.mark.redis, pytest.mark.integration]

# Distinct from the adapter tests' key: decrypted values are registered for log redaction process-wide.
LIVE_KEY = "svcLiveApiKey0000000000000000000000000000000000000000000000000A1b2"
LIVE_SECRET = "svcLiveApiSecret000000000000000000000000000000000000000000000000Z9y8"
DUAL_PATH = "/fapi/v1/positionSide/dual"


@pytest.fixture
def owner_key() -> PrivateKey:
    key: PrivateKey = private_key_from_b64(generate_keypair()[1])
    return key


@pytest.fixture
def sessions(pg_engine: Engine) -> sessionmaker[Session]:
    factory: sessionmaker[Session] = ledger_sessions(pg_engine)
    with factory.begin() as s:
        s.execute(sa.delete(SecretRow).where(SecretRow.scope == "exec"))
    return factory


@pytest.fixture
async def service(
    sessions: sessionmaker[Session], redis_client: Redis, owner_key: PrivateKey, tmp_path: Path
) -> AsyncIterator[ExecutionService]:
    svc = ExecutionService(
        static=static_config(),
        sessions=sessions,
        redis=redis_client,
        keyring=KEYRING,
        pit=PitQuery(tmp_path / "staging", tmp_path / "lake"),
        vault=SecretLoader("exec", sessions, owner_key),
        owner_key=owner_key,
    )
    yield svc
    for account in list(svc.attached):
        await svc.detach(account)


def _add_pending_key(sessions: sessionmaker[Session], owner_key: PrivateKey, version: int) -> None:
    plaintext = json.dumps({"api_key": LIVE_KEY, "api_secret": LIVE_SECRET}).encode()
    with sessions.begin() as s:
        s.add(
            SecretRow(
                scope="exec",
                name="binance_live",
                version=version,
                sealed_blob=seal(owner_key.public_key, plaintext),
                last4=last4(LIVE_KEY),
                fingerprint=fingerprint(plaintext),
                status="pending",
                reason=None,
                check_details={},
                last_request_id=None,
                checked_at=None,
                created_by="owner",
                created_at=utcnow(),
            )
        )


async def _owner_test(
    sessions: sessionmaker[Session], redis: Redis, owner_key: PrivateKey, routes: BinanceRoutes, version: int
) -> str:
    """Run the owner worker's key test for `exec/binance_live` against the recorded exchange."""

    async def check(payload: object) -> CheckOutcome:
        assert isinstance(payload, BinanceKeySecret)
        return await check_binance_key(payload, base_url="https://fapi.test", transport=routes.transport())

    worker = OwnerWorker(
        scope="exec",
        redis=redis,
        session_factory=sessions,
        private_key=owner_key,
        checks={"binance_live": check},
        consumer="test",
    )
    result = await worker.process_secret_request(
        SecretTestRequest(
            request_id=f"req-{version}",
            scope="exec",
            name="binance_live",
            version=version,
            requested_by="owner",
            requested_at=utcnow(),
        )
    )
    assert result is not None
    return str(result.status)


def _binance_through(monkeypatch: pytest.MonkeyPatch, routes: BinanceRoutes) -> None:
    """Every Binance client the service opens talks to the recorded exchange."""
    factory: Callable[..., BinanceClient] = functools.partial(BinanceClient, transport=routes.transport())
    monkeypatch.setattr(venues, "BinanceClient", factory)


def _namespace_alerts(caplog: pytest.LogCaptureFixture) -> list[tuple[str, str]]:
    return [(r.getMessage(), r.levelname) for r in caplog.records if _raised(r, "namespace_error")]


@pytest.mark.parametrize(
    ("mode", "accounts"),
    [
        (Account.PAPER, (Account.PAPER,)),
        (Account.TESTNET, (Account.PAPER, Account.TESTNET)),
        (Account.LIVE, (Account.PAPER, Account.LIVE)),
    ],
)
def test_paper_is_always_enabled_and_the_pinned_mode_adds_its_namespace(
    mode: Account, accounts: tuple[Account, ...]
) -> None:
    assert enabled_accounts(mode) == accounts


async def test_live_mode_without_a_validated_key_runs_paper_and_refuses_live(
    service: ExecutionService, caplog: pytest.LogCaptureFixture
) -> None:
    service._mode = Account.LIVE
    await service.sync_namespaces()
    assert set(service.attached) == {Account.PAPER}
    assert isinstance(service.attached[Account.PAPER].rt.ns.adapter, PaperAdapter)
    assert service.attached[Account.PAPER].rt.ns.synced  # the paper venue is synced at attach
    assert "no usable binance_live key" in service.refused[Account.LIVE]
    assert _namespace_alerts(caplog) == [
        (f"alert: live namespace refused: {service.refused[Account.LIVE]}", "CRITICAL")
    ]

    await service.sync_namespaces()  # the refusal is not retried every config poll
    assert len(_namespace_alerts(caplog)) == 1

    service._mode = Account.PAPER  # the mode change detaches nothing of paper and forgets the refusal
    await service.sync_namespaces()
    assert set(service.attached) == {Account.PAPER}
    assert service.refused == {}


async def test_testnet_without_the_exec_vault_is_refused(
    sessions: sessionmaker[Session], redis_client: Redis, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    svc = ExecutionService(
        static=static_config(),
        sessions=sessions,
        redis=redis_client,
        keyring=KEYRING,
        pit=PitQuery(tmp_path / "staging", tmp_path / "lake"),
        vault=None,
        owner_key=None,
    )
    await svc.attach(Account.TESTNET)
    assert svc.attached == {}
    assert "vault scope exec is not available" in svc.refused[Account.TESTNET]
    assert len(_namespace_alerts(caplog)) == 1


async def test_a_failed_owner_key_test_keeps_the_live_namespace_detached(
    service: ExecutionService,
    sessions: sessionmaker[Session],
    redis_client: Redis,
    owner_key: PrivateKey,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    routes = BinanceRoutes()
    routes.queue("GET", ACCOUNT_PATH, error(401, -2015, "Invalid API-key, IP, or permissions for action."))
    _add_pending_key(sessions, owner_key, version=1)
    _binance_through(monkeypatch, routes)

    assert await _owner_test(sessions, redis_client, owner_key, routes, version=1) == "invalid"
    (tested,) = routes.calls("GET", ACCOUNT_PATH)
    assert tested.headers["x-mbx-apikey"] == LIVE_KEY  # the decrypted candidate was really tested
    await service.attach(Account.LIVE)
    assert service.attached == {}
    assert "no usable binance_live key" in service.refused[Account.LIVE]
    assert routes.calls("GET", DUAL_PATH) == []  # no request was signed with the refused key

    await service.rotate_keys()  # still no active key: the refusal stands
    assert Account.LIVE in service.refused
    routes.queue(
        "GET", ACCOUNT_PATH, {"totalWalletBalance": "100", "availableBalance": "100", "positions": []}
    )
    routes.always("GET", DUAL_PATH, {"dualSidePosition": False})
    _add_pending_key(sessions, owner_key, version=2)  # the owner saves a working key
    assert await _owner_test(sessions, redis_client, owner_key, routes, version=2) == "active"
    await service.rotate_keys()
    assert Account.LIVE not in service.refused  # attached by the next sync_namespaces, not left refused


async def test_a_validated_key_on_a_hedge_mode_account_is_refused_then_a_new_key_gets_a_new_chance(
    service: ExecutionService,
    sessions: sessionmaker[Session],
    redis_client: Redis,
    owner_key: PrivateKey,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    routes = BinanceRoutes()
    routes.always(
        "GET", ACCOUNT_PATH, {"totalWalletBalance": "100", "availableBalance": "100", "positions": []}
    )
    routes.always("GET", DUAL_PATH, {"dualSidePosition": True})
    _binance_through(monkeypatch, routes)
    _add_pending_key(sessions, owner_key, version=1)
    assert await _owner_test(sessions, redis_client, owner_key, routes, version=1) == "active"

    await service.attach(Account.LIVE)
    assert service.attached == {}
    assert "Hedge Mode" in service.refused[Account.LIVE]
    (checked,) = routes.calls("GET", DUAL_PATH)
    assert checked.headers["x-mbx-apikey"] == LIVE_KEY  # the vault's active key
    assert [level for _m, level in _namespace_alerts(caplog)] == ["CRITICAL"]
    assert not [c for c in routes.sent if c.method != "GET"]  # nothing was placed or changed

    await service.rotate_keys()  # the same key: the refusal stands
    assert Account.LIVE in service.refused

    _add_pending_key(sessions, owner_key, version=2)  # the owner fixes the account and saves a key again
    assert await _owner_test(sessions, redis_client, owner_key, routes, version=2) == "active"
    await service.rotate_keys()
    assert Account.LIVE not in service.refused
    service._mode = Account.LIVE
    service._retry_at.clear()
    await service.sync_namespaces()  # retried: still Hedge Mode, refused again with one more alert
    assert "Hedge Mode" in service.refused[Account.LIVE]
    assert len(routes.calls("GET", DUAL_PATH)) == 2
    assert len(_namespace_alerts(caplog)) == 2
    assert set(service.attached) == {Account.PAPER}


def _ban_alerts(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [r.getMessage() for r in caplog.records if _raised(r, "ip_banned")]


async def test_an_ip_ban_stops_the_namespace_alerts_and_sends_nothing_until_it_ends(
    service: ExecutionService,
    sessions: sessionmaker[Session],
    redis_client: Redis,
    owner_key: PrivateKey,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Phase 09: on HTTP 418 the namespace stops and alerts; no request may extend the ban; paper runs on."""
    routes = BinanceRoutes()
    routes.always(
        "GET", ACCOUNT_PATH, {"totalWalletBalance": "100", "availableBalance": "100", "positions": []}
    )
    _binance_through(monkeypatch, routes)
    _add_pending_key(sessions, owner_key, version=1)
    assert await _owner_test(sessions, redis_client, owner_key, routes, version=1) == "active"
    service._mode = Account.LIVE

    # Banned while attaching: not attached, one critical alert, retried only after Retry-After.
    routes.queue(
        "GET", DUAL_PATH, error(418, -1003, "Way too many requests; IP banned.", {"Retry-After": "300"})
    )
    before = utcnow()
    await service.sync_namespaces()
    assert set(service.attached) == {Account.PAPER}
    assert Account.LIVE not in service.refused  # a ban is temporary, not a refusal
    assert len(_ban_alerts(caplog)) == 1
    retry_at = service._retry_at[Account.LIVE]
    assert 290 <= (retry_at - before).total_seconds() <= 310
    recorded = Ledger(sessions, Account.LIVE).banned_until()  # honoured by hdt-kill and the key test
    assert recorded is not None
    assert abs((recorded - retry_at).total_seconds()) < 1
    sent = len(routes.sent)
    await service.sync_namespaces()  # inside the ban: no new request, no new alert
    assert len(routes.sent) == sent
    assert len(_ban_alerts(caplog)) == 1
    key_test = exec_secret_checks(sessions)["binance_live"]  # the owner key test honours the recorded ban
    outcome = await key_test(BinanceKeySecret(api_key=LIVE_KEY, api_secret=LIVE_SECRET))
    assert (outcome.status, "not tested" in outcome.reason) == ("error", True)
    assert len(routes.sent) == sent

    # The ban is over: the namespace attaches and the ban alert resolves (the next ban pages again).
    assert _open_ban_alert(sessions)
    routes.always("GET", DUAL_PATH, {"dualSidePosition": False})
    service._retry_at.clear()
    with sessions.begin() as s:
        s.execute(sa.update(ExecAccountRow).values(banned_until=None))
    await service.sync_namespaces()
    assert Account.LIVE in service.attached
    assert not _open_ban_alert(sessions)

    # Banned while running: the next config poll stops the namespace and alerts; paper keeps running.
    client = service.attached[Account.LIVE].rt.ns.adapter.client  # type: ignore[attr-defined]
    routes.queue("GET", ACCOUNT_PATH, error(418, -1003, "IP banned.", {"Retry-After": "120"}))
    with pytest.raises(IpBannedError) as banned:
        await client.signed("GET", ACCOUNT_PATH)
    assert banned.value.banned_until is not None
    await service.sync_namespaces()
    assert set(service.attached) == {Account.PAPER}
    assert len(_ban_alerts(caplog)) == 2
    assert _open_ban_alert(sessions)
    assert service._retry_at[Account.LIVE] >= utcnow()


def _open_ban_alert(sessions: sessionmaker[Session]) -> bool:
    with sessions() as s:
        found = s.scalar(
            sa.select(AlertRow.alert_id).where(
                AlertRow.kind == "ip_banned",
                AlertRow.dedupe_key == Account.LIVE.value,
                AlertRow.resolved_at.is_(None),
            )
        )
    return found is not None


def _version(section: Section, version_id: int, payload: dict[str, Any]) -> ConfigVersion:
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
    service: ExecutionService, sessions: sessionmaker[Session], caplog: pytest.LogCaptureFixture
) -> None:
    """Review cycle 2 m5: re-validating such a version raised `CeilingViolationError` on every config load,
    which stopped the service at start and on every config poll."""
    static = service.static
    risk_id = 900_000 + secrets.randbelow(90_000)  # a fresh episode: the alert outbox is shared by the run
    service.watcher.current.update(
        {
            Section.RISK: _version(
                Section.RISK,
                risk_id,
                {**static.risk.model_dump(include=set(RISK_FIELDS)), "leverage_max": 5, "risk_pct": 0.01},
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
    service._load_config()
    service._load_config()  # the next config poll: still clipped, no second alert
    assert (service.risk().leverage_max, service.risk().risk_pct) == (LEVERAGE_MAX, RISK_PCT_MAX)
    assert service.mode() is Account.LIVE
    alerts = [(r.levelname, r.alert_kind) for r in caplog.records if _raised(r, "config_invalid")]
    assert alerts == [("CRITICAL", "config_invalid")]
    with sessions() as s:
        row = s.scalar(
            sa.select(AlertRow).where(
                AlertRow.kind == "config_invalid", AlertRow.dedupe_key == f":risk:{risk_id}"
            )
        )
    assert row is not None
    assert (row.severity, row.resolved_at) == ("critical", None)
