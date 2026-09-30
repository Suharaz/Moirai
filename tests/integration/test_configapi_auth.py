"""Direct API calls cannot bypass authentication, CSRF, risk ceilings or live prerequisites."""

from __future__ import annotations

import base64
from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import UTC, datetime
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from argon2 import PasswordHasher
from fastapi.testclient import TestClient
from nacl.public import PrivateKey
from redis.asyncio import Redis
from sqlalchemy.orm import Session

from hdt.configapi.context import ConfigApiSettings
from hdt.configapi.main import create_app
from hdt.configapi.totp import seal_totp_secret, totp_code
from hdt.core.config import require_env_value
from hdt.db.models.settings import AdminUserRow
from hdt.vault.sealed import fingerprint, seal

pytestmark = [pytest.mark.pg, pytest.mark.redis, pytest.mark.integration]
ROOT = Path(__file__).resolve().parents[2]
SECRET = b"test-session-secret-is-not-for-production"
TOTP_SEED = "JBSWY3DPEHPK3PXP"


def test_authenticated_api_enforces_policy(
    fresh_database: Callable[[], AbstractContextManager[str]],
    pg_role_url: Callable[[str, str], str],
) -> None:
    with fresh_database() as url:
        cfg = Config(str(ROOT / "alembic.ini"))
        cfg.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
        command.upgrade(cfg, "head")
        engine = sa.create_engine(url)
        with Session(engine) as session, session.begin():
            session.add(
                AdminUserRow(
                    username="operator",
                    password_hash=PasswordHasher().hash("correct-password"),
                    totp_secret_sealed=seal_totp_secret(TOTP_SEED, SECRET),
                    failed_attempts=0,
                    disabled=False,
                    created_at=datetime.now(UTC),
                )
            )
        engine.dispose()
        app = create_app(
            ConfigApiSettings(
                pg_dsn=pg_role_url(url, "hdt_configapi"),
                redis_url=require_env_value("HDT_REDIS_URL"),
                session_secret=SECRET,
            )
        )
        with TestClient(app, base_url="https://testserver") as client:
            assert client.get("/api/config/risk").status_code == 401
            login = client.post(
                "/api/auth/login",
                json={
                    "username": "operator",
                    "password": "correct-password",
                    "totp": totp_code(TOTP_SEED, datetime.now(UTC)),
                },
            )
            assert login.status_code == 200, login.text
            assert "HttpOnly" in login.headers["set-cookie"]
            assert "Secure" in login.headers["set-cookie"]
            assert "SameSite=strict" in login.headers["set-cookie"]
            csrf = login.json()["csrf_token"]
            current = client.get("/api/config/risk").json()["active"]
            assert current is not None
            payload = current["payload"]
            payload["leverage_max"] = 4
            path = "/api/config/risk"
            body = {"payload": payload, "reason": "attempt to loosen ceiling", "parent_id": current["id"]}
            assert client.post(path, json=body).status_code == 403
            rejected = client.post(path, json=body, headers={"X-CSRF-Token": csrf})
            assert rejected.status_code == 422, rejected.text
            mode = client.get("/api/config/mode").json()["active"]
            live = client.post(
                "/api/config/mode",
                json={
                    "payload": {"mode": "live", "size_multiplier": 0.25},
                    "reason": "gate test",
                    "parent_id": mode["id"],
                },
                headers={"X-CSRF-Token": csrf},
            )
            assert live.status_code == 409, live.text
            assert live.json()["code"] == "live_gate_closed"
            assert {"g4_gate_not_passed", "live_key_not_valid", "checklist_incomplete"} <= set(
                live.json()["missing"]
            )


def test_sealed_secret_write_succeeds_under_the_config_api_role(
    fresh_database: Callable[[], AbstractContextManager[str]],
    pg_role_url: Callable[[str, str], str],
    redis_client: Redis,
) -> None:
    """The column-level INSERT grant of `hdt_configapi` on `secrets` omits `reason`, `last_request_id` and
    `checked_at`: a write naming them (even as NULL) is refused by Postgres and the API answered 500."""
    plaintext = b"cmc-test-key-0123456789abcdef"
    redis_db = redis_client.connection_pool.connection_kwargs["db"]
    with fresh_database() as url:
        cfg = Config(str(ROOT / "alembic.ini"))
        cfg.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
        command.upgrade(cfg, "head")
        engine = sa.create_engine(url)
        with Session(engine) as session, session.begin():
            session.add(
                AdminUserRow(
                    username="operator",
                    password_hash=PasswordHasher().hash("correct-password"),
                    totp_secret_sealed=seal_totp_secret(TOTP_SEED, SECRET),
                    failed_attempts=0,
                    disabled=False,
                    created_at=datetime.now(UTC),
                )
            )
        app = create_app(
            ConfigApiSettings(
                pg_dsn=pg_role_url(url, "hdt_configapi"),
                # the leased test DB, so the self-test request never reaches a running dev recorder
                redis_url=f"{require_env_value('HDT_TEST_REDIS_URL').rstrip('/')}/{redis_db}",
                session_secret=SECRET,
            )
        )
        try:
            with TestClient(app, base_url="https://testserver") as client:
                login = client.post(
                    "/api/auth/login",
                    json={
                        "username": "operator",
                        "password": "correct-password",
                        "totp": totp_code(TOTP_SEED, datetime.now(UTC)),
                    },
                )
                assert login.status_code == 200, login.text
                blob = seal(PrivateKey.generate().public_key, plaintext)
                saved = client.post(
                    "/api/secrets/data/cmc_api_key",
                    json={
                        "sealed_blob": base64.b64encode(blob).decode("ascii"),
                        "last4": "cdef",
                        "fingerprint": fingerprint(plaintext),
                    },
                    headers={"X-CSRF-Token": login.json()["csrf_token"]},
                )
                assert saved.status_code == 201, saved.text
                assert saved.json()["version"] == 1
            with engine.connect() as conn:
                row = conn.execute(
                    sa.text(
                        "SELECT status, reason, checked_at, last_request_id, created_by FROM secrets "
                        "WHERE scope = 'data' AND name = 'cmc_api_key'"
                    )
                ).one()
            assert (row.status, row.reason, row.checked_at, row.created_by) == (
                "pending",
                None,
                None,
                "operator",
            )
        finally:
            engine.dispose()
