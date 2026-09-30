"""`/api/lessons`: the console review of Reflection lessons through config-api (role `hdt_configapi`).

An approval or retirement is one lesson event under the configapi row-level policy, visible in the `lessons`
view at once (no scorer run), audited, and refused (409) when the transition is not allowed."""

from __future__ import annotations

import contextlib
import threading
from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from argon2 import PasswordHasher
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from hdt.configapi.context import ConfigApiSettings
from hdt.configapi.main import create_app
from hdt.configapi.routes import lessons as lessons_route
from hdt.configapi.totp import seal_totp_secret, totp_code
from hdt.contracts.common import AgentName
from hdt.core.config import require_env_value
from hdt.db.models.settings import AdminUserRow
from hdt.memory.lessons import AbResult, LessonLog, LessonTemplate
from hdt.memory.store import open_postgres_store

pytestmark = [pytest.mark.pg, pytest.mark.redis, pytest.mark.integration]
ROOT = Path(__file__).resolve().parents[2]
SECRET = b"test-session-secret-is-not-for-production"
TOTP_SEED = "JBSWY3DPEHPK3PXP"
TEMPLATE = LessonTemplate(
    title="Funding spikes fade",
    when_text="Funding turns sharply positive while open interest is flat",
    observation="The agent treated the funding spike as trend confirmation",
    adjustment="Weigh funding spikes lower unless open interest confirms",
)
IMPROVED = AbResult(n=60, logloss_with=0.61, logloss_without=0.66, ci_low=-0.08, ci_high=-0.02)


def _migrate(url: str) -> sa.Engine:
    cfg = Config(str(ROOT / "alembic.ini"))
    cfg.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
    command.upgrade(cfg, "head")
    admin = sa.create_engine(url)
    with Session(admin) as session, session.begin():
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
    return admin


def _app(url: str, pg_role_url: Callable[[str, str], str]) -> Any:
    return create_app(
        ConfigApiSettings(
            pg_dsn=pg_role_url(url, "hdt_configapi"),
            redis_url=require_env_value("HDT_REDIS_URL"),
            session_secret=SECRET,
        )
    )


def _login(client: TestClient) -> dict[str, str]:
    login = client.post(
        "/api/auth/login",
        json={
            "username": "operator",
            "password": "correct-password",
            "totp": totp_code(TOTP_SEED, datetime.now(UTC)),
        },
    )
    assert login.status_code == 200, login.text
    return {"X-CSRF-Token": login.json()["csrf_token"]}


def _ready_lesson(url: str, pg_role_url: Callable[[str, str], str]) -> str:
    """A crowding shadow lesson with an improving A/B (approvable, or rejectable by a retire)."""
    with open_postgres_store(pg_role_url(url, "hdt_scorer")) as memory:
        scorer = LessonLog(memory)
        ready = scorer.propose(AgentName.CROWDING, TEMPLATE, actor="reflection")
        scorer.record_ab(AgentName.CROWDING, ready.lesson_id, IMPROVED, actor="reflection")
    return ready.lesson_id


def _state(admin: sa.Engine, lesson_id: str) -> str:
    with admin.connect() as conn:
        return str(
            conn.execute(
                sa.text("SELECT state FROM lessons WHERE lesson_id = :id"), {"id": lesson_id}
            ).scalar_one()
        )


def test_concurrent_reviews_give_one_review_event(
    fresh_database: Callable[[], AbstractContextManager[str]],
    pg_role_url: Callable[[str, str], str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """L1: two reviews that would both read the shadow chain before either writes: exactly one wins, the
    other answers 409, and one review event plus one audit row are stored."""
    with fresh_database() as url:
        admin = _migrate(url)
        lesson_id = _ready_lesson(url, pg_role_url)
        barrier = threading.Barrier(2, timeout=3)
        read_chain = LessonLog._current

        def racing_current(self: LessonLog, agent: AgentName) -> Any:
            chain = read_chain(self, agent)
            with contextlib.suppress(threading.BrokenBarrierError):
                barrier.wait()
            return chain

        monkeypatch.setattr(LessonLog, "_current", racing_current)
        try:
            with TestClient(_app(url, pg_role_url), base_url="https://testserver") as client:
                csrf = _login(client)
                results: list[str] = []

                def review(note: str) -> None:
                    try:
                        response = client.post(
                            f"/api/lessons/{lesson_id}/approve", json={"note": note}, headers=csrf
                        )
                        results.append(str(response.status_code))
                    except Exception as exc:  # the pre-fix code path fails with a server error
                        results.append(type(exc).__name__)

                threads = [threading.Thread(target=review, args=(n,)) for n in ("first", "second")]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join(timeout=30)
            assert sorted(results) == ["200", "409"]
            with admin.connect() as conn:
                reviews = conn.execute(
                    sa.text("SELECT count(*) FROM store WHERE key = :key"), {"key": f"{lesson_id}:review"}
                ).scalar_one()
                audits = conn.execute(
                    sa.text("SELECT count(*) FROM audit_log WHERE section = 'lessons'")
                ).scalar_one()
            assert (reviews, audits) == (1, 1)
            assert _state(admin, lesson_id) == "active"
        finally:
            admin.dispose()


def test_racing_approve_and_retire_with_the_seen_state_give_one_200_one_409(
    fresh_database: Callable[[], AbstractContextManager[str]],
    pg_role_url: Callable[[str, str], str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """M-13: an approve and a retire of the same shadow lesson, both sent with `expected_state` shadow:
    the loser answers 409 instead of retiring (or rejecting) the winner's fresh result."""
    with fresh_database() as url:
        admin = _migrate(url)
        lesson_id = _ready_lesson(url, pg_role_url)
        barrier = threading.Barrier(2, timeout=3)
        read_chain = LessonLog._current

        def racing_current(self: LessonLog, agent: AgentName) -> Any:
            chain = read_chain(self, agent)
            with contextlib.suppress(threading.BrokenBarrierError):
                barrier.wait()
            return chain

        monkeypatch.setattr(LessonLog, "_current", racing_current)
        try:
            with TestClient(_app(url, pg_role_url), base_url="https://testserver") as client:
                csrf = _login(client)
                results: dict[str, int] = {}

                def review(action: str) -> None:
                    response = client.post(
                        f"/api/lessons/{lesson_id}/{action}",
                        json={"note": action, "expected_state": "shadow"},
                        headers=csrf,
                    )
                    results[action] = response.status_code

                threads = [threading.Thread(target=review, args=(a,)) for a in ("approve", "retire")]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join(timeout=30)
                assert sorted(results.values()) == [200, 409]
                with admin.connect() as conn:
                    audits = conn.execute(
                        sa.text("SELECT count(*) FROM audit_log WHERE section = 'lessons'")
                    ).scalar_one()
                assert audits == 1
                assert _state(admin, lesson_id) == ("active" if results["approve"] == 200 else "retired")
                # A stale expectation is refused, a matching one still goes through.
                seen = _state(admin, lesson_id)
                stale = client.post(
                    f"/api/lessons/{lesson_id}/retire",
                    json={"note": "late", "expected_state": "shadow"},
                    headers=csrf,
                )
                assert stale.status_code == 409
                if seen == "active":
                    ok = client.post(
                        f"/api/lessons/{lesson_id}/retire",
                        json={"note": "done", "expected_state": "active"},
                        headers=csrf,
                    )
                    assert ok.status_code == 200
                    assert _state(admin, lesson_id) == "retired"
        finally:
            admin.dispose()


def test_a_failed_audit_write_leaves_the_lesson_unchanged(
    fresh_database: Callable[[], AbstractContextManager[str]],
    pg_role_url: Callable[[str, str], str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """L2: the lesson event and its audit row commit together or not at all."""
    with fresh_database() as url:
        admin = _migrate(url)
        lesson_id = _ready_lesson(url, pg_role_url)

        def broken_audit(*_args: Any, **_kwargs: Any) -> None:
            raise RuntimeError("audit log unavailable")

        monkeypatch.setattr(lessons_route, "write_audit", broken_audit)
        try:
            with TestClient(
                _app(url, pg_role_url), base_url="https://testserver", raise_server_exceptions=False
            ) as client:
                csrf = _login(client)
                failed = client.post(f"/api/lessons/{lesson_id}/approve", json={"note": "ok"}, headers=csrf)
                assert failed.status_code == 500
                assert _state(admin, lesson_id) == "shadow"
                # A whitespace-only note is refused up front (422), never a server error.
                blank = client.post(f"/api/lessons/{lesson_id}/retire", json={"note": "   "}, headers=csrf)
                assert blank.status_code == 422
            assert _state(admin, lesson_id) == "shadow"
        finally:
            admin.dispose()


def test_console_approves_and_retires_lessons(
    fresh_database: Callable[[], AbstractContextManager[str]],
    pg_role_url: Callable[[str, str], str],
) -> None:
    with fresh_database() as url:
        admin = _migrate(url)
        with open_postgres_store(pg_role_url(url, "hdt_scorer")) as memory:
            scorer = LessonLog(memory)
            ready = scorer.propose(AgentName.CROWDING, TEMPLATE, actor="reflection")
            scorer.record_ab(AgentName.CROWDING, ready.lesson_id, IMPROVED, actor="reflection")
            running = scorer.propose(AgentName.NEWS, TEMPLATE, actor="reflection")

        app = _app(url, pg_role_url)
        try:
            with TestClient(app, base_url="https://testserver") as client:
                assert client.get("/api/lessons").status_code == 401
                csrf = _login(client)

                listed = {x["lesson_id"]: x for x in client.get("/api/lessons").json()["items"]}
                assert listed[ready.lesson_id]["state"] == "shadow"
                assert listed[ready.lesson_id]["ab_n"] == 60
                assert listed[running.lesson_id]["ab_n"] is None

                approve = f"/api/lessons/{ready.lesson_id}/approve"
                assert client.post(approve, json={"note": "looks right"}).status_code == 403
                early = client.post(
                    f"/api/lessons/{running.lesson_id}/approve", json={"note": "x"}, headers=csrf
                )
                assert early.status_code == 409, early.text
                assert early.json()["code"] == "invalid_transition"

                approved = client.post(approve, json={"note": "looks right"}, headers=csrf)
                assert approved.status_code == 200, approved.text
                assert approved.json()["state"] == "active"
                assert approved.json()["decided_by"] == "operator"
                with admin.connect() as conn:
                    state = conn.execute(
                        sa.text("SELECT state, decided_by, review_note FROM lessons WHERE lesson_id = :id"),
                        {"id": ready.lesson_id},
                    ).one()
                    assert tuple(state) == ("active", "operator", "looks right")
                assert client.post(approve, json={"note": "twice"}, headers=csrf).status_code == 409

                rejected = client.post(
                    f"/api/lessons/{running.lesson_id}/retire", json={"note": "not useful"}, headers=csrf
                )
                assert rejected.status_code == 200, rejected.text
                assert rejected.json()["state"] == "retired"
                retired = client.post(
                    f"/api/lessons/{ready.lesson_id}/retire", json={"note": "regime changed"}, headers=csrf
                )
                assert retired.status_code == 200, retired.text
                assert retired.json()["state"] == "retired"

                assert (
                    client.post(
                        "/api/lessons/crowding-9999/retire", json={"note": "x"}, headers=csrf
                    ).status_code
                    == 404
                )
                assert (
                    client.post(
                        "/api/lessons/nobody-0001/retire", json={"note": "x"}, headers=csrf
                    ).status_code
                    == 404
                )
                assert client.post(approve, json={"note": ""}, headers=csrf).status_code == 422

                with admin.connect() as conn:
                    states = dict(conn.execute(sa.text("SELECT lesson_id, state FROM lessons")).all())
                    actions = (
                        conn.execute(
                            sa.text("SELECT action FROM audit_log WHERE section = 'lessons' ORDER BY id")
                        )
                        .scalars()
                        .all()
                    )
                assert states == {ready.lesson_id: "retired", running.lesson_id: "retired"}
                assert actions == ["lesson.approved", "lesson.rejected", "lesson.retired"]
        finally:
            admin.dispose()
