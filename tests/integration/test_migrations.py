"""`alembic upgrade head` on an empty database must produce exactly the model metadata."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext

from hdt.db.base import Base
from hdt.db.models import import_all_models

pytestmark = [pytest.mark.pg, pytest.mark.integration]

ROOT = Path(__file__).resolve().parents[2]


def test_upgrade_head_matches_models(fresh_database: Callable[[], AbstractContextManager[str]]) -> None:
    import_all_models()
    with fresh_database() as url:
        cfg = Config(str(ROOT / "alembic.ini"))
        cfg.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
        command.upgrade(cfg, "head")
        engine = sa.create_engine(url)
        try:
            with engine.connect() as conn:
                diff = compare_metadata(
                    MigrationContext.configure(conn, opts={"compare_type": True}), Base.metadata
                )
        finally:
            engine.dispose()
    assert diff == []


def test_every_migration_downgrades_and_upgrades_again(
    fresh_database: Callable[[], AbstractContextManager[str]],
) -> None:
    """Downgrade to base leaves no application table, view or function behind, and head applies again."""
    import_all_models()
    with fresh_database() as url:
        cfg = Config(str(ROOT / "alembic.ini"))
        cfg.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
        command.upgrade(cfg, "head")
        command.downgrade(cfg, "base")
        engine = sa.create_engine(url)
        try:
            with engine.connect() as conn:
                left = (
                    conn.execute(
                        sa.text(
                            "SELECT table_name FROM information_schema.tables "
                            "WHERE table_schema = 'public' AND table_name <> 'alembic_version' "
                            "UNION ALL SELECT p.proname FROM pg_proc p "
                            "JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'public'"
                        )
                    )
                    .scalars()
                    .all()
                )
        finally:
            engine.dispose()
        assert left == []
        command.upgrade(cfg, "head")


def test_migration_chain_is_linear() -> None:
    """Every phase migration sits in one chain: `upgrade head` never has to pick between branches."""
    from alembic.script import ScriptDirectory

    script = ScriptDirectory.from_config(Config(str(ROOT / "alembic.ini")))
    assert len(script.get_heads()) == 1
    assert all(len(rev.nextrev) <= 1 for rev in script.walk_revisions())


def test_no_decision_ttl_keeps_expired_outbox_rows_and_downgrades_them_back(
    fresh_database: Callable[[], AbstractContextManager[str]],
) -> None:
    """0012: an `expired` outbox row is kept as `published` without a stream id (never picked up again),
    `expires_at` is dropped, and the downgrade restores both exactly."""
    rows = (
        ("evt-expired", "expired", None),
        ("evt-published", "published", "1-0"),
        ("evt-pending", "pending", None),
    )
    with fresh_database() as url:
        cfg = Config(str(ROOT / "alembic.ini"))
        cfg.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
        command.upgrade(cfg, "0011_golive")
        engine = sa.create_engine(url)
        try:
            with engine.begin() as conn:
                for event_id, status, stream_id in rows:
                    conn.execute(
                        sa.text(
                            "INSERT INTO decision_outbox (event_id, payload, as_of, expires_at, status, "
                            "created_at, published_at, stream_id) VALUES (:e, '{}', "
                            "'2026-09-27T12:00:00Z', '2026-09-27T12:02:00Z', :s, '2026-09-27T12:00:00Z', "
                            "CASE WHEN CAST(:sid AS text) IS NULL THEN NULL "
                            "ELSE TIMESTAMPTZ '2026-09-27T12:01:00Z' END, "
                            "CAST(:sid AS text))"
                        ),
                        {"e": event_id, "s": status, "sid": stream_id},
                    )
            command.upgrade(cfg, "head")
            with engine.connect() as conn:
                after = dict(conn.execute(sa.text("SELECT event_id, status FROM decision_outbox")).all())
                columns = conn.execute(
                    sa.text(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_name = 'decision_outbox' AND column_name = 'expires_at'"
                    )
                ).all()
            assert after == {
                "evt-expired": "published",
                "evt-published": "published",
                "evt-pending": "pending",
            }
            assert columns == []
            with engine.begin() as conn, pytest.raises(sa.exc.IntegrityError):
                conn.execute(sa.text("UPDATE decision_outbox SET status = 'expired'"))
            command.downgrade(cfg, "0011_golive")
            with engine.connect() as conn:
                back = {
                    r.event_id: (r.status, r.ttl)
                    for r in conn.execute(
                        sa.text(
                            "SELECT event_id, status, extract(epoch FROM expires_at - as_of) AS ttl "
                            "FROM decision_outbox"
                        )
                    )
                }
            assert back == {
                "evt-expired": ("expired", 120),
                "evt-published": ("published", 120),
                "evt-pending": ("pending", 120),
            }
        finally:
            engine.dispose()
