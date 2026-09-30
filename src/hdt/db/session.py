"""Engines and sessions. Each service connects with its own Postgres role (DSN from bootstrap secrets)."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from hdt.core.config import require_env_value


def normalize_dsn(dsn: str) -> str:
    """Use the psycopg 3 driver for plain postgresql:// DSNs."""
    if dsn.startswith("postgresql://"):
        return "postgresql+psycopg://" + dsn[len("postgresql://") :]
    if dsn.startswith("postgres://"):
        return "postgresql+psycopg://" + dsn[len("postgres://") :]
    return dsn


def make_engine(dsn: str | None = None, *, pool_size: int = 5) -> Engine:
    """Engine for `dsn` (default: HDT_PG_DSN, the role of the running service)."""
    url = normalize_dsn(dsn or require_env_value("HDT_PG_DSN"))
    return create_engine(url, pool_size=pool_size, pool_pre_ping=True, future=True)


def make_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(engine, expire_on_commit=False, future=True)


@contextmanager
def transaction(factory: sessionmaker[Session]) -> Iterator[Session]:
    """One unit of work: commit on success, rollback on error."""
    with factory() as session, session.begin():
        yield session
