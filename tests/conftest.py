"""Shared fixtures.

- `pg_engine`: a fresh database `hdt_test_<hex>` on the dev Postgres with every model table created,
  dropped at session end (parallel test runs never collide).
- `fresh_database`: factory for extra empty throwaway databases (e.g. migration tests).
- `redis_client`: async client on a dev Redis DB index 1-15 (admin ACL user) that this test holds an
  exclusive lease on (`SET NX EX` in DB 0), flushed before and after; concurrent sessions never share one.
- `pg_role_url`: URL of a database connected as a service role, for grant tests.
- `main_lake` / `main_features`: the phase 03 synthetic lake (`fixtures/synthetic_lake.py`), written once
  per session, and a feature engine over it whose clock treats every fixture hour as closed.

Start the dev stack first: `docker compose -f docker-compose.dev.yml up -d`.
"""

from __future__ import annotations

import asyncio
import secrets
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import AbstractContextManager, contextmanager

import pytest
import sqlalchemy as sa
from redis.asyncio import Redis
from sqlalchemy import Engine

from fixtures.synthetic_lake import FAR_FUTURE, SyntheticLake, build_main
from hdt.core.config import read_env_value, scanner_config, static_config
from hdt.db.base import Base
from hdt.db.models import import_all_models
from hdt.db.session import normalize_dsn
from hdt.features.engine import FeatureEngine
from hdt.features.lake_io import LakeView


def _require(name: str) -> str:
    value = read_env_value(name)
    if value is None:
        raise RuntimeError(
            f"{name} is not set: copy .env.example to .env and start "
            "`docker compose -f docker-compose.dev.yml up -d`"
        )
    return value


@pytest.fixture(scope="session")
def pg_admin_dsn() -> str:
    return normalize_dsn(_require("HDT_TEST_PG_ADMIN_DSN"))


def _database_url(admin_dsn: str, name: str) -> str:
    return sa.engine.make_url(admin_dsn).set(database=name).render_as_string(hide_password=False)


@contextmanager
def _temporary_database(admin_dsn: str) -> Iterator[str]:
    name = f"hdt_test_{secrets.token_hex(6)}"
    admin = sa.create_engine(admin_dsn, isolation_level="AUTOCOMMIT")
    try:
        with admin.connect() as conn:
            conn.execute(sa.text(f'CREATE DATABASE "{name}" OWNER hdt_migrator'))
        yield _database_url(admin_dsn, name)
    finally:
        with admin.connect() as conn:
            conn.execute(sa.text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        admin.dispose()


@pytest.fixture(scope="session")
def fresh_database(pg_admin_dsn: str) -> Callable[[], AbstractContextManager[str]]:
    """Factory: `with fresh_database() as url:` yields the URL of an empty throwaway database."""

    def factory() -> AbstractContextManager[str]:
        return _temporary_database(pg_admin_dsn)

    return factory


@pytest.fixture(scope="session")
def pg_engine(pg_admin_dsn: str) -> Iterator[Engine]:
    import_all_models()
    with _temporary_database(pg_admin_dsn) as url:
        engine = sa.create_engine(url, future=True)
        Base.metadata.create_all(engine)
        try:
            yield engine
        finally:
            engine.dispose()


@pytest.fixture(scope="session")
def pg_role_url() -> Callable[[str, str], str]:
    """`pg_role_url(database_url, "hdt_risk")` -> the same database, logged in as that service role."""

    def factory(database_url: str, role: str) -> str:
        password = _require("HDT_PG_PASSWORD_" + role.removeprefix("hdt_").upper())
        url = sa.engine.make_url(database_url).set(username=role, password=password)
        return url.render_as_string(hide_password=False)

    return factory


_LEASE_TTL_S = 600


async def _lease_db_index(base: str) -> tuple[int, str]:
    lock_client: Redis = Redis.from_url(f"{base}/0")
    token = secrets.token_hex(8)
    try:
        for _ in range(200):
            order = list(range(1, 16))
            secrets.SystemRandom().shuffle(order)
            for index in order:
                if await lock_client.set(f"hdt:test:dblease:{index}", token, nx=True, ex=_LEASE_TTL_S):
                    return index, token
            await asyncio.sleep(0.1)
    finally:
        await lock_client.aclose()
    raise RuntimeError("no free Redis test DB index (1-15) after 20 s")


async def _release_db_index(base: str, index: int, token: str) -> None:
    lock_client: Redis = Redis.from_url(f"{base}/0")
    try:
        key = f"hdt:test:dblease:{index}"
        script = (
            "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end return 0"
        )
        await lock_client.eval(script, 1, key, token)  # type: ignore[misc]
    finally:
        await lock_client.aclose()


@pytest.fixture
async def redis_client() -> AsyncIterator[Redis]:
    base = _require("HDT_TEST_REDIS_URL").rstrip("/")
    index, token = await _lease_db_index(base)
    client: Redis = Redis.from_url(f"{base}/{index}")
    try:
        await client.flushdb()
        yield client
    finally:
        await client.flushdb()
        await client.aclose()
        await _release_db_index(base, index, token)


@pytest.fixture(scope="session")
def main_lake(tmp_path_factory: pytest.TempPathFactory) -> SyntheticLake:
    return build_main(tmp_path_factory.mktemp("main_lake"))


@pytest.fixture(scope="session")
def main_features(main_lake: SyntheticLake) -> FeatureEngine:
    view = LakeView(main_lake.pit, clock=lambda: FAR_FUTURE)
    return FeatureEngine(view, static_config(), scanner_config(), max_snapshots=8)
