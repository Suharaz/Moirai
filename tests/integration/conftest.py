"""Integration fixtures.

- `quant_db`: one throwaway database per test module, migrated to head (so the service-role grants exist)
  and seeded with version 1 of every config section; engines for the admin, `hdt_council` (the quant core
  and scanner writer) and `hdt_risk` (a packet reader), plus the pinned config version ids.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy.orm import Session

from fixtures.quant_db import QuantDb
from hdt.core.config import static_config
from hdt.settings.store import seed_if_missing
from hdt.settings.versions import pin_current

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def quant_db(
    fresh_database: Callable[[], AbstractContextManager[str]], pg_role_url: Callable[[str, str], str]
) -> Iterator[QuantDb]:
    with fresh_database() as url:
        cfg = Config(str(ROOT / "alembic.ini"))
        cfg.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
        command.upgrade(cfg, "head")
        admin = sa.create_engine(url)
        council = sa.create_engine(pg_role_url(url, "hdt_council"))
        risk = sa.create_engine(pg_role_url(url, "hdt_risk"))
        try:
            with Session(admin) as session, session.begin():
                seed_if_missing(session, static_config())
            with Session(admin) as session:
                version_ids = pin_current(session).version_ids()
            yield QuantDb(admin, council, risk, version_ids)
        finally:
            for engine in (council, risk, admin):
                engine.dispose()
