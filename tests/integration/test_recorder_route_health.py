"""`route_health` through the real recorder role: a CMC route refused by the plan (1006) is `disabled`, never
a failure, never late and never dead; the console (as `hdt_console_ro`) shows it as not on the current plan
and leaves it out of the late/failing count; the first success after a plan upgrade returns it to `ok`."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy.orm import Session, sessionmaker

from hdt.console.readmodels import ROUTE_NOT_ON_PLAN, Available, ReadModels, route_counts
from hdt.db.models.ingest import NOT_ON_PLAN_ERROR, RouteHealthRow
from hdt.ingest.recorder_db import RouteStatus, mark_late_routes, upsert_route_health
from hdt.ops.alerts import dead_routes

pytestmark = [pytest.mark.pg, pytest.mark.integration]

ROOT = Path(__file__).resolve().parents[2]
T0 = datetime(2026, 9, 29, 0, 0, tzinfo=UTC)
HOUR = timedelta(hours=1)
REFUSED = f"{NOT_ON_PLAN_ERROR}: ohlcv_historical: Your API Key subscription plan doesn't support this."


def _route(key: str) -> RouteStatus:
    return RouteStatus(key, int(key) * 10, f"#{key} /v1/test", "cmc", "1 h", "technical", 2)


@pytest.fixture(scope="module")
def sessions(
    fresh_database: Callable[[], AbstractContextManager[str]], pg_role_url: Callable[[str, str], str]
) -> Iterator[tuple[sessionmaker[Session], sessionmaker[Session], ReadModels]]:
    """(recorder, telegram, console): the writer, the role that evaluates `route_dead`, the console."""
    with fresh_database() as url:
        cfg = Config(str(ROOT / "alembic.ini"))
        cfg.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
        command.upgrade(cfg, "head")
        recorder, telegram, console = (
            sa.create_engine(pg_role_url(url, role))
            for role in ("hdt_recorder", "hdt_telegram", "hdt_console_ro")
        )
        try:
            yield sessionmaker(recorder, expire_on_commit=False), sessionmaker(telegram), ReadModels(console)
        finally:
            for engine in (recorder, telegram, console):
                engine.dispose()


def _write(
    recorder: sessionmaker[Session], key: str, at: datetime, status: str, error: str | None = None
) -> None:
    with recorder.begin() as s:
        upsert_route_health(
            s, _route(key), attempted_at=at, success=status == "ok", status=status, error=error
        )


def _row(recorder: sessionmaker[Session], key: str) -> RouteHealthRow:
    with recorder() as s:
        row = s.get(RouteHealthRow, key)
        assert row is not None
        return row


def test_plan_refusal_is_disabled_not_failing_late_or_dead_until_a_success(
    sessions: tuple[sessionmaker[Session], sessionmaker[Session], ReadModels],
) -> None:
    recorder, telegram, console = sessions
    # "7": failing twice (before this change a 1006 counted as a failure), then refused by the plan.
    # "8": succeeded once, then refused (plan downgrade). "9": a genuine failure with the same history as "8".
    _write(recorder, "7", T0, "failing", "boom")
    _write(recorder, "7", T0 + HOUR, "failing", "boom")
    _write(recorder, "8", T0, "ok")
    _write(recorder, "9", T0, "ok")
    for n in range(2, 5):
        _write(recorder, "7", T0 + n * HOUR, "disabled", REFUSED)
        _write(recorder, "8", T0 + n * HOUR, "disabled", REFUSED)
        _write(recorder, "9", T0 + n * HOUR, "failing", "HTTP 500")

    refused, downgraded, failing = _row(recorder, "7"), _row(recorder, "8"), _row(recorder, "9")
    assert (refused.status, refused.consecutive_failures, refused.last_error) == ("disabled", 0, REFUSED)
    assert refused.last_success_at is None
    assert (downgraded.status, downgraded.consecutive_failures) == ("disabled", 0)
    assert downgraded.last_success_at == T0
    assert (failing.status, failing.consecutive_failures) == ("failing", 3)

    with recorder.begin() as s:
        mark_late_routes(s, T0 + 10 * HOUR, {"7": 3600, "8": 3600, "9": 3600})
    assert _row(recorder, "7").cycles_late == 0
    assert _row(recorder, "8").cycles_late == 0
    assert _row(recorder, "9").cycles_late == 9
    with telegram() as s:
        assert set(dead_routes(s)) == {"route:9"}  # the genuine failure only
    shown = console.route_health()
    assert isinstance(shown, Available)
    assert [(r.route_key, r.status) for r in shown.value] == [
        ("7", ROUTE_NOT_ON_PLAN),
        ("8", ROUTE_NOT_ON_PLAN),
        ("9", "failing"),
    ]
    counts = route_counts(shown.value)
    assert (counts.ok, counts.late_or_failing, counts.not_on_plan) == (0, 1, ("#7 /v1/test", "#8 /v1/test"))

    _write(recorder, "7", T0 + 11 * HOUR, "ok")  # the plan was upgraded: the next scheduled call succeeds
    upgraded = _row(recorder, "7")
    assert (upgraded.status, upgraded.consecutive_failures, upgraded.last_error) == ("ok", 0, None)
    assert upgraded.last_success_at == T0 + 11 * HOUR
