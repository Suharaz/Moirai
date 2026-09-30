"""The mode policy refuses a run mode change that would stop a namespace holding exposure (review H7).

`paper` always runs, so only leaving `testnet` or `live` can strand positions: the change is refused with a
clear error until that namespace is flat with no working order.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from typing import Any, TypeVar, cast

import pytest
from sqlalchemy import Engine, update
from sqlalchemy.orm import Session, sessionmaker

from fake_exchange import ledger_sessions
from hdt.configapi.auth import AuthContext
from hdt.configapi.context import AppState
from hdt.configapi.errors import ApiError
from hdt.configapi.routes.mode import ModePolicy
from hdt.configapi.sections import SaveContext
from hdt.contracts.common import Account
from hdt.core.clock import utcnow
from hdt.core.ids import canonical_sha256
from hdt.db.models.ledger import PositionRow
from hdt.settings.schemas import ModeSection, Section
from hdt.settings.versions import ConfigVersion

pytestmark = [pytest.mark.pg, pytest.mark.integration]

T = TypeVar("T")


@dataclass
class _State:
    """The part of `AppState` the mode policy uses: one transaction per `db` call."""

    sessions: sessionmaker[Session]

    async def db(self, fn: Callable[[Session], T]) -> T:
        with self.sessions.begin() as s:
            return fn(s)


@pytest.fixture
def sessions(pg_engine: Engine) -> sessionmaker[Session]:
    return ledger_sessions(pg_engine)


def _auth() -> AuthContext:
    now = utcnow()
    return AuthContext(
        user_id=1,
        username="operator",
        token_hash="t" * 64,
        csrf_token="c" * 32,
        expires_at=now + timedelta(hours=1),
        step_up_until=now + timedelta(minutes=5),
        ip=None,
    )


def _active(mode: Account) -> ConfigVersion:
    payload: dict[str, Any] = {"mode": mode.value, "size_multiplier": 0.25 if mode is Account.LIVE else 1.0}
    return ConfigVersion(
        id=1,
        section=Section.MODE,
        payload=payload,
        payload_sha256=canonical_sha256(payload),
        schema_version=1,
        author="operator",
        reason="test",
        created_at=utcnow(),
        parent_id=None,
    )


def _ctx(sessions: sessionmaker[Session], active: Account | None) -> SaveContext:
    return SaveContext(
        state=cast(AppState, _State(sessions)),
        auth=_auth(),
        section=Section.MODE,
        active=_active(active) if active is not None else None,
    )


def _open_position(sessions: sessionmaker[Session], account: Account) -> int:
    now = utcnow()
    with sessions.begin() as s:
        row = PositionRow(
            account=account.value,
            event_id="evt-1",
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


@pytest.mark.parametrize("left", [Account.TESTNET, Account.LIVE])
async def test_leaving_a_namespace_with_open_positions_is_refused(
    sessions: sessionmaker[Session], left: Account
) -> None:
    position_id = _open_position(sessions, left)
    with pytest.raises(ApiError) as err:
        await ModePolicy().check(_ctx(sessions, left), ModeSection(mode=Account.PAPER, size_multiplier=1.0))
    assert (err.value.status, err.value.code) == (409, "namespace_has_exposure")
    assert err.value.extra["accounts"] == [left.value]
    assert left.value in err.value.message
    assert "close them" in err.value.message

    with sessions.begin() as s:
        s.execute(
            update(PositionRow).where(PositionRow.position_id == position_id).values(closed_at=utcnow())
        )
    await ModePolicy().check(_ctx(sessions, left), ModeSection(mode=Account.PAPER, size_multiplier=1.0))


async def test_paper_exposure_never_blocks_a_mode_change(sessions: sessionmaker[Session]) -> None:
    _open_position(sessions, Account.PAPER)  # paper runs under every mode
    await ModePolicy().check(
        _ctx(sessions, Account.PAPER), ModeSection(mode=Account.TESTNET, size_multiplier=0.5)
    )
    await ModePolicy().check(
        _ctx(sessions, Account.TESTNET), ModeSection(mode=Account.PAPER, size_multiplier=1.0)
    )
    await ModePolicy().check(_ctx(sessions, None), ModeSection(mode=Account.TESTNET, size_multiplier=1.0))


async def test_a_change_that_keeps_the_exposed_namespace_is_allowed(sessions: sessionmaker[Session]) -> None:
    _open_position(sessions, Account.TESTNET)
    await ModePolicy().check(
        _ctx(sessions, Account.TESTNET), ModeSection(mode=Account.TESTNET, size_multiplier=0.5)
    )
