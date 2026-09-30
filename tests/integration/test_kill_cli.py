"""`hdt-kill` (phase 09 section 8): the independent kill path works with Redis and Telegram down.

`kill_account` talks only to Postgres and the exchange adapter; it keeps the STOPs, removes entries and
take-profits, and reports every position with the STOP that covers it. The CLI refuses unsafe usage and
never contacts an exchange that IP-banned the namespace (review cycle 2, m3).
"""

from __future__ import annotations

import time
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, NoReturn

import pytest
from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

from fake_exchange import SOL, Harness, ledger_sessions, make_harness, open_position, working
from hdt.contracts.common import Account, Leg
from hdt.core.clock import utcnow
from hdt.core.config import static_config
from hdt.execution import actions, kill_cli
from hdt.execution.adapter_base import IpBannedError, RateLimitedError
from hdt.execution.kill_cli import (
    CliContext,
    KillOutcome,
    PositionStop,
    kill_account,
    kill_namespace,
    main,
    render,
)
from hdt.execution.kill_switch import KillReport
from hdt.execution.ledger import Ledger
from hdt.execution.venues import NamespaceUnavailableError
from hdt.lake.pit_query import PitQuery
from intent_builders import KEYRING
from risk_builders import risk_file

pytestmark = [pytest.mark.pg, pytest.mark.integration]


@pytest.fixture(autouse=True)
def _no_retry_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(actions, "CANCEL_CONFIRM_BACKOFF_S", 0.0)
    monkeypatch.setattr(actions, "UNKNOWN_VERIFY_DELAY_S", 0.0)


@pytest.fixture
async def h(pg_engine: Engine) -> Harness:
    harness = make_harness(ledger_sessions(pg_engine), Account.PAPER, KEYRING)
    await harness.start()
    await open_position(harness)  # SOLUSDT 9 long, STOP 98.00, TP1 104.00
    return harness


async def test_kill_keeps_the_stop_and_reports_it(h: Harness) -> None:
    (stop,) = working(h, Leg.SL)
    outcome = await kill_account(
        Account.PAPER, sessions=h.ns.sessions, adapter=h.exchange, risk=risk_file, reason="ops over ssh"
    )
    assert outcome.safe(flatten=False)
    assert outcome.stops == [PositionStop(SOL, Decimal("9"), stop.client_algo_id, Decimal("98.00"))]
    assert working(h, Leg.TP1) == []
    assert [a.client_algo_id for a in working(h, Leg.SL)] == [stop.client_algo_id]
    assert h.exchange.venue.positions[SOL].qty == Decimal("9")
    kill = h.ledger.kill_state()
    assert kill is not None
    assert (kill.state, kill.cause) == ("killed", "manual")
    lines = render(Account.PAPER, outcome, flatten=False)
    assert lines[0].startswith("paper: SAFE")
    assert any(SOL in line and stop.client_algo_id in line for line in lines)


async def test_kill_with_flatten_closes_every_position(h: Harness) -> None:
    outcome = await kill_account(
        Account.PAPER, sessions=h.ns.sessions, adapter=h.exchange, risk=risk_file, reason="ops", flatten=True
    )
    assert outcome.safe(flatten=True)
    assert outcome.stops == []
    assert h.exchange.venue.positions == {}
    assert working(h) == []


@pytest.mark.parametrize(
    "argv",
    [
        ["--account", "paper", "--reason", "x", "--flatten"],  # flatten needs its confirmation
        ["--account", "paper", "--reason", "x", "--confirm-flatten"],
        ["--account", "paper", "--reason", "  "],
        ["--account", "paper", "--reason", "x", "--wait-s", "-1"],
        ["--account", "moon", "--reason", "x"],
        ["--account", "paper"],
    ],
)
def test_unsafe_or_incomplete_usage_is_refused(argv: list[str]) -> None:
    assert main(argv) == 2


def test_help_exits_zero() -> None:
    assert main(["--help"]) == 0


def _context(sessions: sessionmaker[Session], tmp_path: Path) -> CliContext:
    return CliContext(
        static=static_config(),
        sessions=sessions,
        pit=PitQuery(tmp_path / "staging", tmp_path / "lake"),
        risk=risk_file(),
        reason="ops over ssh",
        flatten=False,
        wait_s=0.0,
    )


def _live_service_alive(sessions: sessionmaker[Session]) -> None:
    """A fresh reconcile run: the CLI takes the execution service as running `live`."""
    Ledger(sessions, Account.LIVE).insert_reconcile_run(
        clean=True, stop_invariant_ok=True, detail=None, mismatches=[]
    )


async def test_a_recorded_ip_ban_keeps_hdt_kill_off_the_exchange_even_while_the_service_runs(
    pg_engine: Engine, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sessions = ledger_sessions(pg_engine)
    _live_service_alive(sessions)
    Ledger(sessions, Account.LIVE).record_ban(utcnow() + timedelta(minutes=5))

    def contacted(*_args: Any, **_kwargs: Any) -> NoReturn:
        raise AssertionError("the banned exchange was contacted")

    monkeypatch.setattr(kill_cli, "exec_vault", contacted)
    monkeypatch.setattr(kill_cli, "open_adapter", contacted)
    with pytest.raises(NamespaceUnavailableError, match="IP ban recorded"):
        await kill_namespace(Account.LIVE, _context(sessions, tmp_path))
    kill = Ledger(sessions, Account.LIVE).kill_state()
    assert kill is not None
    assert (kill.state, kill.cause) == ("killed", "manual")  # the kill row stands, enforced after the ban


class _BannedExchange:
    """An adapter whose every read answers HTTP 418."""

    def __init__(self, until: float) -> None:
        self.until = until
        self.closed = False

    async def positions(self) -> NoReturn:
        raise IpBannedError("IP banned", code=-1003, banned_until=self.until)

    async def aclose(self) -> None:
        self.closed = True


async def test_a_ban_hit_while_hdt_kill_watches_the_service_is_recorded(
    pg_engine: Engine, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sessions = ledger_sessions(pg_engine)
    _live_service_alive(sessions)
    until = time.time() + 300
    exchange = _BannedExchange(until)
    monkeypatch.setattr(kill_cli, "exec_vault", lambda _sessions: (None, None))
    monkeypatch.setattr(kill_cli, "open_adapter", lambda *_a, **_k: exchange)
    with pytest.raises(IpBannedError):
        await kill_namespace(Account.LIVE, _context(sessions, tmp_path))
    assert exchange.closed
    recorded = Ledger(sessions, Account.LIVE).banned_until()
    assert recorded is not None
    assert abs(recorded.timestamp() - until) < 1


CLOCK_SLACK_S = 0.05  # asyncio may wake a timer up to one clock tick early (about 16 ms on Windows)


class _RateLimitedExchange:
    """An adapter whose first `limited` reads answer HTTP 429, then show a flat namespace."""

    def __init__(self, *, limited: int, retry_after_s: float) -> None:
        self.limited = limited
        self.retry_after_s = retry_after_s
        self.reads: list[float] = []  # monotonic time of every positions read

    async def positions(self) -> list[Any]:
        self.reads.append(time.monotonic())
        if len(self.reads) <= self.limited:
            raise RateLimitedError("HTTP 429: too many requests", retry_after_s=self.retry_after_s)
        return []

    async def open_algo_orders(self) -> list[Any]:
        return []

    async def open_orders(self) -> list[Any]:
        return []

    async def aclose(self) -> None:
        return None


def _watch(monkeypatch: pytest.MonkeyPatch, exchange: _RateLimitedExchange) -> list[float]:
    """Route hdt-kill to `exchange`; returns the monotonic times the CLI ran its own kill sequence."""
    ran: list[float] = []

    async def own_sequence(account: Account, **_kwargs: Any) -> KillOutcome:
        ran.append(time.monotonic())
        return KillOutcome(KillReport(account.value, "manual"), [])

    monkeypatch.setattr(kill_cli, "exec_vault", lambda _sessions: (None, None))
    monkeypatch.setattr(kill_cli, "open_adapter", lambda *_a, **_k: exchange)
    monkeypatch.setattr(kill_cli, "kill_account", own_sequence)
    monkeypatch.setattr(kill_cli, "POLL_S", 0.01)
    return ran


async def test_a_429_while_watching_the_service_keeps_the_watch_going(
    pg_engine: Engine, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Review cycle 3 m-e: a 429 used to end the watch as NOT SAFE, although the service was killing."""
    sessions = ledger_sessions(pg_engine)
    _live_service_alive(sessions)
    exchange = _RateLimitedExchange(limited=1, retry_after_s=0.2)
    ran = _watch(monkeypatch, exchange)
    ctx = replace(_context(sessions, tmp_path), wait_s=5.0)
    outcome = await kill_namespace(Account.LIVE, ctx)
    assert outcome.safe(flatten=False)
    assert outcome.by == "execution"  # the service reached SAFE: the CLI never ran its own sequence
    assert ran == []
    assert exchange.reads[1] - exchange.reads[0] >= 0.2 - CLOCK_SLACK_S  # no read inside the 429 window


async def test_a_429_lasting_past_the_wait_falls_back_after_the_window(
    pg_engine: Engine, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sessions = ledger_sessions(pg_engine)
    _live_service_alive(sessions)
    exchange = _RateLimitedExchange(limited=100, retry_after_s=0.3)
    ran = _watch(monkeypatch, exchange)
    outcome = await kill_namespace(Account.LIVE, _context(sessions, tmp_path))  # --wait-s 0
    assert len(ran) == 1  # the kill sequence runs from the CLI, instead of ending NOT SAFE
    assert ran[0] - exchange.reads[0] >= 0.3 - CLOCK_SLACK_S  # only after the 429 window (a 418 otherwise)
    assert outcome.by == "hdt-kill"
    assert "not readable" in (outcome.note or "")
