"""Alerts outbox (migration 0006_ops) exercised through the real service roles and grants."""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager
from datetime import UTC, datetime, timedelta
from functools import partial
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from prometheus_client import REGISTRY
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import Session, sessionmaker

from hdt.core.alerts import alert, resolve
from hdt.core.clock import ManualClock, use_clock
from hdt.db.models.ingest import CmcCreditUsageRow, CmcKeyInfoRow, LakeStatsRow, RouteHealthRow
from hdt.db.models.ops import AlertRow
from hdt.db.session import transaction
from hdt.ops.alerts import (
    ALERT_CATALOG,
    NOTIFY_BACKOFF_BASE,
    NOTIFY_BACKOFF_MAX,
    NOTIFY_MAX_AGE,
    AlertInputError,
    AlertKind,
    AlertMonitor,
    Severity,
    due_notifications,
    mark_notified,
    mark_notify_failed,
    raise_alert,
    raise_test_alerts,
    resolve_alert,
)
from hdt.ops.telegram_bot import AlertDispatcher
from hdt.ops.telegram_client import TelegramApiError

pytestmark = [pytest.mark.pg, pytest.mark.integration]

ROOT = Path(__file__).resolve().parents[2]
NOW = datetime(2026, 9, 27, 12, 0, tzinfo=UTC)


@pytest.fixture(scope="module")
def migrated_url(fresh_database: Callable[[], AbstractContextManager[str]]) -> Iterator[str]:
    with fresh_database() as url:
        cfg = Config(str(ROOT / "alembic.ini"))
        cfg.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
        command.upgrade(cfg, "head")
        yield url


@pytest.fixture
def role_factory(
    migrated_url: str, pg_role_url: Callable[[str, str], str]
) -> Iterator[Callable[[str], sessionmaker[Session]]]:
    engines: list[sa.Engine] = []

    def factory(role: str) -> sessionmaker[Session]:
        engine = sa.create_engine(pg_role_url(migrated_url, role))
        engines.append(engine)
        return sessionmaker(engine, expire_on_commit=False)

    yield factory
    admin = sa.create_engine(migrated_url)
    with admin.begin() as conn:
        for table in (
            "alert_deliveries",
            "alerts",
            "route_health",
            "cmc_key_info",
            "cmc_credit_usage",
            "lake_stats",
        ):
            conn.execute(sa.text(f"DELETE FROM {table}"))
    admin.dispose()
    for engine in engines:
        engine.dispose()


def test_open_alert_is_deduplicated_until_resolved(
    role_factory: Callable[[str], sessionmaker[Session]],
) -> None:
    risk = role_factory("hdt_execution")
    with use_clock(ManualClock(NOW)):
        with transaction(risk) as s:
            first = raise_alert(
                s,
                kind="kill_switch",
                severity="critical",
                title="Kill",
                dedupe_key="paper",
                service="execution",
            )
        with transaction(risk) as s:
            again = raise_alert(
                s,
                kind="kill_switch",
                severity="critical",
                title="Kill",
                dedupe_key="paper",
                service="execution",
            )
            other_key = raise_alert(
                s,
                kind="kill_switch",
                severity="critical",
                title="Kill",
                dedupe_key="live",
                service="execution",
            )
        with transaction(risk) as s:
            assert resolve_alert(s, kind="kill_switch", dedupe_key="paper") == 1
            assert resolve_alert(s, kind="kill_switch", dedupe_key="paper") == 0
        with transaction(risk) as s:
            reopened = raise_alert(
                s,
                kind="kill_switch",
                severity="critical",
                title="Kill",
                dedupe_key="paper",
                service="execution",
            )
    assert first is not None
    assert again is None
    assert other_key is not None
    assert reopened is not None
    assert reopened != first


def test_alert_text_is_redacted_and_bounded(role_factory: Callable[[str], sessionmaker[Session]]) -> None:
    factory = role_factory("hdt_risk")
    with transaction(factory) as s:
        alert_id = raise_alert(
            s,
            kind="integrity_error",
            severity="critical",
            title="Bad intent",
            detail="token sk-or-v1-" + "a" * 64 + " leaked " + "x" * 5000,
            service="risk",
        )
    with factory() as s:
        row = s.get(AlertRow, alert_id)
    assert row is not None
    assert row.detail is not None
    assert "sk-or-v1-" + "a" * 64 not in row.detail
    assert len(row.detail) <= 2000


def test_unknown_kind_or_account_is_refused(role_factory: Callable[[str], sessionmaker[Session]]) -> None:
    factory = role_factory("hdt_risk")
    with pytest.raises(AlertInputError), transaction(factory) as s:
        raise_alert(s, kind="nope", severity="warning", title="x", service="risk")  # type: ignore[arg-type]
    with pytest.raises(AlertInputError), transaction(factory) as s:
        raise_alert(s, kind="kill_switch", severity="warning", title="x", service="risk", account="demo")


def test_delivery_outbox_lifecycle(role_factory: Callable[[str], sessionmaker[Session]]) -> None:
    raiser = role_factory("hdt_execution")
    bot = role_factory("hdt_telegram")
    clock = ManualClock(NOW)
    with use_clock(clock):
        with transaction(raiser) as s:
            alert_id = raise_alert(
                s,
                kind="reconcile_mismatch",
                severity="critical",
                title="Mismatch",
                dedupe_key="paper",
                service="execution",
                account="paper",
            )
        assert alert_id is not None
        with transaction(bot) as s:
            due = due_notifications(s)
            assert [(n.alert_id, n.phase) for n in due] == [(alert_id, "raised")]
            mark_notify_failed(s, alert_id, "raised", "Forbidden: bot was blocked by the user")
        with transaction(bot) as s:
            assert due_notifications(s) == []  # backing off
        clock.advance(NOTIFY_BACKOFF_BASE)
        with transaction(bot) as s:
            assert [n.alert_id for n in due_notifications(s)] == [alert_id]
            mark_notified(s, alert_id, "raised")
        with transaction(bot) as s:
            assert due_notifications(s) == []
        with transaction(raiser) as s:
            resolve_alert(s, kind="reconcile_mismatch", dedupe_key="paper")
        with transaction(bot) as s:
            due = due_notifications(s)
            assert [(n.alert_id, n.phase) for n in due] == [(alert_id, "resolved")]
            mark_notified(s, alert_id, "resolved")
        with bot() as s:
            row = s.get(AlertRow, alert_id)
            assert due_notifications(s) == []
    assert row is not None
    assert row.notify_attempts == 0  # reset per phase
    assert row.last_notify_error is None
    assert row.next_attempt_at is None


def test_kill_switch_pages_again_after_it_was_resolved(
    role_factory: Callable[[str], sessionmaker[Session]],
) -> None:
    execution = role_factory("hdt_execution")
    bot = role_factory("hdt_telegram")
    clock = ManualClock(NOW)

    def kill(s: Session) -> str | None:
        return alert(
            s,
            kind="kill_switch",
            severity="critical",
            title="Kill switch engaged",
            account="paper",
            service="execution",
        )

    with use_clock(clock):
        with transaction(execution) as s:
            first = kill(s)
        assert first is not None
        with transaction(bot) as s:
            mark_notified(s, first, "raised")
        with transaction(execution) as s:
            assert kill(s) is None  # same episode, still open
            resolve(s, kind="kill_switch", account="paper", service="execution")  # resumed
        clock.advance(timedelta(hours=1))
        with transaction(execution) as s:
            second = kill(s)
        with transaction(bot) as s:
            due = [(n.alert_id, n.phase) for n in due_notifications(s)]
    assert second is not None
    assert second != first
    assert due == [(second, "raised"), (first, "resolved")]


def test_each_raise_and_resolve_is_logged_once(
    role_factory: Callable[[str], sessionmaker[Session]], caplog: pytest.LogCaptureFixture
) -> None:
    """Through `hdt.core.alerts` the core module logs and the outbox does not; a direct outbox call logs."""
    execution = role_factory("hdt_execution")
    caplog.set_level(logging.INFO)
    with transaction(execution) as s:
        raised = alert(s, kind="kill_switch", severity="critical", title="K", account="paper", service="x")
        resolve(s, kind="kill_switch", account="paper", service="x")
        direct = raise_alert(
            s, kind="stop_invariant", severity="critical", title="S", dedupe_key="k", service="x"
        )
        closed = resolve_alert(s, kind="stop_invariant", dedupe_key="k")
    assert (raised is not None, direct is not None, closed) == (True, True, 1)
    logged = [(r.alert_kind, r.levelno) for r in caplog.records if hasattr(r, "alert_kind")]
    assert logged == [
        ("kill_switch", logging.CRITICAL),
        ("kill_switch", logging.INFO),
        ("stop_invariant", logging.WARNING),
        ("stop_invariant", logging.INFO),
    ]


def test_episode_scopes_the_dedupe_key(role_factory: Callable[[str], sessionmaker[Session]]) -> None:
    risk = role_factory("hdt_risk")

    def warn(s: Session, day: str) -> str | None:
        return alert(
            s,
            kind="daily_loss_warning",
            severity="warning",
            title="Daily loss at -1.5%",
            account="paper",
            service="risk",
            episode=day,
        )

    with use_clock(ManualClock(NOW)), transaction(risk) as s:
        day1 = warn(s, "2026-09-27")
        again = warn(s, "2026-09-27")
        day2 = warn(s, "2026-09-28")
        resolve(s, kind="daily_loss_warning", account="paper", service="risk", episode="2026-09-27")
        open_keys = s.scalars(
            sa.select(AlertRow.dedupe_key).where(
                AlertRow.kind == "daily_loss_warning", AlertRow.resolved_at.is_(None)
            )
        ).all()
    assert day1 is not None
    assert again is None
    assert day2 is not None
    assert open_keys == ["paper:2026-09-28"]


def test_failed_delivery_backs_off_exponentially_and_honors_retry_after(
    role_factory: Callable[[str], sessionmaker[Session]],
) -> None:
    raiser = role_factory("hdt_execution")
    bot = role_factory("hdt_telegram")
    clock = ManualClock(NOW)
    with use_clock(clock):
        with transaction(raiser) as s:
            alert_id = raise_alert(
                s, kind="ip_banned", severity="critical", title="IP banned", dedupe_key="live", service="x"
            )
        assert alert_id is not None
        delays = []
        for _ in range(8):
            with transaction(bot) as s:
                assert [n.alert_id for n in due_notifications(s)] == [alert_id]
                next_at = mark_notify_failed(s, alert_id, "raised", "Bad Gateway")
            assert next_at is not None
            delays.append(next_at - clock.now())
            clock.set(next_at - timedelta(seconds=1))
            with transaction(bot) as s:
                assert due_notifications(s) == []
            clock.set(next_at)
        with transaction(bot) as s:
            flood = mark_notify_failed(
                s, alert_id, "raised", "Too Many Requests", retry_after=timedelta(minutes=9)
            )
            short = mark_notify_failed(
                s, alert_id, "raised", "Too Many Requests", retry_after=timedelta(seconds=1)
            )
    assert delays == [min(NOTIFY_BACKOFF_BASE * 2**n, NOTIFY_BACKOFF_MAX) for n in range(8)]
    assert delays[-1] == NOTIFY_BACKOFF_MAX
    assert flood == clock.now() + timedelta(minutes=9)  # Telegram retry_after wins when longer
    assert short == clock.now() + NOTIFY_BACKOFF_MAX


def test_warnings_expire_but_critical_alerts_are_retried_forever(
    role_factory: Callable[[str], sessionmaker[Session]],
) -> None:
    raiser = role_factory("hdt_execution")
    bot = role_factory("hdt_telegram")
    clock = ManualClock(NOW)
    with use_clock(clock):
        with transaction(raiser) as s:
            critical = raise_alert(
                s, kind="stop_invariant", severity="critical", title="No STOP", dedupe_key="live", service="x"
            )
            warning = raise_alert(
                s, kind="route_dead", severity="warning", title="Route dead", dedupe_key="r", service="x"
            )
        for _ in range(60):  # more attempts than the old 50-attempt cap, spread over more than NOTIFY_MAX_AGE
            with transaction(bot) as s:
                for note in due_notifications(s):
                    mark_notify_failed(s, note.alert_id, note.phase, "Bad Gateway")
            clock.advance(timedelta(minutes=30))
        assert clock.now() - NOW > NOTIFY_MAX_AGE
        with transaction(bot) as s:
            due = [n.alert_id for n in due_notifications(s)]
    assert due == [critical]
    assert warning is not None


class FakeTelegram:
    """`send_message` stand-in: records deliveries, raises the configured error per chat id."""

    def __init__(self) -> None:
        self.failing: dict[int, TelegramApiError] = {}
        self.sent: list[int] = []
        self.messages: list[tuple[int, str]] = []
        self.attempts: list[int] = []

    async def send_message(self, chat_id: int, text: str) -> None:
        self.attempts.append(chat_id)
        error = self.failing.get(chat_id)
        if error is not None:
            raise error
        self.sent.append(chat_id)
        self.messages.append((chat_id, text))


def _heads(telegram: FakeTelegram) -> list[tuple[int, str]]:
    return [(chat_id, text.splitlines()[0]) for chat_id, text in telegram.messages]


async def test_dispatcher_retries_missing_recipients_only(
    role_factory: Callable[[str], sessionmaker[Session]],
) -> None:
    raiser = role_factory("hdt_execution")
    bot = role_factory("hdt_telegram")
    clock = ManualClock(NOW)
    dispatcher = AlertDispatcher(bot, clock=lambda: 0.0)
    telegram = FakeTelegram()
    send = partial(dispatcher.dispatch_once, telegram, [111, 222])  # type: ignore[arg-type]
    with use_clock(clock):
        with transaction(raiser) as s:
            first = raise_alert(
                s, kind="kill_switch", severity="critical", title="K", dedupe_key="a", service="x"
            )
        telegram.failing[222] = TelegramApiError("sendMessage", "Bad Gateway", error_code=502)
        assert await send() == 0
        assert telegram.sent == [111]
        telegram.failing.clear()
        clock.advance(NOTIFY_BACKOFF_BASE)
        assert await send() == 1
    assert telegram.sent == [111, 222]  # 111 already had it: not sent twice
    with bot() as s:
        row = s.get(AlertRow, first)
    assert row is not None
    assert row.notified_at is not None


async def test_higher_severity_escalates_the_open_alert_and_pages_again(
    role_factory: Callable[[str], sessionmaker[Session]],
) -> None:
    # The order manager's reconcile warning and the reconciler's critical mismatch share nothing but a
    # kind and account: a critical raised while the warning is open must still page (C3-R).
    execution = role_factory("hdt_execution")
    bot = role_factory("hdt_telegram")
    dispatcher = AlertDispatcher(bot, clock=lambda: 0.0)
    telegram = FakeTelegram()
    send = partial(dispatcher.dispatch_once, telegram, [111, 222])  # type: ignore[arg-type]

    def mismatch(s: Session, severity: Severity, title: str) -> str | None:
        return alert(
            s,
            kind="reconcile_mismatch",
            severity=severity,
            title=title,
            account="paper",
            service="execution",
        )

    clock = ManualClock(NOW)
    with use_clock(clock):
        with transaction(execution) as s:
            warning = mismatch(s, "warning", "Order state drift")
        assert await send() == 1
        clock.advance(timedelta(hours=30))  # the warning sat for a day before the mismatch became critical
        with transaction(execution) as s:
            critical = mismatch(s, "critical", "Reconcile mismatch: position without STOP")
            again = mismatch(s, "critical", "Reconcile mismatch: position without STOP")
            lower = mismatch(s, "warning", "Order state drift")
        with bot() as s:
            (due,) = due_notifications(s)
        assert await send() == 1
        with transaction(execution) as s:
            resolve(s, kind="reconcile_mismatch", account="paper", service="execution")
        assert await send() == 1
    with bot() as s:
        row = s.get(AlertRow, warning)
    assert warning is not None
    assert critical == warning  # the open alert is escalated in place, not duplicated
    assert again is None
    assert lower is None  # a lower severity never downgrades it
    assert (due.alert_id, due.phase, due.severity, due.delivered_to) == (
        warning,
        "raised",
        "critical",
        frozenset(),
    )
    assert _heads(telegram) == [
        (111, "[WARNING] Order state drift (paper)"),
        (222, "[WARNING] Order state drift (paper)"),
        (111, "[CRITICAL] Reconcile mismatch: position without STOP (paper)"),
        (222, "[CRITICAL] Reconcile mismatch: position without STOP (paper)"),
        (111, "RESOLVED - Reconcile mismatch: position without STOP (paper)"),
        (222, "RESOLVED - Reconcile mismatch: position without STOP (paper)"),
    ]
    # The escalated page carries its own time, not the day-old warning's (review m2).
    assert telegram.messages[2][1].splitlines()[-1] == (
        "kind=reconcile_mismatch service=execution escalated at 2026-09-28 18:00:00 UTC "
        "(first raised 2026-09-27 12:00:00 UTC)"
    )
    assert telegram.messages[0][1].splitlines()[-1] == (
        "kind=reconcile_mismatch service=execution at 2026-09-27 12:00:00 UTC"
    )
    assert row is not None
    assert (row.severity, row.escalated_at, row.notified_at is not None) == (
        "critical",
        NOW + timedelta(hours=30),
        True,
    )
    assert row.resolution_notified_at is not None


async def test_monitor_closes_stale_episode_alerts_silently(
    role_factory: Callable[[str], sessionmaker[Session]],
) -> None:
    # Episode alerts (one per intent, decision, symbol-day, config version) are never resolved by their
    # raiser, so the open count grew for good (review m1). Closing them must not send RESOLVED noise.
    execution = role_factory("hdt_execution")
    bot = role_factory("hdt_telegram")
    monitor = AlertMonitor(
        bot, credit_alert_fraction=0.85, prometheus_url=None, episode_close_after=timedelta(hours=24)
    )
    clock = ManualClock(NOW)

    def raise_(s: Session, kind: AlertKind, severity: Severity, key: str) -> str:
        alert_id = raise_alert(s, kind=kind, severity=severity, title=kind, dedupe_key=key, service="x")
        assert alert_id is not None
        return alert_id

    with use_clock(clock):
        with transaction(execution) as s:
            refused = raise_(s, "intent_signature", "warning", "live:intent-1")
            integrity = raise_(s, "integrity_error", "critical", "live:decision-1")
            kill = raise_(s, "kill_switch", "critical", "live")
            undelivered = raise_(s, "liquidation_distance", "critical", "live:SOL:2026-09-27")
        with transaction(bot) as s:
            for alert_id in (refused, integrity, kill):
                mark_notified(s, alert_id, "raised")
        clock.advance(timedelta(hours=23))
        with transaction(execution) as s:
            recent = raise_(s, "price_crosscheck", "critical", "live:BTC:2026-09-28")
        with transaction(bot) as s:
            mark_notified(s, recent, "raised")
        await monitor.run_once()
        with bot() as s:
            open_before = set(s.scalars(sa.select(AlertRow.alert_id).where(AlertRow.resolved_at.is_(None))))
        clock.advance(timedelta(hours=1, seconds=1))
        await monitor.run_once()
        with bot() as s:
            open_after = set(s.scalars(sa.select(AlertRow.alert_id).where(AlertRow.resolved_at.is_(None))))
            closed = s.get(AlertRow, refused)
            due = due_notifications(s)
    assert open_before == {refused, integrity, kill, undelivered, recent}
    # Delivered more than 24 h ago: closed. The kill switch is not an episode kind, the recent one is
    # younger than 24 h, and an undelivered critical stays until it is delivered.
    assert open_after == {kill, undelivered, recent}
    assert closed is not None
    assert closed.resolved_at == closed.resolution_notified_at == NOW + timedelta(hours=24, seconds=1)
    assert [(n.alert_id, n.phase) for n in due] == [(undelivered, "raised")]  # no RESOLVED message


def test_stale_send_of_the_escalated_alert_neither_closes_nor_delays_it(
    role_factory: Callable[[str], sessionmaker[Session]],
) -> None:
    execution = role_factory("hdt_execution")
    bot = role_factory("hdt_telegram")
    with use_clock(ManualClock(NOW)):
        with transaction(execution) as s:
            alert_id = raise_alert(
                s, kind="stop_invariant", severity="warning", title="STOP late", dedupe_key="k", service="x"
            )
        with bot() as s:
            (stale,) = due_notifications(s)
        with transaction(execution) as s:
            raise_alert(
                s, kind="stop_invariant", severity="critical", title="No STOP", dedupe_key="k", service="x"
            )
        with transaction(bot) as s:
            closed = mark_notified(s, stale.alert_id, "raised", severity=stale.severity)
            backoff = mark_notify_failed(s, stale.alert_id, "raised", "Bad Gateway", severity=stale.severity)
        with bot() as s:
            due = [(n.alert_id, n.severity) for n in due_notifications(s)]
    assert (stale.alert_id, stale.severity) == (alert_id, "warning")
    assert closed is False
    assert backoff is None
    assert due == [(alert_id, "critical")]


async def test_unreachable_recipient_is_terminal_for_the_message_and_raises_one_alert(
    role_factory: Callable[[str], sessionmaker[Session]],
) -> None:
    # Chat 222 never sent /start (Telegram 400 "chat not found") or blocked the bot (403): the alert
    # still closes once 111 has it instead of retrying forever, and one ops alert names the chat (N1).
    execution = role_factory("hdt_execution")
    bot = role_factory("hdt_telegram")
    clock = ManualClock(NOW)
    dispatcher = AlertDispatcher(bot, clock=lambda: 0.0)
    telegram = FakeTelegram()
    send = partial(dispatcher.dispatch_once, telegram, [111, 222])  # type: ignore[arg-type]
    failures = partial(REGISTRY.get_sample_value, "hdt_alerts_delivery_failures_total")
    with use_clock(clock):
        telegram.failing[222] = TelegramApiError("sendMessage", "Forbidden: bot was blocked", error_code=403)
        with transaction(execution) as s:
            kill = raise_alert(
                s, kind="kill_switch", severity="critical", title="Kill", dedupe_key="n1", service="x"
            )
        failures_before = failures() or 0.0
        assert await send() == 1  # the kill alert: 111 has it, 222 is unreachable for this message
        assert await send() == 1  # the unreachable warning raised by the first cycle (111 only)
        assert failures() == failures_before
        with transaction(execution) as s:
            resolve_alert(s, kind="kill_switch", dedupe_key="n1")
        assert await send() == 1  # RESOLVED goes only to the recipient that got the raise
        with bot() as s:
            unreachable = s.scalars(
                sa.select(AlertRow).where(AlertRow.kind == "telegram_recipient_unreachable")
            ).all()
        assert [(a.dedupe_key, a.resolved_at) for a in unreachable] == [("chat:222", None)]
        telegram.failing.clear()
        with transaction(execution) as s:
            banned = raise_alert(
                s, kind="ip_banned", severity="critical", title="Banned", dedupe_key="n1", service="x"
            )
        assert await send() == 1
        with bot() as s:
            open_unreachable = s.scalars(
                sa.select(AlertRow.alert_id).where(
                    AlertRow.kind == "telegram_recipient_unreachable", AlertRow.resolved_at.is_(None)
                )
            ).all()
    assert kill is not None
    assert banned is not None
    assert telegram.attempts.count(222) == 3  # kill, unreachable warning, ban: none retried, no resolution
    assert [h for h in _heads(telegram) if h[1].startswith("RESOLVED - Kill")] == [(111, "RESOLVED - Kill")]
    assert (222, "[CRITICAL] Banned") in _heads(telegram)  # the next message reached 222 again
    assert open_unreachable == []  # ... which resolved the unreachable alert


async def test_nobody_reachable_keeps_the_alert_due_and_counts_failures(
    role_factory: Callable[[str], sessionmaker[Session]],
) -> None:
    execution = role_factory("hdt_execution")
    bot = role_factory("hdt_telegram")
    clock = ManualClock(NOW)
    dispatcher = AlertDispatcher(bot, clock=lambda: 0.0)
    telegram = FakeTelegram()
    send = partial(dispatcher.dispatch_once, telegram, [111])  # type: ignore[arg-type]
    failures = partial(REGISTRY.get_sample_value, "hdt_alerts_delivery_failures_total")
    with use_clock(clock):
        telegram.failing[111] = TelegramApiError("sendMessage", "Bad Request: chat not found", error_code=400)
        with transaction(execution) as s:
            alert_id = raise_alert(
                s, kind="kill_switch", severity="critical", title="Kill", dedupe_key="nobody", service="x"
            )
        failures_before = failures() or 0.0
        assert await send() == 0
        assert failures() == failures_before + 1
        clock.advance(NOTIFY_BACKOFF_BASE)
        assert await send() == 0  # still due: the kill alert and the unreachable warning both fail
        assert failures() == failures_before + 3
        clock.advance(NOTIFY_BACKOFF_BASE * 2)
        telegram.failing.clear()  # the operator sends /start
        assert await send() == 2
        with bot() as s:
            row = s.get(AlertRow, alert_id)
            open_unreachable = s.scalars(
                sa.select(AlertRow.alert_id).where(
                    AlertRow.kind == "telegram_recipient_unreachable", AlertRow.resolved_at.is_(None)
                )
            ).all()
    assert row is not None
    assert row.notified_at is not None
    assert telegram.sent == [111, 111]
    assert open_unreachable == []


async def test_transport_error_ends_the_dispatch_cycle(
    role_factory: Callable[[str], sessionmaker[Session]],
) -> None:
    # Telegram (or the egress proxy) does not answer: one failed send ends the cycle instead of waiting
    # for a timeout per recipient and per alert while polling (and /kill) waits (N14).
    execution = role_factory("hdt_execution")
    bot = role_factory("hdt_telegram")
    dispatcher = AlertDispatcher(bot, clock=lambda: 0.0)
    telegram = FakeTelegram()
    send = partial(dispatcher.dispatch_once, telegram, [111, 222])  # type: ignore[arg-type]
    with use_clock(ManualClock(NOW)):
        with transaction(execution) as s:
            for key in ("t1", "t2", "t3"):
                raise_alert(
                    s, kind="kill_switch", severity="critical", title=key, dedupe_key=key, service="x"
                )
        telegram.failing[111] = TelegramApiError("sendMessage", "ConnectTimeout", transport=True)
        assert await send() == 0
        with bot() as s:
            attempts = sorted(r.notify_attempts for r in s.scalars(sa.select(AlertRow)))
    assert telegram.attempts == [111]  # neither 222 nor the two other alerts were tried
    assert attempts == [0, 0, 1]


async def test_dispatcher_obeys_flood_control(
    role_factory: Callable[[str], sessionmaker[Session]],
) -> None:
    raiser = role_factory("hdt_execution")
    bot = role_factory("hdt_telegram")
    clock = ManualClock(NOW)
    ticks = [0.0]
    dispatcher = AlertDispatcher(bot, clock=lambda: ticks[0])
    telegram = FakeTelegram()
    send = partial(dispatcher.dispatch_once, telegram, [111, 222])  # type: ignore[arg-type]
    with use_clock(clock):
        with transaction(raiser) as s:
            flooded = raise_alert(
                s, kind="ip_banned", severity="critical", title="B", dedupe_key="b", service="x"
            )
        telegram.failing[111] = TelegramApiError(
            "sendMessage", "Too Many Requests", error_code=429, retry_after=30
        )
        assert await send() == 0
        assert telegram.sent == []  # 222 was not tried during flood control
        telegram.failing.clear()
        clock.advance(timedelta(seconds=30))
        ticks[0] = 29.0
        assert await send() == 0  # dispatcher still paused
        ticks[0] = 30.0
        assert await send() == 1
    assert telegram.sent == [111, 222]
    with bot() as s:
        row = s.get(AlertRow, flooded)
    assert row is not None
    assert row.notified_at is not None


def test_test_alerts_cover_every_kind_and_never_block_real_alerts(
    role_factory: Callable[[str], sessionmaker[Session]],
) -> None:
    bot = role_factory("hdt_telegram")
    with transaction(bot) as s:
        ids = raise_test_alerts(s, service="ops-cli")
    with transaction(bot) as s:
        due = due_notifications(s, limit=100)
        real = raise_alert(
            s, kind="route_dead", severity="warning", title="dead", dedupe_key="route:x", service="t"
        )
    assert {n.kind for n in due if n.alert_id in ids} == {spec.kind for spec in ALERT_CATALOG}
    assert all(n.is_test and n.phase == "raised" for n in due if n.alert_id in ids)
    assert real is not None


def test_role_grants(role_factory: Callable[[str], sessionmaker[Session]]) -> None:
    with role_factory("hdt_console_ro")() as s:
        s.execute(
            sa.text("SELECT alert_id, raised_at, kind, severity, title, detail, resolved_at FROM alerts")
        )
    publisher = role_factory("hdt_publisher_ro")
    for denied in (
        "SELECT * FROM alerts",
        "SELECT * FROM cmc_key_info",
        "SELECT * FROM kill_state",
        "SELECT * FROM fills",
        "SELECT coin_id FROM candidate_sets",
        "SELECT provider_prefs_hash FROM agent_versions",
    ):
        with pytest.raises(ProgrammingError, match="permission denied"), publisher() as s:
            s.execute(sa.text(denied))
    with publisher() as s:
        s.execute(sa.text("SELECT candidate_set_sha256, payload FROM candidate_sets"))
        s.execute(sa.text("SELECT agent, version, model_slug, created_at FROM agent_versions"))
        s.execute(sa.text("SELECT gate, passed, decided_at FROM gate_flags"))
    # Raisers only resolve; they never rewrite what an alert is (the bot neither).
    recorder = role_factory("hdt_recorder")
    bot = role_factory("hdt_telegram")
    for role in (recorder, bot):
        for denied in (
            "UPDATE alerts SET kind = 'route_dead'",
            "UPDATE alerts SET dedupe_key = 'x'",
            "UPDATE alerts SET raised_at = now()",
            "UPDATE alerts SET is_test = true",
            "UPDATE alerts SET severity = 'info'",
            "UPDATE alerts SET title = 'x'",
            "UPDATE alerts SET escalated_at = now()",
            "DELETE FROM alerts",
        ):
            with pytest.raises(ProgrammingError, match="permission denied"), transaction(role) as s:
                s.execute(sa.text(denied))
    with pytest.raises(ProgrammingError, match="permission denied"), transaction(recorder) as s:
        s.execute(sa.text("SELECT * FROM alert_deliveries"))
    for denied in ("UPDATE alert_deliveries SET chat_id = 1", "DELETE FROM alert_deliveries"):
        with pytest.raises(ProgrammingError, match="permission denied"), transaction(bot) as s:
            s.execute(sa.text(denied))
    with transaction(recorder) as s:
        s.execute(sa.text("UPDATE alerts SET resolved_at = now() WHERE false"))
    with transaction(bot) as s:
        s.execute(sa.text("UPDATE alerts SET next_attempt_at = NULL WHERE false"))
        s.execute(
            sa.text("UPDATE alert_deliveries SET outcome = 'delivered', recorded_at = now() WHERE false")
        )
        s.execute(sa.text("SELECT * FROM route_health, cmc_key_info, lake_stats LIMIT 1"))
    # EXECUTE on the raise function is granted to the raisers only, never to PUBLIC.
    with (
        pytest.raises(ProgrammingError, match="permission denied"),
        transaction(role_factory("hdt_publisher_ro")) as s,
    ):
        s.execute(sa.text("SELECT alerts_raise('x', now(), 'route_dead', 'info', 't', NULL, 'k', 's', NULL)"))


def test_an_llm_facing_raiser_can_neither_silence_nor_downgrade_another_services_alert(
    role_factory: Callable[[str], sessionmaker[Session]],
) -> None:
    # Review cycle 3 I1: the escalation grant let every raiser (news, council, veto-scan included)
    # rewrite the delivery state of any alert, so execution's kill-switch page could be hidden.
    execution = role_factory("hdt_execution")
    news = role_factory("hdt_news")
    bot = role_factory("hdt_telegram")
    with use_clock(ManualClock(NOW)), transaction(execution) as s:
        kill = raise_alert(
            s, kind="kill_switch", severity="critical", title="Kill", dedupe_key="live", service="execution"
        )
        warning = raise_alert(
            s, kind="daily_loss_warning", severity="warning", title="-1.5%", dedupe_key="live", service="x"
        )
    assert kill is not None
    assert warning is not None
    for denied in (
        "UPDATE alerts SET notified_at = now() WHERE notified_at IS NULL",
        "UPDATE alerts SET next_attempt_at = 'infinity'",
        "UPDATE alerts SET severity = 'info'",
        "UPDATE alerts SET notify_attempts = 99",
        "UPDATE alerts SET resolution_notified_at = now()",
        # A pre-inserted, already delivered row would dedupe the real raise away.
        "INSERT INTO alerts (alert_id, raised_at, kind, severity, title, dedupe_key, service, is_test, "
        "notified_at, notify_attempts) VALUES ('squat', now(), 'kill_switch', 'critical', 'x', 'paper', "
        "'news', false, now(), 0)",
    ):
        with pytest.raises(ProgrammingError, match="permission denied"), transaction(news) as s:
            s.execute(sa.text(denied))
    with use_clock(ManualClock(NOW)), transaction(news) as s:
        # The raise function never lowers a severity nor touches an alert of the same severity.
        assert (
            raise_alert(s, kind="kill_switch", severity="info", title="ok", dedupe_key="live", service="n")
            is None
        )
    # A resolved alert stays resolved: resetting `resolved_at` would reopen it behind the real raiser's back.
    with transaction(news) as s:
        s.execute(sa.text("UPDATE alerts SET resolved_at = now() WHERE alert_id = :a"), {"a": warning})
    for value in ("NULL", "now() - interval '1 day'"):
        with pytest.raises(ProgrammingError, match="resolved_at cannot change"), transaction(news) as s:
            s.execute(sa.text(f"UPDATE alerts SET resolved_at = {value} WHERE alert_id = :a"), {"a": warning})
    with use_clock(ManualClock(NOW)), bot() as s:
        due = {(n.alert_id, n.severity, n.title) for n in due_notifications(s)}
    assert due == {(kill, "critical", "Kill"), (warning, "warning", "-1.5%")}


def _seed_recorder(url: str) -> None:
    engine = sa.create_engine(url)
    with Session(engine) as s, s.begin():
        s.add_all(
            [
                RouteHealthRow(
                    route_key="cmc:quotes_latest",
                    sort_order=1,
                    route="quotes_latest",
                    source="cmc",
                    cadence_label="5m",
                    last_success_at=NOW - timedelta(minutes=30),
                    last_attempt_at=NOW,
                    cycles_late=4,
                    consecutive_failures=6,
                    status="failing",
                    credits_per_day=100,
                    consumers="scanner",
                    last_error="HTTP 500",
                ),
                RouteHealthRow(
                    route_key="cmc:late_but_alive",
                    sort_order=2,
                    route="late_but_alive",
                    source="cmc",
                    cadence_label="5m",
                    last_success_at=NOW - timedelta(minutes=15),
                    last_attempt_at=NOW,
                    cycles_late=3,
                    consecutive_failures=0,
                    status="late",
                    credits_per_day=1,
                    consumers="x",
                    last_error=None,
                ),
                RouteHealthRow(
                    route_key="cmc:shed",
                    sort_order=3,
                    route="shed",
                    source="cmc",
                    cadence_label="1h",
                    last_success_at=None,
                    last_attempt_at=None,
                    cycles_late=99,
                    consecutive_failures=0,
                    status="shed",
                    credits_per_day=1,
                    consumers="x",
                    last_error=None,
                ),
                CmcKeyInfoRow(
                    checked_at=NOW,
                    credits_used_cycle=90_000,
                    credit_limit_cycle=100_000,
                    cycle_start=NOW - timedelta(days=20),
                    cycle_end=NOW + timedelta(days=10),
                    credits_used_today=1_000,
                    governor_daily_budget=3_000,
                    rate_limit_minute=30,
                    halt_state="daily_cap",
                    degraded=True,
                ),
                LakeStatsRow(
                    as_of=NOW,
                    lake_bytes=150_000_000_000,
                    disk_used_fraction=0.83,
                    retention_days=90,
                    merkle_date=None,
                    merkle_root=None,
                    object_locked=False,
                ),
            ]
        )
        for offset in range(1, 8):
            day = (NOW - timedelta(days=offset)).date()
            s.add(CmcCreditUsageRow(date=day, route="quotes_latest", credits=2_000, calls=100))
    engine.dispose()


def test_model_databases_get_the_migration_raise_function_and_guard(
    migrated_url: str, pg_engine: sa.Engine
) -> None:
    # Services and most tests raise alerts through `alerts_raise`: a `create_all` database must behave like
    # a migrated one (same function body, same guard trigger).
    definitions = (
        "SELECT pg_get_functiondef('alerts_raise'::regproc), pg_get_functiondef('alerts_guard'::regproc), "
        "pg_get_triggerdef(oid) FROM pg_trigger WHERE tgname = 'alerts_guard_row'"
    )
    migrated = sa.create_engine(migrated_url)
    try:
        with migrated.connect() as conn:
            expected = conn.execute(sa.text(definitions)).one()
    finally:
        migrated.dispose()
    with pg_engine.connect() as conn:
        assert conn.execute(sa.text(definitions)).one() == expected


async def test_monitor_raises_and_resolves_recorder_rules(
    migrated_url: str, role_factory: Callable[[str], sessionmaker[Session]]
) -> None:
    _seed_recorder(migrated_url)
    bot = role_factory("hdt_telegram")
    monitor = AlertMonitor(bot, credit_alert_fraction=0.85, prometheus_url=None)
    with use_clock(ManualClock(NOW)):
        await monitor.run_once()
        await monitor.run_once()
    with bot() as s:
        open_rows = s.execute(
            sa.select(AlertRow.kind, AlertRow.dedupe_key).where(AlertRow.resolved_at.is_(None))
        ).all()
    assert sorted(open_rows) == sorted(
        [
            ("cmc_credit_projection", f"cycle:{(NOW + timedelta(days=10)).date().isoformat()}"),
            ("cmc_halt", "daily_cap"),
            ("disk_usage", "lake"),
            ("route_dead", "route:cmc:quotes_latest"),
        ]
    )
    admin = sa.create_engine(migrated_url)
    with admin.begin() as conn:
        conn.execute(sa.text("UPDATE route_health SET cycles_late = 0, status = 'ok'"))
        conn.execute(sa.text("UPDATE cmc_key_info SET halt_state = NULL, credits_used_cycle = 10000"))
    admin.dispose()
    with use_clock(ManualClock(NOW + timedelta(minutes=1))):
        await monitor.run_once()
    with bot() as s:
        still_open = s.scalars(sa.select(AlertRow.kind).where(AlertRow.resolved_at.is_(None))).all()
    assert still_open == ["disk_usage"]
