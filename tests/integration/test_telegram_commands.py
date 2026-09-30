"""Telegram operator commands: authorization, confirmation, TOTP replay, lockout and resume refusals.

Runs as the real `hdt_telegram` role on a migrated database, so every refusal, audit row and TOTP
bookkeeping write goes through the real grants.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from redis.asyncio import Redis
from sqlalchemy.engine import make_url
from sqlalchemy.exc import TimeoutError as PoolTimeoutError
from sqlalchemy.orm import Session, sessionmaker
from tests.reader_rows import card_row, insert, scored_row, weight_row

from hdt.configapi.totp import new_totp_secret, totp_code
from hdt.contracts.streams import Stream
from hdt.db.models.decision import DecisionCardRow
from hdt.db.models.scoring import ScoredForecastRow, WeightHistoryRow
from hdt.ops.telegram_bot import (
    CONFIRM_TTL,
    INTERNAL_ERROR_REPLY,
    RESUME_LOCKOUT,
    TOTP_MAX_FAILURES,
    UNAUTHORIZED_AUDIT_EVERY,
    CommandProcessor,
    IncomingMessage,
    parse_update,
)
from hdt.settings.events import ControlCommand

pytestmark = [pytest.mark.pg, pytest.mark.redis, pytest.mark.integration]

ROOT = Path(__file__).resolve().parents[2]
OWNER = 111
STRANGER = 222
SECOND_OWNER = 333
NOW = datetime(2026, 9, 27, 12, 0, 5, tzinfo=UTC)
TOTP_SECRET = new_totp_secret()
KILLED = (
    "INSERT INTO kill_state (account, state, cause, reason, since, updated_at) "
    "VALUES ('{account}', 'killed', '{cause}', 'x', :t, :t)"
)
RECONCILED = (
    "INSERT INTO reconcile_runs (account, ran_at, clean, stop_invariant_ok, detail, mismatches) "
    "VALUES ('live', :t, {clean}, {stop_ok}, '{{}}', '[]')"
)


class Clock:
    def __init__(self, at: datetime) -> None:
        self.at = at

    def __call__(self) -> datetime:
        return self.at


@pytest.fixture(scope="module")
def migrated_url(fresh_database: Callable[[], AbstractContextManager[str]]) -> Iterator[str]:
    with fresh_database() as url:
        cfg = Config(str(ROOT / "alembic.ini"))
        cfg.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
        command.upgrade(cfg, "head")
        yield url


@pytest.fixture
def admin(migrated_url: str) -> Iterator[sa.Engine]:
    engine = sa.create_engine(migrated_url)
    yield engine
    with engine.begin() as conn:
        for table in (
            "kill_state",
            "reconcile_runs",
            "audit_log",
            "control_commands",
            "telegram_totp_state",
            "telegram_totp_failures",
            "alert_deliveries",
            "alerts",
            "scored_forecasts",
            "decision_cards",
            "weight_history",
        ):
            conn.execute(sa.text(f"DELETE FROM {table}"))
    engine.dispose()


@pytest.fixture
def factory(migrated_url: str, pg_role_url: Callable[[str, str], str]) -> Iterator[sessionmaker[Session]]:
    engine = sa.create_engine(pg_role_url(migrated_url, "hdt_telegram"))
    yield sessionmaker(engine, expire_on_commit=False)
    engine.dispose()


@pytest.fixture
def clock() -> Clock:
    return Clock(NOW)


@pytest.fixture
def processor(factory: sessionmaker[Session], redis_client: Redis, clock: Clock) -> CommandProcessor:
    return CommandProcessor(
        session_factory=factory,
        redis=redis_client,
        allowed_user_ids=[OWNER],
        totp_secret=TOTP_SECRET,
        clock=clock,
        new_code=lambda: "ABC123",
    )


def _msg(
    text: str,
    *,
    user: int = OWNER,
    chat: int | None = None,
    chat_type: str = "private",
    sent_at: datetime = NOW,
) -> IncomingMessage:
    return IncomingMessage(
        update_id=1,
        chat_id=user if chat is None else chat,
        chat_type=chat_type,
        user_id=user,
        text=text,
        sent_at=sent_at,
    )


async def _controls(redis: Redis) -> list[dict[str, Any]]:
    entries = await redis.xrange(Stream.CONTROLS.value)
    return [json.loads(fields[b"data"]) for _id, fields in entries]


def _audit(admin: sa.Engine) -> list[tuple[str, str, dict[str, Any]]]:
    with admin.connect() as conn:
        rows = conn.execute(sa.text('SELECT "user", action, diff_redacted FROM audit_log ORDER BY id')).all()
    return [(r[0], r[1], r[2]) for r in rows]


def test_parse_update_ignores_non_text_updates() -> None:
    assert parse_update({"update_id": 5, "edited_message": {"text": "/kill"}}) is None
    assert (
        parse_update({"update_id": 5, "message": {"chat": {"id": 1}, "from": {"id": 1}, "date": 0}}) is None
    )
    parsed = parse_update(
        {
            "update_id": 5,
            "message": {
                "chat": {"id": 1, "type": "private"},
                "from": {"id": 1},
                "date": 1790000000,
                "text": " /status ",
            },
        }
    )
    assert parsed is not None
    assert parsed.text == "/status"


@pytest.mark.parametrize(
    "message",
    [
        _msg("/kill", user=STRANGER),
        _msg("/kill", chat=-100123, chat_type="group"),
        _msg("/kill", chat=999),
    ],
    ids=["stranger", "group_chat", "chat_not_sender"],
)
async def test_unauthorized_senders_get_no_reply_and_no_effect(
    processor: CommandProcessor, redis_client: Redis, admin: sa.Engine, message: IncomingMessage
) -> None:
    assert await processor.handle(message) is None
    assert await processor.handle(message) is None
    assert await _controls(redis_client) == []
    audit = _audit(admin)
    assert [a[1] for a in audit] == ["telegram.unauthorized"]  # throttled: one row per minute per sender


async def test_stale_command_is_ignored(
    processor: CommandProcessor, redis_client: Redis, admin: sa.Engine
) -> None:
    assert await processor.handle(_msg("/pause", sent_at=NOW - timedelta(minutes=2, seconds=1))) is None
    assert await _controls(redis_client) == []
    assert _audit(admin)[-1][2]["outcome"] == "stale"


async def test_pause_publishes_one_control_per_account(
    processor: CommandProcessor, redis_client: Redis, admin: sa.Engine
) -> None:
    reply = await processor.handle(_msg("/pause live funding spike"))
    assert reply == "PAUSE sent for LIVE."
    (control,) = await _controls(redis_client)
    assert (control["account"], control["action"], control["source"]) == ("live", "pause", "telegram")
    assert control["reason"] == "funding spike"
    assert control["requested_by"] == f"telegram:{OWNER}"
    with admin.connect() as conn:
        stored = conn.execute(sa.text("SELECT command_id, stream_id FROM control_commands")).one()
    assert stored[0] == control["command_id"]
    assert stored[1] is not None
    assert _audit(admin)[-1][:2] == (f"telegram:{OWNER}", "telegram.pause")


async def test_agents_and_decision_read_the_scoring_and_council_tables(
    processor: CommandProcessor, admin: sa.Engine
) -> None:
    scored_as_of = NOW - timedelta(hours=20)
    with admin.begin() as conn:
        insert(
            conn,
            WeightHistoryRow,
            [
                weight_row(
                    "crowding", NOW - timedelta(days=2), w=0.18, a=0.9, r=0.6, coverage=0.95, forecasts=200
                ),
                weight_row(
                    "crowding",
                    NOW - timedelta(days=1),
                    w=0.2,
                    w_capped=0.19,
                    a=0.9,
                    r=0.6,
                    coverage=0.97,
                    forecasts=214,
                    agent_version=2,
                ),
                weight_row(
                    "technical",
                    NOW - timedelta(days=1),
                    w=0.15,
                    a=0.8,
                    r=0.5,
                    coverage=0.9,
                    forecasts=120,
                    agent_version=None,
                ),
            ],
        )
        insert(
            conn,
            DecisionCardRow,
            [
                card_row("EVT-TG", "OPUSDT", scored_as_of),
                card_row(
                    "EVT-TG-HELD",
                    "ARBUSDT",
                    NOW - timedelta(hours=10),
                    source="HELD",
                    outcome="HOLD",
                    held_side="LONG",
                    intent="HOLD",
                    manager_size=0.0,
                    unscored=True,
                ),
                card_row("EVT-TG-YOUNG", "SOLUSDT", NOW - timedelta(hours=2)),
            ],
        )
        insert(
            conn,
            ScoredForecastRow,
            [scored_row("EVT-TG", "pooled", as_of=scored_as_of, hit=True, scored_at=NOW, p=0.65)],
        )
    assert await processor.handle(_msg("/agents")) == (
        "Agents (RESID_12H): w / a / r / coverage\n"
        "crowding v2: 0.19 / 0.90 / 0.60 / 97% (214 forecasts)\n"
        "technical: 0.15 / 0.80 / 0.50 / 90% (120 forecasts)"
    )
    assert await processor.handle(_msg("/agents raw_12h")) == "No weights recorded yet for RAW_12H."
    assert await processor.handle(_msg("/decision EVT-TG")) == (
        "EVT-TG OPUSDT LTX at 2026-09-26 16:00 UTC\n"
        "Outcome LONG x1.0, pooled p 0.65, D 0.20, rounds 2\n"
        "Result: correct (up)"
    )
    held = await processor.handle(_msg("/decision EVT-TG-HELD"))
    assert held is not None
    assert held.splitlines()[1:] == [
        "Outcome HOLD, pooled p 0.65, D 0.20, rounds 2",
        "Result: not scored (held-position re-evaluation)",
    ]
    young = await processor.handle(_msg("/decision EVT-TG-YOUNG"))
    assert young is not None
    assert young.splitlines()[-1] == "Result: pending"


async def test_kill_needs_the_code_within_60_seconds(
    processor: CommandProcessor, redis_client: Redis, clock: Clock
) -> None:
    reply = await processor.handle(_msg("/kill"))
    assert reply is not None
    assert "/confirm ABC123" in reply
    assert await _controls(redis_client) == []
    assert await processor.handle(_msg("/confirm ZZZ999")) == "Wrong confirmation code."
    clock.at = NOW + CONFIRM_TTL + timedelta(seconds=1)
    expired = await processor.handle(_msg("/confirm abc123", sent_at=clock.at))
    assert expired == "The confirmation expired. Send the command again."
    assert await _controls(redis_client) == []
    await processor.handle(_msg("/kill", sent_at=clock.at))
    confirmed = await processor.handle(_msg("/confirm abc123", sent_at=clock.at))
    assert confirmed == "KILL sent for PAPER, TESTNET, LIVE."
    assert [c["action"] for c in await _controls(redis_client)] == ["kill", "kill", "kill"]


async def test_resume_requires_totp_and_rejects_replay(
    processor: CommandProcessor, redis_client: Redis, admin: sa.Engine, clock: Clock
) -> None:
    with admin.begin() as conn:
        conn.execute(
            sa.text(KILLED.format(account="paper", cause="manual")),
            {"t": NOW - timedelta(hours=1)},
        )
    assert "one account at a time" in (await processor.handle(_msg("/resume all")) or "")
    await processor.handle(_msg("/resume paper checked"))
    assert await processor.handle(_msg("/confirm ABC123")) == "Send /confirm ABC123 <TOTP code>."
    assert "rejected" in (await processor.handle(_msg("/confirm ABC123 000000")) or "")
    code = totp_code(TOTP_SECRET, NOW)
    assert await processor.handle(_msg(f"/confirm ABC123 {code}")) == "RESUME sent for PAPER."
    (control,) = await _controls(redis_client)
    assert (control["account"], control["action"]) == ("paper", "resume")
    clock.at = NOW + timedelta(seconds=5)
    await processor.handle(_msg("/resume paper", sent_at=clock.at))
    replay = await processor.handle(_msg(f"/confirm ABC123 {code}", sent_at=clock.at))
    assert (replay or "").startswith("TOTP code rejected (wrong, expired or already used)")
    assert len(await _controls(redis_client)) == 1


async def test_totp_code_accepted_for_one_user_is_rejected_for_another(
    factory: sessionmaker[Session], redis_client: Redis, admin: sa.Engine, clock: Clock
) -> None:
    # One seed is shared by every allowed user, so a used code must be burnt for all of them.
    processor = CommandProcessor(
        session_factory=factory,
        redis=redis_client,
        allowed_user_ids=[OWNER, SECOND_OWNER],
        totp_secret=TOTP_SECRET,
        clock=clock,
        new_code=lambda: "ABC123",
    )
    code = totp_code(TOTP_SECRET, NOW)
    await processor.handle(_msg("/resume paper"))
    assert await processor.handle(_msg(f"/confirm ABC123 {code}")) == "RESUME sent for PAPER."
    await processor.handle(_msg("/resume paper", user=SECOND_OWNER))
    replay = await processor.handle(_msg(f"/confirm ABC123 {code}", user=SECOND_OWNER))
    assert (replay or "").startswith("TOTP code rejected")
    assert len(await _controls(redis_client)) == 1


async def test_wrong_totp_codes_lock_resume_and_alert(
    factory: sessionmaker[Session], redis_client: Redis, admin: sa.Engine, clock: Clock
) -> None:
    def bot() -> CommandProcessor:
        return CommandProcessor(
            session_factory=factory,
            redis=redis_client,
            allowed_user_ids=[OWNER, SECOND_OWNER],
            totp_secret=TOTP_SECRET,
            clock=clock,
            new_code=lambda: "ABC123",
        )

    wrong = "999999" if totp_code(TOTP_SECRET, NOW) != "999999" else "888888"
    processor = bot()
    await processor.handle(_msg("/resume paper"))
    for left in range(TOTP_MAX_FAILURES - 1, TOTP_MAX_FAILURES - 3, -1):
        reply = await processor.handle(_msg(f"/confirm ABC123 {wrong}"))
        assert f"{left} tries left" in (reply or "")
    processor = bot()  # a restart does not reset the failure count
    await processor.handle(_msg("/resume paper"))
    for _ in range(TOTP_MAX_FAILURES - 3):
        await processor.handle(_msg(f"/confirm ABC123 {wrong}"))
    locked = await processor.handle(_msg(f"/confirm ABC123 {wrong}"))
    assert "was dropped" in (locked or "")
    assert "is locked" in (locked or "")
    valid = totp_code(TOTP_SECRET, NOW)
    assert await processor.handle(_msg(f"/confirm ABC123 {valid}")) == "Nothing to confirm."
    assert "is locked" in (await processor.handle(_msg("/resume paper")) or "")
    # The lockout is per Telegram user; the other operator can still resume.
    assert "/confirm ABC123" in (await processor.handle(_msg("/resume paper", user=SECOND_OWNER)) or "")
    first_key = f"user:{OWNER}:{NOW + RESUME_LOCKOUT:%Y%m%dT%H%M%SZ}"
    assert _lockout_alerts(admin) == [(first_key, "critical", False)]
    assert await _controls(redis_client) == []

    # The lockout expires and the same user fails again: a new episode, so it pages again (N6).
    clock.at = relock = NOW + RESUME_LOCKOUT + timedelta(seconds=1)
    await processor.handle(_msg("/resume paper", sent_at=clock.at))
    for _ in range(TOTP_MAX_FAILURES):
        locked = await processor.handle(_msg(f"/confirm ABC123 {_wrong_code(clock.at)}", sent_at=clock.at))
    assert "is locked" in (locked or "")
    second_key = f"user:{OWNER}:{relock + RESUME_LOCKOUT:%Y%m%dT%H%M%SZ}"
    assert _lockout_alerts(admin) == [(first_key, "critical", False), (second_key, "critical", False)]

    clock.at = relock + RESUME_LOCKOUT + timedelta(seconds=1)
    await processor.handle(_msg("/resume paper", sent_at=clock.at))
    accepted = await processor.handle(
        _msg(f"/confirm ABC123 {totp_code(TOTP_SECRET, clock.at)}", sent_at=clock.at)
    )
    assert accepted == "RESUME sent for PAPER."
    with admin.connect() as conn:
        failures: int = conn.execute(
            sa.text("SELECT failures FROM telegram_totp_failures WHERE user_id = :u"), {"u": OWNER}
        ).scalar_one()
    assert _lockout_alerts(admin) == [(first_key, "critical", True), (second_key, "critical", True)]
    assert failures == 0


def _wrong_code(at: datetime) -> str:
    return "999999" if totp_code(TOTP_SECRET, at) != "999999" else "888888"


def _lockout_alerts(admin: sa.Engine) -> list[tuple[str, str, bool]]:
    with admin.connect() as conn:
        rows = conn.execute(
            sa.text(
                "SELECT dedupe_key, severity, resolved_at IS NOT NULL FROM alerts "
                "WHERE kind = 'telegram_totp_lockout' ORDER BY raised_at"
            )
        ).all()
    return [(r[0], r[1], r[2]) for r in rows]


async def test_confirmation_and_totp_codes_are_never_audited(
    processor: CommandProcessor, admin: sa.Engine
) -> None:
    code = totp_code(TOTP_SECRET, NOW)
    await processor.handle(_msg("/resume paper"))
    await processor.handle(_msg(f"/confirm ABC123 {code}"))
    audit = _audit(admin)
    assert audit[-1][1] == "telegram.confirm"
    assert audit[-1][2]["outcome"] == "ok"
    assert "ABC123" not in json.dumps(audit)
    assert code not in json.dumps(audit)


async def test_unknown_commands_are_audited_and_unauthorized_tracking_is_bounded(
    processor: CommandProcessor, admin: sa.Engine, clock: Clock
) -> None:
    assert await processor.handle(_msg("/shutdown now")) == "Unknown command. Send /help."
    (action, diff) = _audit(admin)[-1][1:]
    assert (action, diff["outcome"]) == ("telegram.unknown", "unknown")
    for sender in range(1000, 1010):
        await processor.handle(_msg("/kill", user=sender))
    clock.at = NOW + UNAUTHORIZED_AUDIT_EVERY
    await processor.handle(_msg("/kill", user=2000, sent_at=clock.at))
    assert list(processor._unauthorized_audited) == [2000]  # expired senders are forgotten


async def test_database_errors_are_answered_and_never_raised(
    migrated_url: str, pg_role_url: Callable[[str, str], str], redis_client: Redis, clock: Clock
) -> None:
    # A database that does not exist fails every connection at once, like Postgres being down.
    broken = make_url(pg_role_url(migrated_url, "hdt_telegram")).set(database="hdt_no_such_database")
    engine = sa.create_engine(broken)
    try:
        processor = CommandProcessor(
            session_factory=sessionmaker(engine),
            redis=redis_client,
            allowed_user_ids=[OWNER],
            totp_secret=TOTP_SECRET,
            clock=clock,
            new_code=lambda: "ABC123",
        )
        assert await processor.handle(_msg("/status")) == INTERNAL_ERROR_REPLY
        await processor.handle(_msg("/kill live"))
        refused = await processor.handle(_msg("/confirm ABC123"))
        resume = await processor.handle(_msg("/resume paper"))  # the lockout check cannot read
    finally:
        engine.dispose()
    assert refused is not None
    assert "KILL was NOT sent for LIVE (database unavailable)" in refused
    assert resume == INTERNAL_ERROR_REPLY
    assert await _controls(redis_client) == []


async def test_a_pool_timeout_midway_reports_exactly_what_was_sent(
    processor: CommandProcessor, redis_client: Redis, monkeypatch: pytest.MonkeyPatch
) -> None:
    # `/kill` for every account: TESTNET cannot be recorded (pool exhausted, not a DBAPIError) after
    # PAPER was already published. The reply must say PAPER and LIVE were killed, not "nothing was done".
    record = processor._record_control

    def exhausted_on_testnet(command: ControlCommand) -> None:
        if command.account.value == "testnet":
            raise PoolTimeoutError("QueuePool limit of size 5 overflow 10 reached, connection timed out")
        record(command)

    monkeypatch.setattr(processor, "_record_control", exhausted_on_testnet)
    await processor.handle(_msg("/kill"))
    reply = await processor.handle(_msg("/confirm ABC123"))
    assert reply == (
        "KILL sent for PAPER, LIVE.\n"
        "KILL was NOT sent for TESTNET (database unavailable). Use hdt-kill over SSH if this is an emergency."
    )
    assert [(c["account"], c["action"]) for c in await _controls(redis_client)] == [
        ("paper", "kill"),
        ("live", "kill"),
    ]


async def test_resume_disabled_without_totp_seed(
    factory: sessionmaker[Session], redis_client: Redis, clock: Clock
) -> None:
    processor = CommandProcessor(
        session_factory=factory, redis=redis_client, allowed_user_ids=[OWNER], totp_secret=None, clock=clock
    )
    reply = await processor.handle(_msg("/resume paper"))
    assert reply is not None
    assert "disabled" in reply
    assert "/confirm" not in reply


async def test_daily_loss_kill_cannot_be_resumed_the_same_utc_day(
    processor: CommandProcessor, admin: sa.Engine, clock: Clock
) -> None:
    with admin.begin() as conn:
        conn.execute(
            sa.text(KILLED.format(account="live", cause="daily_loss")),
            {"t": datetime(2026, 9, 27, 0, 0, 1, tzinfo=UTC)},
        )
    refused = await processor.handle(_msg("/resume live"))
    assert refused is not None
    assert "00:00 UTC" in refused
    clock.at = datetime(2026, 9, 28, 0, 0, 1, tzinfo=UTC)
    allowed = await processor.handle(_msg("/resume live", sent_at=clock.at))
    assert allowed is not None
    assert "/confirm ABC123 <TOTP code>" in allowed


async def test_reconcile_kill_needs_a_clean_run_and_a_repaired_stop_does_not_block(
    processor: CommandProcessor, admin: sa.Engine
) -> None:
    # Execution's resume predicate is `clean` alone: a run that repaired a routine STOP (clean, STOP
    # invariant false) must not make Telegram refuse what the console and hdt-kill accept (review m3).
    with admin.begin() as conn:
        conn.execute(
            sa.text(KILLED.format(account="live", cause="reconcile")),
            {"t": NOW - timedelta(hours=3)},
        )
        conn.execute(
            sa.text(RECONCILED.format(clean="false", stop_ok="false")),
            {"t": NOW - timedelta(minutes=1)},
        )
    assert "not clean" in (await processor.handle(_msg("/resume live")) or "")
    with admin.begin() as conn:
        conn.execute(
            sa.text(RECONCILED.format(clean="true", stop_ok="false")),
            {"t": NOW},
        )
    assert "/confirm" in (await processor.handle(_msg("/resume live")) or "")


@pytest.mark.parametrize("cause", ["manual", "daily_loss", "rate_limit"])
async def test_dirty_latest_reconcile_refuses_resume_whatever_the_stored_cause(
    processor: CommandProcessor, admin: sa.Engine, cause: str
) -> None:
    # A manual (or yesterday's daily-loss) kill followed by a mismatch keeps the first cause: the
    # dirty reconcile run alone must still refuse the resume (N2).
    with admin.begin() as conn:
        conn.execute(
            sa.text(KILLED.format(account="live", cause=cause)),
            {"t": NOW - timedelta(days=1)},
        )
        conn.execute(
            sa.text(RECONCILED.format(clean="false", stop_ok="true")),
            {"t": NOW - timedelta(minutes=1)},
        )
    refused = await processor.handle(_msg("/resume live"))
    assert "latest reconcile run is not clean" in (refused or "")
    assert "/confirm" not in (refused or "")
    with admin.begin() as conn:
        conn.execute(sa.text(RECONCILED.format(clean="true", stop_ok="true")), {"t": NOW})
    assert "/confirm ABC123 <TOTP code>" in (await processor.handle(_msg("/resume live")) or "")


async def test_cancel_drops_the_pending_confirmation(
    processor: CommandProcessor, redis_client: Redis
) -> None:
    await processor.handle(_msg("/kill live"))
    assert await processor.handle(_msg("/cancel")) == "Pending confirmation cancelled."
    assert await processor.handle(_msg("/confirm ABC123")) == "Nothing to confirm."
    assert await _controls(redis_client) == []
