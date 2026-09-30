"""Telegram bot: operator commands, alert delivery, webhook watchdog and the `ops` vault scope owner.

Entry point: `python -m hdt.ops.telegram_bot` (service `telegram-bot`, role `hdt_telegram`, Redis user
`telegram`, scope `ops`).

Security rules (Design Contract section 10, phase 10 Red Team Delta):
- The bot token and the allowed user ids come from the vault secret `ops/telegram` (entered on the
  console, sealed for the `ops` scope). This service owns the scope: it self-tests new versions with
  `getMe` + `getWebhookInfo` and activates them.
- Only private chats are served, and only for `from.id` in `allowed_user_ids`; anything else gets no
  reply (so the bot does not reveal itself) and is audited (throttled per sender).
- `/kill` is two-step: the bot answers with a one-time code that must be sent back with `/confirm`
  within 60 s. `/resume` is two-step and needs a TOTP code (seed: bootstrap secret
  `HDT_TELEGRAM_TOTP_SECRET_FILE`, shared by every allowed user; a TOTP code is accepted once only, for any
  user, across restarts). `TOTP_MAX_FAILURES` (5) wrong codes within `TOTP_FAILURE_WINDOW` drop the pending
  resume, lock `/resume` for that user for `RESUME_LOCKOUT` and raise a critical `telegram_totp_lockout`
  alert (one per lockout, so a repeated attack pages again; all resolved by that user's next accepted TOTP
  code). The bot refuses a resume while the latest reconcile run is not clean, and a resume that would
  clear a daily-loss kill within the same UTC day; execution enforces the same rules on its side, so the
  bot is only a first gate.
- Every command, accepted, refused or unknown, is written to `audit_log` (confirmation and TOTP codes are
  never stored); controls are also logged in `control_commands` and published as
  `ControlCommand(source="telegram")` on stream `controls`.
- An unexpected error (database down, ...) while handling one message is answered with
  `INTERNAL_ERROR_REPLY` and never stops the loop; a failing loop iteration is logged and retried. If a
  service task dies anyway, the process exits non-zero so the container restarts, and the
  `hdt_telegram_poll_heartbeat_timestamp_seconds` gauge (healthcheck + Prometheus rule) goes stale.
- `/decision` shows only published-grade figures (no levels, sizes, order ids or config versions).
- `getWebhookInfo` is polled: a webhook set on the token (which would silently stop `getUpdates`, so
  `/kill` would never arrive) raises a critical `telegram_webhook` alert; persistent polling failures raise
  `telegram_polling`. The independent kill path is `hdt-kill` over SSH (runbook).
"""

from __future__ import annotations

import asyncio
import contextlib
import hmac
import logging
import secrets
import signal
import time
import uuid
from collections import OrderedDict
from collections.abc import Callable, Coroutine, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from functools import partial
from typing import Any, Final, Literal, get_args

import httpx
from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import DBAPIError, ProgrammingError, SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from hdt.configapi.audit import write_audit
from hdt.configapi.totp import verify_totp
from hdt.contracts.common import Account
from hdt.contracts.streams import Stream
from hdt.core.clock import ensure_utc, utcnow
from hdt.core.config import read_env_value, require_env_value, static_config
from hdt.core.logging import configure_logging
from hdt.core.streams import connect_redis, publish
from hdt.db.models.ops import TelegramTotpFailureRow, TelegramTotpStateRow
from hdt.db.models.settings import ControlCommandRow
from hdt.db.session import make_engine, make_session_factory, transaction
from hdt.ops import metrics
from hdt.ops.alerts import (
    AlertMonitor,
    Notification,
    due_notifications,
    format_notification,
    mark_notified,
    mark_notify_failed,
    open_alert_count,
    open_dedupe_keys,
    raise_alert,
    record_delivery,
    resolve_alert,
)
from hdt.ops.telegram_client import TelegramApiError, TelegramClient
from hdt.settings.events import ControlAction, ControlCommand
from hdt.settings.versions import SectionNotConfiguredError, pin_current
from hdt.vault.loader import SecretIntegrityError, SecretLoader, SecretNotAvailableError
from hdt.vault.owner import CheckOutcome, OwnerWorker, SecretCheck
from hdt.vault.scopes import TelegramSecret, load_private_key

log = logging.getLogger(__name__)

SERVICE: Final[str] = "telegram-bot"
SECRET_NAME: Final[str] = "telegram"  # noqa: S105 - vault item name, not a secret
TOTP_SECRET_ENV: Final[str] = "HDT_TELEGRAM_TOTP_SECRET"  # noqa: S105 - env name (read via ..._FILE)
PROMETHEUS_URL_ENV: Final[str] = "HDT_PROMETHEUS_URL"
CONFIRM_TTL: Final[timedelta] = timedelta(seconds=60)
MAX_MESSAGE_AGE: Final[timedelta] = timedelta(minutes=2)
UNAUTHORIZED_AUDIT_EVERY: Final[timedelta] = timedelta(minutes=1)
UNAUTHORIZED_TRACKED_MAX: Final[int] = 1024  # senders remembered for the audit throttle
TOTP_MAX_FAILURES: Final[int] = 5
TOTP_FAILURE_WINDOW: Final[timedelta] = timedelta(hours=1)  # older failures are forgotten
RESUME_LOCKOUT: Final[timedelta] = timedelta(hours=1)
# sendMessage answers that are final for one chat: 400 chat not found (the user never sent /start),
# 403 bot blocked / user deactivated. Retrying the same message to that chat can never succeed.
UNREACHABLE_ERROR_CODES: Final[frozenset[int]] = frozenset({400, 403})
INTERNAL_ERROR_REPLY: Final[str] = (
    "Internal error, nothing was done. Use hdt-kill over SSH if this is an emergency."
)
ACCOUNTS: Final[tuple[str, ...]] = tuple(a.value for a in Account)
ROI_WINDOWS: Final[dict[str, timedelta]] = {
    "1d": timedelta(days=1),
    "7d": timedelta(days=7),
    "30d": timedelta(days=30),
}
TARGET_TYPES: Final[tuple[str, ...]] = ("RESID_12H", "RAW_12H")
Command = Literal[
    "start", "help", "status", "roi", "agents", "decision", "pause", "resume", "kill", "confirm", "cancel"
]
COMMANDS: Final[frozenset[str]] = frozenset(get_args(Command))

HELP_TEXT: Final[str] = (
    "Moirai operator bot\n"
    "/status [account] - run state, equity, today's PnL, open positions, open alerts\n"
    "/roi 1d|7d|30d [account] - equity change over the window\n"
    "/agents [RESID_12H|RAW_12H] - learned w, a, r and coverage per agent\n"
    "/decision <event_id> - summary of a council decision\n"
    "/pause [account|all] [reason] - stop new entries (default: all accounts)\n"
    "/resume <account> [reason] - two steps + TOTP\n"
    "/kill [account|all] [reason] - two steps (default: all accounts)\n"
    "/confirm <code> [totp] - confirm a pending /kill or /resume\n"
    "/cancel - drop the pending confirmation\n"
    "Independent kill path: hdt-kill over SSH (runbook)."
)


# --------------------------------------------------------------------------- messages


@dataclass(frozen=True)
class IncomingMessage:
    update_id: int
    chat_id: int
    chat_type: str
    user_id: int
    text: str
    sent_at: datetime


def parse_update(update: dict[str, Any]) -> IncomingMessage | None:
    """A text message with a sender, or None (edits, channel posts, service messages)."""
    message = update.get("message")
    update_id = update.get("update_id")
    if not isinstance(message, dict) or not isinstance(update_id, int):
        return None
    chat = message.get("chat")
    sender = message.get("from")
    text_value = message.get("text")
    date = message.get("date")
    if not (isinstance(chat, dict) and isinstance(sender, dict) and isinstance(text_value, str)):
        return None
    chat_id, user_id = chat.get("id"), sender.get("id")
    if not (isinstance(chat_id, int) and isinstance(user_id, int) and isinstance(date, int)):
        return None
    return IncomingMessage(
        update_id=update_id,
        chat_id=chat_id,
        chat_type=str(chat.get("type", "")),
        user_id=user_id,
        text=text_value.strip(),
        sent_at=datetime.fromtimestamp(date, tz=utcnow().tzinfo),
    )


def split_command(text_value: str) -> tuple[str, list[str]] | None:
    """`/kill@MyBot live reason` -> ("kill", ["live", "reason"]); None when not a command."""
    if not text_value.startswith("/"):
        return None
    head, *args = text_value.split()
    return head[1:].split("@", 1)[0].lower(), args


# --------------------------------------------------------------------------- read side


class SourceUnavailableError(RuntimeError):
    """A table the command needs does not exist yet or is not granted to hdt_telegram."""


def _query(session: Session, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    try:
        return [dict(row._mapping) for row in session.execute(text(sql), params or {})]
    except (ProgrammingError, DBAPIError) as exc:
        raise SourceUnavailableError(str(getattr(exc, "orig", exc)).splitlines()[0][:200]) from None


def _fmt_num(value: Any, dp: int = 2) -> str:
    return "-" if value is None else f"{float(value):,.{dp}f}"


def _fmt_signed(value: Any, dp: int = 2) -> str:
    return "-" if value is None else f"{float(value):+,.{dp}f}"


def _fmt_pct(value: float | None) -> str:
    return "-" if value is None else f"{value * 100:+.2f}%"


class BotReadModel:
    """Read-only queries behind /status, /roi, /agents and /decision (one transaction each)."""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._factory = session_factory

    def _read[T](self, fn: Callable[[Session], T]) -> T:
        with self._factory() as session:
            return fn(session)

    def run_mode(self) -> str:
        def read(session: Session) -> str:
            try:
                mode = pin_current(session).mode()
            except SectionNotConfiguredError:
                return "not configured"
            except ValueError:  # stored version outside the current ceilings: the services clip it
                return "stored version invalid (services run it clipped to the ceilings)"
            return f"{mode.mode.upper()} x{mode.size_multiplier:g}"

        return self._read(read)

    def kill_state(self, account: str) -> dict[str, Any] | None:
        """Phase 09 keeps one `kill_state` row per account (cause: manual|daily_loss|reconcile|rate_limit)."""
        rows = self._read(
            lambda s: _query(
                s,
                "SELECT state, cause, reason, since FROM kill_state WHERE account = :a",
                {"a": account},
            )
        )
        return rows[0] if rows else None

    def latest_reconcile(self, account: str) -> tuple[bool, datetime] | None:
        """(clean, ran_at) of the latest reconcile run; None before any.

        `clean` alone, exactly execution's resume predicate (`hdt.execution.kill_switch`): a run that
        repaired a routine STOP (`clean`, `stop_invariant_ok` false) never blocks a resume.
        """
        rows = self._read(
            lambda s: _query(
                s,
                "SELECT clean, ran_at FROM reconcile_runs WHERE account = :a ORDER BY ran_at DESC LIMIT 1",
                {"a": account},
            )
        )
        if not rows:
            return None
        row = rows[0]
        return bool(row["clean"]), ensure_utc(row["ran_at"])

    def status_lines(self, accounts: Sequence[str]) -> list[str]:
        lines = [f"Run mode: {self.run_mode()}"]
        for account in accounts:
            parts = [account.upper()]
            try:
                state = self.kill_state(account)
                parts.append(f"state {state['state']}" if state else "state unknown")
                if state and state.get("cause"):
                    parts.append(f"cause {state['cause']}")
                equity = self._read(partial(self._latest_equity, account=account))
                if equity:
                    eq = float(equity["equity"])
                    start = float(equity["day_start_equity"])
                    pnl = eq - start
                    pct = _fmt_pct(pnl / start if start else None)
                    parts.append(f"equity {_fmt_num(eq)}, today {_fmt_signed(pnl)} ({pct})")
                parts.append(f"open positions {self._read(partial(self._open_positions, account=account))}")
            except SourceUnavailableError as exc:
                parts.append(f"ledger not available ({exc})")
            lines.append(" - ".join([parts[0], ", ".join(parts[1:])]))
        lines.append(f"Open alerts: {self._read(open_alert_count)}")
        return lines

    @staticmethod
    def _latest_equity(session: Session, account: str) -> dict[str, Any] | None:
        rows = _query(
            session,
            "SELECT ts, equity, day_start_equity FROM equity_snapshots WHERE account = :a "
            "ORDER BY ts DESC LIMIT 1",
            {"a": account},
        )
        return rows[0] if rows else None

    @staticmethod
    def _open_positions(session: Session, account: str) -> int:
        rows = _query(
            session,
            "SELECT count(*) AS n FROM positions WHERE account = :a AND closed_at IS NULL "
            "AND NOT is_hedge_book",
            {"a": account},
        )
        return int(rows[0]["n"])

    def roi(self, account: str, window: timedelta) -> str:
        def read(session: Session) -> str:
            latest = _query(
                session,
                "SELECT ts, equity FROM equity_snapshots WHERE account = :a ORDER BY ts DESC LIMIT 1",
                {"a": account},
            )
            if not latest:
                return f"{account.upper()}: no equity snapshots yet."
            ts = ensure_utc(latest[0]["ts"])
            base = _query(
                session,
                "SELECT ts, equity FROM equity_snapshots WHERE account = :a AND ts <= :t "
                "ORDER BY ts DESC LIMIT 1",
                {"a": account, "t": ts - window},
            )
            if not base:
                base = _query(
                    session,
                    "SELECT ts, equity FROM equity_snapshots WHERE account = :a ORDER BY ts LIMIT 1",
                    {"a": account},
                )
            now_eq, base_eq = float(latest[0]["equity"]), float(base[0]["equity"])
            change = now_eq - base_eq
            since = ensure_utc(base[0]["ts"]).strftime("%Y-%m-%d %H:%M")
            return (
                f"{account.upper()} since {since} UTC: {_fmt_signed(change)} "
                f"({_fmt_pct(change / base_eq if base_eq else None)}), equity {_fmt_num(now_eq)}"
            )

        return self._read(read)

    def agents(self, target_type: str) -> list[str]:
        rows = self._read(
            lambda s: _query(
                s,
                "SELECT DISTINCT ON (agent) agent, agent_version, w, w_capped, a, r, coverage, forecasts "
                "FROM weight_history WHERE target_type = :t ORDER BY agent, as_of DESC",
                {"t": target_type},
            )
        )
        if not rows:
            return [f"No weights recorded yet for {target_type}."]
        lines = [f"Agents ({target_type}): w / a / r / coverage"]
        for row in rows:
            w = row["w_capped"] if row.get("w_capped") is not None else row["w"]
            coverage = row.get("coverage")
            cov = "-" if coverage is None else f"{float(coverage) * 100:.0f}%"
            # weight_history.agent_version is NULL while the scorer has seen no numbered agent version.
            version = "" if row["agent_version"] is None else f" v{row['agent_version']}"
            lines.append(
                f"{row['agent']}{version}: {_fmt_num(w)} / {_fmt_num(row['a'])} / "
                f"{_fmt_num(row['r'])} / {cov} ({row['forecasts']} forecasts)"
            )
        return lines

    def decision(self, event_id: str) -> list[str]:
        def read(session: Session) -> list[str]:
            cards = _query(
                session,
                "SELECT event_id, symbol, as_of, source, outcome, manager_size, p_pooled, disagreement, "
                "rounds, unscored FROM decision_cards WHERE event_id = :e",
                {"e": event_id},
            )
            if not cards:
                return [f"No decision {event_id}."]
            card = cards[0]
            as_of = ensure_utc(card["as_of"]).strftime("%Y-%m-%d %H:%M")
            size = f" x{float(card['manager_size']):.1f}" if card.get("manager_size") else ""
            lines = [
                f"{card['event_id']} {card['symbol']} {card['source']} at {as_of} UTC",
                f"Outcome {card['outcome']}{size}, pooled p {_fmt_num(card['p_pooled'], 2)}, "
                f"D {_fmt_num(card['disagreement'], 2)}, rounds {card['rounds']}",
            ]
            if card.get("unscored"):
                lines.append("Result: not scored (held-position re-evaluation)")
            else:
                try:
                    score = _query(
                        session,
                        "SELECT hit, label FROM scored_forecasts WHERE event_id = :e AND agent = 'pooled'",
                        {"e": event_id},
                    )
                except SourceUnavailableError:
                    score = []
                if score and score[0]["hit"] is not None:
                    verdict = "correct" if score[0]["hit"] else "wrong"
                    lines.append(f"Result: {verdict} ({score[0]['label']})")
                else:
                    lines.append("Result: pending")
            return lines

        return self._read(read)


# --------------------------------------------------------------------------- command processor


@dataclass
class PendingConfirmation:
    code: str
    action: ControlAction
    accounts: tuple[str, ...]
    reason: str
    expires_at: datetime
    needs_totp: bool


@dataclass
class ControlOutcome:
    delivered: list[str] = field(default_factory=list)
    undelivered: list[str] = field(default_factory=list)  # logged, not published (Redis unavailable)
    failed: list[str] = field(default_factory=list)  # neither logged nor published (database error)

    @property
    def status(self) -> str:
        """Audit / metric outcome: the worst per-account result."""
        if self.failed:
            return "failed"
        return "undelivered" if self.undelivered else "ok"


@dataclass(frozen=True)
class TotpCheck:
    """Outcome of one TOTP code: accepted, or rejected with the remaining tries / the new lockout."""

    accepted: bool
    failures_left: int = TOTP_MAX_FAILURES
    locked_until: datetime | None = None


def audited_args(command: str, args: Sequence[str]) -> list[str]:
    """Arguments written to `audit_log`: confirmation and TOTP codes are never stored."""
    if command == "confirm":
        return ["<redacted>" for _ in args[:2]]
    return list(args[:6])


def _lockout_key_prefix(user_id: int) -> str:
    """`telegram_totp_lockout` dedupe keys are `user:<id>:<locked_until>`: every lockout pages again."""
    return f"user:{user_id}:"


class CommandProcessor:
    """Authorizes and executes one message; returns the reply text or None (no reply)."""

    def __init__(
        self,
        *,
        session_factory: sessionmaker[Session],
        redis: Redis,
        allowed_user_ids: Iterable[int],
        totp_secret: str | None,
        read_model: BotReadModel | None = None,
        controls_maxlen: int | None = None,
        clock: Callable[[], datetime] = utcnow,
        new_code: Callable[[], str] | None = None,
    ) -> None:
        self._factory = session_factory
        self._redis = redis
        self._allowed = frozenset(allowed_user_ids)
        self._totp_secret = totp_secret
        self._read = read_model or BotReadModel(session_factory)
        self._maxlen = controls_maxlen
        self._clock = clock
        self._new_code = new_code or (lambda: secrets.token_hex(3).upper())
        self._pending: dict[int, PendingConfirmation] = {}
        self._unauthorized_audited: OrderedDict[int, datetime] = OrderedDict()

    def update_allowlist(self, allowed_user_ids: Iterable[int]) -> None:
        self._allowed = frozenset(allowed_user_ids)
        self._pending = {uid: p for uid, p in self._pending.items() if uid in self._allowed}

    def is_authorized(self, msg: IncomingMessage) -> bool:
        return msg.chat_type == "private" and msg.chat_id == msg.user_id and msg.user_id in self._allowed

    async def handle(self, msg: IncomingMessage) -> str | None:
        """Never raises: an unexpected error is logged, audited and answered with `INTERNAL_ERROR_REPLY`.

        Every step that can fail unexpectedly (database reads, TOTP bookkeeping, recording a control)
        runs before anything is published, and `_publish_controls` reports per-account failures itself,
        so the internal-error reply is accurate: nothing was done.
        """
        parsed = split_command(msg.text)
        command = parsed[0] if parsed and parsed[0] in COMMANDS else "unknown"
        try:
            return await self._handle(msg, parsed)
        except Exception:
            log.exception("telegram command failed", extra={"from_id": msg.user_id, "command": command})
            metrics.TELEGRAM_LOOP_ERRORS.labels(stage="message").inc()
            if not self.is_authorized(msg):
                return None
            metrics.TELEGRAM_COMMANDS.labels(command=command, outcome="error").inc()
            await self._audit(msg.user_id, command, {"outcome": "error"})
            return INTERNAL_ERROR_REPLY

    async def _handle(self, msg: IncomingMessage, parsed: tuple[str, list[str]] | None) -> str | None:
        now = self._clock()
        if not self.is_authorized(msg):
            await self._audit_unauthorized(msg, parsed[0] if parsed else "message", now)
            return None
        if now - ensure_utc(msg.sent_at) > MAX_MESSAGE_AGE:
            # Updates survive restarts for 24 h; never act on a stale command.
            await self._audit(msg.user_id, parsed[0][:32] if parsed else "message", {"outcome": "stale"})
            return None
        if parsed is None or parsed[0] not in COMMANDS:
            metrics.TELEGRAM_COMMANDS.labels(command="unknown", outcome="unknown").inc()
            await self._audit(
                msg.user_id,
                "unknown",
                {"command": parsed[0][:32] if parsed else "message", "outcome": "unknown"},
            )
            return "Unknown command. Send /help."
        command, args = parsed
        try:
            reply, outcome = await self._dispatch(command, args, msg.user_id, now)
        except SourceUnavailableError as exc:
            reply, outcome = f"Data source not available yet: {exc}", "unavailable"
        metrics.TELEGRAM_COMMANDS.labels(command=command, outcome=outcome).inc()
        await self._audit(msg.user_id, command, {"args": audited_args(command, args), "outcome": outcome})
        return reply

    async def _dispatch(self, command: str, args: list[str], user_id: int, now: datetime) -> tuple[str, str]:
        if command in ("start", "help"):
            return HELP_TEXT, "ok"
        if command == "status":
            accounts = self._accounts(args[:1], default_all=True)
            if accounts is None:
                return "Usage: /status [paper|testnet|live|all]", "usage"
            return "\n".join(await asyncio.to_thread(self._read.status_lines, accounts)), "ok"
        if command == "roi":
            window = ROI_WINDOWS.get(args[0].lower()) if args else None
            accounts = self._accounts(args[1:2], default_all=False, default=("paper",))
            if window is None or accounts is None:
                return "Usage: /roi 1d|7d|30d [paper|testnet|live]", "usage"
            return await asyncio.to_thread(self._read.roi, accounts[0], window), "ok"
        if command == "agents":
            target = args[0].upper() if args else TARGET_TYPES[0]
            if target not in TARGET_TYPES:
                return "Usage: /agents [RESID_12H|RAW_12H]", "usage"
            return "\n".join(await asyncio.to_thread(self._read.agents, target)), "ok"
        if command == "decision":
            if len(args) != 1 or len(args[0]) > 80:
                return "Usage: /decision <event_id>", "usage"
            return "\n".join(await asyncio.to_thread(self._read.decision, args[0])), "ok"
        if command == "pause":
            return await self._pause(args, user_id)
        if command in ("kill", "resume"):
            return await self._request_confirmation(command, args, user_id, now)
        if command == "confirm":
            return await self._confirm(args, user_id, now)
        # cancel
        dropped = self._pending.pop(user_id, None)
        return ("Pending confirmation cancelled." if dropped else "Nothing to cancel."), "ok"

    @staticmethod
    def _accounts(
        args: Sequence[str], *, default_all: bool, default: tuple[str, ...] = ACCOUNTS
    ) -> tuple[str, ...] | None:
        if not args:
            return ACCOUNTS if default_all else default
        choice = args[0].lower()
        if choice == "all":
            return ACCOUNTS
        return (choice,) if choice in ACCOUNTS else None

    @staticmethod
    def _split_account_reason(args: list[str]) -> tuple[list[str], str]:
        if args and args[0].lower() in (*ACCOUNTS, "all"):
            return args[:1], " ".join(args[1:])
        return [], " ".join(args)

    async def _pause(self, args: list[str], user_id: int) -> tuple[str, str]:
        account_args, reason = self._split_account_reason(args)
        accounts = self._accounts(account_args, default_all=True)
        assert accounts is not None
        outcome = await self._publish_controls(user_id, "pause", accounts, reason or "paused from Telegram")
        return self._control_reply("pause", outcome), outcome.status

    async def _request_confirmation(
        self, command: str, args: list[str], user_id: int, now: datetime
    ) -> tuple[str, str]:
        account_args, reason = self._split_account_reason(args)
        if command == "resume":
            if not account_args or account_args[0].lower() == "all":
                return "Usage: /resume <paper|testnet|live> [reason] (one account at a time)", "usage"
            if self._totp_secret is None:
                return (
                    "Resume from Telegram is disabled: no TOTP seed is configured "
                    "(HDT_TELEGRAM_TOTP_SECRET_FILE). Resume from the console.",
                    "refused",
                )
            account = account_args[0].lower()
            locked_until = await asyncio.to_thread(self._resume_locked_until, user_id, now)
            if locked_until is not None:
                return self._locked_reply(locked_until), "locked"
            refusal = await asyncio.to_thread(self._resume_refusal, account, now)
            if refusal is not None:
                return refusal, "refused"
            accounts: tuple[str, ...] = (account,)
        else:
            parsed = self._accounts(account_args, default_all=True)
            assert parsed is not None
            accounts = parsed
        code = self._new_code()
        action: ControlAction = "kill" if command == "kill" else "resume"
        self._pending[user_id] = PendingConfirmation(
            code=code,
            action=action,
            accounts=accounts,
            reason=reason or f"{command} from Telegram",
            expires_at=now + CONFIRM_TTL,
            needs_totp=action == "resume",
        )
        target = ", ".join(a.upper() for a in accounts)
        how = f"/confirm {code} <TOTP code>" if action == "resume" else f"/confirm {code}"
        return (
            f"Confirm {action.upper()} for {target}: send {how} within {int(CONFIRM_TTL.total_seconds())} s. "
            "/cancel to drop it."
        ), "pending"

    def _resume_refusal(self, account: str, now: datetime) -> str | None:
        """First gate for the execution resume rules (execution enforces them again).

        Whatever the stored kill cause, resume is refused while the latest reconcile run is not clean
        (the cause kept after several kills may be another one, e.g. a manual kill then a mismatch). A
        daily-loss kill cannot be cleared within the UTC day it started; a reconcile kill needs a clean
        reconcile run after the kill. `clean` is the same predicate execution applies.
        """
        try:
            state = self._read.kill_state(account)
            if state is None or state["state"] == "running":
                return None
            cause = str(state.get("cause") or "")
            since = state.get("since")
            if (
                cause == "daily_loss"
                and since is not None
                and ensure_utc(since).date() >= ensure_utc(now).date()
            ):
                return (
                    f"{account.upper()} was stopped by the daily loss limit today (UTC); "
                    "it cannot be resumed before 00:00 UTC."
                )
            latest = self._read.latest_reconcile(account)
            if latest is not None and not latest[0]:
                return (
                    f"{account.upper()}: the latest reconcile run is not clean (ledger mismatch or a "
                    "position without its STOP); resume is refused until a reconcile run is clean."
                )
            if cause == "reconcile" and (
                latest is None or (since is not None and latest[1] <= ensure_utc(since))
            ):
                return (
                    f"{account.upper()} was stopped by a reconcile mismatch and no clean reconcile run "
                    "happened since; resume is refused until one does."
                )
        except SourceUnavailableError:
            return None  # the bot cannot pre-check without the ledger; execution still enforces the rules
        return None

    async def _confirm(self, args: list[str], user_id: int, now: datetime) -> tuple[str, str]:
        pending = self._pending.get(user_id)
        if pending is None:
            return "Nothing to confirm.", "refused"
        if now > pending.expires_at:
            self._pending.pop(user_id, None)
            return "The confirmation expired. Send the command again.", "expired"
        if not args or not hmac.compare_digest(args[0].upper(), pending.code):
            return "Wrong confirmation code.", "refused"
        if pending.needs_totp:
            if len(args) < 2 or not self._totp_secret:
                return f"Send /confirm {pending.code} <TOTP code>.", "refused"
            check = await asyncio.to_thread(self._check_totp, user_id, args[1], now)
            if check.locked_until is not None:
                self._pending.pop(user_id, None)
                return (
                    f"Too many wrong TOTP codes: the pending {pending.action.upper()} was dropped. "
                    + self._locked_reply(check.locked_until)
                    + " The operators were alerted."
                ), "locked"
            if not check.accepted:
                return (
                    "TOTP code rejected (wrong, expired or already used); "
                    f"{check.failures_left} tries left before /resume is locked."
                ), "refused"
        self._pending.pop(user_id, None)
        outcome = await self._publish_controls(user_id, pending.action, pending.accounts, pending.reason)
        return self._control_reply(pending.action, outcome), outcome.status

    @staticmethod
    def _locked_reply(locked_until: datetime) -> str:
        until = ensure_utc(locked_until)
        return (
            f"/resume is locked for your Telegram account until {until:%Y-%m-%d %H:%M} UTC. "
            "Resume from the console if needed."
        )

    def _resume_locked_until(self, user_id: int, now: datetime) -> datetime | None:
        with self._factory() as session:
            row = session.get(TelegramTotpFailureRow, user_id)
        if row is None or row.locked_until is None or ensure_utc(row.locked_until) <= now:
            return None
        return ensure_utc(row.locked_until)

    def _check_totp(self, user_id: int, code: str, now: datetime) -> TotpCheck:
        """Verify one code in one transaction: replay guard per shared seed, failure count per user.

        The seed is shared by every allowed user, so the last accepted time step is kept per seed: a code
        accepted for one user is rejected for every other user too. A wrong, expired or replayed code
        counts as a failure; the `TOTP_MAX_FAILURES`-th failure within `TOTP_FAILURE_WINDOW` locks
        `/resume` for this user and raises a critical alert in the same transaction.
        """
        assert self._totp_secret is not None
        with transaction(self._factory) as session:
            failure = session.get(TelegramTotpFailureRow, user_id, with_for_update=True)
            if (
                failure is not None
                and failure.locked_until is not None
                and ensure_utc(failure.locked_until) > now
            ):
                return TotpCheck(
                    accepted=False, failures_left=0, locked_until=ensure_utc(failure.locked_until)
                )
            state = session.get(TelegramTotpStateRow, TOTP_SECRET_ENV, with_for_update=True)
            counter = verify_totp(
                self._totp_secret,
                code,
                now=now,
                last_counter=state.last_counter if state is not None else None,
            )
            if counter is not None:
                stored = session.execute(
                    pg_insert(TelegramTotpStateRow)
                    .values(seed_name=TOTP_SECRET_ENV, last_counter=counter, updated_at=now)
                    .on_conflict_do_update(
                        index_elements=["seed_name"],
                        set_={"last_counter": counter, "updated_at": now},
                        where=TelegramTotpStateRow.last_counter < counter,
                    )
                    .returning(TelegramTotpStateRow.seed_name)
                ).scalar_one_or_none()
                if stored is not None:
                    if failure is not None:
                        failure.failures, failure.locked_until, failure.updated_at = 0, None, now
                    prefix = _lockout_key_prefix(user_id)
                    for key in sorted(open_dedupe_keys(session, "telegram_totp_lockout")):
                        if key.startswith(prefix):
                            resolve_alert(session, kind="telegram_totp_lockout", dedupe_key=key)
                    return TotpCheck(accepted=True)
            recent = failure is not None and ensure_utc(failure.updated_at) > now - TOTP_FAILURE_WINDOW
            failures = (failure.failures if failure is not None and recent else 0) + 1
            locked_until = now + RESUME_LOCKOUT if failures >= TOTP_MAX_FAILURES else None
            session.execute(
                pg_insert(TelegramTotpFailureRow)
                .values(user_id=user_id, failures=failures, locked_until=locked_until, updated_at=now)
                .on_conflict_do_update(
                    index_elements=["user_id"],
                    set_={"failures": failures, "locked_until": locked_until, "updated_at": now},
                )
            )
            if locked_until is None:
                return TotpCheck(accepted=False, failures_left=TOTP_MAX_FAILURES - failures)
            log.critical("telegram resume locked after wrong TOTP codes", extra={"from_id": user_id})
            raise_alert(
                session,
                kind="telegram_totp_lockout",
                severity="critical",
                title=f"/resume locked for Telegram user {user_id} after {failures} wrong TOTP codes",
                detail=(
                    f"{failures} wrong, expired or replayed TOTP codes within "
                    f"{int(TOTP_FAILURE_WINDOW.total_seconds() // 60)} min. The pending resume was dropped "
                    f"and /resume is locked for this user until {locked_until:%Y-%m-%d %H:%M} UTC. If this "
                    "was not an operator, treat the Telegram account as compromised: remove it from the "
                    "allowed users on the console and rotate the TOTP seed."
                ),
                dedupe_key=f"{_lockout_key_prefix(user_id)}{ensure_utc(locked_until):%Y%m%dT%H%M%SZ}",
                service=SERVICE,
            )
            return TotpCheck(accepted=False, failures_left=0, locked_until=locked_until)

    async def _publish_controls(
        self, user_id: int, action: ControlAction, accounts: Sequence[str], reason: str
    ) -> ControlOutcome:
        """Record then publish one control per account; every outcome is reported per account.

        A control whose `control_commands` row cannot be written (any database error, pool timeouts
        included) is not published (`failed`); once published, a failure to store the stream id is only
        logged (the control was delivered). Nothing here raises, so the reply always tells what was done.
        """
        outcome = ControlOutcome()
        requested_by = f"telegram:{user_id}"
        for account in accounts:
            command = ControlCommand(
                command_id=uuid.uuid4().hex,
                account=Account(account),
                action=action,
                reason=reason[:500],
                requested_by=requested_by,
                source="telegram",
                requested_at=self._clock(),
            )
            try:
                await asyncio.to_thread(self._record_control, command)
            except SQLAlchemyError:
                log.exception(
                    "control not recorded", extra={"command_id": command.command_id, "action": action}
                )
                outcome.failed.append(account)
                continue
            try:
                stream_id = await publish(self._redis, Stream.CONTROLS, command, maxlen=self._maxlen)
            except RedisError:
                log.error("control not delivered", extra={"command_id": command.command_id, "action": action})
                outcome.undelivered.append(account)
                continue
            outcome.delivered.append(account)
            try:
                await asyncio.to_thread(self._record_stream_id, command.command_id, stream_id)
            except SQLAlchemyError:
                log.exception("control stream id not recorded", extra={"command_id": command.command_id})
        return outcome

    def _record_control(self, command: ControlCommand) -> None:
        with transaction(self._factory) as session:
            session.add(
                ControlCommandRow(
                    command_id=command.command_id,
                    account=command.account.value,
                    action=command.action,
                    reason=command.reason,
                    requested_by=command.requested_by,
                    source=command.source,
                    ip=None,
                    requested_at=command.requested_at,
                )
            )

    def _record_stream_id(self, command_id: str, stream_id: str) -> None:
        with transaction(self._factory) as session:
            session.execute(
                update(ControlCommandRow)
                .where(ControlCommandRow.command_id == command_id)
                .values(stream_id=stream_id)
            )

    @staticmethod
    def _control_reply(action: str, outcome: ControlOutcome) -> str:
        lines = []
        if outcome.delivered:
            lines.append(f"{action.upper()} sent for {', '.join(a.upper() for a in outcome.delivered)}.")
        if outcome.undelivered:
            lines.append(
                f"{action.upper()} was logged but NOT delivered for "
                f"{', '.join(a.upper() for a in outcome.undelivered)} (Redis unavailable). "
                "Use hdt-kill over SSH if this is an emergency."
            )
        if outcome.failed:
            lines.append(
                f"{action.upper()} was NOT sent for {', '.join(a.upper() for a in outcome.failed)} "
                "(database unavailable). Use hdt-kill over SSH if this is an emergency."
            )
        return "\n".join(lines)

    async def _audit(self, user_id: int, command: str, diff: dict[str, Any]) -> None:
        await asyncio.to_thread(self._write_audit, f"telegram:{user_id}", command, diff)

    def _write_audit(self, user: str, command: str, diff: dict[str, Any]) -> None:
        try:
            with transaction(self._factory) as session:
                write_audit(
                    session, user=user, ip=None, action=f"telegram.{command}", section="telegram", diff=diff
                )
        except SQLAlchemyError:  # never turns an executed command into an "internal error" reply
            log.exception("audit write failed", extra={"action": f"telegram.{command}"})

    async def _audit_unauthorized(self, msg: IncomingMessage, command: str, now: datetime) -> None:
        seen = self._unauthorized_audited
        last = seen.get(msg.user_id)
        if last is not None and now - last < UNAUTHORIZED_AUDIT_EVERY:
            return
        seen.pop(msg.user_id, None)
        while seen and (
            len(seen) >= UNAUTHORIZED_TRACKED_MAX
            or now - next(iter(seen.values())) >= UNAUTHORIZED_AUDIT_EVERY
        ):
            seen.popitem(last=False)  # oldest first: expired entries, then the least recent sender
        seen[msg.user_id] = now
        log.warning(
            "unauthorized telegram message", extra={"from_id": msg.user_id, "chat_type": msg.chat_type}
        )
        await self._audit(
            msg.user_id,
            "unauthorized",
            {"command": command[:32], "chat_type": msg.chat_type, "chat_id": msg.chat_id},
        )


# --------------------------------------------------------------------------- owner check


async def check_telegram_secret(
    secret: TelegramSecret, *, transport: httpx.AsyncBaseTransport | None = None
) -> CheckOutcome:
    """Owner self-test: the token answers getMe and no webhook is set (a webhook disables polling)."""
    async with httpx.AsyncClient(timeout=15.0, transport=transport) as http:
        client = TelegramClient(secret.bot_token, http=http)
        try:
            me = await client.get_me()
            webhook = await client.get_webhook_info()
        except TelegramApiError as exc:
            if exc.error_code in (401, 404):
                return CheckOutcome("invalid", "Telegram rejected the bot token")
            return CheckOutcome("error", exc.description[:200])
    details: dict[str, str | int | float | bool | None] = {
        "bot_username": str(me.get("username") or ""),
        "is_bot": bool(me.get("is_bot")),
        "webhook_set": bool(webhook.get("url")),
        "allowed_users": len(secret.allowed_user_ids),
    }
    if not me.get("is_bot"):
        return CheckOutcome("invalid", "the token does not belong to a bot", details)
    if webhook.get("url"):
        return CheckOutcome(
            "invalid",
            "a webhook is set on this token, so the bot cannot poll for commands; "
            "delete it or rotate the token",
            details,
        )
    return CheckOutcome("valid", None, details)


async def _owner_check(payload: object) -> CheckOutcome:
    if not isinstance(payload, TelegramSecret):
        raise TypeError("telegram payload must be a TelegramSecret")
    return await check_telegram_secret(payload)


OPS_SECRET_CHECKS: Final[dict[str, SecretCheck]] = {SECRET_NAME: _owner_check}


# --------------------------------------------------------------------------- service


@dataclass
class _SendResult:
    delivered: list[int] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)  # retryable failures of this attempt
    retry_after: float | None = None  # Telegram flood control
    transport_down: bool = False  # no Bot API answer at all (Telegram or egress proxy unreachable)


class AlertDispatcher:
    """Sends due alert notifications to the allowed users (private chat id == user id).

    Outcomes are recorded per recipient and message (`alert_deliveries`), so a retry only goes to the
    recipients still missing the message. Telegram 400 / 403 for a chat (chat not found: the user never
    sent /start; bot blocked; account deleted) is terminal for that message: the chat is recorded
    `unreachable` and skipped, and a `telegram_recipient_unreachable` alert is raised (resolved by the next
    message that reaches the chat). A notification is closed once every reachable recipient has it; a
    raised message that nobody could receive stays due (and counts as a failure), so it reaches the first
    operator who unblocks the bot. A resolution goes only to the recipients that received the raise.

    Any other failure backs off exponentially per alert. A Telegram 429 pauses all dispatching for
    `retry_after` (flood control applies to the whole bot); a transport error ends the cycle, so send
    timeouts never hold up polling (and therefore `/kill`) for long.
    """

    def __init__(
        self, session_factory: sessionmaker[Session], *, clock: Callable[[], float] = time.monotonic
    ) -> None:
        self._factory = session_factory
        self._clock = clock
        self._paused_until = 0.0

    async def dispatch_once(self, client: TelegramClient, recipients: Sequence[int]) -> int:
        """Send what is due; returns the number of notifications completed in this cycle."""
        if self._clock() < self._paused_until:
            return 0
        notes = await asyncio.to_thread(self._due)
        sent = 0
        for note in notes:
            result = await self._send(client, note, recipients)
            if result.errors:
                metrics.ALERTS_DELIVERY_FAILURES.inc()
                wait = timedelta(seconds=result.retry_after) if result.retry_after is not None else None
                await asyncio.to_thread(self._failed, note, "; ".join(result.errors), wait)
            elif await asyncio.to_thread(self._notified, note):
                if note.delivered_to or result.delivered:
                    metrics.ALERTS_DELIVERED.labels(phase=note.phase).inc()
                sent += 1
            if result.retry_after is not None:
                self._paused_until = self._clock() + result.retry_after
                log.warning(
                    "telegram flood control: alert dispatch paused", extra={"retry_after": result.retry_after}
                )
                break
            if result.transport_down:
                log.warning(
                    "telegram unreachable: alert dispatch cycle stopped", extra={"error": result.errors[-1]}
                )
                break
        return sent

    async def _send(
        self, client: TelegramClient, note: Notification, recipients: Sequence[int]
    ) -> _SendResult:
        result = _SendResult()
        if not recipients:
            result.errors.append("no recipients")
            return result
        audience = [c for c in recipients if note.audience is None or c in note.audience]
        missing = [c for c in audience if c not in note.delivered_to]
        targets = [c for c in missing if c not in note.unreachable]
        if note.phase == "raised" and not note.delivered_to and not targets:
            targets = missing  # nobody has it and nobody is reachable: keep trying every recipient
        message = format_notification(note)
        for chat_id in targets:
            try:
                await client.send_message(chat_id, message)
            except TelegramApiError as exc:
                if exc.retry_after is None and exc.error_code in UNREACHABLE_ERROR_CODES:
                    await asyncio.to_thread(self._unreachable, note, chat_id, exc)
                    continue
                result.errors.append(f"chat {chat_id}: {exc.description}")
                if exc.retry_after is not None:
                    result.retry_after = exc.retry_after
                    break
                if exc.transport:
                    result.transport_down = True
                    break
                continue
            await asyncio.to_thread(self._delivered, note, chat_id)
            result.delivered.append(chat_id)
        if not result.errors and note.phase == "raised" and not (note.delivered_to or result.delivered):
            result.errors.append("no allowed user can receive messages from the bot")
        return result

    def _due(self) -> list[Notification]:
        with self._factory() as session:
            return due_notifications(session)

    def _delivered(self, note: Notification, chat_id: int) -> None:
        with transaction(self._factory) as session:
            record_delivery(session, note.alert_id, note.phase, chat_id, severity=note.severity)
            resolve_alert(session, kind="telegram_recipient_unreachable", dedupe_key=f"chat:{chat_id}")

    def _unreachable(self, note: Notification, chat_id: int, error: TelegramApiError) -> None:
        log.warning(
            "telegram recipient unreachable",
            extra={"chat_id": chat_id, "alert_id": note.alert_id, "error": error.description},
        )
        with transaction(self._factory) as session:
            record_delivery(
                session, note.alert_id, note.phase, chat_id, severity=note.severity, outcome="unreachable"
            )
            raise_alert(
                session,
                kind="telegram_recipient_unreachable",
                severity="warning",
                title=f"Telegram user {chat_id} cannot receive alerts",
                detail=(
                    f"Telegram refused an alert message for this allowed user ({error.error_code}: "
                    f"{error.description}). The user must open a private chat with the bot and send /start, "
                    "or unblock the bot. Alerts skip this user until a message gets through; the other "
                    "allowed users still receive them."
                ),
                dedupe_key=f"chat:{chat_id}",
                service=SERVICE,
            )

    def _notified(self, note: Notification) -> bool:
        with transaction(self._factory) as session:
            return mark_notified(session, note.alert_id, note.phase, severity=note.severity)

    def _failed(self, note: Notification, error: str, retry_after: timedelta | None) -> None:
        with transaction(self._factory) as session:
            mark_notify_failed(
                session, note.alert_id, note.phase, error, retry_after=retry_after, severity=note.severity
            )


class TelegramBotService:
    POLL_ERRORS_BEFORE_ALERT: Final[int] = 5
    WEBHOOK_CHECK_EVERY_S: Final[float] = 300.0
    DISPATCH_EVERY_S: Final[float] = 5.0
    SECRET_REFRESH_EVERY_S: Final[float] = 60.0
    ITERATION_ERROR_SLEEP_S: Final[float] = 5.0

    def __init__(
        self,
        *,
        session_factory: sessionmaker[Session],
        redis: Redis,
        loader: SecretLoader,
        owner: OwnerWorker,
        monitor: AlertMonitor,
        totp_secret: str | None,
        controls_maxlen: int | None,
    ) -> None:
        self._factory = session_factory
        self._redis = redis
        self._loader = loader
        self._owner = owner
        self._monitor = monitor
        self._dispatcher = AlertDispatcher(session_factory)
        self._processor = CommandProcessor(
            session_factory=session_factory,
            redis=redis,
            allowed_user_ids=(),
            totp_secret=totp_secret,
            controls_maxlen=controls_maxlen,
        )
        self._secret: TelegramSecret | None = None
        self._secret_version: int | None = None
        self._offset: int | None = None
        self._poll_failures = 0
        self._last_secret_check = 0.0
        self._last_webhook_check = 0.0
        self._last_dispatch = 0.0

    def _load_secret(self) -> bool:
        """Load (or reload) the active `ops/telegram` secret; True when a usable token is present."""
        try:
            active = self._loader.get(SECRET_NAME)
        except SecretNotAvailableError:
            return False
        except SecretIntegrityError:
            log.exception("telegram secret failed its integrity check")
            return False
        if active.version != self._secret_version:
            self._secret = TelegramSecret.model_validate(active.values)
            self._secret_version = active.version
            self._processor.update_allowlist(self._secret.allowed_user_ids)
            self._offset = None
            log.info("telegram secret loaded", extra={"version": active.version})
        return self._secret is not None

    async def run(self, stop: asyncio.Event) -> int:
        """Run the vault owner, the alert monitor and the bot loop until `stop` is set.

        Returns 0 after `stop`, 1 as soon as one of the tasks ends on its own (crash or unexpected
        return): the caller exits non-zero so the container restarts instead of serving `/metrics` with
        a dead bot.
        """
        tasks = [
            asyncio.create_task(self._owner.run(stop), name="ops-owner"),
            asyncio.create_task(self._monitor.run(stop), name="alert-monitor"),
            asyncio.create_task(self._bot_loop(stop), name="telegram-bot"),
        ]
        stopped = asyncio.create_task(stop.wait(), name="stop")
        try:
            done, _ = await asyncio.wait([stopped, *tasks], return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in (stopped, *tasks):
                task.cancel()
            await asyncio.gather(stopped, *tasks, return_exceptions=True)
        if stopped in done:
            return 0
        for task in done:
            error = None if task.cancelled() else task.exception()
            log.critical(
                "service task ended unexpectedly, exiting",
                extra={"task": task.get_name()},
                exc_info=(type(error), error, error.__traceback__) if error is not None else None,
            )
        return 1

    async def _bot_loop(self, stop: asyncio.Event) -> None:
        async with httpx.AsyncClient(timeout=40.0) as http:
            while not stop.is_set():
                try:
                    await self._iteration(http, stop)
                except Exception:
                    metrics.TELEGRAM_LOOP_ERRORS.labels(stage="iteration").inc()
                    log.exception("telegram bot iteration failed")
                    await self._sleep(stop, self.ITERATION_ERROR_SLEEP_S)
                metrics.TELEGRAM_POLL_HEARTBEAT.set(time.time())

    async def _iteration(self, http: httpx.AsyncClient, stop: asyncio.Event) -> None:
        """One loop turn: secret refresh, webhook check and alert dispatch when due, then one poll.

        The webhook check and the dispatch are guarded separately so a failure there never keeps the
        poll (and therefore `/kill`) from running.
        """
        now = time.monotonic()
        if now - self._last_secret_check >= self.SECRET_REFRESH_EVERY_S or self._secret is None:
            self._last_secret_check = now
            if not await asyncio.to_thread(self._load_secret):
                await self._sleep(stop, 15.0)
                return
        assert self._secret is not None
        client = TelegramClient(self._secret.bot_token, http=http)
        if now - self._last_webhook_check >= self.WEBHOOK_CHECK_EVERY_S:
            self._last_webhook_check = now
            await self._guarded("webhook", self._check_webhook(client))
        if now - self._last_dispatch >= self.DISPATCH_EVERY_S:
            self._last_dispatch = now
            await self._guarded(
                "dispatch", self._dispatcher.dispatch_once(client, self._secret.allowed_user_ids)
            )
        await self._poll(client, stop)

    @staticmethod
    async def _guarded(stage: str, step: Coroutine[Any, Any, object]) -> None:
        try:
            await step
        except Exception:
            metrics.TELEGRAM_LOOP_ERRORS.labels(stage=stage).inc()
            log.exception("telegram bot step failed", extra={"stage": stage})

    async def _poll(self, client: TelegramClient, stop: asyncio.Event) -> None:
        try:
            updates = await client.get_updates(self._offset, timeout_s=int(self.DISPATCH_EVERY_S))
        except TelegramApiError as exc:
            metrics.TELEGRAM_POLL_ERRORS.inc()
            self._poll_failures += 1
            log.warning(
                "getUpdates failed", extra={"error": exc.description, "failures": self._poll_failures}
            )
            if self._poll_failures == self.POLL_ERRORS_BEFORE_ALERT:
                await asyncio.to_thread(self._alert_polling, exc.description)
            if exc.error_code == 409:
                await self._check_webhook(client)
            await self._sleep(stop, min(60.0, max(2.0 * self._poll_failures, exc.retry_after or 0.0)))
            return
        if self._poll_failures >= self.POLL_ERRORS_BEFORE_ALERT:
            try:
                await asyncio.to_thread(self._resolve, "telegram_polling", "poll")
            except Exception:  # keep handling messages; the resolution is retried on the next poll
                metrics.TELEGRAM_LOOP_ERRORS.labels(stage="poll").inc()
                log.exception("telegram_polling alert not resolved")
            else:
                self._poll_failures = 0
        else:
            self._poll_failures = 0
        for item in updates:
            update_id = item.get("update_id")
            if isinstance(update_id, int):
                self._offset = update_id + 1
            msg = parse_update(item)
            if msg is None:
                continue
            try:
                reply = await self._processor.handle(msg)
            except Exception:  # handle() already answers errors; this is the last line of defense
                metrics.TELEGRAM_LOOP_ERRORS.labels(stage="message").inc()
                log.exception("telegram message failed", extra={"from_id": msg.user_id})
                reply = INTERNAL_ERROR_REPLY if self._processor.is_authorized(msg) else None
            if reply:
                try:
                    await client.send_message(msg.chat_id, reply)
                except TelegramApiError as exc:
                    log.warning("reply failed", extra={"error": exc.description})

    async def _check_webhook(self, client: TelegramClient) -> None:
        try:
            info = await client.get_webhook_info()
        except TelegramApiError as exc:
            log.warning("getWebhookInfo failed", extra={"error": exc.description})
            return
        url_set = bool(info.get("url"))
        metrics.TELEGRAM_WEBHOOK_SET.set(1 if url_set else 0)
        if url_set:
            await asyncio.to_thread(self._alert_webhook)
        else:
            await asyncio.to_thread(self._resolve, "telegram_webhook", "webhook")

    def _alert_webhook(self) -> None:
        with transaction(self._factory) as session:
            raise_alert(
                session,
                kind="telegram_webhook",
                severity="critical",
                title="A webhook is set on the Telegram bot token",
                detail="getUpdates no longer receives commands, so /kill would not arrive. Assume the token "
                "leaked: rotate it on the console (API keys) and use hdt-kill over SSH if needed.",
                dedupe_key="webhook",
                service=SERVICE,
            )

    def _alert_polling(self, description: str) -> None:
        with transaction(self._factory) as session:
            raise_alert(
                session,
                kind="telegram_polling",
                severity="warning",
                title="Telegram polling keeps failing",
                detail=f"getUpdates failed {self.POLL_ERRORS_BEFORE_ALERT} times in a row: {description}",
                dedupe_key="poll",
                service=SERVICE,
            )

    def _resolve(self, kind: Literal["telegram_polling", "telegram_webhook"], key: str) -> None:
        with transaction(self._factory) as session:
            resolve_alert(session, kind=kind, dedupe_key=key)

    @staticmethod
    async def _sleep(stop: asyncio.Event, seconds: float) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=seconds)


async def _amain() -> int:
    settings = static_config().settings
    configure_logging(SERVICE, settings.logging.level)
    metrics.serve_metrics(SERVICE)
    engine = make_engine(pool_size=5)
    factory = make_session_factory(engine)
    redis: Redis = connect_redis(require_env_value("HDT_REDIS_URL"))
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)
    private_key = load_private_key("ops")
    owner = OwnerWorker(
        scope="ops",
        redis=redis,
        session_factory=factory,
        private_key=private_key,
        checks=OPS_SECRET_CHECKS,
        consumer=SERVICE,
        block_ms=settings.streams.block_ms,
        reclaim_idle_ms=settings.streams.reclaim_idle_ms,
        result_maxlen=settings.streams.maxlen.get(Stream.SECRET_TEST_RESULT.value),
    )
    monitor = AlertMonitor(
        factory,
        credit_alert_fraction=settings.cmc.alert_projection_frac,
        prometheus_url=read_env_value(PROMETHEUS_URL_ENV),
    )
    service = TelegramBotService(
        session_factory=factory,
        redis=redis,
        loader=SecretLoader("ops", factory, private_key),
        owner=owner,
        monitor=monitor,
        totp_secret=read_env_value(TOTP_SECRET_ENV),
        controls_maxlen=settings.streams.maxlen.get(Stream.CONTROLS.value),
    )
    try:
        return await service.run(stop)
    finally:
        await redis.aclose()
        engine.dispose()


def main() -> None:
    """Exit code 1 when a service task died (the container restarts), 0 after SIGTERM / SIGINT."""
    raise SystemExit(asyncio.run(_amain()))


if __name__ == "__main__":
    main()
