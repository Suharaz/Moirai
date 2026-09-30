"""Operational alerts: the Postgres outbox API, the alert catalog and the rule monitor.

Raising (every service): `raise_alert(session, ...)` inserts into `alerts` inside the caller's own
transaction, so an alert is durable exactly when the state change that caused it commits. At most one
unresolved alert exists per `(kind, dedupe_key)`: a repeated condition of the same or a lower severity
returns `None` instead of paging again, while a higher severity escalates the open alert (new severity,
title and detail, `escalated_at`, delivered again to every recipient), so a routine warning can never mask
a critical alert of the same key. The insert-or-escalate runs in the `SECURITY DEFINER` SQL function
`alerts_raise` (migration 0006_ops): raising roles cannot write the delivery state of any alert. A raiser
may only resolve (`resolve_alert`), and a resolved alert stays resolved. Titles and details are passed
through the log redactor first because they are forwarded to Telegram.

Delivery (telegram-bot only): `due_notifications` lists alerts to send (new ones, then resolutions),
`record_delivery` / `mark_notified` / `mark_notify_failed` record the outcome. Delivery is at-least-once
and tracked per recipient and message (`alert_deliveries`): a retry only goes to the recipients still
missing the message. A recipient Telegram refuses for good (400 chat not found, 403 bot blocked) is
recorded `unreachable` for that message and skipped; a notification is closed once every reachable
recipient has it, and a resolution only goes to the recipients that received the raised message. A
failed attempt backs off exponentially (`NOTIFY_BACKOFF_BASE` doubling up to `NOTIFY_BACKOFF_MAX`, or
longer when Telegram answers 429 with `retry_after`). Critical alerts are retried until delivered;
warning and info notifications are abandoned once older than `NOTIFY_MAX_AGE`.

Rules (`AlertMonitor`, run by the telegram-bot service): conditions that no single service owns are
evaluated here and turned into alerts that open and close with the condition:
- `route_dead`: a recorder REST route more than `ROUTE_DEAD_CYCLES` (3) cadences late (`route_health`);
- `cmc_credit_projection`: projected CMC cycle credits above `cmc.alert_projection_frac` (85 %) of the
  cycle limit, computed like the recorder's credit meter from `cmc_key_info` + `cmc_credit_usage`;
- `cmc_halt`: CMC hard cap / IP limit halt state (1009 / 1010 / 1011) from `cmc_key_info`;
- `disk_usage`: lake disk usage at or above 80 % (`lake_stats`);
- Prometheus alert rules (`deploy/prometheus/alerts.yml`): every firing rule carrying an `hdt_kind` label
  (host RAM > 85 %, host disk, fetcher blocks, service down, stale public snapshot, stale backups) is
  mirrored into the outbox and resolved when it stops firing; an unreachable Prometheus raises
  `prometheus_unreachable`;
- episode close: the event alerts raised once per episode that nobody resolves (`EPISODE_KINDS`:
  `intent_signature`, `integrity_error`, `price_crosscheck`, `liquidation_distance`, `config_invalid`,
  `entry_refused`, `weekly_report`, `council_event_failed`) are closed `EPISODE_CLOSE_AFTER`
  (24 h) after delivery, silently (no RESOLVED message), so the open-alert count on `/status` and the
  console does not grow for good.

Alerts owned by other services are raised there: `reconcile_mismatch`, `stop_invariant`, `kill_switch`,
`daily_loss_warning` (-1.5 % of day-start equity), `intent_signature`, `integrity_error`,
`account_state_stale`, `price_crosscheck`, `ip_banned`, `liquidation_distance`, `namespace_error`,
`config_invalid`, `entry_refused` (phase 09); `telegram_webhook`, `telegram_polling`, `telegram_totp_lockout`,
`telegram_recipient_unreachable` (telegram-bot); `claims_audit` and `weekly_report` (scorer, the latter by
`scripts/weekly_report.py`, phase 11); `council_event_failed` (council).

CLI: `python -m hdt.ops.alerts test [--kind KIND ...]` raises one test alert per kind (all kinds by
default). Test alerts are stored already resolved, so they never show as open on the console and never
block a real alert, but the bot delivers them like any other alert.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import logging
import uuid
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Final, Literal, cast, get_args

import httpx
from sqlalchemy import and_, func, or_, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import DBAPIError, ProgrammingError
from sqlalchemy.orm import Session, sessionmaker

from hdt.core.clock import utcnow
from hdt.db.models.ingest import CmcKeyInfoRow, LakeStatsRow, RouteHealthRow
from hdt.db.models.ops import ACCOUNTS, AlertDeliveryRow, AlertRow
from hdt.db.session import transaction
from hdt.ingest.credit_governor import KeyInfo
from hdt.ingest.credit_meter import RUN_RATE_DAYS, project_cycle, recent_daily_credits
from hdt.vault.redact import redact

log = logging.getLogger(__name__)

AlertKind = Literal[
    "route_dead",
    "cmc_credit_projection",
    "cmc_halt",
    "reconcile_mismatch",
    "stop_invariant",
    "kill_switch",
    "daily_loss_warning",
    "intent_signature",
    "integrity_error",
    "account_state_stale",
    "price_crosscheck",
    "ip_banned",
    "liquidation_distance",
    "namespace_error",
    "config_invalid",
    "entry_refused",
    "fetcher_blocks",
    "disk_usage",
    "ram_usage",
    "service_down",
    "public_snapshot_stale",
    "backup_stale",
    "telegram_webhook",
    "telegram_polling",
    "telegram_totp_lockout",
    "telegram_recipient_unreachable",
    "prometheus_unreachable",
    "claims_audit",
    "weekly_report",
    "council_event_failed",
]
Severity = Literal["info", "warning", "critical"]
Phase = Literal["raised", "resolved"]
Outcome = Literal["delivered", "unreachable"]

ALERT_KINDS: Final[frozenset[str]] = frozenset(get_args(AlertKind))
SEVERITY_VALUES: Final[frozenset[str]] = frozenset(get_args(Severity))
TITLE_MAX: Final[int] = 200
DETAIL_MAX: Final[int] = 2000
DEDUPE_MAX: Final[int] = 200
NOTIFY_BACKOFF_BASE: Final[timedelta] = timedelta(seconds=5)
NOTIFY_BACKOFF_MAX: Final[timedelta] = timedelta(minutes=5)
NOTIFY_MAX_AGE: Final[timedelta] = timedelta(hours=24)  # warning / info only; critical never gives up

ROUTE_DEAD_CYCLES: Final[int] = 3  # phase 10: "recorder route dead > 3 cycles"
DISK_WARN_FRACTION: Final[float] = 0.80
DISK_CRITICAL_FRACTION: Final[float] = 0.90
ROUTE_STATUSES_NOT_DEAD: Final[tuple[str, ...]] = ("disabled", "shed", "pending", "halted")
PROMETHEUS_FAILURES_BEFORE_ALERT: Final[int] = 3
PROM_PREFIX: Final[str] = "prom:"
MONITOR_SERVICE: Final[str] = "ops-monitor"
# Event alerts raised once per episode (intent, decision, symbol-day, config version) that no service ever
# resolves: `AlertMonitor` closes them `EPISODE_CLOSE_AFTER` after delivery, without a RESOLVED message.
EPISODE_KINDS: Final[tuple[str, ...]] = (
    "intent_signature",
    "integrity_error",
    "price_crosscheck",
    "liquidation_distance",
    "config_invalid",
    "entry_refused",
    "weekly_report",
    "council_event_failed",
)
EPISODE_CLOSE_AFTER: Final[timedelta] = timedelta(hours=24)


@dataclass(frozen=True)
class AlertKindSpec:
    kind: AlertKind
    severity: Severity
    raised_by: str
    description: str


ALERT_CATALOG: Final[tuple[AlertKindSpec, ...]] = (
    AlertKindSpec("route_dead", "warning", "ops-monitor", "Recorder route more than 3 cycles late"),
    AlertKindSpec(
        "cmc_credit_projection", "warning", "ops-monitor", "Projected CMC credits above 85% of the cycle"
    ),
    AlertKindSpec("cmc_halt", "critical", "ops-monitor", "CMC daily cap, monthly cap or IP limit halt"),
    AlertKindSpec("reconcile_mismatch", "critical", "execution", "Reconcile found a ledger mismatch"),
    AlertKindSpec("stop_invariant", "critical", "execution", "A position is missing its exchange STOP"),
    AlertKindSpec("kill_switch", "critical", "execution", "Kill switch engaged"),
    AlertKindSpec("daily_loss_warning", "warning", "execution", "Daily loss reached -1.5%"),
    AlertKindSpec("intent_signature", "critical", "execution", "OrderIntent signature rejected"),
    AlertKindSpec("integrity_error", "critical", "risk", "Integrity check failed"),
    AlertKindSpec("account_state_stale", "warning", "risk", "AccountState older than 30 s"),
    AlertKindSpec("price_crosscheck", "critical", "risk", "Mark price disagrees with the cross-check source"),
    AlertKindSpec("ip_banned", "critical", "execution", "Binance banned the IP (HTTP 418)"),
    AlertKindSpec("liquidation_distance", "critical", "risk", "Liquidation price too close to the STOP"),
    AlertKindSpec("namespace_error", "critical", "execution", "Order id outside the account namespace"),
    AlertKindSpec(
        "config_invalid", "critical", "execution, risk", "Stored config version outside the ceilings: clipped"
    ),
    AlertKindSpec(
        "entry_refused",
        "warning",
        "execution",
        "An entry was not sent: the mark had invalidated its levels, or no mark was available",
    ),
    AlertKindSpec("fetcher_blocks", "warning", "prometheus", "Abnormal number of fetcher blocks"),
    AlertKindSpec("disk_usage", "warning", "ops-monitor", "Disk usage at or above 80%"),
    AlertKindSpec("ram_usage", "warning", "prometheus", "Host memory usage above 85%"),
    AlertKindSpec("service_down", "critical", "prometheus", "A service stopped answering metrics scrapes"),
    AlertKindSpec("public_snapshot_stale", "warning", "prometheus", "Public snapshot not refreshed"),
    AlertKindSpec("backup_stale", "critical", "prometheus", "Backups or WAL shipping are behind"),
    AlertKindSpec("telegram_webhook", "critical", "telegram-bot", "A webhook is set on the bot token"),
    AlertKindSpec("telegram_polling", "warning", "telegram-bot", "Telegram polling keeps failing"),
    AlertKindSpec(
        "telegram_totp_lockout", "critical", "telegram-bot", "Too many wrong TOTP codes: /resume locked"
    ),
    AlertKindSpec(
        "telegram_recipient_unreachable",
        "warning",
        "telegram-bot",
        "An allowed Telegram user cannot receive alerts (chat not found or bot blocked)",
    ),
    AlertKindSpec("prometheus_unreachable", "warning", "ops-monitor", "Prometheus API unreachable"),
    AlertKindSpec(
        "claims_audit", "warning", "scorer", "An agent's rejected-claim rate exceeded 5% over 7 days"
    ),
    AlertKindSpec("weekly_report", "info", "scorer", "Weekly shadow / go-live report (phase 11)"),
    AlertKindSpec("council_event_failed", "warning", "council", "A council meeting failed permanently"),
)
KIND_SPECS: Final[dict[str, AlertKindSpec]] = {spec.kind: spec for spec in ALERT_CATALOG}
if set(KIND_SPECS) != ALERT_KINDS:  # pragma: no cover - import-time consistency guard
    raise RuntimeError("ALERT_CATALOG must describe every AlertKind exactly once")
if not ALERT_KINDS.issuperset(EPISODE_KINDS):  # pragma: no cover - import-time consistency guard
    raise RuntimeError("EPISODE_KINDS must be alert kinds")


class AlertInputError(ValueError):
    """An alert argument is outside the catalog (unknown kind, severity or account)."""


def _clean(value: str, limit: int) -> str:
    value = redact(value.strip())
    return value if len(value) <= limit else value[: limit - 3] + "..."


def _validate(kind: str, severity: str, account: str | None, dedupe_key: str) -> None:
    if kind not in ALERT_KINDS:
        raise AlertInputError(f"unknown alert kind {kind!r}")
    if severity not in SEVERITY_VALUES:
        raise AlertInputError(f"unknown alert severity {severity!r}")
    if account is not None and account not in ACCOUNTS:
        raise AlertInputError(f"unknown account {account!r}")
    if len(dedupe_key) > DEDUPE_MAX:
        raise AlertInputError(f"dedupe_key longer than {DEDUPE_MAX} characters")


_RAISE_SQL = text(
    "SELECT alerts_raise(CAST(:alert_id AS text), CAST(:raised_at AS timestamptz), CAST(:kind AS text), "
    "CAST(:severity AS text), CAST(:title AS text), CAST(:detail AS text), CAST(:dedupe_key AS text), "
    "CAST(:service AS text), CAST(:account AS text))"
)


def raise_alert(
    session: Session,
    *,
    kind: AlertKind,
    severity: Severity,
    title: str,
    detail: str | None = None,
    dedupe_key: str = "",
    service: str,
    account: str | None = None,
    emit_log: bool = True,
) -> str | None:
    """Insert an alert in the caller's transaction; returns its id, `None` when it is already open.

    A raise whose severity is higher than the open alert of the same `(kind, dedupe_key)` escalates that
    alert instead: severity, title and detail are replaced, `escalated_at` is set and its delivery is
    queued again (every recipient gets the new message, see `due_notifications`), and the open alert's id
    is returned. The same or a lower severity stays deduplicated.

    `emit_log=False` leaves the log record to the caller: `hdt.core.alerts.alert` logs every alert it raises
    itself (also when the outbox is unavailable), so one raise is logged exactly once.

    The write runs in the `SECURITY DEFINER` function `alerts_raise` (migration 0006_ops): raising roles
    hold EXECUTE on it but no INSERT on `alerts` and no UPDATE of its text or delivery columns, so a
    compromised service can page but never silence, defer or downgrade another service's alert.
    """
    _validate(kind, severity, account, dedupe_key)
    if not title.strip():
        raise AlertInputError("alert title must not be empty")
    alert_id = uuid.uuid4().hex
    stored: str | None = session.execute(
        _RAISE_SQL,
        {
            "alert_id": alert_id,
            "raised_at": utcnow(),
            "kind": kind,
            "severity": severity,
            "title": _clean(title, TITLE_MAX),
            "detail": _clean(detail, DETAIL_MAX) if detail else None,
            "dedupe_key": dedupe_key,
            "service": service,
            "account": account,
        },
    ).scalar_one()
    if stored is not None and emit_log:
        log.warning(
            "alert raised" if stored == alert_id else "alert escalated",
            extra={"alert_kind": kind, "severity": severity, "dedupe_key": dedupe_key, "account": account},
        )
    return stored


def resolve_alert(session: Session, *, kind: AlertKind, dedupe_key: str = "", emit_log: bool = True) -> int:
    """Resolve the open alert `(kind, dedupe_key)`; returns the number of rows resolved (0 or 1).

    `emit_log=False` leaves the log record to the caller (`hdt.core.alerts.resolve` logs it itself)."""
    if kind not in ALERT_KINDS:
        raise AlertInputError(f"unknown alert kind {kind!r}")
    result = session.execute(
        update(AlertRow)
        .where(
            AlertRow.kind == kind,
            AlertRow.dedupe_key == dedupe_key,
            AlertRow.resolved_at.is_(None),
            AlertRow.is_test.is_(False),
        )
        .values(resolved_at=utcnow())
    )
    count = int(result.rowcount or 0)  # type: ignore[attr-defined]
    if count and emit_log:
        log.info("alert resolved", extra={"alert_kind": kind, "dedupe_key": dedupe_key})
    return count


def open_dedupe_keys(session: Session, kind: str) -> set[str]:
    rows = session.scalars(
        select(AlertRow.dedupe_key).where(
            AlertRow.kind == kind, AlertRow.resolved_at.is_(None), AlertRow.is_test.is_(False)
        )
    )
    return set(rows)


def raise_test_alerts(session: Session, *, service: str, kinds: Iterable[str] | None = None) -> list[str]:
    """One test alert per kind (born resolved, delivered like a real alert); returns the alert ids."""
    selected = list(kinds) if kinds is not None else [spec.kind for spec in ALERT_CATALOG]
    now = utcnow()
    ids: list[str] = []
    for kind in selected:
        spec = KIND_SPECS.get(kind)
        if spec is None:
            raise AlertInputError(f"unknown alert kind {kind!r}")
        alert_id = uuid.uuid4().hex
        session.add(
            AlertRow(
                alert_id=alert_id,
                raised_at=now,
                kind=spec.kind,
                severity=spec.severity,
                title=f"Test alert: {spec.description}",
                detail=f"Delivery test for alert type {spec.kind} (normally raised by {spec.raised_by}). "
                "No action needed.",
                dedupe_key=f"test:{alert_id}",
                service=service,
                account=None,
                is_test=True,
                resolved_at=now,
                notified_at=None,
                resolution_notified_at=None,
                notify_attempts=0,
                last_notify_error=None,
                next_attempt_at=None,
                escalated_at=None,
            )
        )
        ids.append(alert_id)
    session.flush()
    return ids


# --------------------------------------------------------------------------- delivery


@dataclass(frozen=True)
class Notification:
    alert_id: str
    phase: Phase
    raised_at: datetime
    kind: str
    severity: str
    title: str
    detail: str | None
    service: str
    account: str | None
    is_test: bool
    resolved_at: datetime | None
    escalated_at: datetime | None = None  # last escalation to the current severity; None: never escalated
    delivered_to: frozenset[int] = frozenset()  # chat ids that already received this notification
    unreachable: frozenset[int] = frozenset()  # chat ids that answered it with a terminal 400 / 403
    # Resolved phase only: the chat ids that received the raised message (the only ones told it resolved).
    audience: frozenset[int] | None = None


@dataclass(frozen=True)
class _Delivery:
    phase: str
    chat_id: int
    severity: str
    outcome: str


def _notification(row: AlertRow, phase: Phase, deliveries: Iterable[_Delivery]) -> Notification:
    """`delivered_to` / `unreachable` of the raised phase count only the message of the current severity:
    after an escalation every recipient is due the new message again."""
    records = list(deliveries)
    current = [d for d in records if d.phase == phase and (phase == "resolved" or d.severity == row.severity)]
    return Notification(
        alert_id=row.alert_id,
        phase=phase,
        raised_at=row.raised_at,
        kind=row.kind,
        severity=row.severity,
        title=row.title,
        detail=row.detail,
        service=row.service,
        account=row.account,
        is_test=row.is_test,
        resolved_at=row.resolved_at,
        escalated_at=row.escalated_at,
        delivered_to=frozenset(d.chat_id for d in current if d.outcome == "delivered"),
        unreachable=frozenset(d.chat_id for d in current if d.outcome == "unreachable"),
        audience=None
        if phase == "raised"
        else frozenset(d.chat_id for d in records if d.phase == "raised" and d.outcome == "delivered"),
    )


def due_notifications(session: Session, *, limit: int = 20) -> list[Notification]:
    """Due undelivered alerts oldest first, then due resolutions of delivered real alerts.

    Due means past its `next_attempt_at` backoff; warning / info notifications older than
    `NOTIFY_MAX_AGE` are no longer due (critical ones always are until delivered).
    """
    now = utcnow()
    fresh = now - NOTIFY_MAX_AGE
    due = or_(AlertRow.next_attempt_at.is_(None), AlertRow.next_attempt_at <= now)
    raised = session.scalars(
        select(AlertRow)
        .where(
            AlertRow.notified_at.is_(None),
            due,
            or_(AlertRow.severity == "critical", AlertRow.raised_at >= fresh),
        )
        .order_by(AlertRow.raised_at, AlertRow.alert_id)
        .limit(limit)
    ).all()
    rows: list[tuple[AlertRow, Phase]] = [(row, "raised") for row in raised]
    remaining = limit - len(rows)
    if remaining > 0:
        resolved = session.scalars(
            select(AlertRow)
            .where(
                AlertRow.is_test.is_(False),
                AlertRow.notified_at.is_not(None),
                AlertRow.resolved_at.is_not(None),
                AlertRow.resolution_notified_at.is_(None),
                due,
                or_(AlertRow.severity == "critical", AlertRow.resolved_at >= fresh),
            )
            .order_by(AlertRow.resolved_at, AlertRow.alert_id)
            .limit(remaining)
        ).all()
        rows.extend((row, "resolved") for row in resolved)
    deliveries: dict[str, list[_Delivery]] = {}
    if rows:
        for record in session.execute(
            select(
                AlertDeliveryRow.alert_id,
                AlertDeliveryRow.phase,
                AlertDeliveryRow.chat_id,
                AlertDeliveryRow.severity,
                AlertDeliveryRow.outcome,
            ).where(AlertDeliveryRow.alert_id.in_({row.alert_id for row, _ in rows}))
        ):
            deliveries.setdefault(record.alert_id, []).append(
                _Delivery(record.phase, int(record.chat_id), record.severity, record.outcome)
            )
    return [_notification(row, phase, deliveries.get(row.alert_id, ())) for row, phase in rows]


def record_delivery(
    session: Session,
    alert_id: str,
    phase: Phase,
    chat_id: int,
    *,
    severity: str,
    outcome: Outcome = "delivered",
) -> None:
    """Record what happened to the `severity` message of this phase for `chat_id` (idempotent).

    `unreachable`: Telegram answered 400 / 403 (chat not found, bot blocked, user deactivated). That
    recipient is skipped for this message; the next message tries it again.
    """
    insert = pg_insert(AlertDeliveryRow).values(
        alert_id=alert_id,
        phase=phase,
        chat_id=chat_id,
        severity=severity,
        outcome=outcome,
        recorded_at=utcnow(),
    )
    session.execute(
        insert.on_conflict_do_update(
            index_elements=["alert_id", "phase", "chat_id", "severity"],
            set_={"outcome": insert.excluded.outcome, "recorded_at": insert.excluded.recorded_at},
        )
    )


def mark_notified(session: Session, alert_id: str, phase: Phase, *, severity: str | None = None) -> bool:
    """Every reachable recipient has the notification: close this phase and reset the retry state.

    `severity` is the severity of the message that was sent: when the alert was escalated meanwhile the
    row is left open (the escalated message is still due) and False is returned.
    """
    column = AlertRow.notified_at if phase == "raised" else AlertRow.resolution_notified_at
    stmt = update(AlertRow).where(AlertRow.alert_id == alert_id)
    if severity is not None:
        stmt = stmt.where(AlertRow.severity == severity)
    result = session.execute(
        stmt.values(
            {column: utcnow(), "last_notify_error": None, "notify_attempts": 0, "next_attempt_at": None}
        )
    )
    return bool(result.rowcount)  # type: ignore[attr-defined]


def notify_backoff(attempts: int, retry_after: timedelta | None = None) -> timedelta:
    """Delay before the next attempt after `attempts` consecutive failures (Telegram 429 wins if longer)."""
    exponent = min(max(attempts - 1, 0), 16)
    delay = min(NOTIFY_BACKOFF_BASE * (1 << exponent), NOTIFY_BACKOFF_MAX)
    return max(delay, retry_after) if retry_after is not None else delay


def mark_notify_failed(
    session: Session,
    alert_id: str,
    phase: Phase,
    error: str,
    *,
    retry_after: timedelta | None = None,
    severity: str | None = None,
) -> datetime | None:
    """Count a failed attempt and schedule the next one; returns its time.

    None when the alert does not exist, or (with `severity`, the severity of the message that failed)
    when it was escalated meanwhile: the escalated message is due at once, never behind this backoff.
    """
    row = session.execute(
        select(AlertRow.notify_attempts, AlertRow.severity, AlertRow.raised_at, AlertRow.resolved_at).where(
            AlertRow.alert_id == alert_id
        )
    ).one_or_none()
    if row is None or (severity is not None and row.severity != severity):
        return None
    attempts = int(row.notify_attempts) + 1
    now = utcnow()
    next_at = now + notify_backoff(attempts, retry_after)
    session.execute(
        update(AlertRow)
        .where(AlertRow.alert_id == alert_id, AlertRow.severity == row.severity)
        .values(notify_attempts=attempts, last_notify_error=_clean(error, 500), next_attempt_at=next_at)
    )
    anchor = row.resolved_at if phase == "resolved" and row.resolved_at is not None else row.raised_at
    if row.severity != "critical" and next_at > anchor + NOTIFY_MAX_AGE:
        log.error(
            "alert notification abandoned",
            extra={"alert_id": alert_id, "phase": phase, "attempts": attempts, "error": _clean(error, 200)},
        )
    return next_at


def format_notification(note: Notification) -> str:
    """Plain-text Telegram message (no parse mode, so alert text can never inject markup)."""
    prefix = "TEST - " if note.is_test else ""
    if note.phase == "resolved":
        head = f"{prefix}RESOLVED - {note.title}"
    else:
        head = f"{prefix}[{note.severity.upper()}] {note.title}"
    if note.account:
        head += f" ({note.account})"
    lines = [head]
    if note.detail and note.phase == "raised":
        lines.append(note.detail)
    service = f"kind={note.kind} service={note.service}"
    if note.phase == "resolved" and note.resolved_at:
        lines.append(f"{service} at {_utc_text(note.resolved_at)} UTC")
    elif note.phase == "raised" and note.escalated_at:
        # The escalated message is new: its time is the escalation, not the first (lower severity) raise.
        lines.append(
            f"{service} escalated at {_utc_text(note.escalated_at)} UTC "
            f"(first raised {_utc_text(note.raised_at)} UTC)"
        )
    else:
        lines.append(f"{service} at {_utc_text(note.raised_at)} UTC")
    return "\n".join(lines)


def _utc_text(at: datetime) -> str:
    return at.strftime("%Y-%m-%d %H:%M:%S")


# --------------------------------------------------------------------------- rule monitor


@dataclass(frozen=True)
class Firing:
    severity: Severity
    title: str
    detail: str | None = None
    account: str | None = None


def apply_rule(
    session: Session,
    kind: AlertKind,
    firing: Mapping[str, Firing],
    *,
    service: str,
    owns: Callable[[str], bool],
) -> tuple[int, int]:
    """Raise every firing key, resolve open keys this rule owns that no longer fire; (raised, resolved)."""
    raised = 0
    for key, alert in firing.items():
        if raise_alert(
            session,
            kind=kind,
            severity=alert.severity,
            title=alert.title,
            detail=alert.detail,
            dedupe_key=key,
            service=service,
            account=alert.account,
        ):
            raised += 1
    resolved = 0
    for key in open_dedupe_keys(session, kind):
        if owns(key) and key not in firing:
            resolved += resolve_alert(session, kind=kind, dedupe_key=key)
    return raised, resolved


def close_stale_episodes(session: Session, max_age: timedelta) -> int:
    """Close the open `EPISODE_KINDS` alerts delivered more than `max_age` ago; returns how many.

    Nothing ever resolves them (the episode is over once reported), so without this the open-alert count
    grows for good. `resolution_notified_at` is set in the same update: closing sends no RESOLVED message.
    An undelivered warning / info is closed once past both `max_age` and `NOTIFY_MAX_AGE` (it is no longer
    delivered); an undelivered critical stays open until it is delivered.
    """
    now = utcnow()
    result = session.execute(
        update(AlertRow)
        .where(
            AlertRow.kind.in_(EPISODE_KINDS),
            AlertRow.resolved_at.is_(None),
            AlertRow.is_test.is_(False),
            or_(
                AlertRow.notified_at <= now - max_age,
                and_(
                    AlertRow.notified_at.is_(None),
                    AlertRow.severity != "critical",
                    AlertRow.raised_at <= now - max(max_age, NOTIFY_MAX_AGE),
                ),
            ),
        )
        .values(resolved_at=now, resolution_notified_at=now)
    )
    count = int(result.rowcount or 0)  # type: ignore[attr-defined]
    if count:
        log.info("stale episode alerts closed", extra={"count": count})
    return count


def _not_prom(key: str) -> bool:
    return not key.startswith(PROM_PREFIX)


def dead_routes(session: Session) -> dict[str, Firing]:
    rows = session.scalars(
        select(RouteHealthRow).where(
            RouteHealthRow.cycles_late > ROUTE_DEAD_CYCLES,
            RouteHealthRow.status.not_in(ROUTE_STATUSES_NOT_DEAD),
        )
    ).all()
    out: dict[str, Firing] = {}
    for row in rows:
        last = row.last_success_at.strftime("%Y-%m-%d %H:%M UTC") if row.last_success_at else "never"
        detail = (
            f"{row.source}/{row.route} ({row.cadence_label}) is {row.cycles_late} cycles late; "
            f"last success {last}."
        )
        if row.last_error:
            detail += f" Last error: {row.last_error}"
        out[f"route:{row.route_key}"] = Firing("warning", f"Recorder route {row.route_key} is dead", detail)
    return out


def _latest_key_info(session: Session) -> CmcKeyInfoRow | None:
    return session.scalars(select(CmcKeyInfoRow).order_by(CmcKeyInfoRow.checked_at.desc()).limit(1)).first()


def credit_projection(session: Session, alert_fraction: float) -> dict[str, Firing]:
    row = _latest_key_info(session)
    if row is None:
        return {}
    info = KeyInfo(
        checked_at=row.checked_at,
        credit_limit_cycle=row.credit_limit_cycle,
        credits_used_cycle=row.credits_used_cycle,
        credits_left_cycle=max(0, row.credit_limit_cycle - row.credits_used_cycle),
        cycle_end=row.cycle_end,
        credits_used_today=row.credits_used_today,
        rate_limit_minute=row.rate_limit_minute,
        daily_reset=None,
    )
    daily = recent_daily_credits(session, row.checked_at.date(), RUN_RATE_DAYS)
    projection = project_cycle(info, daily, row.checked_at)
    if projection.fraction <= alert_fraction:
        return {}
    detail = (
        f"Projected {round(projection.projected):,} of {projection.limit_cycle:,} credits this cycle "
        f"({projection.fraction:.1%}); used {projection.used_cycle:,}, run rate "
        f"{projection.run_rate_per_day:,.0f}/day, {projection.days_left:.1f} days left."
    )
    return {
        f"cycle:{row.cycle_end.date().isoformat()}": Firing(
            "warning", f"CMC credits projected at {projection.fraction:.0%} of the cycle", detail
        )
    }


def cmc_halt(session: Session) -> dict[str, Firing]:
    row = _latest_key_info(session)
    if row is None or row.halt_state is None:
        return {}
    detail = (
        f"The recorder stopped keyed CMC calls ({row.halt_state}). Binance-only degraded mode is active "
        "for held positions."
    )
    return {row.halt_state: Firing("critical", f"CMC halt: {row.halt_state}", detail)}


def lake_disk(session: Session) -> dict[str, Firing]:
    row = session.scalars(select(LakeStatsRow).order_by(LakeStatsRow.as_of.desc()).limit(1)).first()
    if row is None or row.disk_used_fraction is None or row.disk_used_fraction < DISK_WARN_FRACTION:
        return {}
    severity: Severity = "critical" if row.disk_used_fraction >= DISK_CRITICAL_FRACTION else "warning"
    detail = (
        f"Lake disk {row.disk_used_fraction:.1%} used (lake {row.lake_bytes / 1e9:,.1f} GB). "
        "Check raw depth retention and backups (runbook: disk full)."
    )
    return {"lake": Firing(severity, f"Lake disk at {row.disk_used_fraction:.0%}", detail)}


def _label_hash(labels: Mapping[str, str]) -> str:
    material = "\n".join(f"{k}={labels[k]}" for k in sorted(labels) if k != "severity")
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]


def prometheus_firing(payload: Mapping[str, Any]) -> dict[str, dict[str, Firing]]:
    """Map `/api/v1/alerts` firing alerts that carry an `hdt_kind` label to outbox alerts."""
    data = payload.get("data")
    alerts = data.get("alerts") if isinstance(data, dict) else None
    if payload.get("status") != "success" or not isinstance(alerts, list):
        raise ValueError("unexpected Prometheus /api/v1/alerts response")
    out: dict[str, dict[str, Firing]] = {}
    for alert in alerts:
        if not isinstance(alert, dict) or alert.get("state") != "firing":
            continue
        labels = {str(k): str(v) for k, v in (alert.get("labels") or {}).items()}
        annotations = {str(k): str(v) for k, v in (alert.get("annotations") or {}).items()}
        kind = labels.get("hdt_kind", "")
        if kind not in ALERT_KINDS:
            continue
        severity = labels.get("severity", "warning")
        sev = cast(Severity, severity if severity in SEVERITY_VALUES else "warning")
        title = annotations.get("summary") or labels.get("alertname") or kind
        key = f"{PROM_PREFIX}{labels.get('alertname', kind)}:{_label_hash(labels)}"
        out.setdefault(kind, {})[key] = Firing(sev, title, annotations.get("description"))
    return out


class AlertMonitor:
    """Evaluates the rule set on a schedule (one short transaction per rule)."""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        *,
        credit_alert_fraction: float,
        prometheus_url: str | None,
        http: httpx.AsyncClient | None = None,
        service: str = MONITOR_SERVICE,
        episode_close_after: timedelta = EPISODE_CLOSE_AFTER,
    ) -> None:
        if episode_close_after <= timedelta(0):
            raise ValueError("episode_close_after must be positive")
        self._factory = session_factory
        self._credit_fraction = credit_alert_fraction
        self._prom = prometheus_url.rstrip("/") if prometheus_url else None
        self._http = http
        self._service = service
        self._episode_close_after = episode_close_after
        self._prom_failures = 0
        self._unavailable_logged: set[str] = set()

    def _guarded(self, name: str, step: Callable[[Session], object]) -> None:
        try:
            with transaction(self._factory) as session:
                step(session)
            self._unavailable_logged.discard(name)
        except (ProgrammingError, DBAPIError) as exc:
            if name not in self._unavailable_logged:
                log.error("alert rule failed", extra={"rule": name, "error": type(exc).__name__})
                self._unavailable_logged.add(name)

    def _rule(self, name: str, kind: AlertKind, evaluate: Callable[[Session], dict[str, Firing]]) -> None:
        def step(session: Session) -> None:
            apply_rule(session, kind, evaluate(session), service=self._service, owns=_not_prom)

        self._guarded(name, step)

    def evaluate_database_rules(self) -> None:
        self._rule("route_dead", "route_dead", dead_routes)
        self._rule("cmc_credit_projection", "cmc_credit_projection", self._credit_rule)
        self._rule("cmc_halt", "cmc_halt", cmc_halt)
        self._rule("lake_disk", "disk_usage", lake_disk)
        self._guarded("episode_close", self._close_episodes)

    def _credit_rule(self, session: Session) -> dict[str, Firing]:
        return credit_projection(session, self._credit_fraction)

    def _close_episodes(self, session: Session) -> int:
        return close_stale_episodes(session, self._episode_close_after)

    async def sync_prometheus(self) -> None:
        if self._prom is None:
            return
        client = self._http or httpx.AsyncClient(timeout=10.0, trust_env=False)
        try:
            response = await client.get(f"{self._prom}/api/v1/alerts")
            response.raise_for_status()
            firing = prometheus_firing(response.json())
        except (httpx.HTTPError, ValueError) as exc:
            self._prom_failures += 1
            log.warning(
                "prometheus alerts unavailable",
                extra={"error": type(exc).__name__, "failures": self._prom_failures},
            )
            if self._prom_failures >= PROMETHEUS_FAILURES_BEFORE_ALERT:
                await asyncio.to_thread(self._prometheus_down)
            return
        finally:
            if self._http is None:
                await client.aclose()
        self._prom_failures = 0
        await asyncio.to_thread(self._apply_prometheus, firing)

    def _prometheus_down(self) -> None:
        with transaction(self._factory) as session:
            raise_alert(
                session,
                kind="prometheus_unreachable",
                severity="warning",
                title="Prometheus API unreachable",
                detail="Metric-based alerts (RAM, host disk, fetcher blocks, service down, public snapshot, "
                "backups) are not evaluated until Prometheus answers again.",
                dedupe_key="api",
                service=self._service,
            )

    def _apply_prometheus(self, firing: dict[str, dict[str, Firing]]) -> None:
        with transaction(self._factory) as session:
            resolve_alert(session, kind="prometheus_unreachable", dedupe_key="api")
            for kind in sorted(ALERT_KINDS):
                apply_rule(
                    session,
                    cast(AlertKind, kind),
                    firing.get(kind, {}),
                    service="prometheus",
                    owns=lambda key: key.startswith(PROM_PREFIX),
                )

    async def run_once(self) -> None:
        await asyncio.to_thread(self.evaluate_database_rules)
        await self.sync_prometheus()

    async def run(self, stop: asyncio.Event, interval_s: float = 60.0) -> None:
        while not stop.is_set():
            try:
                await self.run_once()
            except Exception:
                log.exception("alert monitor cycle failed")
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=interval_s)


def open_alert_count(session: Session) -> int:
    return int(
        session.scalar(
            select(func.count())
            .select_from(AlertRow)
            .where(AlertRow.resolved_at.is_(None), AlertRow.is_test.is_(False))
        )
        or 0
    )


# --------------------------------------------------------------------------- CLI


def main(argv: list[str] | None = None) -> int:
    from hdt.core.logging import configure_logging
    from hdt.db.session import make_engine, make_session_factory

    parser = argparse.ArgumentParser(prog="python -m hdt.ops.alerts", description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    test = sub.add_parser("test", help="raise one test alert per kind (delivered by the Telegram bot)")
    test.add_argument("--kind", action="append", choices=sorted(ALERT_KINDS), help="limit to these kinds")
    test.add_argument("--service", default="ops-cli")
    sub.add_parser("kinds", help="list the alert catalog")
    args = parser.parse_args(argv)
    configure_logging("ops-alerts")
    if args.command == "kinds":
        for spec in ALERT_CATALOG:
            print(f"{spec.kind:24} {spec.severity:9} {spec.raised_by:13} {spec.description}")
        return 0
    engine = make_engine(pool_size=1)
    try:
        with transaction(make_session_factory(engine)) as session:
            ids = raise_test_alerts(session, service=args.service, kinds=args.kind)
    finally:
        engine.dispose()
    print(f"raised {len(ids)} test alerts; the telegram-bot delivers them within one dispatch cycle")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
