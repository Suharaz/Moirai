"""Phase 10 tables: the operational alert outbox and the Telegram TOTP guards.

- `alerts` is written by every service through `hdt.ops.alerts.raise_alert` (inside the caller's own
  transaction, through the `SECURITY DEFINER` function `alerts_raise`) and delivered to Telegram by the
  `telegram-bot` service. At most one unresolved alert exists per `(kind, dedupe_key)`; a raise of a higher
  severity escalates it in place (`escalated_at`); test alerts are born resolved and never collide with
  real ones. A failed delivery sets `next_attempt_at` (exponential backoff, Telegram `retry_after` honored).
- `alert_deliveries` records, per recipient, every notification message (`raised` or `resolved`, at the
  alert's severity) it received (`delivered`) or that Telegram refused for good (`unreachable`: 400 chat
  not found, 403 bot blocked), so a retry only sends to the recipients still missing it and an escalated
  alert is sent again; `notified_at` / `resolution_notified_at` are set once every reachable recipient
  has it.
- `telegram_totp_state` stores the last accepted TOTP time step per TOTP seed (one seed is shared by every
  allowed user), so a code accepted for one user can never be replayed by another, even across restarts.
- `telegram_totp_failures` counts wrong TOTP codes per Telegram user and holds the `/resume` lockout.

Migration `0006_ops` creates the tables, the `alerts_raise` function, the `alerts_guard` trigger and the
per-role grants. `Base.metadata.create_all` databases (tests) get the same function and trigger from the
`after_create` listener below (`test_ops_alerts` checks both definitions match). The console reads `alerts`
(`hdt_console_ro`); the public publisher never does (operational alerts are never published).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Connection,
    ForeignKey,
    Index,
    Integer,
    Table,
    Text,
    event,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from hdt.db.base import Base

SEVERITIES = ("info", "warning", "critical")
ACCOUNTS = ("paper", "testnet", "live")
DELIVERY_PHASES = ("raised", "resolved")
DELIVERY_OUTCOMES = ("delivered", "unreachable")


def _in(column: str, values: tuple[str, ...]) -> str:
    return f"{column} IN ({', '.join(repr(v) for v in values)})"


class AlertRow(Base):
    """One operational alert; delivery and resolution state live on the same row (outbox pattern)."""

    __tablename__ = "alerts"
    __table_args__ = (
        CheckConstraint(_in("severity", SEVERITIES), name="severity"),
        CheckConstraint(f"account IS NULL OR {_in('account', ACCOUNTS)}", name="account"),
        CheckConstraint("notify_attempts >= 0", name="notify_attempts"),
        Index("ix_alerts_raised_at", "raised_at"),
        Index(
            "uq_alerts_one_open",
            "kind",
            "dedupe_key",
            unique=True,
            postgresql_where=text("resolved_at IS NULL AND NOT is_test"),
        ),
        Index("ix_alerts_undelivered", "raised_at", postgresql_where=text("notified_at IS NULL")),
        Index(
            "ix_alerts_resolution_undelivered",
            "resolved_at",
            postgresql_where=text(
                "resolution_notified_at IS NULL AND resolved_at IS NOT NULL AND notified_at IS NOT NULL "
                "AND NOT is_test"
            ),
        ),
    )

    alert_id: Mapped[str] = mapped_column(Text, primary_key=True)
    raised_at: Mapped[datetime]
    kind: Mapped[str] = mapped_column(Text)
    severity: Mapped[str] = mapped_column(Text)
    title: Mapped[str] = mapped_column(Text)
    detail: Mapped[str | None] = mapped_column(Text)
    dedupe_key: Mapped[str] = mapped_column(Text)
    service: Mapped[str] = mapped_column(Text)
    account: Mapped[str | None] = mapped_column(Text)
    is_test: Mapped[bool] = mapped_column(Boolean)
    resolved_at: Mapped[datetime | None]
    notified_at: Mapped[datetime | None]
    resolution_notified_at: Mapped[datetime | None]
    notify_attempts: Mapped[int] = mapped_column(Integer)
    last_notify_error: Mapped[str | None] = mapped_column(Text)
    next_attempt_at: Mapped[datetime | None]
    escalated_at: Mapped[datetime | None]  # last escalation to a higher severity (None: never escalated)


# Same definitions as migration 0006_ops (its frozen copy is the one real databases get).
ALERTS_RAISE_FUNCTION_SQL = """
CREATE OR REPLACE FUNCTION alerts_raise(
    p_alert_id text,
    p_raised_at timestamptz,
    p_kind text,
    p_severity text,
    p_title text,
    p_detail text,
    p_dedupe_key text,
    p_service text,
    p_account text
) RETURNS text
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    INSERT INTO public.alerts AS a (
        alert_id, raised_at, kind, severity, title, detail, dedupe_key, service, account, is_test,
        resolved_at, notified_at, resolution_notified_at, notify_attempts, last_notify_error,
        next_attempt_at, escalated_at
    )
    VALUES (
        p_alert_id, p_raised_at, p_kind, p_severity, p_title, p_detail, p_dedupe_key, p_service, p_account,
        false, NULL, NULL, NULL, 0, NULL, NULL, NULL
    )
    ON CONFLICT (kind, dedupe_key) WHERE resolved_at IS NULL AND NOT is_test
    DO UPDATE SET
        severity = EXCLUDED.severity,
        title = EXCLUDED.title,
        detail = EXCLUDED.detail,
        escalated_at = EXCLUDED.raised_at,
        notified_at = NULL,
        notify_attempts = 0,
        last_notify_error = NULL,
        next_attempt_at = NULL
    WHERE (CASE a.severity WHEN 'info' THEN 0 WHEN 'warning' THEN 1 WHEN 'critical' THEN 2 ELSE -1 END)
        < (CASE EXCLUDED.severity WHEN 'info' THEN 0 WHEN 'warning' THEN 1 WHEN 'critical' THEN 2 ELSE -1 END)
    RETURNING a.alert_id
$$
"""
ALERTS_GUARD_FUNCTION_SQL = """
CREATE OR REPLACE FUNCTION alerts_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.resolved_at IS NOT NULL AND NEW.resolved_at IS DISTINCT FROM OLD.resolved_at THEN
        RAISE EXCEPTION 'alert % is resolved: resolved_at cannot change', OLD.alert_id
            USING ERRCODE = 'insufficient_privilege';
    END IF;
    RETURN NEW;
END
$$
"""


@event.listens_for(AlertRow.__table__, "after_create")
def _create_alert_functions(_table: Table, connection: Connection, **_kw: Any) -> None:
    """`Base.metadata.create_all` (tests) gets the raise function and the guard trigger too."""
    for statement in (
        ALERTS_RAISE_FUNCTION_SQL,
        "REVOKE ALL ON FUNCTION alerts_raise(text, timestamptz, text, text, text, text, text, text, text) "
        "FROM PUBLIC",
        ALERTS_GUARD_FUNCTION_SQL,
        "CREATE TRIGGER alerts_guard_row BEFORE UPDATE ON alerts "
        "FOR EACH ROW EXECUTE FUNCTION alerts_guard()",
    ):
        connection.execute(text(statement))  # text() escapes the guard's `%` for the pyformat driver


class AlertDeliveryRow(Base):
    """What happened to one notification message (`raised` / `resolved` at one severity) for one chat."""

    __tablename__ = "alert_deliveries"
    __table_args__ = (
        CheckConstraint(_in("phase", DELIVERY_PHASES), name="phase"),
        CheckConstraint(_in("severity", SEVERITIES), name="severity"),
        CheckConstraint(_in("outcome", DELIVERY_OUTCOMES), name="outcome"),
    )

    alert_id: Mapped[str] = mapped_column(Text, ForeignKey("alerts.alert_id"), primary_key=True)
    phase: Mapped[str] = mapped_column(Text, primary_key=True)
    chat_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    severity: Mapped[str] = mapped_column(Text, primary_key=True)
    outcome: Mapped[str] = mapped_column(Text)
    recorded_at: Mapped[datetime]


class TelegramTotpStateRow(Base):
    """Last accepted TOTP time step per TOTP seed (`seed_name` = the bootstrap secret it comes from)."""

    __tablename__ = "telegram_totp_state"

    seed_name: Mapped[str] = mapped_column(Text, primary_key=True)
    last_counter: Mapped[int] = mapped_column(BigInteger)
    updated_at: Mapped[datetime]


class TelegramTotpFailureRow(Base):
    """Wrong TOTP codes of one Telegram user (`from.id`) and the `/resume` lockout they caused."""

    __tablename__ = "telegram_totp_failures"
    __table_args__ = (CheckConstraint("failures >= 0", name="failures"),)

    user_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    failures: Mapped[int] = mapped_column(Integer)
    locked_until: Mapped[datetime | None]
    updated_at: Mapped[datetime]
