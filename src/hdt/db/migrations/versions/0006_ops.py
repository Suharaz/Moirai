"""Ops tables: the alert outbox delivered by the Telegram bot and the Telegram TOTP guards.

Grants (least privilege):
- every service that raises alerts (recorder, council, scorer, risk, execution, news, veto-scan,
  telegram-bot) raises through the `SECURITY DEFINER` function `alerts_raise` (EXECUTE only): it inserts the
  alert with a fresh delivery state, or escalates the open alert of the same `(kind, dedupe_key)` when the
  new severity is higher (new text, `escalated_at`, delivery queued again). No raiser may INSERT into
  `alerts` directly (a pre-inserted "delivered" row would silence the real alert) nor UPDATE its text,
  severity or delivery state: a raiser may only resolve (`resolved_at`), and the `alerts_guard` trigger
  keeps a resolved alert resolved;
- only the Telegram bot records delivery outcomes (the delivery columns, `alert_deliveries`) and inserts
  directly (the born-resolved test alerts of `python -m hdt.ops.alerts test`);
- the console reads alerts; the public publisher never does;
- the Telegram bot reads the recorder health tables that feed the route / CMC / disk alert rules and owns
  the TOTP replay guard and failure counters;
- the public publisher reads only the columns it publishes of the phase 12 tables (`agent_versions` model
  slugs, `gate_flags` outcomes) and of `candidate_sets` (decision-card levels). Tables of later phases grant
  `hdt_publisher_ro` in their own migrations.

Revision ID: 0006_ops
Revises: 0005_ledger_risk
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "0006_ops"
down_revision: str | None = "0005_ledger_risk"
branch_labels = None
depends_on = None

TS = sa.DateTime(timezone=True)
ALERT_RAISERS = (
    "hdt_recorder",
    "hdt_council",
    "hdt_scorer",
    "hdt_risk",
    "hdt_execution",
    "hdt_news",
    "hdt_veto_scan",
)
BOT_ALERT_COLUMNS = (
    "resolved_at",
    "notified_at",
    "resolution_notified_at",
    "notify_attempts",
    "last_notify_error",
    "next_attempt_at",
)
RAISE_FUNCTION = "alerts_raise(text, timestamptz, text, text, text, text, text, text, text)"
# Insert-or-escalate with fixed delivery values, owned by the migration role. The raiser chooses only what
# the alert says (the arguments); it can never write the delivery state of any alert, nor lower a severity.
RAISE_FUNCTION_SQL = """
CREATE FUNCTION alerts_raise(
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
GUARD_FUNCTION_SQL = """
CREATE FUNCTION alerts_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.resolved_at IS NOT NULL AND NEW.resolved_at IS DISTINCT FROM OLD.resolved_at THEN
        RAISE EXCEPTION 'alert % is resolved: resolved_at cannot change', OLD.alert_id
            USING ERRCODE = 'insufficient_privilege';
    END IF;
    RETURN NEW;
END
$$
"""
PUBLISHER_GRANTS = (
    "SELECT (agent, version, model_slug, created_at) ON agent_versions",
    "SELECT (gate, passed, decided_at) ON gate_flags",
    "SELECT (candidate_set_sha256, payload) ON candidate_sets",
)


def upgrade() -> None:
    op.create_table(
        "alerts",
        sa.Column("alert_id", sa.Text(), nullable=False),
        sa.Column("raised_at", TS, nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("severity", sa.Text(), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("detail", sa.Text(), nullable=True),
        sa.Column("dedupe_key", sa.Text(), nullable=False),
        sa.Column("service", sa.Text(), nullable=False),
        sa.Column("account", sa.Text(), nullable=True),
        sa.Column("is_test", sa.Boolean(), nullable=False),
        sa.Column("resolved_at", TS, nullable=True),
        sa.Column("notified_at", TS, nullable=True),
        sa.Column("resolution_notified_at", TS, nullable=True),
        sa.Column("notify_attempts", sa.Integer(), nullable=False),
        sa.Column("last_notify_error", sa.Text(), nullable=True),
        sa.Column("next_attempt_at", TS, nullable=True),
        sa.Column("escalated_at", TS, nullable=True),
        sa.CheckConstraint("severity IN ('info', 'warning', 'critical')", name=op.f("ck_alerts_severity")),
        sa.CheckConstraint(
            "account IS NULL OR account IN ('paper', 'testnet', 'live')", name=op.f("ck_alerts_account")
        ),
        sa.CheckConstraint("notify_attempts >= 0", name=op.f("ck_alerts_notify_attempts")),
        sa.PrimaryKeyConstraint("alert_id", name=op.f("pk_alerts")),
    )
    op.create_index("ix_alerts_raised_at", "alerts", ["raised_at"])
    op.create_index(
        "uq_alerts_one_open",
        "alerts",
        ["kind", "dedupe_key"],
        unique=True,
        postgresql_where=sa.text("resolved_at IS NULL AND NOT is_test"),
    )
    op.create_index(
        "ix_alerts_undelivered",
        "alerts",
        ["raised_at"],
        postgresql_where=sa.text("notified_at IS NULL"),
    )
    # Resolution notifications still to deliver (`hdt.ops.alerts.due_notifications`).
    op.create_index(
        "ix_alerts_resolution_undelivered",
        "alerts",
        ["resolved_at"],
        postgresql_where=sa.text(
            "resolution_notified_at IS NULL AND resolved_at IS NOT NULL AND notified_at IS NOT NULL "
            "AND NOT is_test"
        ),
    )
    op.create_table(
        "alert_deliveries",
        sa.Column("alert_id", sa.Text(), nullable=False),
        sa.Column("phase", sa.Text(), nullable=False),
        sa.Column("chat_id", sa.BigInteger(), nullable=False),
        sa.Column("severity", sa.Text(), nullable=False),
        sa.Column("outcome", sa.Text(), nullable=False),
        sa.Column("recorded_at", TS, nullable=False),
        sa.CheckConstraint("phase IN ('raised', 'resolved')", name=op.f("ck_alert_deliveries_phase")),
        sa.CheckConstraint(
            "severity IN ('info', 'warning', 'critical')", name=op.f("ck_alert_deliveries_severity")
        ),
        sa.CheckConstraint(
            "outcome IN ('delivered', 'unreachable')", name=op.f("ck_alert_deliveries_outcome")
        ),
        sa.ForeignKeyConstraint(
            ["alert_id"], ["alerts.alert_id"], name=op.f("fk_alert_deliveries_alert_id_alerts")
        ),
        sa.PrimaryKeyConstraint("alert_id", "phase", "chat_id", "severity", name=op.f("pk_alert_deliveries")),
    )
    op.create_table(
        "telegram_totp_state",
        sa.Column("seed_name", sa.Text(), nullable=False),
        sa.Column("last_counter", sa.BigInteger(), nullable=False),
        sa.Column("updated_at", TS, nullable=False),
        sa.PrimaryKeyConstraint("seed_name", name=op.f("pk_telegram_totp_state")),
    )
    op.create_table(
        "telegram_totp_failures",
        sa.Column("user_id", sa.BigInteger(), nullable=False),
        sa.Column("failures", sa.Integer(), nullable=False),
        sa.Column("locked_until", TS, nullable=True),
        sa.Column("updated_at", TS, nullable=False),
        sa.CheckConstraint("failures >= 0", name=op.f("ck_telegram_totp_failures_failures")),
        sa.PrimaryKeyConstraint("user_id", name=op.f("pk_telegram_totp_failures")),
    )

    op.execute(RAISE_FUNCTION_SQL)
    op.execute(f"REVOKE ALL ON FUNCTION {RAISE_FUNCTION} FROM PUBLIC")
    op.execute(GUARD_FUNCTION_SQL)
    op.execute(
        "CREATE TRIGGER alerts_guard_row BEFORE UPDATE ON alerts FOR EACH ROW EXECUTE FUNCTION alerts_guard()"
    )
    raisers = ", ".join((*ALERT_RAISERS, "hdt_telegram"))
    op.execute(f"GRANT EXECUTE ON FUNCTION {RAISE_FUNCTION} TO {raisers}")
    op.execute(f"GRANT SELECT, UPDATE (resolved_at) ON alerts TO {raisers}")
    op.execute(f"GRANT INSERT, UPDATE ({', '.join(BOT_ALERT_COLUMNS)}) ON alerts TO hdt_telegram")
    op.execute("GRANT SELECT ON alerts TO hdt_console_ro")
    op.execute("GRANT SELECT, INSERT, UPDATE (outcome, recorded_at) ON alert_deliveries TO hdt_telegram")
    op.execute("GRANT SELECT, INSERT, UPDATE ON telegram_totp_state, telegram_totp_failures TO hdt_telegram")
    # Alert rules evaluated by the Telegram bot.
    op.execute(
        "GRANT SELECT ON route_health, ws_health, cmc_key_info, cmc_credit_usage, lake_stats TO hdt_telegram"
    )
    # Every Telegram command is audited; the ORM insert reads the new id back (INSERT ... RETURNING id).
    op.execute("GRANT SELECT (id) ON audit_log TO hdt_telegram")
    # Public snapshot sources: only the published columns.
    for grant in PUBLISHER_GRANTS:
        op.execute(f"GRANT {grant} TO hdt_publisher_ro")


def downgrade() -> None:
    for grant in PUBLISHER_GRANTS:
        op.execute(f"REVOKE {grant} FROM hdt_publisher_ro")
    op.execute("REVOKE SELECT (id) ON audit_log FROM hdt_telegram")
    op.execute(
        "REVOKE SELECT ON route_health, ws_health, cmc_key_info, cmc_credit_usage, lake_stats "
        "FROM hdt_telegram"
    )
    op.drop_table("telegram_totp_failures")
    op.drop_table("telegram_totp_state")
    op.drop_table("alert_deliveries")
    op.drop_index("ix_alerts_resolution_undelivered", table_name="alerts")
    op.drop_index("ix_alerts_undelivered", table_name="alerts")
    op.drop_index("uq_alerts_one_open", table_name="alerts")
    op.drop_index("ix_alerts_raised_at", table_name="alerts")
    op.drop_table("alerts")  # drops the alerts_guard_row trigger with it
    op.execute("DROP FUNCTION alerts_guard()")
    op.execute(f"DROP FUNCTION {RAISE_FUNCTION}")
