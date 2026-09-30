"""Phase 12: config versions, sealed secrets (row-level security per scope), admin auth, audit, gates.

Revision ID: 0001_settings_vault
Revises:
Create Date: 2026-09-27
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_settings_vault"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

TS = sa.DateTime(timezone=True)
JSONB = postgresql.JSONB(astext_type=sa.Text())

# Scope -> (reader roles, owner role that tests and activates new versions).
SCOPE_ROLES: dict[str, tuple[tuple[str, ...], str]] = {
    "data": (("hdt_recorder", "hdt_news", "hdt_veto_scan"), "hdt_recorder"),
    "llm": (("hdt_council", "hdt_scorer", "hdt_news", "hdt_veto_scan"), "hdt_council"),
    "exec": (("hdt_execution",), "hdt_execution"),
    "ops": (("hdt_telegram",), "hdt_telegram"),
}
CONFIG_READERS = (
    "hdt_recorder",
    "hdt_council",
    "hdt_scorer",
    "hdt_risk",
    "hdt_execution",
    "hdt_news",
    "hdt_telegram",
    "hdt_veto_scan",
    "hdt_console_ro",
)
VAULT_METADATA_COLUMNS = (
    "scope, name, version, last4, fingerprint, status, reason, check_details, checked_at, "
    "created_by, created_at"
)


def _create_tables() -> None:
    op.create_table(
        "config_versions",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("section", sa.Text(), nullable=False),
        sa.Column("payload_json", JSONB, nullable=False),
        sa.Column("payload_sha256", sa.Text(), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("author", sa.Text(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("created_at", TS, nullable=False),
        sa.Column("parent_id", sa.BigInteger(), nullable=True),
        sa.CheckConstraint(
            "section IN ('models', 'cmc', 'binance', 'risk', 'council', 'mode', 'golive')",
            name=op.f("ck_config_versions_section"),
        ),
        sa.ForeignKeyConstraint(
            ["parent_id"], ["config_versions.id"], name=op.f("fk_config_versions_parent_id_config_versions")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_config_versions")),
    )
    op.create_index("ix_config_versions_section_id", "config_versions", ["section", "id"])
    op.create_table(
        "active_config",
        sa.Column("section", sa.Text(), nullable=False),
        sa.Column("version_id", sa.BigInteger(), nullable=False),
        sa.Column("activated_at", TS, nullable=False),
        sa.Column("activated_by", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(
            ["version_id"], ["config_versions.id"], name=op.f("fk_active_config_version_id_config_versions")
        ),
        sa.PrimaryKeyConstraint("section", name=op.f("pk_active_config")),
    )
    op.create_table(
        "secrets",
        sa.Column("scope", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("sealed_blob", sa.LargeBinary(), nullable=False),
        sa.Column("last4", sa.Text(), nullable=False),
        sa.Column("fingerprint", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("check_details", JSONB, nullable=False),
        sa.Column("last_request_id", sa.Text(), nullable=True),
        sa.Column("checked_at", TS, nullable=True),
        sa.Column("created_by", sa.Text(), nullable=False),
        sa.Column("created_at", TS, nullable=False),
        sa.CheckConstraint("scope IN ('data', 'llm', 'exec', 'ops')", name=op.f("ck_secrets_scope")),
        sa.CheckConstraint(
            "status IN ('pending', 'active', 'invalid', 'retired')", name=op.f("ck_secrets_status")
        ),
        sa.PrimaryKeyConstraint("scope", "name", "version", name=op.f("pk_secrets")),
    )
    op.create_index(
        "uq_secrets_one_active",
        "secrets",
        ["scope", "name"],
        unique=True,
        postgresql_where=sa.text("status = 'active'"),
    )
    op.create_table(
        "agent_versions",
        sa.Column("agent", sa.Text(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("model_slug", sa.Text(), nullable=False),
        sa.Column("provider_prefs_hash", sa.Text(), nullable=False),
        sa.Column("prompt_hash", sa.Text(), nullable=True),
        sa.Column("skill_commit", sa.Text(), nullable=True),
        sa.Column("config_version_id", sa.BigInteger(), nullable=True),
        sa.Column("created_by", sa.Text(), nullable=False),
        sa.Column("created_at", TS, nullable=False),
        sa.ForeignKeyConstraint(
            ["config_version_id"],
            ["config_versions.id"],
            name=op.f("fk_agent_versions_config_version_id_config_versions"),
        ),
        sa.PrimaryKeyConstraint("agent", "version", name=op.f("pk_agent_versions")),
    )
    op.create_table(
        "audit_log",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("at", TS, nullable=False),
        sa.Column("user", sa.Text(), nullable=False),
        sa.Column("ip", sa.Text(), nullable=True),
        sa.Column("action", sa.Text(), nullable=False),
        sa.Column("section", sa.Text(), nullable=True),
        sa.Column("diff_redacted", JSONB, nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_audit_log")),
    )
    op.create_index("ix_audit_log_at", "audit_log", ["at"])
    op.create_table(
        "admin_users",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("username", sa.Text(), nullable=False),
        sa.Column("password_hash", sa.Text(), nullable=False),
        sa.Column("totp_secret_sealed", sa.LargeBinary(), nullable=False),
        sa.Column("totp_last_counter", sa.BigInteger(), nullable=True),
        sa.Column("failed_attempts", sa.Integer(), nullable=False),
        sa.Column("locked_until", TS, nullable=True),
        sa.Column("disabled", sa.Boolean(), nullable=False),
        sa.Column("created_at", TS, nullable=False),
        sa.Column("last_login_at", TS, nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_admin_users")),
        sa.UniqueConstraint("username", name=op.f("uq_admin_users_username")),
    )
    op.create_table(
        "admin_sessions",
        sa.Column("token_hash", sa.Text(), nullable=False),
        sa.Column("user_id", sa.BigInteger(), nullable=False),
        sa.Column("created_at", TS, nullable=False),
        sa.Column("last_seen_at", TS, nullable=False),
        sa.Column("expires_at", TS, nullable=False),
        sa.Column("step_up_until", TS, nullable=True),
        sa.Column("ip", sa.Text(), nullable=True),
        sa.Column("user_agent", sa.Text(), nullable=True),
        sa.Column("revoked_at", TS, nullable=True),
        sa.ForeignKeyConstraint(
            ["user_id"], ["admin_users.id"], name=op.f("fk_admin_sessions_user_id_admin_users")
        ),
        sa.PrimaryKeyConstraint("token_hash", name=op.f("pk_admin_sessions")),
    )
    op.create_index("ix_admin_sessions_user_id", "admin_sessions", ["user_id"])
    op.create_table(
        "gate_flags",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("gate", sa.Text(), nullable=False),
        sa.Column("passed", sa.Boolean(), nullable=False),
        sa.Column("evidence_ref", sa.Text(), nullable=False),
        sa.Column("decided_by", sa.Text(), nullable=False),
        sa.Column("decided_at", TS, nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_gate_flags")),
    )
    op.create_index("ix_gate_flags_gate_decided_at", "gate_flags", ["gate", "decided_at"])
    op.create_table(
        "control_commands",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("command_id", sa.Text(), nullable=False),
        sa.Column("account", sa.Text(), nullable=False),
        sa.Column("action", sa.Text(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("requested_by", sa.Text(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("ip", sa.Text(), nullable=True),
        sa.Column("requested_at", TS, nullable=False),
        sa.Column("stream_id", sa.Text(), nullable=True),
        sa.CheckConstraint(
            "account IN ('paper', 'testnet', 'live')", name=op.f("ck_control_commands_account")
        ),
        sa.CheckConstraint(
            "action IN ('pause', 'resume', 'kill', 'flatten')", name=op.f("ck_control_commands_action")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_control_commands")),
        sa.UniqueConstraint("command_id", name=op.f("uq_control_commands_command_id")),
    )
    op.create_index("ix_control_commands_requested_at", "control_commands", ["requested_at"])
    op.create_table(
        "model_tests",
        sa.Column("request_id", sa.Text(), nullable=False),
        sa.Column("role", sa.Text(), nullable=False),
        sa.Column("model_slug", sa.Text(), nullable=False),
        sa.Column("config", JSONB, nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("schema_valid", sa.Boolean(), nullable=True),
        sa.Column("latency_ms", sa.Integer(), nullable=True),
        sa.Column("cost_usd", sa.Float(), nullable=True),
        sa.Column("provider", sa.Text(), nullable=True),
        sa.Column("model_returned", sa.Text(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("requested_by", sa.Text(), nullable=False),
        sa.Column("requested_at", TS, nullable=False),
        sa.Column("completed_at", TS, nullable=True),
        sa.CheckConstraint("status IN ('pending', 'done', 'error')", name=op.f("ck_model_tests_status")),
        sa.PrimaryKeyConstraint("request_id", name=op.f("pk_model_tests")),
    )


def _secrets_row_level_security() -> None:
    op.execute("ALTER TABLE secrets ENABLE ROW LEVEL SECURITY")
    # config-api: sees every row but only metadata columns, and may insert pending rows only.
    op.execute(f"GRANT SELECT ({VAULT_METADATA_COLUMNS}) ON secrets TO hdt_configapi")
    op.execute(
        "GRANT INSERT (scope, name, version, sealed_blob, last4, fingerprint, status, check_details, "
        "created_by, created_at) ON secrets TO hdt_configapi"
    )
    op.execute("CREATE POLICY configapi_read ON secrets FOR SELECT TO hdt_configapi USING (true)")
    op.execute(
        "CREATE POLICY configapi_insert_pending ON secrets FOR INSERT TO hdt_configapi "
        "WITH CHECK (status = 'pending' AND checked_at IS NULL AND last_request_id IS NULL)"
    )
    for scope, (readers, owner) in SCOPE_ROLES.items():
        op.execute(f"GRANT SELECT ON secrets TO {', '.join(readers)}")
        op.execute(
            f"CREATE POLICY {scope}_read ON secrets FOR SELECT TO {', '.join(readers)} "
            f"USING (scope = '{scope}')"
        )
        op.execute(
            f"GRANT UPDATE (status, reason, check_details, last_request_id, checked_at) ON secrets TO {owner}"
        )
        op.execute(
            f"CREATE POLICY {scope}_owner_update ON secrets FOR UPDATE TO {owner} "
            f"USING (scope = '{scope}') WITH CHECK (scope = '{scope}')"
        )


def _grants() -> None:
    readers = ", ".join(CONFIG_READERS)
    op.execute(f"GRANT SELECT ON config_versions, active_config TO {readers}, hdt_configapi")
    op.execute("GRANT INSERT ON config_versions TO hdt_configapi")
    op.execute("GRANT INSERT, UPDATE ON active_config TO hdt_configapi")
    op.execute("GRANT SELECT ON agent_versions TO hdt_configapi, hdt_council, hdt_scorer, hdt_console_ro")
    op.execute("GRANT INSERT ON agent_versions TO hdt_configapi, hdt_council")
    # Append-only audit log: INSERT and SELECT only, never UPDATE or DELETE.
    op.execute("GRANT SELECT, INSERT ON audit_log TO hdt_configapi")
    op.execute("GRANT INSERT ON audit_log TO hdt_telegram, hdt_execution, hdt_risk")
    op.execute("GRANT SELECT ON audit_log TO hdt_console_ro")
    op.execute("GRANT SELECT, INSERT, UPDATE ON admin_users, admin_sessions TO hdt_configapi")
    op.execute("GRANT DELETE ON admin_sessions TO hdt_configapi")
    op.execute("GRANT SELECT ON gate_flags TO hdt_configapi, hdt_console_ro, hdt_scorer")
    op.execute("GRANT INSERT ON gate_flags TO hdt_scorer")
    op.execute("GRANT SELECT, INSERT, UPDATE ON control_commands TO hdt_configapi, hdt_telegram")
    op.execute("GRANT SELECT ON control_commands TO hdt_risk, hdt_execution, hdt_console_ro")
    op.execute("GRANT SELECT, INSERT ON model_tests TO hdt_configapi")
    op.execute("GRANT SELECT, UPDATE ON model_tests TO hdt_council")
    op.execute(
        "GRANT USAGE ON SEQUENCE config_versions_id_seq, audit_log_id_seq, admin_users_id_seq "
        "TO hdt_configapi"
    )
    op.execute("GRANT USAGE ON SEQUENCE control_commands_id_seq TO hdt_configapi, hdt_telegram")
    op.execute("GRANT USAGE ON SEQUENCE gate_flags_id_seq TO hdt_scorer")


def upgrade() -> None:
    _create_tables()
    _secrets_row_level_security()
    _grants()


def downgrade() -> None:
    for table in (
        "model_tests",
        "control_commands",
        "gate_flags",
        "admin_sessions",
        "admin_users",
        "audit_log",
        "agent_versions",
        "secrets",
        "active_config",
        "config_versions",
    ):
        op.drop_table(table)
