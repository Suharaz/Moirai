"""Phase 12 tables: immutable config versions, sealed secrets, admin auth, audit, gates, controls.

Migration `0001_settings_vault` creates these tables, enables row-level security on `secrets` (each
service role only sees its own scope) and grants each service role exactly what it needs. `audit_log`
is append-only: no role is granted UPDATE or DELETE on it.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Float,
    ForeignKey,
    Identity,
    Index,
    Integer,
    LargeBinary,
    Text,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from hdt.db.base import Base

SECTION_NAMES = ("models", "cmc", "binance", "risk", "council", "mode", "golive")
SECRET_SCOPES = ("data", "llm", "exec", "ops")
SECRET_STATUSES = ("pending", "active", "invalid", "retired")


def _in(column: str, values: tuple[str, ...]) -> str:
    return f"{column} IN ({', '.join(repr(v) for v in values)})"


class ConfigVersionRow(Base):
    """One immutable version of one config section; never updated or deleted."""

    __tablename__ = "config_versions"
    __table_args__ = (
        CheckConstraint(_in("section", SECTION_NAMES), name="section"),
        Index("ix_config_versions_section_id", "section", "id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    section: Mapped[str] = mapped_column(Text)
    payload_json: Mapped[dict[str, Any]]
    payload_sha256: Mapped[str] = mapped_column(Text)
    schema_version: Mapped[int] = mapped_column(Integer)
    author: Mapped[str] = mapped_column(Text)
    reason: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime]
    parent_id: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("config_versions.id"))


class ActiveConfigRow(Base):
    """The version currently in force for each section (new council events pin it at start)."""

    __tablename__ = "active_config"

    section: Mapped[str] = mapped_column(Text, primary_key=True)
    version_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("config_versions.id"))
    activated_at: Mapped[datetime]
    activated_by: Mapped[str] = mapped_column(Text)


class SecretRow(Base):
    """A sealed application secret version; only the scope owner can decrypt `sealed_blob`."""

    __tablename__ = "secrets"
    __table_args__ = (
        CheckConstraint(_in("scope", SECRET_SCOPES), name="scope"),
        CheckConstraint(_in("status", SECRET_STATUSES), name="status"),
        Index(
            "uq_secrets_one_active",
            "scope",
            "name",
            unique=True,
            postgresql_where=text("status = 'active'"),
        ),
    )

    scope: Mapped[str] = mapped_column(Text, primary_key=True)
    name: Mapped[str] = mapped_column(Text, primary_key=True)
    version: Mapped[int] = mapped_column(Integer, primary_key=True)
    sealed_blob: Mapped[bytes] = mapped_column(LargeBinary)
    last4: Mapped[str] = mapped_column(Text)
    fingerprint: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text)
    reason: Mapped[str | None] = mapped_column(Text)
    check_details: Mapped[dict[str, Any]]
    last_request_id: Mapped[str | None] = mapped_column(Text)
    checked_at: Mapped[datetime | None]
    created_by: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime]


class AgentVersionRow(Base):
    """A new row per role whenever its model or provider preferences (or prompt, phase 05) change."""

    __tablename__ = "agent_versions"

    agent: Mapped[str] = mapped_column(Text, primary_key=True)
    version: Mapped[int] = mapped_column(Integer, primary_key=True)
    model_slug: Mapped[str] = mapped_column(Text)
    provider_prefs_hash: Mapped[str] = mapped_column(Text)
    prompt_hash: Mapped[str | None] = mapped_column(Text)
    skill_commit: Mapped[str | None] = mapped_column(Text)
    config_version_id: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("config_versions.id"))
    created_by: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime]


class AuditLogRow(Base):
    """Append-only audit trail; `diff_redacted` never holds a secret (only last4 / fingerprint)."""

    __tablename__ = "audit_log"
    __table_args__ = (Index("ix_audit_log_at", "at"),)

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    at: Mapped[datetime]
    user: Mapped[str] = mapped_column(Text)
    ip: Mapped[str | None] = mapped_column(Text)
    action: Mapped[str] = mapped_column(Text)
    section: Mapped[str | None] = mapped_column(Text)
    diff_redacted: Mapped[dict[str, Any]]


class AdminUserRow(Base):
    __tablename__ = "admin_users"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    username: Mapped[str] = mapped_column(Text, unique=True)
    password_hash: Mapped[str] = mapped_column(Text)
    totp_secret_sealed: Mapped[bytes] = mapped_column(LargeBinary)
    totp_last_counter: Mapped[int | None] = mapped_column(BigInteger)
    failed_attempts: Mapped[int] = mapped_column(Integer)
    locked_until: Mapped[datetime | None]
    disabled: Mapped[bool] = mapped_column(Boolean)
    created_at: Mapped[datetime]
    last_login_at: Mapped[datetime | None]


class AdminSessionRow(Base):
    """Server-side session; the cookie carries a random token, only its keyed hash is stored."""

    __tablename__ = "admin_sessions"

    token_hash: Mapped[str] = mapped_column(Text, primary_key=True)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("admin_users.id"), index=True)
    created_at: Mapped[datetime]
    last_seen_at: Mapped[datetime]
    expires_at: Mapped[datetime]
    step_up_until: Mapped[datetime | None]
    ip: Mapped[str | None] = mapped_column(Text)
    user_agent: Mapped[str | None] = mapped_column(Text)
    revoked_at: Mapped[datetime | None]


class GateFlagRow(Base):
    """Gate decisions (G1..G4) written by phase 11; the newest row per gate is the current state."""

    __tablename__ = "gate_flags"
    __table_args__ = (Index("ix_gate_flags_gate_decided_at", "gate", "decided_at"),)

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    gate: Mapped[str] = mapped_column(Text)
    passed: Mapped[bool] = mapped_column(Boolean)
    evidence_ref: Mapped[str] = mapped_column(Text)
    decided_by: Mapped[str] = mapped_column(Text)
    decided_at: Mapped[datetime]


class ControlCommandRow(Base):
    """Log of every pause / resume / kill / flatten command published on stream `controls`."""

    __tablename__ = "control_commands"
    __table_args__ = (
        CheckConstraint(_in("account", ("paper", "testnet", "live")), name="account"),
        CheckConstraint(_in("action", ("pause", "resume", "kill", "flatten")), name="action"),
        Index("ix_control_commands_requested_at", "requested_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    command_id: Mapped[str] = mapped_column(Text, unique=True)
    account: Mapped[str] = mapped_column(Text)
    action: Mapped[str] = mapped_column(Text)
    reason: Mapped[str] = mapped_column(Text)
    requested_by: Mapped[str] = mapped_column(Text)
    source: Mapped[str] = mapped_column(Text)
    ip: Mapped[str | None] = mapped_column(Text)
    requested_at: Mapped[datetime]
    stream_id: Mapped[str | None] = mapped_column(Text)


class ModelTestRow(Base):
    """A console model test: requested by config-api, executed and completed by the `llm` scope owner."""

    __tablename__ = "model_tests"
    __table_args__ = (CheckConstraint(_in("status", ("pending", "done", "error")), name="status"),)

    request_id: Mapped[str] = mapped_column(Text, primary_key=True)
    role: Mapped[str] = mapped_column(Text)
    model_slug: Mapped[str] = mapped_column(Text)
    config: Mapped[dict[str, Any]]
    status: Mapped[str] = mapped_column(Text)
    schema_valid: Mapped[bool | None] = mapped_column(Boolean)
    latency_ms: Mapped[int | None] = mapped_column(Integer)
    cost_usd: Mapped[float | None] = mapped_column(Float)
    provider: Mapped[str | None] = mapped_column(Text)
    model_returned: Mapped[str | None] = mapped_column(Text)
    error: Mapped[str | None] = mapped_column(Text)
    requested_by: Mapped[str] = mapped_column(Text)
    requested_at: Mapped[datetime]
    completed_at: Mapped[datetime | None]
