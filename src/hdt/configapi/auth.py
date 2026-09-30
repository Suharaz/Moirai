"""Admin authentication: argon2 password + mandatory TOTP, server-side sessions, lockout, step-up.

- Login needs username, password and a TOTP code; 5 consecutive failures (password or TOTP, at login or
  step-up) lock the account for 15 minutes and revoke its sessions.
- Sessions expire after 30 minutes without a request; every authenticated request slides the expiry.
- Step-up: a fresh TOTP code (login counts) is valid for 5 minutes and is required for secret writes,
  mode changes, risk changes while live, and control actions per the console confirmation flows.
- Every mutating request must carry `X-CSRF-Token` (checked here for all session-authenticated routes).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError
from fastapi import Request
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from hdt.configapi.audit import write_audit
from hdt.configapi.context import AppState, get_state
from hdt.configapi.errors import ApiError
from hdt.configapi.security import (
    CSRF_HEADER,
    LOCKOUT,
    MAX_FAILED_ATTEMPTS,
    MUTATING_METHODS,
    SESSION_COOKIE,
    SESSION_IDLE,
    STEP_UP_TTL,
    client_ip,
    csrf_matches,
    csrf_token_for,
    new_session_token,
    session_token_hash,
)
from hdt.configapi.totp import open_totp_secret, seal_totp_secret, verify_totp
from hdt.core.clock import utcnow
from hdt.db.models.settings import AdminSessionRow, AdminUserRow

AUTH_SECTION = "auth"


@dataclass(frozen=True)
class AuthContext:
    user_id: int
    username: str
    token_hash: str = field(repr=False)
    csrf_token: str = field(repr=False)
    expires_at: datetime
    step_up_until: datetime | None
    ip: str | None

    def has_step_up(self) -> bool:
        return self.step_up_until is not None and self.step_up_until > utcnow()


@dataclass(frozen=True)
class LoginOutcome:
    status: Literal["ok", "invalid", "locked"]
    token: str | None = field(default=None, repr=False)
    context: AuthContext | None = None


def hash_password(hasher: PasswordHasher, password: str) -> str:
    return hasher.hash(password)


def _password_ok(hasher: PasswordHasher, stored: str, password: str) -> bool:
    try:
        return hasher.verify(stored, password)
    except (VerificationError, InvalidHashError):
        return False


def _register_failure(session: Session, user: AdminUserRow, now: datetime) -> bool:
    """Count a failed factor; lock the account and revoke its sessions at the limit. Returns locked."""
    user.failed_attempts += 1
    if user.failed_attempts < MAX_FAILED_ATTEMPTS:
        return False
    user.failed_attempts = 0
    user.locked_until = now + LOCKOUT
    session.execute(
        update(AdminSessionRow)
        .where(AdminSessionRow.user_id == user.id, AdminSessionRow.revoked_at.is_(None))
        .values(revoked_at=now)
    )
    return True


def _context(state: AppState, user: AdminUserRow, row: AdminSessionRow, token: str) -> AuthContext:
    return AuthContext(
        user_id=user.id,
        username=user.username,
        token_hash=row.token_hash,
        csrf_token=csrf_token_for(state.settings.session_secret, token),
        expires_at=row.expires_at,
        step_up_until=row.step_up_until,
        ip=row.ip,
    )


def login(
    state: AppState,
    session: Session,
    *,
    username: str,
    password: str,
    code: str,
    ip: str | None,
    user_agent: str | None,
) -> LoginOutcome:
    now = utcnow()
    user = session.scalar(select(AdminUserRow).where(AdminUserRow.username == username).with_for_update())
    if user is None or user.disabled:
        _password_ok(state.hasher, state.dummy_hash, password)  # same cost as a real check
        write_audit(
            session,
            user=username[:64],
            ip=ip,
            action="login.fail",
            section=AUTH_SECTION,
            diff={"reason": "unknown or disabled user"},
        )
        return LoginOutcome("invalid")
    if user.locked_until is not None and user.locked_until > now:
        write_audit(
            session,
            user=user.username,
            ip=ip,
            action="login.locked",
            section=AUTH_SECTION,
            diff={"locked_until": user.locked_until.isoformat()},
        )
        return LoginOutcome("locked")
    password_ok = _password_ok(state.hasher, user.password_hash, password)
    secret = open_totp_secret(user.totp_secret_sealed, state.settings.session_secret)
    counter = verify_totp(secret, code, now=now, last_counter=user.totp_last_counter)
    if not password_ok or counter is None:
        attempt = user.failed_attempts + 1
        locked = _register_failure(session, user, now)
        write_audit(
            session,
            user=user.username,
            ip=ip,
            action="login.fail",
            section=AUTH_SECTION,
            diff={
                "reason": "wrong password" if not password_ok else "wrong TOTP",
                "attempt": f"{attempt}/{MAX_FAILED_ATTEMPTS}",
                "locked": locked,
            },
        )
        return LoginOutcome("locked" if locked else "invalid")
    if state.hasher.check_needs_rehash(user.password_hash):
        user.password_hash = state.hasher.hash(password)
    user.failed_attempts, user.locked_until = 0, None
    user.totp_last_counter, user.last_login_at = counter, now
    token = new_session_token()
    row = AdminSessionRow(
        token_hash=session_token_hash(state.settings.session_secret, token),
        user_id=user.id,
        created_at=now,
        last_seen_at=now,
        expires_at=now + SESSION_IDLE,
        step_up_until=now + STEP_UP_TTL,
        ip=ip,
        user_agent=(user_agent or "")[:256] or None,
    )
    session.add(row)
    write_audit(session, user=user.username, ip=ip, action="login.ok", section=AUTH_SECTION, diff={})
    return LoginOutcome("ok", token, _context(state, user, row, token))


def authenticate(state: AppState, session: Session, token: str) -> AuthContext | None:
    """Validate the session token and slide its idle expiry; None when missing, revoked or expired."""
    now = utcnow()
    row = session.get(
        AdminSessionRow, session_token_hash(state.settings.session_secret, token), with_for_update=True
    )
    if row is None or row.revoked_at is not None or row.expires_at <= now:
        return None
    user = session.get(AdminUserRow, row.user_id)
    if user is None or user.disabled or (user.locked_until is not None and user.locked_until > now):
        return None
    row.last_seen_at, row.expires_at = now, now + SESSION_IDLE
    return _context(state, user, row, token)


def logout(session: Session, ctx: AuthContext) -> None:
    now = utcnow()
    session.execute(
        update(AdminSessionRow).where(AdminSessionRow.token_hash == ctx.token_hash).values(revoked_at=now)
    )
    write_audit(session, user=ctx.username, ip=ctx.ip, action="logout", section=AUTH_SECTION, diff={})


def step_up(
    state: AppState, session: Session, ctx: AuthContext, code: str
) -> datetime | Literal["invalid", "locked"]:
    now = utcnow()
    user = session.get(AdminUserRow, ctx.user_id, with_for_update=True)
    if user is None:
        return "invalid"
    secret = open_totp_secret(user.totp_secret_sealed, state.settings.session_secret)
    counter = verify_totp(secret, code, now=now, last_counter=user.totp_last_counter)
    if counter is None:
        attempt = user.failed_attempts + 1
        locked = _register_failure(session, user, now)
        write_audit(
            session,
            user=user.username,
            ip=ctx.ip,
            action="step_up.fail",
            section=AUTH_SECTION,
            diff={"attempt": f"{attempt}/{MAX_FAILED_ATTEMPTS}", "locked": locked},
        )
        return "locked" if locked else "invalid"
    user.totp_last_counter, user.failed_attempts = counter, 0
    until = now + STEP_UP_TTL
    session.execute(
        update(AdminSessionRow)
        .where(AdminSessionRow.token_hash == ctx.token_hash)
        .values(step_up_until=until)
    )
    write_audit(session, user=user.username, ip=ctx.ip, action="step_up.ok", section=AUTH_SECTION, diff={})
    return until


MIN_PASSWORD_LENGTH = 12


class AdminProvisionError(ValueError):
    """The admin account cannot be created or reset as requested."""


def provision_admin(
    session: Session,
    hasher: PasswordHasher,
    session_secret: bytes,
    *,
    username: str,
    password: str,
    totp_secret: str,
    enrolled_counter: int,
    reset: bool = False,
) -> AdminUserRow:
    """Create an admin (or, with `reset`, replace its password and TOTP seed, clear the lockout and
    revoke its sessions). `enrolled_counter` is the step of the code the operator confirmed at
    enrollment, so that code cannot be replayed at login."""
    if not username or len(username) > 64 or not username.isprintable() or username != username.strip():
        raise AdminProvisionError("username must be 1-64 printable characters without surrounding spaces")
    if len(password) < MIN_PASSWORD_LENGTH:
        raise AdminProvisionError(f"password must be at least {MIN_PASSWORD_LENGTH} characters")
    now = utcnow()
    user = session.scalar(select(AdminUserRow).where(AdminUserRow.username == username).with_for_update())
    if user is not None and not reset:
        raise AdminProvisionError(
            f"admin '{username}' already exists; use --reset to replace its credentials"
        )
    if user is None and reset:
        raise AdminProvisionError(f"admin '{username}' does not exist")
    sealed = seal_totp_secret(totp_secret, session_secret)
    if user is None:
        user = AdminUserRow(
            username=username,
            password_hash=hasher.hash(password),
            totp_secret_sealed=sealed,
            totp_last_counter=enrolled_counter,
            failed_attempts=0,
            disabled=False,
            created_at=now,
        )
        session.add(user)
        session.flush()
    else:
        user.password_hash, user.totp_secret_sealed = hasher.hash(password), sealed
        user.totp_last_counter, user.failed_attempts, user.locked_until = enrolled_counter, 0, None
        session.execute(
            update(AdminSessionRow)
            .where(AdminSessionRow.user_id == user.id, AdminSessionRow.revoked_at.is_(None))
            .values(revoked_at=now)
        )
    write_audit(
        session,
        user=username,
        ip=None,
        action="admin.reset" if reset else "admin.create",
        section=AUTH_SECTION,
        diff={"via": "scripts/admin_create.py"},
    )
    return user


# --------------------------------------------------------------------------- FastAPI dependencies


async def require_session(request: Request) -> AuthContext:
    """Every console route: a live server-side session, plus the CSRF header on mutating requests."""
    state = get_state(request)
    token = request.cookies.get(SESSION_COOKIE)
    if not token:
        raise ApiError(401, "not_authenticated", "sign in with password and TOTP")
    ctx = await state.db(lambda s: authenticate(state, s, token))
    if ctx is None:
        raise ApiError(401, "not_authenticated", "the session is missing, revoked or expired")
    if request.method in MUTATING_METHODS and not csrf_matches(
        state.settings.session_secret, token, request.headers.get(CSRF_HEADER)
    ):
        raise ApiError(403, "csrf_failed", f"missing or wrong {CSRF_HEADER} header")
    ip = client_ip(request, state.settings.trusted_proxies)
    return AuthContext(
        ctx.user_id, ctx.username, ctx.token_hash, ctx.csrf_token, ctx.expires_at, ctx.step_up_until, ip
    )


def require_step_up(ctx: AuthContext) -> None:
    if not ctx.has_step_up():
        raise ApiError(403, "step_up_required", "confirm with a fresh TOTP code (POST /api/auth/step-up)")
