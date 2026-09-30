"""`/secrets`: write-only sealed secrets. config-api stores the browser-sealed blob as `pending`, asks the
scope owner to test it, and never reads `sealed_blob` back (its Postgres role cannot select that column).
There is no endpoint that returns a secret value."""

from __future__ import annotations

import re
import uuid
from typing import Any

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, ConfigDict, Field, field_validator
from redis.exceptions import RedisError
from sqlalchemy import func, insert, select, text
from sqlalchemy.orm import Session

from hdt.configapi.audit import write_audit
from hdt.configapi.auth import AuthContext, require_session, require_step_up
from hdt.configapi.context import AppState, get_state
from hdt.configapi.errors import ApiError
from hdt.contracts.streams import Stream
from hdt.core.clock import utcnow
from hdt.core.streams import publish
from hdt.db.models.settings import SecretRow
from hdt.vault.owner import SecretTestRequest
from hdt.vault.scopes import SECRET_SPECS, SecretSpec
from hdt.vault.sealed import FINGERPRINT_RE, SealedBlobError, parse_sealed_blob

router = APIRouter(prefix="/secrets", tags=["secrets"])
AUDIT_SECTION = "keys"
_LAST4_RE = re.compile(r"^[\x21-\x7e]{4}$")

# Every column except `sealed_blob` and `last_request_id`: what config-api is allowed to read.
META_COLUMNS = (
    SecretRow.scope,
    SecretRow.name,
    SecretRow.version,
    SecretRow.last4,
    SecretRow.fingerprint,
    SecretRow.status,
    SecretRow.reason,
    SecretRow.check_details,
    SecretRow.checked_at,
    SecretRow.created_by,
    SecretRow.created_at,
)


class SecretWriteRequest(BaseModel):
    """Only the sealed blob and two browser-computed identifiers; any other field is rejected."""

    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)

    sealed_blob: str = Field(min_length=1, max_length=32 * 1024)
    last4: str
    fingerprint: str

    @field_validator("last4")
    @classmethod
    def _last4(cls, value: str) -> str:
        if not _LAST4_RE.fullmatch(value):
            raise ValueError("last4 must be exactly 4 printable characters")
        return value

    @field_validator("fingerprint")
    @classmethod
    def _fingerprint(cls, value: str) -> str:
        if not FINGERPRINT_RE.fullmatch(value):
            raise ValueError("fingerprint must be the first 16 lowercase hex chars of sha256(plaintext)")
        return value


def insert_pending_secret(
    session: Session,
    *,
    scope: str,
    name: str,
    version: int,
    sealed_blob: bytes,
    last4: str,
    fingerprint: str,
    created_by: str,
) -> None:
    """Insert a new `pending` version naming only the columns config-api may insert (migration 0001).

    An ORM `session.add(SecretRow(...))` would also send `reason`, `last_request_id` and `checked_at` as
    explicit NULLs, which the column-level grant refuses; those belong to the scope owner's self-test.
    """
    session.execute(
        insert(SecretRow).values(
            scope=scope,
            name=name,
            version=version,
            sealed_blob=sealed_blob,
            last4=last4,
            fingerprint=fingerprint,
            status="pending",
            check_details={},
            created_by=created_by,
            created_at=utcnow(),
        )
    )


def secret_metadata(
    session: Session, scope: str | None = None, name: str | None = None
) -> list[dict[str, Any]]:
    stmt = select(*META_COLUMNS).order_by(SecretRow.scope, SecretRow.name, SecretRow.version.desc())
    if scope is not None:
        stmt = stmt.where(SecretRow.scope == scope)
    if name is not None:
        stmt = stmt.where(SecretRow.name == name)
    return [
        {
            "version": row.version,
            "status": row.status,
            "last4": row.last4,
            "fingerprint": row.fingerprint,
            "reason": row.reason,
            "details": row.check_details,
            "checked_at": row.checked_at,
            "created_by": row.created_by,
            "created_at": row.created_at,
            "scope": row.scope,
            "name": row.name,
        }
        for row in session.execute(stmt).all()
    ]


def _spec(scope: str, name: str) -> SecretSpec:
    spec = SECRET_SPECS.get((scope, name))
    if spec is None:
        raise ApiError(404, "unknown_secret", f"unknown secret {scope}/{name}")
    return spec


async def _request_test(state: AppState, auth: AuthContext, scope: str, name: str, version: int) -> str:
    request_id = uuid.uuid4().hex
    message = SecretTestRequest(
        request_id=request_id,
        scope=scope,
        name=name,
        version=version,
        requested_by=auth.username,
        requested_at=utcnow(),
    )
    try:
        await publish(
            state.redis,
            Stream.SECRET_TEST_REQUEST,
            message,
            maxlen=state.stream_maxlen(Stream.SECRET_TEST_REQUEST.value),
        )
    except RedisError:
        raise ApiError(
            503, "publish_failed", "stored, but the owner test request could not be published; save again"
        ) from None
    return request_id


_EMPTY_VERSION: dict[str, Any] = {
    "version": None,
    "status": None,
    "last4": None,
    "fingerprint": None,
    "reason": None,
    "details": {},
    "checked_at": None,
    "created_by": None,
    "created_at": None,
}


@router.get("")
async def list_secrets(request: Request, _ctx: AuthContext = Depends(require_session)) -> dict[str, Any]:
    """One row per known secret: the metadata of its latest version (so a pending or rejected rotation
    is visible) plus `active`, the version services currently use (None when no version is active)."""
    rows = await get_state(request).db(secret_metadata)
    result = []
    for spec in SECRET_SPECS.values():
        versions = [r for r in rows if (r["scope"], r["name"]) == (spec.scope, spec.name)]
        active = next((r for r in versions if r["status"] == "active"), None)
        latest = versions[0] if versions else _EMPTY_VERSION
        result.append(
            {
                **latest,
                "scope": spec.scope,
                "name": spec.name,
                "label": spec.label,
                "fields": list(spec.fields),
                "primary_field": spec.primary_field,
                "active": (
                    {k: active[k] for k in ("version", "last4", "fingerprint", "checked_at", "details")}
                    if active
                    else None
                ),
            }
        )
    return {"secrets": result}


@router.get("/public-keys")
async def public_keys(request: Request, _ctx: AuthContext = Depends(require_session)) -> dict[str, str]:
    return {str(scope): key for scope, key in get_state(request).public_keys.items()}


@router.post("/{scope}/{name}", status_code=201)
async def write_secret(
    scope: str,
    name: str,
    body: SecretWriteRequest,
    request: Request,
    ctx: AuthContext = Depends(require_session),
) -> dict[str, Any]:
    spec = _spec(scope, name)
    require_step_up(ctx)
    try:
        blob = parse_sealed_blob(body.sealed_blob)
    except SealedBlobError as exc:
        raise ApiError(
            422,
            "validation_error",
            "the sealed blob is malformed",
            errors=[{"loc": ["body", "sealed_blob"], "msg": str(exc)}],
        ) from None
    state = get_state(request)

    def write(session: Session) -> int:
        session.execute(
            text("SELECT pg_advisory_xact_lock(hashtext(:key))"), {"key": f"hdt.secret.{scope}.{name}"}
        )
        latest = session.scalar(
            select(func.max(SecretRow.version)).where(SecretRow.scope == scope, SecretRow.name == name)
        )
        previous = next(
            iter(r for r in secret_metadata(session, scope, name) if r["status"] == "active"), None
        )
        version = (latest or 0) + 1
        insert_pending_secret(
            session,
            scope=scope,
            name=name,
            version=version,
            sealed_blob=blob,
            last4=body.last4,
            fingerprint=body.fingerprint,
            created_by=ctx.username,
        )
        write_audit(
            session,
            user=ctx.username,
            ip=ctx.ip,
            action="secret.rotate" if previous else "secret.add",
            section=AUDIT_SECTION,
            diff={
                "scope": spec.scope,
                "name": spec.name,
                "version": version,
                "last4": body.last4,
                "fingerprint": body.fingerprint,
                "previous": (
                    {k: previous[k] for k in ("version", "last4", "fingerprint")} if previous else None
                ),
            },
        )
        return version

    version = await state.db(write)
    request_id = await _request_test(state, ctx, scope, name, version)
    return {"scope": scope, "name": name, "version": version, "status": "pending", "request_id": request_id}


@router.post("/{scope}/{name}/test", status_code=202)
async def retest_secret(
    scope: str, name: str, request: Request, ctx: AuthContext = Depends(require_session)
) -> dict[str, Any]:
    _spec(scope, name)
    state = get_state(request)

    def read(session: Session) -> int | None:
        active = next((r for r in secret_metadata(session, scope, name) if r["status"] == "active"), None)
        if active is not None:
            write_audit(
                session,
                user=ctx.username,
                ip=ctx.ip,
                action="secret.test",
                section=AUDIT_SECTION,
                diff={"scope": scope, "name": name, "version": active["version"], "last4": active["last4"]},
            )
        return active["version"] if active else None

    version = await state.db(read)
    if version is None:
        raise ApiError(404, "no_active_version", f"{scope}/{name} has no active version to test")
    request_id = await _request_test(state, ctx, scope, name, version)
    return {"scope": scope, "name": name, "version": version, "request_id": request_id}
