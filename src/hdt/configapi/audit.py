"""Append-only audit log: who, from where, when, which section, what changed.

Every mutating route writes its audit entry in the same transaction as the change. Secrets appear only
as last4 and fingerprint; every string is additionally passed through the redaction filter.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from hdt.core.clock import utcnow
from hdt.db.models.settings import AuditLogRow
from hdt.vault.redact import redact_value

MAX_DIFF_ENTRIES = 200


def write_audit(
    session: Session,
    *,
    user: str,
    ip: str | None,
    action: str,
    section: str | None,
    diff: Mapping[str, Any],
) -> AuditLogRow:
    row = AuditLogRow(
        at=utcnow(),
        user=user,
        ip=ip,
        action=action,
        section=section,
        diff_redacted=redact_value("diff", dict(diff)),
    )
    session.add(row)
    return row


def _flatten(value: Any, prefix: str, out: dict[str, Any]) -> None:
    if isinstance(value, Mapping) and value:
        for key in sorted(value):
            _flatten(value[key], f"{prefix}.{key}" if prefix else str(key), out)
    elif isinstance(value, list | tuple) and value and all(isinstance(v, Mapping) for v in value):
        keyed = all("id" in v for v in value)
        for index, item in enumerate(value):
            _flatten(item, f"{prefix}[{item['id'] if keyed else index}]", out)
    else:
        out[prefix] = value


def config_diff(old: Mapping[str, Any] | None, new: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Changed leaf paths between two payloads, e.g. `{"path": "max_positions", "old": 5, "new": 4}`."""
    before: dict[str, Any] = {}
    after: dict[str, Any] = {}
    _flatten(old or {}, "", before)
    _flatten(new, "", after)
    changes = [
        {"path": path, "old": before.get(path), "new": after.get(path)}
        for path in sorted(before.keys() | after.keys())
        if before.get(path) != after.get(path)
    ]
    return changes[:MAX_DIFF_ENTRIES]


def audit_dict(row: AuditLogRow) -> dict[str, Any]:
    return {
        "id": row.id,
        "at": row.at,
        "user": row.user,
        "ip": row.ip,
        "action": row.action,
        "section": row.section,
        "diff_redacted": row.diff_redacted,
    }


def query_audit(
    session: Session,
    *,
    section: str | None = None,
    user: str | None = None,
    action: str | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int = 100,
) -> list[AuditLogRow]:
    stmt = select(AuditLogRow).order_by(AuditLogRow.at.desc(), AuditLogRow.id.desc()).limit(limit)
    if section:
        stmt = stmt.where(AuditLogRow.section == section)
    if user:
        stmt = stmt.where(AuditLogRow.user == user)
    if action:
        stmt = stmt.where(AuditLogRow.action.startswith(action, autoescape=True))
    if since:
        stmt = stmt.where(AuditLogRow.at >= since)
    if until:
        stmt = stmt.where(AuditLogRow.at < until)
    return list(session.scalars(stmt).all())
