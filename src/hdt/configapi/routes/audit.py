"""`/audit`: filtered, newest-first view of the append-only audit log."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, Query, Request

from hdt.configapi.audit import audit_dict, query_audit
from hdt.configapi.auth import AuthContext, require_session
from hdt.configapi.context import get_state

router = APIRouter(prefix="/audit", tags=["audit"])


@router.get("")
async def audit(
    request: Request,
    section: str | None = None,
    user: str | None = None,
    action: str | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int = Query(100, ge=1, le=1000),
    _ctx: AuthContext = Depends(require_session),
) -> dict[str, Any]:
    rows = await get_state(request).db(
        lambda s: [
            audit_dict(row)
            for row in query_audit(
                s, section=section, user=user, action=action, since=since, until=until, limit=limit
            )
        ]
    )
    return {"items": rows}
