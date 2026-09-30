"""`/controls`: pause / resume / kill / flatten, published as `ControlCommand` on stream `controls`.

Confirmation flows follow the console mockup: `pause` needs nothing more than the session; `resume` needs
a fresh step-up TOTP; `kill` needs the typed word `KILL` plus step-up; `flatten` needs the typed word
`FLATTEN` plus step-up. Execution applies the command and enforces the resume rules (a daily-loss kill
cannot be cleared within the same UTC day, a reconcile kill not until reconcile is clean).
"""

from __future__ import annotations

import uuid
from typing import Any, Final

from fastapi import APIRouter, Depends, Query, Request
from pydantic import BaseModel, ConfigDict, Field
from redis.exceptions import RedisError
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from hdt.configapi.audit import write_audit
from hdt.configapi.auth import AuthContext, require_session, require_step_up
from hdt.configapi.context import get_state
from hdt.configapi.errors import ApiError
from hdt.contracts.common import Account
from hdt.contracts.streams import Stream
from hdt.core.clock import utcnow
from hdt.core.streams import publish
from hdt.db.models.settings import ControlCommandRow
from hdt.settings.events import ControlAction, ControlCommand

router = APIRouter(prefix="/controls", tags=["controls"])
AUDIT_SECTION: Final[str] = "controls"
CONFIRM_WORDS: Final[dict[str, str]] = {"kill": "KILL", "flatten": "FLATTEN"}
STEP_UP_ACTIONS: Final[frozenset[str]] = frozenset({"resume", "kill", "flatten"})


class ControlRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    account: Account
    action: ControlAction
    reason: str = Field(min_length=1, max_length=500)
    confirm_text: str | None = None


def _command_dict(row: ControlCommandRow) -> dict[str, Any]:
    return {
        "id": row.id,
        "at": row.requested_at,
        "user": row.requested_by,
        "account": row.account,
        "action": row.action,
        "reason": row.reason,
        "command_id": row.command_id,
        "source": row.source,
        "ip": row.ip,
        "stream_id": row.stream_id,
    }


@router.post("", status_code=202)
async def send_control(
    body: ControlRequest, request: Request, ctx: AuthContext = Depends(require_session)
) -> dict[str, Any]:
    word = CONFIRM_WORDS.get(body.action)
    if word is not None and (body.confirm_text or "").strip() != word:
        raise ApiError(
            422, "confirmation_required", f"type {word} to confirm {body.action}", confirm_word=word
        )
    if body.action in STEP_UP_ACTIONS:
        require_step_up(ctx)
    state = get_state(request)
    command = ControlCommand(
        command_id=uuid.uuid4().hex,
        account=body.account,
        action=body.action,
        reason=body.reason,
        requested_by=ctx.username,
        source="console",
        requested_at=utcnow(),
    )

    def record(session: Session) -> None:
        session.add(
            ControlCommandRow(
                command_id=command.command_id,
                account=command.account.value,
                action=command.action,
                reason=command.reason,
                requested_by=command.requested_by,
                source=command.source,
                ip=ctx.ip,
                requested_at=command.requested_at,
            )
        )
        write_audit(
            session,
            user=ctx.username,
            ip=ctx.ip,
            action=f"control.{body.action}",
            section=AUDIT_SECTION,
            diff={"account": body.account.value, "reason": body.reason, "command_id": command.command_id},
        )

    await state.db(record)
    try:
        stream_id = await publish(
            state.redis, Stream.CONTROLS, command, maxlen=state.stream_maxlen(Stream.CONTROLS.value)
        )
    except RedisError:
        raise ApiError(
            503,
            "publish_failed",
            "the command was logged but not delivered; retry, or use the independent hdt-kill path",
            command_id=command.command_id,
        ) from None
    await state.db(
        lambda s: s.execute(
            update(ControlCommandRow)
            .where(ControlCommandRow.command_id == command.command_id)
            .values(stream_id=stream_id)
        )
    )
    return {"command_id": command.command_id, "stream_id": stream_id}


@router.get("/log")
async def control_log(
    request: Request,
    limit: int = Query(100, ge=1, le=1000),
    account: Account | None = None,
    _ctx: AuthContext = Depends(require_session),
) -> dict[str, Any]:
    def read(session: Session) -> list[dict[str, Any]]:
        stmt = select(ControlCommandRow).order_by(
            ControlCommandRow.requested_at.desc(), ControlCommandRow.id.desc()
        )
        if account is not None:
            stmt = stmt.where(ControlCommandRow.account == account.value)
        return [_command_dict(row) for row in session.scalars(stmt.limit(limit)).all()]

    return {"items": await get_state(request).db(read)}
