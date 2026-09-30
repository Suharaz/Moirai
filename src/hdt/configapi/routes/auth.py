"""`/auth`: login (password + TOTP), logout, session, step-up."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, Request, Response
from pydantic import BaseModel, ConfigDict, Field

from hdt.configapi import auth
from hdt.configapi.auth import AuthContext, require_session
from hdt.configapi.context import get_state
from hdt.configapi.errors import ApiError
from hdt.configapi.security import clear_session_cookie, client_ip, set_session_cookie

router = APIRouter(prefix="/auth", tags=["auth"])


class LoginRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)

    username: str = Field(min_length=1, max_length=64)
    password: str = Field(min_length=1, max_length=1024)
    totp: str = Field(min_length=6, max_length=6)


class StepUpRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)

    totp: str = Field(min_length=6, max_length=6)


def session_body(ctx: AuthContext) -> dict[str, Any]:
    return {
        "user": ctx.username,
        "csrf_token": ctx.csrf_token,
        "expires_at": ctx.expires_at,
        "step_up_until": ctx.step_up_until,
    }


@router.post("/login")
async def login(body: LoginRequest, request: Request, response: Response) -> dict[str, Any]:
    state = get_state(request)
    ip = client_ip(request, state.settings.trusted_proxies)
    outcome = await state.db(
        lambda s: auth.login(
            state,
            s,
            username=body.username,
            password=body.password,
            code=body.totp,
            ip=ip,
            user_agent=request.headers.get("user-agent"),
        )
    )
    if outcome.status == "locked":
        raise ApiError(423, "locked", "too many failed attempts; the account is temporarily locked")
    if outcome.status != "ok" or outcome.token is None or outcome.context is None:
        raise ApiError(401, "invalid_credentials", "wrong username, password or TOTP code")
    set_session_cookie(response, outcome.token)
    return session_body(outcome.context)


@router.post("/logout")
async def logout(
    request: Request, response: Response, ctx: AuthContext = Depends(require_session)
) -> dict[str, Any]:
    await get_state(request).db(lambda s: auth.logout(s, ctx))
    clear_session_cookie(response)
    return {"status": "signed_out"}


@router.get("/session")
async def session(ctx: AuthContext = Depends(require_session)) -> dict[str, Any]:
    return session_body(ctx)


@router.post("/step-up")
async def step_up(
    body: StepUpRequest, request: Request, ctx: AuthContext = Depends(require_session)
) -> dict[str, Any]:
    state = get_state(request)
    result = await state.db(lambda s: auth.step_up(state, s, ctx, body.totp))
    if result == "locked":
        raise ApiError(423, "locked", "too many failed attempts; the account is locked and signed out")
    if not isinstance(result, datetime):
        raise ApiError(403, "invalid_totp", "wrong or already used TOTP code")
    return {"step_up_until": result}
