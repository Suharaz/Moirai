"""`/models`: model catalogs (OpenRouter, DeepSeek), model test requests, and the `models` section rules.

Rules on save (console and direct API alike):
- every model of a changed role must be in its gateway's catalog (OpenRouter: structured-output models
  only; DeepSeek: the models priced in `config/deepseek.yaml`),
- a role whose model changed must have passed a model test for that exact role and model in the last
  24 h (a rollback restores previously tested models and is exempt),
- roles whose model or provider preferences changed get a new `agent_versions` row.
"""

from __future__ import annotations

import uuid
from datetime import timedelta
from typing import Any, Final

from fastapi import APIRouter, Depends, Query, Request
from pydantic import BaseModel, ConfigDict, ValidationError
from redis.exceptions import RedisError
from sqlalchemy import select
from sqlalchemy.orm import Session

from hdt.configapi.audit import write_audit
from hdt.configapi.auth import AuthContext, require_session
from hdt.configapi.context import AppState, get_state
from hdt.configapi.errors import ApiError
from hdt.configapi.sections import SaveContext, SectionPolicy
from hdt.contracts.streams import Stream
from hdt.core.clock import utcnow
from hdt.core.config import LlmRole
from hdt.core.streams import publish
from hdt.db.models.settings import ModelTestRow
from hdt.llm.deepseek import deepseek_catalog
from hdt.llm.openrouter_catalog import Catalog, CatalogUnavailableError
from hdt.settings import store
from hdt.settings.schemas import Gateway, ModelsSection, RoleModelConfig, Section, SectionModel
from hdt.settings.versions import ConfigVersion
from hdt.vault.owner import ModelTestRequest

router = APIRouter(prefix="/models", tags=["models"])
MODEL_TEST_MAX_AGE: Final[timedelta] = timedelta(hours=24)
CATALOG_NAMES: Final[dict[Gateway, str]] = {
    "openrouter": "OpenRouter structured-output",
    "deepseek": "DeepSeek model",
}


class ModelTestBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    role: LlmRole
    config: RoleModelConfig


async def load_catalog(state: AppState, gateway: Gateway = "openrouter", *, refresh: bool = False) -> Catalog:
    if gateway == "deepseek":
        try:
            return deepseek_catalog()
        except (OSError, ValueError) as exc:  # pydantic.ValidationError is a ValueError
            raise ApiError(
                503, "catalog_unavailable", f"config/deepseek.yaml is unusable: {type(exc).__name__}"
            ) from None
    try:
        return await state.catalog.get(refresh=refresh)
    except CatalogUnavailableError as exc:
        raise ApiError(503, "catalog_unavailable", str(exc)) from None


def missing_slugs(catalog: Catalog, config: RoleModelConfig) -> list[str]:
    return [slug for slug in config.slugs() if catalog.get(slug) is None]


def changed_roles(active: ConfigVersion | None, models: ModelsSection) -> dict[str, RoleModelConfig]:
    """Roles whose config differs from the active version. Stored roles are compared parsed, so a field
    added later with its default (such as `gateway`) does not make an unchanged role look changed."""
    previous: dict[str, Any] = dict((active.payload.get("roles") or {}) if active else {})
    return {
        role: config for role, config in models.roles.items() if _stored_role(previous.get(role)) != config
    }


def _stored_role(payload: object) -> RoleModelConfig | None:
    if payload is None:
        return None
    try:
        return RoleModelConfig.model_validate(payload)
    except ValidationError:
        return None


class ModelsPolicy(SectionPolicy):
    async def check(self, ctx: SaveContext, model: SectionModel) -> None:
        if not isinstance(model, ModelsSection):
            raise TypeError("models policy received another section")
        changed = changed_roles(ctx.active, model)
        if not changed:
            return
        catalogs = {
            gateway: await load_catalog(ctx.state, gateway)
            for gateway in sorted({config.gateway for config in changed.values()})
        }
        errors = [
            {
                "loc": ["payload", "roles", role, "model"],
                "msg": f"not in the {CATALOG_NAMES[config.gateway]} catalog: {slug}",
            }
            for role, config in changed.items()
            for slug in missing_slugs(catalogs[config.gateway], config)
        ]
        if errors:
            raise ApiError(422, "validation_error", "unknown model slug", errors=errors)
        if ctx.rollback_of is not None:
            return
        previous = (ctx.active.payload.get("roles") or {}) if ctx.active else {}
        new_models = {
            role: config.model
            for role, config in changed.items()
            if (previous.get(role) or {}).get("model") != config.model
        }
        if new_models:
            untested = await ctx.state.db(lambda s: _untested(s, new_models))
            if untested:
                raise ApiError(
                    409,
                    "model_test_required",
                    "run a passing model test for each new model before saving",
                    roles=untested,
                )

    def after_create(
        self, session: Session, ctx: SaveContext, version: ConfigVersion, model: SectionModel
    ) -> dict[str, Any]:
        if not isinstance(model, ModelsSection):
            raise TypeError("models policy received another section")
        rows = store.record_agent_versions(
            session, model, config_version_id=version.id, author=ctx.auth.username
        )
        return {
            "agent_versions": [
                {"agent": row.agent, "version": row.version, "model_slug": row.model_slug} for row in rows
            ]
        }


def _untested(session: Session, new_models: dict[str, str]) -> list[str]:
    since = utcnow() - MODEL_TEST_MAX_AGE
    untested = []
    for role, slug in sorted(new_models.items()):
        passed = session.scalar(
            select(ModelTestRow.request_id)
            .where(
                ModelTestRow.role == role,
                ModelTestRow.model_slug == slug,
                ModelTestRow.status == "done",
                ModelTestRow.schema_valid.is_(True),
                ModelTestRow.completed_at >= since,
            )
            .limit(1)
        )
        if passed is None:
            untested.append(role)
    return untested


def _test_dict(row: ModelTestRow) -> dict[str, Any]:
    return {
        "request_id": row.request_id,
        "role": row.role,
        "model": row.model_slug,
        "status": row.status,
        "schema_valid": row.schema_valid,
        "latency_ms": row.latency_ms,
        "cost_usd": row.cost_usd,
        "provider": row.provider,
        "model_returned": row.model_returned,
        "error": row.error,
        "requested_at": row.requested_at,
        "completed_at": row.completed_at,
    }


@router.get("/catalog")
async def catalog(
    request: Request,
    refresh: bool = Query(False),
    gateway: Gateway = Query("openrouter"),
    _ctx: AuthContext = Depends(require_session),
) -> dict[str, Any]:
    data = await load_catalog(get_state(request), gateway, refresh=refresh)
    return data.model_dump()


@router.post("/test", status_code=202)
async def request_test(
    body: ModelTestBody, request: Request, ctx: AuthContext = Depends(require_session)
) -> dict[str, Any]:
    state = get_state(request)
    missing = missing_slugs(await load_catalog(state, body.config.gateway), body.config)
    if missing:
        raise ApiError(
            422,
            "validation_error",
            "unknown model slug",
            errors=[
                {"loc": ["body", "config", "model"], "msg": f"not in the catalog: {slug}"} for slug in missing
            ],
        )
    request_id = uuid.uuid4().hex
    now = utcnow()
    config = body.config.model_dump(mode="json")

    def write(session: Session) -> None:
        session.add(
            ModelTestRow(
                request_id=request_id,
                role=body.role,
                model_slug=body.config.model,
                config=config,
                status="pending",
                requested_by=ctx.username,
                requested_at=now,
            )
        )
        write_audit(
            session,
            user=ctx.username,
            ip=ctx.ip,
            action="model.test",
            section=Section.MODELS.value,
            diff={"role": body.role, "model": body.config.model, "request_id": request_id},
        )

    await state.db(write)
    message = ModelTestRequest(
        request_id=request_id, role=body.role, config=body.config, requested_by=ctx.username, requested_at=now
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
            503, "publish_failed", "the model test request could not be published; try again"
        ) from None
    return {"request_id": request_id}


@router.get("/test/{request_id}")
async def model_test_result(
    request_id: str, request: Request, _ctx: AuthContext = Depends(require_session)
) -> dict[str, Any]:
    row = await get_state(request).db(lambda s: s.get(ModelTestRow, request_id))
    if row is None:
        raise ApiError(404, "unknown_request", f"no model test {request_id}")
    return _test_dict(row)
