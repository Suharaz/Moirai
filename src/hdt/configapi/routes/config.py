"""`/config`: read, save (new immutable version) and roll back any section."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query, Request
from pydantic import BaseModel, ConfigDict, Field, PositiveInt

from hdt.configapi.auth import AuthContext, require_session
from hdt.configapi.context import get_state
from hdt.configapi.errors import ApiError
from hdt.configapi.sections import save_section, version_dict
from hdt.settings import store
from hdt.settings.ceilings import (
    ACCOUNT_STATE_MAX_AGE_S,
    DAILY_LOSS_KILL_FLOOR,
    LEVERAGE_MAX,
    LIQ_DISTANCE_MULT_MIN,
    MAX_ENTRY_DISTANCE_ATR,
    MAX_MARGIN_PCT,
    MAX_OI_FRAC,
    MAX_POSITIONS,
    MAX_POSITIONS_PER_COIN,
    MAX_POSITIONS_PER_NARRATIVE,
    MAX_VOLUME_1H_FRAC,
    MIN_RR,
    RISK_PCT_MAX,
    SAME_DIRECTION_RISK_MAX,
    SIZE_MULTIPLIER_MAX,
)
from hdt.settings.schemas import SECTION_MODELS, Section
from hdt.settings.versions import UnknownVersionError, active_versions

router = APIRouter(prefix="/config", tags=["config"])

# Hard ceilings (hdt.settings.ceilings) rendered as JSON-schema bounds, mirroring check_risk_limits.
RISK_CEILING_BOUNDS: dict[str, dict[str, float]] = {
    "leverage_max": {"minimum": 1, "maximum": LEVERAGE_MAX},
    "risk_pct": {"exclusiveMinimum": 0, "maximum": RISK_PCT_MAX},
    "max_positions": {"minimum": 1, "maximum": MAX_POSITIONS},
    "max_positions_per_coin": {"minimum": 1, "maximum": MAX_POSITIONS_PER_COIN},
    "same_direction_risk_max": {"exclusiveMinimum": 0, "maximum": SAME_DIRECTION_RISK_MAX},
    "daily_loss_kill": {"minimum": DAILY_LOSS_KILL_FLOOR, "exclusiveMaximum": 0},
    "max_margin_pct": {"exclusiveMinimum": 0, "maximum": MAX_MARGIN_PCT},
    "max_oi_frac": {"exclusiveMinimum": 0, "maximum": MAX_OI_FRAC},
    "max_volume_1h_frac": {"exclusiveMinimum": 0, "maximum": MAX_VOLUME_1H_FRAC},
    "liq_distance_mult": {"minimum": LIQ_DISTANCE_MULT_MIN},
    "max_positions_per_narrative": {"minimum": 1, "maximum": MAX_POSITIONS_PER_NARRATIVE},
    "min_rr": {"minimum": MIN_RR},
    "max_entry_distance_atr": {"exclusiveMinimum": 0, "maximum": MAX_ENTRY_DISTANCE_ATR},
    "account_state_max_age_s": {"minimum": 1, "maximum": ACCOUNT_STATE_MAX_AGE_S},
}
MODE_BOUNDS: dict[str, dict[str, float]] = {
    "size_multiplier": {"exclusiveMinimum": 0, "maximum": SIZE_MULTIPLIER_MAX}
}


class SaveRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    payload: dict[str, Any]
    reason: str = Field(min_length=1, max_length=500)
    parent_id: PositiveInt | None


class RollbackRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version_id: PositiveInt
    reason: str = Field(min_length=1, max_length=500)


def section_json_schema(section: Section) -> dict[str, Any]:
    schema = SECTION_MODELS[section].model_json_schema()
    bounds = (
        RISK_CEILING_BOUNDS if section is Section.RISK else MODE_BOUNDS if section is Section.MODE else {}
    )
    for name, extra in bounds.items():
        schema["properties"][name].update(extra)
    return schema


@router.get("/sections")
async def sections(request: Request, _ctx: AuthContext = Depends(require_session)) -> dict[str, Any]:
    active = await get_state(request).db(active_versions)
    return {
        "sections": [
            {
                "section": section.value,
                "active_version_id": active[section].id if section in active else None,
                "updated_at": active[section].created_at if section in active else None,
                "author": active[section].author if section in active else None,
            }
            for section in Section
        ]
    }


@router.get("/{section}")
async def get_section(
    section: Section, request: Request, _ctx: AuthContext = Depends(require_session)
) -> dict[str, Any]:
    active = await get_state(request).db(lambda s: store.get_active(s, section))
    return {"section": section.value, "active": version_dict(active) if active else None}


@router.get("/{section}/versions")
async def versions(
    section: Section,
    request: Request,
    limit: int = Query(50, ge=1, le=500),
    _ctx: AuthContext = Depends(require_session),
) -> dict[str, Any]:
    def read(session: Any) -> tuple[int | None, list[Any]]:
        active = store.get_active(session, section)
        return (active.id if active else None), store.list_versions(session, section, limit)

    active_id, items = await get_state(request).db(read)
    return {
        "section": section.value,
        "active_version_id": active_id,
        "versions": [version_dict(v) for v in items],
    }


@router.get("/{section}/schema")
async def schema(section: Section, _ctx: AuthContext = Depends(require_session)) -> dict[str, Any]:
    return section_json_schema(section)


@router.post("/{section}", status_code=201)
async def save(
    section: Section, body: SaveRequest, request: Request, ctx: AuthContext = Depends(require_session)
) -> dict[str, Any]:
    result = await save_section(
        get_state(request), ctx, section, body.payload, reason=body.reason, parent_id=body.parent_id
    )
    return {**version_dict(result.version), **result.extra}


@router.post("/{section}/rollback", status_code=201)
async def rollback(
    section: Section, body: RollbackRequest, request: Request, ctx: AuthContext = Depends(require_session)
) -> dict[str, Any]:
    state = get_state(request)

    def read(session: Any) -> tuple[Any, int | None]:
        target = store.get_version(session, body.version_id, section)
        active = store.get_active(session, section)
        return target, (active.id if active else None)

    try:
        target, active_id = await state.db(read)
    except UnknownVersionError as exc:
        raise ApiError(404, "unknown_version", str(exc)) from None
    result = await save_section(
        state, ctx, section, target.payload, reason=body.reason, parent_id=active_id, rollback_of=target.id
    )
    return {**version_dict(result.version), **result.extra}
