"""`/cmc/projection` and the `cmc` section rule: a route config projecting above 90 % of the monthly quota
cannot be saved.

Inputs from phase 02 when available:
- measured credits per call from the credit meter table `cmc_credit_usage(date, route, credits, calls)`
  over the last 7 days (`route` holds the route id or the route name),
- the real monthly quota `credit_limit_monthly` reported by the `data/cmc_api_key` owner check
  (`/v1/key/info`); otherwise the design quota from `config/settings.yaml`.
"""

from __future__ import annotations

from dataclasses import asdict
from typing import Any, Final

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, ConfigDict, ValidationError
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from hdt.configapi.auth import AuthContext, require_session
from hdt.configapi.cmc_projection import CreditProjection, project_credits
from hdt.configapi.context import AppState, get_state
from hdt.configapi.errors import ApiError, validation_error
from hdt.configapi.sections import SaveContext, SectionPolicy
from hdt.core.config import StaticConfig
from hdt.db.models.settings import SecretRow
from hdt.settings.schemas import CmcSection, Section, SectionModel, parse_section

router = APIRouter(prefix="/cmc", tags=["cmc"])
MEASURED_WINDOW_DAYS: Final[int] = 7
QUOTA_DETAIL: Final[str] = "credit_limit_monthly"


class ProjectionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    payload: dict[str, Any]


def measured_credits_per_call(session: Session, static: StaticConfig) -> dict[int, float]:
    """Credits per call by route id from the phase 02 credit meter; empty until that table exists."""
    if session.scalar(text("SELECT to_regclass('public.cmc_credit_usage')")) is None:
        return {}
    rows = session.execute(
        text(
            "SELECT route::text AS route, sum(credits) AS credits, sum(calls) AS calls FROM cmc_credit_usage "
            "WHERE date >= current_date - make_interval(days => :days) GROUP BY route::text"
        ),
        {"days": MEASURED_WINDOW_DAYS},
    ).all()
    by_name = {route.name: route.id for route in static.cmc_routes.routes}
    measured: dict[int, float] = {}
    for row in rows:
        route_id = int(row.route) if row.route.isdigit() else by_name.get(row.route)
        if route_id is not None and row.calls:
            measured[route_id] = float(row.credits) / float(row.calls)
    return measured


def key_info_quota(session: Session) -> int | None:
    details = session.scalar(
        select(SecretRow.check_details).where(
            SecretRow.scope == "data", SecretRow.name == "cmc_api_key", SecretRow.status == "active"
        )
    )
    value = (details or {}).get(QUOTA_DETAIL)
    return int(value) if isinstance(value, int | float) and value > 0 else None


async def projection_for(state: AppState, section: CmcSection) -> CreditProjection:
    measured, quota = await state.db(
        lambda s: (measured_credits_per_call(s, state.static), key_info_quota(s))
    )
    try:
        return project_credits(section, state.static, measured_per_call=measured, quota=quota)
    except ValueError as exc:
        raise validation_error(exc) from None


def projection_body(projection: CreditProjection) -> dict[str, Any]:
    body = asdict(projection)
    body["per_route"] = [asdict(route) for route in projection.per_route]
    return body


class CmcPolicy(SectionPolicy):
    async def check(self, ctx: SaveContext, model: SectionModel) -> None:
        if not isinstance(model, CmcSection):
            raise TypeError("cmc policy received another section")
        projection = await projection_for(ctx.state, model)
        if projection.blocked:
            raise ApiError(
                422,
                "credit_projection_exceeded",
                f"projected {projection.monthly_projection:,.0f} credits/month is "
                f"{projection.fraction:.1%} of the quota (limit {projection.block_fraction:.0%})",
                projection=projection_body(projection),
            )


@router.post("/projection")
async def projection(
    body: ProjectionRequest, request: Request, _ctx: AuthContext = Depends(require_session)
) -> dict[str, Any]:
    state = get_state(request)
    try:
        section = parse_section(Section.CMC, body.payload)
    except (ValidationError, ValueError) as exc:
        raise validation_error(exc) from None
    if not isinstance(section, CmcSection):
        raise TypeError("unexpected cmc section model")
    return projection_body(await projection_for(state, section))
