"""All `/api` routes and the section save policies they rely on."""

from __future__ import annotations

from fastapi import APIRouter

from hdt.configapi.routes import (
    audit,
    auth,
    cmc,
    config,
    controls,
    golive,
    lessons,
    mode,
    models,
    risk,
    secrets,
)
from hdt.configapi.sections import register_policy
from hdt.settings.schemas import Section


def register_section_policies() -> None:
    """Section rules applied by every save path (`/config/{section}`, dedicated routes, rollbacks).
    `binance` needs no rule beyond its schema; `models` and `council` saves are refused while the run mode is
    live (`mode.LivePinnedPolicy`)."""
    register_policy(Section.MODELS, mode.LivePinnedPolicy(models.ModelsPolicy()))
    register_policy(Section.COUNCIL, mode.LivePinnedPolicy())
    register_policy(Section.CMC, cmc.CmcPolicy())
    register_policy(Section.RISK, risk.RiskPolicy())
    register_policy(Section.MODE, mode.ModePolicy())
    register_policy(Section.GOLIVE, golive.GoLivePolicy())


def api_router() -> APIRouter:
    register_section_policies()
    router = APIRouter(prefix="/api")
    for module in (auth, config, models, secrets, cmc, mode, golive, controls, audit, lessons):
        router.include_router(module.router)
    return router
