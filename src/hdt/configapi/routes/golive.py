"""Go-live checklist (`golive` section): who checked each item and when is always set by the server."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, ConfigDict, Field

from hdt.configapi.auth import AuthContext, require_session
from hdt.configapi.context import get_state
from hdt.configapi.sections import SaveContext, SectionPolicy, save_section, version_dict
from hdt.core.clock import utcnow
from hdt.settings import store
from hdt.settings.schemas import GOLIVE_ITEMS, GOLIVE_LABELS, GoLiveSection, Section
from hdt.settings.versions import ConfigVersion

router = APIRouter(prefix="/golive", tags=["golive"])


class ItemState(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    checked: bool


class ChecklistRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[ItemState]
    reason: str = Field(default="go-live checklist update", min_length=1, max_length=500)


class GoLivePolicy(SectionPolicy):
    def normalize(self, ctx: SaveContext, raw: dict[str, Any]) -> dict[str, Any]:
        """Attribute newly checked items to the caller; keep the recorded attribution of the others."""
        if ctx.rollback_of is not None:
            return raw  # a rollback restores the old attribution as it was
        previous: dict[str, dict[str, Any]] = {}
        if ctx.active is not None:
            previous = {item["id"]: item for item in ctx.active.payload.get("items", [])}
        items = raw.get("items")
        if not isinstance(items, list):
            return raw
        now = utcnow().isoformat()
        normalized = []
        for item in items:
            if not isinstance(item, dict) or not isinstance(item.get("checked"), bool):
                return raw  # let schema validation report the malformed item
            before = previous.get(str(item.get("id")), {})
            checked = item["checked"]
            if checked and before.get("checked"):
                by, at = before.get("checked_by"), before.get("checked_at")
            elif checked:
                by, at = ctx.auth.username, now
            else:
                by, at = None, None
            normalized.append({"id": item.get("id"), "checked": checked, "checked_by": by, "checked_at": at})
        return {**raw, "items": normalized}


def checklist_body(version: ConfigVersion | None) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    model = version.model() if version else None
    if isinstance(model, GoLiveSection):
        for item in model.items:
            items.append({"label": GOLIVE_LABELS[item.id], **item.model_dump()})
    else:
        items = [
            {"id": i, "label": label, "checked": False, "checked_by": None, "checked_at": None}
            for i, label in GOLIVE_ITEMS
        ]
    done = sum(1 for item in items if item["checked"])
    return {
        "version_id": version.id if version else None,
        "items": items,
        "done": done,
        "total": len(items),
        "complete": done == len(items),
    }


@router.get("/checklist")
async def get_checklist(request: Request, _ctx: AuthContext = Depends(require_session)) -> dict[str, Any]:
    active = await get_state(request).db(lambda s: store.get_active(s, Section.GOLIVE))
    return checklist_body(active)


@router.post("/checklist", status_code=201)
async def post_checklist(
    body: ChecklistRequest, request: Request, ctx: AuthContext = Depends(require_session)
) -> dict[str, Any]:
    state = get_state(request)
    active = await state.db(lambda s: store.get_active(s, Section.GOLIVE))
    result = await save_section(
        state,
        ctx,
        Section.GOLIVE,
        {"items": [item.model_dump() for item in body.items]},
        reason=body.reason,
        parent_id=active.id if active else None,
    )
    return {**checklist_body(result.version), "version": version_dict(result.version)}
