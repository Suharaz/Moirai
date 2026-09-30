"""Save pipeline shared by every config section (generic `/config/{section}` and dedicated routes).

normalize (policy) -> validate (schemas + hard ceilings + static cross-field rules) -> check (policy:
step-up, live gate, catalog, credit projection) -> one transaction {new immutable version, activate,
policy side effects, audit} -> publish `config_changed`. A direct API call therefore goes through
exactly the same rules as the console.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from pydantic import ValidationError
from redis.exceptions import RedisError
from sqlalchemy.orm import Session

from hdt.configapi.audit import config_diff, write_audit
from hdt.configapi.auth import AuthContext
from hdt.configapi.context import AppState
from hdt.configapi.errors import ApiError, validation_error
from hdt.contracts.streams import Stream
from hdt.settings import store
from hdt.settings.events import publish_config_changed
from hdt.settings.schemas import Section, SectionModel, validate_section, without_retired
from hdt.settings.versions import ConfigVersion

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class SaveContext:
    state: AppState
    auth: AuthContext
    section: Section
    active: ConfigVersion | None
    rollback_of: int | None = None


@dataclass(frozen=True)
class SaveResult:
    version: ConfigVersion
    extra: dict[str, Any] = field(default_factory=dict)


class SectionPolicy:
    """Section-specific rules; the default accepts anything the schema accepts."""

    def normalize(self, ctx: SaveContext, raw: dict[str, Any]) -> dict[str, Any]:
        return raw

    async def check(self, ctx: SaveContext, model: SectionModel) -> None:
        return None

    def after_create(
        self, session: Session, ctx: SaveContext, version: ConfigVersion, model: SectionModel
    ) -> dict[str, Any]:
        """Side effects in the save transaction; the returned dict is added to the response and audit."""
        return {}


_POLICIES: dict[Section, SectionPolicy] = {}
_DEFAULT = SectionPolicy()


def register_policy(section: Section, policy: SectionPolicy) -> None:
    _POLICIES[section] = policy


def policy_for(section: Section) -> SectionPolicy:
    return _POLICIES.get(section, _DEFAULT)


async def save_section(
    state: AppState,
    auth: AuthContext,
    section: Section,
    raw: Any,
    *,
    reason: str,
    parent_id: int | None,
    rollback_of: int | None = None,
) -> SaveResult:
    if not isinstance(raw, dict):
        raise ApiError(
            422,
            "validation_error",
            "payload must be an object",
            errors=[{"loc": ["payload"], "msg": "must be an object"}],
        )
    active = await state.db(lambda s: store.get_active(s, section))
    active_id = active.id if active else None
    if parent_id != active_id:
        raise ApiError(
            409, "stale_parent", "the section changed since it was loaded", active_version_id=active_id
        )
    policy = policy_for(section)
    ctx = SaveContext(state, auth, section, active, rollback_of)
    try:
        model = validate_section(section, policy.normalize(ctx, raw), state.static)
    except (ValidationError, ValueError) as exc:
        raise validation_error(exc) from None
    await policy.check(ctx, model)

    def write(session: Session) -> SaveResult:
        version = store.create_version(
            session, section, model, author=auth.username, reason=reason, parent_id=parent_id
        )
        extra = policy.after_create(session, ctx, version, model)
        write_audit(
            session,
            user=auth.username,
            ip=auth.ip,
            action="config.rollback" if rollback_of is not None else "config.save",
            section=section.value,
            diff={
                "from_version": active_id,
                "to_version": version.id,
                "rollback_of": rollback_of,
                "reason": reason,
                "changes": config_diff(active.payload if active else None, version.payload),
                **extra,
            },
        )
        return SaveResult(version, extra)

    try:
        result = await state.db(write)
    except store.StaleParentError as exc:
        raise ApiError(
            409, "stale_parent", "the section changed since it was loaded", active_version_id=exc.active_id
        ) from None
    except store.NoChangeError as exc:
        raise ApiError(409, "no_change", str(exc)) from None
    await notify_config_changed(state, result.version, active_id)
    return result


async def notify_config_changed(state: AppState, version: ConfigVersion, previous_id: int | None) -> None:
    """Publish `config_changed`; services also reconcile with `active_config`, so a failure only delays."""
    try:
        await publish_config_changed(
            state.redis, version, previous_id, maxlen=state.stream_maxlen(Stream.CONFIG_CHANGED.value)
        )
    except RedisError:
        log.exception(
            "config_changed publish failed; services will pick the version up on reconcile",
            extra={"section": version.section.value, "version_id": version.id},
        )


def version_dict(version: ConfigVersion) -> dict[str, Any]:
    return {
        "id": version.id,
        "section": version.section.value,
        "schema_version": version.schema_version,
        "payload": without_retired(version.section, version.payload),
        "payload_sha256": version.payload_sha256,
        "author": version.author,
        "reason": version.reason,
        "created_at": version.created_at,
        "parent_id": version.parent_id,
    }
