"""Persistence of immutable config versions and `active_config` (Postgres, synchronous sessions).

Writers (config-api) call `create_version` inside a transaction: a per-section advisory lock serializes
concurrent saves, the caller's `parent_id` must equal the currently active version (optimistic
concurrency), and the new version becomes active in the same transaction. Versions are never updated or
deleted; a rollback is a new version carrying an older payload.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from hdt.core.clock import utcnow
from hdt.core.config import StaticConfig
from hdt.core.ids import canonical_sha256
from hdt.db.models.settings import ActiveConfigRow, AgentVersionRow, ConfigVersionRow
from hdt.settings.schemas import SECTION_SCHEMA_VERSION, ModelsSection, RoleModelConfig, Section, SectionModel
from hdt.settings.schemas import seed_payloads as _seed_payloads
from hdt.settings.versions import ConfigVersion, UnknownVersionError

SEED_AUTHOR = "seed"


class StaleParentError(Exception):
    """The caller edited a version that is no longer active."""

    def __init__(self, section: Section, parent_id: int | None, active_id: int | None) -> None:
        super().__init__(f"section '{section.value}': parent {parent_id} is stale, active is {active_id}")
        self.section, self.parent_id, self.active_id = section, parent_id, active_id


class NoChangeError(Exception):
    """The submitted payload equals the active version."""


def _lock_section(session: Session, section: Section) -> None:
    session.execute(
        text("SELECT pg_advisory_xact_lock(hashtext(:key))"), {"key": f"hdt.config.{section.value}"}
    )


def get_active(session: Session, section: Section) -> ConfigVersion | None:
    row = session.scalar(
        select(ConfigVersionRow)
        .join(ActiveConfigRow, ActiveConfigRow.version_id == ConfigVersionRow.id)
        .where(ActiveConfigRow.section == section.value)
    )
    return ConfigVersion.from_row(row) if row is not None else None


def get_version(session: Session, version_id: int, section: Section | None = None) -> ConfigVersion:
    row = session.get(ConfigVersionRow, version_id)
    if row is None or (section is not None and row.section != section.value):
        raise UnknownVersionError(
            f"config version {version_id} not found" + (f" in '{section}'" if section else "")
        )
    return ConfigVersion.from_row(row)


def list_versions(session: Session, section: Section, limit: int = 50) -> list[ConfigVersion]:
    rows = session.scalars(
        select(ConfigVersionRow)
        .where(ConfigVersionRow.section == section.value)
        .order_by(ConfigVersionRow.id.desc())
        .limit(limit)
    ).all()
    return [ConfigVersion.from_row(row) for row in rows]


def _set_active(session: Session, section: Section, version_id: int, by: str) -> None:
    now = utcnow()
    stmt = insert(ActiveConfigRow).values(
        section=section.value, version_id=version_id, activated_at=now, activated_by=by
    )
    session.execute(
        stmt.on_conflict_do_update(
            index_elements=[ActiveConfigRow.section],
            set_={"version_id": version_id, "activated_at": now, "activated_by": by},
        )
    )


def create_version(
    session: Session,
    section: Section,
    model: SectionModel,
    *,
    author: str,
    reason: str,
    parent_id: int | None,
) -> ConfigVersion:
    """Insert a new immutable version and make it active; raises `StaleParentError` / `NoChangeError`."""
    _lock_section(session, section)
    current = get_active(session, section)
    current_id = current.id if current else None
    if parent_id != current_id:
        raise StaleParentError(section, parent_id, current_id)
    payload: dict[str, Any] = model.model_dump(mode="json")
    digest = canonical_sha256(payload)
    if current is not None and current.payload_sha256 == digest:
        raise NoChangeError(f"section '{section.value}' already has this content (version {current.id})")
    row = ConfigVersionRow(
        section=section.value,
        payload_json=payload,
        payload_sha256=digest,
        schema_version=SECTION_SCHEMA_VERSION,
        author=author,
        reason=reason,
        created_at=utcnow(),
        parent_id=current_id,
    )
    session.add(row)
    session.flush()
    _set_active(session, section, row.id, author)
    return ConfigVersion.from_row(row)


def activate(session: Session, section: Section, version_id: int, *, by: str) -> bool:
    """Make an existing version active; idempotent (returns False when it already is)."""
    _lock_section(session, section)
    get_version(session, version_id, section)
    current = get_active(session, section)
    if current is not None and current.id == version_id:
        return False
    _set_active(session, section, version_id, by)
    return True


def seed_if_missing(session: Session, static: StaticConfig) -> list[ConfigVersion]:
    """Create version 1 from `config/*.yaml` for every seedable section that has no version yet."""
    created: list[ConfigVersion] = []
    for section, model in _seed_payloads(static).items():
        _lock_section(session, section)
        exists = session.scalar(select(func.count()).where(ConfigVersionRow.section == section.value))
        if exists:
            continue
        created.append(
            create_version(
                session, section, model, author=SEED_AUTHOR, reason="seed from config/*.yaml", parent_id=None
            )
        )
    return created


# --------------------------------------------------------------------------- agent versions


def provider_prefs_hash(config: RoleModelConfig) -> str:
    """Hash of how a role's requests are routed. OpenRouter: the `provider` object plus the ordered
    `fallback_models`; without fallbacks it is the hash of the `provider` object alone, so versions recorded
    before fallbacks (and before gateways) were hashed keep matching. DeepSeek: the gateway and the thinking
    mode (no provider object is sent). Shared by `hdt.agents.versions.VersionKey`."""
    if config.gateway == "deepseek":
        return canonical_sha256({"gateway": config.gateway, "thinking": config.thinking})
    prefs = config.provider_preferences()
    if config.fallback_models:
        return canonical_sha256({"provider": prefs, "fallback_models": list(config.fallback_models)})
    return canonical_sha256(prefs)


def record_agent_versions(
    session: Session,
    models: ModelsSection,
    *,
    config_version_id: int,
    author: str,
) -> list[AgentVersionRow]:
    """New `agent_versions` rows for roles whose model or provider preferences changed.

    Prompt hash and skill commit are carried over from the role's previous version (phase 05 creates a
    new version when those change). Weight handling of a new version belongs to phase 08.
    """
    created: list[AgentVersionRow] = []
    latest = _latest_agent_versions(session)
    for role, config in sorted(models.roles.items()):
        prefs_hash = provider_prefs_hash(config)
        previous = latest.get(role)
        if previous is not None and (previous.model_slug, previous.provider_prefs_hash) == (
            config.model,
            prefs_hash,
        ):
            continue
        row = AgentVersionRow(
            agent=role,
            version=(previous.version + 1) if previous else 1,
            model_slug=config.model,
            provider_prefs_hash=prefs_hash,
            prompt_hash=previous.prompt_hash if previous else None,
            skill_commit=previous.skill_commit if previous else None,
            config_version_id=config_version_id,
            created_by=author,
            created_at=utcnow(),
        )
        session.add(row)
        created.append(row)
    session.flush()
    return created


def _latest_agent_versions(session: Session) -> Mapping[str, AgentVersionRow]:
    latest = (
        select(AgentVersionRow.agent, func.max(AgentVersionRow.version).label("version"))
        .group_by(AgentVersionRow.agent)
        .subquery()
    )
    rows = session.scalars(
        select(AgentVersionRow).join(
            latest, (AgentVersionRow.agent == latest.c.agent) & (AgentVersionRow.version == latest.c.version)
        )
    ).all()
    return {row.agent: row for row in rows}
