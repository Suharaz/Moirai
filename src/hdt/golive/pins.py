"""What a G4 pass is bound to (`golive_g4_finals.pins`) and whether today's configuration still matches it.

The final evaluation records, per target type, the pins its window ran with (`hdt.golive.g4.pinned_check`)
plus the static digests it loaded:
- `models`, `council`: the config version ids (None: no console version was active);
- `agents`: agent -> `{model_slug, agent_version}` (`agent_versions.version` as a decimal string);
- `static`: sha256 of the static defaults that shape a forecast (`scanner.yaml`, `scoring.yaml` and the
  `council.yaml` defaults under the console `council` section). Decision cards do not record these files, so
  the digests bind the files the final evaluation ran with, not strictly those of the window.

`pin_changes` compares by content, never by id: a config version matches when its payload digest equals
the pinned version's (so a rollback to the pinned content restores the binding), and an agent matches when
its newest `agent_versions` row has the model slug, provider preferences, prompt hash and skill commit of
the pinned version. Anything that cannot be resolved (no pins, a pinned id or version that is not found)
counts as changed.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from hdt.core.config import (
    ScannerFile,
    ScoringFile,
    StaticConfig,
    scanner_config,
    scoring_config,
    static_config,
)
from hdt.core.ids import canonical_sha256
from hdt.db.models.settings import AgentVersionRow, ConfigVersionRow
from hdt.settings import store
from hdt.settings.schemas import Section

PINNED_SECTIONS = ("models", "council")
"""Console config sections a G4 pass is bound to: the forecaster that was evaluated."""
UNPINNED = "unpinned"


def static_digests(static: StaticConfig, scanner: ScannerFile, scoring: ScoringFile) -> dict[str, str]:
    """sha256 of the static defaults a forecast depends on, as loaded by this process."""
    return {
        "scanner": canonical_sha256(scanner.model_dump(mode="json")),
        "scoring": canonical_sha256(scoring.model_dump(mode="json")),
        "council": canonical_sha256(static.council.model_dump(mode="json")),
    }


def current_static_digests() -> dict[str, str]:
    """`static_digests` of this process's cached static config (`hdt.core.config`)."""
    return static_digests(static_config(), scanner_config(), scoring_config())


def _pinned_digest(session: Session, section: str, version_id: Any) -> str | None:
    if not isinstance(version_id, int) or isinstance(version_id, bool):
        return None
    v = ConfigVersionRow
    return session.scalar(select(v.payload_sha256).where(v.id == version_id, v.section == section))


def _section_changed(session: Session, section: str, pinned_id: Any) -> bool:
    active = store.get_active(session, Section(section))
    if pinned_id is None:
        return active is not None
    digest = _pinned_digest(session, section, pinned_id)
    return digest is None or active is None or active.payload_sha256 != digest


def _agent_identity(row: AgentVersionRow | None) -> tuple[str, str, str | None, str | None] | None:
    if row is None:
        return None
    return (row.model_slug, row.provider_prefs_hash, row.prompt_hash, row.skill_commit)


def _agent_changed(session: Session, agent: str, pin: Any) -> bool:
    if not isinstance(pin, Mapping):
        return True
    version = pin.get("agent_version")
    try:
        number = int(str(version))
    except ValueError:
        return True
    a = AgentVersionRow
    pinned = session.get(a, (agent, number))
    if pinned is None or pinned.model_slug != pin.get("model_slug"):
        return True
    newest = session.scalar(
        select(a).where(
            a.agent == agent,
            a.version == select(func.max(a.version)).where(a.agent == agent).scalar_subquery(),
        )
    )
    return _agent_identity(newest) != _agent_identity(pinned)


def pin_changes(session: Session, pins: Mapping[str, Any] | None, *, static: Mapping[str, str]) -> list[str]:
    """What differs between `pins` (one `golive_g4_finals.pins`) and today's configuration: `unpinned`,
    `config:<section>`, `agent:<name>`, `static:<file>`. Empty: today's configuration is the pinned one."""
    if pins is None:
        return [UNPINNED]
    changes = [f"config:{s}" for s in PINNED_SECTIONS if _section_changed(session, s, pins.get(s))]
    agents = pins.get("agents")
    if isinstance(agents, Mapping):
        changes += [f"agent:{a}" for a in sorted(agents) if _agent_changed(session, str(a), agents[a])]
    else:
        changes.append("agents")
    pinned_static = pins.get("static")
    if isinstance(pinned_static, Mapping):
        changes += [f"static:{name}" for name in sorted(static) if pinned_static.get(name) != static[name]]
    else:
        changes.append("static")
    return changes
