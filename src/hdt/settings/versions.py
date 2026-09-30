"""Immutable config versions and per-event pins.

A council event calls `pin_current(session)` once at start and uses only that `ConfigPin` until it ends,
so a config change mid-event never affects it. `ConfigPin.version_ids()` is the `config_version_ids`
map recorded in QuantPacket, DecisionMsg and the decision card; replay rebuilds the exact same pin with
`load_pin(session, config_version_ids)`.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from hdt.core.config import BinanceFile, CmcRoutesFile, CouncilFile, RiskFile, StaticConfig
from hdt.core.ids import canonical_sha256
from hdt.db.models.settings import ActiveConfigRow, ConfigVersionRow
from hdt.settings.schemas import (
    CmcSection,
    GoLiveSection,
    ModelsSection,
    ModeSection,
    Section,
    SectionModel,
    effective_binance,
    effective_cmc_routes,
    effective_council,
    effective_risk,
    parse_section,
)


class SectionNotConfiguredError(LookupError):
    """The section has no active version (e.g. `models` before the first console save)."""


class UnknownVersionError(LookupError):
    """A pinned or requested version id does not exist or belongs to another section."""


@dataclass(frozen=True)
class ConfigVersion:
    id: int
    section: Section
    payload: dict[str, Any] = field(repr=False)
    payload_sha256: str
    schema_version: int
    author: str
    reason: str
    created_at: datetime
    parent_id: int | None

    @classmethod
    def from_row(cls, row: ConfigVersionRow) -> ConfigVersion:
        return cls(
            id=row.id,
            section=Section(row.section),
            payload=dict(row.payload_json),
            payload_sha256=row.payload_sha256,
            schema_version=row.schema_version,
            author=row.author,
            reason=row.reason,
            created_at=row.created_at,
            parent_id=row.parent_id,
        )

    def model(self) -> SectionModel:
        return parse_section(self.section, self.payload)


@dataclass(frozen=True)
class ConfigPin:
    """The config versions one event runs with (immutable)."""

    versions: Mapping[Section, ConfigVersion]

    def version_ids(self) -> dict[str, int]:
        return {section.value: version.id for section, version in sorted(self.versions.items())}

    def config_hash(self) -> str:
        return canonical_sha256(self.version_ids())

    def get(self, section: Section) -> ConfigVersion:
        try:
            return self.versions[section]
        except KeyError:
            raise SectionNotConfiguredError(f"section '{section.value}' has no active version") from None

    def section(self, section: Section) -> SectionModel:
        return self.get(section).model()

    def models(self) -> ModelsSection:
        return _typed(self.section(Section.MODELS), ModelsSection)

    def mode(self) -> ModeSection:
        return _typed(self.section(Section.MODE), ModeSection)

    def golive(self) -> GoLiveSection:
        return _typed(self.section(Section.GOLIVE), GoLiveSection)

    def cmc_routes(self, static: StaticConfig) -> CmcRoutesFile:
        return effective_cmc_routes(_typed(self.section(Section.CMC), CmcSection), static)

    def binance(self, static: StaticConfig) -> BinanceFile:
        return effective_binance(self.section(Section.BINANCE), static)

    def risk(self, static: StaticConfig) -> RiskFile:
        return effective_risk(self.section(Section.RISK), self.section(Section.BINANCE), self.mode(), static)

    def council(self, static: StaticConfig) -> CouncilFile:
        return effective_council(self.section(Section.COUNCIL), static)


def active_versions(session: Session) -> dict[Section, ConfigVersion]:
    """Every section's active version, read in one statement (one consistent snapshot)."""
    rows = session.scalars(
        select(ConfigVersionRow).join(ActiveConfigRow, ActiveConfigRow.version_id == ConfigVersionRow.id)
    ).all()
    return {Section(row.section): ConfigVersion.from_row(row) for row in rows}


def _typed[T: SectionModel](model: SectionModel, cls: type[T]) -> T:
    if not isinstance(model, cls):
        raise TypeError(f"expected {cls.__name__}, got {type(model).__name__}")
    return model


def pin_current(session: Session) -> ConfigPin:
    """Pin the currently active version of every configured section for one event."""
    return ConfigPin(active_versions(session))


def load_pin(session: Session, version_ids: Mapping[str, int]) -> ConfigPin:
    """Rebuild a recorded pin exactly (replay); every id must exist and belong to its section."""
    wanted = {Section(name): int(version_id) for name, version_id in version_ids.items()}
    rows = session.scalars(select(ConfigVersionRow).where(ConfigVersionRow.id.in_(wanted.values()))).all()
    by_id = {row.id: row for row in rows}
    versions: dict[Section, ConfigVersion] = {}
    for section, version_id in wanted.items():
        row = by_id.get(version_id)
        if row is None or row.section != section.value:
            raise UnknownVersionError(f"config version {version_id} is not a '{section.value}' version")
        versions[section] = ConfigVersion.from_row(row)
    return ConfigPin(versions)
