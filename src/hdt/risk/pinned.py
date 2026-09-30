"""Effective risk file and run mode of a config pin, clipped to the hard ceilings (never raises on a stored
version).

Stored versions are re-validated on every read. A risk or mode version saved before a ceiling was tightened
therefore fails validation for good, and the risk and execution services would stop at their next config
read (no stop managed). Instead the offending section is clipped back inside the ceilings (tighter values
are kept), each problem is reported so the caller raises one critical `config_invalid` alert per stored
version (`alert_config_problem`), and the service keeps running on the clipped values. A section that still
fails after clipping falls back to the static YAML values; a mode that cannot be read falls back to `paper`.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass, fields
from typing import Any, Final

from pydantic import BaseModel
from sqlalchemy.orm import Session

from hdt.contracts.common import Account
from hdt.core.alerts import alert
from hdt.core.config import RiskFile, StaticConfig
from hdt.settings.ceilings import PAPER_SIZE_MULTIPLIER, SIZE_MULTIPLIER_MAX, RiskLimits, clip_risk_limits
from hdt.settings.schemas import (
    BINANCE_FIELDS,
    BINANCE_RISK_FIELDS,
    RISK_FIELDS,
    BinanceSection,
    ModeSection,
    RiskSection,
    Section,
    default_size_multiplier,
    effective_risk,
)
from hdt.settings.versions import ConfigPin, ConfigVersion, SectionNotConfiguredError

ERROR_MAX: Final[int] = 1500

type Payload = Mapping[str, Any]


@dataclass(frozen=True)
class ConfigProblem:
    """A stored version that fails the current validation (hard ceilings) and runs clipped."""

    section: Section
    version_id: int
    error: str

    @property
    def episode(self) -> str:
        """Alert episode: one alert per stored version, a later bad version pages again."""
        return f"{self.section.value}:{self.version_id}"


@dataclass(frozen=True)
class PinnedConfig:
    risk: RiskFile
    mode: Account
    problems: tuple[ConfigProblem, ...] = ()


def pinned_config(pin: ConfigPin, static: StaticConfig) -> PinnedConfig:
    """The runtime risk file and run mode of `pin`: the static risk file while risk, binance or mode has no
    active version, and `paper` while mode has none."""
    problems: list[ConfigProblem] = []
    mode = _mode_section(pin, static, problems)
    versions = _versions(pin, Section.RISK, Section.BINANCE)
    if mode is None or versions is None:
        return PinnedConfig(static.risk, mode.mode if mode is not None else Account.PAPER, tuple(problems))
    risk_version, binance_version = versions
    risk_section = _section(risk_version, problems, RiskSection, _clipped_risk, _static_risk(static))
    binance_section = _section(binance_version, problems, BinanceSection, None, _static_binance(static))
    try:
        risk = effective_risk(risk_section, binance_section, mode, static)
    except ValueError as exc:  # a combination no single section check catches: run on the static file
        problems.append(ConfigProblem(Section.RISK, risk_version.id, _error_text(exc)))
        risk = static.risk
    return PinnedConfig(risk, mode.mode, tuple(problems))


def alert_config_problem(session: Session, problem: ConfigProblem, *, service: str) -> str | None:
    """The critical alert for one clipped stored version (deduplicated per version by the outbox)."""
    return alert(
        session,
        kind="config_invalid",
        severity="critical",
        title=f"stored {problem.section.value} version {problem.version_id} breaks the ceilings; clipped",
        detail=f"{problem.error}. Running on the clipped values; save a new version within the ceilings.",
        service=service,
        episode=problem.episode,
    )


def _error_text(exc: Exception) -> str:
    return str(exc)[:ERROR_MAX]


def _versions(pin: ConfigPin, *sections: Section) -> tuple[ConfigVersion, ...] | None:
    try:
        return tuple(pin.get(section) for section in sections)
    except SectionNotConfiguredError:
        return None


def _section[T: BaseModel](
    version: ConfigVersion,
    problems: list[ConfigProblem],
    cls: type[T],
    clip: Callable[[Payload], Payload] | None,
    fallback: Payload,
) -> T:
    """The parsed section; after a validation error the clipped payload, else the static fallback."""
    try:
        return cls.model_validate(version.payload)
    except ValueError as exc:
        problems.append(ConfigProblem(version.section, version.id, _error_text(exc)))
    if clip is not None:
        try:
            return cls.model_validate(clip(version.payload))
        except (KeyError, TypeError, ValueError):
            pass
    return cls.model_validate(fallback)


def _clipped_risk(payload: Payload) -> Payload:
    """The risk payload with every ceiling-bounded value moved back inside its ceiling."""
    limits = RiskLimits(**{f.name: payload[f.name] for f in fields(RiskLimits)})
    return {**payload, **asdict(clip_risk_limits(limits))}


def _static_risk(static: StaticConfig) -> Payload:
    return static.risk.model_dump(include=set(RISK_FIELDS))


def _static_binance(static: StaticConfig) -> Payload:
    return {
        **static.binance.model_dump(include=set(BINANCE_FIELDS)),
        **static.risk.model_dump(include=set(BINANCE_RISK_FIELDS)),
    }


def _mode_section(pin: ConfigPin, static: StaticConfig, problems: list[ConfigProblem]) -> ModeSection | None:
    """The pinned mode section, its multiplier clipped when the stored one breaks the ceilings; None while
    mode has no active version."""
    versions = _versions(pin, Section.MODE)
    if versions is None:
        return None
    (version,) = versions

    def clip(payload: Payload) -> Payload:
        mode = Account(payload["mode"])
        value = float(payload["size_multiplier"])
        if mode is Account.PAPER:
            value = PAPER_SIZE_MULTIPLIER
        elif value > 0:
            value = min(value, SIZE_MULTIPLIER_MAX)
        else:
            value = default_size_multiplier(mode, static)
        return {**payload, "size_multiplier": value}

    fallback = {"mode": Account.PAPER.value, "size_multiplier": PAPER_SIZE_MULTIPLIER}
    return _section(version, problems, ModeSection, clip, fallback)
