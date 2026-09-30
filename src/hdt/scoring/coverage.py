"""Coverage, the new-version prior and the per-agent weight ceilings (phase 08).

- coverage_i = opinionated (non-abstaining) forecasts / scored events, per (target_type, label_spec_version);
- prior: an `agent_version` newer than the agent's first one with fewer than `min_forecasts_per_version`
  (50) opinionated forecasts, counted globally over every target type, has its weight capped at
  `new_version_cap` (0.10). The version seen first has no earlier history to inherit and is not capped;
- an open claims-audit flag lowers the agent's ceiling to `flagged_ceiling` (0.20) until it is reviewed.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime


def coverage(opinions: int, events: int) -> float:
    return opinions / events if events else 0.0


@dataclass(frozen=True)
class FlagWindow:
    """A claims-audit flag: open from `start` until `end` (the review; None while still open)."""

    agent: str
    start: datetime
    end: datetime | None

    def covers(self, at: datetime) -> bool:
        return self.start <= at and (self.end is None or at < self.end)


class VersionBook:
    """Current agent_version per agent and its opinionated forecast count (global, every key)."""

    def __init__(self, min_forecasts: int) -> None:
        self.min_forecasts = min_forecasts
        self.current: dict[str, int] = {}
        self.first: dict[str, int] = {}
        self.counts: dict[tuple[str, int], int] = {}

    def observe(self, agent: str, version: int | None) -> bool:
        """Record the version seen; True when it is newer than the current one (a version bump)."""
        if version is None:
            return False
        current = self.current.get(agent)
        if current is None:
            self.current[agent] = version
            self.first[agent] = version
            return False
        if version > current:
            self.current[agent] = version
            return True
        return False

    def count(self, agent: str) -> int:
        version = self.current.get(agent)
        return 0 if version is None else self.counts.get((agent, version), 0)

    def add(self, agent: str, version: int | None) -> None:
        if version is not None:
            self.counts[(agent, version)] = self.counts.get((agent, version), 0) + 1

    def is_new(self, agent: str) -> bool:
        version = self.current.get(agent)
        return (
            version is not None
            and version != self.first.get(agent)
            and self.count(agent) < self.min_forecasts
        )


def ceilings(
    agents: Iterable[str],
    *,
    ceiling: float,
    new_version_cap: float,
    flagged_ceiling: float,
    versions: VersionBook,
    flags: Sequence[FlagWindow],
    at: datetime,
) -> dict[str, float]:
    out: dict[str, float] = {}
    for agent in agents:
        cap = ceiling
        if versions.is_new(agent):
            cap = min(cap, new_version_cap)
        if any(f.agent == agent and f.covers(at) for f in flags):
            cap = min(cap, flagged_ceiling)
        out[agent] = cap
    return out
