"""Blind round-1 commit: canonical sha256 per forecast and per round (pure).

Each forecast commits to `sha256(AgentForecast.commit_bytes())` (canonical JSON without tool latency, capture
manifest included). The round hash is the canonical sha256 of `{agent: forecast_sha256}` (abstentions
included, so dropping or adding an agent changes it). The store writes both to Postgres
(`decision_commits`, insert-only for the council) and to the append-only `audit_log` BEFORE any claim is
shared; `verify_commit` detects any later edit of a stored forecast.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from hdt.contracts.forecast import AgentForecast
from hdt.core.ids import canonical_sha256, sha256_hex


class CommitMismatchError(RuntimeError):
    """A stored commit differs from what is being committed or verified (tampering or a non-replay)."""


def forecast_sha256(forecast: AgentForecast) -> str:
    return sha256_hex(forecast.commit_bytes())


def round_sha256(hashes: Mapping[str, str]) -> str:
    return canonical_sha256(dict(sorted(hashes.items())))


@dataclass(frozen=True)
class RoundCommit:
    event_id: str
    round: int
    forecasts: Mapping[str, str]
    """agent -> forecast sha256."""
    round_sha256: str

    @classmethod
    def of(cls, event_id: str, round_: int, forecasts: Mapping[str, AgentForecast]) -> RoundCommit:
        hashes = {agent: forecast_sha256(f) for agent, f in sorted(forecasts.items())}
        return cls(event_id, round_, hashes, round_sha256(hashes))


def verify_commit(commit: RoundCommit, forecasts: Mapping[str, AgentForecast]) -> list[str]:
    """Agents whose forecast no longer matches the commit (missing, added or edited); [] when intact."""
    problems = [
        agent
        for agent in sorted(set(commit.forecasts) | set(forecasts))
        if agent not in forecasts
        or agent not in commit.forecasts
        or forecast_sha256(forecasts[agent]) != commit.forecasts[agent]
    ]
    if not problems and round_sha256(commit.forecasts) != commit.round_sha256:
        problems.append("round")
    return problems
