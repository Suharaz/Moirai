"""`PgParamsSource`: the council's `ParamsSource` over the scorer's served parameter snapshots.

`load(target_type, label_spec_version, *, at=None, params_version=None)` returns the newest
`scoring_params` snapshot of that key created by `at` (role `hdt_council` has SELECT), or exactly
`params_version` when it is pinned (`LookupError` when that version does not exist by `at`). Before any
scored history the defaults apply (`params_version` 0): uniform `w` over the present agents (then the floor
and ceilings), `a = a_initial` (1.0), `r = r_initial` (0.5), `b = 0`, identity calibration, no stacker.

Point-in-time overlays, resolved at `at` (now when None), so they reach the council without waiting for the
next scored label (Design Contract section 4):
- an agent whose newest `agent_versions` row is newer than the version the snapshot scored gets
  `a = a_initial`, `r = r_initial` and a ceiling of at most `new_version_cap` (its new version has no
  scored forecasts yet; an agent's first version stays uncapped, as in the engine);
- an agent with a claims-audit flag open at `at` gets a ceiling of at most `flagged_ceiling`.
The stacker is the newest `stacking_models` row of the key created by `at`, attached only when that row
is enabled (a newer fit that lost to the log pool out of sample disables stacking).
The rows are stamped by the writer's clock before commit, so a time-filtered load is not repeatable on its
own: the view exposes `pins` (stacker id, live versions, open flags) and `load(..., params_version=v,
pins=view.pins)` rebuilds the identical view (deterministic replay and resume).
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Any

import sqlalchemy as sa

from hdt.contracts.common import TargetType
from hdt.contracts.packet import QuantPacket
from hdt.core.clock import ensure_utc, utcnow
from hdt.core.config import CouncilFile, scoring_config, static_config
from hdt.db.models.scoring import ClaimsAuditFlagRow, ScoringParamsRow, StackingModelRow
from hdt.db.models.settings import AgentVersionRow
from hdt.scoring.calibration import IsotonicMap
from hdt.scoring.loss import DEFAULT_CLIP
from hdt.scoring.regime_weights import regime_of, served_weights
from hdt.scoring.stacking import LightGbmStacker

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class PoolParamsView:
    """One immutable parameter snapshot (implements the council `PoolParams` protocol)."""

    params_version: int
    intercept_b: float
    stacker: LightGbmStacker | None
    global_log_w: Mapping[str, float]
    cells: Mapping[str, tuple[Mapping[str, float], int]]
    shrink_n: float
    floor: float
    ceilings: Mapping[str, float]
    groups: tuple[tuple[str, ...], ...]
    group_ceiling: float
    a_by_agent: Mapping[str, float]
    r_by_agent: Mapping[str, float]
    maps: Mapping[str, IsotonicMap]
    clip: tuple[float, float]
    a_default: float
    r_default: float
    stacker_trained_through: datetime | None = None
    """Id of the attached stacker (`stacking_models.trained_through`), None without a stacker."""
    live_versions: Mapping[str, int] = field(default_factory=dict)
    """The `agent_versions` overlay input resolved at load (the newest version per agent)."""
    flagged: tuple[str, ...] = ()
    """The agents whose claims-audit flag was open at load."""

    @property
    def pins(self) -> dict[str, Any]:
        """JSON-safe overlay inputs; `PgParamsSource.load(..., pins=view.pins)` rebuilds this exact view."""
        at = self.stacker_trained_through
        return {
            "stacker_trained_through": at.isoformat() if at is not None else None,
            "live_versions": dict(sorted(self.live_versions.items())),
            "flagged": sorted(self.flagged),
        }

    def a(self, agent: str) -> float:
        return self.a_by_agent.get(agent, self.a_default)

    def r(self, agent: str) -> float:
        return self.r_by_agent.get(agent, self.r_default)

    def calibrate(self, agent: str, p: float) -> float:
        return self.maps.get(agent, IsotonicMap())(p, self.clip)

    def weights(self, regime: str | None, present: Iterable[str]) -> dict[str, float]:
        agents = list(dict.fromkeys(present))
        if not agents:
            return {}
        return served_weights(
            self.global_log_w or dict.fromkeys(agents, 0.0),
            self.cells,
            regime=regime,
            shrink_n=self.shrink_n,
            present=agents,
            floor=self.floor,
            ceilings=self.ceilings,
            groups=self.groups,
            group_ceiling=self.group_ceiling,
        )

    @classmethod
    def defaults(cls, council: CouncilFile) -> PoolParamsView:
        w = council.weights
        agents = tuple(str(a) for a in council.agents)
        return cls(
            params_version=0,
            intercept_b=0.0,
            stacker=None,
            global_log_w={},
            cells={},
            shrink_n=1.0,
            floor=w.floor,
            ceilings=dict.fromkeys(agents, w.ceiling),
            groups=tuple(tuple(str(a) for a in g) for g in council.correlation_groups.values()),
            group_ceiling=w.group_ceiling,
            a_by_agent={},
            r_by_agent={},
            maps={},
            clip=(council.p_clip[0], council.p_clip[1]),
            a_default=w.a_initial,
            r_default=w.r_initial,
        )

    @classmethod
    def from_payload(
        cls,
        params_version: int,
        payload: Mapping[str, Any],
        council: CouncilFile,
        stacker: LightGbmStacker | None,
        stacker_trained_through: datetime | None = None,
    ) -> PoolParamsView:
        w = council.weights
        clip = payload.get("clip") or list(DEFAULT_CLIP)
        return cls(
            params_version=params_version,
            intercept_b=float(payload.get("b", 0.0)),
            stacker=stacker,
            global_log_w={str(k): float(v) for k, v in payload["global"].items()},
            cells={
                str(name): ({str(k): float(v) for k, v in cell["log_w"].items()}, int(cell["n"]))
                for name, cell in payload.get("cells", {}).items()
            },
            shrink_n=float(payload["shrink_n"]),
            floor=float(payload["floor"]),
            ceilings={str(k): float(v) for k, v in payload["ceilings"].items()},
            groups=tuple(tuple(str(a) for a in g) for g in payload.get("groups", [])),
            group_ceiling=float(payload["group_ceiling"]),
            a_by_agent={str(k): float(v) for k, v in payload.get("a", {}).items()},
            r_by_agent={str(k): float(v) for k, v in payload.get("r", {}).items()},
            maps={str(k): IsotonicMap.from_json(v) for k, v in payload.get("calibration", {}).items()},
            clip=(float(clip[0]), float(clip[1])),
            a_default=w.a_initial,
            r_default=w.r_initial,
            stacker_trained_through=stacker_trained_through if stacker is not None else None,
        )

    def with_overlays(
        self,
        council: CouncilFile,
        *,
        scored_versions: Mapping[str, int],
        live_versions: Mapping[str, int],
        flagged: Iterable[str],
        flagged_ceiling: float,
    ) -> PoolParamsView:
        """The view with the point-in-time new-version reset and cap and the open claims-audit flags."""
        w = council.weights
        ceilings = {str(a): self.ceilings.get(str(a), w.ceiling) for a in council.agents}
        ceilings.update({a: c for a, c in self.ceilings.items() if a not in ceilings})
        a_by, r_by = dict(self.a_by_agent), dict(self.r_by_agent)
        for agent, scored in scored_versions.items():
            live = live_versions.get(agent)
            if live is not None and live > scored:
                ceilings[agent] = min(ceilings.get(agent, w.ceiling), w.new_version_cap)
                a_by[agent] = w.a_initial
                r_by[agent] = w.r_initial
        flagged = tuple(sorted(set(flagged)))
        for agent in flagged:
            ceilings[agent] = min(ceilings.get(agent, w.ceiling), flagged_ceiling)
        return replace(
            self,
            ceilings=ceilings,
            a_by_agent=a_by,
            r_by_agent=r_by,
            live_versions=dict(live_versions),
            flagged=flagged,
        )


class PgParamsSource:
    """Council `ParamsSource`: the served snapshot per key at a point in time, with the live overlays."""

    def __init__(
        self,
        engine: sa.Engine,
        *,
        council: CouncilFile | None = None,
        flagged_ceiling: float | None = None,
    ) -> None:
        self._engine = engine
        self._council = council or static_config().council
        self._flagged_ceiling = (
            flagged_ceiling if flagged_ceiling is not None else scoring_config().claims_audit.flagged_ceiling
        )

    def load(
        self,
        target_type: TargetType,
        label_spec_version: str,
        *,
        at: datetime | None = None,
        params_version: int | None = None,
        pins: Mapping[str, Any] | None = None,
    ) -> PoolParamsView:
        """The served view; with `pins` (a prior view's `pins`) the overlays and the stacker are rebuilt from
        them instead of re-queried by time, so a replay equals the original load even when a row stamped
        before `at` was committed after it."""
        target = TargetType(target_type).value
        moment = ensure_utc(at) if at is not None else utcnow()
        p, s = ScoringParamsRow, StackingModelRow
        with self._engine.connect() as conn:
            query = sa.select(p.params_version, p.payload).where(
                p.target_type == target, p.label_spec_version == label_spec_version, p.created_at <= moment
            )
            if params_version is not None:
                query = query.where(p.params_version == params_version)
            row = (
                None
                if params_version == 0
                else conn.execute(query.order_by(p.params_version.desc()).limit(1)).one_or_none()
            )
            if row is None and params_version not in (None, 0):
                raise LookupError(
                    f"scoring params {target}/{label_spec_version} version {params_version} "
                    f"does not exist by {moment.isoformat()}"
                )
            pinned = _parse_pins(pins) if pins is not None else None
            if pinned is not None:
                live_versions, flagged, pinned_at = pinned
            else:
                pinned_at = None
                live_versions = {
                    str(r.agent): int(r.version)
                    for r in conn.execute(
                        sa.select(
                            AgentVersionRow.agent, sa.func.max(AgentVersionRow.version).label("version")
                        )
                        .where(AgentVersionRow.created_at <= moment)
                        .group_by(AgentVersionRow.agent)
                    )
                }
                f = ClaimsAuditFlagRow
                flagged = set(
                    conn.execute(
                        sa.select(f.agent).where(
                            f.raised_at <= moment, sa.or_(f.reviewed_at.is_(None), f.reviewed_at > moment)
                        )
                    ).scalars()
                )
            stacker_query = sa.select(s.model, s.features, s.enabled, s.trained_through).where(
                s.target_type == target, s.label_spec_version == label_spec_version
            )
            if pinned is not None:
                model = (
                    None
                    if row is None or pinned_at is None
                    else conn.execute(stacker_query.where(s.trained_through == pinned_at)).one_or_none()
                )
                if model is None and pinned_at is not None and row is not None:
                    raise LookupError(
                        f"pinned stacker {target}/{label_spec_version} trained through "
                        f"{pinned_at.isoformat()} does not exist"
                    )
            else:
                model = (
                    None
                    if row is None
                    else conn.execute(
                        stacker_query.where(s.created_at <= moment)
                        .order_by(s.trained_through.desc())
                        .limit(1)
                    ).one_or_none()
                )
        if row is None:
            view = PoolParamsView.defaults(self._council)
            scored: dict[str, int] = {}
        else:
            # A pinned stacker was attached (enabled) when the pins were taken.
            enabled = model is not None and (pinned is not None or bool(model.enabled))
            stacker = LightGbmStacker.from_stored(model.model, model.features) if enabled and model else None
            view = PoolParamsView.from_payload(
                int(row.params_version),
                row.payload,
                self._council,
                stacker,
                model.trained_through if stacker is not None and model is not None else None,
            )
            scored = {
                str(agent): version
                for agent, version in (row.payload.get("versions") or {}).items()
                if isinstance(version, int) and not isinstance(version, bool)
            }
        return view.with_overlays(
            self._council,
            scored_versions=scored,
            live_versions=live_versions,
            flagged=flagged,
            flagged_ceiling=self._flagged_ceiling,
        )

    def regime(self, packets: Mapping[str, QuantPacket]) -> str | None:
        return regime_of(packets)


def _parse_pins(pins: Mapping[str, Any]) -> tuple[dict[str, int], set[str], datetime | None]:
    """Validate a stored `PoolParamsView.pins` mapping: (live versions, flagged agents, stacker id)."""
    try:
        raw_at = pins["stacker_trained_through"]
        raw_live = pins["live_versions"]
        raw_flagged = pins["flagged"]
    except (KeyError, TypeError) as exc:
        raise ValueError(f"malformed params pins: {pins!r}") from exc
    if not isinstance(raw_live, Mapping) or not isinstance(raw_flagged, list | tuple):
        raise ValueError(f"malformed params pins: {pins!r}")
    live: dict[str, int] = {}
    for agent, version in raw_live.items():
        if not isinstance(version, int) or isinstance(version, bool):
            raise ValueError(f"malformed pinned version for {agent!r}: {version!r}")
        live[str(agent)] = version
    if raw_at is None:
        at = None
    elif isinstance(raw_at, datetime):
        at = ensure_utc(raw_at)
    elif isinstance(raw_at, str):
        at = ensure_utc(datetime.fromisoformat(raw_at))
    else:
        raise ValueError(f"malformed pinned stacker id: {raw_at!r}")
    return live, {str(a) for a in raw_flagged}, at
