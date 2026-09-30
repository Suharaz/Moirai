"""Rescoring engine: the whole labeled history in canonical order -> scored forecasts, w/a/r history,
calibration and the served pool parameters. Pure and deterministic (no clock, no I/O), so rescoring the same
history twice gives identical tables (the service upserts the result).

Rules (Design Contract section 4, phase 08 Red Team Delta):
- scoring unit: one scored council event per (coin, 12 h window), chosen by the council (`unscored=false`);
  NO_TRADE events are scored too. Only round-1 forecasts teach `w` and `a`; `r` learns from debated events.
- every learned table is keyed by `(target_type, label_spec_version)`; nothing mixes two keys;
- events are applied in canonical `(as_of, event_id)` order;
- the 50-forecast threshold of a new `agent_version` counts opinionated scored forecasts globally (every
  key); a new version keeps `w`, resets `a` to `a_initial` and `r` to `r_initial`, and its served weight is
  capped at `new_version_cap` until it has `min_forecasts_per_version` forecasts. An agent's first version
  is the prior and is not capped;
- a claims-audit flag lowers the agent's ceiling to `flagged_ceiling` while it is open (in the weight
  history). The served payload carries the version-cap ceilings only, plus each agent's current scored
  `agent_version` (`versions`): `hdt.scoring.params.PgParamsSource` applies open flags and agent versions
  registered since the payload at load time, so neither waits for the next scored label.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from itertools import groupby
from typing import Any

from hdt.core.config import CouncilFile, ScoringFile
from hdt.scoring.calibration import (
    IsotonicMap,
    Reliability,
    fit_intercept,
    fit_isotonic,
    in_window,
    pooled_logit,
    reliability,
)
from hdt.scoring.coverage import FlagWindow, VersionBook, ceilings, coverage
from hdt.scoring.hedge import apply_caps
from hdt.scoring.llm_trust import update_a
from hdt.scoring.loss import hit, log_loss
from hdt.scoring.regime_weights import HierarchicalHedge, served_weights
from hdt.scoring.revision import update_r

POOLED = "pooled"
Key = tuple[str, str]
"""(target_type, label_spec_version)"""


@dataclass(frozen=True)
class AgentRound1:
    agent: str
    agent_version: int | None
    p_used: float | None
    """None when the agent abstained in round 1."""
    p_model: float | None
    p_final: float | None = None
    """The agent's effective final-round p_used when the event went to debate (None otherwise)."""


@dataclass(frozen=True)
class ScoringEvent:
    event_id: str
    coin_id: int
    as_of: datetime
    horizon_h: int
    target_type: str
    label_spec_version: str
    y: int
    regime: str | None
    p_pooled: float | None
    debated: bool
    agents: tuple[AgentRound1, ...]
    barrier_y: int | None = None

    @property
    def key(self) -> Key:
        return (self.target_type, self.label_spec_version)

    @property
    def known_at(self) -> datetime:
        return self.as_of + timedelta(hours=self.horizon_h)


@dataclass(frozen=True)
class EngineConfig:
    agents: tuple[str, ...]
    eta: float
    alpha: float
    shrink_n: float
    kappa_a: float
    kappa_r: float
    clip: tuple[float, float]
    floor: float
    ceiling: float
    group_ceiling: float
    groups: tuple[tuple[str, ...], ...]
    new_version_cap: float
    min_forecasts_per_version: int
    a_initial: float
    r_initial: float
    cal_window: timedelta
    cal_min_samples: int
    cal_bins: int
    intercept_bound: float
    flagged_ceiling: float

    @classmethod
    def from_config(cls, council: CouncilFile, scoring: ScoringFile) -> EngineConfig:
        w = council.weights
        return cls(
            agents=tuple(str(a) for a in council.agents),
            eta=scoring.hedge.eta,
            alpha=scoring.hedge.fixed_share_alpha,
            shrink_n=scoring.hedge.regime_shrink_n,
            kappa_a=scoring.trust.kappa_a,
            kappa_r=scoring.trust.kappa_r,
            clip=scoring.loss_clip,
            floor=w.floor,
            ceiling=w.ceiling,
            group_ceiling=w.group_ceiling,
            groups=tuple(tuple(str(a) for a in g) for g in council.correlation_groups.values()),
            new_version_cap=w.new_version_cap,
            min_forecasts_per_version=w.min_forecasts_per_version,
            a_initial=w.a_initial,
            r_initial=w.r_initial,
            cal_window=timedelta(days=scoring.calibration.window_days),
            cal_min_samples=scoring.calibration.min_samples,
            cal_bins=scoring.calibration.bins,
            intercept_bound=scoring.calibration.intercept_bound,
            flagged_ceiling=scoring.claims_audit.flagged_ceiling,
        )


@dataclass(frozen=True)
class ScoredRow:
    event_id: str
    agent: str
    target_type: str
    label_spec_version: str
    coin_id: int
    as_of: datetime
    agent_version: int | None
    p: float
    p_model: float | None
    y: int
    hit: bool
    log_loss: float
    regime: str | None
    scored_at: datetime

    @property
    def label(self) -> str:
        return "up" if self.y == 1 else "down"


@dataclass(frozen=True)
class WeightRow:
    target_type: str
    label_spec_version: str
    agent: str
    as_of: datetime
    agent_version: int | None
    w: float
    w_capped: float
    a: float
    r: float
    coverage: float
    forecasts: int
    events: int


@dataclass(frozen=True)
class KeyResult:
    key: Key
    computed_at: datetime
    """Knowledge time of the newest scored event of this key."""
    payload: dict[str, Any]
    """The served pool parameters (`hdt.scoring.params.PoolParamsView` reads them)."""
    reliability: dict[str, Reliability]

    @property
    def payload_sha256(self) -> str:
        return payload_sha256(self.payload)


@dataclass(frozen=True)
class ScoringResult:
    scored: tuple[ScoredRow, ...]
    weights: tuple[WeightRow, ...]
    keys: dict[Key, KeyResult]


def payload_sha256(payload: Mapping[str, Any]) -> str:
    body = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


@dataclass
class _KeyState:
    hedge: HierarchicalHedge
    a: dict[str, float]
    r: dict[str, float]
    events: int = 0
    opinions: dict[str, int] = field(default_factory=dict)
    per_version: dict[tuple[str, int], int] = field(default_factory=dict)
    """Opinionated forecasts of this key per (agent, agent_version)."""
    last_as_of: datetime | None = None

    def forecasts(self, agent: str, version: int | None) -> int:
        return 0 if version is None else self.per_version.get((agent, version), 0)


def rescore(
    events: Iterable[ScoringEvent], cfg: EngineConfig, flags: Sequence[FlagWindow] = ()
) -> ScoringResult:
    ordered = sorted(events, key=lambda e: (e.as_of, e.event_id))
    states: dict[Key, _KeyState] = {}
    versions = VersionBook(cfg.min_forecasts_per_version)
    scored: list[ScoredRow] = []
    weights: list[WeightRow] = []
    per_key: dict[Key, list[ScoringEvent]] = {}
    for as_of, group in groupby(ordered, key=lambda e: e.as_of):
        touched: list[Key] = []
        for event in group:
            state = states.get(event.key)
            if state is None:
                state = _KeyState(
                    HierarchicalHedge(cfg.agents, cfg.eta, cfg.alpha, cfg.shrink_n),
                    dict.fromkeys(cfg.agents, cfg.a_initial),
                    dict.fromkeys(cfg.agents, cfg.r_initial),
                )
                states[event.key] = state
            if event.key not in touched:
                touched.append(event.key)
            per_key.setdefault(event.key, []).append(event)
            _apply(event, state, states, versions, cfg, scored)
        for key in touched:
            state = states[key]
            raw = state.hedge.global_state.weights()
            capped = apply_caps(
                raw,
                floor=cfg.floor,
                ceilings=_ceilings(cfg, versions, flags, as_of),
                groups=cfg.groups,
                group_ceiling=cfg.group_ceiling,
            )
            for agent in cfg.agents:
                weights.append(
                    WeightRow(
                        target_type=key[0],
                        label_spec_version=key[1],
                        agent=agent,
                        as_of=as_of,
                        agent_version=versions.current.get(agent),
                        w=_r(raw[agent]),
                        w_capped=_r(capped[agent]),
                        a=_r(state.a[agent]),
                        r=_r(state.r[agent]),
                        coverage=_r(coverage(state.opinions.get(agent, 0), state.events)),
                        forecasts=state.forecasts(agent, versions.current.get(agent)),
                        events=state.events,
                    )
                )
    results: dict[Key, KeyResult] = {}
    for key, key_events in per_key.items():
        results[key] = _finish(key, key_events, states[key], versions, cfg, flags, scored)
    return ScoringResult(tuple(scored), tuple(weights), results)


def _apply(
    event: ScoringEvent,
    state: _KeyState,
    states: Mapping[Key, _KeyState],
    versions: VersionBook,
    cfg: EngineConfig,
    scored: list[ScoredRow],
) -> None:
    losses: dict[str, float] = {}
    for item in event.agents:
        if item.agent not in cfg.agents:
            continue
        if versions.observe(item.agent, item.agent_version):
            for other in states.values():
                other.a[item.agent] = cfg.a_initial
                other.r[item.agent] = cfg.r_initial
        if item.p_used is None:
            continue
        loss = log_loss(item.p_used, event.y, cfg.clip)
        losses[item.agent] = loss
        versions.add(item.agent, item.agent_version)
        if item.agent_version is not None:
            vkey = (item.agent, item.agent_version)
            state.per_version[vkey] = state.per_version.get(vkey, 0) + 1
        state.opinions[item.agent] = state.opinions.get(item.agent, 0) + 1
        if item.p_model is not None:
            state.a[item.agent] = update_a(
                state.a[item.agent],
                p_model=item.p_model,
                p_used=item.p_used,
                y=event.y,
                kappa=cfg.kappa_a,
                clip=cfg.clip,
            )
        if event.debated and item.p_final is not None:
            state.r[item.agent] = update_r(
                state.r[item.agent],
                p_round1=item.p_used,
                p_final=item.p_final,
                y=event.y,
                kappa=cfg.kappa_r,
                clip=cfg.clip,
            )
        scored.append(_scored(event, item.agent, item.agent_version, item.p_used, item.p_model, loss))
    state.events += 1
    state.last_as_of = event.as_of
    state.hedge.update(losses, event.regime, event.as_of.date())
    if event.p_pooled is not None:
        pooled_loss = log_loss(event.p_pooled, event.y, cfg.clip)
        scored.append(_scored(event, POOLED, None, event.p_pooled, None, pooled_loss))


def _scored(
    event: ScoringEvent, agent: str, version: int | None, p: float, p_model: float | None, loss: float
) -> ScoredRow:
    return ScoredRow(
        event_id=event.event_id,
        agent=agent,
        target_type=event.target_type,
        label_spec_version=event.label_spec_version,
        coin_id=event.coin_id,
        as_of=event.as_of,
        agent_version=version,
        p=p,
        p_model=p_model,
        y=event.y,
        hit=hit(p, event.y),
        log_loss=_r(loss),
        regime=event.regime,
        scored_at=event.known_at,
    )


def _finish(
    key: Key,
    events: list[ScoringEvent],
    state: _KeyState,
    versions: VersionBook,
    cfg: EngineConfig,
    flags: Sequence[FlagWindow],
    scored: Sequence[ScoredRow],
) -> KeyResult:
    now = max(e.known_at for e in events)
    horizon = timedelta(hours=max(e.horizon_h for e in events))
    window = [e for e in events if in_window(e.as_of, now, horizon, cfg.cal_window)]
    maps: dict[str, IsotonicMap] = {}
    for agent in cfg.agents:
        pairs = [
            (i.p_used, e.y) for e in window for i in e.agents if i.agent == agent and i.p_used is not None
        ]
        maps[agent] = fit_isotonic(
            [p for p, _ in pairs], [y for _, y in pairs], min_samples=cfg.cal_min_samples, clip=cfg.clip
        )
    pooled_pairs = [(e.p_pooled, e.y) for e in window if e.p_pooled is not None]
    maps[POOLED] = fit_isotonic(
        [p for p, _ in pooled_pairs],
        [y for _, y in pooled_pairs],
        min_samples=cfg.cal_min_samples,
        clip=cfg.clip,
    )
    caps = _ceilings(cfg, versions, flags, now)
    served_caps = _ceilings(cfg, versions, (), now)
    cells = {name: (cell.log_w, cell.updates) for name, cell in state.hedge.cells.items()}
    logits: list[float] = []
    labels: list[int] = []
    for e in window:
        present = {i.agent: i.p_used for i in e.agents if i.agent in cfg.agents and i.p_used is not None}
        if not present:
            continue
        served = served_weights(
            state.hedge.global_state.log_w,
            cells,
            regime=e.regime,
            shrink_n=cfg.shrink_n,
            present=present,
            floor=cfg.floor,
            ceilings=caps,
            groups=cfg.groups,
            group_ceiling=cfg.group_ceiling,
        )
        logits.append(pooled_logit(present, served, maps, cfg.clip))
        labels.append(e.y)
    b = fit_intercept(
        logits, labels, bound=cfg.intercept_bound, min_samples=cfg.cal_min_samples, clip=cfg.clip
    )
    charts: dict[str, Reliability] = {}
    key_rows = [row for row in scored if (row.target_type, row.label_spec_version) == key]
    for agent in (*cfg.agents, POOLED):
        rows = [row for row in key_rows if row.agent == agent]
        if rows:
            charts[agent] = reliability([row.p for row in rows], [row.y for row in rows], cfg.cal_bins)
    snapshot = state.hedge.snapshot()
    payload: dict[str, Any] = {
        "target_type": key[0],
        "label_spec_version": key[1],
        "as_of": now.isoformat(),
        "events": state.events,
        "global": {a: _r(v) for a, v in snapshot["global"].items()},
        "cells": {
            name: {"log_w": {a: _r(v) for a, v in cell["log_w"].items()}, "n": cell["n"]}
            for name, cell in snapshot["cells"].items()
        },
        "shrink_n": cfg.shrink_n,
        "floor": cfg.floor,
        "ceilings": {a: served_caps[a] for a in cfg.agents},
        "versions": {a: versions.current[a] for a in cfg.agents if a in versions.current},
        "group_ceiling": cfg.group_ceiling,
        "groups": [list(g) for g in cfg.groups],
        "a": {a: _r(v) for a, v in state.a.items()},
        "r": {a: _r(v) for a, v in state.r.items()},
        "b": b,
        "clip": list(cfg.clip),
        "calibration": {a: m.to_json() for a, m in sorted(maps.items())},
    }
    return KeyResult(key, now, payload, charts)


def _ceilings(
    cfg: EngineConfig, versions: VersionBook, flags: Sequence[FlagWindow], at: datetime
) -> dict[str, float]:
    return ceilings(
        cfg.agents,
        ceiling=cfg.ceiling,
        new_version_cap=cfg.new_version_cap,
        flagged_ceiling=cfg.flagged_ceiling,
        versions=versions,
        flags=flags,
        at=at,
    )


def _r(value: float) -> float:
    """Round to 12 significant decimals so equal histories give byte-identical rows and payloads."""
    return round(float(value), 12)
