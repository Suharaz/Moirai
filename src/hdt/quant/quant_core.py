"""`quant_core(agent, coin_id, as_of, pins) -> QuantPacket` (the tool phase 04 wraps for the agents).

For one agent: the agent's feature family + the common keys + reconciliation, the agent's own `p_model`,
the shared candidate set of (coin, as_of) and the data-quality flags. Both the candidate set and the packet
are stored insert-only in Postgres before the packet is returned. Missing data never raises: it yields null
features plus flags. Errors: `UnsupportedAgentError` (news has no quant packet), `CoinNotInUniverseError`
(coin absent from the PIT universe of `pins.universe_date`, BTC, or no universe for that date); database and
configuration errors propagate, except a stored risk or mode version outside the current hard ceilings,
which runs clipped (`hdt.risk.pinned`).
"""

from __future__ import annotations

import json
import logging
from collections import OrderedDict
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from threading import Lock

import sqlalchemy as sa
from sqlalchemy.orm import Session

from hdt.contracts.candidate import CandidateSet
from hdt.contracts.common import AgentName, TargetType
from hdt.contracts.packet import QuantPacket
from hdt.core.clock import ensure_utc, utcnow
from hdt.core.config import (
    CONFIG_DIR,
    CmcRoutesFile,
    ScannerFile,
    StaticConfig,
    scanner_config,
    static_config,
)
from hdt.features.engine import FeatureEngine, Snapshot
from hdt.features.lake_io import LakeView
from hdt.features.levels import LevelRules
from hdt.lake.pit_query import PitQuery
from hdt.lake.universe import UNIVERSE_ROUTE, UNIVERSE_SOURCE, Universe, UniverseMember, load_universe
from hdt.quant.cache import CacheKey, PacketCache, config_hash
from hdt.quant.p_model import MODEL_AGENTS, p_model_for
from hdt.quant.packet import build_packet, store_candidate_set, store_packet
from hdt.risk.pinned import pinned_config
from hdt.settings.versions import load_pin

log = logging.getLogger(__name__)


class UnsupportedAgentError(ValueError):
    """The agent has no quant packet (News: its p_model comes from the phase 07 regime rules)."""


class CoinNotInUniverseError(LookupError):
    """The coin is not tradable by the council at this universe date (absent, BTC, or no universe)."""


@dataclass(frozen=True)
class QuantPins:
    """What phase 06 pins for one event."""

    config_version_ids: Mapping[str, int]
    universe_date: date
    target_type: TargetType
    label_spec_version: str


@dataclass(frozen=True)
class _PinnedConfig:
    routes: CmcRoutesFile
    rules: LevelRules


def universe_for_date(pit: PitQuery, day: date, as_of: datetime) -> Universe | None:
    """The universe built for `day`, as recorded at or before `as_of` (None when none exists)."""
    latest = load_universe(pit, as_of)
    if latest is not None and latest.date == day:
        return latest
    start = datetime(day.year, day.month, day.day, tzinfo=ensure_utc(as_of).tzinfo)
    records = pit.series(UNIVERSE_SOURCE, UNIVERSE_ROUTE, start, start + timedelta(days=2), as_of=as_of)
    for record in reversed(records):
        universe = Universe.model_validate(json.loads(record.body()))
        if universe.date == day:
            return universe
    return None


class QuantCore:
    """Sync and blocking; safe to share across threads of one process (packets are cached per full key)."""

    def __init__(
        self,
        pit: PitQuery,
        engine: sa.Engine,
        *,
        static: StaticConfig | None = None,
        cache: PacketCache | None = None,
        scanner: ScannerFile | None = None,
        view: LakeView | None = None,
        features: FeatureEngine | None = None,
        p_model_dir: Path = CONFIG_DIR,
        clock: Callable[[], datetime] = utcnow,
    ) -> None:
        self.pit = pit
        self.engine = engine
        self.static = static or static_config()
        self.scanner = scanner or scanner_config()
        self.view = view or LakeView(pit, clock=clock)
        self.features = features or FeatureEngine(self.view, self.static, self.scanner)
        self.cache = cache or PacketCache()
        self._p_model_dir = p_model_dir
        self._pins: dict[tuple[tuple[str, int], ...], _PinnedConfig] = {}
        self._universes: dict[date, Universe] = {}
        # Candidate sets this core has committed, by object identity (the value keeps the object alive, so an
        # id is never reused while its entry exists): the agents of one (coin, as_of) share the snapshot's one
        # set object, which is then hashed once and inserted once. At most one set per cached packet.
        self._stored_sets: OrderedDict[int, tuple[CandidateSet, str]] = OrderedDict()
        self._lock = Lock()

    # ------------------------------------------------------------------ public API

    def quant_core(
        self, agent: AgentName | str, coin_id: int, as_of: datetime, pins: QuantPins
    ) -> QuantPacket:
        agent = AgentName(agent)
        if agent not in MODEL_AGENTS:
            raise UnsupportedAgentError(f"agent {agent.value} has no quant packet")
        as_of = ensure_utc(as_of)
        model = p_model_for(agent, pins.target_type, self._p_model_dir)
        key = CacheKey(
            agent,
            coin_id,
            as_of,
            self.features.feature_ver,
            model.version,
            config_hash(pins.config_version_ids, pins.universe_date),
            pins.target_type,
            pins.label_spec_version,
        )
        cached = self.cache.get(key)
        if cached is not None:
            return cached
        with self._lock:
            snap, member, pinned = self._context(coin_id, as_of, pins)
            candidate_set = snap.candidate_set(member, pinned.rules)
            block = snap.agent_block(agent, member)
            stored_sha = self._stored_set_sha(candidate_set)
        set_sha = stored_sha or candidate_set.candidate_set_sha256
        packet = build_packet(
            agent=agent,
            coin_id=coin_id,
            as_of=as_of,
            block=block,
            p_model=model.predict(block.values),
            p_model_ver=model.version,
            candidate_set_sha256=set_sha,
            universe_date=pins.universe_date,
            config_version_ids=pins.config_version_ids,
            feature_ver=self.features.feature_ver,
            target_type=pins.target_type,
            label_spec_version=pins.label_spec_version,
        )
        with self.engine.begin() as conn:
            if stored_sha is None:
                store_candidate_set(conn, candidate_set)
            store_packet(conn, packet)
        if stored_sha is None:
            self._remember_stored_set(candidate_set, set_sha)
        self.cache.put(packet)
        return packet

    __call__ = quant_core

    def candidate_set(self, coin_id: int, as_of: datetime, pins: QuantPins) -> CandidateSet:
        """The shared candidate set of (coin, as_of), stored before it is returned."""
        with self._lock:
            snap, member, pinned = self._context(coin_id, ensure_utc(as_of), pins)
            candidate_set = snap.candidate_set(member, pinned.rules)
            stored_sha = self._stored_set_sha(candidate_set)
        if stored_sha is None:
            with self.engine.begin() as conn:
                store_candidate_set(conn, candidate_set)
            self._remember_stored_set(candidate_set, candidate_set.candidate_set_sha256)
        return candidate_set

    # ------------------------------------------------------------------ internals

    def _stored_set_sha(self, candidate_set: CandidateSet) -> str | None:
        """The sha256 of `candidate_set` if this very object was committed already (caller holds the lock)."""
        found = self._stored_sets.get(id(candidate_set))
        if found is None or found[0] is not candidate_set:
            return None
        self._stored_sets.move_to_end(id(candidate_set))
        return found[1]

    def _remember_stored_set(self, candidate_set: CandidateSet, sha: str) -> None:
        with self._lock:
            self._stored_sets[id(candidate_set)] = (candidate_set, sha)
            self._stored_sets.move_to_end(id(candidate_set))
            while len(self._stored_sets) > self.cache.max_entries:
                self._stored_sets.popitem(last=False)

    def _context(
        self, coin_id: int, as_of: datetime, pins: QuantPins
    ) -> tuple[Snapshot, UniverseMember, _PinnedConfig]:
        universe = self._universe(pins.universe_date, as_of)
        member = next((m for m in universe.members if m.cmc_id == coin_id), None)
        if member is None:
            raise CoinNotInUniverseError(f"coin {coin_id} is not in the universe of {pins.universe_date}")
        if member.cmc_symbol == self.static.indicators.reserves.btc_symbol:
            raise CoinNotInUniverseError("BTC is not in the council's tradable coin set")
        pinned = self._pinned(pins.config_version_ids)
        return self.features.snapshot(as_of, universe, pinned.routes), member, pinned

    def _universe(self, day: date, as_of: datetime) -> Universe:
        cached = self._universes.get(day)
        if cached is not None and cached.built_at <= as_of:
            return cached
        universe = universe_for_date(self.pit, day, as_of)
        if universe is None:
            raise CoinNotInUniverseError(f"no universe recorded for {day} at or before {as_of.isoformat()}")
        self._universes[day] = universe
        return universe

    def _pinned(self, version_ids: Mapping[str, int]) -> _PinnedConfig:
        key = tuple(sorted((str(k), int(v)) for k, v in version_ids.items()))
        found = self._pins.get(key)
        if found is None:
            with Session(self.engine) as session:
                pin = load_pin(session, dict(key))
            # A stored risk or mode version outside the current hard ceilings runs clipped, as in Risk and
            # execution (which raise its `config_invalid` alert), instead of failing every packet.
            pinned = pinned_config(pin, self.static)
            for problem in pinned.problems:
                log.warning(
                    "quant core: stored %s version %d breaks the hard ceilings; running clipped",
                    problem.section.value,
                    problem.version_id,
                )
            risk = pinned.risk
            found = _PinnedConfig(
                pin.cmc_routes(self.static), LevelRules(risk.min_rr, risk.max_entry_distance_atr)
            )
            self._pins[key] = found
        return found
