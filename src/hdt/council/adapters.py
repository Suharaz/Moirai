"""Council port implementations over the phase 02/03/09 stores (Postgres, the lake and Redis).

- `PgPacketStore`: committed packets by `packet_sha256` (`quant_packets`, hash re-verified on load);
- `QuantCandidateSets`: the phase 03 shared candidate set with the event's pins;
- `EventQuantCore`: the live `quant_core` tool backend, bound to each event's pins before its agents run;
- `LakeUniverse`: the point-in-time universe entry (Binance symbol, universe date) of a coin;
- `StreamHeldPositions`: the held side from the newest `AccountState` of the running account namespace;
- `PgScannerLog`: whether a scanner candidate came only from the loose superset thresholds;
- `PinnedSettings`: the council file of an event's pinned config versions;
"""

from __future__ import annotations

import logging
import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Mapping
from datetime import datetime
from typing import Any, Final

import sqlalchemy as sa
from redis.asyncio import Redis
from sqlalchemy.orm import Session, sessionmaker

from hdt.contracts.candidate import Candidate, CandidateSet
from hdt.contracts.common import Account, AgentName, CandidateSource, Side
from hdt.contracts.packet import QuantPacket
from hdt.contracts.streams import Stream
from hdt.core.clock import ensure_utc
from hdt.core.config import CouncilFile, StaticConfig
from hdt.council.graph import EventSettings, UniverseEntry
from hdt.council.ports import EventPins
from hdt.db.models.quant import ScannerLogRow
from hdt.lake.pit_query import PitQuery
from hdt.lake.universe import load_universe
from hdt.quant.packet import load_packet
from hdt.quant.quant_core import QuantCore, QuantPins
from hdt.quant.scanner import ACCOUNT_STATE_SCAN, latest_account_state
from hdt.settings.versions import SectionNotConfiguredError, load_pin

log = logging.getLogger(__name__)

BOUND_EVENTS_MAX: Final[int] = 256
SCANNER_LOG_WAIT_S: Final[float] = 1.0
"""How long the trigger waits for the candidate's committed `scanner_log` row (the scanner publishes only
after that commit, so a missing row is a rollback or a foreign publisher: treated as shadow only)."""
SCANNER_LOG_POLL_S: Final[float] = 0.25


class PgPacketStore:
    def __init__(self, engine: sa.Engine) -> None:
        self.engine = engine

    def packet(self, packet_sha256: str) -> QuantPacket | None:
        with self.engine.connect() as conn:
            return load_packet(conn, packet_sha256)


def quant_pins(pins: EventPins) -> QuantPins:
    return QuantPins(
        config_version_ids=dict(pins.config_version_ids),
        universe_date=pins.universe_date,
        target_type=pins.target_type,
        label_spec_version=pins.label_spec_version,
    )


class QuantCandidateSets:
    def __init__(self, core: QuantCore) -> None:
        self.core = core

    def candidate_set(self, coin_id: int, as_of: datetime, pins: EventPins) -> CandidateSet:
        return self.core.candidate_set(coin_id, as_of, quant_pins(pins))


class EventQuantCore:
    """`QuantCoreFn` for the live tool: the pins of the event at (coin, as_of) are bound by the graph
    before any agent of that event runs (`bind`)."""

    def __init__(self, core: QuantCore) -> None:
        self.core = core
        self._pins: OrderedDict[tuple[int, datetime], QuantPins] = OrderedDict()
        self._lock = threading.Lock()

    def bind(self, coin_id: int, as_of: datetime, pins: EventPins) -> None:
        with self._lock:
            key = (coin_id, ensure_utc(as_of))
            self._pins[key] = quant_pins(pins)
            self._pins.move_to_end(key)
            while len(self._pins) > BOUND_EVENTS_MAX:
                self._pins.popitem(last=False)

    def __call__(self, agent: AgentName, coin_id: int, as_of: datetime) -> QuantPacket:
        with self._lock:
            pins = self._pins.get((coin_id, ensure_utc(as_of)))
        if pins is None:
            raise LookupError(f"no council event is bound for coin {coin_id} at {as_of.isoformat()}")
        return self.core.quant_core(agent, coin_id, as_of, pins)


class LakeUniverse:
    def __init__(self, pit: PitQuery) -> None:
        self.pit = pit

    def entry(self, coin_id: int, as_of: datetime) -> UniverseEntry | None:
        universe = load_universe(self.pit, as_of)
        if universe is None:
            return None
        member = next((m for m in universe.members if m.cmc_id == coin_id), None)
        if member is None:
            return None
        return UniverseEntry(symbol=member.binance_symbol, universe_date=universe.date)


class StreamHeldPositions:
    """Held side of a coin in the running namespace (newest `AccountState`; hedge book excluded)."""

    def __init__(self, redis: Redis, universe: LakeUniverse, account: Callable[[], Account]) -> None:
        self.redis = redis
        self.universe = universe
        self.account = account

    async def held_side(self, coin_id: int, as_of: datetime) -> Side | None:
        entry = self.universe.entry(coin_id, as_of)
        if entry is None:
            return None
        entries: Any = await self.redis.xrevrange(str(Stream.ACCOUNT_STATE), count=ACCOUNT_STATE_SCAN)
        state = latest_account_state(entries, self.account())
        if state is None:
            return None
        for position in state.positions:
            if position.symbol == entry.symbol and not position.is_hedge_book and position.qty != 0:
                return Side.LONG if position.qty > 0 else Side.SHORT
        return None


class PgScannerLog:
    """Whether an LTX / MIGRATION candidate passed only the loose superset thresholds (shadow only).

    The scanner publishes a candidate only after its `scanner_log` row is committed; the row is still polled
    for up to `wait_s` (defense in depth), and when it is missing the candidate is treated as shadow only
    (fail closed: an unproven strict pass never opens a position)."""

    def __init__(
        self,
        engine: sa.Engine,
        *,
        wait_s: float = SCANNER_LOG_WAIT_S,
        poll_s: float = SCANNER_LOG_POLL_S,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.engine = engine
        self.wait_s = wait_s
        self.poll_s = poll_s
        self.sleep = sleep

    def _strict(self, candidate: Candidate) -> bool | None:
        row = ScannerLogRow
        with self.engine.connect() as conn:
            strict: bool | None = conn.execute(
                sa.select(row.strict_pass).where(
                    row.coin_id == candidate.coin_id,
                    row.as_of == candidate.as_of,
                    row.rule == candidate.source.value,
                    row.rule_version == candidate.rule_version,
                    row.emitted.is_(True),
                )
            ).scalar_one_or_none()
        return strict

    def shadow_only(self, candidate: Candidate) -> bool:
        if candidate.source not in (CandidateSource.LTX, CandidateSource.MIGRATION):
            return False
        waited = 0.0
        while True:
            strict = self._strict(candidate)
            if strict is not None:
                return not strict
            if waited >= self.wait_s:
                break
            self.sleep(self.poll_s)
            waited += self.poll_s
        log.warning(
            "no committed scanner_log row for the candidate; admitted as shadow only",
            extra={"coin_id": candidate.coin_id, "rule": candidate.source.value},
        )
        return True


class PinnedSettings:
    """Council file of recorded config versions (cached per version set)."""

    def __init__(self, session_factory: sessionmaker[Session], static: StaticConfig) -> None:
        self.session_factory = session_factory
        self.static = static
        self._cache: dict[tuple[tuple[str, int], ...], EventSettings] = {}
        self._lock = threading.Lock()

    def __call__(self, version_ids: Mapping[str, int]) -> EventSettings:
        key = tuple(sorted((str(k), int(v)) for k, v in version_ids.items()))
        with self._lock:
            found = self._cache.get(key)
        if found is not None:
            return found
        with self.session_factory() as session:
            pin = load_pin(session, dict(key))
        try:
            council = pin.council(self.static)
        except SectionNotConfiguredError:
            council = self.static.council
        settings = EventSettings(council=council)
        with self._lock:
            self._cache[key] = settings
        return settings

    def council(self, version_ids: Mapping[str, int]) -> CouncilFile:
        return self(version_ids).council
