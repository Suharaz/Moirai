"""In-process packet cache with the full key (red-team #34).

Key: (agent, coin_id, as_of, feature_ver, p_model_ver, config_hash, target_type, label_spec_version), with
`config_hash = sha256(canonical {config_version_ids, universe_date})`. The target fields are part of the key
because they are part of the packet: a packet built for one target is never served for another. A hit is
returned only when the stored packet's own fields match the key, so one agent's packet can never be served
to another agent even if a caller builds a wrong key.
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from threading import Lock

from hdt.contracts.common import AgentName, TargetType
from hdt.contracts.packet import QuantPacket
from hdt.core.clock import ensure_utc
from hdt.core.ids import canonical_sha256


def config_hash(config_version_ids: Mapping[str, int], universe_date: date) -> str:
    return canonical_sha256({"config_version_ids": dict(config_version_ids), "universe_date": universe_date})


@dataclass(frozen=True)
class CacheKey:
    agent: AgentName
    coin_id: int
    as_of: datetime
    feature_ver: str
    p_model_ver: str
    config_hash: str
    target_type: TargetType
    label_spec_version: str

    @classmethod
    def of(cls, packet: QuantPacket) -> CacheKey:
        return cls(
            packet.agent,
            packet.coin_id,
            ensure_utc(packet.as_of),
            packet.feature_ver,
            packet.p_model_ver,
            config_hash(packet.config_version_ids, packet.universe_date),
            packet.target_type,
            packet.label_spec_version,
        )


class PacketCache:
    """Thread-safe bounded LRU of packets."""

    def __init__(self, max_entries: int = 4096) -> None:
        self._max = max_entries
        self._items: OrderedDict[CacheKey, QuantPacket] = OrderedDict()
        self._lock = Lock()

    @property
    def max_entries(self) -> int:
        return self._max

    def get(self, key: CacheKey) -> QuantPacket | None:
        with self._lock:
            packet = self._items.get(key)
            if packet is None:
                return None
            self._items.move_to_end(key)
        return packet if CacheKey.of(packet) == key else None

    def put(self, packet: QuantPacket) -> CacheKey:
        key = CacheKey.of(packet)
        with self._lock:
            self._items[key] = packet
            self._items.move_to_end(key)
            while len(self._items) > self._max:
                self._items.popitem(last=False)
        return key

    def __len__(self) -> int:
        return len(self._items)
