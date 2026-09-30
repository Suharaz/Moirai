"""Packet assembly and insert-only storage.

`packet_sha256` is the sha256 of the canonical JSON of the packet without that field
(`QuantPacket.build`). Storage writes the candidate set first, then the packet, both with
`ON CONFLICT DO NOTHING` on the hash (idempotent on resume): the same content always has the same key, so a
duplicate insert is by construction the same row. The caller returns the packet only after the commit.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert

from hdt.contracts.candidate import CandidateSet
from hdt.contracts.common import AgentName, TargetType
from hdt.contracts.packet import QuantPacket
from hdt.core.clock import utcnow
from hdt.core.ids import to_canonical
from hdt.db.models.quant import CandidateSetRow, QuantPacketRow
from hdt.features.common import FeatureBlock


def build_packet(
    *,
    agent: AgentName,
    coin_id: int,
    as_of: datetime,
    block: FeatureBlock,
    p_model: float,
    p_model_ver: str,
    candidate_set_sha256: str,
    universe_date: date,
    config_version_ids: Mapping[str, int],
    feature_ver: str,
    target_type: TargetType,
    label_spec_version: str,
) -> QuantPacket:
    return QuantPacket.build(
        agent=agent,
        coin_id=coin_id,
        as_of=as_of,
        features=dict(sorted(block.values.items())),
        p_model=p_model,
        candidate_set_sha256=candidate_set_sha256,
        data_quality=block.data_quality(),
        universe_date=universe_date,
        config_version_ids=dict(sorted(config_version_ids.items())),
        feature_ver=feature_ver,
        p_model_ver=p_model_ver,
        target_type=target_type,
        label_spec_version=label_spec_version,
    )


def store_candidate_set(conn: sa.Connection, candidate_set: CandidateSet) -> None:
    conn.execute(
        insert(CandidateSetRow)
        .values(
            candidate_set_sha256=candidate_set.candidate_set_sha256,
            coin_id=candidate_set.coin_id,
            as_of=candidate_set.as_of,
            payload=to_canonical(candidate_set),
            created_at=utcnow(),
        )
        .on_conflict_do_nothing(index_elements=["candidate_set_sha256"])
    )


def store_packet(conn: sa.Connection, packet: QuantPacket) -> None:
    conn.execute(
        insert(QuantPacketRow)
        .values(
            packet_sha256=packet.packet_sha256,
            agent=packet.agent.value,
            coin_id=packet.coin_id,
            as_of=packet.as_of,
            candidate_set_sha256=packet.candidate_set_sha256,
            payload=to_canonical(packet),
            created_at=utcnow(),
        )
        .on_conflict_do_nothing(index_elements=["packet_sha256"])
    )


def load_packet(conn: sa.Connection, packet_sha256: str) -> QuantPacket | None:
    """Parse a stored packet. Parsing re-verifies the payload against its own hash, and that hash must be
    the requested one: a row whose payload was modified, or swapped for another self-consistent packet,
    raises `ValueError`."""
    payload = conn.execute(
        sa.select(QuantPacketRow.payload).where(QuantPacketRow.packet_sha256 == packet_sha256)
    ).scalar_one_or_none()
    if payload is None:
        return None
    packet = QuantPacket.model_validate(payload)
    if packet.packet_sha256 != packet_sha256:
        raise ValueError(f"packet_sha256 {packet_sha256} does not match its stored payload")
    return packet


def load_candidate_set(conn: sa.Connection, candidate_set_sha256: str) -> CandidateSet | None:
    payload = conn.execute(
        sa.select(CandidateSetRow.payload).where(CandidateSetRow.candidate_set_sha256 == candidate_set_sha256)
    ).scalar_one_or_none()
    if payload is None:
        return None
    candidate_set = CandidateSet.model_validate(payload)
    if candidate_set.candidate_set_sha256 != candidate_set_sha256:
        raise ValueError(f"candidate set {candidate_set_sha256} does not match its stored payload")
    return candidate_set
