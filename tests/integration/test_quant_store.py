"""Packet store: insert-only for hdt_council, readable and re-verifiable by hdt_risk (after migrations)."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy.exc import ProgrammingError

from hdt.contracts.common import AgentName, TargetType
from hdt.contracts.packet import QuantPacket
from hdt.features.common import FeatureBlock
from hdt.quant.packet import build_packet, load_packet, store_packet

pytestmark = [pytest.mark.pg, pytest.mark.integration]

ROOT = Path(__file__).resolve().parents[2]


def _packet() -> QuantPacket:
    block = FeatureBlock()
    block.update({"atr_1h": 2.5, "beta_btc": 1.2, "mark_price": 100.0})
    return build_packet(
        agent=AgentName.CROWDING,
        coin_id=1027,
        as_of=datetime(2026, 9, 1, 12, 5, 30, tzinfo=UTC),
        block=block,
        p_model=0.5,
        p_model_ver="crowding-pm0",
        candidate_set_sha256="b" * 64,
        universe_date=date(2026, 9, 1),
        config_version_ids={"risk": 1},
        feature_ver="f1.s1",
        target_type=TargetType.RESID_12H,
        label_spec_version="lbl1",
    )


def test_council_inserts_once_risk_reads_and_detects_tampering(
    fresh_database: Callable[[], AbstractContextManager[str]], pg_role_url: Callable[[str, str], str]
) -> None:
    packet = _packet()
    with fresh_database() as url:
        cfg = Config(str(ROOT / "alembic.ini"))
        cfg.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
        command.upgrade(cfg, "head")
        council = sa.create_engine(pg_role_url(url, "hdt_council"))
        risk = sa.create_engine(pg_role_url(url, "hdt_risk"))
        admin = sa.create_engine(url)
        try:
            for _ in range(2):  # resume after a crash: the duplicate insert is a no-op
                with council.begin() as conn:
                    store_packet(conn, packet)
            with risk.connect() as conn:
                assert load_packet(conn, packet.packet_sha256) == packet
                count = conn.execute(sa.text("SELECT count(*) FROM quant_packets")).scalar_one()
                assert count == 1
            with council.begin() as conn, pytest.raises(ProgrammingError, match="permission denied"):
                conn.execute(sa.text("UPDATE quant_packets SET agent = 'macro'"))
            with risk.begin() as conn, pytest.raises(ProgrammingError, match="permission denied"):
                store_packet(conn, packet)
            with admin.begin() as conn:
                conn.execute(
                    sa.text("UPDATE quant_packets SET payload = jsonb_set(payload, '{p_model}', '0.9')")
                )
            with risk.connect() as conn, pytest.raises(ValueError, match="packet_sha256"):
                load_packet(conn, packet.packet_sha256)
        finally:
            for engine in (council, risk, admin):
                engine.dispose()
