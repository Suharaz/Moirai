"""Phase 03 quant tables: stored packets, shared candidate sets and the scanner superset log.

All three are insert-only and written only by `hdt_council`; `hdt_risk` (verifies `packet_sha256`),
`hdt_scorer` and the console read them. `payload` holds the canonical JSON form of the contract
(`hdt.core.ids.to_canonical`), so `QuantPacket.model_validate(payload)` re-verifies the hash.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from sqlalchemy import Boolean, CheckConstraint, Date, Float, ForeignKeyConstraint, Index, Integer, Text
from sqlalchemy.orm import Mapped, mapped_column

from hdt.db.base import Base

SCANNER_RULES = ("LTX", "MIGRATION", "HELD")


class CandidateSetRow(Base):
    __tablename__ = "candidate_sets"
    __table_args__ = (Index("ix_candidate_sets_coin_id_as_of", "coin_id", "as_of"),)

    candidate_set_sha256: Mapped[str] = mapped_column(Text, primary_key=True)
    coin_id: Mapped[int] = mapped_column(Integer)
    as_of: Mapped[datetime]
    payload: Mapped[dict[str, Any]]
    created_at: Mapped[datetime]


class QuantPacketRow(Base):
    __tablename__ = "quant_packets"
    __table_args__ = (
        Index("ix_quant_packets_coin_id_as_of", "coin_id", "as_of"),
        Index("ix_quant_packets_candidate_set_sha256", "candidate_set_sha256"),
    )

    packet_sha256: Mapped[str] = mapped_column(Text, primary_key=True)
    agent: Mapped[str] = mapped_column(Text)
    coin_id: Mapped[int] = mapped_column(Integer)
    as_of: Mapped[datetime]
    candidate_set_sha256: Mapped[str] = mapped_column(Text)
    payload: Mapped[dict[str, Any]]
    created_at: Mapped[datetime]


class ScannerLogRow(Base):
    """One row per (coin, as_of, rule, rule_version) evaluation, emitted or not (superset log #24)."""

    __tablename__ = "scanner_log"
    __table_args__ = (
        CheckConstraint(f"rule IN ({', '.join(repr(r) for r in SCANNER_RULES)})", name="rule"),
        CheckConstraint("side IS NULL OR side IN ('LONG', 'SHORT')", name="side"),
        CheckConstraint("NOT (emitted AND dropped_budget)", name="emitted_or_dropped"),
        Index("ix_scanner_log_as_of", "as_of"),
    )

    coin_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    as_of: Mapped[datetime] = mapped_column(primary_key=True)
    rule: Mapped[str] = mapped_column(Text, primary_key=True)
    rule_version: Mapped[str] = mapped_column(Text, primary_key=True)
    side: Mapped[str | None] = mapped_column(Text)
    conditions: Mapped[dict[str, Any]]
    """Each condition's value plus its strict / loose check (null when the input is missing)."""
    strict_pass: Mapped[bool] = mapped_column(Boolean)
    loose_pass: Mapped[bool] = mapped_column(Boolean)
    contagion_blocked: Mapped[bool | None] = mapped_column(Boolean)
    emitted: Mapped[bool] = mapped_column(Boolean)
    dropped_budget: Mapped[bool] = mapped_column(Boolean)
    score: Mapped[float | None] = mapped_column(Float)
    target_type: Mapped[str] = mapped_column(Text)
    label_spec_version: Mapped[str] = mapped_column(Text)
    universe_date: Mapped[date] = mapped_column(Date)
    feature_ver: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime]


class ScannerPublishedRow(Base):
    """Delivery mark of an emitted `scanner_log` row: inserted once its candidate is XADDed (insert-only)."""

    __tablename__ = "scanner_published"
    __table_args__ = (
        ForeignKeyConstraint(
            ["coin_id", "as_of", "rule", "rule_version"],
            ["scanner_log.coin_id", "scanner_log.as_of", "scanner_log.rule", "scanner_log.rule_version"],
        ),
    )

    coin_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    as_of: Mapped[datetime] = mapped_column(primary_key=True)
    rule: Mapped[str] = mapped_column(Text, primary_key=True)
    rule_version: Mapped[str] = mapped_column(Text, primary_key=True)
    published_at: Mapped[datetime]
