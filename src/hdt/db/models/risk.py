"""Phase 09 risk tables: decision dedupe, verdicts, the signed-intent outbox and the BTC hedge book.

Written only by `hdt_risk`. `risk_verdicts` backs the console decision card (`sizing` and `checks` JSON
shapes are the console read contract, `hdt.console.readmodels.SizingBreakdown` / `_CheckJson`).
`risk_intents` is a transactional outbox: signed `OrderIntent`s are written in the same transaction as the
verdict and relayed to stream `orders`, so a crash between commit and XADD never loses an intent.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import BigInteger, Boolean, CheckConstraint, Float, Identity, Index, Integer, Text, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from hdt.db.base import Base

ACCOUNTS = ("paper", "testnet", "live")
VERDICTS = ("approved", "rejected", "hold", "ignored")
DECISION_INTENTS = ("OPEN", "HOLD", "EXIT")


def _in(column: str, values: tuple[str, ...]) -> str:
    return f"{column} IN ({', '.join(repr(v) for v in values)})"


class ProcessedDecisionRow(Base):
    """Natural-key dedupe for stream `decisions`, inserted in the verdict transaction."""

    __tablename__ = "processed_decisions"
    __table_args__ = (CheckConstraint(_in("account", ACCOUNTS), name="account"),)

    account: Mapped[str] = mapped_column(Text, primary_key=True)
    event_id: Mapped[str] = mapped_column(Text, primary_key=True)
    processed_at: Mapped[datetime]


class RiskVerdictRow(Base):
    """One verdict per (account, decision); the console decision card reads it."""

    __tablename__ = "risk_verdicts"
    __table_args__ = (
        CheckConstraint(_in("account", ACCOUNTS), name="account"),
        CheckConstraint(_in("verdict", VERDICTS), name="verdict"),
        CheckConstraint(_in("decision_intent", DECISION_INTENTS), name="decision_intent"),
        Index("ix_risk_verdicts_account_decided_at", "account", "decided_at"),
    )

    account: Mapped[str] = mapped_column(Text, primary_key=True)
    event_id: Mapped[str] = mapped_column(Text, primary_key=True)
    msg_id: Mapped[str | None] = mapped_column(Text)
    coin_id: Mapped[int] = mapped_column(Integer)
    symbol: Mapped[str | None] = mapped_column(Text)
    decision_intent: Mapped[str] = mapped_column(Text)
    effective_intent: Mapped[str | None] = mapped_column(Text)
    verdict: Mapped[str] = mapped_column(Text)
    reason: Mapped[str | None] = mapped_column(Text)
    intent_id: Mapped[str | None] = mapped_column(Text)
    signature_ok: Mapped[bool | None] = mapped_column(Boolean)
    entry_client_id: Mapped[str | None] = mapped_column(Text)
    stop_client_algo_id: Mapped[str | None] = mapped_column(Text)
    sizing: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    checks: Mapped[list[dict[str, Any]]] = mapped_column(JSONB)
    hedge_beta: Mapped[float | None] = mapped_column(Float)
    config_version_ids: Mapped[dict[str, Any]]
    decided_at: Mapped[datetime]


class RiskIntentRow(Base):
    """Signed `OrderIntent` outbox (written with the verdict, relayed to stream `orders`)."""

    __tablename__ = "risk_intents"
    __table_args__ = (
        CheckConstraint(_in("account", ACCOUNTS), name="account"),
        Index("ix_risk_intents_account_event_id", "account", "event_id"),
        Index("ix_risk_intents_unpublished", "created_at", postgresql_where=text("published_at IS NULL")),
    )

    intent_id: Mapped[str] = mapped_column(Text, primary_key=True)
    account: Mapped[str] = mapped_column(Text)
    event_id: Mapped[str] = mapped_column(Text)
    leg: Mapped[str] = mapped_column(Text)
    seq: Mapped[int] = mapped_column(Integer)
    publish_order: Mapped[int] = mapped_column(Integer)
    payload: Mapped[dict[str, Any]]
    created_at: Mapped[datetime]
    published_at: Mapped[datetime | None]
    stream_id: Mapped[str | None] = mapped_column(Text)


class HedgeBookRow(Base):
    """Pooled BTC hedge book history per account (the newest row is the current book)."""

    __tablename__ = "hedge_book"
    __table_args__ = (
        CheckConstraint(_in("account", ACCOUNTS), name="account"),
        Index("ix_hedge_book_account_updated_at", "account", "updated_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    account: Mapped[str] = mapped_column(Text)
    symbol: Mapped[str] = mapped_column(Text)
    target_qty: Mapped[Decimal]
    actual_qty: Mapped[Decimal]
    target_notional: Mapped[Decimal]
    beta: Mapped[float | None] = mapped_column(Float)
    btc_mark: Mapped[Decimal]
    btc_atr_1h: Mapped[Decimal | None]
    stop_price: Mapped[Decimal | None]
    rebalance_seq: Mapped[int] = mapped_column(Integer)
    event_id: Mapped[str | None] = mapped_column(Text)
    updated_at: Mapped[datetime]
