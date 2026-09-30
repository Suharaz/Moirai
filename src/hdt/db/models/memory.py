"""Phase 04 table: the LangGraph memory store.

`store` has exactly the schema `langgraph.store.postgres.PostgresStore` expects (its own migrations 0-3),
created by Alembic so that no service needs DDL rights or calls `PostgresStore.setup()`. Memory never
expires (`ck_store_no_ttl`) and is append-only: migration `0004_memory` adds a trigger that refuses
DELETE, TRUNCATE and any UPDATE that changes a record, and row-level security that limits each writer
role to the namespaces and record types it owns.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import CheckConstraint, Index, Integer, Text, text
from sqlalchemy.orm import Mapped, mapped_column

from hdt.db.base import Base


class MemoryStoreRow(Base):
    """LangGraph `BaseStore` items; `prefix` is the namespace joined with dots (`news.episodic`)."""

    __tablename__ = "store"
    __table_args__ = (
        CheckConstraint("expires_at IS NULL AND ttl_minutes IS NULL", name="no_ttl"),
        Index("store_prefix_idx", "prefix", postgresql_ops={"prefix": "text_pattern_ops"}),
        Index("idx_store_expires_at", "expires_at", postgresql_where=text("expires_at IS NOT NULL")),
    )

    prefix: Mapped[str] = mapped_column(Text, primary_key=True)
    key: Mapped[str] = mapped_column(Text, primary_key=True)
    value: Mapped[dict[str, Any]]
    created_at: Mapped[datetime | None] = mapped_column(server_default=text("CURRENT_TIMESTAMP"))
    updated_at: Mapped[datetime | None] = mapped_column(server_default=text("CURRENT_TIMESTAMP"))
    expires_at: Mapped[datetime | None]
    ttl_minutes: Mapped[int | None] = mapped_column(Integer)
