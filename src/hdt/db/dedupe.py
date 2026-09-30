"""Natural-key deduplication for stream consumers.

Every consumer owns a `processed_*` table whose primary key is the message's natural key (for example
`processed_decisions(account, event_id)`). The handler calls `insert_once` inside the same transaction as
its effect: `True` means this is the first delivery and the effect must be applied; `False` means the
message was already handled, so the handler applies nothing and returns `True` to ack it.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert


def insert_once(conn: sa.Connection, table: sa.Table, values: Mapping[str, Any]) -> bool:
    """INSERT ... ON CONFLICT DO NOTHING on the table's primary key; True when the row was inserted."""
    primary_key = [column.name for column in table.primary_key.columns]
    if not primary_key:
        raise ValueError(f"table {table.name} has no primary key to deduplicate on")
    missing = [name for name in primary_key if name not in values]
    if missing:
        raise ValueError(f"natural key columns missing: {missing}")
    statement = (
        insert(table)
        .values(dict(values))
        .on_conflict_do_nothing(index_elements=primary_key)
        .returning(sa.literal(1))
    )
    return conn.execute(statement).first() is not None
