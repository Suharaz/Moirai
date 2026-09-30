"""Natural-key dedupe: a redelivered message applies its effect exactly once."""

from __future__ import annotations

import pytest
import sqlalchemy as sa
from sqlalchemy import Engine

from hdt.db.dedupe import insert_once

pytestmark = pytest.mark.pg

_metadata = sa.MetaData()
processed = sa.Table(
    "test_processed_decisions",
    _metadata,
    sa.Column("account", sa.Text, primary_key=True),
    sa.Column("event_id", sa.Text, primary_key=True),
    sa.Column("verdict", sa.Text, nullable=False),
)


def test_second_delivery_of_the_same_natural_key_is_a_no_op(pg_engine: Engine) -> None:
    _metadata.create_all(pg_engine)
    with pg_engine.begin() as conn:
        assert insert_once(conn, processed, {"account": "paper", "event_id": "e1", "verdict": "accepted"})
    with pg_engine.begin() as conn:
        assert not insert_once(conn, processed, {"account": "paper", "event_id": "e1", "verdict": "rejected"})
        assert insert_once(conn, processed, {"account": "live", "event_id": "e1", "verdict": "accepted"})
    with pg_engine.connect() as conn:
        rows = conn.execute(sa.select(processed).order_by(processed.c.account)).all()
    assert [(r.account, r.verdict) for r in rows] == [("live", "accepted"), ("paper", "accepted")]


def test_missing_natural_key_column_is_rejected(pg_engine: Engine) -> None:
    _metadata.create_all(pg_engine)
    with pg_engine.begin() as conn, pytest.raises(ValueError, match="event_id"):
        insert_once(conn, processed, {"account": "paper", "verdict": "accepted"})
