"""HOLLOW_HYPE candidates on Postgres: one recorded but never published is sent on the next cycle, rebuilt
from the recorded row alone (target type and label spec fixed at insert)."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from types import SimpleNamespace
from typing import Any

import pytest
import sqlalchemy as sa

from hdt.contracts.candidate import Candidate
from hdt.contracts.common import TargetType
from hdt.core.clock import utcnow
from hdt.council.trigger import event_id_for
from hdt.news import main as news_main
from hdt.news.config import load_news_config
from hdt.news.main import republish_unsent
from hdt.news.store import CANDIDATES, insert_rows

CFG = load_news_config()


class Redis:
    def __init__(self) -> None:
        self.added: list[dict[Any, Any]] = []

    async def xadd(self, stream: str, fields: dict[Any, Any], **_: Any) -> bytes:
        self.added.append(fields)
        return b"1-0"

    def candidates(self, coin_id: int) -> list[Candidate]:
        sent = [Candidate.model_validate_json(fields["data"]) for fields in self.added]
        return [c for c in sent if c.coin_id == coin_id]


def _row(coin_id: int, minutes_ago: int, published: bool, label_spec: str = "spec-test") -> dict[str, Any]:
    at = utcnow() - timedelta(minutes=minutes_ago)
    return {
        "coin_id": coin_id,
        "as_of": at,
        "mode": "fade_hype",
        "score": 0.12,
        "rule_version": CFG.rule_version,
        "target_type": TargetType.RAW_12H.value,
        "label_spec_version": label_spec,
        "evidence": {},
        "published_at": at if published else None,
    }


def _published_at(engine: sa.Engine, coin_ids: list[int]) -> dict[int, object]:
    with engine.connect() as conn:
        return dict(
            conn.execute(
                sa.select(CANDIDATES.c.coin_id, CANDIDATES.c.published_at).where(
                    CANDIDATES.c.coin_id.in_(coin_ids)
                )
            ).all()
        )


def _republish(engine: sa.Engine, redis: Redis, since: Any) -> int:
    return asyncio.run(republish_unsent(engine, redis, since=since, maxlen=None))  # type: ignore[arg-type]


def test_unpublished_candidate_is_published_exactly_once_by_the_next_cycle(pg_engine: sa.Engine) -> None:
    since = utcnow() - timedelta(minutes=30)
    with pg_engine.begin() as conn:
        insert_rows(conn, CANDIDATES, [_row(801, 5, published=False)])  # the worker stopped before XADD
        insert_rows(conn, CANDIDATES, [_row(802, 5, published=True)])
        insert_rows(conn, CANDIDATES, [_row(803, 120, published=False)])  # too old: stays unpublished
    redis = Redis()
    assert _republish(pg_engine, redis, since) == 1
    assert [c.coin_id for c in redis.candidates(801)] == [801]
    assert redis.candidates(802) == []
    assert redis.candidates(803) == []
    rows = _published_at(pg_engine, [801, 803])
    assert rows[801] is not None
    assert rows[803] is None
    assert _republish(pg_engine, redis, since) == 0
    assert len(redis.candidates(801)) == 1


def test_republish_carries_the_recorded_label_spec_not_the_current_config(
    pg_engine: sa.Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    since = utcnow() - timedelta(minutes=30)
    with pg_engine.begin() as conn:
        insert_rows(conn, CANDIDATES, [_row(811, 5, published=False, label_spec="spec-x")])
    # The worker restarts with a config whose label spec is now Y.
    changed = SimpleNamespace(labels=SimpleNamespace(label_spec_version="spec-y"))
    monkeypatch.setattr(news_main, "scanner_config", lambda: changed)
    redis = Redis()
    assert _republish(pg_engine, redis, since) == 1
    [candidate] = redis.candidates(811)
    assert (candidate.target_type, candidate.label_spec_version) == (TargetType.RAW_12H, "spec-x")


def test_a_kill_after_the_xadd_republishes_under_the_same_council_event_id(
    pg_engine: sa.Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    since = utcnow() - timedelta(minutes=30)
    with pg_engine.begin() as conn:
        insert_rows(conn, CANDIDATES, [_row(821, 5, published=False)])
    redis = Redis()

    def killed(*_: Any, **__: Any) -> None:
        raise SystemExit("killed between the XADD and the mark")

    with monkeypatch.context() as patch:
        patch.setattr(news_main, "mark_candidate_published", killed)
        with pytest.raises(SystemExit):
            _republish(pg_engine, redis, since)
    assert _published_at(pg_engine, [821])[821] is None
    assert _republish(pg_engine, redis, since) == 1
    assert _published_at(pg_engine, [821])[821] is not None
    first, second = redis.candidates(821)
    assert first == second
    assert event_id_for(first) == event_id_for(second)
