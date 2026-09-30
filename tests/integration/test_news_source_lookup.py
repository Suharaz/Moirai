"""PgSourceLookup on Postgres: the event coins come from the agreed verdict known at `as_of`."""

from __future__ import annotations

import uuid
from datetime import timedelta
from pathlib import Path
from typing import Any

import sqlalchemy as sa

from hdt.core.clock import utcnow
from hdt.core.config import static_config
from hdt.lake.pit_query import PitQuery
from hdt.news.config import load_news_config
from hdt.news.source_lookup import PgSourceLookup
from hdt.news.store import ITEMS, VERDICTS, insert_rows

CFG = load_news_config()


def _item(item_id: str, coin_ids: list[int]) -> dict[str, Any]:
    at = utcnow() - timedelta(minutes=10)
    url = f"https://www.coindesk.com/{item_id}"
    return {
        "item_id": item_id,
        "source_key": "coindesk",
        "source_kind": "rss",
        "source_name": "CoinDesk",
        "url": url,
        "url_key": url,
        "domain": "www.coindesk.com",
        "tier": "T1",
        "title": "Xyz bridge drained",
        "summary": None,
        "content": None,
        "published_at": at,
        "ingested_at": at,
        "coin_ids": coin_ids,
        "title_fingerprint": "f",
        "duplicate_of": None,
        "duplicate_kind": None,
        "similarity": None,
        "raw_sha256": "r",
    }


def _verdict(item_id: str, coin_ids: list[int], status: str) -> dict[str, Any]:
    agreed = status == "agreed"
    return {
        "item_id": item_id,
        "rule_version": CFG.rule_version,
        "processed_at": utcnow() - timedelta(minutes=8),
        "processor": "news",
        "coin_ids": coin_ids,
        "status": status,
        "event_key": "exploit:x" if agreed else None,
        "event_class": "EXPLOIT" if agreed else None,
        "direction": "down" if agreed else None,
        "hardness": 0.9 if agreed else None,
        "novelty": 0.9 if agreed else None,
        "confidence": 0.8 if agreed else None,
        "event_time": None,
        "quote": "Xyz bridge drained" if agreed else None,
        "quote_ok": agreed,
        "domain": "www.coindesk.com",
        "tier": "T1",
        "official": False,
        "official_ref": None,
        "class_a": "EXPLOIT" if agreed else None,
        "class_b": "EXPLOIT" if agreed else None,
        "direction_a": None,
        "direction_b": None,
        "known_event": None,
        "detail": {},
    }


def test_event_coins_are_the_agreed_verdict_coins(pg_engine: sa.Engine, tmp_path: Path) -> None:
    tag = uuid.uuid4().hex[:8]
    base = 780_000 + int(tag[:4], 16)
    coins = [base + 2, base]
    agreed, failed = f"rss:sl-a-{tag}", f"rss:sl-q-{tag}"
    with pg_engine.begin() as conn:
        insert_rows(conn, ITEMS, [_item(agreed, coins), _item(failed, coins)])
        insert_rows(
            conn, VERDICTS, [_verdict(agreed, coins, "agreed"), _verdict(failed, coins, "quote_failed")]
        )
    lookup = PgSourceLookup(
        pg_engine, PitQuery(tmp_path / "staging", tmp_path / "lake"), static_config().news_sources
    )
    now = utcnow()
    source = lookup.stored_source(agreed, as_of=now, pinned=(), coin_id=base)
    assert source is not None
    assert source.event_coin_ids == (base, base + 2)
    unconfirmed = lookup.stored_source(failed, as_of=now, pinned=(), coin_id=base)
    assert unconfirmed is not None
    assert unconfirmed.event_coin_ids == ()
    before = lookup.stored_source(agreed, as_of=now - timedelta(minutes=9), pinned=(), coin_id=base)
    assert before is not None
    assert before.event_coin_ids == ()  # the verdict was not known yet
