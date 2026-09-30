"""Veto scan on Postgres: a poisoned item never stops the scan, a failed keyword verdict stays a soft veto."""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import timedelta
from pathlib import Path
from typing import Any

import sqlalchemy as sa
from pydantic import BaseModel

from hdt.agents.llm_types import Generation
from hdt.contracts.forecast import LlmUsage
from hdt.core.clock import utcnow
from hdt.core.config import static_config
from hdt.lake.pit_query import PitQuery
from hdt.lake.raw_store import RawStore
from hdt.lake.schemas import Capture
from hdt.lake.universe import Universe, UniverseMember, universe_capture
from hdt.news.config import load_news_config
from hdt.news.extractor import ExtractorOutput
from hdt.news.judge import JudgeOutput
from hdt.news.store import ITEMS, VERDICTS, VETO_SCANS, insert_rows, unjudged_items
from hdt.news.verdict import VerdictJob, base_row, store_verdict
from hdt.news.veto_scan import VetoScan
from hdt.settings.schemas import RoleModelConfig
from hdt.tools.impl.dex import LIQUIDITY_ROUTE

CFG = load_news_config()
POISON = "9999-12-31T23:59:59-01:00"
TITLE = "Xyz Protocol hacked: attacker drained the bridge"


class Redis:
    def __init__(self) -> None:
        self.added: list[tuple[str, dict[str, Any]]] = []

    async def xadd(self, stream: str, fields: dict[str, Any], **_: Any) -> bytes:
        self.added.append((stream, fields))
        return b"1-0"


class Models:
    def __init__(self) -> None:
        cfg = RoleModelConfig(model="vendor/model", temperature=0.0, max_tokens=800, timeout_s=30)
        self.roles = {"news_extractor": cfg, "news_judge_a": cfg, "news_judge_b": cfg}


class Pin:
    def models(self) -> Models:
        return Models()


class Watcher:
    def apply_pending(self) -> None:
        return None

    def pin(self) -> Pin:
        return Pin()


class PoisonLlm:
    """Well-formed replies whose event times sit at the far end of the calendar."""

    def __init__(self) -> None:
        self.by_role: dict[str, BaseModel] = {
            "news_extractor": ExtractorOutput(
                has_event=True,
                event_class="EXPLOIT",
                event_slug="bridge_hack",
                direction="down",
                quote=TITLE,
                event_time=POISON,
            ),
            "news_judge_a": _judged(),
            "news_judge_b": _judged(),
        }

    async def structured(self, *, role: str, schema: type[Any], **_: Any) -> Generation[Any]:
        output = self.by_role[role]
        assert isinstance(output, schema)
        developer = {"news_judge_a": "vendor-a", "news_judge_b": "vendor-b"}.get(role, "vendor-x")
        usage = LlmUsage(prompt_tokens=1, completion_tokens=1)
        return Generation(output, "m/x", f"{developer}/x", "p", f"g-{role}", usage, "0" * 64, "0" * 64, False)


def _judged() -> JudgeOutput:
    return JudgeOutput(
        event_class="EXPLOIT",
        direction="down",
        hardness=0.9,
        novelty=0.9,
        verified_tier="T1",
        event_time=POISON,
        confidence=0.8,
    )


def _item(item_id: str, coin_id: int, title: str) -> dict[str, Any]:
    now = utcnow() - timedelta(minutes=5)
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
        "title": title,
        "summary": None,
        "content": None,
        "published_at": now,
        "ingested_at": now,
        "coin_ids": [coin_id],
        "title_fingerprint": "f",
        "duplicate_of": None,
        "duplicate_kind": None,
        "similarity": None,
        "raw_sha256": "r",
    }


def _lake(tmp_path: Path, members: list[tuple[int, str]]) -> PitQuery:
    store = RawStore(tmp_path / "staging", tmp_path / "lake")
    built = utcnow() - timedelta(hours=1)
    universe = Universe(
        date=built.date(),
        built_at=built,
        members=tuple(
            UniverseMember(
                cmc_id=coin_id,
                cmc_symbol=symbol,
                binance_symbol=f"{symbol}USDT",
                multiplier=1,
                cmc_rank=40 + i,
                open_interest_usd=1e8,
            )
            for i, (coin_id, symbol) in enumerate(members)
        ),
        ltx_size=len(members),
        watchlist_size=len(members),
        sources={},
    )
    store.append(universe_capture(universe))
    return PitQuery(tmp_path / "staging", tmp_path / "lake")


def _scan(pg_engine: sa.Engine, pit: PitQuery, redis: Redis) -> VetoScan:
    return VetoScan(
        engine=pg_engine,
        sessions=None,  # type: ignore[arg-type]  # only used to alert on a failed cycle
        redis=redis,  # type: ignore[arg-type]
        pit=pit,
        llm=PoisonLlm(),
        watcher=Watcher(),  # type: ignore[arg-type]
        static=static_config(),
        config=CFG,
    )


def test_poisoned_event_time_never_stops_the_scan(pg_engine: sa.Engine, tmp_path: Path) -> None:
    tag = uuid.uuid4().hex[:8]
    poisoned, other = 710_000 + int(tag[:4], 16), 720_000 + int(tag[:4], 16)
    pit = _lake(tmp_path, [(poisoned, "XYZ"), (other, "ABCD")])
    with pg_engine.begin() as conn:
        insert_rows(conn, ITEMS, [_item(f"rss:poison-{tag}", poisoned, TITLE)])
    redis = Redis()
    published = asyncio.run(_scan(pg_engine, pit, redis).scan_once())
    assert published == 2
    assert len(redis.added) == 2
    with pg_engine.connect() as conn:
        row = conn.execute(
            sa.select(VERDICTS.c.status, VERDICTS.c.event_time).where(
                VERDICTS.c.item_id == f"rss:poison-{tag}"
            )
        ).one()
    assert row.status == "agreed"
    assert row.event_time is None  # the model's far-future time is ignored, never parsed into a crash


def test_unrepresentable_liquidity_time_never_stops_the_scan(pg_engine: sa.Engine, tmp_path: Path) -> None:
    tag = uuid.uuid4().hex[:8]
    drained, other = 740_000 + int(tag[:4], 16), 750_000 + int(tag[:4], 16)
    pit = _lake(tmp_path, [(drained, "XYZ"), (other, "ABCD")])
    body = json.dumps({"status": {"error_code": 0}, "data": {"lcs": [{"ts": 1.7e18, "tp": "remove"}]}})
    RawStore(tmp_path / "staging", tmp_path / "lake").append(
        Capture(
            "cmc",
            LIQUIDITY_ROUTE,
            utcnow() - timedelta(minutes=2),
            200,
            body.encode(),
            params={"address": "0xabc"},
            key=str(drained),
        )
    )
    item_id = f"rss:ts-{tag}"
    agreed: dict[str, Any] = {
        "status": "agreed",
        "event_class": "EXPLOIT",
        "direction": "down",
        "quote": TITLE,
    }
    agreed |= {"quote_ok": True, "class_a": "EXPLOIT", "class_b": "EXPLOIT", "detail": {}}
    with pg_engine.begin() as conn:
        insert_rows(conn, ITEMS, [_item(item_id, drained, TITLE)])
        insert_rows(conn, VERDICTS, [_verdict(item_id, drained, **agreed)])
    redis = Redis()
    # veto_verdicts re-checks the agreed EXPLOIT on chain at scan time and reads the unrepresentable time.
    assert asyncio.run(_scan(pg_engine, pit, redis).scan_once()) == 2
    assert len(redis.added) == 2


def _verdict(item_id: str, coin_id: int, **over: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "item_id": item_id,
        "rule_version": CFG.rule_version,
        "processed_at": utcnow() - timedelta(minutes=4),
        "processor": "news",
        "coin_ids": [coin_id],
        "status": "quote_failed",
        "event_key": None,
        "event_class": None,
        "direction": None,
        "hardness": None,
        "novelty": None,
        "confidence": None,
        "event_time": None,
        "quote": None,
        "quote_ok": False,
        "domain": "www.coindesk.com",
        "tier": "T1",
        "official": False,
        "official_ref": None,
        "class_a": None,
        "class_b": None,
        "direction_a": None,
        "direction_b": None,
        "known_event": None,
        "detail": {},
    }
    return row | over


def test_nul_in_a_model_quote_never_stalls_judging(pg_engine: sa.Engine, tmp_path: Path) -> None:
    tag = uuid.uuid4().hex[:8]
    coin_id = 760_000 + int(tag[:4], 16)
    pit = _lake(tmp_path, [(coin_id, "XYZ")])
    ids = [f"rss:nul-{tag}-{n}" for n in range(2)]
    with pg_engine.begin() as conn:
        insert_rows(conn, ITEMS, [_item(item_id, coin_id, TITLE) for item_id in ids])
    scan = _scan(pg_engine, pit, Redis())
    extractor = scan.llm.by_role["news_extractor"]  # type: ignore[attr-defined]
    scan.llm.by_role["news_extractor"] = extractor.model_copy(  # type: ignore[attr-defined]
        update={"quote": "funds\x00 are safe"}
    )
    asyncio.run(scan.scan_once())
    with pg_engine.connect() as conn:
        rows = conn.execute(
            sa.select(VERDICTS.c.item_id, VERDICTS.c.status, VERDICTS.c.detail).where(
                VERDICTS.c.item_id.in_(ids)
            )
        ).all()
    assert sorted(r.item_id for r in rows) == ids
    assert {r.status for r in rows} == {"quote_failed"}
    assert all("\x00" not in r.detail["rejected_quote"] for r in rows)


def test_refused_verdict_row_is_stored_as_minimal_failure(pg_engine: sa.Engine) -> None:
    tag = uuid.uuid4().hex[:8]
    item_id = f"rss:refused-{tag}"
    now = utcnow()
    with pg_engine.begin() as conn:
        insert_rows(conn, ITEMS, [_item(item_id, 770_000 + int(tag[:4], 16), TITLE)])
    with pg_engine.connect() as conn:
        item = next(
            i
            for i in unjudged_items(conn, CFG.rule_version, now - timedelta(hours=1), now)
            if i.item_id == item_id
        )
    job = VerdictJob(item, (), None, now, "news")  # type: ignore[arg-type]
    row = base_row(item, CFG.rule_version, now, "news")
    row["detail"]["rejected_quote"] = "raw\x00model text"  # bypasses clean_row: the database refuses it
    assert store_verdict(pg_engine, row, job, CFG.rule_version)
    with pg_engine.connect() as conn:
        stored = conn.execute(
            sa.select(VERDICTS.c.status, VERDICTS.c.detail).where(VERDICTS.c.item_id == item_id)
        ).one()
    assert stored.status == "llm_failed"
    assert stored.detail["failed_step"] == "store"
    assert "rejected_quote" not in stored.detail


def test_quote_failed_keyword_item_stays_a_soft_veto(pg_engine: sa.Engine, tmp_path: Path) -> None:
    tag = uuid.uuid4().hex[:8]
    coin_id = 730_000 + int(tag[:4], 16)
    pit = _lake(tmp_path, [(coin_id, "QRS")])
    item_id = f"rss:qf-{tag}"
    with pg_engine.begin() as conn:
        insert_rows(conn, ITEMS, [_item(item_id, coin_id, "Qrs Finance exploited, funds drained")])
        insert_rows(
            conn,
            VERDICTS,
            [
                {
                    "item_id": item_id,
                    "rule_version": CFG.rule_version,
                    "processed_at": utcnow() - timedelta(minutes=4),
                    "processor": "news",
                    "coin_ids": [coin_id],
                    "status": "quote_failed",
                    "event_key": None,
                    "event_class": None,
                    "direction": None,
                    "hardness": None,
                    "novelty": None,
                    "confidence": None,
                    "event_time": None,
                    "quote": None,
                    "quote_ok": False,
                    "domain": "www.coindesk.com",
                    "tier": "T1",
                    "official": False,
                    "official_ref": None,
                    "class_a": None,
                    "class_b": None,
                    "direction_a": None,
                    "direction_b": None,
                    "known_event": None,
                    "detail": {"rejected_quote": "funds are safe"},
                }
            ],
        )
    redis = Redis()
    asyncio.run(_scan(pg_engine, pit, redis).scan_once())
    with pg_engine.connect() as conn:
        size_mult: float = conn.execute(
            sa.select(VETO_SCANS.c.size_mult)
            .where(VETO_SCANS.c.coin_id == coin_id)
            .order_by(VETO_SCANS.c.as_of.desc())
        ).scalar_one()
    assert size_mult == CFG.veto.soft_size_mult
