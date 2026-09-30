"""Council replay against Postgres and the synthetic lake (phase 05 / 06 replay criteria).

A meeting is recorded the way the council service runs one (`LlmAgentRunner` over the offline tool registry,
the LLM router live against a fake OpenRouter, `PgDecisionStore`, `llm_cache` / `llm_calls`, agent versions,
episodic memory), as role `hdt_council` on a migrated database. The production replay wiring
(`open_pg_replay`) then replays it read only: the identical card hash, a doctored `llm_cache` reply reported
as a difference, a deleted `llm_cache` row as a cache miss, and the day report.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import httpx2
import pytest
import sqlalchemy as sa
from langgraph.checkpoint.memory import InMemorySaver
from sqlalchemy.orm import Session

from fixtures.quant_db import QuantDb
from fixtures.synthetic_lake import FAR_FUTURE, MAIN_DAY, XTRG, A, SyntheticLake
from hdt.agents.llm_router import LlmRouter
from hdt.agents.runner import LlmAgentRunner, PgPins
from hdt.agents.versions import AgentVersionBook
from hdt.contracts.candidate import Candidate
from hdt.contracts.common import AgentName, CandidateSource, Side, TargetType
from hdt.core.clock import ManualClock, use_clock
from hdt.core.config import scanner_config, static_config
from hdt.core.ids import to_canonical
from hdt.council.adapters import (
    EventQuantCore,
    LakeUniverse,
    PgPacketStore,
    PinnedSettings,
    QuantCandidateSets,
)
from hdt.council.decision_card import PgDecisionStore
from hdt.council.graph import CouncilGraph, CouncilServices
from hdt.council.replay import ReplayReport, main, open_pg_replay
from hdt.council.trigger import event_id_for
from hdt.db.models.decision import CouncilEventRow
from hdt.db.models.llm import LlmCacheRow
from hdt.db.session import make_session_factory
from hdt.memory.store import open_postgres_store
from hdt.news.assess import PgNewsAssessor
from hdt.news.market import LakeMarketView
from hdt.news.official import PgOfficialSource
from hdt.news.source_lookup import PgSourceLookup
from hdt.news.store import PgNewsIndex
from hdt.news.unlocks import PgUnlockCalendar
from hdt.quant.quant_core import QuantCore
from hdt.scoring.params import PgParamsSource
from hdt.scoring.shadow import MemoryShadowLessons, PgShadowForecastSink
from hdt.settings import store
from hdt.settings.schemas import ModelsSection, RoleModelConfig, Section
from hdt.tools.replay import build_replay_registry

pytestmark = [pytest.mark.pg, pytest.mark.integration]

MODEL = "anthropic/claude-sonnet-4.5"
P_LLM = 0.6
DOCTORED_P_LLM = 0.3
STARTED = A + timedelta(seconds=30)
WRITTEN_TABLES = (
    "council_events",
    "decision_commits",
    "decision_cards",
    "decision_forecasts",
    "decision_claims",
    "decision_outbox",
    "lesson_shadow_forecasts",
    "llm_calls",
    "llm_cache",
    "agent_versions",
    "quant_packets",
    "candidate_sets",
    "audit_log",
    "store",
)


def draft(p_llm: float) -> str:
    return json.dumps({"p_llm": p_llm, "abstain": False, "claims": [], "cited_claim_ids": [], "reason": "r"})


class FakeOpenRouter:
    """Tool steps end without a call; every structured forecast is `p_llm = 0.6`."""

    def __init__(self) -> None:
        self.requests = 0

    def handler(self, request: httpx2.Request) -> httpx2.Response:
        body = json.loads(request.content)
        self.requests += 1
        content = "no tool needed" if "tools" in body else draft(P_LLM)
        return httpx2.Response(
            200,
            json={
                "id": f"gen-{self.requests}",
                "object": "chat.completion",
                "created": 1,
                "model": MODEL,
                "provider": "Anthropic",
                "choices": [
                    {
                        "index": 0,
                        "finish_reason": "stop",
                        "message": {"role": "assistant", "content": content},
                    }
                ],
                "usage": {"prompt_tokens": 100, "completion_tokens": 20, "total_tokens": 120, "cost": 0.0025},
            },
        )


class NotHeld:
    async def held_side(self, coin_id: int, as_of: datetime) -> Side | None:
        return None


@dataclass(frozen=True)
class Recorded:
    event_id: str
    url: str
    """`hdt_council` URL of the test database."""
    requests: int
    """OpenRouter requests the recording made."""


def _url(engine: sa.Engine) -> str:
    return engine.url.render_as_string(hide_password=False)


def _models(db: QuantDb) -> dict[str, int]:
    role = RoleModelConfig.model_validate(
        {
            "model": MODEL,
            "temperature": 0.2,
            "max_tokens": 800,
            "timeout_s": 30,
            "provider": {"only": ["Anthropic"]},
        }
    )
    models = ModelsSection(roles={agent: role for agent in static_config().council.agents})
    with Session(db.admin) as session, session.begin():
        active = store.get_active(session, Section.MODELS)
        version = store.create_version(
            session,
            Section.MODELS,
            models,
            author="test",
            reason="replay test models",
            parent_id=active.id if active is not None else None,
        )
    return {**db.version_ids, Section.MODELS.value: version.id}


async def _record(db: QuantDb, lake: SyntheticLake) -> Recorded:
    static = static_config()
    version_ids = _models(db)
    candidate = Candidate(
        coin_id=XTRG.cmc_id,
        as_of=A,
        source=CandidateSource.LTX,
        score=2.5,
        rule_version="ltx-v1",
        target_type=TargetType.RESID_12H,
        label_spec_version=scanner_config().labels.label_spec_version,
    )
    event_id = event_id_for(candidate)
    with db.council.begin() as conn:
        conn.execute(
            sa.insert(CouncilEventRow).values(
                event_id=event_id,
                coin_id=candidate.coin_id,
                as_of=candidate.as_of,
                source=candidate.source.value,
                candidate=to_canonical(candidate),
                config_version_ids=version_ids,
                unscored=False,
                shadow_only=False,
                status="done",
                skip_reason=None,
                attempts=1,
                error=None,
                msg_id=None,
                created_at=A,
                updated_at=STARTED,
            )
        )
    url = _url(db.council)
    sessions = make_session_factory(db.council)
    core = QuantCore(lake.pit, db.council, clock=lambda: FAR_FUTURE)
    event_core = EventQuantCore(core)
    fake = FakeOpenRouter()
    llm = LlmRouter(
        mode="live",
        session_factory=sessions,
        api_key=lambda: "sk-or-test",
        http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(fake.handler)),
    )
    with open_postgres_store(url) as memory:
        runner = LlmAgentRunner(
            llm=llm,
            registry=build_replay_registry(
                pit=lake.pit,
                stale_s=static.settings.data.stale_s,
                news_sources=static.news_sources,
                quant_core=event_core,
                news=PgNewsIndex(db.council),
                official=PgOfficialSource(db.council),
                unlocks=PgUnlockCalendar(db.council),
                memory=memory,
            ),
            pins=PgPins(sessions),
            memory=memory,
            versions=AgentVersionBook(sessions, write=True, author="council"),
            health=None,
            news=PgNewsAssessor(db.council, LakeMarketView(lake.pit, static=static)),
            static=static,
        )
        app = CouncilGraph(
            CouncilServices(
                runner=runner,
                packets=PgPacketStore(db.council),
                candidate_sets=QuantCandidateSets(core),
                sources=PgSourceLookup(db.council, lake.pit, static.news_sources),
                params=PgParamsSource(db.council, council=static.council),
                held=NotHeld(),
                universe=LakeUniverse(lake.pit),
                store=PgDecisionStore(db.council, shadow_sink=PgShadowForecastSink(), memory=memory),
                settings=PinnedSettings(sessions, static),
                mode="replay",
                lessons=MemoryShadowLessons(memory),
                bind_pins=event_core.bind,
            )
        ).compile(InMemorySaver())
        start = {
            "event": {
                "event_id": event_id,
                "candidate": to_canonical(candidate),
                "config_version_ids": version_ids,
                "unscored": False,
                "shadow_only": False,
            }
        }
        with use_clock(ManualClock(STARTED)):
            await app.ainvoke(start, {"configurable": {"thread_id": event_id}}, durability="sync")
    await llm.aclose()
    return Recorded(event_id=event_id, url=url, requests=fake.requests)


@pytest.fixture(scope="module")
def recorded(quant_db: QuantDb, main_lake: SyntheticLake) -> Recorded:
    return asyncio.run(_record(quant_db, main_lake))


def _counts(engine: sa.Engine) -> dict[str, int]:
    with engine.connect() as conn:
        return {
            t: int(conn.execute(sa.text(f"SELECT count(*) FROM {t}")).scalar_one()) for t in WRITTEN_TABLES
        }


CACHE = cast(sa.Table, LlmCacheRow.__table__)
CACHE_KEY = ("prompt_hash", "model_slug", "provider", "params_hash")


def _key(row: dict[str, Any]) -> list[Any]:
    return [CACHE.c[k] == row[k] for k in CACHE_KEY]


@pytest.fixture
def technical_reply(quant_db: QuantDb) -> Iterator[dict[str, Any]]:
    """The cached structured reply of the Technical agent (restored after the test)."""
    with quant_db.admin.connect() as conn:
        row = conn.execute(
            sa.select(CACHE).where(
                CACHE.c.role == AgentName.TECHNICAL.value,
                CACHE.c.response["content"].astext.contains("p_llm"),
            )
        ).one()
    original = dict(row._mapping)
    yield original
    with quant_db.admin.begin() as conn:
        conn.execute(sa.delete(CACHE).where(*_key(original)))
        conn.execute(sa.insert(CACHE).values(**original))


async def test_the_recorded_meeting_replays_to_its_card_hash_read_only(
    recorded: Recorded, quant_db: QuantDb, main_lake: SyntheticLake
) -> None:
    before = _counts(quant_db.admin)
    with open_pg_replay(recorded.url, static=static_config(), pit=main_lake.pit) as replay:
        result = await replay.event(recorded.event_id)
        with pytest.raises(sa.exc.DBAPIError, match="read-only transaction"), replay.engine.begin() as conn:
            conn.execute(sa.text("UPDATE council_events SET attempts = attempts"))

    assert result.status == "identical", result.differences
    assert result.replayed_card_sha256 == result.recorded_card_sha256
    assert recorded.requests > 0  # the recording asked the model; the replay has no HTTP client at all
    assert not any(r.verbatim for r in result.runs)
    assert {r.agent for r in result.runs} == {a.value for a in AgentName}  # all six agents ran
    assert _counts(quant_db.admin) == before  # nothing written: no card, outbox, commit, cache or memory row


async def test_a_doctored_cache_reply_is_reported_as_a_difference(
    recorded: Recorded, quant_db: QuantDb, main_lake: SyntheticLake, technical_reply: dict[str, Any]
) -> None:
    doctored = {**technical_reply["response"], "content": draft(DOCTORED_P_LLM)}
    with quant_db.admin.begin() as conn:
        conn.execute(sa.update(CACHE).where(*_key(technical_reply)).values(response=doctored))

    with open_pg_replay(recorded.url, static=static_config(), pit=main_lake.pit) as replay:
        result = await replay.event(recorded.event_id)

    assert result.status == "different"
    by_key = {d.key: d for d in result.differences}
    p_llm = by_key["forecast.technical.1.submitted.p_llm"]
    assert (p_llm.recorded, p_llm.replayed) == (P_LLM, DOCTORED_P_LLM)
    assert "card.card_sha256" in by_key
    assert ReplayReport((result,)).exit_code() == 1


async def test_a_deleted_cache_row_fails_the_replay_with_a_cache_miss(
    recorded: Recorded, quant_db: QuantDb, main_lake: SyntheticLake, technical_reply: dict[str, Any]
) -> None:
    with quant_db.admin.begin() as conn:
        conn.execute(sa.delete(CACHE).where(*_key(technical_reply)))

    with open_pg_replay(recorded.url, static=static_config(), pit=main_lake.pit) as replay:
        result = await replay.event(recorded.event_id)

    assert result.status == "cache_miss"
    assert result.error is not None
    assert result.error.startswith("replay: no cached reply for technical")
    assert result.replayed_card_sha256 is None
    report = ReplayReport((result,))
    assert report.to_json()["cache_misses"] == [{"event_id": recorded.event_id, "error": result.error}]
    assert report.exit_code() == 1


async def test_the_day_report_has_the_agent_metrics(recorded: Recorded, main_lake: SyntheticLake) -> None:
    day = datetime(MAIN_DAY.year, MAIN_DAY.month, MAIN_DAY.day, tzinfo=UTC)
    with open_pg_replay(recorded.url, static=static_config(), pit=main_lake.pit) as replay:
        report = await replay.range(day, day + timedelta(days=1))
        empty = await replay.range(day + timedelta(days=1), day + timedelta(days=2))

    out = report.to_json()
    assert (out["events"], out["identical"], out["different"], out["cache_misses"], out["errors"]) == (
        1,
        1,
        [],
        [],
        [],
    )
    assert set(out["agents"]) == {a.value for a in AgentName}
    for name, stats in out["agents"].items():
        assert stats["runs"] >= 1, name
        assert stats["parse_errors"] == 0, name
        assert stats["parse_error_rate"] == 0.0, name
        assert stats["runs"] == stats["llm_forecasts"] + sum(stats["abstain_reasons"].values()), name
    technical = out["agents"][AgentName.TECHNICAL.value]
    assert technical["llm_forecasts"] == technical["runs"]
    assert technical["mean_abs_p_llm_minus_p_model"] is not None
    assert out["passed"] is True
    assert report.exit_code() == 0
    assert empty.exit_code() == 2


def test_the_cli_reports_nothing_to_replay_with_exit_code_two(
    recorded: Recorded,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("HDT_PG_DSN", recorded.url)
    target = tmp_path / "reports" / "replay.json"
    assert main(["range", "--day", "2031-01-01", "--out", str(target)]) == 2
    printed = capsys.readouterr().out
    out = json.loads(printed)
    assert (out["events"], out["passed"], out["start"]) == (0, False, "2031-01-01T00:00:00.000000Z")
    assert json.loads(target.read_text(encoding="utf-8")) == out
    assert main(["event", "ev_unknown"]) == 2
    with pytest.raises(SystemExit):
        main(["range", "--start", "2031-01-01T00:00:00+00:00"])  # --start needs --end
