"""Tool sessions: budget, mode and agent isolation, look-ahead refusal, the live fetch write path, skills."""

from __future__ import annotations

import shutil
import subprocess
import time
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy as sa
from sqlalchemy import Engine

from hdt.agents.skills.loader import SkillError, SkillLoader, git_tree_id
from hdt.contracts.common import AgentName, CandidateSource, TargetType
from hdt.contracts.forecast import AgentForecast, LakeRef
from hdt.contracts.packet import QuantPacket
from hdt.core.clock import ManualClock, use_clock
from hdt.core.config import static_config
from hdt.core.ids import sha256_hex
from hdt.lake.pit_query import PitQuery
from hdt.lake.raw_store import RawStore
from hdt.lake.schemas import Capture
from hdt.tools.base import ToolContext, ToolInputError
from hdt.tools.budget import ABSTAIN_REASON, ToolBudget
from hdt.tools.impl.fetch_source import ROUTE
from hdt.tools.live import build_live_registry
from hdt.tools.ports import FetchedDocument, NewsItem
from hdt.tools.registry import RegistryModeError, ToolRegistry
from hdt.tools.replay import build_replay_registry, replay_context

NEWS_TOOLS = {"get_news", "fetch_source"}  # the other News tools need phase 07 backends
AS_OF = datetime(2026, 3, 2, 12, tzinfo=UTC)
COIN = 42
URL = "https://www.coindesk.com/a?id=1"
ITEM = NewsItem(
    item_id="rss:1", coin_ids=(COIN,), title="AAA listed", url=URL, source_name="CoinDesk", ingested_at=AS_OF
)


def packet(agent: AgentName, coin_id: int, as_of: datetime) -> QuantPacket:
    return QuantPacket.build(
        agent=agent,
        coin_id=coin_id,
        as_of=as_of,
        features={"x": 1.0},
        p_model=0.5,
        candidate_set_sha256="b" * 64,
        data_quality={},
        universe_date=as_of.date(),
        config_version_ids={},
        feature_ver="f",
        p_model_ver="p",
        target_type=TargetType.RAW_12H,
        label_spec_version="l",
    )


class News:
    def items(self, coin_id: int, since: datetime, as_of: datetime, limit: int) -> Sequence[NewsItem]:
        return [ITEM] if since <= ITEM.ingested_at <= as_of else []

    def item(self, item_id: str, as_of: datetime) -> NewsItem | None:
        return ITEM if item_id == ITEM.item_id and ITEM.ingested_at <= as_of else None


class Fetcher:
    def __init__(self, chain: tuple[str, ...] = ()) -> None:
        self.chain = chain
        self.calls = 0

    async def fetch(self, item_id: str, url: str) -> FetchedDocument:
        self.calls += 1
        body = b"<p>AAA will be listed on Friday.</p>"
        return FetchedDocument(
            item_id=item_id,
            requested_url=url,
            final_url=self.chain[-1] if self.chain else url,
            redirect_chain=self.chain,
            http_status=200,
            content_type="text/html",
            body=body,
            body_sha256=sha256_hex(body),
        )


def replay(tmp_path: Path, quant: Any = packet) -> ToolRegistry:
    pit = PitQuery(tmp_path / "staging", tmp_path / "lake")
    return build_replay_registry(
        pit=pit, stale_s=120, news_sources=static_config().news_sources, quant_core=quant, news=News()
    )


def ctx(agent: AgentName, mode: Any = "replay") -> ToolContext:
    return ToolContext(mode, agent, "evt-1", COIN, AS_OF)


async def test_call_budget_forces_abstain(tmp_path: Path) -> None:
    session = replay(tmp_path).session(ctx(AgentName.TECHNICAL), ToolBudget(max_calls=2, max_seconds=45))
    statuses = [(await session.call("quant_core", {})).status for _ in range(4)]
    assert statuses == ["ok", "ok", "budget_exceeded", "budget_exceeded"]
    assert [r.status for r in session.records] == statuses
    assert session.abstain_reason == ABSTAIN_REASON


async def test_time_budget_forces_abstain(tmp_path: Path) -> None:
    def slow(agent: AgentName, coin_id: int, as_of: datetime) -> QuantPacket:
        time.sleep(0.5)
        return packet(agent, coin_id, as_of)

    session = replay(tmp_path, slow).session(ctx(AgentName.MACRO), ToolBudget(max_calls=8, max_seconds=0.05))
    assert (await session.call("quant_core", {})).status == "budget_exceeded"
    assert session.abstain_reason == ABSTAIN_REASON
    assert (await session.call("quant_core", {})).status == "budget_exceeded"


async def test_agents_only_reach_their_own_tools(tmp_path: Path) -> None:
    reg = replay(tmp_path)
    session = reg.session(ctx(AgentName.TECHNICAL), ToolBudget(8, 45))
    assert session.tool_names() == ("quant_core",)
    result = await session.call("get_news", {})
    assert result.status == "error"
    assert "not available" in (result.message or "")
    with pytest.raises(ToolInputError):
        reg.session(ctx(AgentName.TECHNICAL), ToolBudget(8, 45), allowed={"get_news"})
    news = reg.session(ctx(AgentName.NEWS), ToolBudget(8, 45), allowed={"get_news"})
    assert (await news.call("quant_core", {})).status == "error"


def test_registries_refuse_the_other_mode(tmp_path: Path) -> None:
    reg = replay(tmp_path)
    with pytest.raises(RegistryModeError):
        reg.session(ctx(AgentName.TECHNICAL, "live"), ToolBudget(8, 45))
    with pytest.raises(RegistryModeError):
        ToolRegistry("live", [reg.get("quant_core")])


async def test_look_ahead_and_other_coins_are_refused(tmp_path: Path) -> None:
    session = replay(tmp_path).session(ctx(AgentName.NEWS), ToolBudget(8, 45), allowed=NEWS_TOOLS)
    later = (AS_OF + timedelta(seconds=1)).isoformat()
    ahead = await session.call("get_news", {"as_of": later})
    assert ahead.status == "error"
    assert "after the event as_of" in (ahead.message or "")
    other = await session.call("get_news", {"coin_id": COIN + 1})
    assert other.status == "error"
    bad = await session.call("get_news", {"limit": 500})
    assert bad.status == "error"
    assert "invalid arguments" in (bad.message or "")


async def test_live_fetch_writes_raw_store_before_returning_and_replays_identically(tmp_path: Path) -> None:
    pit = PitQuery(tmp_path / "staging", tmp_path / "lake")
    store = RawStore(tmp_path / "staging", tmp_path / "lake")
    fetcher = Fetcher()
    live = build_live_registry(
        pit=pit,
        raw_store=store,
        stale_s=120,
        news_sources=static_config().news_sources,
        news=News(),
        fetcher=fetcher,
    )
    with use_clock(ManualClock(AS_OF + timedelta(seconds=20))):
        session = live.session(ctx(AgentName.NEWS, "live"), ToolBudget(8, 45), allowed=NEWS_TOOLS)
        first = await session.call("fetch_source", {"item_id": "rss:1"})
        again = await session.call("fetch_source", {"item_id": "rss:1"})
    assert first.status == "ok"
    assert first.data is not None
    assert first.data["text"] == "AAA will be listed on Friday."
    assert first.data["tier"] == "T1"
    stored = pit.series(
        "news", ROUTE, AS_OF, AS_OF + timedelta(hours=1), as_of=AS_OF + timedelta(hours=1), key="rss:1"
    )
    assert len(stored) == 1
    assert stored[0].body_sha256 == first.data["body_sha256"]
    assert fetcher.calls == 1
    assert again == first
    manifest = session.captures  # stored on the decision card
    assert [(ref.key, ref.body_sha256) for ref in manifest] == [("rss:1", first.data["body_sha256"])]

    async def replayed(event_id: str, pinned: tuple[LakeRef, ...]) -> Any:
        context = ToolContext("replay", AgentName.NEWS, event_id, COIN, AS_OF, pinned)
        session = replay(tmp_path).session(context, ToolBudget(8, 45), allowed=NEWS_TOOLS)
        return await session.call("fetch_source", {"item_id": "rss:1"})

    assert await replayed("evt-1", manifest) == first
    # recorded after as_of and not pinned: invisible, even to the event that created it
    assert (await replayed("evt-1", ())).status == "not_available"
    assert (await replayed("evt-2", ())).status == "not_available"
    # another event may not open this event's capture through a copied manifest
    assert (await replayed("evt-2", manifest)).status == "error"
    altered = (manifest[0].model_copy(update={"body_sha256": "0" * 64}),)
    refused = await replayed("evt-1", altered)
    assert refused.status == "error"
    assert refused.data is None


@pytest.mark.parametrize(
    ("owner_event", "owner_item", "status"),
    [("evt-1", "rss:1", "ok"), ("evt-other", "rss:1", "error"), ("evt-1", "rss:9", "error")],
)
async def test_replay_opens_a_pin_only_for_its_own_event_and_item(
    tmp_path: Path, owner_event: str, owner_item: str, status: str
) -> None:
    store = RawStore(tmp_path / "staging", tmp_path / "lake")
    record = store.append(
        Capture(
            source="news",
            route=ROUTE,
            fetched_at=AS_OF + timedelta(seconds=30),
            http_status=200,
            body=b"<p>AAA will be listed on Friday.</p>",
            params={
                "item_id": owner_item,
                "event_id": owner_event,
                "url": URL,
                "final_url": URL,
                "redirect_chain": [],
                "content_type": "text/html",
            },
            key="rss:1",
        )
    )
    context = ToolContext("replay", AgentName.NEWS, "evt-1", COIN, AS_OF, (LakeRef.of(record),))
    session = replay(tmp_path).session(context, ToolBudget(8, 45), allowed=NEWS_TOOLS)
    result = await session.call("fetch_source", {"item_id": "rss:1"})
    assert result.status == status
    assert (result.data is not None) == (status == "ok")


def card_forecast(agent: AgentName, round_: int, session_captures: tuple[LakeRef, ...]) -> AgentForecast:
    return AgentForecast(
        agent=agent,
        agent_version="v1",
        event_id="evt-1",
        round=round_,
        coin_id=COIN,
        as_of=AS_OF,
        target_type=TargetType.RAW_12H,
        label_spec_version="l",
        abstain=True,
        abstain_reason="test",
        capture_manifest=session_captures,
    )


@pytest.mark.pg
async def test_decision_card_round_trip_replays_exactly_the_stored_captures(
    tmp_path: Path, pg_engine: Engine
) -> None:
    pit = PitQuery(tmp_path / "staging", tmp_path / "lake")
    store = RawStore(tmp_path / "staging", tmp_path / "lake")
    live = build_live_registry(
        pit=pit,
        raw_store=store,
        stale_s=120,
        news_sources=static_config().news_sources,
        news=News(),
        fetcher=Fetcher(),
    )
    with use_clock(ManualClock(AS_OF + timedelta(seconds=20))):
        session = live.session(ctx(AgentName.NEWS, "live"), ToolBudget(8, 45), allowed=NEWS_TOOLS)
        first = await session.call("fetch_source", {"item_id": "rss:1"})
    assert first.status == "ok"
    # a later capture of the same item by the same event, never listed on the card
    store.append(
        Capture(
            source="news",
            route=ROUTE,
            fetched_at=AS_OF + timedelta(minutes=5),
            http_status=200,
            body=b"<p>AAA listing cancelled.</p>",
            params={"item_id": "rss:1", "event_id": "evt-1", "url": URL, "content_type": "text/html"},
            key="rss:1",
        )
    )

    card = [
        card_forecast(AgentName.NEWS, 1, session.captures),
        card_forecast(AgentName.MACRO, 1, ()),
    ]
    with pg_engine.connect() as conn:  # stored as the jsonb `decision_forecasts.forecast` value
        stored: list[str] = [
            conn.execute(sa.text("SELECT CAST(:f AS jsonb)::text"), {"f": f.model_dump_json()}).scalar_one()
            for f in card
        ]
    reloaded = [AgentForecast.model_validate_json(value) for value in stored]
    assert reloaded == card
    assert [f.commit_bytes() for f in reloaded] == [f.commit_bytes() for f in card]

    context = replay_context(AgentName.NEWS, reloaded, round_=1)
    assert context.pinned == session.captures
    assert (context.event_id, context.coin_id, context.as_of) == ("evt-1", COIN, AS_OF)
    replayed = replay(tmp_path).session(context, ToolBudget(8, 45), allowed=NEWS_TOOLS)
    assert await replayed.call("fetch_source", {"item_id": "rss:1"}) == first
    assert replayed.captures == session.captures

    unpinned = replay_context(AgentName.NEWS, [card_forecast(AgentName.NEWS, 1, ())], round_=1)
    fresh = replay(tmp_path).session(unpinned, ToolBudget(8, 45), allowed=NEWS_TOOLS)
    assert (await fresh.call("fetch_source", {"item_id": "rss:1"})).status == "not_available"
    with pytest.raises(ValueError, match="no stored forecast"):
        replay_context(AgentName.TECHNICAL, reloaded, round_=1)


def test_a_replayed_round_never_opens_a_capture_first_made_in_a_later_round() -> None:
    def ref(minutes: int) -> LakeRef:
        return LakeRef(
            source="news",
            route=ROUTE,
            key="rss:1",
            fetched_at=AS_OF + timedelta(minutes=minutes),
            http_status=200,
            body_sha256=f"{minutes:064x}",
        )

    round1, round2 = ref(1), ref(7)
    card = [
        card_forecast(AgentName.NEWS, 2, (round1, round2)),
        card_forecast(AgentName.NEWS, 1, (round1,)),
        card_forecast(AgentName.MACRO, 3, (ref(9),)),
    ]
    assert replay_context(AgentName.NEWS, card, round_=1).pinned == (round1,)
    assert replay_context(AgentName.NEWS, card, round_=2).pinned == (round1, round2)
    with pytest.raises(ValueError, match="round 3"):
        replay_context(AgentName.NEWS, card, round_=3)


def test_capture_manifest_lists_unique_records_fetched_after_as_of() -> None:
    ref = LakeRef(
        source="news",
        route=ROUTE,
        key="rss:1",
        fetched_at=AS_OF + timedelta(seconds=1),
        http_status=200,
        body_sha256="a" * 64,
    )
    assert card_forecast(AgentName.NEWS, 1, (ref,)).capture_manifest == (ref,)
    with pytest.raises(ValueError, match="unique"):
        card_forecast(AgentName.NEWS, 1, (ref, ref))
    with pytest.raises(ValueError, match="after as_of"):
        card_forecast(AgentName.NEWS, 1, (ref.model_copy(update={"fetched_at": AS_OF}),))


async def test_live_fetch_refuses_redirects_that_add_query_parameters(tmp_path: Path) -> None:
    pit = PitQuery(tmp_path / "staging", tmp_path / "lake")
    store = RawStore(tmp_path / "staging", tmp_path / "lake")
    fetcher = Fetcher(chain=("https://www.coindesk.com/b?id=1&utm_source=x",))
    live = build_live_registry(
        pit=pit,
        raw_store=store,
        stale_s=120,
        news_sources=static_config().news_sources,
        news=News(),
        fetcher=fetcher,
    )
    with use_clock(ManualClock(AS_OF + timedelta(seconds=20))):
        result = await live.session(ctx(AgentName.NEWS, "live"), ToolBudget(8, 45), allowed=NEWS_TOOLS).call(
            "fetch_source", {"item_id": "rss:1"}
        )
    assert result.status == "error"
    assert "utm_source" in (result.message or "")
    assert (
        pit.series(
            "news", ROUTE, AS_OF, AS_OF + timedelta(hours=1), as_of=AS_OF + timedelta(hours=1), key="rss:1"
        )
        == []
    )


def test_every_agent_has_skills_selected_by_event_type() -> None:
    loader = SkillLoader()
    for agent in AgentName:
        skills = loader.load(agent, CandidateSource.HELD)
        assert 2 <= len(loader.all_skills(agent)) <= 3
        assert skills.skills
        assert len(skills.skill_commit) == 40
    ltx_news = loader.load(AgentName.NEWS, CandidateSource.LTX).names()
    assert "classify_circular_news" not in ltx_news
    assert "detect_exploit" in ltx_news


def test_skill_commit_is_the_git_tree_id(tmp_path: Path) -> None:
    skills = tmp_path / "news"
    skills.mkdir()
    (skills / "README.md").write_bytes(b"# index\n")
    (skills / "a_skill.md").write_bytes(b"---\nname: a_skill\ntitle: A\nevent_types: [ALL]\n---\nDo a.\n")
    (skills / "sub").mkdir()
    (skills / "sub" / "b.md").write_bytes(b"b\n")
    git_bin = shutil.which("git")
    if git_bin is None:
        pytest.skip("git is not installed")
    git = [git_bin, "-c", "user.name=t", "-c", "user.email=t@t", "-c", "core.autocrlf=false"]
    for args in (["init", "-q"], ["add", "."], ["commit", "-qm", "skills"]):
        subprocess.run([*git, *args], cwd=tmp_path, check=True)  # noqa: S603
    expected = subprocess.run(  # noqa: S603
        [git_bin, "rev-parse", "HEAD:news"], cwd=tmp_path, check=True, capture_output=True, text=True
    ).stdout.strip()
    assert git_tree_id(skills) == expected
    before = SkillLoader(tmp_path).commit(AgentName.NEWS)
    (skills / "a_skill.md").write_bytes(b"---\nname: a_skill\ntitle: A\nevent_types: [ALL]\n---\nDo b.\n")
    assert SkillLoader(tmp_path).commit(AgentName.NEWS) != before


def test_malformed_skills_are_refused(tmp_path: Path) -> None:
    (tmp_path / "macro").mkdir()
    (tmp_path / "macro" / "x.md").write_bytes(b"---\nname: y\ntitle: X\nevent_types: [LTX]\n---\nbody\n")
    with pytest.raises(SkillError):
        SkillLoader(tmp_path).load(AgentName.MACRO, CandidateSource.LTX)
    (tmp_path / "macro" / "x.md").write_bytes(b"---\nname: x\ntitle: X\nevent_types: [NOPE]\n---\nbody\n")
    with pytest.raises(SkillError):
        SkillLoader(tmp_path).load(AgentName.MACRO, CandidateSource.LTX)
