"""Replay registry: a full recorded day replays without opening a socket, and a replayed event yields
byte-identical prompts whether it is replayed on day 4 or on day 13 (later recordings do not leak)."""

from __future__ import annotations

import json
import socket
import subprocess
import sys
from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from langgraph.store.memory import InMemoryStore

from hdt.agents.skills.loader import SkillLoader
from hdt.contracts.common import AgentName, CandidateSource, TargetType, Tier
from hdt.contracts.packet import QuantPacket
from hdt.core.clock import ManualClock, use_clock
from hdt.core.config import static_config
from hdt.core.ids import canonical_json
from hdt.lake.pit_query import PitQuery
from hdt.lake.raw_store import RawStore
from hdt.lake.schemas import Capture
from hdt.lake.universe import Universe, UniverseMember, universe_capture
from hdt.memory.episodic import DecisionEpisode, EpisodicWriter, KnownEvent, OutcomeRecord
from hdt.memory.lessons import AbResult, LessonLog, LessonTemplate
from hdt.memory.recall import AgentMemory
from hdt.memory.store import MemoryStore
from hdt.tools.base import ToolContext
from hdt.tools.budget import ToolBudget
from hdt.tools.ports import Announcement, NewsItem, UnlockEvent
from hdt.tools.registry import AGENT_TOOLS, ToolRegistry
from hdt.tools.replay import build_replay_registry

DAY = datetime(2026, 3, 2, tzinfo=UTC)
COIN = 5994
SYMBOL = "AAAUSDT"
ITEM = "rss:aaa-listing-1"
ITEM_URL = "https://www.coindesk.com/markets/aaa-listing?id=7"
TOKEN = "0x1111111111111111111111111111111111111111"
BUDGET = ToolBudget(max_calls=8, max_seconds=45.0)
ARGS: dict[str, dict[str, Any]] = {
    "fetch_source": {"item_id": ITEM},
    "check_official": {"exchange": "binance", "keyword": "AAA"},
}


class Lake:
    def __init__(self, root: Path) -> None:
        self.store = RawStore(root / "staging", root / "lake")
        self.pit = PitQuery(root / "staging", root / "lake")

    def add(self, source: Any, route: str, at: datetime, body: Any, *, key: str = "", **params: Any) -> None:
        raw = body if isinstance(body, bytes) else json.dumps(body).encode()
        capture = Capture(source, route, at, 200, raw, params=params or None, key=key)
        self.store.append(capture)


def cmc(data: Any) -> dict[str, Any]:
    return {"status": {"error_code": 0, "error_message": None}, "data": data}


def record_hour(lake: Lake, at: datetime, mark: float) -> None:
    lake.add(
        "binance",
        "premium_index",
        at,
        [
            {
                "symbol": SYMBOL,
                "markPrice": str(mark),
                "indexPrice": str(mark * 0.999),
                "lastFundingRate": "0.0001",
                "nextFundingTime": int((at + timedelta(hours=2)).timestamp() * 1000),
                "time": int(at.timestamp() * 1000),
            }
        ],
    )
    lake.add("binance", "open_interest", at, {"symbol": SYMBOL, "openInterest": "1000000"}, key=SYMBOL)
    quote = {
        "long_liquidations_1h": 5e5,
        "short_liquidations_1h": 1e5,
        "total_liquidations_1h": 6e5,
        "long_liquidations_4h": 2e6,
        "short_liquidations_4h": 3e5,
        "total_liquidations_4h": 2.3e6,
        "long_liquidations_24h": 4e6,
        "short_liquidations_24h": 1e6,
        "total_liquidations_24h": 5e6,
    }
    page1 = {"cryptocurrencies": [{"crypto_id": 1, "quotes": [quote]}], "has_more": True}
    page2 = {"cryptocurrencies": [{"crypto_id": COIN, "quotes": [quote]}], "has_more": False}
    lake.add("cmc", "liquidations_by_crypto", at, cmc(page1))
    lake.add("cmc", "liquidations_by_crypto", at + timedelta(seconds=2), cmc(page2), key="p2")


def build_day(lake: Lake) -> None:
    member = UniverseMember(
        cmc_id=COIN, cmc_symbol="AAA", binance_symbol=SYMBOL, multiplier=1, cmc_rank=40, open_interest_usd=1e8
    )
    universe = Universe(
        date=DAY.date(),
        built_at=DAY + timedelta(minutes=1),
        members=(member,),
        ltx_size=1,
        watchlist_size=1,
        sources={},
    )
    lake.store.append(universe_capture(universe))
    lake.add(
        "binance", "funding_info", DAY + timedelta(minutes=2), [{"symbol": SYMBOL, "fundingIntervalHours": 4}]
    )
    security = [
        {
            "platformName": "ethereum",
            "securityLevel": 2,
            "extra": {"buyTax": "0", "sellTax": "0.05", "isVerified": True},
            "evmDisplay": {"honeypotStatus": "clean", "mintableStatus": "risky"},
            "securityItems": [{"code": "mint", "riskyLevel": "high", "isHit": True, "des": "Owner can mint"}],
            "tags": ["token"],
        }
    ]
    params = {"platformName": "ethereum", "address": TOKEN}
    lake.add(
        "cmc", "dex_security_detail", DAY + timedelta(minutes=10), cmc(security), key=str(COIN), **params
    )
    changes = {
        "lcs": [{"ts": int((DAY + timedelta(minutes=5)).timestamp() * 1000), "tp": "remove", "tu": 25000.0}]
    }
    lake.add(
        "cmc",
        "dex_liquidity_change",
        DAY + timedelta(minutes=10),
        cmc(changes),
        key=str(COIN),
        platform="ethereum",
        address=TOKEN,
        limit=100,
    )
    article = b"<html><head><script>x()</script></head><body><p>Binance will list AAA.</p></body></html>"
    lake.add(
        "news",
        "fetch_source",
        DAY + timedelta(minutes=20),
        article,
        key=ITEM,
        item_id=ITEM,
        event_id="evt-early",
        url=ITEM_URL,
        final_url=ITEM_URL,
        redirect_chain=[],
        content_type="text/html; charset=utf-8",
    )
    for hour in range(24):
        record_hour(lake, DAY + timedelta(hours=hour, minutes=58), 1.5 + hour / 100)


class News:
    def __init__(self) -> None:
        self._items = [
            NewsItem(
                item_id=ITEM,
                coin_ids=(COIN,),
                title="Binance will list AAA",
                url=ITEM_URL,
                source_name="CoinDesk",
                ingested_at=DAY + timedelta(minutes=15),
            )
        ]

    def items(self, coin_id: int, since: datetime, as_of: datetime, limit: int) -> Sequence[NewsItem]:
        return [i for i in self._items if coin_id in i.coin_ids and since <= i.ingested_at <= as_of][:limit]

    def item(self, item_id: str, as_of: datetime) -> NewsItem | None:
        return next((i for i in self._items if i.item_id == item_id and i.ingested_at <= as_of), None)


class Official:
    def announcements(self, exchange: str, since: datetime, as_of: datetime) -> Sequence[Announcement]:
        found = Announcement(
            exchange="binance",
            title="Binance Will List AAA (AAA)",
            url="https://www.binance.com/en/support/announcement/aaa",
            published_at=DAY + timedelta(minutes=30),
            recorded_at=DAY + timedelta(minutes=31),
        )
        return [found] if found.recorded_at <= as_of and exchange == found.exchange else []


class Unlocks:
    def unlocks(self, coin_id: int, start: datetime, end: datetime, as_of: datetime) -> Sequence[UnlockEvent]:
        event = UnlockEvent(
            coin_id=COIN,
            unlock_at=DAY + timedelta(days=2),
            pct_of_circulating=1.5,
            category="investors",
            source="calendar",
            recorded_at=DAY - timedelta(days=1),
        )
        return [event] if event.recorded_at <= as_of and start <= event.unlock_at <= end else []


def quant_core(agent: AgentName, coin_id: int, as_of: datetime) -> QuantPacket:
    return QuantPacket.build(
        agent=agent,
        coin_id=coin_id,
        as_of=as_of,
        features={"spike": 4.2, "decay": 0.3},
        p_model=0.55,
        candidate_set_sha256="a" * 64,
        data_quality={},
        universe_date=DAY.date(),
        config_version_ids={"risk": 1},
        feature_ver="f1",
        p_model_ver="p1",
        target_type=TargetType.RAW_12H,
        label_spec_version="l1",
    )


def registry(lake: Lake, memory: MemoryStore) -> ToolRegistry:
    return build_replay_registry(
        pit=lake.pit,
        stale_s=120,
        news_sources=static_config().news_sources,
        quant_core=quant_core,
        news=News(),
        official=Official(),
        unlocks=Unlocks(),
        memory=memory,
    )


async def meeting(
    reg: ToolRegistry, memory: MemoryStore, event_id: str, as_of: datetime
) -> dict[str, list[Any]]:
    """What every agent would put into its prompt for this event: tool results, memory, skills."""
    out: dict[str, list[Any]] = {}
    for agent in AgentName:
        session = reg.session(ToolContext("replay", agent, event_id, COIN, as_of), BUDGET)
        results = [await session.call(name, ARGS.get(name, {})) for name in session.tool_names()]
        recall = AgentMemory(memory, agent, as_of).recall(coin_id=COIN)
        skills = SkillLoader().load(agent, CandidateSource.LTX)
        out[agent.value] = [
            [r.prompt_text() for r in results],
            recall.prompt_text(),
            skills.prompt_text(),
            [r.status for r in session.records],
        ]
    return out


@pytest.fixture
def lake(tmp_path: Path) -> Lake:
    built = Lake(tmp_path)
    build_day(built)
    return built


def block_sockets(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Refuse every connection from here on (called inside the test, once the event loop exists)."""
    attempts: list[str] = []

    def refuse(name: str) -> Any:
        def call(*args: Any, **kwargs: Any) -> Any:
            attempts.append(name)
            raise OSError(f"network access during replay: {name}")

        return call

    for name in ("connect", "connect_ex", "sendto"):
        monkeypatch.setattr(socket.socket, name, refuse(f"socket.{name}"))
    monkeypatch.setattr(socket, "create_connection", refuse("create_connection"))
    monkeypatch.setattr(socket, "getaddrinfo", refuse("getaddrinfo"))
    return attempts


async def test_replaying_a_full_day_opens_no_socket(lake: Lake, monkeypatch: pytest.MonkeyPatch) -> None:
    blocked_sockets = block_sockets(monkeypatch)
    memory = MemoryStore(InMemoryStore())
    reg = registry(lake, memory)
    for hour in range(1, 24):
        as_of = DAY + timedelta(hours=hour)
        prompts = await meeting(reg, memory, f"evt-{hour}", as_of)
        for agent, (results, _recall, _skills, statuses) in prompts.items():
            assert len(results) == len(AGENT_TOOLS[AgentName(agent)])
            assert all(status == "ok" for status in statuses), (agent, hour, results)
    assert blocked_sockets == []


async def test_replay_reads_nothing_recorded_after_as_of(lake: Lake, monkeypatch: pytest.MonkeyPatch) -> None:
    blocked_sockets = block_sockets(monkeypatch)
    reg = registry(lake, MemoryStore(InMemoryStore()))
    early = DAY + timedelta(minutes=12)  # before the news item, the announcement and the first market hour
    news = reg.session(ToolContext("replay", AgentName.NEWS, "evt-early", COIN, early), BUDGET)
    assert (await news.call("fetch_source", {"item_id": ITEM})).status == "error"  # not ingested yet
    official = await news.call("check_official", {"exchange": "binance", "keyword": "AAA"})
    assert official.data is not None
    assert official.data["official"] is False
    crowding = reg.session(ToolContext("replay", AgentName.CROWDING, "evt-early", COIN, early), BUDGET)
    snapshot = await crowding.call("get_snapshot", {})
    assert snapshot.data is not None
    assert [v["field"] for v in snapshot.data["values"]] == ["funding_interval_h"]  # recorded at 00:02
    assert snapshot.data["values"][0]["stale"] is False
    assert blocked_sockets == []


def test_replay_module_imports_no_network_client() -> None:
    code = (
        "import sys, hdt.tools.replay, hdt.memory.recall, hdt.agents.skills.loader;"
        "print(sorted(m for m in sys.modules if m.split('.')[0] in ('httpx', 'websockets', 'aiohttp')))"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)  # noqa: S603
    assert out.stdout.strip() == "[]"


def write_memory(memory: MemoryStore, upto: date) -> None:
    """Memory as the system learns it over time; records are written only up to the `upto` day."""
    writer, lessons = EpisodicWriter(memory), LessonLog(memory)
    earlier = DAY - timedelta(days=1)
    for agent in AgentName:
        writer.record_decision(
            DecisionEpisode(
                event_id="evt-prev",
                agent=agent,
                agent_version="v1",
                coin_id=COIN,
                as_of=earlier,
                known_at=earlier + timedelta(seconds=30),
                source=CandidateSource.LTX,
                target_type=TargetType.RAW_12H,
                council_intent=None,
                abstain=False,
                p_used=0.6,
                summary="prior view",
            )
        )
        writer.record_outcome(
            OutcomeRecord.resolved(
                as_of=earlier,
                horizon_h=12,
                event_id="evt-prev",
                agent=agent,
                coin_id=COIN,
                target_type=TargetType.RAW_12H,
                label=1,
                hit=True,
            )
        )
    writer.record_known_event(
        KnownEvent(
            event_key="binance_spot_listing:AAA",
            coin_ids=(COIN,),
            title="Binance will list AAA",
            tier=Tier.T0,
            known_at=DAY + timedelta(minutes=40),
        )
    )
    if upto >= (DAY + timedelta(days=8)).date():
        # Learned after day 4: the replayed event's own decision and outcome, and a lesson approval.
        event_as_of = DAY + timedelta(hours=12)
        writer.record_decision(
            DecisionEpisode(
                event_id="evt-12",
                agent=AgentName.CROWDING,
                agent_version="v1",
                coin_id=COIN,
                as_of=event_as_of,
                known_at=event_as_of + timedelta(seconds=20),
                source=CandidateSource.LTX,
                target_type=TargetType.RAW_12H,
                council_intent=None,
                abstain=True,
                summary="replayed event",
            )
        )
        writer.record_outcome(
            OutcomeRecord.resolved(
                as_of=event_as_of,
                horizon_h=12,
                event_id="evt-12",
                agent=AgentName.CROWDING,
                coin_id=COIN,
                target_type=TargetType.RAW_12H,
                label=0,
            )
        )
        at = DAY + timedelta(days=8)
        with use_clock(ManualClock(at)):
            template = LessonTemplate(
                title="Late cascade",
                when_text="SPIKE above 6",
                observation="Reversal came late",
                adjustment="Wait for DECAY below 0.3",
            )
            lesson = LessonLog(memory, clock=lambda: at).propose(
                AgentName.CROWDING, template, actor="reflection"
            )
            ab = AbResult(n=60, logloss_with=0.6, logloss_without=0.65, ci_low=-0.08, ci_high=-0.01)
            lessons.record_ab(AgentName.CROWDING, lesson.lesson_id, ab, actor="reflection")
            lessons.approve(AgentName.CROWDING, lesson.lesson_id, actor="operator")


async def test_day_4_and_day_13_replays_yield_identical_prompts(
    lake: Lake, monkeypatch: pytest.MonkeyPatch
) -> None:
    blocked_sockets = block_sockets(monkeypatch)
    as_of = DAY + timedelta(hours=12)
    memory = MemoryStore(InMemoryStore())
    write_memory(memory, (DAY + timedelta(days=3)).date())
    with use_clock(ManualClock(DAY + timedelta(days=3))):
        day4 = await meeting(registry(lake, memory), memory, "evt-12", as_of)

    # Nine more days of recording: new market data, a copy of the item fetched by another event right
    # after as_of, the event's own outcome and a lesson approved on day 9.
    for day in range(1, 12):
        record_hour(lake, DAY + timedelta(days=day, hours=1), 3.0)
    lake.add(
        "news",
        "fetch_source",
        as_of + timedelta(minutes=5),
        b"changed",
        key=ITEM,
        item_id=ITEM,
        event_id="evt-other",
    )
    write_memory(memory, (DAY + timedelta(days=12)).date())
    with use_clock(ManualClock(DAY + timedelta(days=12))):
        day13 = await meeting(registry(lake, memory), memory, "evt-12", as_of)

    assert canonical_json(day4) == canonical_json(day13)
    assert '"lessons":[]' in day13["crowding"][1]  # the lesson approved later is not visible
    assert "prior view" in day13["crowding"][1]
    assert blocked_sockets == []
