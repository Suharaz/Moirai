"""`LlmAgentRunner` with a fake LLM and fake tool backends (phase 05, test-strategy 3.4)."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx2
import pytest
from langgraph.store.memory import InMemoryStore
from llm_fakes import MODEL, MemoryLlmStore, config, deepseek_config, flat_deepseek_prices
from pydantic import BaseModel

from hdt.agents.llm_router import LlmRouter
from hdt.agents.llm_types import (
    Generation,
    LlmCacheMissError,
    LlmOutputError,
    ToolCallRequest,
    ToolStep,
)
from hdt.agents.model_test import CLOSED_REASON, RoleHealth
from hdt.agents.prompting import template_hash
from hdt.agents.runner import LlmAgentRunner
from hdt.agents.validate import logit, sigmoid
from hdt.agents.versions import VersionKey
from hdt.contracts.candidate import CandidateSet, LevelCandidate
from hdt.contracts.common import AgentName, CandidateSource, DataQualityFlag, Side, TargetType
from hdt.contracts.forecast import AgentForecastDraft, ClaimDraft, LakeRef, LlmUsage
from hdt.contracts.packet import QuantPacket
from hdt.core.config import static_config
from hdt.core.ids import canonical_sha256
from hdt.council.ports import AgentRequest
from hdt.lake.pit_query import PitQuery
from hdt.memory.store import MemoryStore
from hdt.settings.schemas import ModelsSection, RoleModelConfig, Section, seed_payloads
from hdt.settings.versions import ConfigPin, ConfigVersion
from hdt.tools.base import result_id
from hdt.tools.budget import ABSTAIN_REASON
from hdt.tools.impl.quant_core import QuantCoreData
from hdt.tools.ports import Announcement, NewsAssessment, NewsEvidence, NewsItem
from hdt.tools.replay import build_replay_registry
from hdt.vault.owner import ModelTestOutcome

AS_OF = datetime(2026, 9, 1, 12, tzinfo=UTC)
COIN = 42
LONG_ID = "lc_AAAAAAAAAAAAAAAA"
SHORT_ID = "lc_BBBBBBBBBBBBBBBB"
CANDIDATES = CandidateSet(
    coin_id=COIN,
    as_of=AS_OF,
    levels_ver="lv1",
    candidates=(
        LevelCandidate(
            candidate_id=LONG_ID,
            side=Side.LONG,
            entry=Decimal(10),
            invalidation=Decimal(9),
            tp1=Decimal(12),
            rr=2.0,
            tick=Decimal(1),
        ),
        LevelCandidate(
            candidate_id=SHORT_ID,
            side=Side.SHORT,
            entry=Decimal(10),
            invalidation=Decimal(11),
            tp1=Decimal(8),
            rr=2.0,
            tick=Decimal(1),
        ),
    ),
)
A_I = 0.8125


def make_pin(role: RoleModelConfig | None = None) -> ConfigPin:
    static = static_config()
    council = seed_payloads(static)[Section.COUNCIL]
    models = ModelsSection(roles={agent: role or config() for agent in static.council.agents})

    def version(vid: int, section: Section, payload: BaseModel) -> ConfigVersion:
        return ConfigVersion(
            id=vid,
            section=section,
            payload=payload.model_dump(mode="json"),
            payload_sha256="0" * 64,
            schema_version=1,
            author="test",
            reason="test",
            created_at=AS_OF,
            parent_id=None,
        )

    return ConfigPin(
        {
            Section.MODELS: version(1, Section.MODELS, models),
            Section.COUNCIL: version(2, Section.COUNCIL, council),
        }
    )


PIN = make_pin()


def packet(
    agent: AgentName, coin_id: int, as_of: datetime, *, stale: tuple[str, ...] | None = None
) -> QuantPacket:
    return QuantPacket.build(
        agent=agent,
        coin_id=coin_id,
        as_of=as_of,
        features={"1h_rsi14_last": 71.5, "4h_adx14_last": 31.0, "1h_structure": "HH"},
        p_model=0.5,
        candidate_set_sha256=CANDIDATES.candidate_set_sha256,
        data_quality={} if stale is None else {DataQualityFlag.STALE: stale},
        universe_date=as_of.date(),
        config_version_ids={},
        feature_ver="f1",
        p_model_ver="p1",
        target_type=TargetType.RAW_12H,
        label_spec_version="l1",
    )


class Versions:
    def __init__(self) -> None:
        self.keys: list[VersionKey] = []

    def version(self, key: VersionKey, *, config_version_id: int | None) -> int:
        self.keys.append(key)
        return 3


@dataclass
class FakeLlm:
    """Scripted tool steps and structured replies; records every prompt it was shown."""

    drafts: list[AgentForecastDraft | Exception]
    steps: list[tuple[ToolCallRequest, ...]] = field(default_factory=list)
    mode: str = "live"
    prompts: list[str] = field(default_factory=list)

    async def tool_step(
        self,
        *,
        role: Any,
        pipeline: Any,
        event_id: str | None,
        config: Any,
        messages: Sequence[Mapping[str, Any]],
        tools: Sequence[Mapping[str, Any]],
    ) -> ToolStep:
        self.prompts.extend(json.dumps(m) for m in messages)
        calls = self.steps.pop(0) if self.steps else ()
        return ToolStep(
            content=None if calls else "done",
            tool_calls=calls,
            model_slug=config.model,
            model_returned=config.model,
            provider="Anthropic",
            generation_id=f"gen-step-{len(self.prompts)}",
            usage=LlmUsage(prompt_tokens=10, completion_tokens=2, cost_usd=0.001),
            prompt_hash="1" * 64,
            params_hash="2" * 64,
            cached=False,
        )

    async def structured[T: BaseModel](
        self,
        *,
        role: Any,
        pipeline: Any,
        event_id: str | None,
        config: Any,
        system: str,
        user: str,
        schema: type[T],
    ) -> Generation[T]:
        self.prompts.extend((system, user))
        draft = self.drafts.pop(0)
        if isinstance(draft, Exception):
            raise draft
        assert isinstance(draft, schema)
        return Generation(
            output=draft,
            model_slug=config.model,
            model_returned=MODEL + "-20260101",
            provider="Anthropic",
            generation_id="gen-final",
            usage=LlmUsage(prompt_tokens=100, completion_tokens=20, cost_usd=0.01),
            prompt_hash="a" * 64,
            params_hash="b" * 64,
            cached=False,
        )


class Assessor:
    def __init__(self, mode: str = "fade_hype") -> None:
        self.mode = mode

    def assess(self, coin_id: int, as_of: datetime) -> NewsAssessment:
        return NewsAssessment(
            coin_id=coin_id,
            as_of=as_of,
            p_model=0.4 if self.mode == "fade_hype" else 0.5,
            mode=self.mode,
            abstain_reason="judges disagree" if self.mode == "abstain" else None,
            evidence=(
                NewsEvidence(
                    item_id="rss:77",
                    event_key="aaa-listing",
                    event_class="LISTING",
                    direction="up",
                    tier="T2",
                    official=False,
                    hardness=0.2,
                    novelty=0.1,
                    quote="AAA may list soon",
                    known_at=as_of,
                ),
            ),
            rule_version="news-r1",
        )


class NoNews:
    """News index and official source with nothing recorded."""

    def items(self, coin_id: int, since: datetime, as_of: datetime, limit: int) -> Sequence[NewsItem]:
        return []

    def item(self, item_id: str, as_of: datetime) -> NewsItem | None:
        return None

    def announcements(self, exchange: str, since: datetime, as_of: datetime) -> Sequence[Announcement]:
        return []


def build(
    tmp_path: Path,
    llm: FakeLlm | LlmRouter,
    *,
    stale: tuple[str, ...] | None = None,
    news: Assessor | None = None,
    health: RoleHealth | None = None,
    news_index: Any = None,
    pin: ConfigPin = PIN,
) -> tuple[LlmAgentRunner, Versions]:
    memory = MemoryStore(InMemoryStore())
    registry = build_replay_registry(
        pit=PitQuery(tmp_path / "staging", tmp_path / "lake"),
        stale_s=120,
        news_sources=static_config().news_sources,
        quant_core=lambda agent, coin_id, as_of: packet(agent, coin_id, as_of, stale=stale),
        news=news_index if news_index is not None else NoNews(),
        official=NoNews(),
        memory=memory,
    )
    versions = Versions()
    runner = LlmAgentRunner(
        llm=llm,
        registry=registry,
        pins=lambda ids: pin,
        memory=memory,
        versions=versions,
        health=health,
        news=news,
    )
    return runner, versions


def request(agent: AgentName = AgentName.TECHNICAL, *, round_: int = 1, **extra: Any) -> AgentRequest:
    base: dict[str, Any] = {
        "event_id": "evt-1",
        "agent": agent,
        "round": round_,
        "coin_id": COIN,
        "as_of": AS_OF,
        "source": CandidateSource.LTX,
        "target_type": TargetType.RAW_12H,
        "label_spec_version": "l1",
        "config_version_ids": PIN.version_ids(),
        "universe_date": AS_OF.date(),
        "candidate_set": CANDIDATES,
        "a_i": A_I,
        "mode": "replay",
    }
    return AgentRequest(**{**base, **extra})


def draft(**fields: Any) -> AgentForecastDraft:
    return AgentForecastDraft.model_validate({"abstain": False, **fields})


async def test_forecast_is_clipped_validated_and_carries_its_provenance(tmp_path: Path) -> None:
    llm = FakeLlm(drafts=[])
    runner, versions = build(tmp_path, llm)
    p = packet(AgentName.TECHNICAL, COIN, AS_OF)
    payload = QuantCoreData(packet_sha256=p.packet_sha256, packet=p.model_dump(mode="json"))
    quant_result_id = result_id("quant_core", canonical_sha256(payload.model_dump(mode="json")))

    llm.drafts.append(
        draft(
            p_llm=0.8,
            candidate_id=LONG_ID,
            claims=[
                ClaimDraft(
                    claim_id="c1",
                    kind="packet",
                    ref="features.1h_rsi14_last",
                    statement="rsi hot",
                    value=71.5,
                ),
                ClaimDraft(claim_id="c2", kind="tool", ref=quant_result_id, statement="packet read"),
                ClaimDraft(claim_id="c3", kind="packet", ref="funding_rate", statement="made up field"),
                ClaimDraft(claim_id="c4", kind="url", ref="rss:1", statement="not seen"),
            ],
            cited_claim_ids=["c1", "c3", "c2"],
            reason="trend intact",
        )
    )
    result = await runner.forecast(request())
    f = result.forecast

    assert not f.abstain
    assert f.p_model == 0.5
    assert f.p_llm == 0.8
    assert f.p_used == pytest.approx(sigmoid(A_I * 0.5))  # logit(0.8) = 1.386 clipped to +0.5
    assert f.candidate_id == LONG_ID
    assert [(c.claim_id, c.ref) for c in f.claims] == [("c1", "1h_rsi14_last"), ("c2", quant_result_id)]
    assert f.cited_claim_ids == ("c1", "c2")
    assert all(not c.verified and not c.hard and c.tier is None for c in f.claims)
    assert f.packet_sha256 == packet(AgentName.TECHNICAL, COIN, AS_OF).packet_sha256
    assert f.prompt_hash == "a" * 64
    assert f.model_slug == MODEL
    assert f.model_returned == MODEL + "-20260101"
    assert f.provider == "Anthropic"
    assert f.generation_id == "gen-final"
    assert f.skill_commit is not None
    assert len(f.skill_commit) == 40
    assert f.agent_version == "3"
    assert versions.keys[-1].model_slug == MODEL
    assert [t.tool for t in f.tool_calls] == ["quant_core"]
    assert f.usage is not None
    assert f.usage.prompt_tokens == 110  # the tool step + the final call
    assert all(str(A_I) not in prompt for prompt in llm.prompts)  # the agent never sees its a_i


async def test_candidate_on_the_wrong_side_or_outside_the_set_is_dropped(tmp_path: Path) -> None:
    llm = FakeLlm(
        drafts=[
            draft(p_llm=0.62, candidate_id=SHORT_ID),
            draft(p_llm=0.62, candidate_id="lc_CCCCCCCCCCCCCCCC"),
        ]
    )
    runner, _ = build(tmp_path, llm)
    wrong_side = (await runner.forecast(request(event_id="e1"))).forecast
    unknown = (await runner.forecast(request(event_id="e2"))).forecast
    assert wrong_side.p_used is not None
    assert wrong_side.p_used > 0.55
    assert wrong_side.candidate_id is None
    assert unknown.candidate_id is None


async def test_stale_main_feature_forces_abstain_without_calling_the_llm(tmp_path: Path) -> None:
    llm = FakeLlm(drafts=[draft(p_llm=0.9)])
    runner, _ = build(tmp_path, llm, stale=("1h_rsi14_last",))
    f = (await runner.forecast(request())).forecast
    assert f.abstain
    assert f.abstain_reason == "data_quality_stale"
    assert f.p_used is None
    assert f.candidate_id is None
    assert llm.prompts == []


async def test_stale_flag_on_a_field_outside_the_model_does_not_force_abstain(tmp_path: Path) -> None:
    runner, _ = build(tmp_path, FakeLlm(drafts=[draft(p_llm=0.5)]), stale=("not_a_model_input",))
    assert not (await runner.forecast(request())).forecast.abstain


async def test_two_parse_failures_abstain(tmp_path: Path) -> None:
    runner, _ = build(tmp_path, FakeLlm(drafts=[LlmOutputError("twice")]))
    f = (await runner.forecast(request())).forecast
    assert f.abstain
    assert f.abstain_reason == "llm_output_invalid"
    assert f.p_model == 0.5


async def test_replay_cache_miss_is_raised_not_abstained(tmp_path: Path) -> None:
    runner, _ = build(tmp_path, FakeLlm(drafts=[LlmCacheMissError("miss")]))
    with pytest.raises(LlmCacheMissError):
        await runner.forecast(request())


async def test_tool_budget_overrun_forces_abstain(tmp_path: Path) -> None:
    spam = tuple(ToolCallRequest(f"call_{i}", "quant_core", "{}") for i in range(9))
    llm = FakeLlm(drafts=[draft(p_llm=0.7)], steps=[spam])
    runner, _ = build(tmp_path, llm)
    f = (await runner.forecast(request())).forecast
    assert f.abstain
    assert f.abstain_reason == ABSTAIN_REASON
    assert len(f.tool_calls) == 10
    assert f.tool_calls[-1].status == "budget_exceeded"


async def test_news_agent_uses_the_assessment_and_never_sees_or_names_candidates(tmp_path: Path) -> None:
    llm = FakeLlm(
        drafts=[
            draft(
                p_llm=0.3,
                candidate_id=SHORT_ID,
                claims=[
                    ClaimDraft(
                        claim_id="n1",
                        kind="url",
                        ref="rss:77",
                        statement="recycled",
                        quote="AAA may list soon",
                    )
                ],
                cited_claim_ids=["n1"],
            )
        ]
    )
    runner, _ = build(tmp_path, llm, news=Assessor())
    f = (await runner.forecast(request(AgentName.NEWS))).forecast
    assert f.p_model == 0.4
    assert f.p_used == pytest.approx(sigmoid(logit(0.4) + A_I * (logit(0.3) - logit(0.4))))  # within bound
    assert f.candidate_id is None
    assert f.packet_sha256 is None
    assert [c.claim_id for c in f.claims] == ["n1"]
    assert all(LONG_ID not in p and SHORT_ID not in p and "entry" not in p for p in llm.prompts)


async def test_news_mode_abstain_forces_abstain(tmp_path: Path) -> None:
    llm = FakeLlm(drafts=[draft(p_llm=0.6)])
    runner, _ = build(tmp_path, llm, news=Assessor("abstain"))
    f = (await runner.forecast(request(AgentName.NEWS))).forecast
    assert f.abstain
    assert f.abstain_reason is not None
    assert f.abstain_reason.startswith("news_mode_abstain")
    assert llm.prompts == []


async def test_revision_round_uses_the_total_bound_and_cites_shared_claims(tmp_path: Path) -> None:
    from hdt.contracts.forecast import Claim

    shared = Claim(claim_id="s_x1", kind="packet", ref="cvd_1h_oi", statement="sellers absorb")
    llm = FakeLlm(drafts=[draft(p_llm=0.6), draft(p_llm=0.9, cited_claim_ids=["s_x1", "s_unknown"])])
    runner, _ = build(tmp_path, llm)
    first = (await runner.forecast(request())).forecast
    revised = (await runner.forecast(request(round_=2, previous=first, shared_claims=(shared,)))).forecast
    assert revised.p_used == pytest.approx(sigmoid(A_I * 1.0))  # logit(0.9) = 2.197 clipped to +1.0
    assert revised.cited_claim_ids == ("s_x1",)
    assert revised.tool_calls == ()  # the packet was read once for the whole meeting
    assert revised.packet_sha256 == first.packet_sha256


async def test_role_closed_by_its_self_test_abstains(tmp_path: Path) -> None:
    async def failing(llm: Any, role: Any, cfg: Any) -> ModelTestOutcome:
        return ModelTestOutcome(False, 10, None, None, None, "schema broken")

    llm = FakeLlm(drafts=[draft(p_llm=0.7)])
    health = RoleHealth(llm, None, tester=failing)  # type: ignore[arg-type]
    runner, _ = build(tmp_path, llm, health=health)
    f = (await runner.forecast(request())).forecast
    assert f.abstain
    assert f.abstain_reason == CLOSED_REASON
    assert f.model_slug == MODEL
    assert f.prompt_hash == template_hash(f.agent)  # every output carries a prompt hash, abstains too


async def test_a_resumed_meeting_rebuilds_the_session_from_the_earlier_rounds(tmp_path: Path) -> None:
    capture = LakeRef(
        source="news",
        route="fetch_source",
        key="rss:77",
        fetched_at=AS_OF.replace(minute=5),  # a meeting capture is always recorded after as_of
        http_status=200,
        body_sha256="e" * 64,
    )
    before, _ = build(tmp_path, FakeLlm(drafts=[draft(p_llm=0.6)]))
    played = await before.forecast(request())
    assert [c.tool for c in played.forecast.tool_calls] == ["quant_core"]
    earlier = replace(
        played,
        forecast=played.forecast.model_copy(update={"capture_manifest": (capture,)}),
        captures=(capture,),
    )

    # A new process (another runner) resumes the meeting at round 2.
    llm = FakeLlm(drafts=[draft(p_llm=0.6)])
    resumed, _ = build(tmp_path, llm)
    revised = (
        await resumed.forecast(request(round_=2, previous=played.forecast, earlier=(earlier,)))
    ).forecast
    assert revised.tool_calls == ()  # quant_core is not called again
    assert revised.packet_sha256 == played.forecast.packet_sha256
    assert revised.capture_manifest == (capture,)
    assert any("at most 7 more calls" in p for p in llm.prompts)  # 1 of the 8 calls was used in round 1


async def test_a_resumed_meeting_that_used_its_whole_budget_calls_no_more_tools(tmp_path: Path) -> None:
    before, _ = build(tmp_path, FakeLlm(drafts=[draft(p_llm=0.6)]))
    played = await before.forecast(request())
    used = played.forecast.tool_calls * 8
    earlier = replace(played, forecast=played.forecast.model_copy(update={"tool_calls": used}))
    spam = (ToolCallRequest("call_1", "quant_core", "{}"),)
    llm = FakeLlm(drafts=[draft(p_llm=0.6)], steps=[spam])
    resumed, _ = build(tmp_path, llm)
    revised = (
        await resumed.forecast(request(round_=2, previous=played.forecast, earlier=(earlier,)))
    ).forecast
    assert not revised.abstain
    assert revised.tool_calls == ()
    assert llm.steps == [spam]  # no tool step was offered


async def test_a_resumed_round_prompts_exactly_like_the_uninterrupted_one_after_a_tool_error(
    tmp_path: Path,
) -> None:
    """N1: round 1 got an error tool result; a resumed round 2 rebuilt only the `ok` results and prompted
    "none" where the uninterrupted meeting showed the error, so it asked the model something else."""
    from hdt.council.graph import _earlier_result, _result_json

    denied = (ToolCallRequest("call_1", "get_snapshot", "{}"),)
    whole = FakeLlm(drafts=[draft(p_llm=0.6), draft(p_llm=0.6)], steps=[denied, (), ()])
    runner, _ = build(tmp_path, whole)
    first_request = request()
    first = await runner.forecast(first_request)
    assert [c.tool for c in first.forecast.tool_calls if c.status != "ok"] == ["get_snapshot"]
    round_one_prompts = len(whole.prompts)
    second_request = request(round_=2, previous=first.forecast)
    await runner.forecast(second_request)
    uninterrupted = whole.prompts[round_one_prompts:]
    assert any('"status":"error"' in p or "not available" in p for p in uninterrupted)

    stored = _earlier_result(json.loads(json.dumps(_result_json(first, first_request))))
    resumed_llm = FakeLlm(drafts=[draft(p_llm=0.6)], steps=[()])
    resumed, _ = build(tmp_path, resumed_llm)
    await resumed.forecast(replace(second_request, earlier=(stored,)))
    assert resumed_llm.prompts == uninterrupted


async def test_a_truncated_tool_step_abstains_as_an_output_error(tmp_path: Path) -> None:
    """m8: an unusable tool step was labelled `llm_unavailable` and missed the replay parse-error rate."""

    class Truncated(FakeLlm):
        async def tool_step(self, **kwargs: Any) -> ToolStep:
            raise LlmOutputError("the tool step was cut at max_tokens")

    runner, _ = build(tmp_path, Truncated(drafts=[]))
    forecast = (await runner.forecast(request())).forecast
    assert forecast.abstain
    assert forecast.abstain_reason == "llm_output_invalid"


class OneNewsItem(NoNews):
    def item(self, item_id: str, as_of: datetime) -> NewsItem | None:
        return NewsItem(
            item_id="rss:77",
            coin_ids=(COIN,),
            title="AAA may list soon",
            url="https://example.com/aaa",
            source_name="example",
            ingested_at=AS_OF,
        )


async def test_each_replay_round_opens_the_captures_pinned_for_that_round(tmp_path: Path) -> None:
    capture = LakeRef(
        source="news",
        route="fetch_source",
        key="rss:77",
        fetched_at=AS_OF.replace(minute=5),
        http_status=200,
        body_sha256="e" * 64,
    )
    fetch = (ToolCallRequest("call_1", "fetch_source", json.dumps({"item_id": "rss:77"})),)
    llm = FakeLlm(drafts=[draft(p_llm=0.4), draft(p_llm=0.4)], steps=[fetch, (), fetch, ()])
    runner, _ = build(tmp_path, llm, news=Assessor(), news_index=OneNewsItem())
    first = (await runner.forecast(request(AgentName.NEWS))).forecast
    assert [c.status for c in first.tool_calls] == ["not_available"]  # nothing pinned in round 1
    second = (
        await runner.forecast(request(AgentName.NEWS, round_=2, previous=first, pinned=(capture,)))
    ).forecast
    # Round 2 pins the capture: the replay tool now looks it up (and finds the lake copy missing).
    assert [c.status for c in second.tool_calls] == ["error"]


async def test_a_deepseek_thinking_agent_runs_its_tool_loop_and_forecasts_through_the_router(
    tmp_path: Path,
) -> None:
    """The whole agent call on the deepseek gateway: each tool step's chain of thought goes back to
    DeepSeek with its tool calls (DeepSeek answers 400 otherwise), and the forecast records provider
    `deepseek` with the cost priced from the DeepSeek table."""
    sent: list[dict[str, Any]] = []
    tool_call = {"id": "call_1", "type": "function", "function": {"name": "quant_core", "arguments": "{}"}}
    replies: list[dict[str, Any]] = [
        {"content": "", "reasoning_content": "read the packet", "tool_calls": [tool_call]},
        {"content": "done", "reasoning_content": "enough evidence"},
        {"content": json.dumps({"abstain": False, "p_llm": 0.6}), "reasoning_content": "final"},
    ]

    def deepseek(http_request: httpx2.Request) -> httpx2.Response:
        assert http_request.url.host == "api.deepseek.com"
        sent.append(json.loads(http_request.content))
        message = replies.pop(0)
        body = {
            "id": f"ds-{len(sent)}",
            "object": "chat.completion",
            "created": 1,
            "model": "deepseek-flash",
            "choices": [
                {
                    "index": 0,
                    "finish_reason": "tool_calls" if "tool_calls" in message else "stop",
                    "message": {"role": "assistant", **message},
                }
            ],
            "usage": {
                "prompt_tokens": 1000,
                "completion_tokens": 100,
                "total_tokens": 1100,
                "prompt_cache_hit_tokens": 400,
                "prompt_cache_miss_tokens": 600,
            },
        }
        return httpx2.Response(200, json=body)

    router = LlmRouter(
        mode="live",
        api_key=None,
        deepseek_api_key=lambda: "sk-ds-test",
        http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(deepseek)),
        deepseek_prices=flat_deepseek_prices(hit=0.01, miss=0.2, output=1.0),
        store=MemoryLlmStore(),
    )
    role = deepseek_config(thinking="high", max_tokens=8000, timeout_s=120)
    runner, _ = build(tmp_path, router, pin=make_pin(role))
    f = (await runner.forecast(request())).forecast

    assert not f.abstain
    assert f.p_llm == 0.6
    assert [t.tool for t in f.tool_calls] == ["quant_core", "quant_core"]  # the opening read + the call
    assert len(sent) == 3  # two tool steps and the final structured call
    assistant = next(m for m in sent[1]["messages"] if m["role"] == "assistant")
    assert assistant["reasoning_content"] == "read the packet"
    assert [c["id"] for c in assistant["tool_calls"]] == ["call_1"]
    assert sent[2]["response_format"] == {"type": "json_object"}
    assert (f.model_slug, f.model_returned, f.provider) == ("deepseek-flash", "deepseek-flash", "deepseek")
    assert f.usage is not None
    assert f.usage.cost_usd == pytest.approx(3 * (400 * 0.01 + 600 * 0.2 + 100 * 1.0) / 1_000_000)
