"""`LlmAgentRunner`: the council `AgentRunner` of the six agents (phase 05).

One call = one agent, one round of one event:

    pinned config -> model self-test gate -> agent version -> tool session (quant_core or news assessment)
    -> forced-abstain checks -> memory recall(as_of) -> bounded ReAct tool loop -> structured forecast
    -> code checks (`hdt.agents.validate`) -> AgentForecast

- The tool session is per (event, agent) and shared by every round (the budget of `council.tool_budget`,
  capped at 8 calls, covers the whole meeting); round-1 A/B runs with a shadow lesson get their own session.
  Call `close_event(event_id)` after the meeting; the oldest sessions are dropped beyond `max_sessions`.
  A process that did not play the earlier rounds (a resumed meeting) rebuilds the session from
  `AgentRequest.earlier`: citable tool results, the packet, captures and the used budget. Every round
  re-binds `ToolContext.pinned` to its request's manifest (replay round N opens the captures of 1..N).
- Forced abstain (never an exception): model not configured or closed by its self-test, quant packet not
  available, a `stale` / `missing_route` flag on a main feature (the agent's p_model inputs) or on the whole
  packet, a news assessment in mode `abstain`, a tool budget overrun, an unreachable model, two replies
  failing the schema. An unexpected error also abstains (`internal_error`, logged). Only a replay cache miss
  (`LlmCacheMissError`) is raised.
- The News agent takes `p_model` from the phase 07 `NewsAssessor` (regime rule, 0.5 when no mode), gets no
  packet and no candidates, and never names a candidate.
- The forecast carries the call's tool records, the session's capture manifest, the final prompt hash, the
  requested and returned model, provider, generation id, the summed usage of the call's generations, the
  skill commit and the agent version (as its decimal `agent_versions.version`).
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections import OrderedDict
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime
from functools import cache
from typing import Any, Final

from sqlalchemy.orm import Session, sessionmaker

from hdt.agents.llm_types import (
    LlmCacheMissError,
    LlmOutputError,
    LlmRoleClosedError,
    LlmUnavailableError,
    ToolChatLlm,
    ToolStep,
)
from hdt.agents.model_test import RoleHealth
from hdt.agents.prompting import PromptInputs, final_user, system_prompt, template_hash, tool_step_user
from hdt.agents.skills.loader import SkillLoader, SkillSet
from hdt.agents.validate import Bounds, Checked, check_draft
from hdt.agents.versions import UNREGISTERED_VERSION, VersionKey, VersionSource
from hdt.contracts.common import AgentName, DataQualityFlag
from hdt.contracts.forecast import AgentForecast, AgentForecastDraft, LakeRef, LlmUsage
from hdt.contracts.packet import QuantPacket
from hdt.core.config import CouncilFile, StaticConfig, load_p_model, static_config
from hdt.council.ports import AgentRequest, AgentResult
from hdt.memory.recall import AgentMemory, LessonText, MemoryRecall
from hdt.memory.store import MemoryStore
from hdt.settings.schemas import RoleModelConfig
from hdt.settings.versions import ConfigPin, load_pin
from hdt.tools.base import ToolContext, ToolResult
from hdt.tools.budget import ABSTAIN_REASON as BUDGET_ABSTAIN_REASON
from hdt.tools.budget import ToolBudget
from hdt.tools.ports import NewsAssessment, NewsAssessor
from hdt.tools.registry import ToolRegistry, ToolSession

log = logging.getLogger(__name__)

MAX_TOOL_CALLS: Final[int] = 8
"""Hard cap of the bounded ReAct loop per meeting (the pinned tool budget can only lower it)."""
MAX_REACT_STEPS: Final[int] = MAX_TOOL_CALLS + 1
MEMORY_EPISODES: Final[int] = 5
BLOCKING_FLAGS: Final[frozenset[DataQualityFlag]] = frozenset(
    {DataQualityFlag.STALE, DataQualityFlag.MISSING_ROUTE}
)
PIPELINE: Final = "council"

REASON_NOT_CONFIGURED: Final[str] = "model_not_configured"
REASON_PACKET: Final[str] = "packet_unavailable"
REASON_NEWS: Final[str] = "news_assessment_unavailable"
REASON_NEWS_MODE: Final[str] = "news_mode_abstain"
REASON_LLM_UNAVAILABLE: Final[str] = "llm_unavailable"
REASON_LLM_OUTPUT: Final[str] = "llm_output_invalid"
REASON_INTERNAL: Final[str] = "internal_error"
_MIN_TOOL_SECONDS: Final[float] = 1e-6
"""Tool time left to a resumed meeting that had used all of it: the next call times out (budget exceeded),
as it would have in the uninterrupted meeting (`ToolBudget` needs a positive limit)."""


class PgPins:
    """`ConfigPin` by recorded version ids (`load_pin`); pins are immutable, so they are cached."""

    def __init__(self, session_factory: sessionmaker[Session], *, max_entries: int = 64) -> None:
        self._factory = session_factory
        self._max = max_entries
        self._pins: OrderedDict[tuple[tuple[str, int], ...], ConfigPin] = OrderedDict()

    def __call__(self, version_ids: Mapping[str, int]) -> ConfigPin:
        key = tuple(sorted((name, int(v)) for name, v in version_ids.items()))
        pin = self._pins.get(key)
        if pin is None:
            with self._factory() as session:
                pin = load_pin(session, dict(key))
            self._pins[key] = pin
            while len(self._pins) > self._max:
                self._pins.popitem(last=False)
        return pin


@cache
def main_features(agent: AgentName, target_type: str) -> frozenset[str]:
    """The inputs of the agent's p_model: a blocking data-quality flag on one of them forces abstain."""
    return frozenset(load_p_model(AgentName(agent).value, target_type).coefficients)


@dataclass
class _EventState:
    session: ToolSession
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    opened: bool = False
    results: list[ToolResult] = field(default_factory=list)
    packet_result: ToolResult | None = None
    packet: QuantPacket | None = None
    assessment: NewsAssessment | None = None
    forced: str | None = None
    """Why this agent must abstain for the whole event (set once, when the session opens)."""
    p_model: float | None = None
    earlier_captures: tuple[LakeRef, ...] = ()
    """Captures of rounds played before this process resumed the meeting (`AgentRequest.earlier`)."""
    tools_closed: bool = False
    """The resumed meeting had already used every tool call of its budget."""

    def captures(self) -> tuple[LakeRef, ...]:
        return _unique((*self.earlier_captures, *self.session.captures))

    def ok_result_ids(self) -> frozenset[str]:
        return frozenset(r.result_id for r in self._ok() if r.result_id)

    def _ok(self) -> list[ToolResult]:
        results = [r for r in self.results if r.status == "ok" and r.result_id]
        if self.packet_result is not None and self.packet_result.result_id:
            results.insert(0, self.packet_result)
        return results

    def ok_results(self) -> tuple[ToolResult, ...]:
        return tuple(self._ok())

    def news_item_ids(self) -> frozenset[str]:
        ids: set[str] = set()
        for result in self._ok():
            _collect_item_ids(result.data, ids)
        if self.assessment is not None:
            ids.update(e.item_id for e in self.assessment.evidence)
        return frozenset(ids)


def _collect_item_ids(value: Any, out: set[str]) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            if key == "item_id" and isinstance(child, str):
                out.add(child)
            else:
                _collect_item_ids(child, out)
    elif isinstance(value, list):
        for child in value:
            _collect_item_ids(child, out)


@dataclass
class _Call:
    """Provenance gathered during one call; turned into the forecast by `_finish`."""

    request: AgentRequest
    agent: AgentName
    agent_version: int = UNREGISTERED_VERSION
    config: RoleModelConfig | None = None
    skill_commit: str | None = None
    state: _EventState | None = None
    records_from: int = 0
    usages: list[LlmUsage] = field(default_factory=list)
    prompt_hash: str | None = None
    model_returned: str | None = None
    provider: str | None = None
    generation_id: str | None = None

    def observe(self, step: ToolStep) -> None:
        self.usages.append(step.usage)
        self.model_returned, self.provider, self.generation_id = (
            step.model_returned,
            step.provider,
            step.generation_id,
        )


class LlmAgentRunner:
    """The council `AgentRunner` for the six agents (one instance per service and mode)."""

    def __init__(
        self,
        *,
        llm: ToolChatLlm,
        registry: ToolRegistry,
        pins: Callable[[Mapping[str, int]], ConfigPin],
        memory: MemoryStore,
        versions: VersionSource,
        health: RoleHealth | None,
        news: NewsAssessor | None,
        skills: SkillLoader | None = None,
        static: StaticConfig | None = None,
        max_sessions: int = 512,
    ) -> None:
        self._llm = llm
        self._registry = registry
        self._pins = pins
        self._memory = memory
        self._versions = versions
        self._health = health
        self._news = news
        self._skills = skills or SkillLoader()
        self._static = static or static_config()
        self._max_sessions = max_sessions
        self._states: OrderedDict[tuple[str, AgentName, str | None], _EventState] = OrderedDict()
        self._state_lock = asyncio.Lock()

    def close_event(self, event_id: str) -> None:
        """Forget the tool sessions of a finished meeting."""
        for key in [k for k in self._states if k[0] == event_id]:
            del self._states[key]

    async def forecast(self, request: AgentRequest) -> AgentResult:
        call = _Call(request=request, agent=AgentName(request.agent))
        try:
            return await self._forecast(call)
        except LlmCacheMissError:
            raise
        except Exception:
            log.exception(
                "agent run failed; abstaining",
                extra={"event_id": request.event_id, "agent": call.agent.value, "round": request.round},
            )
            return self._finish(call, abstain_reason=REASON_INTERNAL)

    # ------------------------------------------------------------------ one call

    async def _forecast(self, call: _Call) -> AgentResult:
        request, agent = call.request, call.agent
        if request.mode != self._registry.mode:
            raise ValueError(f"a {request.mode} request reached the {self._registry.mode} runner")
        pin = await asyncio.to_thread(self._pins, request.config_version_ids)
        council = pin.council(self._static)
        skills: SkillSet = await asyncio.to_thread(self._skills.load, agent, request.source)
        call.skill_commit = skills.skill_commit
        config = pin.models().roles.get(agent.value)
        if config is None:
            return self._finish(call, abstain_reason=REASON_NOT_CONFIGURED)
        call.config = config
        key = VersionKey.of(agent, config, prompt_hash=template_hash(agent), skill_commit=skills.skill_commit)
        call.agent_version = await asyncio.to_thread(
            self._versions.version, key, config_version_id=request.config_version_ids.get("models")
        )
        if self._health is not None:
            closed = await self._health.closed_reason(agent.value, config)
            if closed is not None:
                return self._finish(call, abstain_reason=closed)
        state = await self._state(request, council)
        _bind_pinned(state.session, request)
        call.state = state
        call.records_from = len(state.session.records)
        await self._open(state, request)
        if state.forced is not None:
            return self._finish(call, abstain_reason=state.forced)
        recall = await asyncio.to_thread(self._recall, request)
        inputs = self._inputs(request, council, skills, recall, state)
        system = system_prompt(agent)
        try:
            await self._react(call, config, system, inputs, state)
        except (LlmUnavailableError, LlmOutputError) as exc:
            log.warning("tool step failed; abstaining", extra=_log_extra(request, exc))
            return self._finish(call, abstain_reason=_unavailable_reason(exc))
        if state.session.abstain_reason is not None:
            return self._finish(call, abstain_reason=state.session.abstain_reason)
        inputs = self._inputs(request, council, skills, recall, state)
        try:
            generation = await self._llm.structured(
                role=agent.value,
                pipeline=PIPELINE,
                event_id=request.event_id,
                config=config,
                system=system,
                user=final_user(inputs),
                schema=AgentForecastDraft,
            )
        except LlmOutputError as exc:
            log.warning("forecast failed its schema twice; abstaining", extra=_log_extra(request, exc))
            return self._finish(call, abstain_reason=REASON_LLM_OUTPUT)
        except LlmUnavailableError as exc:
            log.warning("model unavailable; abstaining", extra=_log_extra(request, exc))
            return self._finish(call, abstain_reason=_unavailable_reason(exc))
        call.usages.append(generation.usage)
        call.prompt_hash = generation.prompt_hash
        call.model_returned, call.provider = generation.model_returned, generation.provider
        call.generation_id = generation.generation_id
        if state.p_model is None:  # set whenever state.forced is None; kept for type narrowing
            raise RuntimeError("no p_model for a non-forced agent")
        bounds = Bounds(
            agent=agent,
            round=request.round,
            p_model=state.p_model,
            a_i=request.a_i,
            llm_logit_margin=council.llm_logit_margin,
            logit_bound_total=council.logit_bound_total,
            stance_long=council.stance_long,
            stance_short=council.stance_short,
            candidate_set=None if agent is AgentName.NEWS else request.candidate_set,
            packet_fields=frozenset(state.packet.features) if state.packet is not None else frozenset(),
            tool_result_ids=state.ok_result_ids(),
            news_item_ids=state.news_item_ids(),
            shared_claim_ids=frozenset(c.claim_id for c in request.shared_claims),
        )
        checked = check_draft(generation.output, bounds)
        if checked.problems:
            log.info(
                "forecast corrected by code",
                extra={
                    "event_id": request.event_id,
                    "agent": agent.value,
                    "round": request.round,
                    "problems": list(checked.problems),
                },
            )
        return self._finish(call, checked=checked)

    async def _react(
        self, call: _Call, config: RoleModelConfig, system: str, inputs: PromptInputs, state: _EventState
    ) -> None:
        session = state.session
        tools = session.json_schemas()
        if not tools:
            return
        limit = session.budget.budget.max_calls
        if state.tools_closed:
            return
        first_user = tool_step_user(inputs, limit - session.budget.calls)
        messages: list[dict[str, Any]] = [
            {"role": "system", "content": system},
            {"role": "user", "content": first_user},
        ]
        for _ in range(MAX_REACT_STEPS):
            if session.abstain_reason is not None or session.budget.calls >= limit:
                return
            step = await self._llm.tool_step(
                role=call.agent.value,
                pipeline=PIPELINE,
                event_id=call.request.event_id,
                config=config,
                messages=messages,
                tools=tools,
            )
            call.observe(step)
            if not step.tool_calls:
                return
            assistant: dict[str, Any] = {
                "role": "assistant",
                "content": step.content,
                "tool_calls": [
                    {
                        "id": c.call_id,
                        "type": "function",
                        "function": {"name": c.name, "arguments": c.arguments},
                    }
                    for c in step.tool_calls
                ],
            }
            if step.reasoning_content is not None:
                # DeepSeek thinking mode: the chain of thought goes back with its tool calls.
                assistant["reasoning_content"] = step.reasoning_content
            messages.append(assistant)
            for tool_call in step.tool_calls:
                result = await session.call(tool_call.name, _arguments(tool_call.arguments))
                state.results.append(result)
                messages.append(
                    {"role": "tool", "tool_call_id": tool_call.call_id, "content": result.prompt_text()}
                )

    # ------------------------------------------------------------------ event state

    async def _state(self, request: AgentRequest, council: CouncilFile) -> _EventState:
        agent = AgentName(request.agent)
        lesson = request.shadow_lesson.lesson_id if request.shadow_lesson is not None else None
        key = (request.event_id, agent, lesson)
        async with self._state_lock:
            state = self._states.get(key)
            if state is not None:
                self._states.move_to_end(key)
            else:
                ctx = ToolContext(
                    request.mode,
                    agent,
                    request.event_id,
                    request.coin_id,
                    request.as_of,
                    tuple(request.pinned),
                )
                max_calls = min(council.tool_budget.max_calls, MAX_TOOL_CALLS)
                max_seconds = float(council.tool_budget.max_seconds)
                used_calls, used_seconds, exceeded = _budget_used(request.earlier)
                budget = ToolBudget(
                    max_calls=max(max_calls - used_calls, 1),
                    max_seconds=max(max_seconds - used_seconds, _MIN_TOOL_SECONDS),
                )
                state = _EventState(session=self._registry.session(ctx, budget))
                if request.earlier:
                    self._resume(state, request, exhausted=used_calls >= max_calls, exceeded=exceeded)
                self._states[key] = state
                while len(self._states) > self._max_sessions:
                    self._states.popitem(last=False)
        return state

    async def _open(self, state: _EventState, request: AgentRequest) -> None:
        """Read the agent's evidence base once per meeting: the quant packet, or the news assessment."""
        async with state.lock:
            if not state.opened:
                if AgentName(request.agent) is AgentName.NEWS:
                    await self._open_news(state, request)
                else:
                    await self._open_packet(state, request)
                state.opened = True

    def _resume(self, state: _EventState, request: AgentRequest, *, exhausted: bool, exceeded: bool) -> None:
        """Rebuild a meeting this process did not run from the agent's own earlier rounds: the tool results
        it may still cite, the packet (quant_core is not called again), the captures and the used budget,
        so a resumed meeting sees what an uninterrupted one would."""
        seen: set[str] = set()
        for earlier in request.earlier:
            for result in earlier.tool_results:
                if result.status != "ok" or not result.result_id or result.result_id in seen:
                    continue
                seen.add(result.result_id)
                if result.tool == "quant_core" and state.packet_result is None and result.data is not None:
                    self._use_packet(state, request, result)
                elif not request.earlier[-1].tool_log:
                    state.results.append(result)
        # The full ordered log (errors included) is what an uninterrupted session prompts with; it is
        # cumulative, so the last earlier round carries every call of the meeting so far.
        state.results.extend(request.earlier[-1].tool_log)
        state.earlier_captures = _unique(
            tuple(
                ref
                for earlier in request.earlier
                for ref in (*earlier.captures, *earlier.forecast.capture_manifest)
            )
        )
        if state.packet_result is not None:
            state.opened = True
        state.tools_closed = exhausted
        if exceeded and state.forced is None:
            state.forced = BUDGET_ABSTAIN_REASON

    async def _open_packet(self, state: _EventState, request: AgentRequest) -> None:
        result = await state.session.call("quant_core", {})
        if result.status != "ok" or result.data is None:
            state.results.append(result)
            state.forced = f"{REASON_PACKET}: {result.status}"
            return
        self._use_packet(state, request, result)

    @staticmethod
    def _use_packet(state: _EventState, request: AgentRequest, result: ToolResult) -> None:
        if result.data is None:
            raise ValueError("an ok quant_core result carries no data")
        packet = QuantPacket.model_validate(result.data["packet"])
        state.packet_result, state.packet, state.p_model = result, packet, packet.p_model
        flags = packet.flags_touching(set(main_features(AgentName(request.agent), packet.target_type.value)))
        blocking = sorted(flag.value for flag in flags & BLOCKING_FLAGS)
        if blocking:
            state.forced = f"data_quality_{'_'.join(blocking)}"

    async def _open_news(self, state: _EventState, request: AgentRequest) -> None:
        if self._news is None:
            state.forced = REASON_NEWS
            return
        try:
            assessment = await asyncio.to_thread(self._news.assess, request.coin_id, request.as_of)
        except LookupError as exc:
            log.warning("news assessment not available", extra=_log_extra(request, exc))
            state.forced = REASON_NEWS
            return
        if assessment.coin_id != request.coin_id or assessment.as_of != request.as_of:
            raise ValueError("the news assessor answered for another coin or as_of")
        state.assessment, state.p_model = assessment, assessment.p_model
        if assessment.mode == "abstain":
            state.forced = f"{REASON_NEWS_MODE}: {assessment.abstain_reason or 'no reason given'}"[:500]

    # ------------------------------------------------------------------ prompt inputs

    def _recall(self, request: AgentRequest) -> MemoryRecall:
        recall = AgentMemory(self._memory, AgentName(request.agent), request.as_of).recall(
            coin_id=request.coin_id, limit=MEMORY_EPISODES
        )
        lesson = request.shadow_lesson
        if lesson is None:
            return recall
        shadow = LessonText(
            lesson_id=lesson.lesson_id,
            title=lesson.template.title,
            when_text=lesson.template.when_text,
            observation=lesson.template.observation,
            adjustment=lesson.template.adjustment,
        )
        return recall.model_copy(update={"lessons": (*recall.lessons, shadow)})

    def _inputs(
        self,
        request: AgentRequest,
        council: CouncilFile,
        skills: SkillSet,
        recall: MemoryRecall,
        state: _EventState,
    ) -> PromptInputs:
        agent = AgentName(request.agent)
        event: dict[str, Any] = {
            "agent": agent.value,
            "coin_id": request.coin_id,
            "as_of": _iso(request.as_of),
            "round": request.round,
            "max_rounds": council.max_rounds,
            "horizon_h": council.horizon_h,
            "target_type": request.target_type.value,
            "event_type": request.source.value,
            "stance_long": council.stance_long,
            "stance_short": council.stance_short,
        }
        if agent is AgentName.NEWS:
            event["event_type"] = "news"
        return PromptInputs(
            agent=agent,
            event=event,
            skills=skills,
            memory=recall,
            packet_result=state.packet_result,
            news=state.assessment,
            candidate_set=None if agent is AgentName.NEWS else request.candidate_set,
            previous=request.previous,
            shared_claims=tuple(request.shared_claims),
            tool_results=tuple(state.results),
        )

    # ------------------------------------------------------------------ result

    def _finish(
        self, call: _Call, *, checked: Checked | None = None, abstain_reason: str | None = None
    ) -> AgentResult:
        request, state = call.request, call.state
        records = state.session.records[call.records_from :] if state is not None else ()
        captures = state.captures() if state is not None else ()
        packet = state.packet if state is not None else None
        p_model = state.p_model if state is not None else None
        if checked is None:
            checked = Checked(
                abstain=True,
                abstain_reason=(abstain_reason or REASON_INTERNAL)[:500],
                p_llm=None,
                p_used=None,
                stance=None,
                candidate_id=None,
                claims=(),
                cited_claim_ids=(),
                reason="",
                problems=(),
            )
        forecast = AgentForecast(
            agent=call.agent,
            agent_version=str(call.agent_version),
            event_id=request.event_id,
            round=request.round,
            coin_id=request.coin_id,
            as_of=request.as_of,
            target_type=request.target_type,
            label_spec_version=request.label_spec_version,
            p_model=p_model,
            p_llm=checked.p_llm,
            p_used=checked.p_used,
            abstain=checked.abstain,
            abstain_reason=checked.abstain_reason,
            candidate_id=checked.candidate_id,
            claims=checked.claims,
            cited_claim_ids=checked.cited_claim_ids,
            reason=checked.reason,
            packet_sha256=packet.packet_sha256 if packet is not None else None,
            # An abstain before any generation still names the prompt template it would have used.
            prompt_hash=call.prompt_hash or template_hash(call.agent),
            model_slug=call.config.model if call.config is not None else None,
            model_returned=call.model_returned or None,
            provider=call.provider or None,
            generation_id=call.generation_id,
            usage=_sum_usage(call.usages),
            skill_commit=call.skill_commit,
            tool_calls=tuple(records),
            capture_manifest=tuple(captures),
        )
        return AgentResult(
            forecast=forecast,
            captures=tuple(captures),
            tool_results=state.ok_results() if state is not None else (),
            tool_log=tuple(state.results) if state is not None else (),
        )


def _bind_pinned(session: ToolSession, request: AgentRequest) -> None:
    """Replay round N may open the captures of rounds 1..N (`request.pinned`, a growing manifest), so the
    session's context follows every round's request, not only the one that opened the session."""
    pinned = tuple(request.pinned)
    if session.ctx.pinned != pinned:
        session.ctx = replace(session.ctx, pinned=pinned)


def _budget_used(earlier: tuple[AgentResult, ...]) -> tuple[int, float, bool]:
    """Tool calls and seconds the agent's earlier rounds used, and whether its budget was exceeded."""
    records = [record for result in earlier for record in result.forecast.tool_calls]
    return (
        len(records),
        sum(record.latency_ms for record in records) / 1000,
        any(record.status == "budget_exceeded" for record in records),
    )


def _unique(refs: tuple[LakeRef, ...]) -> tuple[LakeRef, ...]:
    out: list[LakeRef] = []
    for ref in refs:
        if ref not in out:
            out.append(ref)
    return tuple(out)


def _arguments(raw: str) -> dict[str, Any]:
    """Tool arguments as the model wrote them; non-object JSON reaches the tool as-is and is rejected there
    (recorded and charged to the budget like any other invalid call)."""
    try:
        parsed = json.loads(raw) if raw.strip() else {}
    except ValueError:
        return {"arguments": raw[:500]}
    return parsed if isinstance(parsed, dict) else {"arguments": parsed}


def _sum_usage(usages: list[LlmUsage]) -> LlmUsage | None:
    if not usages:
        return None
    costs = [u.cost_usd for u in usages if u.cost_usd is not None]
    return LlmUsage(
        prompt_tokens=sum(u.prompt_tokens for u in usages),
        completion_tokens=sum(u.completion_tokens for u in usages),
        cost_usd=sum(costs) if costs else None,
    )


def _iso(value: datetime) -> str:
    return value.isoformat()


def _unavailable_reason(exc: Exception) -> str:
    """A role closed at the router keeps its self-test reason; an unusable reply (a truncated or invalid
    tool step) is an output error, counted in the replay parse-error rate; any other failure is an
    unreachable model."""
    if isinstance(exc, LlmRoleClosedError):
        return exc.reason
    if isinstance(exc, LlmOutputError):
        return REASON_LLM_OUTPUT
    return REASON_LLM_UNAVAILABLE


def _log_extra(request: AgentRequest, exc: BaseException) -> dict[str, Any]:
    return {
        "event_id": request.event_id,
        "agent": AgentName(request.agent).value,
        "round": request.round,
        "error": f"{type(exc).__name__}: {exc}"[:300],
    }
