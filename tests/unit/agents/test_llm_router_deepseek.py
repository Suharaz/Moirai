"""`LlmRouter` on the DeepSeek gateway against a fake DeepSeek transport: request body, JSON Output parse
and retry, thinking-mode tool steps, cost from the price table, cache isolation from OpenRouter."""

from __future__ import annotations

import json
from typing import Any

import httpx2
import pytest
from llm_fakes import DEEPSEEK_MODEL, MemoryLlmStore, config, deepseek_config, flat_deepseek_prices

from hdt.agents.llm_router import LlmRouter
from hdt.agents.llm_types import LlmOutputError, LlmUnavailableError
from hdt.contracts.forecast import AgentForecastDraft
from hdt.settings.schemas import RoleModelConfig

GOOD = json.dumps({"p_llm": 0.6, "abstain": False, "claims": [], "cited_claim_ids": [], "reason": "r"})
PRICES = flat_deepseek_prices(hit=0.01, miss=0.2, output=1.0)
USAGE: dict[str, Any] = {
    "prompt_tokens": 1000,
    "completion_tokens": 100,
    "total_tokens": 1100,
    "prompt_cache_hit_tokens": 400,
    "prompt_cache_miss_tokens": 600,
}
COST = (400 * 0.01 + 600 * 0.2 + 100 * 1.0) / 1_000_000


def completion(message: dict[str, Any], *, n: int, finish: str = "stop", **fields: Any) -> dict[str, Any]:
    body = {
        "id": f"ds-{n}",
        "object": "chat.completion",
        "created": 1,
        "model": DEEPSEEK_MODEL,
        "choices": [{"index": 0, "finish_reason": finish, "message": {"role": "assistant", **message}}],
        "usage": USAGE,
    }
    return {**body, **fields}


class FakeDeepSeek:
    """Answers `https://api.deepseek.com/chat/completions` with queued messages; records every body."""

    def __init__(self, *messages: dict[str, Any], **fields: Any) -> None:
        self.messages = list(messages)
        self.fields = fields
        self.requests: list[dict[str, Any]] = []

    def handler(self, request: httpx2.Request) -> httpx2.Response:
        assert (request.url.host, request.url.path) == ("api.deepseek.com", "/chat/completions")
        assert request.headers["authorization"] == "Bearer sk-ds-test"
        self.requests.append(json.loads(request.content))
        return httpx2.Response(
            200, json=completion(self.messages.pop(0), n=len(self.requests), **self.fields)
        )

    def router(self, store: MemoryLlmStore, *, openrouter: Any = None) -> LlmRouter:
        client = httpx2.AsyncClient(transport=httpx2.MockTransport(openrouter or self.handler))
        return LlmRouter(
            mode="live",
            api_key=lambda: "sk-or-test",
            deepseek_api_key=lambda: "sk-ds-test",
            http_client=client,
            deepseek_prices=PRICES,
            store=store,
        )


async def ask(router: LlmRouter, cfg: RoleModelConfig | None = None) -> Any:
    return await router.structured(
        role="technical",
        pipeline="council",
        event_id="evt-1",
        config=cfg or deepseek_config(),
        system="rules",
        user="packet",
        schema=AgentForecastDraft,
    )


async def test_structured_call_sends_the_deepseek_body_and_records_provider_and_cost() -> None:
    fake, store = FakeDeepSeek({"content": GOOD}), MemoryLlmStore()
    generation = await ask(fake.router(store))

    [sent] = fake.requests
    assert sent["model"] == DEEPSEEK_MODEL
    assert sent["response_format"] == {"type": "json_object"}
    assert sent["thinking"] == {"type": "disabled"}
    assert (sent["temperature"], sent["max_tokens"]) == (0.2, 800)
    assert not {"provider", "models", "usage", "reasoning_effort"} & set(sent)
    system = sent["messages"][0]["content"]
    assert system.startswith("rules\n\n")
    assert "json" in system
    assert json.loads(system.split("JSON Schema:\n", 1)[1]) == AgentForecastDraft.model_json_schema()

    assert generation.output.p_llm == 0.6
    assert (generation.model_slug, generation.model_returned, generation.provider) == (
        DEEPSEEK_MODEL,
        DEEPSEEK_MODEL,
        "deepseek",
    )
    assert generation.usage.cost_usd == pytest.approx(COST)
    [call] = store.calls
    assert (call.provider, call.status, call.cost_usd) == ("deepseek", "ok", pytest.approx(COST))
    assert [key[2] for key in store.cache] == ["deepseek"]


@pytest.mark.parametrize("bad", ["", "not json", "{}"])
async def test_empty_or_invalid_json_is_a_parse_failure_retried_once(bad: str) -> None:
    fake, store = FakeDeepSeek({"content": bad}, {"content": GOOD}), MemoryLlmStore()
    generation = await ask(fake.router(store))
    assert generation.generation_id == "ds-2"
    assert [c.status for c in store.calls] == ["invalid_output", "ok"]
    assert all(c.cost_usd == pytest.approx(COST) for c in store.calls)  # both generations are billed


async def test_two_empty_replies_raise_an_output_error_and_cache_nothing() -> None:
    fake, store = FakeDeepSeek({"content": ""}, {"content": None}), MemoryLlmStore()
    with pytest.raises(LlmOutputError):
        await ask(fake.router(store))
    assert [c.status for c in store.calls] == ["invalid_output", "invalid_output"]
    assert store.cache == {}


async def test_prompt_tokens_without_the_cache_split_are_priced_as_misses() -> None:
    usage = {"prompt_tokens": 1000, "completion_tokens": 100, "total_tokens": 1100}
    fake, store = FakeDeepSeek({"content": GOOD}, usage=usage), MemoryLlmStore()
    generation = await ask(fake.router(store))
    assert generation.usage.cost_usd == pytest.approx((1000 * 0.2 + 100 * 1.0) / 1_000_000)


async def test_thinking_tool_step_sends_the_effort_and_returns_the_chain_of_thought() -> None:
    call = {"id": "call_1", "type": "function", "function": {"name": "quant_core", "arguments": "{}"}}
    fake = FakeDeepSeek({"content": "", "reasoning_content": "read the packet first", "tool_calls": [call]})
    store = MemoryLlmStore()
    kwargs: dict[str, Any] = {
        "role": "technical",
        "pipeline": "council",
        "event_id": "evt-1",
        "config": deepseek_config(thinking="high"),
        "messages": [{"role": "user", "content": "go"}],
        "tools": [{"name": "quant_core", "description": "packet", "parameters": {"type": "object"}}],
    }
    step = await fake.router(store).tool_step(**kwargs)

    [sent] = fake.requests
    assert sent["thinking"] == {"type": "enabled"}
    assert sent["reasoning_effort"] == "high"
    assert "temperature" not in sent  # thinking mode ignores it
    assert sent["tool_choice"] == "auto"
    assert [(c.call_id, c.name) for c in step.tool_calls] == [("call_1", "quant_core")]
    assert step.reasoning_content == "read the packet first"
    assert (step.provider, step.usage.cost_usd) == ("deepseek", pytest.approx(COST))

    replayed = await LlmRouter(mode="replay", api_key=None, store=store).tool_step(**kwargs)
    assert replayed.cached
    assert replayed.reasoning_content == "read the packet first"


async def test_gateways_never_share_a_cache_entry() -> None:
    openrouter_sent: list[dict[str, Any]] = []

    def openrouter_or_deepseek(request: httpx2.Request) -> httpx2.Response:
        if request.url.host == "api.deepseek.com":
            return fake.handler(request)
        openrouter_sent.append(json.loads(request.content))
        body = completion({"content": GOOD}, n=len(openrouter_sent), provider="Anthropic")
        body["model"] = config().model
        return httpx2.Response(200, json=body)

    fake, store = FakeDeepSeek({"content": GOOD}), MemoryLlmStore()
    router = fake.router(store, openrouter=openrouter_or_deepseek)
    on_openrouter = await ask(router, config())
    on_deepseek = await ask(router, deepseek_config())

    assert not on_deepseek.cached
    assert (len(openrouter_sent), len(fake.requests)) == (1, 1)
    assert on_openrouter.params_hash != on_deepseek.params_hash
    assert sorted(key[2] for key in store.cache) == ["deepseek", "only:Anthropic"]
    assert (await ask(router, deepseek_config())).cached


@pytest.mark.parametrize(
    ("requested", "served"),
    [
        ("deepseek-flash", "deepseek-flash"),
        ("deepseek-flash", "deepseek-v4-flash"),  # documented legacy alias served by V4.1-Flash
        ("deepseek-v4-pro", "deepseek-v4-pro"),
    ],
)
async def test_a_reply_naming_the_requested_model_or_its_alias_is_used(requested: str, served: str) -> None:
    fake, store = FakeDeepSeek({"content": GOOD}, model=served), MemoryLlmStore()
    generation = await ask(fake.router(store), deepseek_config(model=requested))
    assert (generation.model_slug, generation.model_returned) == (requested, served)
    assert [c.status for c in store.calls] == ["ok"]


@pytest.mark.parametrize(
    ("requested", "served"),
    [
        ("deepseek-flash", "deepseek-v4-pro"),  # a Flash request never takes a Pro reply
        ("deepseek-v4-pro", "deepseek-flash"),  # nor the reverse
        ("deepseek-v4-pro", "deepseek-v4-flash"),
        ("deepseek-flash", "deepseek-chat"),
        ("deepseek-flash", "gpt-4o"),
    ],
)
async def test_a_reply_from_another_model_is_refused_and_not_cached(requested: str, served: str) -> None:
    fake, store = FakeDeepSeek({"content": GOOD}, model=served), MemoryLlmStore()
    with pytest.raises(LlmUnavailableError, match=f"served model '{served}' is not '{requested}'"):
        await ask(fake.router(store), deepseek_config(model=requested))
    assert [c.status for c in store.calls] == ["invalid_output"]
    assert store.cache == {}


async def test_a_deepseek_role_without_a_deepseek_key_is_unavailable() -> None:
    router = LlmRouter(mode="live", api_key=lambda: "sk-or-test", store=MemoryLlmStore())
    with pytest.raises(LlmUnavailableError, match="no DeepSeek key source"):
        await ask(router)


async def test_an_unknown_deepseek_model_is_refused_before_any_request() -> None:
    fake, store = FakeDeepSeek({"content": GOOD}), MemoryLlmStore()
    cfg = deepseek_config().model_copy(update={"model": "deepseek-chat"})  # bypasses the validator
    with pytest.raises(LlmUnavailableError, match="refused"):
        await ask(fake.router(store), cfg)
    assert fake.requests == []
