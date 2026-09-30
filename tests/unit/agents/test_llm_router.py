"""`LlmRouter` against a fake OpenRouter transport (test-strategy 3.4): request shape, pinned slugs,
recording, cache, replay, parse retry, served-model checks, metrics."""

from __future__ import annotations

import json
from typing import Any

import httpx2
import pytest
from llm_fakes import MODEL, MemoryLlmStore, config
from prometheus_client import REGISTRY

from hdt.agents.llm_router import CachedReply, CacheKey, LlmCallRecord, LlmRouter
from hdt.agents.llm_types import LlmCacheMissError, LlmOutputError, LlmUnavailableError
from hdt.contracts.forecast import AgentForecastDraft
from hdt.settings.schemas import RoleModelConfig

GOOD = json.dumps({"p_llm": 0.6, "abstain": False, "claims": [], "cited_claim_ids": [], "reason": "r"})


class FakeOpenRouter:
    """Answers `/chat/completions` with the queued contents; records every request body."""

    def __init__(self, *contents: str, model: str = MODEL, provider: str = "Anthropic") -> None:
        self.contents = list(contents)
        self.model = model
        self.provider = provider
        self.requests: list[dict[str, Any]] = []

    def handler(self, request: httpx2.Request) -> httpx2.Response:
        assert request.url.path.endswith("/chat/completions")
        assert request.headers["authorization"] == "Bearer sk-or-test"
        self.requests.append(json.loads(request.content))
        n = len(self.requests)
        body = {
            "id": f"gen-{n}",
            "object": "chat.completion",
            "created": 1,
            "model": self.model,
            "provider": self.provider,
            "choices": [
                {
                    "index": 0,
                    "finish_reason": "stop",
                    "message": {"role": "assistant", "content": self.contents.pop(0)},
                }
            ],
            "usage": {"prompt_tokens": 100, "completion_tokens": 20, "total_tokens": 120, "cost": 0.0025},
        }
        return httpx2.Response(200, json=body)

    def router(self, store: MemoryLlmStore, mode: Any = "live") -> LlmRouter:
        client = httpx2.AsyncClient(transport=httpx2.MockTransport(self.handler))
        return LlmRouter(mode=mode, api_key=lambda: "sk-or-test", http_client=client, store=store)


async def ask(router: LlmRouter, cfg: RoleModelConfig | None = None, user: str = "packet") -> Any:
    return await router.structured(
        role="technical",
        pipeline="council",
        event_id="evt-1",
        config=cfg or config(),
        system="rules",
        user=user,
        schema=AgentForecastDraft,
    )


async def test_live_call_sends_strict_schema_and_pinned_provider_prefs_and_records_it() -> None:
    fake, store = FakeOpenRouter(GOOD), MemoryLlmStore()
    generation = await ask(fake.router(store))

    sent = fake.requests[0]
    assert sent["model"] == MODEL
    assert sent["response_format"]["type"] == "json_schema"
    assert sent["response_format"]["json_schema"]["strict"] is True
    schema = sent["response_format"]["json_schema"]["schema"]
    assert schema["additionalProperties"] is False
    assert set(schema["required"]) == set(AgentForecastDraft.model_fields)
    assert sent["provider"] == {
        "allow_fallbacks": False,
        "require_parameters": True,
        "data_collection": "deny",
        "zdr": False,
        "only": ["Anthropic"],
    }
    assert sent["temperature"] == 0.2
    assert sent["max_tokens"] == 800

    assert generation.output.p_llm == 0.6
    assert (generation.model_slug, generation.model_returned, generation.provider) == (
        MODEL,
        MODEL,
        "Anthropic",
    )
    assert generation.generation_id == "gen-1"
    assert not generation.cached
    assert generation.usage.cost_usd == 0.0025
    [call] = store.calls
    assert (call.pipeline, call.role, call.event_id, call.status) == ("council", "technical", "evt-1", "ok")
    assert (call.prompt_tokens, call.completion_tokens, call.cost_usd) == (100, 20, 0.0025)
    assert call.prompt_hash == generation.prompt_hash
    assert len(store.cache) == 1


@pytest.mark.parametrize(
    "slug",
    [
        "openrouter/auto",
        "meta-llama/llama-3.3-70b-instruct:free",
        "openai/gpt-4o:online",
        "openai/gpt-4o:nitro",
        "openai/gpt-4o:floor",
        "anthropic/claude-3.7-sonnet:thinking",
    ],
)
async def test_routers_and_variant_slugs_are_refused_before_any_request(slug: str) -> None:
    fake, store = FakeOpenRouter(GOOD), MemoryLlmStore()
    cfg = config().model_copy(update={"model": slug})  # bypasses the config validator on purpose
    with pytest.raises(LlmUnavailableError, match="refused"):
        await ask(fake.router(store), cfg)
    assert fake.requests == []
    assert store.calls == []


async def test_a_variant_fallback_slug_is_refused_before_any_request() -> None:
    fake, store = FakeOpenRouter(GOOD), MemoryLlmStore()
    cfg = config().model_copy(update={"fallback_models": ("openai/gpt-4o:online",)})
    with pytest.raises(LlmUnavailableError, match="variant"):
        await ask(fake.router(store), cfg)
    assert fake.requests == []


@pytest.mark.parametrize(
    "loosened", [{"allow_fallbacks": True}, {"data_collection": "allow"}], ids=["fallbacks", "data"]
)
async def test_loosened_provider_preferences_are_refused_before_any_request(loosened: dict[str, Any]) -> None:
    fake, store = FakeOpenRouter(GOOD), MemoryLlmStore()
    cfg = config().model_copy(update=loosened)
    with pytest.raises(LlmUnavailableError, match="provider preferences refused"):
        await ask(fake.router(store), cfg)
    assert fake.requests == []
    assert store.calls == []


@pytest.mark.parametrize(
    ("only", "provider"),
    [
        (("amazon-bedrock",), "Amazon Bedrock"),
        (("google-ai-studio",), "Google AI Studio"),
        (("google-vertex",), "google-vertex/us-east5"),
        (("Anthropic",), "anthropic"),
        (("google-vertex/us-east5",), "Google Vertex"),  # m2: replies carry no region
    ],
)
async def test_provider_only_accepts_the_display_name_of_an_allowed_slug(
    only: tuple[str, ...], provider: str
) -> None:
    fake, store = FakeOpenRouter(GOOD, provider=provider), MemoryLlmStore()
    cfg = config().model_copy(update={"provider": config().provider.model_copy(update={"only": only})})
    assert not (await ask(fake.router(store), cfg)).cached
    assert [c.status for c in store.calls] == ["ok"]


async def test_identical_request_is_served_from_the_cache_without_a_new_generation() -> None:
    fake, store = FakeOpenRouter(GOOD), MemoryLlmStore()
    router = fake.router(store)
    first = await ask(router)
    second = await ask(router)
    assert second.cached
    assert second.output == first.output
    assert second.generation_id == first.generation_id
    assert len(fake.requests) == 1
    assert len(store.calls) == 1


async def test_replay_reads_only_the_cache_and_a_miss_is_a_hard_error() -> None:
    fake, store = FakeOpenRouter(GOOD), MemoryLlmStore()
    await ask(fake.router(store))
    replay = LlmRouter(mode="replay", api_key=None, store=store)
    assert (await ask(replay)).cached
    with pytest.raises(LlmCacheMissError):
        await ask(replay, user="another packet")
    assert len(fake.requests) == 1


async def test_parse_failure_is_retried_once_then_succeeds_and_both_generations_are_billed() -> None:
    fake, store = FakeOpenRouter("not json", GOOD), MemoryLlmStore()
    generation = await ask(fake.router(store))
    assert generation.output.p_llm == 0.6
    assert generation.generation_id == "gen-2"
    assert [c.status for c in store.calls] == ["invalid_output", "ok"]
    [cached] = store.cache.values()
    assert cached.generation_id == "gen-2"


async def test_two_parse_failures_raise_output_error_and_cache_nothing() -> None:
    bad = json.dumps({"p_llm": 1.5, "abstain": False})
    fake, store = FakeOpenRouter("not json", bad), MemoryLlmStore()
    with pytest.raises(LlmOutputError):
        await ask(fake.router(store))
    assert [c.status for c in store.calls] == ["invalid_output", "invalid_output"]
    assert store.cache == {}


@pytest.mark.parametrize(
    ("model", "provider"),
    [
        ("openai/gpt-4o", "Anthropic"),
        (MODEL, "DeepInfra"),
        ("openrouter/auto", "Anthropic"),
        ("anthropic/claude-sonnet-4.5:online", "Anthropic"),
        ("", "Anthropic"),
        ("claude-sonnet-4.5", "Anthropic"),
        ("anthropic/claude-3-haiku", "Anthropic"),  # same developer, another model
        ("anthropic/claude-sonnet-4.5-mini", "Anthropic"),  # the pin as a prefix only
        ("anthropic/claude-sonnet-4.5-2025-9-29", "Anthropic"),  # not a date suffix
        ("anthropic/claude-sonnet-4.5-2025-13-45", "Anthropic"),  # m1: not a calendar date
        ("anthropic/claude-sonnet-4.5-20250230", "Anthropic"),  # m1: not a calendar date
        ("anthropic/claude-sonnet-4.5-2025-0131", "Anthropic"),  # m1: mixed separators
        # m1: a date in non-ASCII (Devanagari) digits
        (MODEL + "-\u0968\u0966\u0968\u096b\u0966\u0967\u0969\u0967", "Anthropic"),
        ("anthropic/claude-3-haiku-20240307", "Anthropic"),  # a dated form of another model
    ],
)
async def test_reply_served_outside_the_pin_is_refused(model: str, provider: str) -> None:
    fake, store = FakeOpenRouter(GOOD, model=model, provider=provider), MemoryLlmStore()
    with pytest.raises(LlmUnavailableError, match="refused"):
        await ask(fake.router(store))
    assert [c.status for c in store.calls] == ["invalid_output"]
    assert store.cache == {}


@pytest.mark.parametrize(
    "model",
    [
        MODEL,
        MODEL + "-20250929",
        MODEL + "-2025-09-29",
        "anthropic/claude-opus-4.1",
        "anthropic/claude-opus-4.1-20250805",
    ],
)
async def test_reply_served_by_a_pinned_slug_or_its_dated_form_is_accepted(model: str) -> None:
    fake, store = FakeOpenRouter(GOOD, model=model), MemoryLlmStore()
    cfg = config(fallback_models=["anthropic/claude-opus-4.1"])
    generation = await ask(fake.router(store), cfg)
    assert generation.model_returned == model
    assert [c.status for c in store.calls] == ["ok"]
    assert len(store.cache) == 1


async def test_reply_served_by_a_fallback_that_is_not_configured_is_refused() -> None:
    fake, store = FakeOpenRouter(GOOD, model="anthropic/claude-opus-4.1"), MemoryLlmStore()
    cfg = config(fallback_models=["anthropic/claude-sonnet-4"])
    with pytest.raises(LlmUnavailableError, match="outside the pinned slugs"):
        await ask(fake.router(store), cfg)
    assert [c.status for c in store.calls] == ["invalid_output"]
    assert store.cache == {}


async def test_transport_error_is_unavailable_and_never_recorded() -> None:
    def down(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(503, json={"error": {"message": "no provider"}})

    store = MemoryLlmStore()
    router = LlmRouter(
        mode="live",
        api_key=lambda: "sk-or-test",
        http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(down)),
        store=store,
    )
    with pytest.raises(LlmUnavailableError):
        await ask(router)
    assert store.calls == []


async def test_tokens_and_cost_are_exported_as_metrics() -> None:
    labels = {"pipeline": "council", "role": "technical", "model": MODEL}

    def value(name: str, **extra: str) -> float:
        return REGISTRY.get_sample_value(name, {**labels, **extra}) or 0.0

    before = (value("hdt_llm_tokens_total", kind="prompt"), value("hdt_llm_cost_usd_total"))
    fake = FakeOpenRouter(GOOD)
    await ask(fake.router(MemoryLlmStore()), user="metrics")
    assert value("hdt_llm_tokens_total", kind="prompt") - before[0] == 100
    assert value("hdt_llm_cost_usd_total") - before[1] == pytest.approx(0.0025)


async def test_tool_step_sends_tools_and_returns_the_calls() -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        sent = json.loads(request.content)
        assert sent["tools"][0]["function"]["name"] == "quant_core"
        assert "response_format" not in sent
        return httpx2.Response(
            200,
            json={
                "id": "gen-t",
                "object": "chat.completion",
                "created": 1,
                "model": MODEL,
                "provider": "Anthropic",
                "choices": [
                    {
                        "index": 0,
                        "finish_reason": "tool_calls",
                        "message": {
                            "role": "assistant",
                            "content": None,
                            "tool_calls": [
                                {
                                    "id": "call_1",
                                    "type": "function",
                                    "function": {"name": "quant_core", "arguments": "{}"},
                                }
                            ],
                        },
                    }
                ],
                "usage": {"prompt_tokens": 5, "completion_tokens": 3, "total_tokens": 8},
            },
        )

    store = MemoryLlmStore()
    router = LlmRouter(
        mode="live",
        api_key=lambda: "sk-or-test",
        http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(handler)),
        store=store,
    )
    kwargs: dict[str, Any] = {
        "role": "technical",
        "pipeline": "council",
        "event_id": "evt-1",
        "config": config(),
        "messages": [{"role": "user", "content": "go"}],
        "tools": [{"name": "quant_core", "description": "packet", "parameters": {"type": "object"}}],
    }
    step = await router.tool_step(**kwargs)
    assert [(c.call_id, c.name, c.arguments) for c in step.tool_calls] == [("call_1", "quant_core", "{}")]
    replayed = await LlmRouter(mode="replay", api_key=None, store=store).tool_step(**kwargs)
    assert replayed.cached
    assert replayed.tool_calls == step.tool_calls


async def test_truncated_tool_step_is_recorded_but_never_cached() -> None:
    def handler(request: httpx2.Request) -> httpx2.Response:
        body = {
            "id": "gen-cut",
            "object": "chat.completion",
            "created": 1,
            "model": MODEL,
            "provider": "Anthropic",
            "choices": [
                {"index": 0, "finish_reason": "length", "message": {"role": "assistant", "content": "I will"}}
            ],
            "usage": {"prompt_tokens": 5, "completion_tokens": 3, "total_tokens": 8},
        }
        return httpx2.Response(200, json=body)

    store = MemoryLlmStore()
    router = LlmRouter(
        mode="live",
        api_key=lambda: "sk-or-test",
        http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(handler)),
        store=store,
    )
    kwargs: dict[str, Any] = {
        "role": "technical",
        "pipeline": "council",
        "event_id": "evt-1",
        "config": config(),
        "messages": [{"role": "user", "content": "go"}],
        "tools": [{"name": "quant_core", "description": "packet", "parameters": {"type": "object"}}],
    }
    with pytest.raises(LlmOutputError, match="length"):
        await router.tool_step(**kwargs)
    assert [c.status for c in store.calls] == ["invalid_output"]
    assert store.cache == {}


class FailingRecordStore(MemoryLlmStore):
    def record(self, call: LlmCallRecord, cache: tuple[CacheKey, CachedReply] | None) -> None:
        raise ConnectionError("database down")


async def test_a_record_failure_is_unavailable_not_a_raw_database_error() -> None:
    fake = FakeOpenRouter(GOOD)
    with pytest.raises(LlmUnavailableError, match="not recorded"):
        await ask(fake.router(FailingRecordStore()))


async def test_a_malformed_vault_item_is_unavailable() -> None:
    def key() -> str:
        raise KeyError("api_key")

    router = LlmRouter(mode="live", api_key=key, store=MemoryLlmStore())
    with pytest.raises(LlmUnavailableError, match="key unavailable"):
        await ask(router)
