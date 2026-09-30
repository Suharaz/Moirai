"""Structured-output self-test: console model tests and the fail-closed role gate (test-strategy 3.4)."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any

import httpx2
import pytest
from llm_fakes import MODEL, MemoryLlmStore, config

from hdt.agents.llm_router import LlmRouter
from hdt.agents.llm_types import LlmRoleClosedError, LlmUnavailableError
from hdt.agents.model_test import CLOSED_REASON, RoleHealth, SelfTestReply, model_test_runner
from hdt.settings.schemas import ModelsSection, RoleModelConfig
from hdt.vault.owner import ModelTestRequest

CORRECT = {
    "total": 42,
    "direction": "down",
    "probability": 0.25,
    "items": [{"key": "a", "count": 1}, {"key": "b", "count": 2}],
    "note": None,
}


class Replies:
    def __init__(self, *contents: str) -> None:
        self.contents = list(contents)
        self.sent: list[dict[str, Any]] = []

    def handler(self, request: httpx2.Request) -> httpx2.Response:
        self.sent.append(json.loads(request.content))
        return httpx2.Response(
            200,
            json={
                "id": f"gen-{len(self.sent)}",
                "object": "chat.completion",
                "created": 1,
                "model": MODEL,
                "provider": "Anthropic",
                "choices": [
                    {
                        "index": 0,
                        "finish_reason": "stop",
                        "message": {"role": "assistant", "content": self.contents.pop(0)},
                    }
                ],
                "usage": {"prompt_tokens": 50, "completion_tokens": 30, "total_tokens": 80, "cost": 0.001},
            },
        )

    def router(self) -> LlmRouter:
        return LlmRouter(
            mode="live",
            api_key=lambda: "sk-or-test",
            http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(self.handler)),
            store=MemoryLlmStore(),
        )


def request(cfg: RoleModelConfig) -> ModelTestRequest:
    return ModelTestRequest(
        request_id="req-1", role="technical", config=cfg, requested_by="owner", requested_at=datetime.now(UTC)
    )


async def test_console_model_test_reports_a_valid_schema_with_provenance() -> None:
    replies = Replies(json.dumps(CORRECT))
    outcome = await model_test_runner(replies.router())(request(config()))
    assert outcome.schema_valid is True
    assert outcome.error is None
    assert (outcome.provider, outcome.model_returned, outcome.cost_usd) == ("Anthropic", MODEL, 0.001)
    assert replies.sent[0]["response_format"]["json_schema"]["strict"] is True


async def test_a_model_that_cannot_keep_the_schema_fails_the_test() -> None:
    replies = Replies("sure, here you go", '{"total": "forty-two"}')
    outcome = await model_test_runner(replies.router())(request(config()))
    assert outcome.schema_valid is False
    assert outcome.error


async def test_a_wrong_answer_in_a_valid_shape_fails_the_test() -> None:
    replies = Replies(json.dumps({**CORRECT, "total": 41}))
    outcome = await model_test_runner(replies.router())(request(config()))
    assert outcome.schema_valid is False


async def test_failing_role_is_closed_alerted_then_reopened_by_a_passing_retest(
    caplog: pytest.LogCaptureFixture,
) -> None:
    now = [0.0]
    replies = Replies("broken", "broken", json.dumps(CORRECT))
    health = RoleHealth(replies.router(), None, retry_after_s=600, clock=lambda: now[0])
    models = ModelsSection(roles={"technical": config()})

    with caplog.at_level(logging.WARNING):
        assert await health.check_all(models) == {"technical": CLOSED_REASON}
    assert any(getattr(r, "alert_kind", None) == "config_invalid" for r in caplog.records)
    assert await health.closed_reason("technical", config()) == CLOSED_REASON
    assert len(replies.sent) == 2  # no re-test before retry_after_s

    now[0] = 601.0
    assert await health.closed_reason("technical", config()) is None
    assert len(replies.sent) == 3


async def test_a_config_change_is_tested_again() -> None:
    replies = Replies(json.dumps(CORRECT), "broken", "broken")
    health = RoleHealth(replies.router(), None)
    assert await health.closed_reason("technical", config()) is None
    assert await health.closed_reason("technical", config(temperature=0.7)) == CLOSED_REASON


async def test_replay_never_tests_or_closes_a_role() -> None:
    health = RoleHealth(LlmRouter(mode="replay", api_key=None, store=MemoryLlmStore()), None)
    assert await health.closed_reason("technical", config()) is None


@pytest.mark.parametrize("role", ["news_extractor", "news_judge_a", "news_judge_b", "reflection"])
async def test_a_failed_self_test_closes_every_role_at_the_router(role: Any) -> None:
    replies = Replies("broken", "broken")
    router = replies.router()
    health = RoleHealth(router, None)
    await health.check_all(ModelsSection(roles={role: config()}))
    assert len(replies.sent) == 2
    with pytest.raises(LlmRoleClosedError) as closed:
        await router.structured(
            role=role,
            pipeline="news" if role.startswith("news") else "reflection",
            event_id="evt-1",
            config=config(),
            system="rules",
            user="item",
            schema=SelfTestReply,
        )
    assert closed.value.reason == CLOSED_REASON
    assert isinstance(closed.value, LlmUnavailableError)
    with pytest.raises(LlmRoleClosedError):
        await router.tool_step(
            role=role,
            pipeline="news" if role.startswith("news") else "reflection",
            event_id="evt-1",
            config=config(),
            messages=[{"role": "user", "content": "go"}],
            tools=[],
        )
    assert len(replies.sent) == 2


async def test_a_passing_role_is_not_gated() -> None:
    replies = Replies(json.dumps(CORRECT), json.dumps(CORRECT))
    router = replies.router()
    RoleHealth(router, None)
    generation = await router.structured(
        role="news_judge_a",
        pipeline="news",
        event_id="evt-1",
        config=config(),
        system="rules",
        user="item",
        schema=SelfTestReply,
    )
    assert generation.output.total == 42
    assert len(replies.sent) == 2
