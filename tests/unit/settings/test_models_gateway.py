"""The `models` section across gateways: model names per gateway, OpenRouter-only options, the judges'
developer rule, the DeepSeek switch draft, agent-version hashes and change detection."""

from __future__ import annotations

from typing import Any, get_args

import pytest
from pydantic import ValidationError

from hdt.configapi.routes.models import changed_roles
from hdt.core.config import LlmRole
from hdt.core.ids import canonical_sha256
from hdt.settings.schemas import ModelsSection, RoleModelConfig, deepseek_profile
from hdt.settings.store import provider_prefs_hash
from hdt.settings.versions import ConfigVersion

LIMITS: dict[str, Any] = {"temperature": 0.2, "max_tokens": 800, "timeout_s": 30}


def role(**fields: Any) -> RoleModelConfig:
    return RoleModelConfig.model_validate({**LIMITS, **fields})


def deepseek(**fields: Any) -> RoleModelConfig:
    return role(gateway="deepseek", model="deepseek-flash", **fields)


@pytest.mark.parametrize(
    "fields",
    [
        {"gateway": "deepseek", "model": "deepseek/deepseek-chat"},  # an OpenRouter slug on DeepSeek
        {"gateway": "deepseek", "model": "deepseek-chat"},  # not a current DeepSeek model name
        {"model": "deepseek-flash"},  # a DeepSeek name on OpenRouter
        {"gateway": "deepseek", "model": "deepseek-flash", "fallback_models": ["deepseek-v4-pro"]},
        {"gateway": "deepseek", "model": "deepseek-flash", "provider": {"only": ["DeepSeek"]}},
        {"gateway": "deepseek", "model": "deepseek-flash", "zdr": True},
        {"gateway": "deepseek", "model": "deepseek-flash", "max_price": {"prompt": 1, "completion": 2}},
        {"model": "deepseek/deepseek-chat", "thinking": "high"},  # thinking is DeepSeek only
    ],
)
def test_a_role_is_refused_outside_its_gateway_rules(fields: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        role(**fields)


def test_each_gateway_accepts_its_own_models() -> None:
    assert role(model="deepseek/deepseek-chat").gateway == "openrouter"
    assert deepseek(thinking="max").developer == "deepseek"
    assert role(gateway="deepseek", model="deepseek-v4-pro").model == "deepseek-v4-pro"


def test_judges_on_deepseek_may_share_the_developer_but_mixed_gateways_may_not() -> None:
    both = ModelsSection(roles={"news_judge_a": deepseek(), "news_judge_b": deepseek()})
    assert set(both.roles) == {"news_judge_a", "news_judge_b"}
    with pytest.raises(ValidationError, match="different developers"):
        ModelsSection(
            roles={"news_judge_a": deepseek(), "news_judge_b": role(model="deepseek/deepseek-chat")}
        )
    mixed = ModelsSection(
        roles={"news_judge_a": deepseek(), "news_judge_b": role(model="anthropic/claude-sonnet-4.5")}
    )
    assert mixed.roles["news_judge_b"].developer == "anthropic"


def test_the_deepseek_draft_moves_every_role_and_keeps_each_roles_limits() -> None:
    current = {
        "technical": role(
            model="anthropic/claude-sonnet-4.5",
            provider={"only": ["Anthropic"]},
            fallback_models=["openai/gpt-4o"],
            temperature=0.7,
            max_tokens=1500,
            timeout_s=45,
        )
    }
    draft = deepseek_profile(
        current, model="deepseek-flash", thinking="disabled", temperature=0.1, max_tokens=900, timeout_s=60
    )
    assert set(draft.roles) == set(get_args(LlmRole))
    assert {(c.gateway, c.model, c.fallback_models) for c in draft.roles.values()} == {
        ("deepseek", "deepseek-flash", ())
    }
    technical, macro = draft.roles["technical"], draft.roles["macro"]
    assert (technical.temperature, technical.max_tokens, technical.timeout_s) == (0.7, 1500, 45)
    assert (macro.temperature, macro.max_tokens, macro.timeout_s) == (0.1, 900, 60)


def test_openrouter_version_hashes_are_unchanged_and_deepseek_thinking_makes_a_new_version() -> None:
    legacy = role(model="anthropic/claude-sonnet-4.5", provider={"only": ["Anthropic"]})
    # Agent versions recorded before gateways hashed the provider object alone: they must keep matching.
    assert provider_prefs_hash(legacy) == canonical_sha256(legacy.provider_preferences())
    assert provider_prefs_hash(deepseek()) != provider_prefs_hash(deepseek(thinking="high"))


def test_a_stored_role_without_the_new_fields_is_not_a_change() -> None:
    legacy = {"model": "anthropic/claude-sonnet-4.5", **LIMITS}
    active = ConfigVersion(
        id=1,
        section="models",
        payload={"roles": {"technical": legacy}},
        payload_sha256="0" * 64,
        schema_version=1,
        author="owner",
        reason="first",
        created_at="2026-09-28T00:00:00Z",
        parent_id=None,
    )
    same = ModelsSection(roles={"technical": role(model="anthropic/claude-sonnet-4.5")})
    assert changed_roles(active, same) == {}
    moved = ModelsSection(roles={"technical": deepseek()})
    assert set(changed_roles(active, moved)) == {"technical"}
