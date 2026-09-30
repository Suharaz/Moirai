"""Risk ceilings and event pins are enforced independently of the console."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from hdt.configapi.sections import version_dict
from hdt.contracts.common import Account
from hdt.core.config import static_config
from hdt.settings.schemas import (
    ModelsSection,
    ModeSection,
    RiskSection,
    RoleModelConfig,
    Section,
    seed_payloads,
)
from hdt.settings.versions import ConfigVersion


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("leverage_max", 4),
        ("risk_pct", 0.0051),
        ("daily_loss_kill", -0.0201),
        ("max_positions", 5),
        ("same_direction_risk_max", 0.0151),
        ("max_margin_pct", 0.031),
        ("max_oi_frac", 0.0051),
        ("max_volume_1h_frac", 0.021),
        ("liq_distance_mult", 1.4),
        ("max_positions_per_narrative", 3),
        ("min_rr", 1.4),
        ("max_entry_distance_atr", 1.6),
        ("account_state_max_age_s", 31),
    ],
)
def test_risk_hard_ceilings_reject_direct_payloads(field: str, value: float) -> None:
    risk = seed_payloads(static_config())[Section.RISK].model_dump()
    with pytest.raises(ValidationError, match=field):
        RiskSection.model_validate({**risk, field: value})


def test_risk_payload_may_tighten_every_ceiling() -> None:
    risk = seed_payloads(static_config())[Section.RISK].model_dump()
    tight = {
        "max_margin_pct": 0.02,
        "max_oi_frac": 0.004,
        "max_volume_1h_frac": 0.01,
        "liq_distance_mult": 2.0,
        "max_positions_per_narrative": 1,
        "min_rr": 2.0,
        "max_entry_distance_atr": 1.0,
        "account_state_max_age_s": 20,
    }
    section = RiskSection.model_validate({**risk, **tight})
    assert {k: getattr(section, k) for k in tight} == tight


def test_a_stored_risk_payload_with_the_retired_decision_ttl_still_parses_without_it() -> None:
    """Immutable versions saved before 2026-09-28 carry `decision_ttl_s`: dropped, never a failure, never
    re-saved; an unknown key is still refused and every ceiling still applies."""
    risk = seed_payloads(static_config())[Section.RISK].model_dump()
    parsed = RiskSection.model_validate({**risk, "decision_ttl_s": 120})
    assert parsed.model_dump() == risk
    with pytest.raises(ValidationError, match="decision_ttl"):
        RiskSection.model_validate({**risk, "decision_ttl": 120})
    with pytest.raises(ValidationError, match="leverage_max"):
        RiskSection.model_validate({**risk, "decision_ttl_s": 120, "leverage_max": 5})


def test_the_config_api_never_shows_the_retired_decision_ttl_of_a_stored_version() -> None:
    risk = seed_payloads(static_config())[Section.RISK].model_dump()
    stored = ConfigVersion(
        id=8,
        section=Section.RISK,
        payload={**risk, "decision_ttl_s": 120},
        payload_sha256="0" * 64,
        schema_version=1,
        author="operator",
        reason="saved before 2026-09-28",
        created_at=datetime(2026, 9, 27, tzinfo=UTC),
        parent_id=None,
    )
    assert version_dict(stored)["payload"] == risk


def test_paper_size_multiplier_is_pinned_to_full_size() -> None:
    with pytest.raises(ValidationError, match="size_multiplier"):
        ModeSection(mode=Account.PAPER, size_multiplier=0.5)
    assert ModeSection(mode=Account.PAPER, size_multiplier=1.0).size_multiplier == 1.0
    assert ModeSection(mode=Account.TESTNET, size_multiplier=0.5).size_multiplier == 0.5
    with pytest.raises(ValidationError, match="size_multiplier"):
        ModeSection(mode=Account.LIVE, size_multiplier=1.5)


def _judge(model: str, *fallbacks: str) -> RoleModelConfig:
    return RoleModelConfig(
        model=model, fallback_models=fallbacks, temperature=0.0, max_tokens=800, timeout_s=30
    )


def test_news_judges_must_differ_in_developer_across_fallbacks() -> None:
    ok = ModelsSection(
        roles={
            "news_judge_a": _judge("openai/gpt-4o", "openai/gpt-4o-mini"),
            "news_judge_b": _judge("anthropic/claude-sonnet-4", "google/gemini-2.5-pro"),
        }
    )
    assert set(ok.roles) == {"news_judge_a", "news_judge_b"}
    with pytest.raises(ValidationError, match=r"across their fallbacks too \(shared \['openai'\]\)"):
        ModelsSection(
            roles={
                "news_judge_a": _judge("openai/gpt-4o"),
                "news_judge_b": _judge("anthropic/claude-sonnet-4", "openai/gpt-4o-mini"),
            }
        )


@pytest.mark.parametrize(
    "loosened",
    [
        {"allow_fallbacks": True},
        {"data_collection": "allow"},
        {"model": "anthropic/claude-sonnet-4:online"},
        {"model": "anthropic/claude-sonnet-4:nitro"},
        {"fallback_models": ["openai/gpt-4o:free"]},
    ],
    ids=["fallbacks", "data", "online", "nitro", "fallback-variant"],
)
def test_loosened_provider_preferences_and_variants_fail_config_validation(
    loosened: dict[str, object],
) -> None:
    """m3: these passed a save, then silently closed the role at run time (`llm_router.check_pinned`)."""
    base = {"model": "anthropic/claude-sonnet-4", "temperature": 0.0, "max_tokens": 800, "timeout_s": 30}
    RoleModelConfig.model_validate(base)
    with pytest.raises(ValidationError):
        ModelsSection.model_validate({"roles": {"technical": {**base, **loosened}}})
