"""Stored config versions outside the hard ceilings run clipped, the service keeps running (review m5)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from hdt.contracts.common import Account
from hdt.core.config import static_config
from hdt.risk.pinned import pinned_config
from hdt.settings.ceilings import LEVERAGE_MAX, RISK_PCT_MAX, SIZE_MULTIPLIER_MAX
from hdt.settings.schemas import BINANCE_FIELDS, BINANCE_RISK_FIELDS, RISK_FIELDS, Section
from hdt.settings.versions import ConfigPin, ConfigVersion

STATIC = static_config()


def _version(section: Section, version_id: int, payload: dict[str, Any]) -> ConfigVersion:
    return ConfigVersion(
        id=version_id,
        section=section,
        payload=payload,
        payload_sha256="0" * 64,
        schema_version=1,
        author="test",
        reason="test",
        created_at=datetime(2026, 9, 1, tzinfo=UTC),
        parent_id=None,
    )


def _pin(*, risk: dict[str, Any] | None = None, mode: dict[str, Any] | None = None) -> ConfigPin:
    risk_payload = {**STATIC.risk.model_dump(include=set(RISK_FIELDS)), **(risk or {})}
    binance_payload = {
        **STATIC.binance.model_dump(include=set(BINANCE_FIELDS)),
        **STATIC.risk.model_dump(include=set(BINANCE_RISK_FIELDS)),
    }
    return ConfigPin(
        {
            Section.RISK: _version(Section.RISK, 11, risk_payload),
            Section.BINANCE: _version(Section.BINANCE, 12, binance_payload),
            Section.MODE: _version(Section.MODE, 13, mode or {"mode": "live", "size_multiplier": 0.25}),
        }
    )


def test_a_valid_pin_runs_unchanged_with_no_problem() -> None:
    pinned = pinned_config(_pin(risk={"risk_pct": 0.004}), STATIC)
    assert (pinned.risk.risk_pct, pinned.mode, pinned.problems) == (0.004, Account.LIVE, ())
    assert pinned.risk.size_multiplier["live"] == 0.25


def test_a_risk_version_above_the_ceilings_is_clipped_and_reported() -> None:
    # Saved before the ceilings were tightened: re-validation now fails on every read.
    pinned = pinned_config(_pin(risk={"leverage_max": 5, "risk_pct": 0.01, "max_positions": 2}), STATIC)
    assert (pinned.risk.leverage_max, pinned.risk.risk_pct) == (LEVERAGE_MAX, RISK_PCT_MAX)
    assert pinned.risk.max_positions == 2  # tighter values are kept
    assert [(p.section, p.version_id, p.episode) for p in pinned.problems] == [(Section.RISK, 11, "risk:11")]
    assert "leverage_max" in pinned.problems[0].error


def test_a_mode_multiplier_above_the_ceiling_is_clipped_and_reported() -> None:
    live = pinned_config(_pin(mode={"mode": "live", "size_multiplier": 2.0}), STATIC)
    assert (live.mode, live.risk.size_multiplier["live"]) == (Account.LIVE, SIZE_MULTIPLIER_MAX)
    assert [p.episode for p in live.problems] == ["mode:13"]
    paper = pinned_config(_pin(mode={"mode": "paper", "size_multiplier": 0.5}), STATIC)
    assert (paper.mode, paper.risk.size_multiplier["paper"]) == (Account.PAPER, 1.0)
    assert [p.episode for p in paper.problems] == ["mode:13"]


def test_a_risk_version_clipping_cannot_repair_falls_back_to_the_static_values() -> None:
    pinned = pinned_config(_pin(risk={"leverage_max": 0}), STATIC)
    assert pinned.risk.leverage_max == STATIC.risk.leverage_max
    assert [p.episode for p in pinned.problems] == ["risk:11"]


def test_unconfigured_sections_keep_the_static_file_and_paper() -> None:
    pinned = pinned_config(ConfigPin({}), STATIC)
    assert (pinned.risk, pinned.mode, pinned.problems) == (STATIC.risk, Account.PAPER, ())
