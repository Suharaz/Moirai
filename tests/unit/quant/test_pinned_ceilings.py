"""Quant core and scanner run a stored risk or mode version outside the hard ceilings clipped, as Risk and
execution do (review cycle 3 m-c): re-validating such a version raised on every packet and every scan."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy as sa

from hdt.contracts.common import Account
from hdt.core.config import static_config
from hdt.features.levels import LevelRules
from hdt.lake.pit_query import PitQuery
from hdt.quant import quant_core, scanner
from hdt.quant.quant_core import QuantCore
from hdt.quant.scanner import Scanner
from hdt.risk.pinned import pinned_config
from hdt.settings.ceilings import LEVERAGE_MAX
from hdt.settings.schemas import Section, seed_payloads
from hdt.settings.versions import ConfigPin, ConfigVersion

STATIC = static_config()
T0 = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)


def _stale_pin() -> ConfigPin:
    """Every section seeded from the YAML files; risk and mode saved before the ceilings were tightened."""
    payloads: dict[Section, dict[str, Any]] = {
        section: model.model_dump(mode="json") for section, model in seed_payloads(STATIC).items()
    }
    payloads[Section.RISK] |= {"leverage_max": LEVERAGE_MAX + 2}
    payloads[Section.MODE] = {"mode": "live", "size_multiplier": 2.0}
    return ConfigPin(
        {
            section: ConfigVersion(
                id=100 + number,
                section=section,
                payload=payload,
                payload_sha256="0" * 64,
                schema_version=1,
                author="test",
                reason="test",
                created_at=T0,
                parent_id=None,
            )
            for number, (section, payload) in enumerate(payloads.items())
        }
    )


def _pit(tmp_path: Path) -> PitQuery:
    return PitQuery(tmp_path / "staging", tmp_path / "lake")


def test_quant_core_levels_use_the_clipped_risk_of_a_stale_pin(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    pin = _stale_pin()
    monkeypatch.setattr(quant_core, "load_pin", lambda _session, _ids: pin)
    core = QuantCore(_pit(tmp_path), sa.create_engine("sqlite://"), static=STATIC)
    with caplog.at_level(logging.WARNING, logger=quant_core.__name__):
        found = core._pinned(pin.version_ids())
    risk = pinned_config(pin, STATIC).risk
    assert risk.leverage_max == LEVERAGE_MAX
    assert found.rules == LevelRules(risk.min_rr, risk.max_entry_distance_atr)
    assert any("running clipped" in r.getMessage() for r in caplog.records)


async def test_scanner_runs_on_the_clipped_mode_of_a_stale_pin(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    pin = _stale_pin()
    assert pinned_config(pin, STATIC).mode is Account.LIVE
    monkeypatch.setattr(scanner, "pin_current", lambda _session: pin)
    scan = Scanner(_pit(tmp_path), sa.create_engine("sqlite://"), redis=None, static=STATIC)  # type: ignore[arg-type]
    with caplog.at_level(logging.WARNING, logger=scanner.__name__):
        result = await scan.run(T0)
    assert result.skipped  # the empty lake has no universe: the scan got past the config read
    assert any("running clipped" in r.getMessage() for r in caplog.records)
