from __future__ import annotations

import shutil
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from hdt.core.config import CONFIG_DIR, BootstrapSettings, load_static_config, read_env_value
from hdt.settings.ceilings import CeilingViolationError
from hdt.vault.redact import redact


@pytest.fixture
def config_copy(tmp_path: Path) -> Path:
    target = tmp_path / "config"
    shutil.copytree(CONFIG_DIR, target)
    return target


def _patch(config_dir: Path, name: str, **changes: object) -> None:
    path = config_dir / name
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    data.update(changes)
    path.write_text(yaml.safe_dump(data), encoding="utf-8")


def test_repository_defaults_load() -> None:
    cfg = load_static_config()
    assert cfg.council.quorum == 4
    assert cfg.council.stance_long == 0.55
    assert cfg.council.stance_short == 0.45
    assert cfg.council.manager.d_threshold == 0.8
    assert cfg.settings.data.stale_s == 180
    assert len(cfg.cmc_routes.routes) == 30
    assert cfg.cmc_routes.by_id(10).consumers == ("technical",)
    assert cfg.cmc_routes.by_id(28).consumers == ("macro",)


@pytest.mark.parametrize(
    "changes",
    [
        {"leverage_max": 4},
        {"risk_pct": 0.006},
        {"max_positions": 5},
        {"max_positions_per_coin": 2},
        {"same_direction_risk_max": 0.02},
        {"daily_loss_kill": -0.03},
        {"size_multiplier": {"paper": 1.0, "testnet": 1.0, "live": 1.5}},
        {"size_multiplier": {"paper": 0.5, "testnet": 1.0, "live": 0.25}},
        {"max_margin_pct": 0.05},
        {"max_oi_frac": 0.01},
        {"max_volume_1h_frac": 0.05},
        {"liq_distance_mult": 1.2},
        {"max_positions_per_narrative": 3},
        {"min_rr": 1.2},
        {"max_entry_distance_atr": 2.0},
        {"account_state_max_age_s": 60},
    ],
)
def test_risk_defaults_cannot_loosen_ceilings(config_copy: Path, changes: dict[str, object]) -> None:
    _patch(config_copy, "risk.yaml", **changes)
    with pytest.raises((CeilingViolationError, ValidationError)):
        load_static_config(config_copy)


def test_risk_defaults_may_tighten(config_copy: Path) -> None:
    _patch(config_copy, "risk.yaml", leverage_max=2, risk_pct=0.0025, daily_loss_kill=-0.01, max_positions=2)
    cfg = load_static_config(config_copy)
    assert cfg.risk.leverage_max == 2
    assert cfg.risk.daily_loss_kill == -0.01


def test_enabled_route_without_consumer_is_rejected(config_copy: Path) -> None:
    path = config_copy / "cmc_routes.yaml"
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    data["routes"][0]["consumers"] = []
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    with pytest.raises(ValidationError, match="named consumer"):
        load_static_config(config_copy)


def test_route_catalog_must_be_complete(config_copy: Path) -> None:
    path = config_copy / "cmc_routes.yaml"
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    data["routes"] = data["routes"][:-1]
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    with pytest.raises(ValidationError, match=r"routes 1\.\.30"):
        load_static_config(config_copy)


def test_unknown_config_key_is_rejected(config_copy: Path) -> None:
    _patch(config_copy, "council.yaml", quorom=3)
    with pytest.raises(ValidationError):
        load_static_config(config_copy)


def test_env_value_from_file_and_empty(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    secret_file = tmp_path / "dsn"
    secret_file.write_text("postgresql://x\n", encoding="utf-8")
    monkeypatch.delenv("HDT_UNIT_VALUE", raising=False)
    monkeypatch.setenv("HDT_UNIT_VALUE_FILE", str(secret_file))
    assert read_env_value("HDT_UNIT_VALUE", dotenv=None) == "postgresql://x"
    monkeypatch.setenv("HDT_UNIT_VALUE_FILE", str(tmp_path / "missing"))
    assert read_env_value("HDT_UNIT_VALUE", dotenv=None) is None
    monkeypatch.setenv("HDT_UNIT_VALUE", "direct")
    assert read_env_value("HDT_UNIT_VALUE", dotenv=None) == "direct"


def test_bootstrap_dsn_password_is_registered_for_redaction(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HDT_UNIT_DSN", "postgresql://hdt_risk:Xk29fLq81Zp@127.0.0.1:5432/hdt")
    assert read_env_value("HDT_UNIT_DSN", dotenv=None) is not None
    assert "Xk29fLq81Zp" not in redact("password echoed by a driver error: Xk29fLq81Zp")


def test_bootstrap_settings_hide_secrets_and_read_secret_files(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    secret_file = tmp_path / "dsn"
    secret_file.write_text("postgresql://hdt_council:Hv73kPq20Ln@127.0.0.1:5432/hdt\n", encoding="utf-8")
    monkeypatch.delenv("HDT_PG_DSN", raising=False)
    monkeypatch.setenv("HDT_PG_DSN_FILE", str(secret_file))
    settings = BootstrapSettings()
    assert settings.pg_dsn is not None
    assert settings.pg_dsn.get_secret_value().endswith("/hdt")
    assert "Hv73kPq20Ln" not in repr(settings)
