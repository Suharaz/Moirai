"""Credit meter projection, plan refusals kept out of usage, WebSocket credit accrual and the pilot report's
proposals."""

from __future__ import annotations

import importlib.util
import json
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from hdt.core.config import static_config
from hdt.ingest.cmc_client import CmcResult
from hdt.ingest.cmc_ws import CreditAccumulator
from hdt.ingest.credit_governor import CreditGovernor, KeyInfo
from hdt.ingest.credit_meter import CreditMeter, project_cycle
from hdt.lake.schemas import Capture

NOW = datetime(2026, 9, 27, 12, 0, tzinfo=UTC)
ROOT = Path(__file__).resolve().parents[3]


def _info(used: int, limit: int = 450_000, days_left: float = 10) -> KeyInfo:
    return KeyInfo(
        checked_at=NOW,
        credit_limit_cycle=limit,
        credits_used_cycle=used,
        credits_left_cycle=limit - used,
        cycle_end=NOW + timedelta(days=days_left),
        credits_used_today=0,
        rate_limit_minute=600,
        daily_reset=None,
    )


def test_projection_adds_the_recent_run_rate_for_the_days_left() -> None:
    p = project_cycle(_info(300_000), [12_000, 14_000], NOW)
    assert p.run_rate_per_day == 13_000
    assert p.projected == 300_000 + 13_000 * 10
    assert p.fraction > 0.85  # the meter would raise the alert


def test_projection_without_history_is_the_cycle_use() -> None:
    assert project_cycle(_info(100_000), [], NOW).projected == 100_000


class _Usage:
    """Session factory stand-in: records every `cmc_credit_usage` upsert the meter executes."""

    def __init__(self) -> None:
        self.statements: list[Any] = []

    @contextmanager
    def begin(self) -> Iterator[_Usage]:
        yield self

    def execute(self, statement: Any) -> None:
        self.statements.append(statement)


def _result(route: str, http_status: int, code: int, credits: int) -> CmcResult:
    body = json.dumps({"status": {"error_code": code, "credit_count": credits}}).encode()
    capture = Capture(
        source="cmc", route=route, fetched_at=NOW, http_status=http_status, body=body, params={}
    )
    return CmcResult(
        route=route,
        keyless=False,
        capture=capture,
        http_status=http_status,
        error_code=code,
        error_message=None,
        credit_count=credits,
        status_ts=None,
        data=None,
    )


async def test_plan_refusals_are_not_usage_but_charged_responses_are() -> None:
    governor = CreditGovernor(fallback_monthly_quota=450_000)
    usage = _Usage()
    meter = CreditMeter(governor=governor, sessions=usage, alert_fraction=0.85)  # type: ignore[arg-type]
    await meter.on_response(_result("trending_latest", 403, 1006, 0))
    assert usage.statements == []
    assert governor.used_today == 0
    await meter.on_response(_result("quotes_latest", 200, 0, 3))
    assert len(usage.statements) == 1
    assert governor.used_today == 3


def test_websocket_credits_accrue_in_whole_credits() -> None:
    acc = CreditAccumulator()
    due = [acc.add() for _ in range(120)]  # 0.025 credit per message
    assert sum(due) == 3
    assert due.index(1) == 39  # the 40th message completes the first credit


def _pilot() -> object:
    spec = importlib.util.spec_from_file_location(
        "pilot_credit_report", ROOT / "scripts" / "pilot_credit_report.py"
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["pilot_credit_report"] = module
    spec.loader.exec_module(module)
    return module


def test_pilot_report_projects_a_month_and_proposes_cuts_in_the_decided_order() -> None:
    pilot = _pilot()
    start = date(2026, 9, 20)
    usage = [(start + timedelta(days=d), "exchange_derivative_market_pairs", 6_000) for d in range(7)]
    usage += [(start + timedelta(days=d), "crypto_derivative_market_pairs", 5_000) for d in range(7)]
    report = pilot.build_report(  # type: ignore[attr-defined]
        usage,
        static_config().cmc_routes,
        start=start,
        end=start + timedelta(days=7),
        quota=450_000,
        pilot_target_frac=0.80,
        key_info=None,
    )
    assert report.days_metered == 7
    assert report.projected_month == (6_000 + 5_000) * 30
    assert report.projected_fraction < 0.80
    assert report.proposals == ()
    heavy = [(start + timedelta(days=d), "exchange_derivative_market_pairs", 12_000) for d in range(7)]
    over = pilot.build_report(  # type: ignore[attr-defined]
        heavy + usage[7:],
        static_config().cmc_routes,
        start=start,
        end=start + timedelta(days=7),
        quota=450_000,
        pilot_target_frac=0.80,
        key_info=None,
    )
    assert over.projected_fraction > 0.80
    assert over.proposals[0].startswith("route #2")
