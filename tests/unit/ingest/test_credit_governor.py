"""Credit governor: budget from the billing anchor, shed order context -> derivatives, halt states."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from hdt.ingest.credit_governor import CreditGovernor, KeyInfoError, parse_key_info

NOW = datetime(2026, 9, 27, 0, 30, tzinfo=UTC)


def _key_info(
    left: int = 100_000, used_today: int = 0, cycle_end: datetime | None = None
) -> dict[str, object]:
    end = cycle_end or NOW + timedelta(days=10)
    return {
        "plan": {
            "credit_limit_monthly": 450_000,
            "credit_limit_monthly_reset_timestamp": end.isoformat().replace("+00:00", "Z"),
            "credit_limit_daily_reset_timestamp": (NOW + timedelta(hours=23, minutes=30)).isoformat(),
            "rate_limit_minute": 600,
        },
        "usage": {
            "current_day": {"credits_used": used_today},
            "current_month": {"credits_used": 450_000 - left, "credits_left": left},
        },
    }


def _governor(**kw: object) -> CreditGovernor:
    gov = CreditGovernor(fallback_monthly_quota=450_000)
    gov.update_key_info(parse_key_info(_key_info(**kw), NOW))  # type: ignore[arg-type]
    return gov


def test_daily_budget_spreads_remaining_credits_until_the_reset() -> None:
    gov = _governor(left=100_000, cycle_end=NOW + timedelta(days=9, hours=12))
    assert gov.daily_budget == 10_000  # 10 days left (ceil)


def test_classes_are_shed_in_order_as_todays_use_grows() -> None:
    gov = _governor(left=100_000)  # budget 10_000 per day
    gov.record(7_100, NOW)
    assert not gov.admit("context", at=NOW).allowed
    assert gov.admit("dex", at=NOW).allowed
    gov.record(900, NOW)  # 8_000 used
    assert not gov.admit("attention", at=NOW).allowed
    assert gov.admit("price", at=NOW).allowed
    gov.record(1_500, NOW)  # 9_500 used
    assert not gov.admit("price", at=NOW).allowed
    assert gov.admit("derivatives", at=NOW).allowed
    assert gov.degraded
    gov.record(600, NOW)  # over budget
    assert not gov.admit("derivatives", at=NOW).allowed
    assert gov.admit("system", at=NOW).allowed


def test_new_utc_day_resets_use_and_shedding() -> None:
    gov = _governor(left=100_000)
    gov.record(9_999, NOW)
    assert not gov.admit("context", at=NOW).allowed
    tomorrow = NOW + timedelta(days=1)
    assert gov.admit("context", at=tomorrow).allowed
    assert gov.used_today == 0


def test_cmc_day_counter_raises_metered_use() -> None:
    gov = _governor(left=100_000, used_today=8_500)
    assert not gov.admit("attention", at=NOW).allowed


def test_monthly_cap_halts_every_class_until_the_cycle_resets() -> None:
    end = NOW + timedelta(days=3)
    gov = _governor(cycle_end=end)
    assert gov.halt("monthly_cap", NOW) == end
    assert not gov.admit("derivatives", at=NOW + timedelta(days=2)).allowed
    assert not gov.keyed_fallback_allowed("derivatives")
    assert gov.admit("derivatives", at=end + timedelta(seconds=1)).allowed


def test_ip_limit_is_a_short_cooldown() -> None:
    gov = _governor()
    gov.halt("ip_limit", NOW)
    assert not gov.admit("system", at=NOW + timedelta(minutes=1)).allowed
    assert gov.admit("system", at=NOW + timedelta(minutes=6)).allowed


def test_without_key_info_the_design_quota_is_used() -> None:
    gov = CreditGovernor(fallback_monthly_quota=450_000)
    assert gov.admit("derivatives", at=NOW).allowed
    assert gov.daily_budget == 15_000


def test_malformed_key_info_is_rejected() -> None:
    with pytest.raises(KeyInfoError):
        parse_key_info({"plan": {}}, NOW)
