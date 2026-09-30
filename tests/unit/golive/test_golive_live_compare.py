"""Phase 11 paper vs live: slippage direction, minimum samples and the size-step verdict."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from hdt.golive.live_compare import EntryStats, StopFill, Tolerances, adverse_bp, compare

SINCE = datetime(2026, 12, 1, tzinfo=UTC)
TOL = Tolerances(slippage_bp=2.0)


def test_adverse_bp_is_against_the_trader() -> None:
    assert adverse_bp("BUY", 100.0, 100.05) == pytest.approx(5.0)
    assert adverse_bp("SELL", 100.0, 100.05) == pytest.approx(-5.0)
    assert adverse_bp("SELL", 100.0, 99.97) == pytest.approx(3.0)
    with pytest.raises(ValueError, match="reference price"):
        adverse_bp("BUY", 0.0, 1.0)


def _stats(n: int, filled: int, *, price: float, side: str = "BUY") -> EntryStats:
    ids = [f"e{i}" for i in range(n)]
    return EntryStats(set(ids), {e: (price, side) for e in ids[:filled]})


def _stops(n: int, fill: float) -> list[StopFill]:
    return [StopFill(f"e{i}", "SELL", 100.0, fill) for i in range(n)]


def test_within_tolerance_allows_the_next_step() -> None:
    result = compare(
        _stats(30, 27, price=100.0),
        _stats(30, 26, price=100.01),
        _stops(12, 99.97),
        TOL,
        since=SINCE,
        size_multiplier=0.25,
    )
    by_key = {m.key: m for m in result.metrics}
    assert by_key["fill_rate"].live == pytest.approx(26 / 30)
    assert by_key["entry_price_bp"].live == pytest.approx(1.0)
    assert by_key["entry_price_bp"].limit == 2.0
    assert by_key["stop_slippage_bp"].live == pytest.approx(3.0)
    assert all(m.within for m in result.metrics)
    assert result.verdict == "ok_to_step"


def test_slippage_above_twice_the_assumption_holds_the_size() -> None:
    result = compare(
        _stats(30, 27, price=100.0),
        _stats(30, 27, price=100.0),
        _stops(12, 99.94),
        TOL,
        since=SINCE,
        size_multiplier=0.25,
    )
    stop = next(m for m in result.metrics if m.key == "stop_slippage_bp")
    assert stop.live == pytest.approx(6.0)
    assert stop.limit == 4.0
    assert stop.within is False
    assert result.verdict == "hold_size"


def test_entry_limit_counts_the_slippage_already_in_the_paper_fill() -> None:
    # the paper fill is 2 bp worse than the book; live 3 bp worse than paper is 5 bp worse than the book,
    # beyond twice the assumption
    result = compare(
        _stats(30, 27, price=100.0),
        _stats(30, 27, price=100.03),
        _stops(12, 100.0),
        TOL,
        since=SINCE,
        size_multiplier=0.25,
    )
    entry = next(m for m in result.metrics if m.key == "entry_price_bp")
    assert entry.live == pytest.approx(3.0)
    assert entry.limit == 2.0
    assert entry.within is False
    assert result.verdict == "hold_size"


def test_fill_rate_gap_holds_the_size() -> None:
    result = compare(
        _stats(30, 29, price=100.0),
        _stats(30, 20, price=100.0),
        _stops(12, 100.0),
        TOL,
        since=SINCE,
        size_multiplier=0.5,
    )
    assert next(m for m in result.metrics if m.key == "fill_rate").within is False
    assert result.verdict == "hold_size"


def test_too_few_samples_is_insufficient_not_ok() -> None:
    result = compare(
        _stats(10, 10, price=100.0),
        _stats(10, 10, price=100.0),
        _stops(3, 100.0),
        TOL,
        since=SINCE,
        size_multiplier=0.25,
    )
    assert all(m.within is None for m in result.metrics)
    assert result.verdict == "insufficient_data"
    assert result.to_json()["metrics"][0]["measured"] is False
