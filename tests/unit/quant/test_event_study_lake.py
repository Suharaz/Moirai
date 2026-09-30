"""Gate G1 event study on a lake (`hdt.quant.study`), end to end on the synthetic study lake.

The study lake (`build_study` in `tests/fixtures/synthetic_lake.py`) plants strict LTX LONG triggers for
SXXX in the slots S, S+5m, S+10m and S+12h05m and for SYYY in S+10m, over 10 tradable coins and the
scanner grid of [S, S + 12h15m). Only the first four slots are 12 h mature.
"""

from __future__ import annotations

import json
import statistics
from datetime import datetime, timedelta

import pytest

from fixtures.synthetic_lake import (
    BTC,
    FAR_FUTURE,
    STUDY_COINS,
    STUDY_END,
    SX,
    SY,
    Coin,
    S,
    SyntheticLake,
    build_study,
    study_market,
)
from hdt.core.config import scanner_config, static_config
from hdt.features.lake_io import LakeView
from hdt.quant.study import EventStudy

MARKET = study_market()
MARK_OFFSET = timedelta(seconds=10)
"""1 s mark events are written every 5 minutes, 10 s before each scanner slot."""
MATURE_SLOTS = (S, S + timedelta(minutes=5), S + timedelta(minutes=10), S + timedelta(minutes=15))
TRADABLE = tuple(c for c in STUDY_COINS if c is not BTC)
GRID_SLOTS = 147
"""Slots in [S, S + 12h15m) at the 5 minute cadence."""


def _run(lake: SyntheticLake) -> dict[str, object]:
    view = LakeView(lake.pit, clock=lambda: FAR_FUTURE)
    return EventStudy(lake.pit, static_config(), scanner_config(), view=view).run(S, STUDY_END).to_json()


@pytest.fixture(scope="module")
def study_lake(tmp_path_factory: pytest.TempPathFactory) -> SyntheticLake:
    return build_study(tmp_path_factory.mktemp("study_lake"))


@pytest.fixture(scope="module")
def report(study_lake: SyntheticLake) -> dict[str, object]:
    return _run(study_lake)


def _ret(coin: Coin, t: datetime, hours: int) -> float:
    return (
        MARKET.mark(coin, t + timedelta(hours=hours) - MARK_OFFSET) / MARKET.mark(coin, t - MARK_OFFSET) - 1
    )


def test_one_event_per_coin_per_12h_window(report: dict[str, object]) -> None:
    assert report["slots"] == GRID_SLOTS
    assert report["slots_without_universe"] == 0
    events = report["events"]
    assert isinstance(events, list)
    assert [(e["coin_id"], e["rule"], e["side"], e["as_of"]) for e in events] == [
        (SX.cmc_id, "LTX", "LONG", S.isoformat()),
        (SY.cmc_id, "LTX", "LONG", (S + timedelta(minutes=10)).isoformat()),
        (SX.cmc_id, "LTX", "LONG", (S + timedelta(hours=12, minutes=5)).isoformat()),
    ]
    frequency = report["frequency"]
    assert isinstance(frequency, dict)
    assert frequency["strict"]["LTX"]["per_coin"] == {str(SX.cmc_id): 2, str(SY.cmc_id): 1}
    assert frequency["strict"]["MIGRATION"]["events"] == 0


def test_baseline_counts_every_non_trigger_slot(report: dict[str, object]) -> None:
    baseline = report["baseline"]
    assert isinstance(baseline, dict)
    per_rule = len(TRADABLE) * GRID_SLOTS
    mature = len(TRADABLE) * len(MATURE_SLOTS)
    # LTX: the four strict triggers inside the mature slots (SXXX x3, SYYY x1) and the immature one
    # (SXXX at S+12h05m) are events, not baseline; every other (slot, coin) is counted or immature.
    ltx = baseline["LTX"]
    assert ltx["n"] == mature - 4
    assert ltx["missing_immature"] == per_rule - mature - 1
    assert ltx["n"] + ltx["missing_immature"] + 5 == per_rule
    # MIGRATION never triggers: every mature (slot, coin) is in the baseline, with the gross 12 h return.
    values = [_ret(coin, t, 12) for t in MATURE_SLOTS for coin in TRADABLE]
    migration = baseline["MIGRATION"]
    assert migration["n"] == mature
    assert migration["missing_immature"] == per_rule - mature
    assert migration["mean"] == pytest.approx(statistics.fmean(values), rel=1e-6)
    assert migration["sigma"] == pytest.approx(statistics.stdev(values), rel=1e-6)


def test_labels_past_the_end_are_immature(report: dict[str, object]) -> None:
    events = report["events"]
    assert isinstance(events, list)
    first, second, late = events
    assert first["missing"] == {"24": "immature"}
    assert second["missing"] == {"24": "immature"}
    assert late["missing"] == {h: "immature" for h in ("4", "8", "12", "24")}
    assert all(late["net"][h] is None for h in ("4", "8", "12", "24"))
    # The mature horizons are the LONG residual 12 h return net of the round-trip cost.
    for h in (4, 8, 12):
        resid = _ret(SX, S, h) - first["beta_btc"] * _ret(BTC, S, h)
        assert first["net"][str(h)] == pytest.approx(resid - first["cost"], rel=1e-6, abs=1e-9)


def test_rerun_gives_identical_json(study_lake: SyntheticLake, report: dict[str, object]) -> None:
    again = _run(study_lake)
    assert json.dumps(again, sort_keys=True, default=str) == json.dumps(report, sort_keys=True, default=str)
