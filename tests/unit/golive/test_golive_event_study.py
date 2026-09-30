"""Phase 11 shadow event study: cost and funding netting per horizon, missing data, grouping."""

from __future__ import annotations

import importlib.util
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType

import pytest

from hdt.golive.event_study import Costs, ShadowSignal, SignalRow, net_row, run, summarize

ROOT = Path(__file__).resolve().parents[3]

T0 = datetime(2026, 10, 1, 1, tzinfo=UTC)
COSTS = Costs(taker=0.0005, slippage_bp=2.0)
LEG = 2 * 0.0005 + 2 * 2.0 / 10_000  # 0.0014
MS_H = 3_600_000


class FakeMarket:
    """Label returns by horizon; funding every 8 h at a fixed rate with the settlement mark."""

    def __init__(self, labels: dict[int, float], rates: dict[str, float], marks: dict[str, float]) -> None:
        self.labels = labels
        self.rates = rates
        self.marks = marks

    def label(self, sig: ShadowSignal, horizon_h: int) -> float | None:
        return self.labels.get(horizon_h)

    def mark(self, symbol: str, t: datetime) -> float | None:
        return self.marks.get(symbol)

    def funding(self, symbol: str, start: datetime, end: datetime):  # type: ignore[no-untyped-def]
        from hdt.quant.labels import Settlement

        if symbol not in self.rates:
            return None
        t0 = int(start.timestamp() * 1000)
        hours = int((end - start).total_seconds() // 3600)
        return [
            Settlement(t0 + h * MS_H, self.rates[symbol], self.marks[symbol] * 1.1)
            for h in range(1, hours + 1)
            if (h + 7) % 8 == 0  # settlements at +1 h, +9 h, +17 h
        ]


def _signal(**over: object) -> ShadowSignal:
    base: dict[str, object] = {
        "coin_id": 1,
        "as_of": T0,
        "source": "LTX",
        "target_type": "RAW_12H",
        "side": "LONG",
        "symbol": "ABCUSDT",
        "btc_symbol": "BTCUSDT",
        "beta_btc": 1.2,
        "event_id": "e1",
    }
    base.update(over)
    return ShadowSignal(**base)  # type: ignore[arg-type]


def test_long_raw_nets_fee_slippage_and_funding() -> None:
    market = FakeMarket({4: 0.02, 12: 0.03}, {"ABCUSDT": 0.001}, {"ABCUSDT": 10.0})
    row = net_row(_signal(), market, COSTS, end=T0 + timedelta(days=2), horizons=(4, 12))
    # 4 h: one settlement (+1 h) at mark 11 on entry 10: a long pays 0.001 x 1.1
    assert row.funding["4"] == pytest.approx(0.0011)
    assert row.net["4"] == pytest.approx(0.02 - LEG - 0.0011)
    # 12 h: two settlements (+1 h, +9 h)
    assert row.net["12"] == pytest.approx(0.03 - LEG - 0.0022)
    assert row.cost == pytest.approx(LEG)


def test_short_receives_positive_funding() -> None:
    market = FakeMarket({12: -0.03}, {"ABCUSDT": 0.001}, {"ABCUSDT": 10.0})
    row = net_row(_signal(side="SHORT"), market, COSTS, end=T0 + timedelta(days=2), horizons=(12,))
    assert row.gross["12"] == pytest.approx(0.03)
    assert row.funding["12"] == pytest.approx(-0.0022)
    assert row.net["12"] == pytest.approx(0.03 - LEG + 0.0022)


def test_resid_nets_the_btc_hedge_leg_and_its_funding() -> None:
    market = FakeMarket(
        {12: 0.01}, {"ABCUSDT": 0.001, "BTCUSDT": 0.0005}, {"ABCUSDT": 10.0, "BTCUSDT": 60_000.0}
    )
    row = net_row(_signal(target_type="RESID_12H"), market, COSTS, end=T0 + timedelta(days=2), horizons=(12,))
    # the long coin pays 0.0022, the short 1.2 BTC hedge receives 1.2 x 0.0011
    assert row.cost == pytest.approx(LEG * 2.2)
    assert row.funding["12"] == pytest.approx(0.0022 - 1.2 * 0.0011)
    assert row.net["12"] == pytest.approx(0.01 - LEG * 2.2 - (0.0022 - 1.2 * 0.0011))


def test_missing_inputs_leave_the_value_missing_with_a_reason() -> None:
    end = T0 + timedelta(hours=10)
    full = FakeMarket({4: 0.01, 12: 0.01}, {"ABCUSDT": 0.0}, {"ABCUSDT": 10.0})
    row = net_row(_signal(), full, COSTS, end=end, horizons=(4, 12))
    assert row.net["4"] is not None
    assert row.missing == {"12": "immature"}
    no_funding = FakeMarket({4: 0.01}, {}, {"ABCUSDT": 10.0})
    assert net_row(_signal(), no_funding, COSTS, end=end, horizons=(4,)).missing == {"4": "no_funding"}
    no_mark = FakeMarket({}, {"ABCUSDT": 0.0}, {"ABCUSDT": 10.0})
    assert net_row(_signal(), no_mark, COSTS, end=end, horizons=(4,)).missing == {"4": "no_mark"}
    no_beta = _signal(target_type="RESID_12H", beta_btc=None)
    assert net_row(no_beta, full, COSTS, end=end, horizons=(4,)).missing == {"4": "no_beta"}


def test_groups_split_by_source_and_target_and_decide_on_12h() -> None:
    market = FakeMarket({4: 0.0, 8: 0.0, 12: 0.02, 24: 0.0}, {"ABCUSDT": 0.0}, {"ABCUSDT": 10.0})
    signals = [_signal(as_of=T0 + timedelta(days=i), event_id=f"l{i}") for i in range(12)] + [
        _signal(as_of=T0 + timedelta(days=i), event_id=f"m{i}", source="MIGRATION") for i in range(5)
    ]
    rows, groups = run(signals, market, COSTS, end=T0 + timedelta(days=40))
    assert len(rows) == 17
    by_key = {(g.source, g.target_type): g for g in groups}
    ltx = by_key[("LTX", "RAW_12H")]
    assert ltx.n_signals == 12
    h12 = ltx.horizons["12"]
    assert h12 is not None
    assert h12.mean == pytest.approx(0.02 - LEG)
    assert ltx.state == "edge"
    # 4 / 8 / 24 h lose exactly the cost: every CI is below 0, so the group is not a replan signal
    assert ltx.horizons["4"] is not None
    assert ltx.horizons["4"].ci_high < 0
    assert ltx.replan_signal is False
    assert by_key[("MIGRATION", "RAW_12H")].n_signals == 5


def test_summary_reports_dropped_reasons_and_insufficient_data() -> None:
    market = FakeMarket({}, {"ABCUSDT": 0.0}, {"ABCUSDT": 10.0})
    rows = [net_row(_signal(), market, COSTS, end=T0 + timedelta(days=2), horizons=(12,))]
    (group,) = summarize(rows, (12,))
    assert group.state == "insufficient_data"
    assert group.dropped == {"no_mark": 1}
    assert replace(group, horizons={}).replan_signal is False


def _rows(n: int, *, h12: tuple[float, float], h24: float) -> list[SignalRow]:
    """`n` LTX signals, one per day; the 12 h net alternates between `h12`, the 24 h net is fixed."""
    return [
        SignalRow(
            _signal(as_of=T0 + timedelta(days=i), event_id=f"e{i}"),
            LEG,
            net={"4": 0.0, "8": 0.0, "12": h12[i % 2], "24": h24},
        )
        for i in range(n)
    ]


def test_replan_is_decided_by_the_12h_horizon_alone() -> None:
    # 12 h straddles 0 with enough signals, 24 h is clearly positive: the preregistered horizon decides
    (group,) = summarize(_rows(160, h12=(0.011, -0.01), h24=0.05))
    h12 = group.horizons["12"]
    assert h12 is not None
    assert h12.ci_low <= 0 <= h12.ci_high
    assert group.horizons["24"] is not None
    assert group.horizons["24"].ci_low > 0
    assert group.delta == pytest.approx(2 * LEG)
    assert group.n_min is not None
    assert h12.n >= group.n_min
    assert group.decision == "replan"
    assert group.replan_signal is True
    assert group.to_json()["underpowered"] is False


def test_too_few_12h_values_is_underpowered_not_a_replan() -> None:
    (group,) = summarize(_rows(20, h12=(0.011, -0.01), h24=0.0))
    assert group.n_min is not None
    assert group.n_min > 20
    assert group.decision == "underpowered"
    assert group.underpowered is True
    assert group.replan_signal is False


def test_a_12h_edge_with_enough_power_is_no_replan_whatever_the_other_horizons() -> None:
    (group,) = summarize(_rows(160, h12=(0.03, 0.02), h24=-0.05))
    assert group.decision == "edge"
    assert group.replan_signal is False


def _event_study_script() -> ModuleType:
    spec = importlib.util.spec_from_file_location("event_study_script", ROOT / "scripts" / "event_study.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_exit_code_separates_replan_no_data_and_underpowered() -> None:
    exit_code = _event_study_script().exit_code

    def study(*rows: list[SignalRow]) -> dict[str, object]:
        return {"groups": [g.to_json() for r in rows for g in summarize(r)]}

    assert exit_code(study(_rows(160, h12=(0.011, -0.01), h24=0.05))) == 1
    assert exit_code({"groups": []}) == 2
    assert exit_code(study(_rows(20, h12=(0.011, -0.01), h24=0.0))) == 3
    assert exit_code(study(_rows(160, h12=(0.03, 0.02), h24=-0.05))) == 0
