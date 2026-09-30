"""Label resolver: each event by its own target type, no read past `as_of + horizon`, missing is recorded."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from tests.unit.scoring.mark_lake import (
    AS_OF,
    BTC,
    COIN,
    END,
    FAR,
    RISE,
    build_lake,
    kline_hour,
    mark_events,
    ms,
    ws_capture,
)

from hdt.contracts.common import TargetType
from hdt.core.config import scanner_config, scoring_config
from hdt.features.lake_io import LakeView
from hdt.lake.pit_query import PitQuery
from hdt.lake.raw_store import RawStore
from hdt.scoring.resolver import LabelOutcome, LabelRequest, resolve


def _request(target: TargetType, symbol: str = COIN, as_of: datetime = AS_OF) -> LabelRequest:
    return LabelRequest(
        event_id=f"ev-{target.value}",
        coin_id=1027,
        symbol=symbol,
        btc_symbol=BTC,
        as_of=as_of,
        horizon_h=12,
        target_type=target,
        label_spec_version="lbl1",
        beta_btc=1.0 if target is TargetType.RESID_12H else None,
        atr_frac=0.01,
    )


LAG = timedelta(seconds=scoring_config().resolver.kline_fetch_lag_s)


def _resolve(store: RawStore, request: LabelRequest, now: datetime, *, lag: timedelta = LAG) -> LabelOutcome:
    view = LakeView(PitQuery(store.staging_root, store.lake_root), clock=lambda: FAR)
    return resolve(
        view,
        request,
        labels=scanner_config().labels,
        barrier=scoring_config().barrier,
        now=now,
        missing_after=timedelta(hours=scoring_config().resolver.missing_after_h),
        kline_fetch_lag=lag,
    )


def test_raw_and_resid_events_get_their_own_labels(tmp_path: Path) -> None:
    store = build_lake(tmp_path)
    now = END + timedelta(hours=1)
    raw = _resolve(store, _request(TargetType.RAW_12H), now)
    resid = _resolve(store, _request(TargetType.RESID_12H), now)
    assert raw.status == "resolved"
    assert resid.status == "resolved"
    assert raw.y == 1
    assert raw.value == pytest.approx(0.02)
    assert resid.y == 0
    assert resid.value == pytest.approx(0.02 - 0.04)
    assert raw.btc_return is None
    assert resid.btc_return == pytest.approx(0.04)
    # entry 100, TP at 100 x (1 + 1.5 x 0.01) = 101.5 is reached before the stop (99.0)
    assert raw.barrier == 1
    assert raw.barrier_y == 1


def test_records_after_the_horizon_change_nothing(tmp_path: Path) -> None:
    store = build_lake(tmp_path)
    now = END + timedelta(days=2)
    before = [_resolve(store, _request(t), now) for t in TargetType]
    late = END + LAG + timedelta(minutes=10)
    store.append_many(
        [
            mark_events(late, mark=500.0),
            ws_capture(late, [{"e": "markPriceUpdate", "E": ms(END), "s": COIN, "p": "1.0"}]),
            *(kline_hour(s, END - timedelta(hours=1), late + timedelta(hours=1), spike=1000.0) for s in RISE),
            *(kline_hour(s, END, late + timedelta(hours=2)) for s in RISE),
        ]
    )
    after = [_resolve(store, _request(t), now) for t in TargetType]
    assert after == before


def test_kline_close_is_the_fallback_mark(tmp_path: Path) -> None:
    with_ws = _resolve(build_lake(tmp_path / "a"), _request(TargetType.RESID_12H), END + timedelta(hours=1))
    klines_only = _resolve(
        build_lake(tmp_path / "b", ws=False), _request(TargetType.RESID_12H), END + timedelta(hours=1)
    )
    assert klines_only.status == "resolved"
    assert klines_only.y == with_ws.y
    assert klines_only.value == pytest.approx(with_ws.value, abs=1e-9)


def test_kline_fallback_resolves_between_hourly_fetches(tmp_path: Path) -> None:
    """B2 fetches mark klines hourly: at 06:37 the kline closing then is only in the 07:00 record."""
    store = build_lake(tmp_path, ws=False)
    store.append_many([kline_hour(s, END, END + timedelta(hours=1)) for s in RISE])
    as_of = AS_OF + timedelta(minutes=37)
    now = as_of + timedelta(hours=12) + timedelta(hours=scoring_config().resolver.missing_after_h)
    raw = _resolve(store, _request(TargetType.RAW_12H, as_of=as_of), now)
    start = 100.0 * (1.0 + RISE[COIN] * 37 / 720)
    assert raw.status == "resolved"
    assert raw.y == 1
    assert raw.value == pytest.approx(100.0 * (1.0 + RISE[COIN]) / start - 1.0, abs=1e-9)
    assert raw.barrier is not None
    resid = _resolve(store, _request(TargetType.RESID_12H, as_of=as_of), now)
    assert resid.status == "resolved"
    assert resid.y == 0
    # The live `mark_at` bound alone (records fetched by t + max gap) finds no mark: the old behaviour.
    assert (
        _resolve(store, _request(TargetType.RAW_12H, as_of=as_of), now, lag=timedelta(0)).status == "missing"
    )


def test_unresolvable_label_waits_then_is_recorded_missing(tmp_path: Path) -> None:
    store = build_lake(tmp_path)
    request = _request(TargetType.RAW_12H, symbol="NOPEUSDT")
    missing_after = timedelta(hours=scoring_config().resolver.missing_after_h)
    assert _resolve(store, _request(TargetType.RAW_12H), END).status == "pending"
    assert _resolve(store, request, END + timedelta(hours=1)).status == "pending"
    missing = _resolve(store, request, END + missing_after)
    assert missing.status == "missing"
    assert missing.y is None


def test_barrier_stays_pending_until_the_last_kline_fetch_can_land(tmp_path: Path) -> None:
    """M-5: live marks resolve the 12 h label at 18:35, but the kline hour 18:00 (fetched at 19:00) is not
    in yet, so the triple barrier is not covered: the label stays pending instead of locking a NULL
    `barrier_y`, and resolves with the barrier once the fetch lands."""
    store = build_lake(tmp_path)
    as_of = AS_OF + timedelta(minutes=35)
    end = as_of + timedelta(hours=12)
    store.append_many([mark_events(end)])
    request = replace(_request(TargetType.RAW_12H, as_of=as_of), atr_frac=0.05)
    gap = timedelta(seconds=scanner_config().labels.max_mark_gap_s)
    early = _resolve(store, request, end + gap + timedelta(minutes=1))
    assert early.status == "pending"
    store.append_many([kline_hour(s, END, END + timedelta(hours=1)) for s in RISE])
    done = _resolve(store, request, end + LAG + gap + timedelta(minutes=1))
    assert (done.status, done.y, done.barrier, done.barrier_y) == ("resolved", 1, 0, 1)


def test_barrier_is_null_once_the_kline_fetch_can_no_longer_land(tmp_path: Path) -> None:
    store = build_lake(tmp_path)
    as_of = AS_OF + timedelta(minutes=35)
    end = as_of + timedelta(hours=12)
    store.append_many([mark_events(end)])
    request = replace(_request(TargetType.RAW_12H, as_of=as_of), atr_frac=0.05)
    gap = timedelta(seconds=scanner_config().labels.max_mark_gap_s)
    late = _resolve(store, request, end + LAG + gap)
    assert (late.status, late.y, late.barrier, late.barrier_y) == ("resolved", 1, None, None)
