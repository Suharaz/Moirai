"""CMC hourly bars fill the hours the hourly route #7 records lack from the coin's one-off backfill record,
point in time: a backfill fetched after `as_of` adds nothing."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import numpy as np

from fixtures.synthetic_lake import FAR_FUTURE, A, SyntheticLake, main_market
from hdt.features.bars import cmc_hourly
from hdt.features.lake_io import LakeView

HOUR_MS = 3_600_000


def test_backfill_extends_hourly_history_only_after_it_was_fetched(tmp_path: Path) -> None:
    lake = SyntheticLake(tmp_path, main_market())
    backfilled_at = A - timedelta(hours=2, minutes=30)
    lake.ohlcv_backfill(backfilled_at, days=10)
    lake.ohlcv(A, days=0.5)  # only half a day of hourly recording so far
    lake.flush()
    coin = main_market().coins[0].cmc_id

    before = cmc_hourly(
        LakeView(lake.pit, clock=lambda: FAR_FUTURE), backfilled_at - timedelta(minutes=1), 42
    )
    assert coin not in before  # neither record is visible yet

    bars = cmc_hourly(LakeView(lake.pit, clock=lambda: FAR_FUTURE), A, 42)[coin]
    times = bars.open_time
    # 10 d + 1 bars up to the 08:00 bar closed before the backfill, then 09-11 h from the hourly record
    assert len(times) == 10 * 24 + 1 + 3
    assert np.all(np.diff(times) == HOUR_MS)  # contiguous, no duplicate hour
    assert times[-1] + HOUR_MS <= int(A.timestamp() * 1000)  # closed bars only


def test_a_backfill_made_without_hourly_sampling_is_ignored(tmp_path: Path) -> None:
    # Before route #7 sent `interval=hourly`, CMC sampled the hourly periods daily: such a record must not
    # mix 24 h-spaced bars into the 1h series.
    lake = SyntheticLake(tmp_path, main_market())
    lake.ohlcv_backfill(A - timedelta(hours=2, minutes=30), days=10, interval=None)
    lake.ohlcv(A, days=0.5)
    lake.flush()
    coin = main_market().coins[0].cmc_id
    bars = cmc_hourly(LakeView(lake.pit, clock=lambda: FAR_FUTURE), A, 42)[coin]
    assert len(bars.open_time) == 13  # the hourly record's half day only
