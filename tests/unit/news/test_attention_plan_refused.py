"""News attention when the plan refuses the CMC attention routes (#13 trending, #14 gainers/losers, #15 new
listings): no rank or gainer rank is invented, New_i and Roll_i stay unset, and attention comes from NewsZ."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from fixtures.synthetic_lake import plan_refusal_body
from hdt.lake.pit_query import PitQuery
from hdt.lake.raw_store import RawStore
from hdt.lake.schemas import Capture
from hdt.news.attention import (
    GAINERS_ROUTE,
    NEW_LISTINGS_ROUTE,
    TRENDING_ROUTE,
    LakeAttention,
    attention_for,
)
from hdt.news.config import news_config

AS_OF = datetime(2026, 9, 29, 12, 0, tzinfo=UTC)
COIN = 1027


def _lake(tmp_path: Path, *, refused: bool) -> PitQuery:
    store = RawStore(tmp_path / "staging", tmp_path / "lake")
    if refused:  # every cycle of the last day answered 1006 (HTTP 403, no `data`)
        for route in (TRENDING_ROUTE, GAINERS_ROUTE, NEW_LISTINGS_ROUTE):
            for minutes in range(5, 24 * 60, 15):
                at = AS_OF - timedelta(minutes=minutes)
                store.append(
                    Capture(
                        source="cmc",
                        route=route,
                        fetched_at=at,
                        http_status=403,
                        body=plan_refusal_body(at),
                        params={},
                    )
                )
    return PitQuery(store.staging_root, store.lake_root)


@pytest.mark.parametrize("refused", [True, False], ids=["1006-captures", "no-records"])
def test_refused_attention_routes_leave_lake_signals_unset(tmp_path: Path, refused: bool) -> None:
    cfg = news_config().attention
    lake = LakeAttention.read(_lake(tmp_path, refused=refused), AS_OF, cfg)
    assert (lake.now, lake.prior, lake.history, lake.gainers, lake.new_listings) == (
        None,
        None,
        (),
        None,
        frozenset(),
    )
    # 12 articles in the current 6 h window over a floor baseline: attention from NewsZ alone.
    articles = [AS_OF - timedelta(minutes=10 * n) for n in range(1, 13)]
    signals = attention_for(COIN, AS_OF, lake, articles, cfg)
    assert (signals.trending_rank, signals.prior_rank, signals.gainer_rank, signals.roll_i) == (
        None,
        None,
        None,
        None,
    )
    assert signals.new_i is False
    assert signals.news_z >= cfg.news_z_min
    assert signals.attention
    quiet = attention_for(COIN, AS_OF, lake, [], cfg)
    assert not quiet.attention
