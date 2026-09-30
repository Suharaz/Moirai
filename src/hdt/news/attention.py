"""Attention signals per coin (design contract section 6), point in time, from the lake and `news_items`:

- New_i: the coin is in the top `top_new` of CMC trending (route #13) now and was outside the top
  `prior_outside` (or absent) in the newest trending capture `prior_lookback_h` earlier;
- Roll_i: share of trending captures of the last `roll_window_h` in which the coin was in the top `top_new`;
- NewsZ_i: canonical articles mapped to the coin in the last `news_window_h`, divided by the coin's average
  count per `news_window_h` over the previous `news_baseline_days` (floored at `news_baseline_floor`);
- the rank among trending gainers (route #14) and presence in the newest listings (route #15) as context.
A coin is an attention candidate when New_i holds or NewsZ_i >= `news_z_min`. The trending list order is
the rank (1 = most trending).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Final

from hdt.core.clock import ensure_utc
from hdt.lake.pit_query import PitQuery
from hdt.lake.schemas import RawRecord
from hdt.news.config import AttentionConfig
from hdt.tools.base import ToolBackendError
from hdt.tools.pit import cmc_data

TRENDING_ROUTE: Final[str] = "trending_latest"
GAINERS_ROUTE: Final[str] = "trending_gainers_losers"
NEW_LISTINGS_ROUTE: Final[str] = "listings_new"
CAPTURE_LOOKBACK: Final[timedelta] = timedelta(hours=2)
LISTINGS_LOOKBACK: Final[timedelta] = timedelta(hours=3)


@dataclass(frozen=True)
class Attention:
    coin_id: int
    as_of: datetime
    trending_rank: int | None
    prior_rank: int | None
    new_i: bool
    roll_i: float | None
    gainer_rank: int | None
    new_listing: bool
    news_count: int
    news_baseline: float
    news_z: float
    attention: bool

    def row(self, rule_version: str, computed_at: datetime) -> dict[str, Any]:
        return {
            "coin_id": self.coin_id,
            "as_of": self.as_of,
            "trending_rank": self.trending_rank,
            "prior_rank": self.prior_rank,
            "new_i": self.new_i,
            "roll_i": self.roll_i,
            "gainer_rank": self.gainer_rank,
            "new_listing": self.new_listing,
            "news_count": self.news_count,
            "news_baseline": self.news_baseline,
            "news_z": self.news_z,
            "attention": self.attention,
            "rule_version": rule_version,
            "computed_at": computed_at,
        }


def ranks(record: RawRecord | None) -> dict[int, int] | None:
    """CMC id -> 1-based rank in a list capture; None when the capture is missing or failed."""
    if record is None:
        return None
    try:
        data = cmc_data(record)
    except ToolBackendError:
        return None
    if not isinstance(data, list):
        return None
    out: dict[int, int] = {}
    for position, entry in enumerate(data, start=1):
        coin_id = entry.get("id") if isinstance(entry, Mapping) else None
        if isinstance(coin_id, int) and not isinstance(coin_id, bool) and coin_id not in out:
            out[coin_id] = position
    return out


@dataclass(frozen=True)
class LakeAttention:
    """The lake side of the signals at one `as_of` (shared by every coin)."""

    now: dict[int, int] | None
    prior: dict[int, int] | None
    history: tuple[dict[int, int], ...]
    gainers: dict[int, int] | None
    new_listings: frozenset[int]

    @classmethod
    def read(cls, pit: PitQuery, as_of: datetime, cfg: AttentionConfig) -> LakeAttention:
        as_of = ensure_utc(as_of)
        prior_at = as_of - timedelta(hours=cfg.prior_lookback_h)
        start = as_of - timedelta(hours=cfg.roll_window_h)
        history = tuple(
            parsed
            for record in pit.series("cmc", TRENDING_ROUTE, start, as_of, as_of=as_of, key="")
            if (parsed := ranks(record)) is not None
        )
        listings = ranks(pit.latest("cmc", NEW_LISTINGS_ROUTE, as_of, key="", lookback=LISTINGS_LOOKBACK))
        return cls(
            now=ranks(pit.latest("cmc", TRENDING_ROUTE, as_of, key="", lookback=CAPTURE_LOOKBACK)),
            prior=ranks(pit.latest("cmc", TRENDING_ROUTE, prior_at, key="", lookback=CAPTURE_LOOKBACK)),
            history=history,
            gainers=ranks(pit.latest("cmc", GAINERS_ROUTE, as_of, key="", lookback=CAPTURE_LOOKBACK)),
            new_listings=frozenset(listings or ()),
        )


def news_z(count: int, baseline_times: Sequence[datetime], cfg: AttentionConfig) -> tuple[float, float]:
    """(baseline average per window, NewsZ) from the article times of the baseline period."""
    windows = cfg.news_baseline_days * 24 / cfg.news_window_h
    baseline = max(len(baseline_times) / windows, cfg.news_baseline_floor)
    return round(baseline, 4), round(count / baseline, 4)


def attention_for(
    coin_id: int,
    as_of: datetime,
    lake: LakeAttention,
    article_times: Sequence[datetime],
    cfg: AttentionConfig,
) -> Attention:
    """Signals of one coin; `article_times` are its canonical articles' ingestion times over the baseline
    period plus the current window (`[as_of - baseline_days - news_window_h, as_of]`)."""
    as_of = ensure_utc(as_of)
    window_start = as_of - timedelta(hours=cfg.news_window_h)
    baseline_start = window_start - timedelta(days=cfg.news_baseline_days)
    current = [t for t in article_times if window_start < t <= as_of]
    baseline = [t for t in article_times if baseline_start < t <= window_start]
    baseline_avg, z = news_z(len(current), baseline, cfg)
    rank = lake.now.get(coin_id) if lake.now is not None else None
    prior = lake.prior.get(coin_id) if lake.prior is not None else None
    new_i = (
        rank is not None
        and rank <= cfg.top_new
        and lake.prior is not None
        and (prior is None or prior > cfg.prior_outside)
    )
    roll = (
        round(
            sum(1 for h in lake.history if h.get(coin_id, cfg.top_new + 1) <= cfg.top_new)
            / len(lake.history),
            4,
        )
        if lake.history
        else None
    )
    return Attention(
        coin_id=coin_id,
        as_of=as_of,
        trending_rank=rank,
        prior_rank=prior,
        new_i=new_i,
        roll_i=roll,
        gainer_rank=lake.gainers.get(coin_id) if lake.gainers is not None else None,
        new_listing=coin_id in lake.new_listings,
        news_count=len(current),
        news_baseline=baseline_avg,
        news_z=z,
        attention=new_i or z >= cfg.news_z_min,
    )


def article_window(as_of: datetime, cfg: AttentionConfig) -> datetime:
    """Oldest ingestion time `attention_for` needs."""
    return ensure_utc(as_of) - timedelta(hours=cfg.news_window_h) - timedelta(days=cfg.news_baseline_days)
