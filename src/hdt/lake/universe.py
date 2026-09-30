"""Point-in-time trading universe, one lake record per UTC day (`source=derived/route=universe/date=`).

Built from lake records only, as of the build time: CMC `listings/latest` (top `cmc_universe_top`) intersected
with Binance TRADING USDT perpetuals (`exchangeInfo`), mapped through `SymbolMap`, minus banned symbols,
ranked by Binance open interest in USD. The LTX cross-section is the top `ltx_universe_size`; the route #3
watchlist is the top `watchlist_size`. Readers use `load_universe(pit, as_of)`, so a replay at `as_of`
sees exactly the universe that existed then.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Final, Literal

from pydantic import BaseModel, ConfigDict

from hdt.core.ids import canonical_json
from hdt.ingest.symbol_map import SymbolMap
from hdt.lake.pit_query import PitQuery
from hdt.lake.schemas import Capture

UNIVERSE_SOURCE: Final = "derived"
UNIVERSE_ROUTE: Final[str] = "universe"


class UniverseMember(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    cmc_id: int
    cmc_symbol: str
    binance_symbol: str
    multiplier: int
    cmc_rank: int
    open_interest_usd: float


class Universe(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: Literal[1] = 1
    date: date
    built_at: datetime
    members: tuple[UniverseMember, ...]
    """All eligible coins ordered by open interest (USD), largest first."""
    ltx_size: int
    watchlist_size: int
    sources: dict[str, str]
    """Lake record hashes the universe was derived from (`route -> body_sha256`)."""

    @property
    def ltx(self) -> tuple[UniverseMember, ...]:
        return self.members[: self.ltx_size]

    @property
    def watchlist(self) -> tuple[UniverseMember, ...]:
        return self.members[: self.watchlist_size]


@dataclass(frozen=True)
class UniverseInputs:
    listings: Any
    """`data` array of CMC `/v3/cryptocurrency/listings/latest`."""
    open_interest: Mapping[str, float]
    """Binance symbol -> open interest in USD (openInterest x mark price)."""


def build_universe(
    inputs: UniverseInputs,
    symbols: SymbolMap,
    *,
    day: date,
    built_at: datetime,
    cmc_top: int,
    ltx_size: int,
    watchlist_size: int,
    banned: tuple[str, ...],
    sources: Mapping[str, str],
) -> Universe:
    ranked: list[UniverseMember] = []
    for item in inputs.listings if isinstance(inputs.listings, list) else []:
        rank = item.get("cmc_rank")
        if not isinstance(rank, int) or rank > cmc_top:
            continue
        link = symbols.by_cmc_id(int(item["id"]))
        if link is None or link.binance_symbol in banned:
            continue
        oi = inputs.open_interest.get(link.binance_symbol)
        if oi is None or oi <= 0:
            continue
        ranked.append(
            UniverseMember(
                cmc_id=link.cmc_id,
                cmc_symbol=link.cmc_symbol,
                binance_symbol=link.binance_symbol,
                multiplier=link.multiplier,
                cmc_rank=rank,
                open_interest_usd=oi,
            )
        )
    ranked.sort(key=lambda m: (-m.open_interest_usd, m.cmc_rank))
    return Universe(
        date=day,
        built_at=built_at,
        members=tuple(ranked),
        ltx_size=ltx_size,
        watchlist_size=watchlist_size,
        sources=dict(sources),
    )


def universe_capture(universe: Universe) -> Capture:
    return Capture(
        source=UNIVERSE_SOURCE,
        route=UNIVERSE_ROUTE,
        fetched_at=universe.built_at,
        http_status=200,
        body=canonical_json(universe.model_dump(mode="json")),
        params={"date": universe.date.isoformat()},
        key="",  # one universe per day; `latest` needs a single key
    )


def load_universe(pit: PitQuery, as_of: datetime, lookback: timedelta = timedelta(days=8)) -> Universe | None:
    """The most recent universe built at or before `as_of`."""
    record = pit.latest(UNIVERSE_SOURCE, UNIVERSE_ROUTE, as_of, lookback=lookback)
    if record is None:
        return None
    return Universe.model_validate(json.loads(record.body()))
