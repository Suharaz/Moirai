"""Market inputs of the news modes, point in time, from the lake through the phase 02 feature engine.

- `z_funding` (zF): cross-sectional z-score of the coin's Binance funding rate per hour against every
  universe member with a funding rate at `as_of` (at least `MIN_FUNDING_PEERS` of them); positive = longs
  pay more than the typical coin (crowd long);
- `doi_4h` (dOI4): 4 h open-interest change of the coin over the complete exchange books (fraction);
- `liq_long_1h_usd` / `liq_short_1h_usd`: longs / shorts liquidated in the last hour (USD).
Missing inputs are None; the mode rules abstain when a rule needs one. `onchain` gives the recorded DEX
incident data the Fade panic refutation uses (`hdt.news.onchain`).
"""

from __future__ import annotations

import statistics
from datetime import datetime
from typing import Final, Protocol

from hdt.core.clock import ensure_utc
from hdt.core.config import StaticConfig, scanner_config, static_config
from hdt.features.engine import FeatureEngine
from hdt.features.lake_io import LakeView
from hdt.lake.pit_query import PitQuery
from hdt.lake.universe import load_universe
from hdt.news.modes import MarketState
from hdt.news.onchain import OnchainState, onchain_state

MIN_FUNDING_PEERS: Final[int] = 10


class MarketView(Protocol):
    def state(self, coin_id: int, as_of: datetime) -> MarketState | None:
        """Market inputs of `coin_id` at `as_of`; None when the coin is not in the universe known then."""
        ...

    def onchain(self, coin_id: int, start: datetime, end: datetime) -> OnchainState | None:
        """Recorded DEX incident data of `coin_id` fetched in `[start, end]` (PIT at `end`)."""
        ...


def funding_z(value: float | None, peers: list[float]) -> float | None:
    """z of `value` against `peers` (which include it); None without enough peers or spread."""
    if value is None or len(peers) < MIN_FUNDING_PEERS:
        return None
    sd = statistics.pstdev(peers)
    if sd <= 0:
        return None
    return round((value - statistics.fmean(peers)) / sd, 4)


class LakeMarketView:
    def __init__(
        self,
        pit: PitQuery,
        *,
        features: FeatureEngine | None = None,
        static: StaticConfig | None = None,
    ) -> None:
        self._pit = pit
        self._static = static or static_config()
        self._features = features or FeatureEngine(
            LakeView(pit), self._static, scanner_config(), max_snapshots=2
        )

    def state(self, coin_id: int, as_of: datetime) -> MarketState | None:
        as_of = ensure_utc(as_of)
        universe = load_universe(self._pit, as_of)
        if universe is None:
            return None
        member = next((m for m in universe.members if m.cmc_id == coin_id), None)
        if member is None:
            return None
        snap = self._features.snapshot(as_of, universe, self._static.cmc_routes)
        core = snap.core(member)
        funding = snap.funding_now
        peers = [
            rate for m in universe.members if (rate := funding.per_hour(m.binance_symbol)[0]) is not None
        ]
        own = funding.per_hour(member.binance_symbol)[0]
        return MarketState(
            z_funding=funding_z(own, peers),
            doi_4h=core.doi_4h,
            liq_long_1h_usd=core.liq.long_1h if core.liq is not None else None,
            liq_short_1h_usd=core.liq.short_1h if core.liq is not None else None,
        )

    def onchain(self, coin_id: int, start: datetime, end: datetime) -> OnchainState | None:
        return onchain_state(self._pit, coin_id, start, end)
