"""Binance public REST collectors (matrix B1, B2, B7, B8, B9) and the daily universe build.

Every response is recorded verbatim through `BinancePublic` before it is parsed. Routes (lake names):
- B1 `exchange_info` daily, `funding_info` daily (funding interval per symbol);
- B2 `klines` (5m, 15m) and `mark_price_klines` (1m, label fallback): a startup backfill of
  `BACKFILL_LIMIT` bars, then an hourly batch of the last `BATCH_LIMIT` bars that closes any WS gap;
- B7 `funding_rate` (optional) daily for the LTX cross-section;
- B8 `open_interest` every 5 min for every universe member; `open_interest_hist` (5m period, weight 0,
  kept by Binance for 1 month only) hourly from recorder day 1, with a startup backfill;
- B9 `depth` (limit 1000) whenever a symbol enters the hot set, to rebuild its book from 100 ms diffs;
- `premium_index` (all symbols, weight 10) every 5 min: mark price for the OI-in-USD universe ranking.
Keys are Binance symbols; kline keys are `<symbol>:<interval>`.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Iterable
from datetime import datetime
from typing import Any, Final

from hdt.core.clock import utcnow
from hdt.core.config import BinanceFile
from hdt.ingest.binance_public import BinanceBackoffError, BinanceHttpError, BinancePublic
from hdt.ingest.symbol_map import SymbolMap, parse_cmc_map, parse_exchange_info
from hdt.lake.pit_query import PitQuery
from hdt.lake.raw_store import RawStore
from hdt.lake.universe import Universe, UniverseInputs, build_universe, load_universe, universe_capture

log = logging.getLogger(__name__)

BACKFILL_LIMIT: Final[int] = 1000  # weight 5 per call
BATCH_LIMIT: Final[int] = 24  # weight 1 per call
OI_HIST_BACKFILL: Final[int] = 500  # documented maximum
OI_HIST_BATCH: Final[int] = 13
FUNDING_RATE_LIMIT: Final[int] = 100
DEPTH_SNAPSHOT_LIMIT: Final[int] = 1000  # weight 20
MAX_MAP_PAGES: Final[int] = 20
MAP_PAGE_LIMIT: Final[int] = 5000


class UniverseInputsMissingError(LookupError):
    pass


def _kline_weight(limit: int) -> int:
    if limit < 100:
        return 1
    if limit < 500:
        return 2
    if limit <= 1000:
        return 5
    return 10


class BinanceCollectors:
    def __init__(
        self,
        *,
        rest: BinancePublic,
        store: RawStore,
        pit: PitQuery,
        binance: Callable[[], BinanceFile],
        clock: Callable[[], datetime] = utcnow,
    ) -> None:
        self._rest = rest
        self._store = store
        self._pit = pit
        self._binance = binance
        self._clock = clock

    # ------------------------------------------------------------------ targets

    def universe(self) -> Universe | None:
        return load_universe(self._pit, self._clock())

    def members(self) -> list[str]:
        universe = self.universe()
        return [m.binance_symbol for m in universe.members] if universe else []

    def ltx_members(self) -> list[str]:
        universe = self.universe()
        return [m.binance_symbol for m in universe.ltx] if universe else []

    # ------------------------------------------------------------------ jobs

    async def exchange_info(self) -> Any:
        return (await self._rest.get("exchange_info", "/fapi/v1/exchangeInfo")).data

    async def funding_info(self) -> None:
        await self._rest.get("funding_info", "/fapi/v1/fundingInfo")

    async def premium_index(self) -> Any:
        return (await self._rest.get("premium_index", "/fapi/v1/premiumIndex", weight=10)).data

    async def open_interest(self, symbols: Iterable[str] | None = None) -> dict[str, float]:
        out: dict[str, float] = {}
        for symbol in symbols if symbols is not None else self.members():
            data = await self._each("open_interest", "/fapi/v1/openInterest", {"symbol": symbol}, symbol)
            if isinstance(data, dict) and data.get("openInterest") is not None:
                out[symbol] = float(data["openInterest"])
        return out

    async def open_interest_hist(self, *, backfill: bool = False) -> None:
        limit = OI_HIST_BACKFILL if backfill else OI_HIST_BATCH
        for symbol in self.ltx_members():
            params = {"symbol": symbol, "period": "5m", "limit": limit}
            await self._each("open_interest_hist", "/futures/data/openInterestHist", params, symbol, weight=0)

    async def klines(self, *, backfill: bool = False) -> None:
        limit = BACKFILL_LIMIT if backfill else BATCH_LIMIT
        for symbol in self.members():
            for interval in self._binance().kline_intervals:
                params = {"symbol": symbol, "interval": interval, "limit": limit}
                key = f"{symbol}:{interval}"
                await self._each("klines", "/fapi/v1/klines", params, key, weight=_kline_weight(limit))

    async def mark_price_klines(self, *, backfill: bool = False) -> None:
        limit = BACKFILL_LIMIT if backfill else 2 * 60 + 1
        for symbol in self.members():
            params = {"symbol": symbol, "interval": "1m", "limit": limit}
            key = f"{symbol}:1m"
            await self._each(
                "mark_price_klines", "/fapi/v1/markPriceKlines", params, key, weight=_kline_weight(limit)
            )

    async def funding_rate(self) -> None:
        for symbol in self.ltx_members():
            params = {"symbol": symbol, "limit": FUNDING_RATE_LIMIT}
            await self._each("funding_rate", "/fapi/v1/fundingRate", params, symbol)

    async def depth_snapshot(self, symbols: Iterable[str]) -> None:
        for symbol in symbols:
            params = {"symbol": symbol, "limit": DEPTH_SNAPSHOT_LIMIT}
            await self._each("depth", "/fapi/v1/depth", params, symbol, weight=20)

    async def _each(self, route: str, path: str, params: dict[str, Any], key: str, *, weight: int = 1) -> Any:
        """One per-symbol call; a symbol-level HTTP error is logged and the batch continues."""
        try:
            return (await self._rest.get(route, path, params, key=key, weight=weight)).data
        except BinanceHttpError as exc:
            log.warning("binance call failed", extra={"route": route, "key": key, "code": exc.code})
            return None
        except BinanceBackoffError:
            raise  # the whole batch stops until the rate limit window ends

    # ------------------------------------------------------------------ universe

    def _latest_cmc(self, route: str, key: str = "") -> Any:
        record = self._pit.latest("cmc", route, self._clock(), key=key)
        if record is None or record.http_status != 200:
            return None
        body = json.loads(record.body())
        return body.get("data") if isinstance(body, dict) else None

    def _cmc_map(self) -> list[Any]:
        items: list[Any] = []
        for n in range(1, MAX_MAP_PAGES + 1):
            page = self._latest_cmc("crypto_map", "" if n == 1 else f"p{n}")
            if not isinstance(page, list):
                break
            items.extend(page)
            if len(page) < MAP_PAGE_LIMIT:
                break
        return items

    def universe_inputs_ready(self) -> bool:
        """The lake holds the CMC inputs of `build_universe`: listings_latest (#11) and crypto_map (#12)."""
        listings, map_page = self._latest_cmc("listings_latest"), self._latest_cmc("crypto_map")
        return isinstance(listings, list) and isinstance(map_page, list) and bool(map_page)

    async def build_universe(self) -> Universe:
        """Today's universe from lake records plus fresh exchangeInfo, premiumIndex and open interest."""
        now = self._clock()
        cfg = self._binance()
        listings = self._latest_cmc("listings_latest")
        coins = parse_cmc_map(self._cmc_map())
        if not isinstance(listings, list) or not coins:
            raise UniverseInputsMissingError(
                "listings_latest (#11) and crypto_map (#12) must be recorded first"
            )
        info = await self.exchange_info()
        symbols = SymbolMap.build(coins, parse_exchange_info(info), cfg.symbol_overrides)
        premium = await self.premium_index()
        rows = premium if isinstance(premium, list) else []
        marks = {
            str(item["symbol"]): float(item["markPrice"])
            for item in rows
            if isinstance(item, dict) and item.get("markPrice") is not None
        }
        eligible: list[str] = []
        for item in listings:
            rank = item.get("cmc_rank") if isinstance(item, dict) else None
            in_top = isinstance(rank, int) and rank <= cfg.cmc_universe_top
            link = symbols.by_cmc_id(int(item["id"])) if in_top else None
            if link is not None and link.binance_symbol not in cfg.banned_symbols:
                eligible.append(link.binance_symbol)
        oi = await self.open_interest(eligible)
        oi_usd = {s: qty * marks[s] for s, qty in oi.items() if s in marks}
        universe = build_universe(
            UniverseInputs(listings, oi_usd),
            symbols,
            day=now.date(),
            built_at=now,
            cmc_top=cfg.cmc_universe_top,
            ltx_size=cfg.ltx_universe_size,
            watchlist_size=cfg.watchlist_size,
            banned=cfg.banned_symbols,
            sources=self._source_hashes(),
        )
        self._store.append(universe_capture(universe))
        log.info(
            "universe built",
            extra={"date": universe.date.isoformat(), "members": len(universe.members)},
        )
        return universe

    def _source_hashes(self) -> dict[str, str]:
        now = self._clock()
        out: dict[str, str] = {}
        for source, route in (
            ("cmc", "listings_latest"),
            ("cmc", "crypto_map"),
            ("binance", "exchange_info"),
        ):
            record = self._pit.latest(source, route, now)
            if record is not None:
                out[route] = record.body_sha256
        return out


def ws_market_streams(symbols: Iterable[str], kline_intervals: Iterable[str]) -> list[str]:
    streams = ["!forceOrder@arr"]
    for symbol in symbols:
        s = symbol.lower()
        streams += [f"{s}@kline_{i}" for i in kline_intervals]
        streams += [f"{s}@aggTrade", f"{s}@markPrice@1s"]
    return streams


def ws_depth_streams(symbols: Iterable[str], levels: int) -> list[str]:
    return [f"{s.lower()}@depth{levels}@100ms" for s in symbols]


def ws_hot_streams(symbols: Iterable[str]) -> list[str]:
    return [f"{s.lower()}@depth@100ms" for s in symbols]
