"""Deterministic synthetic lake for the phase 03 tests (edge-case fixtures of the implementation step 1).

Writes every route that `FeatureEngine.snapshot`, `QuantCore`, `Scanner` and `EventStudy` read, through
`RawStore` into staging JSONL only: `PitQuery` reads staging exactly like compacted Parquet partitions, and
compaction would only slow the fixture down. Prices, open interest, funding and liquidations are closed-form
functions of time, so a build is reproducible byte for byte and expected values follow from the constants
below. A `Market` describes the coins and the planted scenario; `SyntheticLake` writes it; `build_main`,
`build_study` and `build_perf` write the three scenarios the tests use.
"""

from __future__ import annotations

import json
import math
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from itertools import pairwise
from pathlib import Path
from typing import Any

from hdt.lake.pit_query import PitQuery
from hdt.lake.raw_store import RawStore
from hdt.lake.schemas import Capture, Source
from hdt.lake.universe import Universe, UniverseMember, universe_capture

REF = datetime(2026, 1, 1, tzinfo=UTC)
"""Origin of the price / OI waves (hours since REF)."""
HOUR = timedelta(hours=1)
MINUTE = timedelta(minutes=1)
FAR_FUTURE = datetime(2100, 1, 1, tzinfo=UTC)
"""LakeView clock for fixtures: every fixture hour counts as closed (memoized), as in a replay."""

EXCHANGES: tuple[tuple[str, float], ...] = (
    ("binance", 990.0),
    ("bybit", 950.0),
    ("okx", 900.0),
    ("bitget", 850.0),
    ("gate", 700.0),
    ("mexc", 650.0),
    ("bingx", 600.0),
    ("htx", 550.0),
    ("kucoin", 400.0),
    ("bitmex", 300.0),
)
"""Route #1 liquidity scores: core = the top 4, peripheral = the next 4 (cmc_routes.yaml cohort sizes)."""
CORE = ("binance", "bybit", "okx", "bitget")
PERIPHERAL = ("gate", "mexc", "bingx", "htx")
VENUE_WEIGHT: Mapping[str, float] = {
    "binance": 0.40,
    "bybit": 0.22,
    "okx": 0.17,
    "bitget": 0.11,
    "gate": 0.04,
    "mexc": 0.03,
    "bingx": 0.02,
    "htx": 0.01,
}
"""Share of a coin's OI per route #2 venue (S_P stays near 0.10, below the MIGRATION sp_min)."""
CORE_BASIS = 0.0002
PERIPHERAL_BASIS = 0.0004
INDEX_DISCOUNT = 0.0003
"""Binance index = mark * (1 - INDEX_DISCOUNT), so basis_binance = INDEX_DISCOUNT / (1 - INDEX_DISCOUNT)."""

QUIET_LIQ_FRAC = 0.002
"""Quiet hourly liquidations = 0.2 % of the coin's OI (L4h = 4x, L24h = 24x)."""
QUIET_LONG_SHARE = 0.55
LIQ_FLOOR_USD = 50_000.0
"""scanner.yaml spike.liq_floor_usd."""
DEFAULT_RATE = 0.0001
DEFAULT_INTERVAL_H = 8

TRADE_NOTIONAL_USD = 10_000.0
TRADE_EVERY = timedelta(seconds=120)
TRADES_PER_SNAPSHOT = 30
"""aggTrade: one trade of TRADE_NOTIONAL_USD every 120 s before the snapshot; every third is a taker sell."""
BOOK_LEVELS = 20
BOOK_STEP = 0.0002
"""Depth: level i (0-based) sits (i + 1) * BOOK_STEP from the mark on each side."""
BOOK_QTY_USD = 5_000.0
BID_WALL_LEVEL = 7
ASK_WALL_LEVEL = 12
WALL_MULT = 8.0

STABLE_BY_AGE_DAYS: Mapping[int, float] = {0: 1.10e9, 1: 1.05e9, 7: 1.00e9}
BTC_BY_AGE_DAYS: Mapping[int, float] = {0: 9_600.0, 1: 9_800.0, 7: 10_000.0}
"""Route #28 balances per exchange, by snapshot age: stablecoins +4.76 % over 1 d and +10 % over 7 d, BTC
-2.04 % and -4 %. Every exchange holds the same balances; an ETH balance is present and must be ignored."""
PERF_PCT: Mapping[str, float] = {"7d": 5.0, "30d": -12.0, "90d": 40.0, "365d": 150.0}
ATH_MULT = 2.0
ATL_MULT = 0.25
"""Route #10: all-time high / low = ATH_MULT / ATL_MULT x the coin's reference price."""
CIRCULATING = 6.0e8
TOTAL_SUPPLY = 1.0e9
MAX_SUPPLY = 2.0e9
LISTING_AGE_DAYS = 400
DEX_POOLS_USD = (1_500_000.0, 500_000.0)
DEX_HOLDER_BALANCES = (120.0, 100.0, 90.0, 80.0, 70.0, 60.0, 50.0, 40.0, 30.0, 20.0, 10.0, 5.0)
DEX_TOTAL_SUPPLY = 1_000.0
MACRO = {
    "fear_greed": 62.0,
    "altcoin_index": 35.0,
    "btc_dominance": 57.5,
    "total_market_cap": 2.5e12,
    "altcoin_market_cap": 1.06e12,
    "cmc20_pct": 1.8,
    "cmc100_pct": -0.6,
}


def hours(t: datetime) -> float:
    return (t - REF).total_seconds() / 3600.0


def ms(t: datetime) -> int:
    return round((t - datetime(1970, 1, 1, tzinfo=UTC)).total_seconds() * 1000)


def iso(t: datetime) -> str:
    return t.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.") + f"{t.microsecond // 1000:03d}Z"


def ticks(start: datetime, end: datetime, every: timedelta) -> Iterator[datetime]:
    """`start`, `start + every`, ... up to and including `end`."""
    t = start
    while t <= end:
        yield t
        t += every


# --------------------------------------------------------------------------- market model


@dataclass(frozen=True)
class Coin:
    cmc_id: int
    symbol: str
    price: float
    """Reference CMC USD price (the price path oscillates around it)."""
    oi_usd: float
    """Reference aggregate OI over the 8 route #2 venues."""
    tick: str
    beta: float = 1.0
    multiplier: int = 1
    """Binance contract units per CMC coin (1000PEPE style); mark = CMC price x multiplier."""
    phase: float = 0.0

    @property
    def binance(self) -> str:
        prefix = str(self.multiplier) if self.multiplier > 1 else ""
        return f"{prefix}{self.symbol}USDT"

    def member(self, rank: int) -> UniverseMember:
        return UniverseMember(
            cmc_id=self.cmc_id,
            cmc_symbol=self.symbol,
            binance_symbol=self.binance,
            multiplier=self.multiplier,
            cmc_rank=rank,
            open_interest_usd=self.oi_usd,
        )


@dataclass(frozen=True)
class Liq:
    """Liquidated notional per window (USD); `long_share` of every window is longs liquidated."""

    h1: float
    h4: float
    h24: float
    long_share: float = QUIET_LONG_SHARE

    def quote(self, updated: datetime) -> dict[str, Any]:
        out: dict[str, Any] = {"symbol": "USD", "last_updated": iso(updated)}
        for window, total in (("1h", self.h1), ("4h", self.h4), ("24h", self.h24)):
            longs = total * self.long_share
            out[f"total_liquidations_{window}"] = total
            out[f"long_liquidations_{window}"] = longs
            out[f"short_liquidations_{window}"] = total - longs
        return out


def quiet_liq(coin: Coin) -> Liq:
    q1 = coin.oi_usd * QUIET_LIQ_FRAC
    return Liq(q1, 4 * q1, 24 * q1)


def floor_of(coin: Coin) -> float:
    """SPIKE floor_c once 24 quiet hourly samples exist: max(liq_floor_usd, q20 of the quiet L1h)."""
    return max(LIQ_FLOOR_USD, quiet_liq(coin).h1)


def flush_liq(coin: Coin, spike: float, decay: float, long_share: float) -> Liq:
    """A flush with the given SPIKE / DECAY; base = 1.25 x the quiet floor, so the floor is not applied."""
    den = 1.25 * floor_of(coin)
    per_hour = spike * den
    h1 = decay * per_hour
    h4 = h1 + 3.0 * per_hour
    return Liq(h1, h4, h4 + 20.0 * den, long_share)


@dataclass(frozen=True)
class Span:
    start: datetime
    end: datetime

    def __contains__(self, t: datetime) -> bool:
        return self.start <= t <= self.end


@dataclass
class Market:
    coins: tuple[Coin, ...]
    """Universe order: largest reference OI first (as `build_universe` ranks)."""
    liq: dict[int, list[tuple[Span, Liq | None]]] = field(default_factory=dict)
    """Overrides by list fetch time; None = absent from the (complete) list."""
    oi_knots: dict[int, tuple[tuple[datetime, float], ...]] = field(default_factory=dict)
    """Piecewise-linear OI multiplier (flat outside the knots)."""
    funding: dict[int, tuple[tuple[datetime, float, int], ...]] = field(default_factory=dict)
    """(from, per-period rate, interval hours) segments; before the first one DEFAULT_RATE at 8 h."""
    quote_price_bias: dict[int, float] = field(default_factory=dict)
    cmc_oi_bias: dict[int, float] = field(default_factory=dict)
    """CMC route #2 Binance-venue OI / direct Binance OI - 1."""
    cmc_binance_funding: dict[int, float] = field(default_factory=dict)
    venue_funding: dict[int, tuple[str, float]] = field(default_factory=dict)
    """A non-Binance venue that reports a funding rate (no interval source)."""
    stale_quote: frozenset[int] = frozenset()
    new_listings: frozenset[int] = frozenset()
    force_1h_usd: dict[int, float] = field(default_factory=dict)
    """Binance forceOrder long liquidations in the hour before a snapshot (split in two orders)."""
    force_old_usd: dict[int, float] = field(default_factory=dict)
    """Binance forceOrder long liquidations 2 h before a snapshot (inside 4 h, outside 1 h)."""

    @property
    def btc(self) -> Coin:
        return next(c for c in self.coins if c.symbol == "BTC")

    # ------------------------------------------------------------------ closed-form paths

    def price(self, coin: Coin, t: datetime) -> float:
        """CMC USD price: BTC waves scaled by beta plus two coin-specific waves."""
        h = hours(t)
        btc = 0.03 * math.sin(2 * math.pi * h / 24) + 0.02 * math.sin(2 * math.pi * h / 61)
        btc += 0.008 * math.sin(2 * math.pi * h / 5.3)
        own = 0.012 * math.sin(2 * math.pi * h / 9.7 + coin.phase)
        own += 0.005 * math.sin(2 * math.pi * h / 2.9 + 2 * coin.phase)
        return coin.price * math.exp(coin.beta * btc + (own if coin.symbol != "BTC" else 0.0))

    def mark(self, coin: Coin, t: datetime) -> float:
        return self.price(coin, t) * coin.multiplier

    def quote_price(self, coin: Coin, t: datetime) -> float:
        return self.price(coin, t) * (1.0 + self.quote_price_bias.get(coin.cmc_id, 0.0))

    def oi_factor(self, coin: Coin, t: datetime) -> float:
        knots = self.oi_knots.get(coin.cmc_id, ())
        if not knots:
            return 1.0
        if t <= knots[0][0]:
            return knots[0][1]
        for (t0, f0), (t1, f1) in pairwise(knots):
            if t <= t1:
                return f0 + (f1 - f0) * ((t - t0) / (t1 - t0))
        return knots[-1][1]

    def oi_total(self, coin: Coin, t: datetime) -> float:
        wave = 1.0 + 0.005 * math.sin(2 * math.pi * hours(t) / 17 + coin.phase)
        return coin.oi_usd * wave * self.oi_factor(coin, t)

    def venue_oi(self, coin: Coin, slug: str, t: datetime) -> float:
        """True OI on one venue; the peripheral share breathes so zM has a non-zero history std."""
        weight = VENUE_WEIGHT[slug]
        if slug in PERIPHERAL:
            weight *= 1.0 + 0.2 * math.sin(2 * math.pi * hours(t) / 11 + coin.phase)
        return self.oi_total(coin, t) * weight

    def funding_at(self, coin: Coin, t: datetime) -> tuple[float, int]:
        rate, interval = DEFAULT_RATE, DEFAULT_INTERVAL_H
        for start, seg_rate, seg_interval in self.funding.get(coin.cmc_id, ()):
            if t >= start:
                rate, interval = seg_rate, seg_interval
        return rate, interval

    def next_funding(self, coin: Coin, t: datetime) -> datetime:
        """The next settlement strictly after `t` on the UTC-midnight grid of the current interval."""
        _, interval = self.funding_at(coin, t)
        midnight = t.replace(hour=0, minute=0, second=0, microsecond=0)
        slot = int((t - midnight) / timedelta(hours=interval)) + 1
        return midnight + slot * timedelta(hours=interval)

    def liq_at(self, coin: Coin, t: datetime) -> Liq | None:
        found: Liq | None = quiet_liq(coin)
        for span, liq in self.liq.get(coin.cmc_id, ()):
            if t in span:
                found = liq
        return found


# --------------------------------------------------------------------------- lake writer


def _cmc_body(data: Any, at: datetime, *, error_code: int = 0) -> bytes:
    status = {"timestamp": iso(at), "error_code": error_code, "error_message": None, "credit_count": 1}
    if error_code:
        status["error_message"] = "synthetic upstream error"
        return json.dumps({"status": status}).encode()
    return json.dumps({"status": status, "data": data}).encode()


def plan_refusal_body(at: datetime) -> bytes:
    """CMC 1006 (HTTP 403): the key's plan does not include the endpoint; not charged, no `data`."""
    status = {
        "timestamp": iso(at),
        "error_code": 1006,
        "error_message": "Your API Key subscription plan doesn't support this endpoint.",
        "credit_count": 0,
    }
    return json.dumps({"status": status}).encode()


def _num(x: float) -> str:
    return f"{x:.10g}"


class SyntheticLake:
    def __init__(self, root: Path, market: Market, *, refused: frozenset[str] = frozenset()) -> None:
        self.root = root
        self.market = market
        self.refused = refused
        """CMC routes the plan refuses: every capture of theirs is a 1006 error response instead of data."""
        self.store = RawStore(root / "staging", root / "lake")
        self._pending: list[Capture] = []

    @property
    def pit(self) -> PitQuery:
        return PitQuery(self.store.staging_root, self.store.lake_root)

    def flush(self) -> None:
        self.store.append_many(self._pending)
        self._pending.clear()

    def _add(
        self,
        source: Source,
        route: str,
        at: datetime,
        body: bytes,
        *,
        key: str = "",
        status: int = 200,
        params: Mapping[str, Any] | None = None,
    ) -> None:
        self._pending.append(Capture(source, route, at, status, body, params=params, key=key))

    def _cmc(self, route: str, at: datetime, data: Any, *, key: str = "", **params: Any) -> None:
        if route in self.refused:
            self._add("cmc", route, at, plan_refusal_body(at), key=key, status=403, params=params or None)
            return
        self._add("cmc", route, at, _cmc_body(data, at), key=key, params=params or None)

    def _binance(self, route: str, at: datetime, data: Any, *, key: str = "") -> None:
        self._add("binance", route, at, json.dumps(data).encode(), key=key)

    def _ws(self, route: str, at: datetime, items: list[dict[str, Any]]) -> None:
        self._add("binance", route, at, json.dumps(items).encode(), status=101, params={"connection": route})

    # ------------------------------------------------------------------ slow-moving routes

    def universe(self, day: date, built_at: datetime, ltx_size: int | None = None) -> Universe:
        coins = self.market.coins
        members = tuple(c.member(rank) for rank, c in enumerate(coins, start=1))
        universe = Universe(
            date=day,
            built_at=built_at,
            members=members,
            ltx_size=ltx_size or len(members),
            watchlist_size=len(members),
            sources={},
        )
        self._pending.append(universe_capture(universe))
        return universe

    def exchanges(self, at: datetime) -> None:
        data = {
            "exchanges": [
                {"id": i, "exchange_slug": slug, "liquidity_score": score}
                for i, (slug, score) in enumerate(EXCHANGES, start=1)
            ]
        }
        self._cmc("derivatives_exchanges", at, data)

    def funding_info(self, at: datetime, listed: Mapping[str, int]) -> None:
        rows = [
            {
                "symbol": symbol,
                "adjustedFundingRateCap": "0.02",
                "adjustedFundingRateFloor": "-0.02",
                "fundingIntervalHours": hours_,
                "disclaimer": False,
            }
            for symbol, hours_ in sorted(listed.items())
        ]
        self._binance("funding_info", at, rows)

    def exchange_info(self, at: datetime) -> None:
        symbols = [
            {
                "symbol": c.binance,
                "status": "TRADING",
                "contractType": "PERPETUAL",
                "filters": [
                    {
                        "filterType": "PRICE_FILTER",
                        "tickSize": c.tick,
                        "minPrice": c.tick,
                        "maxPrice": "1000000",
                    },
                    {"filterType": "LOT_SIZE", "stepSize": "1", "minQty": "1", "maxQty": "1000000"},
                ],
            }
            for c in self.market.coins
        ]
        self._binance("exchange_info", at, {"timezone": "UTC", "symbols": symbols})

    # ------------------------------------------------------------------ cadenced routes

    def premium(self, start: datetime, end: datetime, every: timedelta) -> None:
        """`premium_index` REST snapshots plus the same values as `ws_markprice` events (label source)."""
        m = self.market
        for t in ticks(start, end, every):
            rows, events = [], []
            for c in m.coins:
                mark = m.mark(c, t)
                rate, _ = m.funding_at(c, t)
                nft = ms(m.next_funding(c, t))
                index = mark * (1.0 - INDEX_DISCOUNT)
                rows.append(
                    {
                        "symbol": c.binance,
                        "markPrice": _num(mark),
                        "indexPrice": _num(index),
                        "estimatedSettlePrice": _num(index),
                        "lastFundingRate": _num(rate),
                        "interestRate": "0.00010000",
                        "nextFundingTime": nft,
                        "time": ms(t),
                    }
                )
                events.append(
                    {
                        "e": "markPriceUpdate",
                        "E": ms(t),
                        "s": c.binance,
                        "p": _num(mark),
                        "i": _num(index),
                        "P": _num(index),
                        "r": _num(rate),
                        "T": nft,
                    }
                )
            self._binance("premium_index", t, rows)
            self._ws("ws_markprice", t, [{"recv_ts": ms(t), "stream": "!markPrice@arr@1s", "data": events}])

    def lists(
        self, start: datetime, end: datetime, every: timedelta, *, page2: bool = True, error: bool = False
    ) -> None:
        """CMC liquidation lists: route #3 by crypto (2 pages, `has_more` paging) and by exchange."""
        m = self.market
        for t in ticks(start, end, every):
            if error:
                self._add("cmc", "liquidations_by_crypto", t, _cmc_body(None, t, error_code=500))
                continue
            items = []
            for c in m.coins:
                liq = m.liq_at(c, t)
                if liq is not None:
                    quote = liq.quote(t - timedelta(seconds=5))
                    items.append({"crypto_id": c.cmc_id, "symbol": c.symbol, "quotes": [quote]})
            half = (len(items) + 1) // 2
            self._cmc(
                "liquidations_by_crypto",
                t,
                {"cryptocurrencies": items[:half], "has_more": True},
                start=1,
                limit=half,
            )
            if page2:
                self._cmc(
                    "liquidations_by_crypto",
                    t + timedelta(seconds=2),
                    {"cryptocurrencies": items[half:], "has_more": False},
                    key="p2",
                    start=half + 1,
                    limit=half,
                )
            total = sum(liq.h4 for c in m.coins if (liq := m.liq_at(c, t)) is not None)
            exchanges = [
                {"exchange_slug": slug, "quotes": [Liq(1.0, share * total, 6 * share * total).quote(t)]}
                for slug, share in (("binance", 0.4), ("bybit", 0.3), ("okx", 0.2), ("gate", 0.1))
            ]
            self._cmc(
                "liquidations_by_exchange",
                t + timedelta(seconds=3),
                {"exchanges": exchanges, "has_more": False},
            )

    def books(self, start: datetime, end: datetime, every: timedelta) -> None:
        """Route #2 market pairs of every cohort venue (single page: fewer items than `limit`)."""
        m = self.market
        for t in ticks(start, end, every):
            for slug in CORE + PERIPHERAL:
                basis = CORE_BASIS if slug in CORE else PERIPHERAL_BASIS
                pairs = []
                for c in m.coins:
                    oi = m.venue_oi(c, slug, t)
                    rate: float | None = None
                    if slug == "binance":
                        oi *= 1.0 + m.cmc_oi_bias.get(c.cmc_id, 0.0)
                        rate = m.cmc_binance_funding.get(c.cmc_id, m.funding_at(c, t)[0])
                    elif m.venue_funding.get(c.cmc_id, ("", 0.0))[0] == slug:
                        rate = m.venue_funding[c.cmc_id][1]
                    price = m.price(c, t)
                    pairs.append(
                        {
                            "market_id": c.cmc_id * 100 + len(pairs),
                            "market_pair": f"{c.symbol}/USDT",
                            "category": "perpetual",
                            "fee_type": "percentage",
                            "outlier_detected": False,
                            "exclusions": [],
                            "market_pair_base": {"crypto_id": c.cmc_id, "currency_symbol": c.symbol},
                            "market_pair_quote": {"crypto_id": 825, "currency_symbol": "USDT"},
                            "quotes": [{"price": price, "volume_24h": 20.0 * oi, "open_interest": oi}],
                            "exchange_reported_quotes": [
                                {"price": price * (1 + basis), "index_price": price, "funding_rate": rate}
                            ],
                        }
                    )
                self._cmc(
                    "exchange_derivative_market_pairs",
                    t,
                    {"slug": slug, "num_market_pairs": len(pairs), "market_pairs": pairs},
                    key=slug,
                    slug=slug,
                    category="perpetual",
                    start=1,
                    limit=500,
                )

    # ------------------------------------------------------------------ point-in-time snapshot routes

    def snapshot(self, t: datetime) -> None:
        """Everything a full `quant_core` packet reads at as_of `t` (fetched just before `t`)."""
        self.ohlcv(t - MINUTE)
        self.klines(t - timedelta(seconds=20))
        self._open_interest(t - timedelta(seconds=20))
        self._quotes(t - timedelta(seconds=20))
        self._macro(t - timedelta(seconds=20))
        self._depth(t - timedelta(seconds=3))
        self._trades(t - timedelta(seconds=2))
        self._force(t - timedelta(seconds=5))
        self._slow_cmc(t)

    def ohlcv(self, at: datetime, *, days: float = 43) -> None:
        """CMC hourly OHLCV (`days` of history, 42 d + 1 d by default) of every coin in one record; bars
        closed by `at`."""
        items = [self._ohlcv_item(c, at, days) for c in self.market.coins]
        self._cmc("ohlcv_historical", at, items, interval="hourly")

    def ohlcv_backfill(self, at: datetime, *, days: float, interval: str | None = "hourly") -> None:
        """The route #7 one-off backfill: one record per coin (lake route `ohlcv_backfill`, key = cmc id) with
        `days` of hourly bars; `interval=None` stands for a call made before route #7 sent `interval`."""
        params = {"time_period": "hourly"} | ({"interval": interval} if interval else {})
        for c in self.market.coins:
            self._cmc("ohlcv_backfill", at, self._ohlcv_item(c, at, days), key=str(c.cmc_id), **params)

    def _ohlcv_item(self, c: Coin, at: datetime, days: float) -> dict[str, Any]:
        m = self.market
        last_open = at.replace(minute=0, second=0, microsecond=0) - HOUR
        first_open = last_open - timedelta(days=days)
        quotes = []
        for opened in ticks(first_open, last_open, HOUR):
            path = [m.price(c, opened + k * 10 * MINUTE) for k in range(7)]
            volume = 2.0e6 * (c.oi_usd / 5.0e7) * (1.2 + math.sin(2 * math.pi * hours(opened) / 24 + c.phase))
            quotes.append(
                {
                    "time_open": iso(opened),
                    "time_close": iso(opened + HOUR - timedelta(milliseconds=1)),
                    "quote": {
                        "USD": {
                            "open": path[0],
                            "high": max(path) * 1.001,
                            "low": min(path) * 0.999,
                            "close": path[-1],
                            "volume": volume,
                            "market_cap": path[-1] * CIRCULATING,
                            "timestamp": iso(opened + HOUR - timedelta(milliseconds=1)),
                        }
                    },
                }
            )
        return {"id": c.cmc_id, "name": c.symbol, "symbol": c.symbol, "quotes": quotes}

    def klines(self, at: datetime) -> None:
        """Binance 15m klines, 7 d + 15 min, closed by `at`, one record per symbol."""
        m = self.market
        step = timedelta(minutes=15)
        last_open = at.replace(second=0, microsecond=0)
        last_open -= timedelta(minutes=last_open.minute % 15) + step
        for c in m.coins:
            rows = []
            for opened in ticks(last_open - timedelta(days=7), last_open, step):
                path = [m.mark(c, opened + k * 5 * MINUTE) for k in range(4)]
                volume = 1.0e4 * (1.3 + math.sin(2 * math.pi * hours(opened) / 7 + c.phase))
                close = path[-1]
                rows.append(
                    [
                        ms(opened),
                        _num(path[0]),
                        _num(max(path) * 1.0005),
                        _num(min(path) * 0.9995),
                        _num(close),
                        _num(volume),
                        ms(opened + step) - 1,
                        _num(volume * close),
                        100,
                        _num(volume / 2),
                        _num(volume * close / 2),
                        "0",
                    ]
                )
            self._binance("klines", at, rows, key=f"{c.binance}:15m")

    def _open_interest(self, at: datetime) -> None:
        m = self.market
        for c in m.coins:
            qty = m.venue_oi(c, "binance", at) / m.mark(c, at)
            self._binance(
                "open_interest",
                at,
                {"symbol": c.binance, "openInterest": _num(qty), "time": ms(at)},
                key=c.binance,
            )

    def _quotes(self, at: datetime) -> None:
        m = self.market
        data = []
        for c in m.coins:
            price = m.quote_price(c, at)
            updated = at - (timedelta(minutes=10) if c.cmc_id in m.stale_quote else MINUTE)
            data.append(
                {
                    "id": c.cmc_id,
                    "name": c.symbol,
                    "symbol": c.symbol,
                    "date_added": iso(at - timedelta(days=LISTING_AGE_DAYS)),
                    "tags": ["layer-1", {"slug": "synthetic", "name": "Synthetic"}],
                    "circulating_supply": CIRCULATING,
                    "total_supply": TOTAL_SUPPLY,
                    "max_supply": MAX_SUPPLY,
                    "last_updated": iso(updated),
                    "quote": [
                        {
                            "symbol": "USD",
                            "price": price,
                            "volume_24h": 20.0 * c.oi_usd,
                            "market_cap": price * CIRCULATING,
                            "last_updated": iso(updated),
                        }
                    ],
                }
            )
        self._cmc("quotes_latest", at, data)

    def _macro(self, at: datetime) -> None:
        self._cmc("fear_greed_latest", at, {"value": MACRO["fear_greed"], "value_classification": "Greed"})
        self._cmc("altcoin_season_latest", at, {"altcoin_index": MACRO["altcoin_index"]})
        self._cmc(
            "global_metrics_latest",
            at,
            {
                "btc_dominance": MACRO["btc_dominance"],
                "quote": {
                    "USD": {
                        "total_market_cap": MACRO["total_market_cap"],
                        "altcoin_market_cap": MACRO["altcoin_market_cap"],
                    }
                },
            },
        )
        self._cmc("cmc20_latest", at, {"value": 250.0, "value_24h_percentage_change": MACRO["cmc20_pct"]})
        self._cmc("cmc100_latest", at, {"value": 180.0, "value_24h_percentage_change": MACRO["cmc100_pct"]})
        self._cmc("total_liquidations", at, {"quotes": [Liq(40e6, 150e6, 600e6, 0.6).quote(at)]})

    def _depth(self, at: datetime) -> None:
        m = self.market
        items = []
        for c in m.coins:
            mark = m.mark(c, at)
            bids, asks = [], []
            for i in range(BOOK_LEVELS):
                bid_px, ask_px = mark * (1 - (i + 1) * BOOK_STEP), mark * (1 + (i + 1) * BOOK_STEP)
                bid_qty = BOOK_QTY_USD / bid_px * (WALL_MULT if i == BID_WALL_LEVEL else 1.0)
                ask_qty = BOOK_QTY_USD / ask_px * (WALL_MULT if i == ASK_WALL_LEVEL else 1.0)
                bids.append([_num(bid_px), _num(bid_qty)])
                asks.append([_num(ask_px), _num(ask_qty)])
            items.append(
                {
                    "recv_ts": ms(at),
                    "stream": f"{c.binance.lower()}@depth20@100ms",
                    "data": {
                        "e": "depthUpdate",
                        "E": ms(at),
                        "T": ms(at),
                        "s": c.binance,
                        "b": bids,
                        "a": asks,
                    },
                }
            )
        self._ws("ws_depth20", at, items)

    def _trades(self, at: datetime) -> None:
        m = self.market
        items = []
        for c in m.coins:
            for k in range(1, TRADES_PER_SNAPSHOT + 1):
                t = at - k * TRADE_EVERY
                price = m.mark(c, t)
                items.append(
                    {
                        "recv_ts": ms(t),
                        "stream": f"{c.binance.lower()}@aggTrade",
                        "data": {
                            "e": "aggTrade",
                            "E": ms(t),
                            "s": c.binance,
                            "a": k,
                            "p": _num(price),
                            "q": repr(TRADE_NOTIONAL_USD / price),
                            "T": ms(t),
                            "m": k % 3 == 0,
                        },
                    }
                )
        self._ws("ws_aggtrade", at, items)

    def _force(self, at: datetime) -> None:
        m = self.market
        items = []
        for c in m.coins:
            orders = []
            if c.cmc_id in m.force_1h_usd:
                half = m.force_1h_usd[c.cmc_id] / 2
                orders += [(at - 10 * MINUTE, half), (at - 40 * MINUTE, half)]
            if c.cmc_id in m.force_old_usd:
                orders.append((at - 2 * HOUR, m.force_old_usd[c.cmc_id]))
            for t, notional in orders:
                price = m.mark(c, t)
                qty = repr(notional / price)
                order = {"s": c.binance, "S": "SELL", "o": "LIMIT", "f": "IOC", "q": qty, "p": _num(price)}
                order |= {"ap": _num(price), "X": "FILLED", "l": qty, "z": qty, "T": ms(t)}
                items.append(
                    {
                        "recv_ts": ms(t),
                        "stream": "!forceOrder@arr",
                        "data": {"e": "forceOrder", "E": ms(t), "o": order},
                    }
                )
        self._ws("ws_forceorder", at, items)

    def _slow_cmc(self, t: datetime) -> None:
        """Routes #10, #15, #28 and the DEX routes, fetched well inside their lookbacks."""
        m = self.market
        perf = {
            str(c.cmc_id): {
                "id": c.cmc_id,
                "symbol": c.symbol,
                "periods": {
                    "all_time": {
                        "quote": {
                            "USD": {
                                "high": ATH_MULT * c.price,
                                "low": ATL_MULT * c.price,
                                "percent_change": 900.0,
                            }
                        }
                    },
                    **{p: {"quote": {"USD": {"percent_change": pct}}} for p, pct in PERF_PCT.items()},
                },
            }
            for c in m.coins
        }
        self._cmc("price_performance_stats", t - HOUR, perf)
        self._cmc("listings_new", t - HOUR, [{"id": cid, "name": "new"} for cid in sorted(m.new_listings)])
        for age in (0, 1, 7):
            at = t - timedelta(days=age, seconds=30)
            for slug in CORE + PERIPHERAL:
                self._cmc(
                    "exchange_assets",
                    at,
                    [
                        {
                            "wallet_address": "0x1",
                            "balance": STABLE_BY_AGE_DAYS[age] * 0.7,
                            "currency": {"symbol": "USDT"},
                        },
                        {
                            "wallet_address": "0x2",
                            "balance": STABLE_BY_AGE_DAYS[age] * 0.3,
                            "currency": {"symbol": "USDC"},
                        },
                        {
                            "wallet_address": "bc1",
                            "balance": BTC_BY_AGE_DAYS[age],
                            "currency": {"symbol": "BTC"},
                        },
                        {
                            "wallet_address": "0x3",
                            "balance": 5.0e4 * (age + 1),
                            "currency": {"symbol": "ETH"},
                        },
                    ],
                    key=slug,
                )
        at = t - timedelta(days=1)
        for c in m.coins:
            key = str(c.cmc_id)
            self._cmc("dex_token_pools", at, [{"liqUsd": v} for v in DEX_POOLS_USD], key=key)
            holders = [{"balance": b, "totalSupply": DEX_TOTAL_SUPPLY} for b in DEX_HOLDER_BALANCES]
            self._cmc("dex_holders", at, {"holders": holders}, key=key)
            security = {
                "extra": {"isFlaggedByVendor": False, "buyTax": "0.01", "sellTax": 0.02},
                "securityItems": [{"isHit": True}, {"isHit": False}, {"isHit": True}],
                "evmDisplay": {"honeypotStatus": 0},
            }
            self._cmc("dex_security_detail", at, security, key=key)


# --------------------------------------------------------------------------- scenarios

MAIN_DAY = date(2026, 3, 10)
A = datetime(2026, 3, 10, 12, 0, 30, tzinfo=UTC)
"""Main as_of (a scanner grid slot): full data, one LTX strict trigger (XTRG)."""
A_BLOCK = A + HOUR
"""5 of 11 non-BTC LTX coins flush LONG: breadth B = 0.45 >= 0.40 while BTC has not flushed."""
A_INFLIGHT = datetime(2026, 3, 10, 13, 57, 30, tzinfo=UTC)
"""30 s after a liquidation cycle whose page 2 never arrives: the previous complete cycle is used."""
A_MISS = A + 2 * HOUR
"""210 s after that incomplete cycle: missing page, no fallback (`missing_route`)."""
A_ERR = A + 3 * HOUR
"""10 s after an error response to page 1 (`missing_route` and `cmc_degraded`)."""

BTC = Coin(1, "BTC", 60_000.0, 8.0e9, "0.1")
ETH = Coin(1027, "ETH", 3_000.0, 3.0e9, "0.01", beta=1.1, phase=0.7)
XTRG = Coin(5001, "XTRG", 20.0, 120e6, "0.001", beta=1.3, phase=1.3)
"""LTX strict LONG trigger at A (and at A_BLOCK, where contagion blocks it)."""
MPEP = Coin(5007, "MPEP", 0.00001, 70e6, "0.0000001", multiplier=1000, phase=0.3)
"""1000x contract multiplier: CMC price x 1000 = Binance mark."""
ZERO = Coin(5003, "ZERO", 5.0, 60e6, "0.0001", beta=1.2, phase=2.9)
"""L24h = L4h at A: the SPIKE base is 0 and the floor takes over."""
RCON = Coin(5008, "RCON", 40.0, 55e6, "0.001", phase=1.9)
"""Reconciliation mismatches: CMC price +2 %, CMC Binance OI +25 %, CMC funding, forceOrder above CMC L1h."""
FEIG = Coin(5005, "FEIG", 8.0, 50e6, "0.001", phase=4.4)
"""Funding 8 h -> 4 h at the same hourly cost; fundingInfo not yet updated (inferred interval wins)."""
NVEN = Coin(5009, "NVEN", 0.5, 48e6, "0.00001", phase=2.5)
"""A peripheral venue reports funding (no interval source); stale CMC quote; newest listing."""
FLST = Coin(5006, "FLST", 3.0, 45e6, "0.0001", phase=5.2)
"""Funding 8 h -> 4 h at the same hourly cost; fundingInfo already lists 4 h."""
ABSN = Coin(5004, "ABSN", 1.0, 40e6, "0.0001", phase=3.7)
"""Absent from the complete list at A, present with large values in the list 5 minutes earlier."""
QUIE = Coin(5010, "QUIE", 12.0, 10e6, "0.001", phase=3.3)
"""Quiet coin: 30-day q20 below liq_floor_usd, so floor_c = liq_floor_usd."""
BRST = Coin(5002, "BRST", 2.0, 2.5e6, "0.0001", beta=0.9, phase=2.1)
"""Flush below min_liq_notional_usd at A; every other LTX condition passes."""

MAIN_COINS = (BTC, ETH, XTRG, MPEP, ZERO, RCON, FEIG, NVEN, FLST, ABSN, QUIE, BRST)

BRST_LIQ = Liq(10_000.0, 210_000.0, 400_000.0, 0.9)
ZERO_LIQ = Liq(150_000.0, 1_500_000.0, 1_500_000.0, 0.6)
ABSN_STALE_LIQ = Liq(2.0e6, 5.0e6, 9.0e6, 0.9)
RCON_FORCE_1H = 300_000.0
ETH_FORCE_1H = 50_000.0
ETH_FORCE_OLD = 20_000.0
RCON_QUOTE_BIAS = 0.02
RCON_OI_BIAS = 0.25
RCON_CMC_FUNDING = 0.0012
NVEN_VENUE = ("gate", 0.0002)
FUNDING_CHANGE = A - 3 * HOUR
"""FEIG / FLST switch from 0.0008 per 8 h to 0.0004 per 4 h right after the 08:00 settlement."""


def at_list(t: datetime) -> Span:
    """The liquidation list fetched just before as_of `t` (the 10 s before, within a minute)."""
    return Span(t - MINUTE, t)


def main_market() -> Market:
    xtrg_flush = flush_liq(XTRG, spike=10.0, decay=0.15, long_share=0.85)
    liq: dict[int, list[tuple[Span, Liq | None]]] = {
        XTRG.cmc_id: [(at_list(A), xtrg_flush), (at_list(A_BLOCK), xtrg_flush)],
        BTC.cmc_id: [(at_list(A), flush_liq(BTC, spike=5.0, decay=0.2, long_share=0.85))],
    }
    liq[BRST.cmc_id] = [(at_list(A), BRST_LIQ)]
    liq[ZERO.cmc_id] = [(at_list(A), ZERO_LIQ)]
    liq[ABSN.cmc_id] = [(Span(A - 6 * MINUTE, A - MINUTE), ABSN_STALE_LIQ), (at_list(A), None)]
    for coin in (ETH, FEIG, FLST, MPEP):
        liq[coin.cmc_id] = [(at_list(A_BLOCK), flush_liq(coin, spike=4.0, decay=0.3, long_share=0.8))]
    decline = ((A - 6 * HOUR, 1.0), (A + HOUR, 0.86))
    released = ((REF, 0.0008, 8), (A - timedelta(minutes=150), 0.0003, 8))
    switched = ((REF, 0.0008, 8), (FUNDING_CHANGE, 0.0004, 4))
    return Market(
        coins=MAIN_COINS,
        liq=liq,
        oi_knots={c.cmc_id: decline for c in (XTRG, BRST, BTC)},
        funding={
            **{c.cmc_id: released for c in (XTRG, BRST, BTC)},
            **{c.cmc_id: switched for c in (FEIG, FLST)},
        },
        quote_price_bias={RCON.cmc_id: RCON_QUOTE_BIAS},
        cmc_oi_bias={RCON.cmc_id: RCON_OI_BIAS},
        cmc_binance_funding={RCON.cmc_id: RCON_CMC_FUNDING},
        venue_funding={NVEN.cmc_id: NVEN_VENUE},
        stale_quote=frozenset({NVEN.cmc_id}),
        new_listings=frozenset({NVEN.cmc_id}),
        force_1h_usd={RCON.cmc_id: RCON_FORCE_1H, ETH.cmc_id: ETH_FORCE_1H},
        force_old_usd={ETH.cmc_id: ETH_FORCE_OLD},
    )


def write_common(lake: SyntheticLake, first: datetime, last: datetime, day: date) -> None:
    """Universe, route #1 ranking, exchangeInfo and daily fundingInfo covering [first, last]."""
    month_start = first.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    lake.exchanges(month_start + 5 * MINUTE)
    lake.universe(day, first.replace(minute=0, second=0, microsecond=0) - HOUR)
    for midnight in ticks(
        first.replace(hour=0, minute=0, second=0) - timedelta(days=2), last, timedelta(days=1)
    ):
        lake.funding_info(midnight + 10 * MINUTE, {"OTHERUSDT": 4})
        lake.exchange_info(midnight + 20 * MINUTE)


def build_main(root: Path, *, refused: frozenset[str] = frozenset()) -> SyntheticLake:
    lake = SyntheticLake(root, main_market(), refused=refused)
    write_common(lake, A - 2 * HOUR, A_ERR, MAIN_DAY)
    lake.funding_info(FUNDING_CHANGE + 10 * MINUTE, {"OTHERUSDT": 4, FLST.binance: 4})
    slot_offset = timedelta(seconds=10)
    lake.premium(A - 31 * HOUR - slot_offset, A_ERR + 30 * MINUTE, 5 * MINUTE)
    lake.lists(A - 30 * HOUR - slot_offset, A - 2 * HOUR - HOUR, HOUR)
    lake.lists(A - 2 * HOUR - slot_offset, A_INFLIGHT - timedelta(minutes=2, seconds=10), 5 * MINUTE)
    lake.lists(A_INFLIGHT - timedelta(seconds=30), A_INFLIGHT - timedelta(seconds=30), HOUR, page2=False)
    lake.lists(A_ERR - slot_offset, A_ERR - slot_offset, HOUR, error=True)
    lake.books(A - 26 * HOUR - timedelta(seconds=20), A_ERR, HOUR)
    lake.snapshot(A)
    lake.flush()
    return lake


STUDY_DAY = date(2026, 3, 20)
S = datetime(2026, 3, 20, 0, 0, 30, tzinfo=UTC)
STUDY_END = S + timedelta(hours=12, minutes=15)
SX = Coin(6001, "SXXX", 15.0, 110e6, "0.001", beta=1.2, phase=0.9)
"""Strict LTX trigger in the slots S, S+5m, S+10m and again at S+12h05m."""
SY = Coin(6002, "SYYY", 6.0, 90e6, "0.0001", beta=0.8, phase=1.7)
"""Strict LTX trigger in the slot S+10m only."""
SX_TRIGGERS = (S, S + 5 * MINUTE, S + 10 * MINUTE, S + timedelta(hours=12, minutes=5))
SY_TRIGGERS = (S + 10 * MINUTE,)
STUDY_FILLERS = tuple(
    Coin(6003 + i, f"SF{i}", 4.0 + 3 * i, 80e6 - 5e6 * i, "0.0001", beta=0.7 + 0.1 * i, phase=0.45 * i)
    for i in range(8)
)
STUDY_COINS = (BTC, SX, SY, *STUDY_FILLERS)


def study_market() -> Market:
    liq: dict[int, list[tuple[Span, Liq | None]]] = {}
    for coin, triggers in ((SX, SX_TRIGGERS), (SY, SY_TRIGGERS)):
        flush = flush_liq(coin, spike=8.0, decay=0.15, long_share=0.85)
        liq[coin.cmc_id] = [(at_list(t), flush) for t in triggers]
    knots = (
        (S - 6 * HOUR, 1.0),
        (S, 0.85),
        (S + 7 * HOUR, 0.85),
        (S + timedelta(hours=12, minutes=10), 0.72),
    )
    funding = (
        (REF, 0.0008, 8),
        (S - 3 * HOUR, 0.0003, 8),
        (S + 6 * HOUR, 0.0008, 8),
        (S + 11 * HOUR, 0.0003, 8),
    )
    return Market(
        coins=STUDY_COINS,
        liq=liq,
        oi_knots={SX.cmc_id: knots, SY.cmc_id: knots},
        funding={SX.cmc_id: funding, SY.cmc_id: funding},
    )


def build_study(root: Path) -> SyntheticLake:
    lake = SyntheticLake(root, study_market())
    write_common(lake, S - HOUR, STUDY_END, STUDY_DAY)
    slot_offset = timedelta(seconds=10)
    lake.premium(S - 31 * HOUR - slot_offset, STUDY_END + 5 * MINUTE, 5 * MINUTE)
    lake.lists(S - 30 * HOUR - slot_offset, S - HOUR, HOUR)
    lake.lists(S - slot_offset, STUDY_END, 5 * MINUTE)
    lake.books(S - 26 * HOUR - timedelta(seconds=20), STUDY_END, HOUR)
    lake.ohlcv(S - MINUTE)
    lake.flush()
    return lake


def perf_coins(n: int) -> tuple[Coin, ...]:
    alts = tuple(
        Coin(7000 + i, f"P{i:02d}", 10.0 + i, 3.0e9 / (i + 2), "0.001", beta=0.8 + 0.01 * i, phase=0.37 * i)
        for i in range(n - 1)
    )
    return (BTC, *alts)


def build_perf(root: Path, n: int) -> SyntheticLake:
    """`n` universe members (BTC included) at A with hourly cadence history and full snapshot routes."""
    lake = SyntheticLake(root, Market(coins=perf_coins(n)))
    write_common(lake, A - 2 * HOUR, A, MAIN_DAY)
    slot_offset = timedelta(seconds=10)
    lake.premium(A - 31 * HOUR - slot_offset, A - slot_offset, HOUR)
    lake.lists(A - 30 * HOUR - slot_offset, A - slot_offset, HOUR)
    lake.books(A - 26 * HOUR - timedelta(seconds=20), A, HOUR)
    lake.snapshot(A)
    lake.flush()
    return lake


def members_by_id(coins: Sequence[Coin]) -> dict[int, Coin]:
    return {c.cmc_id: c for c in coins}
