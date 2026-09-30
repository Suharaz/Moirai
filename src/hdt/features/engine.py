"""Feature engine: one market snapshot per (as_of, universe, route config), per-agent feature blocks.

Everything cross-sectional (liquidation list, SPIKE floors, route #2 books, S_P history, funding states,
forceOrder lower bounds, FDz, B, BTC flush) is computed once per snapshot; per-coin inputs (klines, book,
Binance OI) are computed lazily and memoized inside the snapshot. Every read goes through `LakeView`
(point in time), so a snapshot never sees a record fetched after its `as_of`.

Every non-news block carries the common keys (same value for every agent at one (coin, as_of)):
`mark_price`, `atr_1h`, `beta_btc`, `categories`, `open_interest_usd`, `quote_volume_1h_usd`,
`btc_mark_price`, `btc_atr_1h`; plus the reconciliation features.
"""

from __future__ import annotations

import math
from collections import OrderedDict
from collections.abc import Callable, Iterator, Mapping
from datetime import datetime, timedelta
from functools import cached_property
from typing import Any

import numpy as np
import talib

from hdt.contracts.candidate import CandidateSet
from hdt.contracts.common import AgentName, DataQualityFlag
from hdt.core.clock import ensure_utc
from hdt.core.config import CmcRoutesFile, ScannerFile, StaticConfig
from hdt.features.bars import INTERVAL_MS, Bars, binance_klines, cmc_hourly, resample, to_ms
from hdt.features.common import FeatureBlock, finite, last_valid
from hdt.features.crowding import (
    BINANCE_SLUG,
    CoinCrowding,
    CrossSection,
    CrowdingInputs,
    coin_crowding,
    cross_section,
    crowding_block,
    read_vex,
)
from hdt.features.fundamental import (
    CmcQuote,
    dex_features,
    fundamental_features,
    new_listing_ids,
    read_quotes,
)
from hdt.features.funding import FundingState, PremiumRow, funding_state
from hdt.features.lake_io import BINANCE, CMC, LakeView, as_float, cmc_ok
from hdt.features.levels import LevelInputs, LevelRules, candidate_set, tick_size
from hdt.features.liquidations import (
    HourlyLiq,
    force_events,
    force_lower_bounds,
    hourly_liquidations,
    read_liquidations,
)
from hdt.features.micro import Book, TradeFlow, book_features, flow_features, latest_book, trade_flow
from hdt.features.migration import Cohorts, ExchangeBook, cohort_sizes, cohorts, read_book, sp_history
from hdt.features.reconcile import ReconcileInputs, reconcile_features
from hdt.features.regime import context_features, regime_features, reserve_features
from hdt.features.technical import technical_features
from hdt.lake.universe import Universe, UniverseMember

COMMON_KEYS = (
    "mark_price",
    "atr_1h",
    "beta_btc",
    "categories",
    "open_interest_usd",
    "quote_volume_1h_usd",
    "btc_mark_price",
    "btc_atr_1h",
)
FAMILIES: Mapping[AgentName, str] = {
    AgentName.CROWDING: "crowding",
    AgentName.TECHNICAL: "technical",
    AgentName.MICRO: "micro",
    AgentName.FUNDAMENTAL: "fundamental",
    AgentName.MACRO: "macro",
}
PERF_ROUTE = "price_performance_stats"
CORE_CMC_ROUTES = (
    ("liquidations_by_crypto", ""),
    ("quotes_latest", ""),
    ("exchange_derivative_market_pairs", BINANCE_SLUG),
)
"""(route, lake key of the first page) whose newest error response marks the snapshot `cmc_degraded`."""
_HOUR = timedelta(hours=1)


def feature_version(static: StaticConfig, scanner: ScannerFile) -> str:
    """Packet `feature_ver`: indicators feature_ver + scanner rule_version (SPIKE floor lives there)."""
    return f"{static.indicators.feature_ver}.{scanner.rule_version}"


class Snapshot:
    """Cross-sectional state of one `as_of`; per-coin parts are computed on first use."""

    def __init__(
        self, engine: FeatureEngine, as_of: datetime, universe: Universe, routes: CmcRoutesFile
    ) -> None:
        self.engine = engine
        self.view = engine.view
        self.static = engine.static
        self.scanner = engine.scanner
        self.as_of = ensure_utc(as_of)
        self.universe = universe
        self.routes = routes
        self.stale = timedelta(seconds=engine.static.settings.data.stale_s)
        btc_symbol = self.static.indicators.reserves.btc_symbol
        self.btc: UniverseMember | None = next(
            (m for m in universe.members if m.cmc_symbol == btc_symbol), None
        )
        self._members = {m.cmc_id: m for m in universe.members}
        self._cores: dict[int, CoinCrowding] = {}
        self._per_coin: dict[tuple[str, int], Any] = {}
        self._levels: dict[tuple[int, LevelRules], CandidateSet] = {}

    # ------------------------------------------------------------------ shared inputs

    def member(self, coin_id: int) -> UniverseMember | None:
        return self._members.get(coin_id)

    @cached_property
    def groups(self) -> Cohorts | None:
        return cohorts(self.view, self.as_of, self.routes)

    @cached_property
    def books(self) -> dict[str, dict[str, ExchangeBook]]:
        ind = self.static.indicators
        offsets = {
            "now": timedelta(0),
            "outlier": timedelta(minutes=ind.reconcile.outlier_window_min),
            "h1": timedelta(hours=1),
            "h4": timedelta(hours=4),
            "growth": timedelta(hours=ind.migration_growth_window_h),
        }
        slugs = self.groups.all if self.groups is not None else ()
        return {
            label: {slug: read_book(self.view, slug, self.as_of - delta, ind) for slug in slugs}
            for label, delta in offsets.items()
        }

    @cached_property
    def funding_now(self) -> FundingState:
        return funding_state(self.view, self.as_of, self.static.indicators.funding, self.stale)

    @cached_property
    def funding_before(self) -> FundingState:
        lookback = timedelta(hours=self.static.indicators.funding.lookback_h)
        return funding_state(self.view, self.as_of - lookback, self.static.indicators.funding, self.stale)

    @cached_property
    def liq_hist(self) -> HourlyLiq:
        return hourly_liquidations(
            self.view, self.as_of, self.scanner.spike.floor_window_days, self.static.indicators.liquidations
        )

    @cached_property
    def crowding_inputs(self) -> CrowdingInputs:
        ind = self.static.indicators
        groups = self.groups
        sp = sp_history(self.view, self.as_of, groups, ind, list(self._members)) if groups is not None else {}
        events = force_events(self.view, self.as_of, timedelta(hours=4))
        return CrowdingInputs(
            liq=read_liquidations(self.view, self.as_of, ind.liquidations),
            liq_hist=self.liq_hist,
            books=self.books,
            groups=groups,
            sp_hist=sp,
            vex=read_vex(self.view, self.as_of, ind.liquidations),
            force_lb=force_lower_bounds(events, self.as_of),
            binance_oi=_LazyOi(self),
            funding_now=self.funding_now,
            funding_before=self.funding_before,
            stale=self.stale,
            as_of=self.as_of,
        )

    def core(self, member: UniverseMember) -> CoinCrowding:
        found = self._cores.get(member.cmc_id)
        if found is None:
            found = coin_crowding(
                member.cmc_id,
                member.binance_symbol,
                self.crowding_inputs,
                self.static.indicators,
                self.scanner,
            )
            self._cores[member.cmc_id] = found
        return found

    @cached_property
    def cross(self) -> CrossSection:
        ltx = [m.cmc_id for m in self.universe.ltx]
        cores = {m.cmc_id: self.core(m) for m in self.universe.ltx}
        return cross_section(
            cores, ltx, self.btc.cmc_id if self.btc else None, self.static.indicators, self.scanner
        )

    @cached_property
    def quotes(self) -> tuple[bool, dict[int, CmcQuote], datetime | None]:
        record, quotes = read_quotes(self.view, self.as_of, timedelta(hours=2))
        return record is not None, quotes, record.fetched_at if record else None

    @cached_property
    def cmc_degraded(self) -> bool:
        for route, key in CORE_CMC_ROUTES:
            record = self.view.latest(CMC, route, self.as_of, key=key, lookback=timedelta(minutes=30))
            if record is not None and not cmc_ok(record, self.view.body_json(record)):
                return True
        return False

    @cached_property
    def cmc_1h(self) -> dict[int, Bars]:
        days = max(self.static.indicators.technical.cmc_history_days, self.static.indicators.beta_window_days)
        return cmc_hourly(self.view, self.as_of, days)

    @cached_property
    def exchange_info(self) -> Any:
        record = self.view.latest(BINANCE, "exchange_info", self.as_of, lookback=timedelta(days=2))
        return self.view.binance_data(record)

    @cached_property
    def perf(self) -> Mapping[str, Any]:
        _, data = self.view.latest_cmc_ok(PERF_ROUTE, self.as_of, lookback=timedelta(days=2))
        if isinstance(data, Mapping):
            return data
        if isinstance(data, list):
            return {str(item.get("id")): item for item in data if isinstance(item, Mapping)}
        return {}

    @cached_property
    def flow(self) -> TradeFlow:
        windows = set(self.static.indicators.cvd_windows) | set(self.static.indicators.micro.taker_windows)
        longest = max(INTERVAL_MS[w] for w in windows)
        return trade_flow(self.view, self.as_of, timedelta(milliseconds=longest))

    @cached_property
    def new_ids(self) -> set[int] | None:
        return new_listing_ids(self.view, self.as_of)

    @cached_property
    def macro(self) -> FeatureBlock:
        cadence = {
            route.name: timedelta(seconds=route.cadence_s)
            for route in self.static.cmc_routes.routes
            if route.cadence_s is not None
        }
        block = context_features(self.view, self.as_of, self.stale, cadence)
        btc_bars = (
            self.cmc_1h.get(self.btc.cmc_id, Bars.empty(INTERVAL_MS["1h"]))
            if self.btc
            else Bars.empty(INTERVAL_MS["1h"])
        )
        block.merge(regime_features(btc_bars, self.static.indicators))
        slugs = self.groups.all if self.groups is not None else ()
        block.merge(reserve_features(self.view, slugs, self.as_of, self.static.indicators))
        return block

    # ------------------------------------------------------------------ per-coin inputs

    def _memo[T](self, name: str, coin_id: int, compute: Callable[[], T]) -> T:
        key = (name, coin_id)
        if key in self._per_coin:
            hit: T = self._per_coin[key]
            return hit
        value = compute()
        self._per_coin[key] = value
        return value

    def klines_15m(self, member: UniverseMember) -> Bars:
        days = max(
            self.static.indicators.levels.lookback_days, self.static.indicators.technical.binance_history_days
        )
        return self._memo(
            "k15",
            member.cmc_id,
            lambda: binance_klines(self.view, member.binance_symbol, "15m", self.as_of, timedelta(days=days)),
        )

    def bars_1h_binance(self, member: UniverseMember) -> Bars:
        return self._memo("b1h", member.cmc_id, lambda: resample(self.klines_15m(member), "1h"))

    def atr_1h(self, member: UniverseMember) -> tuple[float | None, bool]:
        """ATR on Binance 1h bars and whether the newest closed hour is stale."""

        def compute() -> tuple[float | None, bool]:
            bars = self.bars_1h_binance(member)
            period = self.static.indicators.atr_period
            if len(bars) < period + 1:
                return None, False
            atr = last_valid(talib.ATR(bars.high, bars.low, bars.close, timeperiod=period))
            last_close = bars.last_close_time()
            stale = last_close is None or self.as_of - last_close > _HOUR + self.stale
            return atr, stale

        return self._memo("atr1h", member.cmc_id, compute)

    def premium(self, member: UniverseMember) -> PremiumRow | None:
        return self.funding_now.rows.get(member.binance_symbol)

    def binance_oi_usd(self, member: UniverseMember) -> float | None:
        def compute() -> float | None:
            record = self.view.latest(
                BINANCE, "open_interest", self.as_of, key=member.binance_symbol, lookback=self.stale
            )
            data = self.view.binance_data(record)
            qty = as_float(data.get("openInterest")) if isinstance(data, Mapping) else None
            row = self.premium(member)
            mark = row.mark if row is not None else None
            return qty * mark if qty is not None and mark is not None else None

        return self._memo("oi", member.cmc_id, compute)

    def book(self, member: UniverseMember) -> Book | None:
        max_age = timedelta(seconds=self.static.indicators.micro.book_max_age_s)
        return self._memo(
            "book", member.cmc_id, lambda: latest_book(self.view, member.binance_symbol, self.as_of, max_age)
        )

    def beta(self, member: UniverseMember) -> float | None:
        def compute() -> float | None:
            if self.btc is None:
                return None
            if member.cmc_id == self.btc.cmc_id:
                return 1.0
            ind = self.static.indicators
            start = to_ms(self.as_of - timedelta(days=ind.beta_window_days))
            coin = self.cmc_1h.get(member.cmc_id)
            btc = self.cmc_1h.get(self.btc.cmc_id)
            if coin is None or btc is None:
                return None
            return beta_from_bars(coin.since(start), btc.since(start), ind.beta_min_bars)

        return self._memo("beta", member.cmc_id, compute)

    # ------------------------------------------------------------------ blocks

    def common(self, member: UniverseMember) -> FeatureBlock:
        return self._memo("common", member.cmc_id, lambda: self._common(member))

    def _common(self, member: UniverseMember) -> FeatureBlock:
        block = FeatureBlock()
        _mark(block, "mark_price", self.premium(member), self.as_of, self.stale)
        atr, atr_stale = self.atr_1h(member)
        block.set("atr_1h", atr)
        if atr is None:
            block.flag(DataQualityFlag.MISSING_ROUTE, "atr_1h")
        elif atr_stale:
            block.flag(DataQualityFlag.STALE, "atr_1h")
        beta = self.beta(member)
        block.set("beta_btc", beta)
        if beta is None:
            block.flag(DataQualityFlag.MISSING_ROUTE, "beta_btc")
        _, quotes, _ = self.quotes
        quote = quotes.get(member.cmc_id)
        block.set("categories", quote.categories if quote else None)
        if quote is None or quote.categories is None:
            block.flag(DataQualityFlag.MISSING_ROUTE, "categories")
        oi = self.binance_oi_usd(member)
        block.set("open_interest_usd", oi)
        if oi is None:
            block.flag(DataQualityFlag.MISSING_ROUTE, "open_interest_usd")
        qv = quote_volume_1h(self.klines_15m(member), self.as_of, self.stale)
        block.set("quote_volume_1h_usd", qv)
        if qv is None:
            block.flag(DataQualityFlag.MISSING_ROUTE, "quote_volume_1h_usd")
        if self.btc is None:
            block.update({"btc_mark_price": None, "btc_atr_1h": None})
            block.flag(DataQualityFlag.MISSING_ROUTE, "btc_mark_price", "btc_atr_1h")
        else:
            _mark(block, "btc_mark_price", self.premium(self.btc), self.as_of, self.stale)
            btc_atr, btc_stale = self.atr_1h(self.btc)
            block.set("btc_atr_1h", btc_atr)
            if btc_atr is None:
                block.flag(DataQualityFlag.MISSING_ROUTE, "btc_atr_1h")
            elif btc_stale:
                block.flag(DataQualityFlag.STALE, "btc_atr_1h")
        return block

    def reconcile(self, member: UniverseMember) -> FeatureBlock:
        def compute() -> FeatureBlock:
            _, quotes, _ = self.quotes
            book = self.books.get("now", {}).get(BINANCE_SLUG)
            core = self.core(member)
            inputs = ReconcileInputs(
                as_of=self.as_of,
                stale=self.stale,
                premium=self.premium(member),
                multiplier=member.multiplier,
                quote=quotes.get(member.cmc_id),
                cmc_binance=book.coins.get(member.cmc_id) if book is not None and book.complete else None,
                binance_interval_h=self.funding_now.interval(member.binance_symbol).hours,
                binance_oi_usd=self.binance_oi_usd(member),
                liq=core.liq,
                lower_bound=core.lb,
                cmc_degraded=self.cmc_degraded,
            )
            return reconcile_features(inputs, self.static.indicators.reconcile)

        return self._memo("reconcile", member.cmc_id, compute)

    def crowding(self, member: UniverseMember) -> FeatureBlock:
        return crowding_block(
            self.core(member), self.cross, self.crowding_inputs, self.scanner, self.static.indicators
        )

    def technical(self, member: UniverseMember) -> FeatureBlock:
        ind = self.static.indicators
        hourly = self.cmc_1h.get(member.cmc_id, Bars.empty(INTERVAL_MS["1h"]))
        hourly = hourly.since(to_ms(self.as_of - timedelta(days=ind.technical.cmc_history_days))).scaled(
            member.multiplier
        )
        k15 = self.klines_15m(member).since(
            to_ms(self.as_of - timedelta(days=ind.technical.binance_history_days))
        )
        bars = {"15m": k15, "1h": hourly, "4h": resample(hourly, "4h"), "1d": resample(hourly, "1d")}
        _, quotes, _ = self.quotes
        quote = quotes.get(member.cmc_id)
        perf = self.perf.get(str(member.cmc_id))
        periods = perf.get("periods") if isinstance(perf, Mapping) else None
        return technical_features(bars, periods, quote.price if quote else None, ind)

    def micro(self, member: UniverseMember) -> FeatureBlock:
        ind = self.static.indicators
        windows = tuple(dict.fromkeys((*ind.cvd_windows, *ind.micro.taker_windows)))
        block = flow_features(
            self.flow, member.binance_symbol, self.as_of, windows, self.binance_oi_usd(member)
        )
        return block.merge(book_features(self.book(member), ind))

    def fundamental(self, member: UniverseMember) -> FeatureBlock:
        _, quotes, _ = self.quotes
        block = fundamental_features(
            quotes.get(member.cmc_id), self.core(member).oi_now, self.new_ids, member.cmc_id, self.as_of
        )
        return block.merge(dex_features(self.view, member.cmc_id, self.as_of))

    def macro_block(self, member: UniverseMember) -> FeatureBlock:
        """Market-wide context (same for every coin), copied so callers cannot mutate the shared block."""
        return _copy(self.macro)

    def agent_block(self, agent: AgentName, member: UniverseMember) -> FeatureBlock:
        families: dict[AgentName, Callable[[UniverseMember], FeatureBlock]] = {
            AgentName.CROWDING: self.crowding,
            AgentName.TECHNICAL: self.technical,
            AgentName.MICRO: self.micro,
            AgentName.FUNDAMENTAL: self.fundamental,
            AgentName.MACRO: self.macro_block,
        }
        family = families[agent](member)
        block = FeatureBlock().merge(self.common(member)).merge(family).merge(self.reconcile(member))
        return block.select(block.values)

    # ------------------------------------------------------------------ levels

    def level_inputs(self, member: UniverseMember) -> LevelInputs:
        ind = self.static.indicators.levels
        k15 = self.klines_15m(member).since(to_ms(self.as_of - timedelta(days=ind.lookback_days)))
        row = self.premium(member)
        atr, _ = self.atr_1h(member)
        tick = tick_size(self.exchange_info, member.binance_symbol)
        return LevelInputs(
            mark=row.mark if row is not None else None,
            atr=atr,
            tick=tick,
            bars_1h=resample(k15, "1h"),
            bars_4h=resample(k15, "4h"),
            book=self.book(member),
        )

    def candidate_set(self, member: UniverseMember, rules: LevelRules) -> CandidateSet:
        key = (member.cmc_id, rules)
        cached = self._levels.get(key)
        if cached is None:
            cached = candidate_set(
                member.cmc_id, self.as_of, self.level_inputs(member), self.static.indicators.levels, rules
            )
            self._levels[key] = cached
        return cached


class _LazyOi(Mapping[str, float]):
    """Binance symbol -> direct OI in USD, computed on access (only symbols actually needed)."""

    def __init__(self, snap: Snapshot) -> None:
        self._snap = snap
        self._by_symbol = {m.binance_symbol: m for m in snap.universe.members}

    def __getitem__(self, symbol: str) -> float:
        member = self._by_symbol.get(symbol)
        value = self._snap.binance_oi_usd(member) if member is not None else None
        if value is None:
            raise KeyError(symbol)
        return value

    def __iter__(self) -> Iterator[str]:
        return iter(self._by_symbol)

    def __len__(self) -> int:
        return len(self._by_symbol)


def _copy(block: FeatureBlock) -> FeatureBlock:
    return FeatureBlock().merge(block)


def _mark(block: FeatureBlock, name: str, row: PremiumRow | None, as_of: datetime, stale: timedelta) -> None:
    if row is None or row.mark is None:
        block.set(name, None)
        block.flag(DataQualityFlag.MISSING_ROUTE, name)
        return
    block.set(name, row.mark)
    if (as_of.timestamp() * 1000 - row.time_ms) / 1000.0 > stale.total_seconds():
        block.flag(DataQualityFlag.STALE, name)


def quote_volume_1h(k15: Bars, as_of: datetime, stale: timedelta) -> float | None:
    """Binance quote volume of the last closed hour: the 4 newest closed 15m bars, contiguous and recent."""
    if len(k15) < 4:
        return None
    tail = k15.tail(4)
    step = INTERVAL_MS["15m"]
    if np.any(np.diff(tail.open_time) != step):
        return None
    last_close = int(tail.close_time[-1])
    if to_ms(as_of) - last_close > step + int(stale.total_seconds() * 1000):
        return None
    return finite(float(np.sum(tail.quote_volume)))


def beta_from_bars(coin: Bars, btc: Bars, min_bars: int) -> float | None:
    """OLS beta of the coin's 1h log returns on BTC's, over hours both have (consecutive bars only)."""

    def returns(bars: Bars) -> dict[int, float]:
        out: dict[int, float] = {}
        for i in range(1, len(bars)):
            if (
                bars.open_time[i] - bars.open_time[i - 1] == bars.interval_ms
                and bars.close[i - 1] > 0
                and bars.close[i] > 0
            ):
                out[int(bars.open_time[i])] = math.log(float(bars.close[i]) / float(bars.close[i - 1]))
        return out

    rc, rb = returns(coin), returns(btc)
    keys = sorted(rc.keys() & rb.keys())
    if len(keys) < min_bars:
        return None
    x = np.array([rb[k] for k in keys])
    y = np.array([rc[k] for k in keys])
    var = float(np.var(x, ddof=1))
    if var <= 0:
        return None
    return finite(float(np.cov(y, x, ddof=1)[0, 1]) / var)


class FeatureEngine:
    """Owns the lake view and a small LRU of snapshots (the scanner and quant_core share them)."""

    def __init__(
        self, view: LakeView, static: StaticConfig, scanner: ScannerFile, *, max_snapshots: int = 4
    ) -> None:
        self.view = view
        self.static = static
        self.scanner = scanner
        self._snapshots: OrderedDict[tuple[str, str, str], Snapshot] = OrderedDict()
        self._max = max_snapshots

    @property
    def feature_ver(self) -> str:
        return feature_version(self.static, self.scanner)

    def snapshot(self, as_of: datetime, universe: Universe, routes: CmcRoutesFile) -> Snapshot:
        as_of = ensure_utc(as_of)
        key = (
            as_of.isoformat(),
            f"{universe.date.isoformat()}@{universe.built_at.isoformat()}",
            str(cohort_sizes(routes)),
        )
        found = self._snapshots.get(key)
        if found is not None:
            self._snapshots.move_to_end(key)
            return found
        snap = Snapshot(self, as_of, universe, routes)
        self._snapshots[key] = snap
        if len(self._snapshots) > self._max:
            self._snapshots.popitem(last=False)
        return snap
