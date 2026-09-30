"""Crowding features (Crowding agent: LTX + Migration), per coin and cross-sectional.

- L{1h,4h,24h} long / short / total, Skew{1h,4h,24h}, SPIKE, DECAY (see `liquidations`);
- OI: the coin's USD open interest summed over the route #2 exchanges (core + peripheral); `OI(t-h)` and
  dOI use only exchanges whose list is complete at both times, so a venue dropping out is never a flush;
- FD = L4h / OI(t - 4h); FDz = cross-sectional z of FD over the PIT LTX universe of `universe_date`
  (a coin outside the cross-section is scored against the cross-section's mean and std);
- F_c: OI-weighted hourly funding over venues with a known interval (Binance direct: premium index rate
  with the resolved interval; CMC route #2 venues have no interval source: excluded and flagged);
  dF_c = F_c(t) - F_c(t - lookback); `funding_drop` = relative drop of F_c (see `funding`);
- VEX_e = exchange e's share of market-wide 4h liquidations (route #5); `vex_binance`, `vex_max`;
- B (spread of the flush) = share of the LTX cross-section without BTC with a same-side SPIKE >=
  `contagion.breadth_spike_min`; BTC flush state per side;
- `liq_binance_lb_{1h,4h}_usd`: Binance forceOrder lower bound of the coin's liquidations;
- Migration: S_P, zM, G, BG, OI HHI (see `migration`).
"""

from __future__ import annotations

import statistics
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta

from hdt.contracts.common import DataQualityFlag
from hdt.core.config import IndicatorsFile, LiquidationParams, ScannerFile
from hdt.features.common import FeatureBlock, change, cross_section_z, ratio
from hdt.features.funding import FundingState, VenueFunding, funding_drop, weighted_funding
from hdt.features.lake_io import LakeView
from hdt.features.liquidations import (
    CoinLiq,
    ForceLowerBound,
    HourlyLiq,
    LiqSnapshot,
    Spike,
    parse_liq_quote,
    skew,
    spike_decay,
    spike_floor,
)
from hdt.features.ltx import (
    Breadth,
    LtxEval,
    LtxInputs,
    LtxSide,
    breadth,
    btc_flushed,
    contagion_blocked,
    evaluate,
)
from hdt.features.migration import (
    Cohorts,
    ExchangeBook,
    MigrationEval,
    MigrationMetrics,
    metrics,
    outlier_venues,
    zm,
)
from hdt.features.migration import evaluate as evaluate_migration

BY_EXCHANGE_ROUTE = "liquidations_by_exchange"
BINANCE_SLUG = "binance"
LIQ_NAMES = tuple(f"liq_{side}_{w}_usd" for side in ("total", "long", "short") for w in ("1h", "4h", "24h"))
SPIKE_NAMES = (
    "skew_1h",
    "skew_4h",
    "skew_24h",
    "spike",
    "decay",
    "spike_den_usd",
    "spike_floor_usd",
    "spike_valid",
)
OI_NAMES = ("oi_agg_usd", "fd", "fdz", "doi_1h", "doi_4h")
FUNDING_NAMES = ("f_c_per_h", "f_c_per_h_before", "df_c_per_h", "funding_drop", "funding_interval_h")
MIGRATION_NAMES = ("s_p", "zm", "g", "bg", "oi_hhi", "oi_core_usd", "oi_peripheral_usd")


@dataclass(frozen=True)
class VexShares:
    binance: float | None
    max_share: float | None
    max_exchange: str | None


def read_vex(view: LakeView, as_of: datetime, params: LiquidationParams) -> VexShares | None:
    listing = view.latest_list(
        BY_EXCHANGE_ROUTE,
        as_of,
        items_field="exchanges",
        paging="has_more",
        lookback=timedelta(seconds=params.list_cycle_window_s) + timedelta(hours=1),
        cycle_window=timedelta(seconds=params.list_cycle_window_s),
        max_pages=params.list_max_pages,
    )
    if not listing.complete:
        return None
    shares: dict[str, float] = {}
    for item in listing.items:
        if not isinstance(item, Mapping):
            continue
        slug = item.get("exchange_slug") or item.get("slug")
        quotes = item.get("quotes")
        quote = quotes[0] if isinstance(quotes, list) and quotes and isinstance(quotes[0], Mapping) else None
        parsed = parse_liq_quote(quote) if quote is not None else None
        if isinstance(slug, str) and parsed is not None:
            shares[slug] = shares.get(slug, 0.0) + parsed.total_4h
    total = sum(shares.values())
    if total <= 0:
        return VexShares(0.0 if shares or listing.complete else None, None, None)
    top = max(sorted(shares), key=lambda s: shares[s])
    return VexShares(shares.get(BINANCE_SLUG, 0.0) / total, shares[top] / total, top)


def oi_pair(
    now: Mapping[str, ExchangeBook], then: Mapping[str, ExchangeBook], coin_id: int, slugs: Sequence[str]
) -> tuple[float | None, float | None]:
    """The coin's OI at both times over the exchanges complete at both times (None when none is)."""
    both = [s for s in slugs if s in now and s in then and now[s].complete and then[s].complete]
    if not both:
        return None, None
    a = sum(now[s].coins[coin_id].oi_usd for s in both if coin_id in now[s].coins)
    b = sum(then[s].coins[coin_id].oi_usd for s in both if coin_id in then[s].coins)
    return (a if a > 0 else None), (b if b > 0 else None)


def oi_at(books: Mapping[str, ExchangeBook], coin_id: int, slugs: Sequence[str]) -> float | None:
    complete = [s for s in slugs if s in books and books[s].complete]
    if not complete:
        return None
    total = sum(books[s].coins[coin_id].oi_usd for s in complete if coin_id in books[s].coins)
    return total if total > 0 else None


@dataclass(frozen=True)
class CrowdingInputs:
    """Snapshot-level inputs shared by every coin at one `as_of`."""

    liq: LiqSnapshot
    liq_hist: HourlyLiq
    books: Mapping[str, Mapping[str, ExchangeBook]]
    """Label -> exchange -> book; labels `now`, `outlier`, `h1`, `h4`, `growth`."""
    groups: Cohorts | None
    sp_hist: Mapping[int, Sequence[float]]
    vex: VexShares | None
    force_lb: Mapping[str, ForceLowerBound]
    binance_oi: Mapping[str, float]
    """Binance symbol -> direct open interest in USD at `as_of` (funding weight fallback)."""
    funding_now: FundingState
    funding_before: FundingState
    stale: timedelta
    as_of: datetime

    @property
    def slugs(self) -> tuple[str, ...]:
        return self.groups.all if self.groups is not None else ()

    def books_complete(self, label: str) -> bool:
        books = self.books.get(label, {})
        return bool(self.slugs) and all(s in books and books[s].complete for s in self.slugs)


@dataclass(frozen=True)
class CoinCrowding:
    """Per-coin crowding numbers before cross-sectional steps."""

    coin_id: int
    symbol: str
    liq: CoinLiq | None
    spike: Spike | None
    floor_c: float | None
    oi_now: float | None
    oi_4h: float | None
    doi_1h: float | None
    doi_4h: float | None
    fd: float | None
    f_now: float | None
    f_before: float | None
    interval_h: float | None
    interval_changed: bool
    excluded_venues: tuple[str, ...]
    migration: MigrationMetrics | None
    zm: float | None
    lb: ForceLowerBound | None

    @property
    def skew4(self) -> float | None:
        return skew(self.liq.long_4h, self.liq.total_4h) if self.liq is not None else None


def coin_crowding(
    coin_id: int, symbol: str, x: CrowdingInputs, indicators: IndicatorsFile, scanner: ScannerFile
) -> CoinCrowding:
    liq = x.liq.coin(coin_id)
    slugs = x.slugs
    books_now = x.books.get("now", {})
    oi_now = oi_at(books_now, coin_id, slugs)
    oi_4h = oi_at(x.books.get("h4", {}), coin_id, slugs)
    a1, b1 = oi_pair(books_now, x.books.get("h1", {}), coin_id, slugs)
    a4, b4 = oi_pair(books_now, x.books.get("h4", {}), coin_id, slugs)
    floor_c = spike_floor(
        x.liq_hist.samples(coin_id), scanner.spike, indicators.liquidations.floor_history_min_samples
    )
    spike = spike_decay(liq, oi_4h, floor_c, scanner.spike) if liq is not None else None
    fd = ratio(liq.total_4h, oi_4h) if liq is not None else None
    f_now, f_before, interval_h, changed, excluded = _funding(coin_id, symbol, x)
    migration: MigrationMetrics | None = None
    z_m: float | None = None
    if x.groups is not None:
        skip = outlier_venues(books_now, x.books.get("outlier", {}), coin_id, indicators)
        migration = metrics(books_now, x.books.get("growth", {}), x.groups, coin_id, skip)
        z_m = zm(migration.s_p, x.sp_hist.get(coin_id, ()))
    return CoinCrowding(
        coin_id=coin_id,
        symbol=symbol,
        liq=liq,
        spike=spike,
        floor_c=floor_c,
        oi_now=oi_now,
        oi_4h=oi_4h,
        doi_1h=change(a1, b1),
        doi_4h=change(a4, b4),
        fd=fd,
        f_now=f_now,
        f_before=f_before,
        interval_h=interval_h,
        interval_changed=changed,
        excluded_venues=excluded,
        migration=migration,
        zm=z_m,
        lb=x.force_lb.get(symbol),
    )


def _funding(
    coin_id: int, symbol: str, x: CrowdingInputs
) -> tuple[float | None, float | None, float | None, bool, tuple[str, ...]]:
    def venues(
        state: FundingState, books: Mapping[str, ExchangeBook]
    ) -> tuple[list[VenueFunding], float | None, bool]:
        row = state.rows.get(symbol)
        resolution = state.interval(symbol)
        binance_book = books.get(BINANCE_SLUG)
        binance_oi = (
            binance_book.coins[coin_id].oi_usd
            if binance_book is not None and binance_book.complete and coin_id in binance_book.coins
            else x.binance_oi.get(symbol)
        )
        out = [VenueFunding(BINANCE_SLUG, row.rate if row else None, resolution.hours, binance_oi)]
        for slug, book in books.items():
            venue = book.coins.get(coin_id) if slug != BINANCE_SLUG and book.complete else None
            if venue is not None and venue.funding_rate is not None:
                out.append(VenueFunding(slug, venue.funding_rate, None, venue.oi_usd))
        return out, resolution.hours, resolution.changed

    now_venues, interval_h, changed = venues(x.funding_now, x.books.get("now", {}))
    before_venues, _, changed_before = venues(x.funding_before, x.books.get("h4", {}))
    f_now, excluded = weighted_funding(now_venues)
    f_before, _ = weighted_funding(before_venues)
    return f_now, f_before, interval_h, changed or changed_before, tuple(excluded)


@dataclass(frozen=True)
class CrossSection:
    fd_mean: float | None
    fd_std: float | None
    fdz: Mapping[int, float | None]
    breadth: Breadth
    btc_flush: Mapping[str, bool]


def cross_section(
    cores: Mapping[int, CoinCrowding],
    ltx_ids: Sequence[int],
    btc_id: int | None,
    indicators: IndicatorsFile,
    scanner: ScannerFile,
) -> CrossSection:
    fds = {cid: cores[cid].fd for cid in ltx_ids if cid in cores}
    fdz = cross_section_z(fds, indicators.fdz_min_coins)
    present = [v for v in fds.values() if v is not None]
    mean = std = None
    if len(present) >= indicators.fdz_min_coins:
        mean = statistics.fmean(present)
        std = statistics.stdev(present)
    pairs: list[tuple[float | None, float | None]] = []
    for cid in ltx_ids:
        core = cores.get(cid)
        if core is None or cid == btc_id or core.liq is None:
            continue
        pairs.append((core.spike.spike if core.spike is not None else None, core.skew4))
    b = breadth(pairs, scanner.contagion)
    flush = {"LONG": False, "SHORT": False}
    btc = cores.get(btc_id) if btc_id is not None else None
    if btc is not None and btc.spike is not None:
        sides: tuple[LtxSide, ...] = ("LONG", "SHORT")
        for side in sides:
            flush[side] = btc_flushed(btc.spike.spike, btc.spike.decay, btc.skew4, side, scanner.contagion)
    return CrossSection(mean, std if std else None, fdz, b, flush)


def fdz_of(core: CoinCrowding, cs: CrossSection) -> float | None:
    if core.coin_id in cs.fdz:
        return cs.fdz[core.coin_id]
    if core.fd is None or cs.fd_mean is None or not cs.fd_std:
        return None
    return (core.fd - cs.fd_mean) / cs.fd_std


@dataclass(frozen=True)
class RuleView:
    ltx: LtxEval
    ltx_inputs: LtxInputs
    blocked: bool | None
    migration: MigrationEval | None
    migration_loose: MigrationEval | None


def rules(core: CoinCrowding, cs: CrossSection, scanner: ScannerFile, min_abs_per_h: float) -> RuleView:
    inputs = LtxInputs(
        skew4=core.skew4,
        spike=core.spike.spike if core.spike else None,
        decay=core.spike.decay if core.spike else None,
        fdz=fdz_of(core, cs),
        doi4=core.doi_4h,
        funding_drop=funding_drop(core.f_now, core.f_before, min_abs_per_h),
    )
    ev = evaluate(inputs, scanner.ltx)
    blocked = (
        contagion_blocked(ev.side, cs.breadth, cs.btc_flush[ev.side], scanner.contagion) if ev.side else None
    )
    m = core.migration
    strict = loose = None
    if m is not None:
        strict = evaluate_migration(m.s_p, core.zm, m.g, m.bg, scanner.migration.strict)
        loose = evaluate_migration(m.s_p, core.zm, m.g, m.bg, scanner.migration.loose)
    return RuleView(ev, inputs, blocked, strict, loose)


def crowding_block(
    core: CoinCrowding, cs: CrossSection, x: CrowdingInputs, scanner: ScannerFile, indicators: IndicatorsFile
) -> FeatureBlock:
    block = FeatureBlock()
    liq = core.liq
    for side in ("total", "long", "short"):
        for w in ("1h", "4h", "24h"):
            block.set(f"liq_{side}_{w}_usd", getattr(liq, f"{side}_{w}") if liq is not None else None)
    for w in ("1h", "4h", "24h"):
        block.set(f"skew_{w}", skew(liq.long(w), liq.total(w)) if liq is not None else None)
    spike = core.spike
    block.set("spike", spike.spike if spike else None)
    block.set("decay", spike.decay if spike else None)
    block.set("spike_den_usd", spike.den if spike else None)
    block.set("spike_floor_usd", core.floor_c)
    block.set("spike_valid", spike.valid if spike else None)
    if x.liq.status != "complete":
        block.flag(DataQualityFlag.MISSING_ROUTE, *LIQ_NAMES, *SPIKE_NAMES, "fd", "fdz")
    elif x.liq.fetched_at is not None and x.as_of - x.liq.fetched_at > x.stale:
        block.flag(DataQualityFlag.STALE, *LIQ_NAMES, *SPIKE_NAMES, "fd", "fdz")
    if spike is not None and spike.floor_applied:
        block.flag(DataQualityFlag.SPIKE_FLOOR_APPLIED, "spike")
    block.set("oi_agg_usd", core.oi_now)
    block.set("fd", core.fd)
    block.set("fdz", fdz_of(core, cs))
    block.set("doi_1h", core.doi_1h)
    block.set("doi_4h", core.doi_4h)
    if not x.books_complete("now") or not x.books_complete("h4"):
        block.flag(DataQualityFlag.MISSING_ROUTE, *OI_NAMES, *MIGRATION_NAMES)
    block.set("f_c_per_h", core.f_now)
    block.set("f_c_per_h_before", core.f_before)
    block.set(
        "df_c_per_h",
        core.f_now - core.f_before if core.f_now is not None and core.f_before is not None else None,
    )
    block.set("funding_drop", funding_drop(core.f_now, core.f_before, indicators.funding.min_abs_per_h))
    block.set("funding_interval_h", core.interval_h)
    if core.interval_changed:
        block.flag(DataQualityFlag.FUNDING_INTERVAL_CHANGED, *FUNDING_NAMES)
    if core.excluded_venues or core.interval_h is None:
        block.flag(DataQualityFlag.FUNDING_INTERVAL_UNKNOWN, *FUNDING_NAMES)
    vex = x.vex
    block.set("vex_binance", vex.binance if vex else None)
    block.set("vex_max", vex.max_share if vex else None)
    block.set("vex_max_exchange", vex.max_exchange if vex else None)
    if vex is None:
        block.flag(DataQualityFlag.MISSING_ROUTE, "vex_binance", "vex_max", "vex_max_exchange")
    block.set("breadth_long", cs.breadth.long)
    block.set("breadth_short", cs.breadth.short)
    block.set("btc_flushed_long", cs.btc_flush["LONG"])
    block.set("btc_flushed_short", cs.btc_flush["SHORT"])
    lb = core.lb
    block.set("liq_binance_lb_1h_usd", lb.total_1h if lb else 0.0)
    block.set("liq_binance_lb_4h_usd", lb.total_4h if lb else 0.0)
    m = core.migration
    block.set("s_p", m.s_p if m else None)
    block.set("zm", core.zm)
    block.set("g", m.g if m else None)
    block.set("bg", m.bg if m else None)
    block.set("oi_hhi", m.hhi if m else None)
    block.set("oi_core_usd", m.oi_core_usd if m else None)
    block.set("oi_peripheral_usd", m.oi_peripheral_usd if m else None)
    if m is None:
        block.flag(DataQualityFlag.MISSING_ROUTE, *MIGRATION_NAMES)
    elif m.outlier_venues:
        block.flag(DataQualityFlag.OUTLIER, *MIGRATION_NAMES)
    view = rules(core, cs, scanner, indicators.funding.min_abs_per_h)
    block.set("ltx_side", view.ltx.side)
    block.set("ltx_strict_pass", view.ltx.strict_pass and view.blocked is False)
    block.set("ltx_loose_pass", view.ltx.loose_pass and view.blocked is False)
    block.set("ltx_contagion_blocked", view.blocked)
    block.set("migration_side", view.migration.side if view.migration else None)
    block.set("migration_strict_pass", view.migration.passed if view.migration else None)
    block.set("migration_loose_pass", view.migration_loose.passed if view.migration_loose else None)
    return block
