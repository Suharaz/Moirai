"""Leverage Migration features from CMC route #2 (market pairs per exchange) and route #1 (exchanges).

- Cohorts: route #1 exchanges ranked by `liquidity_score`, frozen at the first successful record of the UTC
  month (the same rule the recorder uses to pick the route #2 exchanges): top `core_exchanges` = core, the
  next `peripheral_exchanges` = peripheral. Point in time: the month's first record at or before `as_of`,
  else the newest earlier record.
- Per exchange and coin, perpetual pairs only; pairs with `outlier_detected` or any `exclusions` are dropped.
  OI is the USD open interest from `quotes`; the basis is self-computed `(price - index) / index` from the
  exchange-reported quote (the CMC `index_basis` unit is unverified).
- S_P = peripheral OI / (core + peripheral OI); zM = z of S_P against hourly samples over
  `migration_zm_window_days`; G = peripheral OI growth - core OI growth over `migration_growth_window_h`;
  BG = OI-weighted basis(peripheral) - basis(core); HHI = sum of squared venue OI shares.
- A venue whose OI jumps by more than `outlier_oi_jump_frac` within `outlier_window_min` while the price
  moved less than `outlier_price_move_max` is treated as a data outlier (`outlier`) and left out.
"""

from __future__ import annotations

import math
import statistics
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import partial
from typing import Any

from hdt.core.config import CmcRoutesFile, IndicatorsFile, MigrationThresholds
from hdt.features.lake_io import CMC, LakeView, ListStatus, as_float

PAIRS_ROUTE = "exchange_derivative_market_pairs"
EXCHANGES_ROUTE = "derivatives_exchanges"
_HOUR = timedelta(hours=1)


@dataclass(frozen=True)
class VenueCoin:
    """One coin's perpetual market on one exchange (sums / OI-weighted means over its pairs)."""

    oi_usd: float
    price: float | None
    basis: float | None
    funding_rate: float | None
    """Per-period rate as reported by CMC (interval unknown)."""
    volume_24h_usd: float | None


@dataclass(frozen=True)
class ExchangeBook:
    slug: str
    status: ListStatus
    fetched_at: datetime | None
    coins: Mapping[int, VenueCoin]

    @property
    def complete(self) -> bool:
        return self.status == "complete"


def read_book(view: LakeView, slug: str, as_of: datetime, indicators: IndicatorsFile) -> ExchangeBook:
    params = indicators.liquidations
    listing = view.latest_list(
        PAIRS_ROUTE,
        as_of,
        base_key=slug,
        items_field="market_pairs",
        paging="start_limit",
        lookback=timedelta(hours=2),
        cycle_window=timedelta(seconds=params.list_cycle_window_s),
        max_pages=params.list_max_pages,
    )
    if not listing.complete:
        return ExchangeBook(slug, listing.status, listing.fetched_at, {})
    return ExchangeBook(slug, "complete", listing.fetched_at, parse_pairs(listing.items))


def parse_pairs(items: Iterable[Any]) -> dict[int, VenueCoin]:
    acc: dict[int, list[tuple[float, float | None, float | None, float | None, float | None]]] = {}
    for pair in items:
        if not isinstance(pair, Mapping) or pair.get("category") != "perpetual":
            continue
        if pair.get("outlier_detected") is True or pair.get("exclusions"):
            continue
        base = pair.get("market_pair_base")
        coin_id = base.get("crypto_id") if isinstance(base, Mapping) else None
        if not isinstance(coin_id, int) or isinstance(coin_id, bool):
            continue
        usd = _first(pair.get("quotes"))
        reported = _first(pair.get("exchange_reported_quotes"))
        oi = as_float(usd.get("open_interest")) if usd else None
        if oi is None or oi <= 0:
            continue
        price = as_float(reported.get("price")) if reported else None
        index = as_float(reported.get("index_price")) if reported else None
        basis = (price - index) / index if price is not None and index else None
        funding = as_float(reported.get("funding_rate")) if reported else None
        volume = as_float(usd.get("volume_24h")) if usd else None
        acc.setdefault(coin_id, []).append(
            (oi, as_float(usd.get("price")) if usd else None, basis, funding, volume)
        )
    return {coin_id: _combine(rows) for coin_id, rows in acc.items()}


def _first(value: Any) -> Mapping[str, Any] | None:
    if isinstance(value, list):
        for entry in value:
            if isinstance(entry, Mapping):
                return entry
    return value if isinstance(value, Mapping) else None


def _combine(
    rows: Sequence[tuple[float, float | None, float | None, float | None, float | None]],
) -> VenueCoin:
    oi = math.fsum(r[0] for r in rows)

    def weighted(i: int) -> float | None:
        pairs = [(r[0], v) for r in rows if (v := r[i]) is not None]
        den = math.fsum(w for w, _ in pairs)
        return math.fsum(w * v for w, v in pairs) / den if den > 0 else None

    volumes = [v for r in rows if (v := r[4]) is not None]
    return VenueCoin(oi, weighted(1), weighted(2), weighted(3), math.fsum(volumes) if volumes else None)


# --------------------------------------------------------------------------- cohorts


@dataclass(frozen=True)
class Cohorts:
    core: tuple[str, ...]
    peripheral: tuple[str, ...]

    @property
    def all(self) -> tuple[str, ...]:
        return self.core + self.peripheral

    @property
    def key(self) -> str:
        return ",".join(self.core) + "|" + ",".join(self.peripheral)


def cohort_sizes(routes: CmcRoutesFile) -> tuple[int, int]:
    """(core, peripheral) exchange counts of route #2 (0, 0 when the route is not configured)."""
    spec = next((r for r in routes.routes if r.name == PAIRS_ROUTE and r.enabled), None)
    if spec is None:
        return 0, 0
    return int(spec.params.get("core_exchanges", 0)), int(spec.params.get("peripheral_exchanges", 0))


def cohorts(view: LakeView, as_of: datetime, routes: CmcRoutesFile) -> Cohorts | None:
    core_n, peripheral_n = cohort_sizes(routes)
    if core_n + peripheral_n == 0:
        return None
    month_start = as_of.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    fact = view.facts.get(("cohorts", month_start))
    if fact is not None and fact[0] <= as_of:
        ranked = fact[1]
    else:
        found = _month_first_ranking(view, month_start, as_of)
        if found is not None:
            view.facts[("cohorts", month_start)] = found
            ranked = found[1]
        else:
            _, data = view.latest_cmc_ok(EXCHANGES_ROUTE, month_start, lookback=timedelta(days=35))
            ranked = rank_exchanges(data)
    if not ranked:
        return None
    return Cohorts(tuple(ranked[:core_n]), tuple(ranked[core_n : core_n + peripheral_n]))


def _month_first_ranking(
    view: LakeView, month_start: datetime, as_of: datetime
) -> tuple[datetime, list[str]] | None:
    """The ranking of the month's first successful route #1 record at or before `as_of`.

    Once found it is frozen for the month (the lake is append-only by fetch time), so it is kept in
    `view.facts` and reused for every later `as_of` of that month.
    """
    hour = month_start
    while hour <= as_of:
        end = min(hour + _HOUR - timedelta(microseconds=1), as_of)
        for record in view.records(CMC, EXCHANGES_ROUTE, hour, end, as_of=as_of):
            ranked = rank_exchanges(view.cmc_data(record))
            if ranked:
                return record.fetched_at, ranked
        hour += _HOUR
    return None


def rank_exchanges(data: Any) -> list[str]:
    exchanges = data.get("exchanges") if isinstance(data, Mapping) else None
    refs: list[tuple[bool, float, str]] = []
    for item in exchanges if isinstance(exchanges, list) else []:
        if not isinstance(item, Mapping) or not isinstance(item.get("exchange_slug"), str):
            continue
        score = as_float(item.get("liquidity_score"))
        refs.append((score is None, -(score or 0.0), item["exchange_slug"]))
    refs.sort()
    return [slug for _, _, slug in refs]


# --------------------------------------------------------------------------- metrics


@dataclass(frozen=True)
class MigrationMetrics:
    s_p: float | None
    g: float | None
    bg: float | None
    hhi: float | None
    oi_core_usd: float | None
    oi_peripheral_usd: float | None
    outlier_venues: tuple[str, ...]


def outlier_venues(
    now: Mapping[str, ExchangeBook],
    before: Mapping[str, ExchangeBook],
    coin_id: int,
    indicators: IndicatorsFile,
) -> set[str]:
    rec = indicators.reconcile
    out: set[str] = set()
    for slug, book in now.items():
        prev = before.get(slug)
        cur = book.coins.get(coin_id)
        old = prev.coins.get(coin_id) if prev is not None and prev.complete else None
        if cur is None or old is None or old.oi_usd <= 0:
            continue
        jump = abs(cur.oi_usd / old.oi_usd - 1.0)
        moved = abs(cur.price / old.price - 1.0) if cur.price and old.price else None
        if jump > rec.outlier_oi_jump_frac and moved is not None and moved < rec.outlier_price_move_max:
            out.add(slug)
    return out


def share_peripheral(
    books: Mapping[str, ExchangeBook], groups: Cohorts, coin_id: int, skip: set[str]
) -> float | None:
    core = _cohort_oi(books, groups.core, coin_id, skip)
    peri = _cohort_oi(books, groups.peripheral, coin_id, skip)
    if core is None or peri is None or core + peri <= 0:
        return None
    return peri / (core + peri)


def _cohort_oi(
    books: Mapping[str, ExchangeBook], slugs: Sequence[str], coin_id: int, skip: set[str]
) -> float | None:
    """Sum of the coin's OI over the cohort; None when any cohort exchange's list is not complete."""
    total = 0.0
    for slug in slugs:
        if slug in skip:
            continue
        book = books.get(slug)
        if book is None or not book.complete:
            return None
        venue = book.coins.get(coin_id)
        total += venue.oi_usd if venue is not None else 0.0
    return total


def _cohort_basis(
    books: Mapping[str, ExchangeBook], slugs: Sequence[str], coin_id: int, skip: set[str]
) -> float | None:
    num = den = 0.0
    for slug in slugs:
        book = books.get(slug)
        venue = book.coins.get(coin_id) if book is not None and book.complete and slug not in skip else None
        if venue is not None and venue.basis is not None:
            num += venue.oi_usd * venue.basis
            den += venue.oi_usd
    return num / den if den > 0 else None


def metrics(
    now: Mapping[str, ExchangeBook],
    growth_base: Mapping[str, ExchangeBook],
    groups: Cohorts,
    coin_id: int,
    skip: set[str],
) -> MigrationMetrics:
    core = _cohort_oi(now, groups.core, coin_id, skip)
    peri = _cohort_oi(now, groups.peripheral, coin_id, skip)
    s_p = peri / (core + peri) if core is not None and peri is not None and core + peri > 0 else None
    core_0 = _cohort_oi(growth_base, groups.core, coin_id, skip)
    peri_0 = _cohort_oi(growth_base, groups.peripheral, coin_id, skip)
    g = None
    if core is not None and peri is not None and core_0 and peri_0:
        g = (peri / peri_0 - 1.0) - (core / core_0 - 1.0)
    basis_core = _cohort_basis(now, groups.core, coin_id, skip)
    basis_peri = _cohort_basis(now, groups.peripheral, coin_id, skip)
    bg = basis_peri - basis_core if basis_core is not None and basis_peri is not None else None
    ois = [
        now[slug].coins[coin_id].oi_usd
        for slug in groups.all
        if slug not in skip and slug in now and now[slug].complete and coin_id in now[slug].coins
    ]
    total = sum(ois)
    hhi = sum((x / total) ** 2 for x in ois) if total > 0 else None
    return MigrationMetrics(s_p, g, bg, hhi, core, peri, tuple(sorted(skip)))


def sp_history(
    view: LakeView, as_of: datetime, groups: Cohorts, indicators: IndicatorsFile, coin_ids: Sequence[int]
) -> dict[int, list[float]]:
    """Hourly S_P samples per coin over `migration_zm_window_days` (excluding the current value)."""
    end = as_of.replace(minute=0, second=0, microsecond=0)
    boundary = end - timedelta(days=indicators.migration_zm_window_days)
    out: dict[int, list[float]] = {coin_id: [] for coin_id in coin_ids}
    name = f"sp_sample:{groups.key}"
    while boundary <= end:
        sample = view.per_hour(
            name, boundary - _HOUR, partial(_sp_sample, view, boundary, groups, indicators)
        )
        for coin_id in coin_ids:
            value = sample.get(coin_id)
            if value is not None:
                out[coin_id].append(value)
        boundary += _HOUR
    return out


def _sp_sample(
    view: LakeView, boundary: datetime, groups: Cohorts, indicators: IndicatorsFile
) -> dict[int, float]:
    books = {slug: read_book(view, slug, boundary, indicators) for slug in groups.all}
    coin_ids = {coin_id for book in books.values() for coin_id in book.coins}
    out: dict[int, float] = {}
    for coin_id in coin_ids:
        value = share_peripheral(books, groups, coin_id, set())
        if value is not None:
            out[coin_id] = value
    return out


def zm(current: float | None, history: Sequence[float]) -> float | None:
    """z of the current S_P against its hourly history (the history excludes the current value)."""
    if current is None or len(history) < 3:
        return None
    mean = statistics.fmean(history)
    std = statistics.stdev(history)
    return (current - mean) / std if std > 0 else None


# --------------------------------------------------------------------------- rule


@dataclass(frozen=True)
class MigrationEval:
    passed: bool
    side: str | None
    """SHORT when the peripheral basis is richer than core (BG > 0), else LONG."""
    conditions: dict[str, bool | None]


def evaluate(
    s_p: float | None, z_m: float | None, g: float | None, bg: float | None, t: MigrationThresholds
) -> MigrationEval:
    conditions: dict[str, bool | None] = {
        "s_p": None if s_p is None else s_p >= t.sp_min,
        "zm": None if z_m is None else z_m >= t.zm_min,
        "g": None if g is None else g >= t.g_min,
        "bg": None if bg is None else abs(bg) >= t.bg_abs_min,
    }
    passed = all(v is True for v in conditions.values())
    side = None if bg is None or bg == 0 else ("SHORT" if bg > 0 else "LONG")
    return MigrationEval(passed and side is not None, side, conditions)
