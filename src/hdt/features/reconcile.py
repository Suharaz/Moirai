"""Reconciliation features, included in every non-news packet.

- mark: Binance mark vs the CMC aggregate price (scaled to contract units): `price_gap_cmc_binance`;
- funding: CMC's Binance-pair rate per hour (Binance interval) vs Binance direct per hour;
- OI: CMC's Binance-pair OI (USD) vs Binance direct OI x mark;
- basis: self-computed `(mark - index) / index` from Binance (the CMC `index_basis` unit is unverified);
- staleness: `stale` when a source is older than `data.stale_s` (Binance mark: event time; CMC quotes:
  `last_updated`);
- liquidations: the Binance forceOrder 1h / 4h sums are a lower bound of the Binance part of the CMC
  all-exchange totals, so a CMC total below the lower bound (beyond `liq_lb_tolerance_frac`) is
  `liq_mismatch`;
- `cmc_degraded` (whole packet): the newest record of a core CMC route is an error response.
A gap above its `reconcile.*_max` raises `reconcile_mismatch` on that gap feature.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from hdt.contracts.common import DataQualityFlag
from hdt.core.config import ReconcileParams
from hdt.features.common import FeatureBlock, ratio
from hdt.features.fundamental import CmcQuote
from hdt.features.funding import PremiumRow
from hdt.features.liquidations import CoinLiq, ForceLowerBound
from hdt.features.migration import VenueCoin

RECONCILE_NAMES = (
    "price_gap_cmc_binance",
    "funding_gap_cmc_binance_per_h",
    "oi_gap_cmc_binance",
    "basis_binance",
    "mark_age_s",
    "cmc_quote_age_s",
)


@dataclass(frozen=True)
class ReconcileInputs:
    as_of: datetime
    stale: timedelta
    premium: PremiumRow | None
    multiplier: int
    quote: CmcQuote | None
    cmc_binance: VenueCoin | None
    binance_interval_h: float | None
    binance_oi_usd: float | None
    liq: CoinLiq | None
    lower_bound: ForceLowerBound | None
    cmc_degraded: bool


def reconcile_features(x: ReconcileInputs, params: ReconcileParams) -> FeatureBlock:
    block = FeatureBlock()
    mark = x.premium.mark if x.premium is not None else None
    cmc_price = x.quote.price * x.multiplier if x.quote is not None and x.quote.price is not None else None
    price_gap = ratio(cmc_price - mark, mark) if cmc_price is not None and mark else None
    block.set("price_gap_cmc_binance", price_gap)
    if price_gap is not None and abs(price_gap) > params.price_gap_frac_max:
        block.flag(DataQualityFlag.RECONCILE_MISMATCH, "price_gap_cmc_binance")

    direct = (
        x.premium.rate / x.binance_interval_h
        if x.premium and x.premium.rate is not None and x.binance_interval_h
        else None
    )
    cmc_rate = x.cmc_binance.funding_rate if x.cmc_binance is not None else None
    cmc_per_h = cmc_rate / x.binance_interval_h if cmc_rate is not None and x.binance_interval_h else None
    funding_gap = cmc_per_h - direct if cmc_per_h is not None and direct is not None else None
    block.set("funding_gap_cmc_binance_per_h", funding_gap)
    if funding_gap is not None and abs(funding_gap) > params.funding_gap_per_h_max:
        block.flag(DataQualityFlag.RECONCILE_MISMATCH, "funding_gap_cmc_binance_per_h")

    cmc_oi = x.cmc_binance.oi_usd if x.cmc_binance is not None else None
    oi_gap = (
        ratio(cmc_oi - x.binance_oi_usd, x.binance_oi_usd)
        if cmc_oi is not None and x.binance_oi_usd
        else None
    )
    block.set("oi_gap_cmc_binance", oi_gap)
    if oi_gap is not None and abs(oi_gap) > params.oi_gap_frac_max:
        block.flag(DataQualityFlag.RECONCILE_MISMATCH, "oi_gap_cmc_binance")

    index = x.premium.index if x.premium is not None else None
    block.set("basis_binance", ratio(mark - index, index) if mark is not None and index else None)

    mark_age = (x.as_of.timestamp() * 1000 - x.premium.time_ms) / 1000.0 if x.premium is not None else None
    block.set("mark_age_s", mark_age)
    if mark_age is None:
        block.flag(DataQualityFlag.MISSING_ROUTE, "mark_age_s", "basis_binance", "price_gap_cmc_binance")
    elif mark_age > x.stale.total_seconds():
        block.flag(DataQualityFlag.STALE, "mark_age_s", "basis_binance", "price_gap_cmc_binance")
    updated = x.quote.last_updated if x.quote is not None else None
    quote_age = (x.as_of - updated).total_seconds() if updated is not None else None
    block.set("cmc_quote_age_s", quote_age)
    if quote_age is None:
        block.flag(DataQualityFlag.MISSING_ROUTE, "cmc_quote_age_s", "price_gap_cmc_binance")
    elif quote_age > x.stale.total_seconds():
        block.flag(DataQualityFlag.STALE, "cmc_quote_age_s", "price_gap_cmc_binance")

    if x.liq is not None and x.lower_bound is not None:
        tol = 1.0 - params.liq_lb_tolerance_frac
        mismatched = [
            name
            for name, cmc, lb in (
                ("liq_total_1h_usd", x.liq.total_1h, x.lower_bound.total_1h),
                ("liq_total_4h_usd", x.liq.total_4h, x.lower_bound.total_4h),
            )
            if cmc < lb * tol
        ]
        if mismatched:
            block.flag(DataQualityFlag.LIQ_MISMATCH, *mismatched)
    if x.cmc_degraded:
        block.flag(DataQualityFlag.CMC_DEGRADED)
    return block
