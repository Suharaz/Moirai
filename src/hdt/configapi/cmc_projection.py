"""Monthly CoinMarketCap credit projection for a proposed `cmc` section (pure function).

Each route has a billing unit and a unit count per 30-day month:
- interval routes: one call per cadence (route #2: one call per exchange per cadence, core and peripheral
  exchanges separately; route #3: one call per watched coin per cadence),
- daily routes: 30 calls; one-off backfills and on-event routes: their monthly design budget (1 unit),
- the WebSocket route: one unit per subscribed coin.
Credits per unit come from the credit meter when phase 02 has measured them (credits / calls over the
last days), otherwise from the design estimate (`est_credits_month` / design units). The save is blocked
when the projection exceeds `save_block_projection_frac` (90 %) of the quota.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final, Literal

from hdt.core.config import CmcRoute, CmcRoutesFile, StaticConfig
from hdt.settings.schemas import CmcSection, effective_cmc_routes

SECONDS_PER_MONTH: Final[int] = 30 * 86400
DAYS_PER_MONTH: Final[int] = 30
# Units whose count is a number of HTTP calls, so a measured credits-per-call value applies to them.
_CALL_SCHEDULES: Final[frozenset[str]] = frozenset({"interval", "daily"})


@dataclass(frozen=True)
class RouteProjection:
    route: int
    name: str
    enabled: bool
    units_per_month: float
    credits_per_unit: float
    credits: float  # projected credits per month
    source: Literal["measured", "design"]


@dataclass(frozen=True)
class CreditProjection:
    monthly_projection: float
    quota: int
    quota_source: Literal["key_info", "design"]
    fraction: float
    block_fraction: float
    blocked: bool
    per_route: tuple[RouteProjection, ...]


def units_per_month(route: CmcRoute) -> float:
    """Billing units per month for one (effective) route; see the module docstring."""
    params = route.params
    if route.schedule == "interval":
        if route.cadence_s is None:
            raise ValueError(f"route #{route.id}: interval route without cadence_s")
        calls = SECONDS_PER_MONTH / route.cadence_s
        if route.id == 2:
            core = int(params["core_exchanges"]) * SECONDS_PER_MONTH / int(params["core_cadence_s"])
            peripheral = (
                int(params["peripheral_exchanges"]) * SECONDS_PER_MONTH / int(params["peripheral_cadence_s"])
            )
            return core + peripheral
        if route.id == 3:
            return int(params["watchlist_coins"]) * calls
        return calls
    if route.schedule == "daily":
        return float(DAYS_PER_MONTH)
    if route.schedule == "stream":
        return float(params.get("coins", params["max_coins"]))
    return 1.0  # once / on_event: the design figure is already a monthly budget


def _design_units(route: CmcRoute) -> float:
    """Units the design estimate was computed for (the WebSocket estimate assumes max_coins)."""
    if route.schedule == "stream":
        return float(route.params["max_coins"])
    return units_per_month(route)


def project_credits(
    section: CmcSection,
    static: StaticConfig,
    *,
    measured_per_call: Mapping[int, float] | None = None,
    quota: int | None = None,
) -> CreditProjection:
    """Project monthly credits for `section`; raises ValueError when the section is inconsistent."""
    effective: CmcRoutesFile = effective_cmc_routes(section, static)
    measured = measured_per_call or {}
    per_route: list[RouteProjection] = []
    for design, route in zip(static.cmc_routes.routes, effective.routes, strict=True):
        design_units = _design_units(design)
        units = units_per_month(route) if route.enabled else 0.0
        source: Literal["measured", "design"]
        if route.id in measured and route.schedule in _CALL_SCHEDULES:
            per_unit, source = float(measured[route.id]), "measured"
        else:
            per_unit, source = (design.est_credits_month / design_units if design_units else 0.0), "design"
        per_route.append(
            RouteProjection(
                route=route.id,
                name=route.name,
                enabled=route.enabled,
                units_per_month=units,
                credits_per_unit=per_unit,
                credits=units * per_unit,
                source=source,
            )
        )
    total = sum(r.credits for r in per_route)
    effective_quota = quota if quota and quota > 0 else static.settings.cmc.quota_monthly_design
    block_fraction = static.settings.cmc.save_block_projection_frac
    fraction = total / effective_quota
    return CreditProjection(
        monthly_projection=total,
        quota=effective_quota,
        quota_source="key_info" if quota and quota > 0 else "design",
        fraction=fraction,
        block_fraction=block_fraction,
        blocked=fraction > block_fraction,
        per_route=tuple(per_route),
    )
