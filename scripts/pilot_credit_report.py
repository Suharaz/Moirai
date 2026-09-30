"""7-day CMC credit pilot report (phase 02 success criterion).

Compares the metered credits per route (`cmc_credit_usage`, from `status.credit_count`) over the last
`--days` complete UTC days with the design estimate (`est_credits_month` in `config/cmc_routes.yaml`,
Design Contract section 7), projects a 30-day month, reconciles with the latest `/v1/key/info` snapshot
and prints cadence proposals when the projection exceeds the pilot target (80 % of the quota).

Usage: `HDT_PG_DSN=... uv run python scripts/pilot_credit_report.py [--days 7] [--json]`
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import date, timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from hdt.core.clock import utcnow
from hdt.core.config import CmcRoutesFile, static_config
from hdt.db.models.ingest import CmcCreditUsageRow, CmcKeyInfoRow
from hdt.db.session import make_engine, make_session_factory

MONTH_DAYS = 30
# Decided handling for an over-budget pilot (phase 02 risk assessment), in the order to apply them.
PROPOSALS = (
    ("exchange_derivative_market_pairs", "route #2: peripheral exchanges every 30 min instead of 15 min"),
    ("crypto_derivative_market_pairs", "route #3: watchlist 10 coins instead of 20"),
    ("ws_crypto_latest_price", "route #30: disable the optional CMC WebSocket"),
    ("liquidations_by_crypto", "route #6: every 2 min instead of 1 min"),
)


@dataclass(frozen=True)
class RouteLine:
    route: str
    route_id: int | None
    days: int
    actual_per_day: float
    design_per_day: float
    projected_month: float

    @property
    def ratio(self) -> float | None:
        return self.actual_per_day / self.design_per_day if self.design_per_day else None


@dataclass(frozen=True)
class PilotReport:
    start: date
    end: date
    days_metered: int
    lines: tuple[RouteLine, ...]
    projected_month: float
    quota: int
    pilot_target_frac: float
    key_info_used_cycle: int | None
    key_info_limit_cycle: int | None
    proposals: tuple[str, ...]

    @property
    def projected_fraction(self) -> float:
        return self.projected_month / self.quota if self.quota else 0.0


def build_report(
    usage: list[tuple[date, str, int]],
    routes: CmcRoutesFile,
    *,
    start: date,
    end: date,
    quota: int,
    pilot_target_frac: float,
    key_info: tuple[int, int] | None,
) -> PilotReport:
    per_route: dict[str, int] = defaultdict(int)
    days_seen: set[date] = set()
    for day, route, credits in usage:
        if start <= day < end:
            per_route[route] += credits
            days_seen.add(day)
    days = max(len(days_seen), 1)
    catalog = {r.name: r for r in routes.routes}
    names = sorted(set(per_route) | {r.name for r in routes.routes if r.enabled})
    lines = []
    for name in names:
        route = catalog.get(name)
        actual = per_route.get(name, 0) / days
        design = route.est_credits_month / MONTH_DAYS if route is not None and route.enabled else 0.0
        lines.append(RouteLine(name, route.id if route else None, days, actual, design, actual * MONTH_DAYS))
    lines.sort(key=lambda line: -line.projected_month)
    projected = sum(line.projected_month for line in lines)
    proposals: list[str] = []
    if quota and projected / quota > pilot_target_frac:
        excess = projected - pilot_target_frac * quota
        for route_name, text in PROPOSALS:
            if excess <= 0:
                break
            line = next((x for x in lines if x.route == route_name), None)
            if line is None or line.projected_month == 0:
                continue
            proposals.append(f"{text} (route now projects {line.projected_month:,.0f} credits/month)")
            excess -= line.projected_month / 2
    return PilotReport(
        start=start,
        end=end,
        days_metered=len(days_seen),
        lines=tuple(lines),
        projected_month=projected,
        quota=quota,
        pilot_target_frac=pilot_target_frac,
        key_info_used_cycle=key_info[0] if key_info else None,
        key_info_limit_cycle=key_info[1] if key_info else None,
        proposals=tuple(proposals),
    )


def _load(
    session: Session, start: date, end: date
) -> tuple[list[tuple[date, str, int]], tuple[int, int] | None]:
    rows = session.execute(
        select(CmcCreditUsageRow.date, CmcCreditUsageRow.route, CmcCreditUsageRow.credits).where(
            CmcCreditUsageRow.date >= start, CmcCreditUsageRow.date < end
        )
    ).all()
    latest = session.scalars(select(CmcKeyInfoRow).order_by(CmcKeyInfoRow.checked_at.desc()).limit(1)).first()
    key_info = (latest.credits_used_cycle, latest.credit_limit_cycle) if latest else None
    return [(r[0], r[1], int(r[2])) for r in rows], key_info


def _print(report: PilotReport) -> None:
    print(f"CMC credit pilot {report.start} .. {report.end} ({report.days_metered} metered days)")
    print(f"{'route':38} {'#':>3} {'actual/day':>11} {'design/day':>11} {'ratio':>6} {'month':>10}")
    for line in report.lines:
        ratio = f"{line.ratio:.2f}" if line.ratio is not None else "-"
        rid = str(line.route_id) if line.route_id else "-"
        print(
            f"{line.route:38} {rid:>3} {line.actual_per_day:11,.1f} {line.design_per_day:11,.1f} "
            f"{ratio:>6} {line.projected_month:10,.0f}"
        )
    print(
        f"projected month: {report.projected_month:,.0f} / {report.quota:,} "
        f"({report.projected_fraction:.1%}; pilot target {report.pilot_target_frac:.0%})"
    )
    if report.key_info_limit_cycle:
        print(f"/v1/key/info cycle: {report.key_info_used_cycle:,} used of {report.key_info_limit_cycle:,}")
    if report.proposals:
        print("cadence proposals (apply on the console CMC page, in order):")
        for text in report.proposals:
            print(f"  - {text}")
    else:
        print("within the pilot target: keep the current cadence")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    static = static_config()
    end = utcnow().date()
    start = end - timedelta(days=args.days)
    with make_session_factory(make_engine())() as session:
        usage, key_info = _load(session, start, end)
    quota = key_info[1] if key_info else static.settings.cmc.quota_monthly_design
    report = build_report(
        usage,
        static.cmc_routes,
        start=start,
        end=end,
        quota=quota,
        pilot_target_frac=static.settings.cmc.pilot_max_projection_frac,
        key_info=key_info,
    )
    if args.json:
        payload = asdict(report) | {"projected_fraction": report.projected_fraction}
        print(json.dumps(payload, default=str, indent=2))
    else:
        _print(report)
    return 0 if report.projected_fraction <= report.pilot_target_frac else 1


if __name__ == "__main__":
    sys.exit(main())
