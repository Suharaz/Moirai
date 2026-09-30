"""Portfolio ceilings (Design Contract section 5). Pure functions over the namespace's book.

- at most `max_positions` council positions (the BTC hedge book is not counted), pending entries included;
- at most `max_positions_per_coin` (= 1) position per coin;
- total same-direction risk (position risk to its stop, pending entries by their sized risk, the hedge
  book by its distance to the book stop) at most `same_direction_risk_max` of equity, **including the
  hedge book**. An OPEN also grows the hedge book on the other side: the projected book (current book +
  this position's beta hedge) is counted on its side, and an OPEN whose hedge would push that side over
  the ceiling is rejected like one that pushes its own side over it;
- at most `max_positions_per_narrative` positions sharing a narrative (a CMC category slug);
- the daily loss (equity vs the UTC day start) at or below `daily_loss_kill` blocks every OPEN (execution
  also kills the namespace).

Every ceiling is re-clipped to the constants in `hdt.settings.ceilings` so a bad config cannot loosen it.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal

from hdt.contracts.common import Side
from hdt.settings.ceilings import (
    DAILY_LOSS_KILL_FLOOR,
    MAX_POSITIONS,
    MAX_POSITIONS_PER_COIN,
    MAX_POSITIONS_PER_NARRATIVE,
    SAME_DIRECTION_RISK_MAX,
)


@dataclass(frozen=True)
class Exposure:
    """One position (or pending entry) as seen by the ceilings."""

    symbol: str
    side: Side
    risk_usd: Decimal
    categories: frozenset[str] = frozenset()
    is_hedge_book: bool = False
    pending: bool = False


@dataclass(frozen=True)
class PortfolioLimits:
    max_positions: int
    max_positions_per_coin: int
    same_direction_risk_max: float
    max_positions_per_narrative: int
    daily_loss_kill: float

    def clipped(self) -> PortfolioLimits:
        return PortfolioLimits(
            max_positions=min(self.max_positions, MAX_POSITIONS),
            max_positions_per_coin=min(self.max_positions_per_coin, MAX_POSITIONS_PER_COIN),
            same_direction_risk_max=min(self.same_direction_risk_max, SAME_DIRECTION_RISK_MAX),
            max_positions_per_narrative=min(self.max_positions_per_narrative, MAX_POSITIONS_PER_NARRATIVE),
            daily_loss_kill=max(self.daily_loss_kill, DAILY_LOSS_KILL_FLOOR),
        )


@dataclass(frozen=True)
class LimitCheck:
    passed: bool
    text: str
    reason: str | None = None


def parse_categories(value: object) -> frozenset[str]:
    """The packet `categories` feature: sorted comma-joined CMC tag slugs (None / empty = no narrative)."""
    if not isinstance(value, str):
        return frozenset()
    return frozenset(part.strip() for part in value.split(",") if part.strip())


def daily_pnl_fraction(equity: Decimal, day_start_equity: Decimal) -> float:
    if day_start_equity <= 0:
        return 0.0
    return float((equity - day_start_equity) / day_start_equity)


def daily_loss_breached(equity: Decimal, day_start_equity: Decimal, daily_loss_kill: float) -> bool:
    kill_at = max(daily_loss_kill, DAILY_LOSS_KILL_FLOOR)
    return daily_pnl_fraction(equity, day_start_equity) <= kill_at


def side_risk(book: Sequence[Exposure], side: Side) -> Decimal:
    return sum((e.risk_usd for e in book if e.side is side), Decimal(0))


def _hedge_risk(book: Sequence[Exposure], side: Side, projected: Exposure | None) -> Decimal:
    """Hedge book risk on `side`: the larger of the current book and the projected one (either can hold
    while the rebalance is in flight)."""
    current = side_risk([e for e in book if e.is_hedge_book], side)
    if projected is not None and projected.side is side:
        return max(current, projected.risk_usd)
    return current


def check_portfolio(
    new: Exposure,
    book: Sequence[Exposure],
    *,
    equity: Decimal,
    day_start_equity: Decimal,
    limits: PortfolioLimits,
    projected_hedge: Exposure | None = None,
) -> list[LimitCheck]:
    """Every ceiling for adding `new` to `book`; the first failing check carries the verdict reason.

    `projected_hedge` is the hedge book once it absorbed `new` (None: no hedge projection available)."""
    lim = limits.clipped()
    council = [e for e in book if not e.is_hedge_book]
    checks: list[LimitCheck] = []

    pnl = daily_pnl_fraction(equity, day_start_equity)
    ok = pnl > lim.daily_loss_kill
    checks.append(
        LimitCheck(
            ok,
            f"Daily PnL {pnl:+.2%} above the kill level {lim.daily_loss_kill:.2%}",
            None if ok else "daily_loss",
        )
    )

    count = len(council)
    ok = count + 1 <= lim.max_positions
    checks.append(
        LimitCheck(
            ok, f"Positions {count + 1} / {lim.max_positions} with this one", None if ok else "max_positions"
        )
    )

    same_coin = sum(1 for e in council if e.symbol == new.symbol)
    ok = same_coin + 1 <= lim.max_positions_per_coin
    checks.append(
        LimitCheck(
            ok,
            f"{same_coin} existing position(s) on {new.symbol}, limit {lim.max_positions_per_coin}",
            None if ok else "coin_limit",
        )
    )

    budget = equity * Decimal(repr(lim.same_direction_risk_max))
    total = side_risk(council, new.side) + _hedge_risk(book, new.side, projected_hedge) + new.risk_usd
    ok = equity > 0 and total <= budget
    share = float(total / equity) if equity > 0 else float("inf")
    checks.append(
        LimitCheck(
            ok,
            f"{new.side.value} risk incl. hedge {share:.2%} of equity, "
            f"ceiling {lim.same_direction_risk_max:.2%}",
            None if ok else "same_direction_risk",
        )
    )
    if projected_hedge is not None and projected_hedge.side is not new.side:
        other = projected_hedge.side
        before = side_risk(council, other) + _hedge_risk(book, other, None)
        after = side_risk(council, other) + _hedge_risk(book, other, projected_hedge)
        # Only a hedge that adds risk to the other side can fail here (never a pre-existing condition).
        ok = equity > 0 and (after <= budget or after <= before)
        share = float(after / equity) if equity > 0 else float("inf")
        checks.append(
            LimitCheck(
                ok,
                f"{other.value} risk incl. the projected hedge {share:.2%} of equity, "
                f"ceiling {lim.same_direction_risk_max:.2%}",
                None if ok else "same_direction_risk",
            )
        )

    crowded = sorted(
        tag
        for tag in new.categories
        if sum(1 for e in council if tag in e.categories) + 1 > lim.max_positions_per_narrative
    )
    ok = not crowded
    text = (
        f"Narrative limit {lim.max_positions_per_narrative} per category respected"
        if ok
        else f"Narrative limit {lim.max_positions_per_narrative} reached for {', '.join(crowded)}"
    )
    checks.append(LimitCheck(ok, text, None if ok else "narrative_limit"))
    return checks


def first_failure(checks: Sequence[LimitCheck]) -> LimitCheck | None:
    for check in checks:
        if not check.passed:
            return check
    return None
