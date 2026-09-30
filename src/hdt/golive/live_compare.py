"""Paper vs live on the same signals (phase 11 Red Team Delta #36), re-measured at each size step.

The window is the current size step: from `active_config.activated_at` of section `mode` (the moment the
current `mode` / `size_multiplier` version became active) unless the caller gives `since`. Metrics, each with
its own minimum N (below it the metric is `insufficient_data` and never decides):
- fill rate: share of events with an entry order whose entry filled, paper vs live on the events both
  accounts ordered; within `fill_rate_tol` (absolute) of paper;
- entry price: the live average entry price vs the paper one on events both filled, adverse slippage in bp
  (positive = live worse); within `slippage_bp`: the paper fill already carries `slippage_bp` adverse
  against the book (`hdt.execution.paper_venue`), so the live fill stays within `2 x slippage_bp` of the
  book, the same budget as the stop metric;
- stop slippage: the live stop fill vs its trigger price, adverse bp; within `2 x slippage_bp`.
The verdict `hold_size` (any metric with enough N outside its tolerance) stops the next size increase;
`ok_to_step` needs every metric measured and inside tolerance; anything else is `insufficient_data`.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

import sqlalchemy as sa

from hdt.core.clock import ensure_utc
from hdt.db.models.ledger import AlgoOrderRow, FillRow, OrderRow
from hdt.db.models.settings import ActiveConfigRow, ConfigVersionRow
from hdt.golive.data import ENTRY_LEGS

STOP_LEG = "sl"
Verdict = Literal["ok_to_step", "hold_size", "insufficient_data"]


@dataclass(frozen=True)
class Tolerances:
    slippage_bp: float
    """The paper fill-model slippage assumption (`risk.yaml` `fees.slippage_bp`)."""
    fill_rate_tol: float = 0.10
    min_n_fill_rate: int = 20
    min_n_entry: int = 20
    min_n_stop: int = 10

    @property
    def max_slippage_bp(self) -> float:
        """Live stop fill vs its trigger: twice the paper fill-model assumption."""
        return 2.0 * self.slippage_bp

    @property
    def max_entry_vs_paper_bp(self) -> float:
        """Live entry vs the paper entry, whose price already includes `slippage_bp`: the remaining budget
        up to `max_slippage_bp` against the book."""
        return self.max_slippage_bp - self.slippage_bp


@dataclass(frozen=True)
class Metric:
    key: str
    n: int
    min_n: int
    live: float | None
    paper: float | None
    limit: float
    detail: dict[str, Any] = field(default_factory=dict)

    @property
    def measured(self) -> bool:
        return self.n >= self.min_n and self.live is not None

    @property
    def within(self) -> bool | None:
        if not self.measured or self.live is None:
            return None
        if self.key == "fill_rate":
            return self.paper is not None and abs(self.live - self.paper) <= self.limit
        return self.live <= self.limit

    def to_json(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "n": self.n,
            "min_n": self.min_n,
            "live": self.live,
            "paper": self.paper,
            "limit": self.limit,
            "measured": self.measured,
            "within": self.within,
            **({"detail": self.detail} if self.detail else {}),
        }


@dataclass(frozen=True)
class Comparison:
    since: datetime
    size_multiplier: float | None
    metrics: list[Metric]

    @property
    def verdict(self) -> Verdict:
        if any(m.within is False for m in self.metrics):
            return "hold_size"
        if all(m.within is True for m in self.metrics):
            return "ok_to_step"
        return "insufficient_data"

    def to_json(self) -> dict[str, Any]:
        return {
            "since": self.since.isoformat(),
            "size_multiplier": self.size_multiplier,
            "verdict": self.verdict,
            "metrics": [m.to_json() for m in self.metrics],
        }


@dataclass(frozen=True)
class EntryStats:
    ordered: set[str]
    filled: dict[str, tuple[float, str]]
    """event_id -> (quantity-weighted average entry price, order side BUY / SELL)."""


@dataclass(frozen=True)
class StopFill:
    event_id: str
    side: str
    """Order side of the stop (SELL closes a long)."""
    trigger: float
    price: float


def adverse_bp(side: str, reference: float, price: float) -> float:
    """Slippage in bp against the trader: buying above / selling below the reference is positive."""
    if reference <= 0:
        raise ValueError("reference price must be positive")
    move = (price - reference) / reference * 10_000.0
    return move if side == "BUY" else -move


def compare(
    paper: EntryStats,
    live: EntryStats,
    live_stops: list[StopFill],
    tol: Tolerances,
    *,
    since: datetime,
    size_multiplier: float | None,
) -> Comparison:
    both = sorted(paper.ordered & live.ordered)
    paper_rate = sum(1 for e in both if e in paper.filled) / len(both) if both else None
    live_rate = sum(1 for e in both if e in live.filled) / len(both) if both else None
    fill = Metric("fill_rate", len(both), tol.min_n_fill_rate, live_rate, paper_rate, tol.fill_rate_tol)
    paired = sorted(set(paper.filled) & set(live.filled))
    entry_bp = [adverse_bp(live.filled[e][1], paper.filled[e][0], live.filled[e][0]) for e in paired]
    entry = Metric(
        "entry_price_bp",
        len(entry_bp),
        tol.min_n_entry,
        sum(entry_bp) / len(entry_bp) if entry_bp else None,
        0.0,
        tol.max_entry_vs_paper_bp,
        {"worst_bp": max(entry_bp) if entry_bp else None},
    )
    stop_bp = [adverse_bp(s.side, s.trigger, s.price) for s in live_stops]
    stop = Metric(
        "stop_slippage_bp",
        len(stop_bp),
        tol.min_n_stop,
        sum(stop_bp) / len(stop_bp) if stop_bp else None,
        tol.slippage_bp,
        tol.max_slippage_bp,
        {"worst_bp": max(stop_bp) if stop_bp else None},
    )
    return Comparison(ensure_utc(since), size_multiplier, [fill, entry, stop])


# ------------------------------------------------------------------------------------------- readers


def current_step(conn: sa.Connection) -> tuple[datetime | None, float | None, str | None]:
    """(activated_at, size_multiplier, mode) of the active `mode` config version."""
    a, v = ActiveConfigRow, ConfigVersionRow
    row = conn.execute(
        sa.select(a.activated_at, v.payload_json).join(v, v.id == a.version_id).where(a.section == "mode")
    ).one_or_none()
    if row is None:
        return None, None, None
    payload = row.payload_json or {}
    size = payload.get("size_multiplier")
    mode = payload.get("mode")
    return (
        ensure_utc(row.activated_at),
        float(size) if isinstance(size, int | float) else None,
        str(mode) if mode is not None else None,
    )


def entry_stats(conn: sa.Connection, account: str, since: datetime) -> EntryStats:
    o, f = OrderRow, FillRow
    ordered = {
        str(e)
        for e in conn.execute(
            sa.select(o.event_id)
            .where(o.account == account, o.leg.in_(ENTRY_LEGS), o.created_at >= ensure_utc(since))
            .distinct()
        ).scalars()
    }
    notional: dict[str, float] = defaultdict(float)
    qty: dict[str, float] = defaultdict(float)
    side: dict[str, str] = {}
    for row in conn.execute(
        sa.select(f.event_id, f.side, f.price, f.qty).where(
            f.account == account,
            f.leg.in_(ENTRY_LEGS),
            f.filled_at >= ensure_utc(since),
            f.event_id.is_not(None),
        )
    ):
        event = str(row.event_id)
        notional[event] += float(row.price) * float(row.qty)
        qty[event] += float(row.qty)
        side[event] = str(row.side)
    filled = {e: (notional[e] / qty[e], side[e]) for e in qty if qty[e] > 0 and e in ordered}
    return EntryStats(ordered, filled)


def stop_fills(conn: sa.Connection, account: str, since: datetime) -> list[StopFill]:
    """Stop fills with the trigger price of the triggered stop of the same event (average fill price)."""
    g, f = AlgoOrderRow, FillRow
    triggers: dict[str, tuple[float, str]] = {}
    for row in conn.execute(
        sa.select(g.event_id, g.trigger_price, g.side, g.triggered_at)
        .where(
            g.account == account,
            g.leg == STOP_LEG,
            g.triggered_at.is_not(None),
            g.triggered_at >= ensure_utc(since),
        )
        .order_by(g.triggered_at)
    ):
        triggers[str(row.event_id)] = (float(row.trigger_price), str(row.side))
    notional: dict[str, float] = defaultdict(float)
    qty: dict[str, float] = defaultdict(float)
    for fill in conn.execute(
        sa.select(f.event_id, f.price, f.qty).where(
            f.account == account, f.leg == STOP_LEG, f.filled_at >= ensure_utc(since), f.event_id.is_not(None)
        )
    ):
        notional[str(fill.event_id)] += float(fill.price) * float(fill.qty)
        qty[str(fill.event_id)] += float(fill.qty)
    out = []
    for event, (trigger, side) in sorted(triggers.items()):
        if qty.get(event, 0.0) > 0 and trigger > 0:
            out.append(StopFill(event, side, trigger, notional[event] / qty[event]))
    return out


def measure(conn: sa.Connection, tol: Tolerances, *, since: datetime | None = None) -> Comparison | None:
    """The comparison of the current size step; None when no `mode` version is active and no `since`."""
    activated, size, _ = current_step(conn)
    start = since or activated
    if start is None:
        return None
    paper = entry_stats(conn, "paper", start)
    live = entry_stats(conn, "live", start)
    return compare(paper, live, stop_fills(conn, "live", start), tol, since=start, size_multiplier=size)
