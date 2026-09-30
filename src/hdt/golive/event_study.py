"""Event study on shadow data (phase 11, Red Team Delta #30): G1 rechecked on the signals the running system
actually emitted, net of every cost including realized funding.

Signals: every convened council event (`council_events`, status `done`: skipped, failed and unfinished
events never reached a decision) of source LTX, MIGRATION or HOLLOW_HYPE in `[start, end)`, one per (coin,
non-overlapping 12 h window) over all sources (`hdt.quant.gates.independent_events`, first signal wins).
Direction: the scanner superset log row for LTX / MIGRATION; the News agent's round-1 `p_model` (the regime
rule output) for HOLLOW_HYPE (0.5 = no direction, dropped with a reason). Symbols come from the
point-in-time universe, `beta_btc` from the event's committed packets (`hdt.scoring.resolver.build_request`,
the same inputs as the 12 h label).

Per horizon h in {4, 8, 12, 24} (12 is preregistered and decides; the others are reference only):
`net = side x label_return - round_trip_cost - side x (funding(coin) - beta x funding(BTC))` with
`round_trip_cost` = taker fee and slippage on entry and exit (and on the BTC hedge leg for `RESID_12H`),
funding = realized Binance settlements in `(as_of, as_of + h]` paid by a long of notional 1
(`hdt.quant.labels.funding_paid`). A missing mark, beta or funding history makes the value missing with a
reason; nothing is interpolated. Results are split by `target_type` and source, with the UTC-day block
bootstrap 95% CI of G1 (fixed seed).

Decision per group, on the 12 h horizon only (as G1, `hdt.quant.gates.decide`): `n_min` = the events
needed to detect `delta` = 2 x the median round-trip cost of the group with power 0.8, from the sample
standard deviation of the group's 12 h nets. Fewer 12 h values than `n_min`: `underpowered` (no verdict
yet). Enough values: `edge` when the CI is above 0, otherwise `replan` (the CI contains 0 or lies below it:
phase 11 step 2, stop and replan the core strategy).
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from statistics import median, stdev
from typing import Any, Literal, Protocol

import sqlalchemy as sa

from hdt.contracts.common import TargetType
from hdt.core.clock import ensure_utc
from hdt.db.models.decision import CouncilEventRow, DecisionCardRow, DecisionForecastRow
from hdt.db.models.quant import ScannerLogRow
from hdt.features.lake_io import LakeView
from hdt.lake.pit_query import PitQuery
from hdt.lake.universe import load_universe
from hdt.quant.gates import (
    DECISION_HORIZON_H,
    BootstrapResult,
    Trigger,
    day_block_bootstrap,
    independent_events,
    n_min,
    round_trip_cost,
)
from hdt.quant.labels import MarkBook, Settlement, funding_paid, funding_settlements, resolve_label
from hdt.scoring.resolver import build_request
from hdt.scoring.store import event_packets

SOURCES: tuple[str, ...] = ("LTX", "MIGRATION", "HOLLOW_HYPE")
HORIZONS: tuple[int, ...] = (4, 8, 12, 24)
NEWS_AGENT = "news"
DONE = "done"
Decision = Literal["insufficient_data", "underpowered", "edge", "replan"]


@dataclass(frozen=True)
class ShadowSignal:
    coin_id: int
    as_of: datetime
    source: str
    target_type: str
    side: str
    symbol: str
    btc_symbol: str
    beta_btc: float | None
    event_id: str


class Market(Protocol):
    def label(self, sig: ShadowSignal, horizon_h: int) -> float | None:
        """The target-typed return over the horizon (RAW: coin; RESID: coin - beta x BTC)."""

    def mark(self, symbol: str, t: datetime) -> float | None: ...

    def funding(self, symbol: str, start: datetime, end: datetime) -> list[Settlement] | None: ...


@dataclass(frozen=True)
class Costs:
    taker: float
    slippage_bp: float


@dataclass
class SignalRow:
    signal: ShadowSignal
    cost: float
    gross: dict[str, float | None] = field(default_factory=dict)
    funding: dict[str, float | None] = field(default_factory=dict)
    net: dict[str, float | None] = field(default_factory=dict)
    missing: dict[str, str] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        sig = asdict(self.signal)
        sig["as_of"] = self.signal.as_of.isoformat()
        return {
            **sig,
            "cost": self.cost,
            "gross": self.gross,
            "funding": self.funding,
            "net": self.net,
            "missing": self.missing,
        }


def net_row(
    sig: ShadowSignal, market: Market, costs: Costs, *, end: datetime, horizons: Iterable[int]
) -> SignalRow:
    resid = sig.target_type == TargetType.RESID_12H.value
    hedge = sig.beta_btc if resid else None
    row = SignalRow(sig, round_trip_cost(costs.taker, costs.slippage_bp, hedge_beta=hedge))
    s = 1.0 if sig.side == "LONG" else -1.0
    for h in horizons:
        key = str(h)
        stop = sig.as_of + timedelta(hours=h)
        row.gross[key] = row.funding[key] = row.net[key] = None
        if resid and sig.beta_btc is None:
            row.missing[key] = "no_beta"
            continue
        if stop > end:
            row.missing[key] = "immature"
            continue
        value = market.label(sig, h)
        if value is None:
            row.missing[key] = "no_mark"
            continue
        entry = market.mark(sig.symbol, sig.as_of)
        coin_funding = market.funding(sig.symbol, sig.as_of, stop)
        if entry is None or coin_funding is None:
            row.missing[key] = "no_funding"
            continue
        paid = funding_paid(coin_funding, entry)
        if resid:
            btc_entry = market.mark(sig.btc_symbol, sig.as_of)
            btc_funding = market.funding(sig.btc_symbol, sig.as_of, stop)
            if btc_entry is None or btc_funding is None or sig.beta_btc is None:
                row.missing[key] = "no_funding"
                continue
            paid -= sig.beta_btc * funding_paid(btc_funding, btc_entry)
        gross = s * value
        row.gross[key] = gross
        row.funding[key] = s * paid
        row.net[key] = gross - row.cost - s * paid
    return row


@dataclass(frozen=True)
class GroupResult:
    source: str
    target_type: str
    n_signals: int
    horizons: dict[str, BootstrapResult | None]
    dropped: dict[str, int]
    mean_funding_12h: float | None
    sigma_12h: float | None = None
    """Sample standard deviation of the group's 12 h nets (None under 2 values)."""
    delta: float | None = None
    """Edge to detect: 2 x the median round-trip cost of the group's 12 h signals."""
    n_min: int | None = None

    @property
    def state(self) -> str:
        """Of the preregistered 12 h horizon: `edge`, `no_edge` (CI contains 0), `negative` or
        `insufficient_data`, whatever the power."""
        ci = self.horizons.get(str(DECISION_HORIZON_H))
        if ci is None:
            return "insufficient_data"
        if ci.ci_low > 0:
            return "edge"
        if ci.ci_high < 0:
            return "negative"
        return "no_edge"

    @property
    def decision(self) -> Decision:
        """The 12 h decision: `underpowered` below `n_min`, then `edge` (CI above 0) or `replan`."""
        ci = self.horizons.get(str(DECISION_HORIZON_H))
        if ci is None or self.n_min is None:
            return "insufficient_data"
        if ci.n < self.n_min:
            return "underpowered"
        return "edge" if ci.ci_low > 0 else "replan"

    @property
    def replan_signal(self) -> bool:
        """Phase 11 step 2 on the preregistered horizon: the 12 h CI does not exclude 0 from above with at
        least `n_min` values. The 4 / 8 / 24 h horizons never decide."""
        return self.decision == "replan"

    @property
    def underpowered(self) -> bool:
        return self.decision == "underpowered"

    def to_json(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "target_type": self.target_type,
            "n_signals": self.n_signals,
            "state_12h": self.state,
            "decision_12h": self.decision,
            "replan_signal": self.replan_signal,
            "underpowered": self.underpowered,
            "sigma_12h": self.sigma_12h,
            "delta": self.delta,
            "n_min": self.n_min,
            "mean_funding_12h": self.mean_funding_12h,
            "dropped_12h": self.dropped,
            "horizons": {k: (asdict(v) if v else None) for k, v in self.horizons.items()},
        }


def summarize(rows: Sequence[SignalRow], horizons: Iterable[int] = HORIZONS) -> list[GroupResult]:
    groups: dict[tuple[str, str], list[SignalRow]] = defaultdict(list)
    for row in rows:
        groups[(row.signal.source, row.signal.target_type)].append(row)
    out: list[GroupResult] = []
    decide = str(DECISION_HORIZON_H)
    for (source, target), mine in sorted(groups.items()):
        cis: dict[str, BootstrapResult | None] = {}
        for h in horizons:
            key = str(h)
            valid = [(r.net[key], r.signal.as_of.date()) for r in mine if r.net.get(key) is not None]
            cis[key] = day_block_bootstrap(
                [float(v) for v, _ in valid if v is not None], [d for _, d in valid]
            )
        fund = [r.funding[decide] for r in mine if r.funding.get(decide) is not None]
        decided = [r for r in mine if r.net.get(decide) is not None]
        nets = [float(r.net[decide] or 0.0) for r in decided]
        sigma = stdev(nets) if len(nets) >= 2 else None
        delta = 2.0 * median(r.cost for r in decided) if decided else None
        out.append(
            GroupResult(
                source=source,
                target_type=target,
                n_signals=len(mine),
                horizons=cis,
                dropped=dict(sorted(Counter(r.missing[decide] for r in mine if decide in r.missing).items())),
                mean_funding_12h=sum(v for v in fund if v is not None) / len(fund) if fund else None,
                sigma_12h=sigma,
                delta=delta,
                n_min=n_min(sigma, delta) if sigma is not None and delta is not None else None,
            )
        )
    return out


# -------------------------------------------------------------------------------------------- readers


class LakeMarket:
    """`Market` over the recorded lake: marks as `hdt.quant.labels`, funding from `funding_rate` records
    fetched by `fetched_by` (the study end)."""

    def __init__(self, pit: PitQuery, *, label_params: Any, fetched_by: datetime) -> None:
        self.view = LakeView(pit)
        self.marks = MarkBook(self.view)
        self.params = label_params
        self.fetched_by = ensure_utc(fetched_by)
        self._gap = timedelta(seconds=label_params.max_mark_gap_s)

    def label(self, sig: ShadowSignal, horizon_h: int) -> float | None:
        return resolve_label(
            self.view,
            symbol=sig.symbol,
            btc_symbol=sig.btc_symbol,
            as_of=sig.as_of,
            horizon_h=horizon_h,
            target_type=TargetType(sig.target_type),
            beta_btc=sig.beta_btc,
            params=self.params,
            marks=self.marks.mark,
        ).value

    def mark(self, symbol: str, t: datetime) -> float | None:
        return self.marks.mark(symbol, t, self._gap) if symbol else None

    def funding(self, symbol: str, start: datetime, end: datetime) -> list[Settlement] | None:
        if not symbol:
            return None
        return funding_settlements(self.view, symbol, start, end, fetched_by=self.fetched_by)


def load_signals(
    conn: sa.Connection,
    pit: PitQuery,
    *,
    start: datetime,
    end: datetime,
    btc_cmc_symbol: str,
) -> tuple[list[ShadowSignal], dict[str, int]]:
    """Independent shadow signals of `[start, end)` and the count of signals dropped per reason."""
    ev = CouncilEventRow
    rows = conn.execute(
        sa.select(ev.event_id, ev.coin_id, ev.as_of, ev.source, ev.candidate)
        .where(
            ev.as_of >= ensure_utc(start),
            ev.as_of < ensure_utc(end),
            ev.source.in_(SOURCES),
            ev.status == DONE,
        )
        .order_by(ev.as_of, ev.coin_id, ev.source)
    ).all()
    dropped: Counter[str] = Counter()
    sides = _scanner_sides(conn, [(int(r.coin_id), ensure_utc(r.as_of), str(r.source)) for r in rows])
    news = _news_sides(conn, [str(r.event_id) for r in rows if r.source == "HOLLOW_HYPE"])
    triggers: list[Trigger] = []
    by_key: dict[tuple[int, str, datetime], Any] = {}
    for r in rows:
        at = ensure_utc(r.as_of)
        side = (
            news.get(str(r.event_id))
            if r.source == "HOLLOW_HYPE"
            else sides.get((int(r.coin_id), at, str(r.source)))
        )
        if side is None:
            dropped["no_direction"] += 1
            continue
        trig = Trigger(int(r.coin_id), str(r.source), side, at)
        triggers.append(trig)
        by_key[(trig.coin_id, trig.rule, at)] = r
    cards = _card_symbols(conn, [str(r.event_id) for r in rows])
    out: list[ShadowSignal] = []
    for trig in independent_events(triggers, timedelta(hours=DECISION_HORIZON_H)):
        r = by_key[(trig.coin_id, trig.rule, ensure_utc(trig.as_of))]
        candidate = r.candidate or {}
        target = str(candidate.get("target_type") or ("RESID_12H" if trig.rule == "LTX" else "RAW_12H"))
        request = build_request(
            event_id=str(r.event_id),
            coin_id=trig.coin_id,
            card_symbol=cards.get(str(r.event_id), ""),
            as_of=trig.as_of,
            horizon_h=DECISION_HORIZON_H,
            target_type=TargetType(target),
            label_spec_version=str(candidate.get("label_spec_version") or ""),
            packets=event_packets(conn, trig.coin_id, trig.as_of),
            universe=load_universe(pit, trig.as_of),
            btc_cmc_symbol=btc_cmc_symbol,
        )
        if not request.symbol:
            dropped["no_symbol"] += 1
            continue
        out.append(
            ShadowSignal(
                coin_id=trig.coin_id,
                as_of=ensure_utc(trig.as_of),
                source=trig.rule,
                target_type=target,
                side=trig.side,
                symbol=request.symbol,
                btc_symbol=request.btc_symbol,
                beta_btc=request.beta_btc,
                event_id=str(r.event_id),
            )
        )
    return out, dict(sorted(dropped.items()))


def _scanner_sides(
    conn: sa.Connection, keys: Sequence[tuple[int, datetime, str]]
) -> dict[tuple[int, datetime, str], str]:
    wanted = {k for k in keys if k[2] in ("LTX", "MIGRATION")}
    if not wanted:
        return {}
    s = ScannerLogRow
    out: dict[tuple[int, datetime, str], str] = {}
    for row in conn.execute(
        sa.select(s.coin_id, s.as_of, s.rule, s.side)
        .where(
            s.as_of >= min(k[1] for k in wanted),
            s.as_of <= max(k[1] for k in wanted),
            s.coin_id.in_(sorted({k[0] for k in wanted})),
            s.side.is_not(None),
        )
        .order_by(s.as_of, s.coin_id, s.rule, s.emitted.desc(), s.rule_version)
    ):
        key = (int(row.coin_id), ensure_utc(row.as_of), str(row.rule))
        if key in wanted and key not in out:
            out[key] = str(row.side)
    return out


def _news_sides(conn: sa.Connection, event_ids: Sequence[str]) -> dict[str, str]:
    """HOLLOW_HYPE direction from the News agent's round-1 `p_model` (0.5: no direction)."""
    out: dict[str, str] = {}
    f = DecisionForecastRow
    for start in range(0, len(event_ids), 1000):
        chunk = event_ids[start : start + 1000]
        for row in conn.execute(
            sa.select(f.event_id, f.forecast).where(
                f.event_id.in_(chunk), f.agent == NEWS_AGENT, f.round == 1
            )
        ):
            p = (row.forecast or {}).get("p_model")
            if isinstance(p, int | float) and not isinstance(p, bool) and p != 0.5:
                out[str(row.event_id)] = "LONG" if p > 0.5 else "SHORT"
    return out


def _card_symbols(conn: sa.Connection, event_ids: Sequence[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    c = DecisionCardRow
    for start in range(0, len(event_ids), 1000):
        chunk = event_ids[start : start + 1000]
        out.update(
            {
                str(r.event_id): str(r.symbol)
                for r in conn.execute(sa.select(c.event_id, c.symbol).where(c.event_id.in_(chunk)))
            }
        )
    return out


def run(
    signals: Sequence[ShadowSignal],
    market: Market,
    costs: Costs,
    *,
    end: datetime,
    horizons: Iterable[int] = HORIZONS,
) -> tuple[list[SignalRow], list[GroupResult]]:
    hs = tuple(horizons)
    rows = [net_row(sig, market, costs, end=ensure_utc(end), horizons=hs) for sig in signals]
    return rows, summarize(rows, hs)


def study_json(
    conn: sa.Connection,
    pit: PitQuery,
    *,
    start: datetime,
    end: datetime,
    costs: Costs,
    label_params: Any,
    btc_cmc_symbol: str,
    fetched_by: datetime,
) -> dict[str, Any]:
    """The whole study as JSON (inputs, per-group CIs, every signal row); `end` bounds the labels."""
    start, end = ensure_utc(start), ensure_utc(end)
    signals, dropped = load_signals(conn, pit, start=start, end=end, btc_cmc_symbol=btc_cmc_symbol)
    market = LakeMarket(pit, label_params=label_params, fetched_by=fetched_by)
    rows, groups = run(signals, market, costs, end=end)
    return {
        "inputs": {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "taker": costs.taker,
            "slippage_bp": costs.slippage_bp,
            "horizons_h": list(HORIZONS),
            "decision_horizon_h": DECISION_HORIZON_H,
            "label_spec_version": getattr(label_params, "label_spec_version", None),
        },
        "signals": len(signals),
        "dropped_before_labels": dropped,
        "groups": [g.to_json() for g in groups],
        "rows": [r.to_json() for r in rows],
    }


def render_markdown(study: dict[str, Any]) -> str:
    inp = study["inputs"]
    lines = [
        f"# Shadow event study {inp['start']} to {inp['end']}",
        "",
        f"Net of taker fee {inp['taker']} per side, slippage {inp['slippage_bp']} bp per side and realized "
        f"funding; 12 h is preregistered and decides (replan when its CI does not lie above 0 with at least "
        f"N min values), 4 / 8 / 24 h are reference only. {study['signals']} independent signals; dropped "
        f"before labels: {study['dropped_before_labels'] or 'none'}.",
        "",
        "| source | target | N | N min | decision 12h | h | n | mean | 95% CI | replan |",
        "|:--|:--|--:|--:|:--|--:|--:|--:|:--|:--|",
    ]
    for g in study["groups"]:
        needed = g["n_min"] if g["n_min"] is not None else "-"
        for h, ci in g["horizons"].items():
            cell = f"[{ci['ci_low']:.5f}, {ci['ci_high']:.5f}]" if ci else "-"
            lines.append(
                f"| {g['source']} | {g['target_type']} | {g['n_signals']} | {needed} | {g['decision_12h']} | "
                f"{h} | {ci['n'] if ci else 0} | {f'{ci["mean"]:.5f}' if ci else '-'} | {cell} | "
                f"{'yes' if g['replan_signal'] else 'no'} |"
            )
    return "\n".join(lines) + "\n"
