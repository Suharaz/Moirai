"""The Risk gate: one `DecisionMsg` in, one verdict and a leg plan out (phase 09 section 1). Pure.

The caller (`hdt.risk.consumer`) parses and deduplicates the message and loads every input (packet by
`packet_sha256`, candidate set, market data, the namespace book, flags, kill state); this module decides.
Steps, stopping at the first failure (reason codes are the `risk_verdicts.reason` values):

3. no expiry: a decision is valid however long the debate took (owner decision 2026-09-28; `expires_at`
   of a v1 message is ignored); its OPEN is instead re-checked against the current mark in step 5b;
4. integrity: the packet exists, it is the packet the decision names (its recomputed sha256 equals
   `packet_sha256`) and it is about this coin (`integrity`);
5. OPEN only: `candidate_id` belongs to the packet's candidate set and `candidate.side == side`
   (`candidate_unknown`, `candidate_side`);
6. OPEN only: `p_side` recomputed from `p` equals the message and is above 0.5 (`sign_mismatch`);
7. intent by position in this namespace: no position + OPEN -> open; same side + OPEN/HOLD -> HOLD (no
   add); OPEN against the position -> EXIT it (no reversal in the same event); EXIT -> close; HOLD or EXIT
   without a position -> ignored. BTC is reserved for the hedge book (`btc_reserved`) and is checked
   before the position lookup so a BTC decision can never touch the book;
5b. opening only: the run mode still enables the namespace (`mode_disabled`: a namespace kept attached
   only to wind down its positions takes no new one); the candidate re-checked against current
   exchangeInfo (TRADING, tick alignment), R:R >= `min_rr`, the mark not already beyond the invalidation
   (`stop_crossed`) or the take-profit (`tp_crossed`), entry within `max_entry_distance_atr` ATR of the
   Binance mark (`entry_distance`);
9. veto, fail-closed (opening only; EXIT always passes): kill state, stale `AccountState`, stale coin
   Binance data, news veto scan age;
10. sizing and portfolio ceilings, the projected hedge book included;
11. the leg plan: `sl`, `tp1`, `trail`, `entry_ioc`, `entry` (in that publish order, so execution holds the
    stop plan before the entry), or one `exit`. The entry legs carry `max_entry_distance` (the step 5b
    bound in price units) so execution re-applies the same `hdt.core.entry_guard` rule right before sending.

No number in the message is used as a price, size or leverage: prices come from the stored candidate, the
size from the sizing chain. On protective legs `expires_at` is the position time stop (horizon end); on the
`trail` leg `trigger_price` is the activation price (TP1) and `price` the trailing distance.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Final, Literal

from hdt.contracts.account import AccountState
from hdt.contracts.candidate import CandidateSet, LevelCandidate
from hdt.contracts.common import Account, Intent, Leg, OrderSide, OrderType, Side, TargetType, TimeInForce
from hdt.contracts.decision import DecisionMsg
from hdt.contracts.order import OrderIntent
from hdt.contracts.packet import QuantPacket
from hdt.contracts.risk_flags import RiskFlags
from hdt.core.config import RiskFile
from hdt.core.entry_guard import ENTRY_DISTANCE, entry_distance_limit, entry_refusal
from hdt.core.ids import b32_digest
from hdt.execution.client_ids import client_id
from hdt.execution.filters import SymbolFilters
from hdt.risk.hedge_book import BTC_SYMBOL, DEFAULT_BETA
from hdt.risk.kill_state import KillView
from hdt.risk.limits import (
    Exposure,
    LimitCheck,
    PortfolioLimits,
    check_portfolio,
    first_failure,
    parse_categories,
)
from hdt.risk.sizing import SizingInput, SizingRejectedError, p_side_of, size_position
from hdt.risk.veto import evaluate_open
from hdt.settings.ceilings import RiskLimits, clip_risk_limits

Verdict = Literal["approved", "rejected", "hold", "ignored"]
HORIZON: Final[dict[TargetType, timedelta]] = {
    TargetType.RAW_12H: timedelta(hours=12),
    TargetType.RESID_12H: timedelta(hours=12),
}
P_SIDE_TOLERANCE: Final[float] = 1e-9


@dataclass(frozen=True)
class Alert:
    kind: Literal["integrity_error", "account_state_stale"]
    severity: Literal["critical", "warning"]
    title: str


@dataclass(frozen=True)
class MarketInputs:
    """What Risk knows about the coin now (lake reads, never a signed endpoint)."""

    symbol: str | None
    is_btc: bool
    filters: SymbolFilters | None
    mark: Decimal | None
    mark_age_s: float | None
    book_age_s: float | None
    btc_mark: Decimal | None = None


@dataclass(frozen=True)
class HeldPosition:
    side: Side
    qty: Decimal  # absolute


@dataclass(frozen=True)
class BookInputs:
    """The namespace as Risk sees it."""

    account_state: AccountState | None
    state_age_s: float | None
    held: Mapping[str, HeldPosition]  # council positions by symbol (AccountState, else the ledger)
    exposures: tuple[Exposure, ...]  # positions, pending entries and the hedge book
    kill: KillView
    hedge_qty: Decimal = Decimal(0)  # the signed BTC hedge book now (0 = no book)


@dataclass(frozen=True)
class GateInputs:
    decision: DecisionMsg
    packet: QuantPacket | None
    packet_error: str | None
    candidate_set: CandidateSet | None
    flags: RiskFlags | None
    market: MarketInputs
    book: BookInputs


@dataclass(frozen=True)
class GateConfig:
    account: Account
    risk: RiskFile
    stale_s: float
    key_id: str
    opens_enabled: bool  # False while the run mode no longer enables the namespace (winding down)

    @property
    def mode_size_multiplier(self) -> float:
        return float(self.risk.size_multiplier[self.account.value])

    @property
    def limits(self) -> RiskLimits:
        """The pinned risk limits clipped to the hard ceilings."""
        return clip_risk_limits(RiskLimits.of(self.risk))


@dataclass(frozen=True)
class GateResult:
    verdict: Verdict
    reason: str | None
    effective_intent: Intent | None
    symbol: str | None
    checks: tuple[LimitCheck, ...]
    sizing: dict[str, Any] | None = None
    intents: tuple[OrderIntent, ...] = ()
    hedge_beta: float | None = None
    alert: Alert | None = None
    exposure: Exposure | None = None

    @property
    def entry_client_id(self) -> str | None:
        return next((i.client_id for i in self.intents if i.leg is Leg.ENTRY), None)

    @property
    def stop_client_algo_id(self) -> str | None:
        return next((i.client_id for i in self.intents if i.leg is Leg.SL), None)

    @property
    def entry_intent_id(self) -> str | None:
        leg = Leg.ENTRY if any(i.leg is Leg.ENTRY for i in self.intents) else Leg.EXIT
        return next((i.intent_id for i in self.intents if i.leg is leg), None)

    def checks_json(self) -> list[dict[str, Any]]:
        return [{"passed": c.passed, "text": c.text} for c in self.checks]


def intent_id_for(account: Account, event_id: str, leg: Leg, seq: int) -> str:
    """Deterministic intent id: a retry after a Risk crash re-signs the same id (execution replay guard)."""
    return "oi_" + b32_digest(f"{account.value}|{event_id}|{leg.value}|{seq}", 26)


@dataclass
class _Run:
    checks: list[LimitCheck] = field(default_factory=list)

    def ok(self, text: str) -> None:
        self.checks.append(LimitCheck(True, text))

    def fail(
        self,
        reason: str,
        text: str,
        *,
        intent: Intent | None = None,
        symbol: str | None = None,
        sizing: dict[str, Any] | None = None,
        alert: Alert | None = None,
    ) -> GateResult:
        self.checks.append(LimitCheck(False, text, reason))
        return GateResult("rejected", reason, intent, symbol, tuple(self.checks), sizing, alert=alert)


def evaluate(inp: GateInputs, cfg: GateConfig, now: datetime) -> GateResult:
    d = inp.decision
    run = _Run()

    # 3. no expiry: a decision stays valid however long the debate took; 5b re-checks the levels
    # 4. integrity
    packet = inp.packet
    if d.packet_sha256 is not None:
        problem = inp.packet_error
        if problem is None and packet is None:
            problem = f"packet {d.packet_sha256[:12]} is not in the packet store"
        if problem is None and packet is not None and packet.packet_sha256 != d.packet_sha256:
            problem = (
                f"stored packet re-hashes to {packet.packet_sha256[:12]}, decision names "
                f"{d.packet_sha256[:12]}"
            )
        if problem is None and packet is not None and packet.coin_id != d.coin_id:
            problem = f"packet is about coin {packet.coin_id}, decision about {d.coin_id}"
        if problem is not None:
            return run.fail(
                "integrity",
                f"Packet integrity: {problem}",
                alert=Alert("integrity_error", "critical", f"Decision {d.event_id}: {problem}"),
            )
        run.ok(f"Packet {d.packet_sha256[:12]} re-verified by sha256")

    # 5. + 6. OPEN message integrity
    candidate: LevelCandidate | None = None
    if d.intent is Intent.OPEN:
        assert packet is not None  # guaranteed by step 4
        assert d.candidate_id is not None  # guaranteed by DecisionMsg
        cs = inp.candidate_set
        if cs is None or cs.candidate_set_sha256 != packet.candidate_set_sha256:
            problem = "candidate set missing or not the packet's candidate set"
            return run.fail(
                "integrity",
                f"Candidate set: {problem}",
                alert=Alert("integrity_error", "critical", f"Decision {d.event_id}: {problem}"),
            )
        candidate = cs.get(d.candidate_id)
        if candidate is None:
            return run.fail("candidate_unknown", f"Candidate {d.candidate_id} is not in the packet's set")
        if candidate.side is not d.side:
            return run.fail(
                "candidate_side",
                f"Candidate {d.candidate_id} is {candidate.side.value}, decision {d.side.value}",
            )
        run.ok(f"Candidate {d.candidate_id} belongs to the set, side {candidate.side.value}")
        p_side = p_side_of(d.p, d.side)
        if abs(p_side - d.p_side) > P_SIDE_TOLERANCE or p_side <= 0.5:
            return run.fail(
                "sign_mismatch",
                f"p {d.p:.4f} gives p_side {p_side:.4f} for {d.side.value} "
                f"(message {d.p_side:.4f}), need > 0.5",
            )
        run.ok(f"p_side {p_side:.4f} > 0.5 matches {d.side.value}")

    # 7. + 8. intent by position (BTC first: it must never touch the hedge book)
    symbol = inp.market.symbol
    if inp.market.is_btc:
        return run.fail("btc_reserved", "BTC is reserved for the hedge book", symbol=symbol)
    if symbol is None:
        return run.fail("symbol_unknown", f"Coin {d.coin_id} has no Binance symbol in the current universe")
    held = inp.book.held.get(symbol)
    effective: Intent
    if d.intent is Intent.OPEN:
        if held is None:
            effective = Intent.OPEN
        elif held.side is d.side:
            effective = Intent.HOLD
        else:
            effective = Intent.EXIT
    elif held is None:
        run.checks.append(
            LimitCheck(True, f"No {symbol} position in {cfg.account.value}: nothing to {d.intent.value}")
        )
        return GateResult("ignored", "no_position", None, symbol, tuple(run.checks))
    else:
        effective = d.intent
    if effective is Intent.HOLD:
        assert held is not None
        run.ok(f"Holding {held.side.value} {held.qty} {symbol}: no add, no size change")
        return GateResult("hold", None, Intent.HOLD, symbol, tuple(run.checks))
    if effective is Intent.EXIT:
        assert held is not None
        why = "decision EXIT" if d.intent is Intent.EXIT else f"{d.side.value} decision against the position"
        run.ok(f"Exit {held.side.value} {held.qty} {symbol}: {why} (no reversal in the same event)")
        exit_leg = _exit_intent(cfg, d, symbol, held, now)
        return GateResult("approved", None, Intent.EXIT, symbol, tuple(run.checks), intents=(exit_leg,))

    # 5b. opening: re-check the candidate against the current market
    assert candidate is not None
    assert packet is not None
    return _open(inp, cfg, now, run, symbol, candidate, packet)


def _feature(packet: QuantPacket, name: str) -> float | None:
    value = packet.features.get(name)
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    return float(value)


def _open(
    inp: GateInputs,
    cfg: GateConfig,
    now: datetime,
    run: _Run,
    symbol: str,
    candidate: LevelCandidate,
    packet: QuantPacket,
) -> GateResult:
    d, r, m, book = inp.decision, cfg.risk, inp.market, inp.book
    lim = cfg.limits

    def fail(reason: str, text: str, **kw: Any) -> GateResult:
        return run.fail(reason, text, intent=Intent.OPEN, symbol=symbol, **kw)

    if not cfg.opens_enabled:
        return fail(
            "mode_disabled",
            f"The run mode no longer enables {cfg.account.value}: no new position while its open "
            "positions and orders wind down",
        )
    filters = m.filters
    if filters is None or not filters.trading:
        return fail("symbol_not_trading", f"{symbol} is not TRADING in the current exchangeInfo")
    for name in ("entry", "invalidation", "tp1"):
        if not filters.price_aligned(getattr(candidate, name)):
            return fail("tick", f"Candidate {name} is not aligned to the current tick {filters.tick_size}")
    if candidate.rr < lim.min_rr:
        return fail("rr", f"Candidate R:R {candidate.rr:.2f} below {lim.min_rr}")
    atr = _feature(packet, "atr_1h")
    if atr is None or atr <= 0 or m.mark is None:
        return fail("missing_data", "ATR 1h or the Binance mark price is unavailable")
    atr_d = Decimal(repr(atr))
    max_distance = entry_distance_limit(atr_d, lim.max_entry_distance_atr)
    refused = entry_refusal(
        long=d.side is Side.LONG,
        mark=m.mark,
        entry=candidate.entry,
        stop=candidate.invalidation,
        tp1=candidate.tp1,
        max_distance=max_distance,
    )
    distance_atr = float(abs(candidate.entry - m.mark) / atr_d)
    if refused is not None:
        text = refused.text
        if refused.reason == ENTRY_DISTANCE:
            text = (
                f"Entry {candidate.entry} is {distance_atr:.2f} ATR from mark {m.mark}, "
                f"limit {lim.max_entry_distance_atr}"
            )
        return fail(refused.reason, text)
    run.ok(
        f"Candidate valid on current exchangeInfo, R:R {candidate.rr:.2f}, "
        f"entry {distance_atr:.2f} ATR from mark, mark between the invalidation and the take-profit"
    )

    # 9. fail-closed veto
    if book.kill.blocks_open:
        return fail("kill_state", f"No new position: {book.kill.describe()}")
    state = book.account_state
    max_age = lim.account_state_max_age_s
    if state is None or book.state_age_s is None or book.state_age_s > max_age:
        age_text = "missing" if book.state_age_s is None else f"{book.state_age_s:.0f} s old"
        return fail(
            "account_state_stale",
            f"AccountState {age_text}, limit {max_age} s",
            alert=Alert("account_state_stale", "warning", f"{cfg.account.value} AccountState {age_text}"),
        )
    if (
        m.mark_age_s is None
        or m.mark_age_s > cfg.stale_s
        or m.book_age_s is None
        or m.book_age_s > cfg.stale_s
    ):
        return fail(
            "stale_data",
            f"Binance data for {symbol} too old (mark {m.mark_age_s}, depth {m.book_age_s} s, "
            f"limit {cfg.stale_s} s)",
        )
    veto = evaluate_open(inp.flags, d.side, now, float(r.veto_scan_interval_s))
    if not veto.allowed:
        return fail(veto.reason or "veto", veto.text)
    run.ok(veto.text)

    # 10. sizing and ceilings
    oi = _feature(packet, "open_interest_usd")
    vol = _feature(packet, "quote_volume_1h_usd")
    if oi is None or vol is None:
        return fail("missing_data", "Open interest or 1 h volume is unavailable for the notional caps")
    try:
        sized = size_position(
            SizingInput(
                equity=state.equity,
                equity_age_s=book.state_age_s,
                side=d.side,
                p=d.p,
                manager_size=d.manager_size,
                flags_size_mult=veto.size_mult,
                flags_age_s=veto.flags_age_s,
                mode=cfg.account.value,
                mode_size_multiplier=cfg.mode_size_multiplier,
                entry=candidate.entry,
                stop=candidate.invalidation,
                filters=filters,
                base_asset=symbol.removesuffix("USDT"),
                open_interest_usd=oi,
                quote_volume_1h_usd=vol,
                risk_pct=lim.risk_pct,
                leverage_max=lim.leverage_max,
                max_margin_pct=lim.max_margin_pct,
                max_oi_frac=lim.max_oi_frac,
                max_volume_1h_frac=lim.max_volume_1h_frac,
                mmr_assumed=r.mmr_assumed,
                liq_distance_mult=lim.liq_distance_mult,
            )
        )
    except SizingRejectedError as exc:
        return fail(exc.reason, exc.detail, sizing=exc.breakdown or None)
    beta = _feature(packet, "beta_btc")
    breakdown = dict(sized.breakdown)
    breakdown["hedge_beta"] = beta
    if beta is not None and m.btc_mark is not None and m.btc_mark > 0:
        breakdown["hedge_qty"] = float(Decimal(repr(beta)) * sized.notional / m.btc_mark)
    run.ok(
        f"Size {sized.qty} {symbol} = {sized.notional:.2f} USD notional, leverage {sized.leverage}x, "
        f"risk {sized.risk_usd_actual:.2f} USD"
    )
    exposure = Exposure(
        symbol,
        d.side,
        sized.risk_usd_actual,
        parse_categories(packet.features.get("categories")),
        pending=True,
    )
    hedge = projected_hedge(
        book.hedge_qty,
        d.side,
        sized.notional,
        beta=beta,
        btc_mark=m.btc_mark,
        btc_atr_1h=_feature(packet, "btc_atr_1h"),
        stop_atr_mult=r.hedge.stop_atr_mult,
    )
    breakdown["hedge_risk_projected"] = float(hedge.risk_usd) if hedge is not None else None
    limits = PortfolioLimits(
        max_positions=lim.max_positions,
        max_positions_per_coin=lim.max_positions_per_coin,
        same_direction_risk_max=lim.same_direction_risk_max,
        max_positions_per_narrative=lim.max_positions_per_narrative,
        daily_loss_kill=lim.daily_loss_kill,
    )
    ceilings = check_portfolio(
        exposure,
        book.exposures,
        equity=state.equity,
        day_start_equity=state.day_start_equity,
        limits=limits,
        projected_hedge=hedge,
    )
    run.checks.extend(ceilings)
    failed = first_failure(ceilings)
    if failed is not None:
        reason = failed.reason or "ceiling"
        return GateResult(
            "rejected", reason, Intent.OPEN, symbol, tuple(run.checks), breakdown, hedge_beta=beta
        )

    # 11. leg plan
    legs = _open_legs(cfg, d, symbol, candidate, filters, sized.qty, sized.leverage, atr_d, max_distance, now)
    return GateResult(
        "approved",
        None,
        Intent.OPEN,
        symbol,
        tuple(run.checks),
        breakdown,
        intents=legs,
        hedge_beta=beta,
        exposure=exposure,
    )


def projected_hedge(
    book_qty: Decimal,
    side: Side,
    notional: Decimal,
    *,
    beta: float | None,
    btc_mark: Decimal | None,
    btc_atr_1h: float | None,
    stop_atr_mult: float,
) -> Exposure | None:
    """The BTC hedge book once it absorbed a new position of `notional` (the hedger's target move), valued
    at its book stop distance `stop_atr_mult x` BTC 1 h ATR. None when the book would be flat or BTC mark or
    ATR is unknown (the hedger never grows a book without them and clamps growth to the same ceiling)."""
    if btc_mark is None or btc_mark <= 0 or btc_atr_1h is None or btc_atr_1h <= 0:
        return None
    signed = notional if side is Side.LONG else -notional
    after = book_qty - Decimal(repr(beta if beta is not None else DEFAULT_BETA)) * signed / btc_mark
    if after == 0:
        return None
    distance = Decimal(repr(stop_atr_mult)) * Decimal(repr(btc_atr_1h))
    return Exposure(
        BTC_SYMBOL,
        Side.LONG if after > 0 else Side.SHORT,
        abs(after) * distance,
        is_hedge_book=True,
        pending=True,
    )


def _base(cfg: GateConfig, event_id: str, leg: Leg, symbol: str, now: datetime) -> dict[str, Any]:
    return {
        "intent_id": intent_id_for(cfg.account, event_id, leg, 0),
        "account": cfg.account,
        "event_id": event_id,
        "leg": leg,
        "seq": 0,
        "symbol": symbol,
        "client_id": client_id(event_id, leg, 0),
        "created_at": now,
        "key_id": cfg.key_id,
    }


def _exit_intent(
    cfg: GateConfig, d: DecisionMsg, symbol: str, held: HeldPosition, now: datetime
) -> OrderIntent:
    return OrderIntent(
        **_base(cfg, d.event_id, Leg.EXIT, symbol, now),
        side=OrderSide.SELL if held.side is Side.LONG else OrderSide.BUY,
        order_type=OrderType.MARKET,
        qty=held.qty,
        reduce_only=True,
    )


def _open_legs(
    cfg: GateConfig,
    d: DecisionMsg,
    symbol: str,
    c: LevelCandidate,
    filters: SymbolFilters,
    qty: Decimal,
    leverage: int,
    atr: Decimal,
    max_distance: Decimal,
    now: datetime,
) -> tuple[OrderIntent, ...]:
    r = cfg.risk
    entry_side = OrderSide.BUY if d.side is Side.LONG else OrderSide.SELL
    exit_side = OrderSide.SELL if d.side is Side.LONG else OrderSide.BUY
    time_stop = d.as_of + HORIZON[d.target_type]
    legs: list[OrderIntent] = [
        OrderIntent(
            **_base(cfg, d.event_id, Leg.SL, symbol, now),
            side=exit_side,
            order_type=OrderType.STOP_MARKET,
            qty=qty,
            trigger_price=c.invalidation,
            reduce_only=True,
            expires_at=time_stop,
        )
    ]
    tp_qty = filters.floor_qty(qty * Decimal(repr(r.tp1_fraction)))
    if tp_qty < filters.min_qty:
        tp_qty = Decimal(0)
    if tp_qty > 0:
        legs.append(
            OrderIntent(
                **_base(cfg, d.event_id, Leg.TP1, symbol, now),
                side=exit_side,
                order_type=OrderType.TAKE_PROFIT_MARKET,
                qty=tp_qty,
                trigger_price=c.tp1,
                reduce_only=True,
                expires_at=time_stop,
            )
        )
    trail_qty = qty - tp_qty
    if trail_qty > 0:
        distance = max(filters.ceil_price(atr * Decimal(repr(r.trail_atr_mult))), filters.tick_size)
        legs.append(
            OrderIntent(
                **_base(cfg, d.event_id, Leg.TRAIL, symbol, now),
                side=exit_side,
                order_type=OrderType.STOP_MARKET,
                qty=trail_qty,
                trigger_price=c.tp1,
                price=distance,
                reduce_only=True,
                expires_at=time_stop,
            )
        )
    slip = Decimal(repr(r.ioc_max_slippage))
    ioc_price = (
        filters.floor_price(c.entry * (1 + slip))
        if d.side is Side.LONG
        else filters.ceil_price(c.entry * (1 - slip))
    )
    for leg, tif, price in (
        (Leg.ENTRY_IOC, TimeInForce.IOC, ioc_price),
        (Leg.ENTRY, TimeInForce.GTX, c.entry),
    ):
        legs.append(
            OrderIntent(
                **_base(cfg, d.event_id, leg, symbol, now),
                side=entry_side,
                order_type=OrderType.LIMIT,
                qty=qty,
                price=price,
                reduce_only=False,
                tif=tif,
                leverage=leverage,
                max_entry_distance=max_distance,
            )
        )
    return tuple(legs)
