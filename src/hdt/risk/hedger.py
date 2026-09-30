"""BTC hedge book rebalancer of one namespace (phase 09 section 7); the math lives in `hdt.risk.hedge_book`.

Each cycle reads the namespace's latest `AccountState` (fresh only), values every open alt position at its
Binance mark with the beta recorded on the verdict that opened it (`risk_verdicts.hedge_beta`, default 1),
and compares the pooled BTC target with the book the exchange holds. When the drift passes the configured
threshold it signs, in ONE transaction with a new `hedge_book` row, the event `hedge:{account}:{seq}`:
1. `sl` (closePosition STOP) at `stop_atr_mult x` BTC 1 h ATR from the mark whenever the book grows or
   flips side, so the whole book always has one stop. A shrinking book keeps its stop. It is published
   first (like an OPEN plan's stop before its entry): the hedge fill must find the book's stop level;
2. `hedge`: MARKET for the signed BTC delta (reduce-only when it only shrinks the book).
Both go through the `risk_intents` outbox like decision legs. A rebalance younger than `HEDGE_SETTLE_S`
is still in flight and is never stacked; growing the book is skipped while the namespace is paused or
killed, or when no fresh BTC ATR is known (a book never grows without a stop). A growing book is capped so
its side's total risk (the book to its new stop + that side's alt positions and pending entries) stays
within `same_direction_risk_max x equity` (phase 09 section 2: the ceiling includes the hedge book).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Final

import sqlalchemy as sa
from sqlalchemy.orm import Session, sessionmaker

from hdt.contracts.account import AccountState
from hdt.contracts.common import Account, Leg, OrderSide, OrderType
from hdt.contracts.order import OrderIntent
from hdt.core.clock import utcnow
from hdt.db.models.quant import QuantPacketRow
from hdt.db.models.risk import HedgeBookRow
from hdt.db.session import transaction
from hdt.execution.client_ids import client_id
from hdt.execution.filters import SymbolFilters
from hdt.risk.consumer import (
    AccountStateBook,
    IntentOutbox,
    LakeMarket,
    RiskRuntime,
    beta_for_positions,
    latest_hedge_row,
    load_book,
    open_positions,
    write_intents,
)
from hdt.risk.gate import intent_id_for
from hdt.risk.hedge_book import (
    BTC_SYMBOL,
    DEFAULT_BETA,
    HEDGE_SETTLE_S,
    AltLeg,
    Rebalance,
    book_side,
    book_stop,
    cap_growth,
    hedge_event_id,
    plan_rebalance,
    target_notional,
    target_qty,
)
from hdt.risk.limits import side_risk
from hdt.risk.signer import IntentSigner
from hdt.settings.ceilings import RiskLimits, clip_risk_limits

log = logging.getLogger(__name__)

ATR_MAX_AGE: Final[timedelta] = timedelta(hours=2)
ATR_FEATURE: Final[str] = "btc_atr_1h"


@dataclass(frozen=True)
class BookView:
    """What the rebalancer knows about the namespace at one instant."""

    legs: tuple[AltLeg, ...]
    current: Decimal
    btc_mark: Decimal


def alt_legs(state: AccountState, betas: dict[str, float]) -> tuple[AltLeg, ...]:
    return tuple(
        AltLeg(p.symbol, p.qty * p.mark_price, betas.get(p.symbol, DEFAULT_BETA))
        for p in state.positions
        if p.qty != 0 and not p.is_hedge_book and p.symbol != BTC_SYMBOL
    )


def book_qty(state: AccountState) -> tuple[Decimal, Decimal | None]:
    """Signed BTC book and its mark (0, None when the namespace holds no book)."""
    for p in state.positions:
        if p.qty != 0 and (p.is_hedge_book or p.symbol == BTC_SYMBOL):
            return p.qty, p.mark_price
    return Decimal(0), None


def weighted_beta(legs: Sequence[AltLeg]) -> float | None:
    weight = sum((abs(leg.notional) for leg in legs), Decimal(0))
    if weight == 0:
        return None
    return float(sum((Decimal(repr(leg.beta)) * abs(leg.notional) for leg in legs), Decimal(0)) / weight)


def latest_btc_atr(session: Session, coin_ids: Sequence[int], now: datetime) -> Decimal | None:
    """`btc_atr_1h` of the newest packet (of a held coin) built within `ATR_MAX_AGE`."""
    if not coin_ids:
        return None
    payload = session.scalars(
        sa.select(QuantPacketRow.payload)
        .where(QuantPacketRow.coin_id.in_(coin_ids), QuantPacketRow.as_of >= now - ATR_MAX_AGE)
        .order_by(QuantPacketRow.as_of.desc())
        .limit(1)
    ).first()
    features = payload.get("features") if isinstance(payload, dict) else None
    value = features.get(ATR_FEATURE) if isinstance(features, dict) else None
    if isinstance(value, bool) or not isinstance(value, int | float) or value <= 0:
        return None
    return Decimal(repr(float(value)))


class HedgeRebalancer:
    def __init__(
        self,
        *,
        account: Account,
        sessions: sessionmaker[Session],
        signer: IntentSigner,
        states: AccountStateBook,
        market: LakeMarket,
        outbox: IntentOutbox,
        runtime: Callable[[], RiskRuntime],
        clock: Callable[[], datetime] = utcnow,
    ) -> None:
        self.account = account
        self.sessions = sessions
        self.signer = signer
        self.states = states
        self.market = market
        self.outbox = outbox
        self.runtime = runtime
        self.clock = clock

    async def run_once(self) -> HedgeBookRow | None:
        """One rebalance check; returns the new `hedge_book` row when a rebalance was signed."""
        now = self.clock()
        runtime = self.runtime()
        state = self.states.get(self.account)
        max_age = clip_risk_limits(RiskLimits.of(runtime.risk)).account_state_max_age_s
        if state is None or (now - state.ts).total_seconds() > max_age:
            return None
        filters = await asyncio.to_thread(self.market.filters, BTC_SYMBOL, now)
        if filters is None:
            log.warning("hedge book: no BTC exchangeInfo in the lake", extra={"account": self.account.value})
            return None
        current, book_mark = book_qty(state)
        btc_mark = book_mark
        if btc_mark is None:
            mark = (await asyncio.to_thread(self.market.marks, {BTC_SYMBOL}, now)).get(BTC_SYMBOL)
            btc_mark = mark.price if mark is not None else None
        if btc_mark is None or btc_mark <= 0:
            log.warning("hedge book: no BTC mark", extra={"account": self.account.value})
            return None
        coin_ids = await asyncio.to_thread(self._coin_ids, state, now)
        row = await asyncio.to_thread(
            self._rebalance, state, current, btc_mark, filters, coin_ids, runtime, now
        )
        if row is not None:
            await self.outbox.relay_once()
        return row

    def _coin_ids(self, state: AccountState, now: datetime) -> list[int]:
        ids: list[int] = []
        for p in state.positions:
            member = self.market.member_by_symbol(p.symbol, now) if p.qty != 0 else None
            if member is not None and p.symbol != BTC_SYMBOL:
                ids.append(member.cmc_id)
        return ids

    def _rebalance(
        self,
        state: AccountState,
        current: Decimal,
        btc_mark: Decimal,
        filters: SymbolFilters,
        coin_ids: Sequence[int],
        runtime: RiskRuntime,
        now: datetime,
    ) -> HedgeBookRow | None:
        a = self.account
        cfg = runtime.risk
        lim = clip_risk_limits(RiskLimits.of(cfg))
        with transaction(self.sessions) as s:
            latest = latest_hedge_row(s, a)
            if latest is not None and (now - latest.updated_at).total_seconds() < HEDGE_SETTLE_S:
                return None
            legs = alt_legs(state, beta_for_positions(s, a, open_positions(s, a)))
            target = target_qty(legs, btc_mark, filters)
            plan = plan_rebalance(target, current, btc_mark, filters, cfg.hedge)
            if not plan.needed:
                return None
            after = current + plan.delta
            atr: Decimal | None = None
            stop: Decimal | None = None
            if not plan.reduces:
                book = load_book(s, a, state, now)
                if book.kill.blocks_open:
                    log.info(
                        "hedge book growth skipped", extra={"account": a.value, "kill": book.kill.describe()}
                    )
                    return None
                atr = latest_btc_atr(s, coin_ids, now)
                if atr is None:
                    log.warning("hedge book growth skipped: no fresh BTC ATR", extra={"account": a.value})
                    return None
                stop = book_stop(after, btc_mark, atr, cfg.hedge, filters)
                side = book_side(after)
                assert side is not None  # a growing book is never flat
                alts = [e for e in book.exposures if not e.is_hedge_book]
                room = state.equity * Decimal(repr(lim.same_direction_risk_max)) - side_risk(alts, side)
                capped = cap_growth(plan, room, abs(btc_mark - stop), btc_mark, filters)
                if capped != plan:
                    log.warning(
                        "hedge book growth capped",
                        extra={"account": a.value, "reason": capped.reason, "room": str(room)},
                    )
                    plan = capped
                    if not plan.needed:
                        return None
                    after = current + plan.delta
                    stop = None if plan.reduces else book_stop(after, btc_mark, atr, cfg.hedge, filters)
            seq = (
                int(
                    s.scalar(
                        sa.select(sa.func.coalesce(sa.func.max(HedgeBookRow.rebalance_seq), 0)).where(
                            HedgeBookRow.account == a.value
                        )
                    )
                    or 0
                )
                + 1
            )
            event_id = hedge_event_id(a, seq)
            intents = [
                self.signer.sign(i) for i in self._intents(event_id, plan, after, stop, lim.leverage_max, now)
            ]
            row = HedgeBookRow(
                account=a.value,
                symbol=BTC_SYMBOL,
                target_qty=target,
                actual_qty=current,
                target_notional=target_notional(legs),
                beta=weighted_beta(legs),
                btc_mark=btc_mark,
                btc_atr_1h=atr,
                stop_price=stop if stop is not None else (latest.stop_price if latest is not None else None),
                rebalance_seq=seq,
                event_id=event_id,
                updated_at=now,
            )
            s.add(row)
            write_intents(s, intents, now)
        log.info(
            "hedge rebalance signed",
            extra={
                "account": a.value,
                "event_id": event_id,
                "current": str(current),
                "target": str(target),
                "delta": str(plan.delta),
                "reason": plan.reason,
            },
        )
        return row

    def _intents(
        self,
        event_id: str,
        plan: Rebalance,
        after: Decimal,
        stop: Decimal | None,
        leverage: int,
        now: datetime,
    ) -> list[OrderIntent]:
        a = self.account
        hedge = OrderIntent(
            intent_id=intent_id_for(a, event_id, Leg.HEDGE, 0),
            account=a,
            event_id=event_id,
            leg=Leg.HEDGE,
            seq=0,
            symbol=BTC_SYMBOL,
            side=OrderSide.BUY if plan.delta > 0 else OrderSide.SELL,
            order_type=OrderType.MARKET,
            qty=abs(plan.delta),
            reduce_only=plan.reduces,
            leverage=None if plan.reduces else leverage,
            client_id=client_id(event_id, Leg.HEDGE, 0),
            created_at=now,
            key_id=self.signer.key_id,
        )
        if stop is None or after == 0:
            return [hedge]
        return [
            OrderIntent(
                intent_id=intent_id_for(a, event_id, Leg.SL, 0),
                account=a,
                event_id=event_id,
                leg=Leg.SL,
                seq=0,
                symbol=BTC_SYMBOL,
                side=OrderSide.SELL if after > 0 else OrderSide.BUY,
                order_type=OrderType.STOP_MARKET,
                trigger_price=stop,
                reduce_only=False,
                close_position=True,
                client_id=client_id(event_id, Leg.SL, 0),
                created_at=now,
                key_id=self.signer.key_id,
            ),
            hedge,
        ]
