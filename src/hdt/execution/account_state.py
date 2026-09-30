"""`AccountState` publisher of one namespace (phase 09 section 5, step 10).

Emits to stream `account_state` every `account_state_interval_s` (5 s) and whenever something changed
(fills, order / algo updates, kill state), **only while the namespace is synced**: during RESYNCING nothing
is emitted, so Risk's 30 s staleness veto also covers a lost user stream.

Equity, available, positions and marks come from the exchange (`/fapi/v3/account`, `positionRisk`; the
paper venue for `paper`); open orders and conditional orders from the ledger, which the user stream and
the reconciler keep current. The first equity of each UTC day is the day-start anchor of the daily loss
kill: equity at or below `daily_loss_kill` (never looser than -2%) kills the namespace (cause
`daily_loss`); three quarters of it (-1.5% at the -2% default, the level of the ops alert catalog) raises
a `daily_loss_warning` once per UTC day (the alert episode is the date, so each day pages again). The
warnings of earlier UTC days are resolved once the day rolls (also after a restart), so open alerts never
pile up. An equity snapshot row is written at most once a minute for the console and the public dashboard.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Final

from redis.asyncio import Redis

from hdt.contracts.account import AccountState, OpenAlgoOrderState, OpenOrderState, PositionState
from hdt.contracts.common import OrderSide, OrderType, TimeInForce
from hdt.contracts.streams import Stream
from hdt.core.alerts import open_episodes
from hdt.core.clock import ensure_utc
from hdt.core.streams import publish
from hdt.execution.kill_switch import KillSwitch
from hdt.execution.limits import BTC_SYMBOL
from hdt.execution.runtime import Namespace
from hdt.risk.limits import daily_loss_breached, daily_pnl_fraction
from hdt.settings.ceilings import DAILY_LOSS_KILL_FLOOR

log = logging.getLogger(__name__)

SNAPSHOT_EVERY: Final[timedelta] = timedelta(minutes=1)
MIN_SPACING_S: Final[float] = 1.0
DAILY_LOSS_WARNING_SHARE: Final[float] = 0.75
"""Share of the daily loss kill level at which the warning fires (-1.5% at the -2% kill)."""
_TIF = {t.value for t in TimeInForce}
_ORDER_TYPES = {t.value for t in OrderType}


class AccountStatePublisher:
    def __init__(
        self,
        ns: Namespace,
        redis: Redis | None,
        kill: KillSwitch,
        *,
        interval_s: float,
        maxlen: int | None,
    ) -> None:
        self.ns = ns
        self.redis = redis
        self.kill = kill
        self.interval_s = interval_s
        self.maxlen = maxlen
        self._last_snapshot: datetime | None = None
        self._warned_day: date | None = None
        self._swept_day: date | None = None  # UTC day whose earlier warnings are resolved
        self.last: AccountState | None = None

    async def build(self) -> AccountState | None:
        """Current state (lock held by the caller); None while re-syncing."""
        ns = self.ns
        if not ns.synced:
            return None
        adapter = ns.adapter
        balance = await adapter.balance()
        positions = await adapter.positions()
        ns.balance = balance
        now = ns.clock()
        day_start = ns.ledger.day_start_equity(now.date(), balance.equity)
        hedge = {p.symbol for p in ns.ledger.open_positions() if p.is_hedge_book} | {BTC_SYMBOL}
        state = AccountState(
            account=ns.account,
            equity=balance.equity,
            available=balance.available,
            day_start_equity=day_start,
            positions=tuple(
                PositionState(
                    symbol=p.symbol,
                    qty=p.qty,
                    entry_price=p.entry_price,
                    mark_price=p.mark_price,
                    unrealized_pnl=p.unrealized_pnl,
                    leverage=max(1, p.leverage),
                    liquidation_price=p.liquidation_price,
                    margin_type=p.margin_type,
                    is_hedge_book=p.symbol in hedge,
                )
                for p in positions
                if p.qty != 0
            ),
            open_orders=tuple(
                OpenOrderState(
                    symbol=o.symbol,
                    client_id=o.client_id,
                    side=OrderSide(o.side),
                    order_type=OrderType(o.order_type),
                    price=o.price,
                    qty=o.qty,
                    executed_qty=o.executed_qty,
                    reduce_only=o.reduce_only,
                    tif=TimeInForce(o.tif) if o.tif in _TIF else None,
                    status=o.status,
                )
                for o in ns.ledger.open_orders()
                if o.order_type in _ORDER_TYPES
            ),
            open_algo_orders=tuple(
                OpenAlgoOrderState(
                    symbol=a.symbol,
                    client_algo_id=a.client_algo_id,
                    side=OrderSide(a.side),
                    order_type=OrderType(a.order_type),
                    trigger_price=a.trigger_price,
                    qty=a.qty,
                    reduce_only=a.reduce_only,
                    close_position=a.close_position,
                    status=a.status,
                )
                for a in ns.ledger.working_algos()
                if a.order_type in _ORDER_TYPES
            ),
            ts=now,
        )
        if self._last_snapshot is None or now - self._last_snapshot >= SNAPSHOT_EVERY:
            ns.ledger.insert_equity_snapshot(
                ts=now,
                equity=balance.equity,
                available=balance.available,
                day_start_equity=day_start,
                wallet_balance=balance.wallet_balance,
                unrealized_pnl=balance.unrealized_pnl,
                open_positions=len(state.positions),
            )
            self._last_snapshot = now
        await self._daily_loss(balance.equity, day_start, now)
        return state

    async def _daily_loss(self, equity: Decimal, day_start: Decimal, now: datetime) -> None:
        ns = self.ns
        self._resolve_earlier_warnings(now.date())
        limit = max(ns.risk().daily_loss_kill, DAILY_LOSS_KILL_FLOOR)
        pnl = daily_pnl_fraction(equity, day_start)
        if daily_loss_breached(equity, day_start, limit):
            kill = ns.ledger.kill_state()
            killed_today = (
                kill is not None
                and kill.state == "killed"
                and kill.cause == "daily_loss"
                and kill.since is not None
                and ensure_utc(kill.since).date() == ensure_utc(now).date()
            )
            if not killed_today:  # a weaker kill (manual, reconcile) takes the daily-loss cause
                await self.kill.engage("daily_loss", f"daily PnL {pnl:+.2%} at or below {limit:.2%}")
        elif pnl <= limit * DAILY_LOSS_WARNING_SHARE and self._warned_day != now.date():
            self._warned_day = now.date()
            ns.alert(
                "daily_loss_warning",
                "warning",
                f"{ns.name} daily PnL {pnl:+.2%}, kill at {limit:.2%}",
                episode=now.date().isoformat(),
            )

    def _resolve_earlier_warnings(self, today: date) -> None:
        """Resolve the open `daily_loss_warning` alerts of UTC days before `today` (once per day; a failed
        read is retried on the next publish)."""
        if self._swept_day == today:
            return
        ns = self.ns
        try:
            with ns.sessions() as s:
                episodes = open_episodes(s, kind="daily_loss_warning", account=ns.name)
        except Exception:
            log.exception("open daily loss warnings could not be read", extra={"account": ns.name})
            return
        for episode in episodes:
            try:
                day = date.fromisoformat(episode)
            except ValueError:
                continue
            if day < today:
                ns.resolve("daily_loss_warning", episode=episode)
        self._swept_day = today

    async def publish_once(self) -> AccountState | None:
        async with self.ns.lock:
            state = await self.build()
        if state is not None:
            self.last = state
            if self.redis is not None:
                await publish(self.redis, Stream.ACCOUNT_STATE, state, maxlen=self.maxlen)
        return state

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            self.ns.changed.clear()
            try:
                await self.publish_once()
            except Exception:
                log.exception("AccountState publish failed", extra={"account": self.ns.name})
            with contextlib.suppress(TimeoutError):  # at most one publish per MIN_SPACING_S
                await asyncio.wait_for(stop.wait(), timeout=MIN_SPACING_S)
            if self.ns.changed.is_set() or stop.is_set():
                continue
            waiter = asyncio.create_task(self.ns.changed.wait())
            stopper = asyncio.create_task(stop.wait())
            await asyncio.wait(
                {waiter, stopper},
                timeout=max(0.0, self.interval_s - MIN_SPACING_S),
                return_when=asyncio.FIRST_COMPLETED,
            )
            waiter.cancel()
            stopper.cancel()
