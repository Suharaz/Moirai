"""`PaperAdapter`: the `ExchangeAdapter` of namespace `paper` over the simulated venue.

The venue (`PaperVenue`) is persisted in `paper_state` after every change, so a restart resumes resting
orders with their queue estimates, conditional orders, positions and the wallet. The ledger is a separate
book fed by the venue's events, exactly as Binance's user data stream feeds it for live, so the reconciler
compares two independent books in paper too.

Events (order updates, fills, conditional triggers, funding) are queued and delivered to the sink by
`run()`; they are never delivered from inside an adapter call, because callers hold the namespace lock
that the sink takes. Callers that need a result right away read it from the returned snapshot or from
`user_trades`, like with Binance.

Only lake IO runs in a worker thread (`PaperFeed.read`, `PaperFeed.read_seed`, under the venue lock, so
the venue cannot change while they look at it). Every venue change runs on the event loop under the venue
lock (`PaperFeed.apply`, `PaperFeed.apply_seed`, the order calls), so the read-only calls, which run on
the event loop too, never see a change in progress.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Final

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session, sessionmaker

from hdt.contracts.common import Account
from hdt.core.clock import utcnow
from hdt.db.models.ledger import PaperStateRow
from hdt.db.session import transaction
from hdt.execution.adapter_base import (
    AlgoEvent,
    AlgoRequest,
    AlgoSnapshot,
    Balance,
    CashFlow,
    ExchangeAdapter,
    ExchangeEventSink,
    ExchangePosition,
    OrderEvent,
    OrderRequest,
    OrderSnapshot,
    TradeFill,
)
from hdt.execution.filters import FilterError, SymbolFilters, parse_exchange_filters
from hdt.execution.paper_feed import PaperFeed
from hdt.execution.paper_venue import PaperVenue, VenueEvent
from hdt.lake.pit_query import PitQuery

log = logging.getLogger(__name__)

EXCHANGE_INFO_LOOKBACK: Final[timedelta] = timedelta(days=2)
POLL_INTERVAL_S: Final[float] = 1.0


def _ms(ts: datetime) -> int:
    return int(ts.timestamp() * 1000)


class PaperAdapter(ExchangeAdapter):
    fill_model = "queue"

    def __init__(
        self,
        sessions: sessionmaker[Session],
        pit: PitQuery,
        *,
        maker_fee: Decimal,
        taker_fee: Decimal,
        slippage_bp: Decimal,
        starting_equity: Decimal,
        clock: Callable[[], datetime] = utcnow,
    ) -> None:
        self.account = Account.PAPER
        self.sessions = sessions
        self.pit = pit
        self.clock = clock
        self.venue = PaperVenue(
            maker_fee=maker_fee, taker_fee=taker_fee, slippage_bp=slippage_bp, wallet=starting_equity
        )
        self.feed = PaperFeed(pit, self.venue)
        self.starting_equity = starting_equity
        self._events: asyncio.Queue[VenueEvent | None] = asyncio.Queue()
        self._lock = asyncio.Lock()
        self._loaded = False

    # ------------------------------------------------------------------ persistence
    def load(self) -> bool:
        """Restore the venue from `paper_state`; False when this is a fresh paper account."""
        with self.sessions() as s:
            row = s.get(PaperStateRow, Account.PAPER.value)
            if row is not None:
                self.venue.load(row.state)
                self.feed.cursor = row.market_cursor
        self._loaded = True
        return row is not None

    def _save(self) -> None:
        now = utcnow()
        state = self.venue.to_dict()
        with transaction(self.sessions) as s:
            stmt = (
                pg_insert(PaperStateRow)
                .values(
                    account=Account.PAPER.value, state=state, market_cursor=self.feed.cursor, updated_at=now
                )
                .on_conflict_do_update(
                    index_elements=[PaperStateRow.account],
                    set_={"state": state, "market_cursor": self.feed.cursor, "updated_at": now},
                )
            )
            s.execute(stmt)

    def _emit(self, events: Sequence[VenueEvent]) -> None:
        for event in events:
            self._events.put_nowait(event)

    async def _mutate[T](self, fn: Callable[[int], T], symbol: str | None = None) -> T:
        """Run one venue change under the venue lock (seeding `symbol`'s market first) and persist it."""
        async with self._lock:
            if not self._loaded:
                self.load()
            if symbol is not None:
                seed = await asyncio.to_thread(self.feed.read_seed, symbol, self.clock())
                self._emit(self.feed.apply_seed(seed))
            result = fn(_ms(self.clock()))
            self._save()
            return result

    # ------------------------------------------------------------------ market data loop
    async def step(self, now: datetime | None = None) -> int:
        """Replay the lake up to `now` once; returns the number of venue events produced."""
        at = now or self.clock()
        async with self._lock:
            if not self._loaded:
                self.load()
            batch = await asyncio.to_thread(self.feed.read, at)
            events = self.feed.apply(batch)
            self._emit(events)  # the venue changed: its events are delivered even if persisting fails
            self._save()
        return len(events)

    async def run(
        self, sink: ExchangeEventSink, stop: asyncio.Event, poll_interval_s: float = POLL_INTERVAL_S
    ) -> None:
        """Feed market data and deliver venue events to the sink until `stop` is set."""
        deliver = asyncio.create_task(self._deliver(sink))
        try:
            while not stop.is_set():
                try:
                    await self.step()
                except Exception:  # a failed lake read or save must not stop the venue: retried next step
                    log.exception("paper venue replay step failed")
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=poll_interval_s)
        finally:
            self._events.put_nowait(None)
            await deliver

    async def drain(self, sink: ExchangeEventSink) -> int:
        """Deliver every queued event now (tests and replays)."""
        count = 0
        while not self._events.empty():
            event = self._events.get_nowait()
            if event is None:
                continue
            await self._dispatch(sink, event)
            count += 1
        return count

    async def _deliver(self, sink: ExchangeEventSink) -> None:
        while True:
            event = await self._events.get()
            if event is None:
                return
            try:
                await self._dispatch(sink, event)
            except Exception:
                log.exception("paper event delivery failed; the reconciler repairs the ledger")

    async def _dispatch(self, sink: ExchangeEventSink, event: VenueEvent) -> None:
        if isinstance(event, OrderEvent):
            await sink.on_order_event(event)
            if event.fill is not None:
                await sink.on_account_update(self.venue.wallet)
        elif isinstance(event, AlgoEvent):
            await sink.on_algo_event(event)

    # ------------------------------------------------------------------ ExchangeAdapter
    async def exchange_filters(self) -> dict[str, SymbolFilters]:
        """Mainnet exchangeInfo as recorded in the lake (paper trades mainnet symbols)."""
        now = self.clock()
        record = await asyncio.to_thread(
            self.pit.latest, "binance", "exchange_info", now, lookback=EXCHANGE_INFO_LOOKBACK
        )
        if record is None:
            raise FilterError("no recorded exchangeInfo in the lake within 2 days")
        return parse_exchange_filters(json.loads(record.body()))

    async def ensure_one_way(self) -> None:
        return None  # the simulated venue is One-way by construction

    async def prepare_symbol(self, symbol: str, leverage: int) -> None:
        def apply(_now_ms: int) -> None:
            self.venue.leverage[symbol] = leverage

        await self._mutate(apply, symbol)

    async def place_order(self, request: OrderRequest) -> OrderSnapshot:
        def apply(now_ms: int) -> tuple[OrderSnapshot, list[VenueEvent]]:
            return self.venue.place_order(request, now_ms)

        snap, events = await self._mutate(apply, request.symbol)
        self._emit(events)
        final = self.venue.orders.get(request.client_id)
        return final.snapshot() if final is not None else snap

    async def query_order(self, symbol: str, client_id: str) -> OrderSnapshot | None:
        order = self.venue.orders.get(client_id)
        return order.snapshot() if order is not None and order.symbol == symbol else None

    async def cancel_order(self, symbol: str, client_id: str) -> OrderSnapshot | None:
        def apply(now_ms: int) -> tuple[OrderSnapshot | None, list[VenueEvent]]:
            return self.venue.cancel_order(client_id, now_ms)

        snap, events = await self._mutate(apply)
        self._emit(events)
        return snap

    async def place_algo(self, request: AlgoRequest) -> AlgoSnapshot:
        def apply(now_ms: int) -> tuple[AlgoSnapshot, list[VenueEvent]]:
            return self.venue.place_algo(request, now_ms)

        snap, events = await self._mutate(apply, request.symbol)
        self._emit(events)
        return snap

    async def query_algo(self, client_algo_id: str) -> AlgoSnapshot | None:
        algo = self.venue.algos.get(client_algo_id)
        return algo.snapshot() if algo is not None else None

    async def cancel_algo(self, client_algo_id: str) -> AlgoSnapshot | None:
        def apply(now_ms: int) -> tuple[AlgoSnapshot | None, list[VenueEvent]]:
            return self.venue.cancel_algo(client_algo_id, now_ms)

        snap, events = await self._mutate(apply)
        self._emit(events)
        return snap

    async def cancel_all_algos(self, symbol: str) -> None:
        def apply(now_ms: int) -> list[VenueEvent]:
            return self.venue.cancel_all_algos(symbol, now_ms)

        self._emit(await self._mutate(apply))

    async def open_orders(self) -> list[OrderSnapshot]:
        return [
            o.snapshot()
            for key, o in self.venue.orders.items()
            if key == o.client_id and o.status in ("NEW", "PARTIALLY_FILLED")
        ]

    async def open_algo_orders(self) -> list[AlgoSnapshot]:
        return [a.snapshot() for a in self.venue.algos.values() if a.status == "NEW"]

    async def positions(self) -> list[ExchangePosition]:
        return self.venue.exchange_positions()

    async def balance(self) -> Balance:
        return self.venue.balance()

    async def user_trades(
        self, symbol: str, from_id: int | None, start: datetime | None, known: Mapping[int, str] | None = None
    ) -> list[TradeFill]:
        out = []
        for t in self.venue.trades:
            if t.symbol != symbol:
                continue
            if from_id is not None and t.trade_id < from_id:
                continue
            if start is not None and t.time < start:
                continue
            out.append(t)
        return out

    async def cash_flows(self, start: datetime) -> list[CashFlow]:
        return [f for f in self.venue.flows if f.time >= start]

    async def mark_prices(self, symbols: Sequence[str]) -> dict[str, Decimal]:
        out: dict[str, Decimal] = {}
        for symbol in symbols:
            mark = self.venue.mark(symbol)
            if mark is None:
                await self._mutate(lambda _now_ms: None, symbol)
                mark = self.venue.mark(symbol)
            if mark is not None:
                out[symbol] = mark
        return out
