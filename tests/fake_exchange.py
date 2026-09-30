"""In-process Binance stand-in and execution harness for the phase 09 tests (dev Postgres, no network).

`FakeExchange` implements `ExchangeAdapter` over the paper venue state machine (`PaperVenue`), so orders,
conditional orders, fills, positions, fees and the wallet follow the same rules as the exchange. On top it
models the failures the tests need:
- `stream_up = False`: push events (the user data stream) are lost, REST keeps working;
- `rest_up = False`: the network is gone; mutating calls raise `UnknownOrderStateError` (never landed),
  reads raise `ExchangeError`;
- `lost_responses = n`: the next n mutating calls land on the exchange but their response is lost;
- `reject_algos = n`: the next n conditional placements are rejected definitively;
- `stuck_cancels = True`: cancels of regular orders time out and the order stays open (never confirmed);
- `cut_after_landing = True`: the next mutating call lands, then the whole network drops (REST and stream).
Every mutating call is appended to `calls` (`place_order:<cid>`, `place_algo:<cid>`, `cancel_order:<cid>`,
`cancel_algo:<cid>`, `cancel_all_algos:<symbol>`); `synced_at_call` pairs it with the namespace sync flag.

`Harness` wires one namespace exactly as the execution service does (ledger, stop guard, order manager,
kill switch, reconciler, re-sync, intake) around an injected adapter and a controllable clock.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Final

import sqlalchemy as sa
from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

from hdt.contracts.common import Account, Leg, Side
from hdt.contracts.order import OrderIntent
from hdt.core.clock import utcnow
from hdt.core.config import RiskFile, static_config
from hdt.db.session import make_session_factory
from hdt.execution.adapter_base import (
    AlgoEvent,
    AlgoRequest,
    AlgoSnapshot,
    Balance,
    CashFlow,
    ExchangeAdapter,
    ExchangeError,
    ExchangeEventSink,
    ExchangePosition,
    OrderEvent,
    OrderRejectedError,
    OrderRequest,
    OrderSnapshot,
    PositionModeError,
    TradeFill,
    UnknownOrderStateError,
)
from hdt.execution.client_ids import parse_client_id
from hdt.execution.filters import SymbolFilters
from hdt.execution.intake import Intake
from hdt.execution.kill_switch import KillSwitch
from hdt.execution.ledger import Ledger
from hdt.execution.order_manager import OrderManager
from hdt.execution.paper_venue import Book, PaperVenue, VenueEvent
from hdt.execution.reconciler import Reconciler
from hdt.execution.resync import Resync
from hdt.execution.runtime import Namespace
from hdt.execution.stop_guard import StopGuard
from hdt.execution.verify import Keyring
from intent_builders import open_plan
from risk_builders import symbol_filters

SOL = "SOLUSDT"
ETH = "ETHUSDT"
BTC = "BTCUSDT"
FILTERS: Final[dict[str, SymbolFilters]] = {
    SOL: symbol_filters(SOL),
    ETH: symbol_filters(ETH),
    BTC: symbol_filters(BTC, tick="0.1", step="0.001", min_notional="100"),
}
PHASE09_TABLES = (
    "processed_intents",
    "orders",
    "algo_orders",
    "fills",
    "positions",
    "cash_flows",
    "equity_snapshots",
    "kill_state",
    "reconcile_runs",
    "exec_accounts",
    "paper_state",
    "processed_decisions",
    "risk_verdicts",
    "risk_intents",
    "hedge_book",
)


def ledger_sessions(engine: Engine) -> sessionmaker[Session]:
    """A session factory on the test database with every phase 09 table emptied first."""
    with engine.begin() as conn:
        conn.execute(sa.text(f"TRUNCATE {', '.join(PHASE09_TABLES)} RESTART IDENTITY"))
    return make_session_factory(engine)


class FakeClock:
    """Business clock of the namespace; starts at the real time so ledger timestamps stay comparable."""

    def __init__(self, start: datetime | None = None) -> None:
        self.now = start or utcnow()

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> datetime:
        self.now += timedelta(seconds=seconds)
        return self.now


def depth(bids: Iterable[tuple[str, str]], asks: Iterable[tuple[str, str]]) -> Book:
    """A book maintained from 100 ms depth diffs (fill model `queue`)."""
    book = Book()
    book.apply_snapshot(list(bids), list(asks), u=1, ts_ms=0)
    book.apply_diff(2, 2, 1, [], [], ts_ms=0)
    return book


class FakeExchange(ExchangeAdapter):
    fill_model = "exchange"

    def __init__(
        self,
        account: Account,
        clock: FakeClock,
        filters: Mapping[str, SymbolFilters],
        *,
        wallet: Decimal = Decimal("10000"),
        maker_fee: Decimal = Decimal("0.0002"),
        taker_fee: Decimal = Decimal("0.0005"),
        slippage_bp: Decimal = Decimal(0),
    ) -> None:
        self.account = account
        self.clock = clock
        self.venue = PaperVenue(
            maker_fee=maker_fee, taker_fee=taker_fee, slippage_bp=slippage_bp, wallet=wallet
        )
        self._filters = dict(filters)
        self.pushes: list[VenueEvent] = []
        self.stream_up = True
        self.rest_up = True
        self.lost_responses = 0
        self.reject_algos = 0
        self.stuck_cancels = False
        self.cut_after_landing = False
        self.one_way = True
        self.liquidation: dict[str, Decimal] = {}
        self.calls: list[str] = []
        self.synced_at_call: list[tuple[str, bool]] = []
        self.observer: Callable[[], bool] | None = None  # returns the namespace sync flag

    # ------------------------------------------------------------------ test controls
    def _ms(self) -> int:
        return int(self.clock().timestamp() * 1000)

    def _market_ms(self) -> int:
        """Market data arrives 1 ms after whatever happened before it."""
        self.clock.advance(0.001)
        return self._ms()

    def _push(self, events: Sequence[VenueEvent]) -> None:
        if self.stream_up:
            self.pushes.extend(events)

    def _rest(self, name: str, *, mutating: bool) -> None:
        if not self.rest_up:
            if mutating:
                raise UnknownOrderStateError(f"{name}: network unreachable")
            raise ExchangeError(f"{name}: network unreachable")

    def _record(self, call: str) -> None:
        self.calls.append(call)
        self.synced_at_call.append((call, self.observer() if self.observer is not None else True))

    def _maybe_lose_response(self, name: str) -> None:
        if self.cut_after_landing:
            self.cut_after_landing = False
            self.rest_up = False
            self.stream_up = False
            raise UnknownOrderStateError(f"{name}: connection reset after the request landed")
        if self.lost_responses > 0:
            self.lost_responses -= 1
            raise UnknownOrderStateError(f"{name}: response lost after the request landed")

    async def deliver(self, sink: ExchangeEventSink) -> int:
        """Deliver queued push events in exchange order (what the user data stream would do)."""
        count = 0
        while self.pushes:
            event = self.pushes.pop(0)
            if isinstance(event, OrderEvent):
                await sink.on_order_event(event)
                if event.fill is not None:
                    await sink.on_account_update(self.venue.wallet)
            elif isinstance(event, AlgoEvent):
                await sink.on_algo_event(event)
            count += 1
        return count

    def set_book(self, symbol: str, bids: Iterable[tuple[str, str]], asks: Iterable[tuple[str, str]]) -> None:
        self.venue.books[symbol] = depth(bids, asks)
        self._push(self.venue.on_book(symbol, self._market_ms()))

    def set_mark(self, symbol: str, mark: str | Decimal) -> None:
        self._push(self.venue.on_mark(symbol, Decimal(mark), self._market_ms()))

    def trade(self, symbol: str, price: str, qty: str, *, buyer_maker: bool) -> None:
        self._push(self.venue.on_trade(symbol, Decimal(price), Decimal(qty), buyer_maker, self._market_ms()))

    def external_cancel_algo(self, client_algo_id: str) -> None:
        """The conditional order disappears on the exchange side (UI cancel, exchange expiry)."""
        _, events = self.venue.cancel_algo(client_algo_id, self._ms())
        self._push(events)

    def algos(self) -> list[AlgoSnapshot]:
        return [a.snapshot() for a in self.venue.algos.values() if a.status == "NEW"]

    def mutating_calls(self, prefix: str = "") -> list[str]:
        return [c for c in self.calls if c.startswith(prefix)]

    # ------------------------------------------------------------------ ExchangeAdapter
    async def exchange_filters(self) -> dict[str, SymbolFilters]:
        self._rest("exchangeInfo", mutating=False)
        return dict(self._filters)

    async def ensure_one_way(self) -> None:
        self._rest("positionSide/dual", mutating=False)
        if not self.one_way:
            raise PositionModeError("account is in Hedge Mode")

    async def prepare_symbol(self, symbol: str, leverage: int) -> None:
        self._rest("leverage", mutating=True)
        self.venue.leverage[symbol] = leverage

    async def place_order(self, request: OrderRequest) -> OrderSnapshot:
        self._rest("order", mutating=True)
        self._record(f"place_order:{request.client_id}")
        _, events = self.venue.place_order(request, self._ms())
        self._push(events)
        self._maybe_lose_response("order")
        return self.venue.orders[request.client_id].snapshot()

    async def query_order(self, symbol: str, client_id: str) -> OrderSnapshot | None:
        self._rest("order", mutating=False)
        order = self.venue.orders.get(client_id)
        return order.snapshot() if order is not None and order.symbol == symbol else None

    async def cancel_order(self, symbol: str, client_id: str) -> OrderSnapshot | None:
        self._rest("order", mutating=True)
        self._record(f"cancel_order:{client_id}")
        if self.stuck_cancels:
            raise UnknownOrderStateError("cancel: timed out, order still working")
        snap, events = self.venue.cancel_order(client_id, self._ms())
        self._push(events)
        self._maybe_lose_response("cancel")
        return snap

    async def place_algo(self, request: AlgoRequest) -> AlgoSnapshot:
        self._rest("algoOrder", mutating=True)
        self._record(f"place_algo:{request.client_algo_id}")
        if self.reject_algos > 0:
            self.reject_algos -= 1
            raise OrderRejectedError("algoOrder rejected", code=-4131)
        snap, events = self.venue.place_algo(request, self._ms())
        self._push(events)
        self._maybe_lose_response("algoOrder")
        return snap

    async def query_algo(self, client_algo_id: str) -> AlgoSnapshot | None:
        self._rest("algoOrder", mutating=False)
        algo = self.venue.algos.get(client_algo_id)
        return algo.snapshot() if algo is not None else None

    async def cancel_algo(self, client_algo_id: str) -> AlgoSnapshot | None:
        self._rest("algoOrder", mutating=True)
        self._record(f"cancel_algo:{client_algo_id}")
        snap, events = self.venue.cancel_algo(client_algo_id, self._ms())
        self._push(events)
        return snap

    async def cancel_all_algos(self, symbol: str) -> None:
        self._rest("algoOpenOrders", mutating=True)
        self._record(f"cancel_all_algos:{symbol}")
        self._push(self.venue.cancel_all_algos(symbol, self._ms()))

    async def open_orders(self) -> list[OrderSnapshot]:
        self._rest("openOrders", mutating=False)
        return [
            o.snapshot()
            for key, o in self.venue.orders.items()
            if key == o.client_id and o.status in ("NEW", "PARTIALLY_FILLED")
        ]

    async def open_algo_orders(self) -> list[AlgoSnapshot]:
        self._rest("openAlgoOrders", mutating=False)
        return [a.snapshot() for a in self.venue.algos.values() if a.status == "NEW"]

    async def positions(self) -> list[ExchangePosition]:
        self._rest("positionRisk", mutating=False)
        out = []
        for p in self.venue.exchange_positions():
            liq = self.liquidation.get(p.symbol)
            out.append(p if liq is None else replace(p, liquidation_price=liq))
        return out

    async def balance(self) -> Balance:
        self._rest("account", mutating=False)
        return self.venue.balance()

    async def user_trades(
        self, symbol: str, from_id: int | None, start: datetime | None, known: Mapping[int, str] | None = None
    ) -> list[TradeFill]:
        self._rest("userTrades", mutating=False)
        return [
            t
            for t in self.venue.trades
            if t.symbol == symbol
            and (from_id is None or t.trade_id >= from_id)
            and (start is None or t.time >= start)
        ]

    async def cash_flows(self, start: datetime) -> list[CashFlow]:
        self._rest("income", mutating=False)
        return [f for f in self.venue.flows if f.time >= start]

    async def mark_prices(self, symbols: Sequence[str]) -> dict[str, Decimal]:
        self._rest("premiumIndex", mutating=False)
        return {s: m for s in symbols if (m := self.venue.mark(s)) is not None}


@dataclass
class Harness:
    """One execution namespace wired like the service, over an injected adapter."""

    ns: Namespace
    exchange: FakeExchange
    clock: FakeClock
    guard: StopGuard
    manager: OrderManager
    kill: KillSwitch
    reconciler: Reconciler
    resync: Resync
    intake: Intake
    statuses: list[str] = field(default_factory=list)

    @property
    def ledger(self) -> Ledger:
        return self.ns.ledger

    async def start(self) -> None:
        """Startup: one-way check, first re-sync (anchors the wallet), then open for business."""
        await self.exchange.ensure_one_way()
        await self.resync.run()

    async def send(self, intents: Iterable[OrderIntent]) -> list[str]:
        """Feed signed intents through the intake (as the `exec-{account}` consumer does)."""
        out = []
        for intent in intents:
            async with self.ns.lock:
                out.append(await self.intake.process(intent.model_dump(mode="json")))
        self.statuses.extend(out)
        return out

    async def pump(self) -> int:
        """Deliver pending exchange push events to the order manager."""
        return await self.exchange.deliver(self.manager)

    async def tick(self, seconds: float = 0.0) -> None:
        self.clock.advance(seconds)
        async with self.ns.lock:
            await self.manager.tick(self.clock())

    async def reconcile(self) -> Any:
        async with self.ns.lock:
            return await self.reconciler.run_once()


def make_harness(
    sessions: sessionmaker[Session],
    account: Account,
    keyring: Keyring,
    *,
    filters: Mapping[str, SymbolFilters] = FILTERS,
    clock: FakeClock | None = None,
    allowlist: Iterable[str] | None = None,
    risk: RiskFile | None = None,
    exchange: FakeExchange | None = None,
    with_market: bool = True,
) -> Harness:
    """A namespace over the fake exchange; `with_market` seeds the SOLUSDT book and mark (100.00)."""
    clock = clock or FakeClock()
    exchange = exchange or FakeExchange(account, clock, filters)
    pinned = risk or static_config().risk
    universe = frozenset(allowlist if allowlist is not None else (s for s in filters if s != BTC))
    ns = Namespace(
        account=account,
        adapter=exchange,
        ledger=Ledger(sessions, account),
        sessions=sessions,
        risk=lambda: pinned,
        allowlist=lambda: universe,
        clock=clock,
    )
    exchange.observer = lambda: ns.synced
    guard = StopGuard(ns)
    manager = OrderManager(ns, guard)
    kill = KillSwitch(ns, manager, guard)
    reconciler = Reconciler(ns, manager, guard, kill)
    resync = Resync(ns, reconciler)
    intake = Intake(ns, keyring, manager)
    if with_market:
        seed_market(exchange)
    return Harness(ns, exchange, clock, guard, manager, kill, reconciler, resync, intake)


def seed_market(exchange: FakeExchange, symbol: str = SOL) -> None:
    """Mark 100.00; bids 100.00 x 5, 99.90 x 50, 99.80 x 100; asks 100.10 x 8, 100.20 x 50, 100.30 x 100."""
    exchange.set_mark(symbol, "100.00")
    exchange.set_book(
        symbol,
        [("100.00", "5"), ("99.90", "50"), ("99.80", "100")],
        [("100.10", "8"), ("100.20", "50"), ("100.30", "100")],
    )


def working(h: Harness, leg: Leg | None = None, symbol: str | None = None) -> list[AlgoSnapshot]:
    """Working conditional orders on the exchange (optionally of one leg / symbol), oldest first."""
    out = []
    for algo in h.exchange.algos():
        parsed = parse_client_id(algo.client_algo_id)
        if leg is not None and (parsed is None or parsed.leg is not leg):
            continue
        if symbol is not None and algo.symbol != symbol:
            continue
        out.append(algo)
    return out


def leg_of_call(call: str) -> Leg | None:
    parsed = parse_client_id(call.split(":", 1)[1])
    return parsed.leg if parsed is not None else None


async def open_position(
    h: Harness,
    *,
    event_id: str = "evt-1",
    symbol: str = SOL,
    qty: str = "9",
    fill: str | None = "9",
    ttl: timedelta | None = None,
    mark_before_fill: str | None = None,
) -> list[OrderIntent]:
    """Send a signed LONG plan (entry 100.00 post-only, stop 98.00, TP1 104.00, IOC 100.10) on a symbol
    seeded by `seed_market` and let sellers hit the bid until `fill` of it executed (queue ahead 5)."""
    plan = open_plan(
        account=h.ns.account,
        event_id=event_id,
        symbol=symbol,
        side=Side.LONG,
        qty=Decimal(qty),
        entry=Decimal("100.00"),
        stop=Decimal("98.00"),
        tp1=Decimal("104.00"),
        ioc_price=Decimal("100.10"),
        now=h.clock(),
        ttl=ttl,
    )
    statuses = await h.send(plan)
    assert statuses == ["accepted"] * len(plan), statuses
    await h.pump()
    if mark_before_fill is not None:
        h.exchange.set_mark(symbol, mark_before_fill)
    if fill is not None:
        queue = h.exchange.venue.orders[plan[-1].client_id].queue_ahead
        h.exchange.trade(symbol, "100.00", str(queue + Decimal(fill)), buyer_maker=True)
        await h.pump()
    return plan
