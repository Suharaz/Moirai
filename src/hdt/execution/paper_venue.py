"""The simulated exchange behind namespace `paper` (phase 09 section 10). Pure state machine, no IO.

Fed with recorded mainnet market data (`hdt.execution.paper_feed`), it behaves like Binance USD-M One-way
for everything execution uses, with explicit fill rules:
- **post-only (GTX)**: rejected with -5022 when it would take; otherwise it rests with an estimated queue
  `queue_ahead` = displayed quantity at its price level when placed (100 ms depth-diff book). A price the
  book does not show (beyond the deepest level of the last snapshot) has an unknown queue (`None`): the
  first book that shows the level sets it, and a book that does not show it never lowers it. aggTrades at
  that price on our side (sells hitting a resting BUY, `m = true`; buys lifting a resting SELL) consume a
  known queue; the overflow fills us. A trade through the level, or the opposite best price reaching it,
  fills the rest. Trades at the price never fill us while estimated queue remains or is unknown. The
  queue never exceeds the displayed level;
- **IOC / MARKET / triggered stops**: walk the opposite side of the book level by level and add
  `slippage_bp` (2 bp) adverse on every level; an IOC takes a level only while its slipped price is within
  the limit, so no fill is ever beyond the signed limit. A MARKET quantity beyond the displayed book fills
  at the worse of the last walked level and the mark (both slipped), never better than a level already
  walked, flagged `degraded`. With no depth-diff book the 1 s depth20 snapshot is used and the fill is
  flagged `degraded`; without any book a MARKET fills at mark +/- slippage, also `degraded`;
- **conditional orders** (`STOP_MARKET`, `TAKE_PROFIT_MARKET`, working type MARK_PRICE) trigger on the mark
  price, -2021 when placed already triggered; the triggered MARKET is reduce-only (`closePosition` closes
  the whole position);
- **fees**: maker / taker rates of the pinned risk config on every fill (USDT); **funding** at each funding
  timestamp from the mark stream (`T` next funding time, `r` rate; the last rate published before the
  timestamp applies): payment = -position x mark x rate, booked as a `FUNDING_FEE` cash flow.

Positions are net per symbol; realized PnL = (exit - entry) x closed qty x direction; the wallet moves by
realized PnL - fees + funding, exactly the arithmetic the ledger reproduces.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Final

from hdt.contracts.common import OrderSide, OrderType, TimeInForce
from hdt.execution.adapter_base import (
    CODE_DUPLICATE_CLIENT_ID,
    CODE_GTX_WOULD_TAKE,
    CODE_REDUCE_ONLY_REJECTED,
    CODE_WOULD_TRIGGER_IMMEDIATELY,
    AlgoEvent,
    AlgoRequest,
    AlgoSnapshot,
    Balance,
    CashFlow,
    ExchangePosition,
    FillModel,
    OrderEvent,
    OrderRejectedError,
    OrderRequest,
    OrderSnapshot,
    TradeFill,
)

ZERO: Final[Decimal] = Decimal(0)
BP: Final[Decimal] = Decimal("0.0001")
MARGIN_ASSET: Final[str] = "USDT"
MAX_TRADES_KEPT: Final[int] = 1_000
MAX_FLOWS_KEPT: Final[int] = 500
MAX_FINAL_KEPT: Final[int] = 500
RESTING: Final[frozenset[str]] = frozenset({"NEW", "PARTIALLY_FILLED"})

VenueEvent = OrderEvent | AlgoEvent


def ms_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=UTC)


# ---------------------------------------------------------------------------------------------- book


@dataclass
class Book:
    """Order book of one symbol: bids/asks price -> qty. `diff_ok` = maintained from 100 ms diffs.

    `bid_edge` / `ask_edge` = the deepest price of each side the last snapshot showed (depth20: 20 levels).
    Between it and the best price every level is known (0 when absent); beyond it a level absent from the
    book is unknown. Diffs carry absolute quantities, so a level present in the book is always known.
    """

    bids: dict[Decimal, Decimal] = field(default_factory=dict)
    asks: dict[Decimal, Decimal] = field(default_factory=dict)
    last_u: int | None = None
    diff_ok: bool = False
    ts_ms: int = 0
    bid_edge: Decimal | None = None
    ask_edge: Decimal | None = None

    @property
    def model(self) -> FillModel:
        return "queue" if self.diff_ok else "degraded"

    def best_bid(self) -> Decimal | None:
        return max(self.bids) if self.bids else None

    def best_ask(self) -> Decimal | None:
        return min(self.asks) if self.asks else None

    def level(self, side: OrderSide, price: Decimal) -> Decimal | None:
        """Displayed quantity at `price` on the side a resting `side` order joins; None when the book does
        not cover `price` (beyond the deepest level the last snapshot showed, or that side is empty)."""
        buy = side is OrderSide.BUY
        shown = (self.bids if buy else self.asks).get(price)
        if shown is not None:
            return shown
        edge = self.bid_edge if buy else self.ask_edge
        if edge is None or (price < edge if buy else price > edge):
            return None
        return ZERO

    def apply_snapshot(self, bids: list[Any], asks: list[Any], u: int | None, ts_ms: int) -> None:
        self.bids = {Decimal(str(p)): Decimal(str(q)) for p, q in bids if Decimal(str(q)) > 0}
        self.asks = {Decimal(str(p)): Decimal(str(q)) for p, q in asks if Decimal(str(q)) > 0}
        self.bid_edge = min(self.bids, default=None)
        self.ask_edge = max(self.asks, default=None)
        self.last_u = u
        self.diff_ok = False
        self.ts_ms = ts_ms

    def apply_diff(
        self, first_u: int, final_u: int, prev_u: int, bids: list[Any], asks: list[Any], ts_ms: int
    ) -> bool:
        """Apply one diff event; False (and `diff_ok` cleared) when continuity is broken."""
        if self.last_u is None:
            return False
        if final_u <= self.last_u:
            return True  # already contained in the snapshot
        continuous = prev_u == self.last_u or (first_u <= self.last_u + 1 <= final_u)
        if not continuous:
            self.diff_ok = False
            return False
        for book, updates in ((self.bids, bids), (self.asks, asks)):
            for p, q in updates:
                price, qty = Decimal(str(p)), Decimal(str(q))
                if qty == 0:
                    book.pop(price, None)
                else:
                    book[price] = qty
        self.last_u = final_u
        self.diff_ok = True
        self.ts_ms = ts_ms
        return True

    def walk(self, taker_side: OrderSide, qty: Decimal) -> list[tuple[Decimal, Decimal]]:
        """Levels a taker of `taker_side` consumes for `qty`, best price first."""
        levels = (
            sorted(self.asks.items())
            if taker_side is OrderSide.BUY
            else sorted(self.bids.items(), reverse=True)
        )
        out: list[tuple[Decimal, Decimal]] = []
        left = qty
        for price, available in levels:
            if left <= 0:
                break
            take = min(left, available)
            out.append((price, take))
            left -= take
        return out


# ---------------------------------------------------------------------------------------------- state


@dataclass
class VOrder:
    client_id: str
    order_id: int
    symbol: str
    side: str
    type: str
    tif: str | None
    price: Decimal | None
    qty: Decimal
    executed: Decimal
    avg_price: Decimal | None
    reduce_only: bool
    status: str
    queue_ahead: Decimal | None  # resting post-only only; None = unknown (the book did not show the price)
    fill_model: str
    created_ms: int
    updated_ms: int

    def snapshot(self) -> OrderSnapshot:
        return OrderSnapshot(
            symbol=self.symbol,
            client_id=self.client_id,
            order_id=self.order_id,
            side=OrderSide(self.side),
            order_type=self.type,
            tif=self.tif,
            status=self.status,
            qty=self.qty,
            executed_qty=self.executed,
            avg_price=self.avg_price,
            price=self.price,
            reduce_only=self.reduce_only,
            updated_at=ms_dt(self.updated_ms),
        )

    @property
    def remaining(self) -> Decimal:
        return self.qty - self.executed


@dataclass
class VAlgo:
    client_algo_id: str
    algo_id: int
    symbol: str
    side: str
    type: str
    trigger: Decimal
    qty: Decimal | None
    close_position: bool
    reduce_only: bool
    status: str
    triggered_order_id: int | None
    created_ms: int
    updated_ms: int

    def snapshot(self) -> AlgoSnapshot:
        return AlgoSnapshot(
            symbol=self.symbol,
            client_algo_id=self.client_algo_id,
            algo_id=self.algo_id,
            side=OrderSide(self.side),
            order_type=self.type,
            status=self.status,
            trigger_price=self.trigger,
            qty=self.qty,
            close_position=self.close_position,
            reduce_only=self.reduce_only,
            triggered_order_id=self.triggered_order_id,
            updated_at=ms_dt(self.updated_ms),
        )


@dataclass
class VPosition:
    symbol: str
    qty: Decimal  # signed
    entry_price: Decimal


@dataclass
class Funding:
    rate: Decimal | None = None
    next_ms: int | None = None


_DEC_FIELDS: Final[dict[str, tuple[str, ...]]] = {
    "order": ("price", "qty", "executed", "avg_price", "queue_ahead"),
    "algo": ("trigger", "qty"),
    "position": ("qty", "entry_price"),
}


def _dec_in(values: dict[str, Any], names: tuple[str, ...]) -> dict[str, Any]:
    out = dict(values)
    for name in names:
        if out.get(name) is not None:
            out[name] = Decimal(str(out[name]))
    return out


def _dec_out(values: dict[str, Any]) -> dict[str, Any]:
    return {k: (str(v) if isinstance(v, Decimal) else v) for k, v in values.items()}


class PaperVenue:
    def __init__(
        self, *, maker_fee: Decimal, taker_fee: Decimal, slippage_bp: Decimal, wallet: Decimal
    ) -> None:
        self.maker_fee = maker_fee
        self.taker_fee = taker_fee
        self.slippage_bp = slippage_bp
        self.wallet = wallet
        self.positions: dict[str, VPosition] = {}
        self.orders: dict[str, VOrder] = {}
        self.algos: dict[str, VAlgo] = {}
        self.trades: list[TradeFill] = []
        self.flows: list[CashFlow] = []
        self.leverage: dict[str, int] = {}
        self.next_id = 1
        self.next_trade: dict[str, int] = {}
        self.funding: dict[str, Funding] = {}
        self.marks: dict[str, tuple[Decimal, int]] = {}
        self.books: dict[str, Book] = {}  # market data, never persisted

    # ------------------------------------------------------------------ persistence
    def to_dict(self) -> dict[str, Any]:
        return {
            "wallet": str(self.wallet),
            "positions": {s: _dec_out(asdict(p)) for s, p in self.positions.items()},
            "orders": {c: _dec_out(asdict(o)) for c, o in self.orders.items() if o.status in RESTING}
            | {c: _dec_out(asdict(o)) for c, o in list(self.orders.items())[-MAX_FINAL_KEPT:]},
            "algos": {c: _dec_out(asdict(a)) for c, a in self.algos.items() if a.status == "NEW"}
            | {c: _dec_out(asdict(a)) for c, a in list(self.algos.items())[-MAX_FINAL_KEPT:]},
            "trades": [_trade_out(t) for t in self.trades[-MAX_TRADES_KEPT:]],
            "flows": [_flow_out(f) for f in self.flows[-MAX_FLOWS_KEPT:]],
            "leverage": self.leverage,
            "next_id": self.next_id,
            "next_trade": self.next_trade,
            "funding": {
                s: {"rate": str(f.rate) if f.rate is not None else None, "next_ms": f.next_ms}
                for s, f in self.funding.items()
            },
            "marks": {s: [str(m), ts] for s, (m, ts) in self.marks.items()},
        }

    def load(self, data: dict[str, Any]) -> None:
        self.wallet = Decimal(str(data["wallet"]))
        self.positions = {
            s: VPosition(**_dec_in(p, _DEC_FIELDS["position"])) for s, p in data["positions"].items()
        }
        self.orders = {c: VOrder(**_dec_in(o, _DEC_FIELDS["order"])) for c, o in data["orders"].items()}
        self.algos = {c: VAlgo(**_dec_in(a, _DEC_FIELDS["algo"])) for c, a in data["algos"].items()}
        self.trades = [_trade_in(t) for t in data.get("trades", [])]
        self.flows = [_flow_in(f) for f in data.get("flows", [])]
        self.leverage = {s: int(v) for s, v in data.get("leverage", {}).items()}
        self.next_id = int(data.get("next_id", 1))
        self.next_trade = {s: int(v) for s, v in data.get("next_trade", {}).items()}
        self.funding = {
            s: Funding(Decimal(f["rate"]) if f.get("rate") is not None else None, f.get("next_ms"))
            for s, f in data.get("funding", {}).items()
        }
        self.marks = {s: (Decimal(m), int(ts)) for s, (m, ts) in data.get("marks", {}).items()}

    # ------------------------------------------------------------------ helpers
    def _id(self) -> int:
        self.next_id += 1
        return self.next_id

    def book(self, symbol: str) -> Book | None:
        book = self.books.get(symbol)
        return book if book is not None and (book.bids or book.asks) else None

    def mark(self, symbol: str) -> Decimal | None:
        found = self.marks.get(symbol)
        return found[0] if found is not None else None

    def _slip(self, side: OrderSide, price: Decimal) -> Decimal:
        adj = self.slippage_bp * BP
        return price * (1 + adj) if side is OrderSide.BUY else price * (1 - adj)

    def position_qty(self, symbol: str) -> Decimal:
        pos = self.positions.get(symbol)
        return pos.qty if pos is not None else ZERO

    def _reduce_cap(self, symbol: str, side: OrderSide) -> Decimal:
        """Quantity a reduce-only `side` order may trade (0 = nothing to reduce)."""
        qty = self.position_qty(symbol)
        if qty > 0 and side is OrderSide.SELL:
            return qty
        if qty < 0 and side is OrderSide.BUY:
            return -qty
        return ZERO

    # ------------------------------------------------------------------ fills
    def _apply_position(self, symbol: str, side: OrderSide, price: Decimal, qty: Decimal) -> Decimal:
        delta = qty if side is OrderSide.BUY else -qty
        pos = self.positions.get(symbol)
        if pos is None or pos.qty == 0:
            self.positions[symbol] = VPosition(symbol, delta, price)
            return ZERO
        if (pos.qty > 0) == (delta > 0):
            total = pos.qty + delta
            pos.entry_price = (pos.entry_price * abs(pos.qty) + price * qty) / abs(total)
            pos.qty = total
            return ZERO
        closing = min(abs(delta), abs(pos.qty))
        direction = Decimal(1) if pos.qty > 0 else Decimal(-1)
        realized = (price - pos.entry_price) * closing * direction
        remaining = pos.qty + delta
        if remaining == 0:
            del self.positions[symbol]
        elif (remaining > 0) == (pos.qty > 0):
            pos.qty = remaining
        else:
            self.positions[symbol] = VPosition(symbol, remaining, price)
        return realized

    def _fill(
        self, order: VOrder, price: Decimal, qty: Decimal, *, maker: bool, now_ms: int, model: str
    ) -> VenueEvent:
        side = OrderSide(order.side)
        realized = self._apply_position(order.symbol, side, price, qty)
        fee = price * qty * (self.maker_fee if maker else self.taker_fee)
        self.wallet += realized - fee
        notional_before = (order.avg_price or ZERO) * order.executed
        order.executed += qty
        order.avg_price = (notional_before + price * qty) / order.executed
        order.status = "FILLED" if order.executed >= order.qty else "PARTIALLY_FILLED"
        order.updated_ms = now_ms
        if model == "degraded":
            order.fill_model = "degraded"
        trade_id = self.next_trade.get(order.symbol, 0) + 1
        self.next_trade[order.symbol] = trade_id
        fill = TradeFill(
            symbol=order.symbol,
            trade_id=trade_id,
            order_id=order.order_id,
            client_id=order.client_id,
            side=side,
            price=price,
            qty=qty,
            fee=fee,
            fee_asset=MARGIN_ASSET,
            maker=maker,
            realized_pnl=realized,
            time=ms_dt(now_ms),
        )
        self.trades.append(fill)
        return OrderEvent(order.snapshot(), fill, "queue" if model == "queue" else "degraded")

    def _take(self, order: VOrder, limit: Decimal | None, now_ms: int) -> list[VenueEvent]:
        """Taker execution: walk the book with slippage on every level. A `limit` (IOC) is checked on the
        slipped price, so no fill is beyond it; a MARKET remainder the book cannot fill goes to
        `_beyond_book`. Whatever stays unfilled expires."""
        side = OrderSide(order.side)
        book = self.book(order.symbol)
        events: list[VenueEvent] = []
        worst: Decimal | None = None  # slipped price of the last level taken
        if book is not None:
            for level, qty in book.walk(side, order.remaining):
                price = self._slip(side, level)
                if limit is not None and (price > limit if side is OrderSide.BUY else price < limit):
                    break  # after slippage this level (and every deeper one) is beyond the limit
                events.append(self._fill(order, price, qty, maker=False, now_ms=now_ms, model=book.model))
                worst = price
        if order.remaining > 0 and order.type == OrderType.MARKET.value:
            beyond = self._beyond_book(side, order.symbol, worst)
            if beyond is not None:
                events.append(
                    self._fill(order, beyond, order.remaining, maker=False, now_ms=now_ms, model="degraded")
                )
        if order.remaining > 0:
            order.status = "EXPIRED"
            order.updated_ms = now_ms
            events.append(OrderEvent(order.snapshot()))
        return events

    def _beyond_book(self, side: OrderSide, symbol: str, worst: Decimal | None) -> Decimal | None:
        """Fill price of a MARKET remainder the displayed book cannot fill: the worse of the last walked
        level and the mark, both slipped, so never better than a level already walked (None: neither)."""
        mark = self.mark(symbol)
        prices = [p for p in (worst, None if mark is None else self._slip(side, mark)) if p is not None]
        if not prices:
            return None
        return max(prices) if side is OrderSide.BUY else min(prices)

    # ------------------------------------------------------------------ regular orders
    def place_order(self, req: OrderRequest, now_ms: int) -> tuple[OrderSnapshot, list[VenueEvent]]:
        if req.client_id in self.orders:
            raise OrderRejectedError("duplicate clientOrderId", code=CODE_DUPLICATE_CLIENT_ID)
        qty = req.qty
        if req.reduce_only:
            cap = self._reduce_cap(req.symbol, req.side)
            if cap <= 0:
                raise OrderRejectedError("ReduceOnly Order is rejected", code=CODE_REDUCE_ONLY_REJECTED)
            qty = min(qty, cap)
        book = self.book(req.symbol)
        order = VOrder(
            client_id=req.client_id,
            order_id=self._id(),
            symbol=req.symbol,
            side=req.side.value,
            type=req.order_type.value,
            tif=req.tif.value if req.tif else None,
            price=req.price,
            qty=qty,
            executed=ZERO,
            avg_price=None,
            reduce_only=req.reduce_only,
            status="NEW",
            queue_ahead=None,
            fill_model=book.model if book is not None else "degraded",
            created_ms=now_ms,
            updated_ms=now_ms,
        )
        if req.order_type is OrderType.LIMIT and req.tif is TimeInForce.GTX:
            assert req.price is not None
            if book is not None:
                best = book.best_ask() if req.side is OrderSide.BUY else book.best_bid()
                if best is not None and (
                    req.price >= best if req.side is OrderSide.BUY else req.price <= best
                ):
                    raise OrderRejectedError("Post Only order will be rejected", code=CODE_GTX_WOULD_TAKE)
                order.queue_ahead = book.level(req.side, req.price)
            self.orders[order.client_id] = order
            return order.snapshot(), [OrderEvent(order.snapshot())]
        self.orders[order.client_id] = order
        limit = req.price if req.order_type is OrderType.LIMIT else None
        events = self._take(order, limit, now_ms)
        return order.snapshot(), events

    def cancel_order(self, client_id: str, now_ms: int) -> tuple[OrderSnapshot | None, list[VenueEvent]]:
        order = self.orders.get(client_id)
        if order is None:
            return None, []
        if order.status not in RESTING:
            raise OrderRejectedError("Unknown order sent.", code=-2011)
        order.status = "CANCELED"
        order.updated_ms = now_ms
        return order.snapshot(), [OrderEvent(order.snapshot())]

    # ------------------------------------------------------------------ conditional orders
    def _triggered(self, algo: VAlgo, mark: Decimal) -> bool:
        sell = algo.side == OrderSide.SELL.value
        if algo.type == OrderType.STOP_MARKET.value:
            return mark <= algo.trigger if sell else mark >= algo.trigger
        return mark >= algo.trigger if sell else mark <= algo.trigger

    def place_algo(self, req: AlgoRequest, now_ms: int) -> tuple[AlgoSnapshot, list[VenueEvent]]:
        if req.client_algo_id in self.algos:
            raise OrderRejectedError("duplicate clientAlgoId", code=CODE_DUPLICATE_CLIENT_ID)
        algo = VAlgo(
            client_algo_id=req.client_algo_id,
            algo_id=self._id(),
            symbol=req.symbol,
            side=req.side.value,
            type=req.order_type.value,
            trigger=req.trigger_price,
            qty=req.qty,
            close_position=req.close_position,
            reduce_only=req.reduce_only,
            status="NEW",
            triggered_order_id=None,
            created_ms=now_ms,
            updated_ms=now_ms,
        )
        mark = self.mark(req.symbol)
        if mark is not None and self._triggered(algo, mark):
            raise OrderRejectedError("Order would immediately trigger.", code=CODE_WOULD_TRIGGER_IMMEDIATELY)
        self.algos[algo.client_algo_id] = algo
        return algo.snapshot(), [AlgoEvent(algo.snapshot())]

    def cancel_algo(self, client_algo_id: str, now_ms: int) -> tuple[AlgoSnapshot | None, list[VenueEvent]]:
        algo = self.algos.get(client_algo_id)
        if algo is None:
            return None, []
        if algo.status != "NEW":
            return algo.snapshot(), []
        algo.status = "CANCELED"
        algo.updated_ms = now_ms
        return algo.snapshot(), [AlgoEvent(algo.snapshot())]

    def cancel_all_algos(self, symbol: str, now_ms: int) -> list[VenueEvent]:
        events: list[VenueEvent] = []
        for algo in self.algos.values():
            if algo.symbol == symbol and algo.status == "NEW":
                algo.status = "CANCELED"
                algo.updated_ms = now_ms
                events.append(AlgoEvent(algo.snapshot()))
        return events

    def _execute_trigger(self, algo: VAlgo, now_ms: int) -> list[VenueEvent]:
        algo.status = "TRIGGERED"
        algo.updated_ms = now_ms
        events: list[VenueEvent] = []
        side = OrderSide(algo.side)
        cap = self._reduce_cap(algo.symbol, side)
        qty = cap if algo.close_position else min(algo.qty or ZERO, cap)
        if qty <= 0:
            algo.status = "EXPIRED"
            events.append(AlgoEvent(algo.snapshot()))
            return events
        order = VOrder(
            client_id=algo.client_algo_id,
            order_id=self._id(),
            symbol=algo.symbol,
            side=algo.side,
            type=OrderType.MARKET.value,
            tif=None,
            price=None,
            qty=qty,
            executed=ZERO,
            avg_price=None,
            reduce_only=True,
            status="NEW",
            queue_ahead=None,
            fill_model="queue",
            created_ms=now_ms,
            updated_ms=now_ms,
        )
        algo.triggered_order_id = order.order_id
        events.append(AlgoEvent(algo.snapshot()))
        self.orders[f"{algo.client_algo_id}#{order.order_id}"] = order
        events.extend(self._take(order, None, now_ms))
        algo.status = "FINISHED"
        algo.updated_ms = now_ms
        events.append(AlgoEvent(algo.snapshot()))
        return events

    # ------------------------------------------------------------------ market data
    def on_book(self, symbol: str, now_ms: int) -> list[VenueEvent]:
        """The book of `symbol` changed: resting orders the opposite side now reaches are filled; the queue
        estimate of the others is capped by the displayed level (set by it when it was unknown)."""
        book = self.book(symbol)
        if book is None:
            return []
        events: list[VenueEvent] = []
        for order in self._resting(symbol):
            assert order.price is not None
            side = OrderSide(order.side)
            opposite = book.best_ask() if side is OrderSide.BUY else book.best_bid()
            crossed = opposite is not None and (
                opposite <= order.price if side is OrderSide.BUY else opposite >= order.price
            )
            if crossed:
                events.append(
                    self._fill(
                        order, order.price, order.remaining, maker=True, now_ms=now_ms, model=book.model
                    )
                )
                continue
            shown = book.level(side, order.price)
            if shown is None:
                continue  # the book does not cover our price: it says nothing about the queue
            order.queue_ahead = shown if order.queue_ahead is None else min(order.queue_ahead, shown)
        return events

    def on_trade(
        self, symbol: str, price: Decimal, qty: Decimal, buyer_maker: bool, ts_ms: int
    ) -> list[VenueEvent]:
        """An aggTrade: consumes the queue ahead of resting orders at its price, fills through-trades."""
        events: list[VenueEvent] = []
        book = self.book(symbol)
        model = book.model if book is not None else "degraded"
        for order in self._resting(symbol):
            if order.created_ms >= ts_ms:
                continue  # the trade happened before the order existed
            assert order.price is not None
            buy = order.side == OrderSide.BUY.value
            through = price < order.price if buy else price > order.price
            if through:
                events.append(
                    self._fill(order, order.price, order.remaining, maker=True, now_ms=ts_ms, model=model)
                )
                continue
            hits_us = price == order.price and (buyer_maker if buy else not buyer_maker)
            if not hits_us or order.queue_ahead is None:
                continue  # an unknown queue is never assumed consumed
            order.queue_ahead -= qty
            if order.queue_ahead < 0:
                take = min(order.remaining, -order.queue_ahead)
                order.queue_ahead = ZERO
                events.append(self._fill(order, order.price, take, maker=True, now_ms=ts_ms, model=model))
        return events

    def on_mark(
        self,
        symbol: str,
        mark: Decimal,
        ts_ms: int,
        rate: Decimal | None = None,
        next_funding_ms: int | None = None,
    ) -> list[VenueEvent]:
        """Mark update: funding settlement when a funding timestamp passed, then conditional triggers."""
        self.marks[symbol] = (mark, ts_ms)
        funding = self.funding.setdefault(symbol, Funding())
        if funding.next_ms is not None and ts_ms >= funding.next_ms and funding.rate is not None:
            self._settle_funding(symbol, mark, funding.rate, funding.next_ms)
            funding.next_ms = None
        if rate is not None:
            funding.rate = rate
        later = funding.next_ms is None or (next_funding_ms is not None and next_funding_ms > funding.next_ms)
        if next_funding_ms is not None and later and next_funding_ms > ts_ms:
            funding.next_ms = next_funding_ms
        events: list[VenueEvent] = []
        for algo in list(self.algos.values()):
            if algo.symbol == symbol and algo.status == "NEW" and self._triggered(algo, mark):
                events.extend(self._execute_trigger(algo, ts_ms))
        return events

    def _settle_funding(self, symbol: str, mark: Decimal, rate: Decimal, funding_ms: int) -> None:
        qty = self.position_qty(symbol)
        if qty == 0:
            return
        payment = -qty * mark * rate
        self.wallet += payment
        self.flows.append(
            CashFlow(
                flow_id=f"paper-funding-{symbol}-{funding_ms}",
                kind="FUNDING_FEE",
                symbol=symbol,
                amount=payment,
                asset=MARGIN_ASSET,
                time=ms_dt(funding_ms),
            )
        )

    def _resting(self, symbol: str) -> list[VOrder]:
        return [
            o
            for o in self.orders.values()
            if o.symbol == symbol
            and o.status in RESTING
            and o.type == OrderType.LIMIT.value
            and o.remaining > 0
        ]

    # ------------------------------------------------------------------ account views
    def exchange_positions(self) -> list[ExchangePosition]:
        out: list[ExchangePosition] = []
        for p in self.positions.values():
            if p.qty == 0:
                continue
            mark = self.mark(p.symbol) or p.entry_price
            out.append(
                ExchangePosition(
                    symbol=p.symbol,
                    qty=p.qty,
                    entry_price=p.entry_price,
                    mark_price=mark,
                    unrealized_pnl=p.qty * (mark - p.entry_price),
                    leverage=self.leverage.get(p.symbol, 1),
                    liquidation_price=None,
                    margin_type="ISOLATED",
                )
            )
        return out

    def balance(self) -> Balance:
        unrealized = ZERO
        margin = ZERO
        for p in self.positions.values():
            mark = self.mark(p.symbol) or p.entry_price
            unrealized += p.qty * (mark - p.entry_price)
            margin += abs(p.qty) * p.entry_price / Decimal(self.leverage.get(p.symbol, 1))
        for o in self.orders.values():
            if o.status in RESTING and not o.reduce_only and o.price is not None:
                margin += o.remaining * o.price / Decimal(self.leverage.get(o.symbol, 1))
        equity = self.wallet + unrealized
        return Balance(
            wallet_balance=self.wallet, unrealized_pnl=unrealized, equity=equity, available=equity - margin
        )

    def watched_symbols(self) -> set[str]:
        return (
            {p.symbol for p in self.positions.values() if p.qty != 0}
            | {o.symbol for o in self.orders.values() if o.status in RESTING}
            | {a.symbol for a in self.algos.values() if a.status == "NEW"}
        )


def _trade_out(t: TradeFill) -> dict[str, Any]:
    return {
        "symbol": t.symbol,
        "trade_id": t.trade_id,
        "order_id": t.order_id,
        "client_id": t.client_id,
        "side": t.side.value,
        "price": str(t.price),
        "qty": str(t.qty),
        "fee": str(t.fee),
        "fee_asset": t.fee_asset,
        "maker": t.maker,
        "realized_pnl": str(t.realized_pnl),
        "time": t.time.isoformat(),
    }


def _trade_in(d: dict[str, Any]) -> TradeFill:
    return TradeFill(
        symbol=d["symbol"],
        trade_id=int(d["trade_id"]),
        order_id=d.get("order_id"),
        client_id=d["client_id"],
        side=OrderSide(d["side"]),
        price=Decimal(d["price"]),
        qty=Decimal(d["qty"]),
        fee=Decimal(d["fee"]),
        fee_asset=d["fee_asset"],
        maker=d.get("maker"),
        realized_pnl=Decimal(d["realized_pnl"]),
        time=datetime.fromisoformat(d["time"]),
    )


def _flow_out(f: CashFlow) -> dict[str, Any]:
    return {
        "flow_id": f.flow_id,
        "kind": f.kind,
        "symbol": f.symbol,
        "amount": str(f.amount),
        "asset": f.asset,
        "time": f.time.isoformat(),
    }


def _flow_in(d: dict[str, Any]) -> CashFlow:
    return CashFlow(
        flow_id=d["flow_id"],
        kind=d["kind"],
        symbol=d.get("symbol"),
        amount=Decimal(d["amount"]),
        asset=d["asset"],
        time=datetime.fromisoformat(d["time"]),
    )
