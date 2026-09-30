"""Risk decision consumer (phase 09 section 1): stream `decisions`, one consumer group per decision namespace
(`risk-paper`, `risk-testnet`, `risk-live`; the run mode's decision targets, `decision_accounts`). The groups
are created at `$`: the first attach of a namespace starts at the stream tail and never replays decisions
published before it existed.
A decision has no time limit (owner decision 2026-09-28): a re-attach after a detach handles what was
published meanwhile, and the gate refuses an OPEN whose levels the current mark has invalidated.

Per message, in ONE Postgres transaction:
1. parse `DecisionMsg`; a malformed message or an unknown `schema_version` is dead-lettered with an
   `integrity_error` alert (one per message: episode = event id, else stream id) and acked;
2. `processed_decisions(account, event_id)` insert: a duplicate (redelivery, restart) writes nothing;
3. load the packet by `packet_sha256` (its recomputed sha256 must equal the requested one), its candidate
   set, and the namespace book (AccountState, ledger positions, pending entries, the BTC hedge book, kill
   state), then run the pure `gate.evaluate` against point-in-time market data from the lake;
4. write `risk_verdicts` and every signed `OrderIntent` leg into the `risk_intents` outbox.
After the commit `IntentOutbox.relay_once` XADDs unpublished intents to `orders` in (created_at,
publish_order) order and marks them published; only then the message is acked. A crash between commit and
XADD leaves rows the next relay publishes, and execution deduplicates by `intent_id`, so a restart never
loses or duplicates an order.

`VetoExits` is the other writer of this module: a held coin whose unexpired `RiskFlags` veto the position
side gets a signed EXIT (event id `veto:{account}:{coin_id}:{as_of}`), deduplicated and recorded like a
decision.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Final, Protocol, cast

import sqlalchemy as sa
from prometheus_client import Counter
from pydantic import ValidationError
from redis.asyncio import Redis
from sqlalchemy.orm import Session, sessionmaker

from hdt.contracts.account import AccountState
from hdt.contracts.candidate import CandidateSet
from hdt.contracts.common import Account, Intent, Leg, OrderSide, OrderType, Side
from hdt.contracts.decision import DecisionMsg
from hdt.contracts.order import OrderIntent
from hdt.contracts.packet import QuantPacket
from hdt.contracts.risk_flags import RiskFlags
from hdt.contracts.streams import Stream
from hdt.core.alerts import alert
from hdt.core.clock import utcnow
from hdt.core.config import RiskFile
from hdt.core.streams import RawPayload, StreamConsumer, dead_letter, publish
from hdt.db.dedupe import insert_once
from hdt.db.models.ledger import PositionRow, ProcessedIntentRow
from hdt.db.models.risk import HedgeBookRow, ProcessedDecisionRow, RiskIntentRow, RiskVerdictRow
from hdt.db.session import transaction
from hdt.execution.client_ids import client_id
from hdt.execution.filters import SymbolFilters, parse_exchange_filters
from hdt.execution.namespaces import DECISION_ACCOUNTS, decision_accounts, risk_group
from hdt.lake.pit_query import PitQuery
from hdt.lake.schemas import RawRecord
from hdt.lake.universe import Universe, UniverseMember, load_universe
from hdt.quant.packet import load_candidate_set, load_packet
from hdt.risk.gate import (
    BookInputs,
    GateConfig,
    GateInputs,
    GateResult,
    HeldPosition,
    MarketInputs,
    evaluate,
    intent_id_for,
)
from hdt.risk.hedge_book import BTC_SYMBOL, book_risk
from hdt.risk.kill_state import read_kill_state
from hdt.risk.limits import Exposure
from hdt.risk.signer import IntentSigner
from hdt.risk.veto import FlagsBook, exit_required

log = logging.getLogger(__name__)

SERVICE: Final[str] = "risk"
SOURCE: Final[str] = "binance"
EXCHANGE_INFO_LOOKBACK: Final[timedelta] = timedelta(days=2)
UNIVERSE_REFRESH_S: Final[float] = 60.0
EXCHANGE_INFO_REFRESH_S: Final[float] = 60.0
PENDING_LOOKBACK: Final[timedelta] = timedelta(days=1)
TERMINAL_INTENT_STATUSES: Final[tuple[str, ...]] = ("done", "void", "rejected")
VETO_EXIT_REASON: Final[str] = "veto_exit"

VERDICTS = Counter("hdt_risk_verdicts_total", "Risk verdicts written", ["account", "verdict"])
INTENTS_PUBLISHED = Counter(
    "hdt_risk_intents_published_total", "Signed intents XADDed to orders", ["account"]
)


# ============================================================================ runtime inputs


@dataclass(frozen=True)
class RiskRuntime:
    """The pinned config one decision runs with (read at the event boundary)."""

    risk: RiskFile
    stale_s: float
    config_version_ids: Mapping[str, int]
    mode: Account  # the pinned run mode

    def opens(self, account: Account) -> bool:
        """True while the run mode enables `account` (a namespace kept only to wind down opens nothing)."""
        return account in decision_accounts(self.mode)


class MarketSource(Protocol):
    """Point-in-time market inputs for one coin (sync: called through `asyncio.to_thread`)."""

    def market(self, coin_id: int, now: datetime) -> MarketInputs: ...


class AccountStateBook:
    """Newest `AccountState` per namespace (an older `ts` never replaces a newer one).

    Also tracks the namespaces that may hold an open `account_state_stale` alert: every namespace at start
    (an alert may survive a restart) and each one the gate alerted since. The next fresh state resolves it.
    """

    def __init__(self) -> None:
        self._states: dict[Account, AccountState] = {}
        self._stale_alerted: set[Account] = set(DECISION_ACCOUNTS)
        self._lock = threading.Lock()  # the gate marks from worker threads

    def offer(self, state: AccountState) -> bool:
        held = self._states.get(state.account)
        if held is not None and held.ts >= state.ts:
            return False
        self._states[state.account] = state
        return True

    def get(self, account: Account) -> AccountState | None:
        return self._states.get(account)

    def mark_stale_alerted(self, account: Account) -> None:
        with self._lock:
            self._stale_alerted.add(account)

    def stale_alerted(self, account: Account) -> bool:
        with self._lock:
            return account in self._stale_alerted

    def clear_stale_alerted(self, account: Account) -> None:
        with self._lock:
            self._stale_alerted.discard(account)


def _alert_episode(kind: str, event_id: str) -> str | None:
    """Per-decision episode for `integrity_error`; `account_state_stale` stays one alert per namespace,
    resolved by the next fresh AccountState (`RiskService.on_state`)."""
    return event_id if kind == "integrity_error" else None


# ============================================================================ lake market data


def _ms_age(now: datetime, ts_ms: int | None, fallback: datetime) -> float:
    if ts_ms is None:
        return max(0.0, (now - fallback).total_seconds())
    return max(0.0, now.timestamp() - ts_ms / 1000)


def _int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _ws_events(record: RawRecord) -> list[tuple[str, Mapping[str, Any]]]:
    """(stream, event) pairs of one recorded WebSocket flush (`{"recv_ts", "stream", "data"}` items)."""
    try:
        body = json.loads(record.body())
    except ValueError:
        return []
    out: list[tuple[str, Mapping[str, Any]]] = []
    for item in body if isinstance(body, list) else []:
        if not isinstance(item, Mapping):
            continue
        stream = str(item.get("stream", ""))
        data = item.get("data")
        for event in data if isinstance(data, list) else [data]:
            if isinstance(event, Mapping):
                out.append((stream, event))
    return out


def _rest_items(record: RawRecord) -> list[Mapping[str, Any]]:
    if record.http_status != 200:
        return []
    try:
        body = json.loads(record.body())
    except ValueError:
        return []
    items = body if isinstance(body, list) else [body]
    return [i for i in items if isinstance(i, Mapping)]


@dataclass(frozen=True)
class Mark:
    price: Decimal
    age_s: float


class LakeMarket:
    """Market inputs from the recorded lake (Risk never calls a signed endpoint).

    - symbol and BTC flag: the point-in-time universe (`derived/universe`);
    - filters: the newest recorded `exchangeInfo` (parsed once per record);
    - mark and its age: `ws_markprice` events (`E`), REST `premium_index` as fallback;
    - order book age: `ws_depth20` snapshots (`E`), REST `depth` (key = symbol) as fallback.
    Data older than `window_s` counts as missing (the gate then rejects `stale_data`).
    """

    def __init__(self, pit: PitQuery, *, btc_cmc_symbol: str, window_s: float) -> None:
        self.pit = pit
        self.btc_cmc_symbol = btc_cmc_symbol
        self.window = timedelta(seconds=window_s)
        self._lock = threading.Lock()
        self._universe: Universe | None = None
        self._universe_at = float("-inf")
        self._filters: dict[str, SymbolFilters] = {}
        self._filters_hash: str | None = None
        self._filters_at = float("-inf")

    # ------------------------------------------------------------------ universe
    def universe(self, now: datetime) -> Universe | None:
        with self._lock:
            if time.monotonic() - self._universe_at >= UNIVERSE_REFRESH_S:
                self._universe = load_universe(self.pit, now)
                self._universe_at = time.monotonic()
            return self._universe

    def member(self, coin_id: int, now: datetime) -> UniverseMember | None:
        universe = self.universe(now)
        if universe is None:
            return None
        return next((m for m in universe.members if m.cmc_id == coin_id), None)

    def member_by_symbol(self, symbol: str, now: datetime) -> UniverseMember | None:
        universe = self.universe(now)
        if universe is None:
            return None
        return next((m for m in universe.members if m.binance_symbol == symbol), None)

    # ------------------------------------------------------------------ exchangeInfo
    def filters(self, symbol: str, now: datetime) -> SymbolFilters | None:
        with self._lock:
            if time.monotonic() - self._filters_at >= EXCHANGE_INFO_REFRESH_S:
                record = self.pit.latest(SOURCE, "exchange_info", now, lookback=EXCHANGE_INFO_LOOKBACK)
                if record is not None and record.record_hash != self._filters_hash:
                    try:
                        self._filters = parse_exchange_filters(json.loads(record.body()))
                        self._filters_hash = record.record_hash
                    except ValueError:
                        log.warning("recorded exchangeInfo is not JSON", extra={"record": record.record_hash})
                self._filters_at = time.monotonic()
            return self._filters.get(symbol)

    # ------------------------------------------------------------------ marks and book
    def marks(self, symbols: Iterable[str], now: datetime) -> dict[str, Mark]:
        wanted = set(symbols)
        found: dict[str, tuple[int, Mark]] = {}
        for record in reversed(self.pit.series(SOURCE, "ws_markprice", now - self.window, now, as_of=now)):
            for _stream, event in _ws_events(record):
                symbol = event.get("s")
                if symbol not in wanted or event.get("p") in (None, ""):
                    continue
                ts = _int(event.get("E"))
                price = Decimal(str(event["p"]))
                held = found.get(str(symbol))
                if price > 0 and (held is None or (ts or 0) > held[0]):
                    found[str(symbol)] = (ts or 0, Mark(price, _ms_age(now, ts, record.fetched_at)))
            if set(found) >= wanted:
                break
        missing = wanted - set(found)
        if missing:
            rest = self.pit.latest(SOURCE, "premium_index", now, lookback=self.window)
            fetched_at = rest.fetched_at if rest is not None else now
            for item in _rest_items(rest) if rest is not None else []:
                symbol = item.get("symbol")
                if symbol in missing and item.get("markPrice") not in (None, ""):
                    price = Decimal(str(item["markPrice"]))
                    if price > 0:
                        ts = _int(item.get("time"))
                        found[str(symbol)] = (ts or 0, Mark(price, _ms_age(now, ts, fetched_at)))
        return {symbol: mark for symbol, (_ts, mark) in found.items()}

    def book_age_s(self, symbol: str, now: datetime) -> float | None:
        prefix = f"{symbol.lower()}@depth20"
        for record in reversed(self.pit.series(SOURCE, "ws_depth20", now - self.window, now, as_of=now)):
            best: int | None = None
            seen = False
            for stream, event in _ws_events(record):
                if stream.startswith(prefix) or event.get("s") == symbol:
                    seen = True
                    ts = _int(event.get("E"))
                    if ts is not None and (best is None or ts > best):
                        best = ts
            if seen:
                return _ms_age(now, best, record.fetched_at)
        snapshot = self.pit.latest(SOURCE, "depth", now, key=symbol, lookback=self.window)
        if snapshot is not None and _rest_items(snapshot):
            return max(0.0, (now - snapshot.fetched_at).total_seconds())
        return None

    # ------------------------------------------------------------------ MarketSource
    def market(self, coin_id: int, now: datetime) -> MarketInputs:
        member = self.member(coin_id, now)
        if member is None:
            return MarketInputs(
                symbol=None, is_btc=False, filters=None, mark=None, mark_age_s=None, book_age_s=None
            )
        symbol = member.binance_symbol
        is_btc = member.cmc_symbol == self.btc_cmc_symbol or symbol == BTC_SYMBOL
        marks = self.marks({symbol, BTC_SYMBOL}, now)
        mark = marks.get(symbol)
        btc = marks.get(BTC_SYMBOL)
        return MarketInputs(
            symbol=symbol,
            is_btc=is_btc,
            filters=self.filters(symbol, now),
            mark=mark.price if mark is not None else None,
            mark_age_s=mark.age_s if mark is not None else None,
            book_age_s=self.book_age_s(symbol, now),
            btc_mark=btc.price if btc is not None else None,
        )


# ============================================================================ the namespace book


def _table(model: Any) -> sa.Table:
    return cast(sa.Table, model.__table__)


def open_positions(session: Session, account: Account) -> list[PositionRow]:
    return list(
        session.scalars(
            sa.select(PositionRow).where(
                PositionRow.account == account.value, PositionRow.closed_at.is_(None)
            )
        ).all()
    )


def _is_book(symbol: str, is_hedge_book: bool) -> bool:
    return is_hedge_book or symbol == BTC_SYMBOL


def held_positions(rows: Sequence[PositionRow], state: AccountState | None) -> dict[str, HeldPosition]:
    """Council positions by symbol: the ledger, overridden by the namespace's latest `AccountState`."""
    held: dict[str, HeldPosition] = {}
    for row in rows:
        if not _is_book(row.symbol, row.is_hedge_book) and row.qty > 0:
            held[row.symbol] = HeldPosition(Side(row.side), row.qty)
    if state is not None:
        for p in state.positions:
            if p.qty != 0 and not _is_book(p.symbol, p.is_hedge_book):
                held[p.symbol] = HeldPosition(Side.LONG if p.qty > 0 else Side.SHORT, abs(p.qty))
    return held


def _categories(sizing: Mapping[str, Any] | None) -> frozenset[str]:
    value = sizing.get("categories") if sizing is not None else None
    return frozenset(str(v) for v in value) if isinstance(value, list) else frozenset()


def _signed(side: Side, qty: Decimal) -> Decimal:
    return qty if side is Side.LONG else -qty


def latest_hedge_row(session: Session, account: Account) -> HedgeBookRow | None:
    return session.scalars(
        sa.select(HedgeBookRow)
        .where(HedgeBookRow.account == account.value)
        .order_by(HedgeBookRow.updated_at.desc(), HedgeBookRow.id.desc())
        .limit(1)
    ).first()


def book_exposures(
    session: Session,
    account: Account,
    rows: Sequence[PositionRow],
    state: AccountState | None,
    held: Mapping[str, HeldPosition],
    now: datetime,
) -> tuple[Exposure, ...]:
    """Positions (risk to their stop), pending entries (their sized risk) and the BTC hedge book."""
    by_symbol = {r.symbol: r for r in rows}
    marks = {p.symbol: p.mark_price for p in state.positions} if state is not None else {}
    sizing_by_event = verdict_sizing(session, account, [r.event_id for r in rows if r.event_id])
    out: list[Exposure] = []
    for symbol, h in held.items():
        row = by_symbol.get(symbol)
        mark = marks.get(symbol) or (row.mark_price if row is not None else None)
        if mark is None and row is not None:
            mark = row.entry_price
        stop = row.stop_price if row is not None else None
        leverage = (row.leverage if row is not None else None) or 1
        risk = book_risk(_signed(h.side, h.qty), stop, mark, leverage) if mark is not None else Decimal(0)
        cats = (
            _categories(sizing_by_event.get(row.event_id))
            if row is not None and row.event_id
            else frozenset()
        )
        out.append(Exposure(symbol, h.side, risk, cats))
    out.extend(pending_entries(session, account, held, now))
    hedge = hedge_exposure(session, account, rows, state)
    if hedge is not None:
        out.append(hedge)
    return tuple(out)


def verdict_sizing(
    session: Session, account: Account, event_ids: Sequence[str]
) -> dict[str, dict[str, Any] | None]:
    """`risk_verdicts.sizing` of this namespace's events."""
    if not event_ids:
        return {}
    rows = session.execute(
        sa.select(RiskVerdictRow.event_id, RiskVerdictRow.sizing).where(
            RiskVerdictRow.account == account.value, RiskVerdictRow.event_id.in_(event_ids)
        )
    )
    return {row.event_id: row.sizing for row in rows}


def pending_entries(
    session: Session, account: Account, held: Mapping[str, HeldPosition], now: datetime
) -> list[Exposure]:
    """Approved entries not filled, expired, rejected or voided yet, valued at their sized risk."""
    rows = session.execute(
        sa.select(RiskIntentRow.event_id, RiskIntentRow.payload, ProcessedIntentRow.status)
        .outerjoin(
            ProcessedIntentRow,
            sa.and_(
                ProcessedIntentRow.account == RiskIntentRow.account,
                ProcessedIntentRow.intent_id == RiskIntentRow.intent_id,
            ),
        )
        .where(
            RiskIntentRow.account == account.value,
            RiskIntentRow.leg == Leg.ENTRY.value,
            RiskIntentRow.created_at > now - PENDING_LOOKBACK,
        )
    ).all()
    live: dict[str, OrderIntent] = {}
    for event_id, payload, status in rows:
        if status in TERMINAL_INTENT_STATUSES:
            continue
        intent = OrderIntent.model_validate(payload)
        if intent.symbol in held or (intent.expires_at is not None and intent.expires_at <= now):
            continue
        live[str(event_id)] = intent
    if not live:
        return []
    sizing = verdict_sizing(session, account, list(live))
    out: list[Exposure] = []
    for event_id, intent in sorted(live.items()):
        breakdown = sizing.get(event_id)
        risk = breakdown.get("risk_usd_actual") if isinstance(breakdown, Mapping) else None
        out.append(
            Exposure(
                intent.symbol,
                Side.LONG if intent.side is OrderSide.BUY else Side.SHORT,
                Decimal(repr(float(risk))) if isinstance(risk, int | float) else Decimal(0),
                _categories(breakdown),
                pending=True,
            )
        )
    return out


@dataclass(frozen=True)
class _BookPosition:
    qty: Decimal  # signed, 0 = no book
    mark: Decimal | None
    leverage: int
    row: PositionRow | None


def _book_position(rows: Sequence[PositionRow], state: AccountState | None) -> _BookPosition:
    """The BTC hedge book: the latest `AccountState` when it has one, else the ledger row."""
    row = next((r for r in rows if _is_book(r.symbol, r.is_hedge_book) and r.qty > 0), None)
    qty = _signed(Side(row.side), row.qty) if row is not None else Decimal(0)
    mark = row.mark_price if row is not None else None
    leverage = (row.leverage if row is not None else None) or 1
    if state is not None:
        pos = next((p for p in state.positions if p.qty != 0 and _is_book(p.symbol, p.is_hedge_book)), None)
        if pos is not None:
            qty, mark, leverage = pos.qty, pos.mark_price, pos.leverage
        elif row is None:
            qty = Decimal(0)
    return _BookPosition(qty, mark, leverage, row)


def hedge_exposure(
    session: Session, account: Account, rows: Sequence[PositionRow], state: AccountState | None
) -> Exposure | None:
    """The BTC hedge book valued at its distance to the book stop (counts toward same-direction risk)."""
    book = _book_position(rows, state)
    if book.qty == 0:
        return None
    row, mark = book.row, book.mark
    booked = latest_hedge_row(session, account)
    stop = row.stop_price if row is not None and row.stop_price is not None else None
    if stop is None and booked is not None:
        stop = booked.stop_price
    if mark is None and booked is not None:
        mark = booked.btc_mark
    if mark is None:
        return None
    side = Side.LONG if book.qty > 0 else Side.SHORT
    return Exposure(BTC_SYMBOL, side, book_risk(book.qty, stop, mark, book.leverage), is_hedge_book=True)


def load_book(session: Session, account: Account, state: AccountState | None, now: datetime) -> BookInputs:
    rows = open_positions(session, account)
    held = held_positions(rows, state)
    return BookInputs(
        account_state=state,
        state_age_s=(now - state.ts).total_seconds() if state is not None else None,
        held=held,
        exposures=book_exposures(session, account, rows, state, held, now),
        kill=read_kill_state(session, account),
        hedge_qty=_book_position(rows, state).qty,
    )


def symbol_from_verdicts(session: Session, account: Account, coin_id: int) -> str | None:
    """The symbol this namespace last traded `coin_id` as (a held coin can leave the universe)."""
    return session.scalars(
        sa.select(RiskVerdictRow.symbol)
        .where(
            RiskVerdictRow.account == account.value,
            RiskVerdictRow.coin_id == coin_id,
            RiskVerdictRow.symbol.is_not(None),
            RiskVerdictRow.verdict == "approved",
        )
        .order_by(RiskVerdictRow.decided_at.desc())
        .limit(1)
    ).first()


def beta_for_positions(session: Session, account: Account, rows: Sequence[PositionRow]) -> dict[str, float]:
    """`hedge_beta` recorded on the verdict that opened each position (by the position's event)."""
    event_to_symbol = {r.event_id: r.symbol for r in rows if r.event_id}
    if not event_to_symbol:
        return {}
    out: dict[str, float] = {}
    for event_id, beta in session.execute(
        sa.select(RiskVerdictRow.event_id, RiskVerdictRow.hedge_beta).where(
            RiskVerdictRow.account == account.value, RiskVerdictRow.event_id.in_(list(event_to_symbol))
        )
    ).all():
        if beta is not None:
            out[event_to_symbol[event_id]] = float(beta)
    return out


# ============================================================================ outbox


def write_intents(session: Session, intents: Sequence[OrderIntent], now: datetime) -> None:
    """Signed intents into the outbox; `publish_order` keeps the leg order (stop plan before the entry)."""
    for order, intent in enumerate(intents):
        session.add(
            RiskIntentRow(
                intent_id=intent.intent_id,
                account=intent.account.value,
                event_id=intent.event_id,
                leg=intent.leg.value,
                seq=intent.seq,
                publish_order=order,
                payload=intent.model_dump(mode="json"),
                created_at=now,
                published_at=None,
                stream_id=None,
            )
        )


class IntentOutbox:
    """Relays one namespace's unpublished `risk_intents` rows to stream `orders` (idempotent on restart)."""

    def __init__(
        self,
        *,
        account: Account,
        sessions: sessionmaker[Session],
        redis: Redis,
        maxlen: int | None = None,
        clock: Callable[[], datetime] = utcnow,
    ) -> None:
        self.account = account
        self.sessions = sessions
        self.redis = redis
        self.maxlen = maxlen
        self.clock = clock
        self._lock = asyncio.Lock()

    def _pending(self) -> list[tuple[str, dict[str, Any]]]:
        with self.sessions() as s:
            rows = s.execute(
                sa.select(RiskIntentRow.intent_id, RiskIntentRow.payload)
                .where(RiskIntentRow.account == self.account.value, RiskIntentRow.published_at.is_(None))
                .order_by(RiskIntentRow.created_at, RiskIntentRow.event_id, RiskIntentRow.publish_order)
            )
            return [(row.intent_id, dict(row.payload)) for row in rows]

    def _mark(self, intent_id: str, stream_id: str) -> None:
        with transaction(self.sessions) as s:
            s.execute(
                sa.update(RiskIntentRow)
                .where(RiskIntentRow.intent_id == intent_id)
                .values(published_at=self.clock(), stream_id=stream_id)
            )

    async def relay_once(self) -> int:
        """XADD every unpublished intent in (created_at, publish_order) order; returns how many."""
        async with self._lock:
            pending = await asyncio.to_thread(self._pending)
            for intent_id, payload in pending:
                intent = OrderIntent.model_validate(payload)
                stream_id = await publish(self.redis, Stream.ORDERS, intent, maxlen=self.maxlen)
                await asyncio.to_thread(self._mark, intent_id, stream_id)
                INTENTS_PUBLISHED.labels(self.account.value).inc()
            if pending:
                log.info("intents relayed", extra={"account": self.account.value, "count": len(pending)})
            return len(pending)


# ============================================================================ decisions


def _load_inputs(
    conn: sa.Connection, decision: DecisionMsg
) -> tuple[QuantPacket | None, str | None, CandidateSet | None]:
    if decision.packet_sha256 is None:
        return None, None, None
    try:
        packet = load_packet(conn, decision.packet_sha256)
    except (ValidationError, ValueError) as exc:
        return None, f"stored packet failed re-verification ({type(exc).__name__})", None
    if packet is None:
        return None, None, None
    try:
        candidate_set = load_candidate_set(conn, packet.candidate_set_sha256)
    except (ValidationError, ValueError):
        candidate_set = None
    return packet, None, candidate_set


class DecisionConsumer:
    """The `decisions` handler of one namespace (consumer group `risk-{account}`)."""

    def __init__(
        self,
        *,
        account: Account,
        sessions: sessionmaker[Session],
        redis: Redis,
        signer: IntentSigner,
        market: MarketSource,
        flags: FlagsBook,
        states: AccountStateBook,
        runtime: Callable[[], RiskRuntime],
        clock: Callable[[], datetime] = utcnow,
        orders_maxlen: int | None = None,
        outbox: IntentOutbox | None = None,
    ) -> None:
        self.account = account
        self.sessions = sessions
        self.redis = redis
        self.signer = signer
        self.market = market
        self.flags = flags
        self.states = states
        self.runtime = runtime
        self.clock = clock
        self.outbox = outbox or IntentOutbox(
            account=account, sessions=sessions, redis=redis, maxlen=orders_maxlen, clock=clock
        )

    async def relay_once(self) -> int:
        return await self.outbox.relay_once()

    async def handle(self, msg_id: str, raw: RawPayload) -> bool:
        """One message, one transaction; True acks it (duplicates and malformed messages included)."""
        payload = raw.root
        try:
            decision = DecisionMsg.model_validate(payload)
        except ValidationError as exc:
            await self._reject_message(msg_id, payload, exc)
            return True
        now = self.clock()
        runtime = self.runtime()
        market = await asyncio.to_thread(self.market.market, decision.coin_id, now)
        await asyncio.to_thread(self.decide, msg_id, decision, market, runtime, now)
        await self.outbox.relay_once()
        return True

    async def _reject_message(self, msg_id: str, payload: dict[str, Any], exc: ValidationError) -> None:
        event_id = payload.get("event_id") if isinstance(payload.get("event_id"), str) else None
        version = payload.get("schema_version")
        reason = f"invalid DecisionMsg (schema_version {version!r}): {exc.error_count()} error(s)"
        await dead_letter(
            self.redis,
            stream=Stream.DECISIONS,
            group=risk_group(self.account),
            msg_id=msg_id,
            reason=reason,
            fields={"data": json.dumps(payload, default=str)},
        )

        def write_alert() -> None:
            with transaction(self.sessions) as s:
                alert(
                    s,
                    kind="integrity_error",
                    severity="critical",
                    title=f"{self.account.value}: decision {event_id or msg_id} dead-lettered",
                    detail=reason,
                    account=self.account.value,
                    service=SERVICE,
                    episode=event_id or msg_id,
                )

        await asyncio.to_thread(write_alert)

    def decide(
        self,
        msg_id: str | None,
        decision: DecisionMsg,
        market: MarketInputs,
        runtime: RiskRuntime,
        now: datetime,
    ) -> GateResult | None:
        """Evaluate and record one decision in one transaction; None when it was already processed."""
        a = self.account
        with transaction(self.sessions) as s:
            first = insert_once(
                s.connection(),
                _table(ProcessedDecisionRow),
                {"account": a.value, "event_id": decision.event_id, "processed_at": now},
            )
            if not first:
                log.info(
                    "duplicate decision acked", extra={"account": a.value, "event_id": decision.event_id}
                )
                return None
            result = self._evaluate(s, decision, market, runtime, now)
            signed = [self.signer.sign(intent) for intent in result.intents]
            sizing = dict(result.sizing) if result.sizing is not None else None
            if sizing is not None and result.exposure is not None:
                sizing["categories"] = sorted(result.exposure.categories)
            s.add(
                RiskVerdictRow(
                    account=a.value,
                    event_id=decision.event_id,
                    msg_id=msg_id,
                    coin_id=decision.coin_id,
                    symbol=result.symbol,
                    decision_intent=decision.intent.value,
                    effective_intent=result.effective_intent.value if result.effective_intent else None,
                    verdict=result.verdict,
                    reason=result.reason,
                    intent_id=result.entry_intent_id,
                    signature_ok=True if signed else None,
                    entry_client_id=result.entry_client_id,
                    stop_client_algo_id=result.stop_client_algo_id,
                    sizing=sizing,
                    checks=result.checks_json(),
                    hedge_beta=result.hedge_beta,
                    config_version_ids=dict(runtime.config_version_ids),
                    decided_at=now,
                )
            )
            write_intents(s, signed, now)
            if result.alert is not None:
                alert(
                    s,
                    kind=result.alert.kind,
                    severity=result.alert.severity,
                    title=result.alert.title,
                    detail=result.reason,
                    account=a.value,
                    service=SERVICE,
                    episode=_alert_episode(result.alert.kind, decision.event_id),
                )
        if result.alert is not None and result.alert.kind == "account_state_stale":
            self.states.mark_stale_alerted(a)  # after the commit: the next fresh state resolves it
        VERDICTS.labels(a.value, result.verdict).inc()
        log.info(
            "risk verdict",
            extra={
                "account": a.value,
                "event_id": decision.event_id,
                "verdict": result.verdict,
                "reason": result.reason,
                "intents": len(signed),
            },
        )
        return result

    def _evaluate(
        self, s: Session, decision: DecisionMsg, market: MarketInputs, runtime: RiskRuntime, now: datetime
    ) -> GateResult:
        a = self.account
        packet, packet_error, candidate_set = _load_inputs(s.connection(), decision)
        if market.symbol is None:
            known = symbol_from_verdicts(s, a, decision.coin_id)
            if known is not None:
                market = replace(market, symbol=known, is_btc=known == BTC_SYMBOL)
        return evaluate(
            GateInputs(
                decision=decision,
                packet=packet,
                packet_error=packet_error,
                candidate_set=candidate_set,
                flags=self.flags.get(decision.coin_id),
                market=market,
                book=load_book(s, a, self.states.get(a), now),
            ),
            GateConfig(a, runtime.risk, runtime.stale_s, self.signer.key_id, runtime.opens(a)),
            now,
        )


def stream_consumer(
    consumer: DecisionConsumer,
    *,
    name: str = "risk",
    block_ms: int = 5000,
    batch_size: int = 50,
    reclaim_idle_ms: int = 60000,
) -> StreamConsumer[RawPayload]:
    """Consumer group `risk-{account}` on `decisions`, created at the stream tail (`$`) so a first attach
    never replays the backlog; a restart reclaims overdue pending messages."""
    return StreamConsumer(
        redis=consumer.redis,
        stream=Stream.DECISIONS,
        group=risk_group(consumer.account),
        consumer=name,
        model=RawPayload,
        handler=consumer.handle,
        batch_size=batch_size,
        block_ms=block_ms,
        reclaim_idle_ms=reclaim_idle_ms,
        start_id="$",
    )


# ============================================================================ veto exits


def veto_event_id(account: Account, flags: RiskFlags) -> str:
    return f"veto:{account.value}:{flags.coin_id}:{int(flags.as_of.timestamp())}"


def exit_intent(
    account: Account, event_id: str, symbol: str, held: HeldPosition, now: datetime, key_id: str
) -> OrderIntent:
    """Unsigned MARKET reduce-only close of the whole held position."""
    return OrderIntent(
        intent_id=intent_id_for(account, event_id, Leg.EXIT, 0),
        account=account,
        event_id=event_id,
        leg=Leg.EXIT,
        seq=0,
        symbol=symbol,
        side=OrderSide.SELL if held.side is Side.LONG else OrderSide.BUY,
        order_type=OrderType.MARKET,
        qty=held.qty,
        reduce_only=True,
        client_id=client_id(event_id, Leg.EXIT, 0),
        created_at=now,
        key_id=key_id,
    )


class VetoExits:
    """Signed EXIT when an unexpired `RiskFlags` vetoes the side of a held position (one per flag)."""

    def __init__(
        self,
        *,
        account: Account,
        sessions: sessionmaker[Session],
        signer: IntentSigner,
        states: AccountStateBook,
        outbox: IntentOutbox,
        coin_symbol: Callable[[int, datetime], str | None],
        runtime: Callable[[], RiskRuntime],
        clock: Callable[[], datetime] = utcnow,
    ) -> None:
        self.account = account
        self.sessions = sessions
        self.signer = signer
        self.states = states
        self.outbox = outbox
        self.coin_symbol = coin_symbol
        self.runtime = runtime
        self.clock = clock

    async def check(self, flags: RiskFlags) -> OrderIntent | None:
        """Write (and relay) the EXIT when required; None when the coin is not held or not vetoed."""
        now = self.clock()
        if not (flags.veto_long or flags.veto_short) or now > flags.expires_at:
            return None
        symbol = await asyncio.to_thread(self._symbol, flags.coin_id, now)
        if symbol is None:
            return None
        runtime = self.runtime()
        written = await asyncio.to_thread(self._write, flags, symbol, runtime, now)
        if written is not None:
            await self.outbox.relay_once()
        return written

    def _symbol(self, coin_id: int, now: datetime) -> str | None:
        symbol = self.coin_symbol(coin_id, now)
        if symbol is not None:
            return symbol
        with self.sessions() as s:
            return symbol_from_verdicts(s, self.account, coin_id)

    def _write(
        self, flags: RiskFlags, symbol: str, runtime: RiskRuntime, now: datetime
    ) -> OrderIntent | None:
        a = self.account
        with transaction(self.sessions) as s:
            held = held_positions(open_positions(s, a), self.states.get(a)).get(symbol)
            if held is None or not exit_required(flags, held.side, now):
                return None
            event_id = veto_event_id(a, flags)
            if not insert_once(
                s.connection(),
                _table(ProcessedDecisionRow),
                {"account": a.value, "event_id": event_id, "processed_at": now},
            ):
                return None
            intent = self.signer.sign(exit_intent(a, event_id, symbol, held, now, self.signer.key_id))
            text = (
                f"RiskFlags of {flags.as_of:%Y-%m-%d %H:%M:%S} UTC veto {held.side.value}: "
                f"exit {held.side.value} {held.qty} {symbol}"
            )
            s.add(
                RiskVerdictRow(
                    account=a.value,
                    event_id=event_id,
                    msg_id=None,
                    coin_id=flags.coin_id,
                    symbol=symbol,
                    decision_intent=Intent.EXIT.value,
                    effective_intent=Intent.EXIT.value,
                    verdict="approved",
                    reason=VETO_EXIT_REASON,
                    intent_id=intent.intent_id,
                    signature_ok=True,
                    entry_client_id=None,
                    stop_client_algo_id=None,
                    sizing=None,
                    checks=[{"passed": True, "text": text}],
                    hedge_beta=None,
                    config_version_ids=dict(runtime.config_version_ids),
                    decided_at=now,
                )
            )
            write_intents(s, [intent], now)
        VERDICTS.labels(a.value, "approved").inc()
        log.warning("veto exit signed", extra={"account": a.value, "symbol": symbol, "event_id": event_id})
        return intent
