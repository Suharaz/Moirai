"""Reconciler (phase 09 section 5): exchange vs ledger every `reconcile_interval_s`, per namespace.

One cycle:
1. read open orders and open algo orders; resolve lost events: ledger-open orders / working algos the
   exchange no longer lists are looked up by client id and their final state applied; missing funding /
   transfers are pulled from the income history (from the last recorded flow on);
2. read open algo orders, the balance (`/fapi/v3/account`) and positions (`/fapi/v3/positionRisk`), in that
   order; a symbol whose position differs from the ledger first pulls its missing fills (`userTrades` from
   the last seen trade id), then the three are read again, up to `CATCH_UP_ROUNDS` times. A fill landing
   mid-cycle (a stop triggering between two reads) is caught up, never compared stale against a ledger
   that already has it;
3. compare **net per symbol** on that settled view: exchange position vs ledger position; orders and
   conditional orders the ledger does not know (foreign ids) are mismatches too; the wallet must match the
   ledger's expected wallet (anchor + realized PnL - USDT fees + cash flows) within a tolerance;
4. `StopGuard.check_exchange` runs every cycle on the same view, whatever the comparison found (the algo
   orders are read before the positions, so a stop that fired in between shows as a smaller position,
   never as a missing stop): a STOP invariant repair never waits for a second sighting;
5. the run is written to `reconcile_runs` (a mismatch makes it dirty, which alone refuses a resume). A
   mismatch kills only once seen on two consecutive runs of the namespace (the same kind and subject, the
   amounts may differ; the previous run is read from `reconcile_runs`, so a restart or a re-sync in between
   still counts): `positionRisk` may show a fill before `userTrades` lists it, and waiting for it inside
   the cycle would hold the namespace lock. A first sighting is logged and re-checked by a cycle
   `MISMATCH_RECHECK_S` later. A confirmed mismatch kills the namespace (cause `reconcile`) and alerts,
   also when it is already killed: the cause becomes `reconcile` unless a stricter one holds, so resume
   needs a clean reconcile whatever stopped the namespace first. A clean run resolves the open
   `reconcile_mismatch` (and, with the invariant intact, `stop_invariant`) alert. Only this namespace:
   `testnet` mismatching never stops `paper`.

Also tracks the real `liquidationPrice` of each position: it must stay farther from the entry than the
stop by `liq_distance_mult` x the stop distance, else an alert is raised (once per symbol and UTC day) and
the stop tightened (the position's stop moves toward the entry so the stop is hit long before
liquidation).
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import timedelta
from decimal import Decimal
from typing import Any, Final

from hdt.execution.adapter_base import AlgoEvent, AlgoSnapshot, ExchangePosition, OrderEvent, OrderSnapshot
from hdt.execution.kill_switch import KillSwitch
from hdt.execution.ledger import signed_qty
from hdt.execution.order_manager import OrderManager
from hdt.execution.runtime import Namespace
from hdt.execution.stop_guard import StopGuard

log = logging.getLogger(__name__)

WALLET_TOLERANCE_ABS: Final[Decimal] = Decimal("1")
WALLET_TOLERANCE_REL: Final[Decimal] = Decimal("0.001")
PENDING_GRACE: Final[timedelta] = timedelta(seconds=60)
CASH_FLOW_LOOKBACK: Final[timedelta] = timedelta(days=2)
CATCH_UP_ROUNDS: Final[int] = 2
MISMATCH_RECHECK_S: Final[float] = 5.0
"""Delay of the cycle that re-checks a mismatch seen once (instead of the regular interval)."""
MISMATCH_SUBJECT: Final[tuple[str, ...]] = ("kind", "symbol", "client_id", "client_algo_id")
"""Fields that identify a mismatch across runs (the amounts are left out: a wallet gap may move)."""


@dataclass
class ReconcileReport:
    clean: bool = True
    stop_invariant_ok: bool = True
    confirmed: bool = False  # a mismatch the previous run saw too: the namespace is killed
    mismatches: list[dict[str, Any]] = field(default_factory=list)

    def add(self, kind: str, **detail: Any) -> None:
        self.clean = False
        self.mismatches.append({"kind": kind, **{k: str(v) for k, v in detail.items()}})

    def detail(self) -> str | None:
        if self.clean and self.stop_invariant_ok:
            return None
        kinds = sorted({m["kind"] for m in self.mismatches})
        label = "mismatch" if self.confirmed else "mismatch seen once, re-checked next cycle"
        parts = [f"{label}: {', '.join(kinds)}"] if kinds else []
        if not self.stop_invariant_ok:
            parts.append("stop invariant repaired")
        return "; ".join(parts)


def mismatch_subjects(mismatches: Iterable[Mapping[str, Any]]) -> frozenset[tuple[str, ...]]:
    return frozenset(tuple(str(m.get(k, "")) for k in MISMATCH_SUBJECT) for m in mismatches)


def net_positions(positions: list[ExchangePosition]) -> dict[str, Decimal]:
    out: dict[str, Decimal] = {}
    for p in positions:
        out[p.symbol] = out.get(p.symbol, Decimal(0)) + p.qty
    return {s: q for s, q in out.items() if q != 0}


def wallet_matches(wallet: Decimal, expected: Decimal) -> bool:
    tolerance = max(WALLET_TOLERANCE_ABS, abs(wallet) * WALLET_TOLERANCE_REL)
    return abs(wallet - expected) <= tolerance


class Reconciler:
    def __init__(self, ns: Namespace, manager: OrderManager, guard: StopGuard, kill: KillSwitch) -> None:
        self.ns = ns
        self.manager = manager
        self.guard = guard
        self.kill = kill
        self.recheck_due = False  # the last run saw a mismatch once: the next cycle comes sooner

    def interval_s(self, regular_s: float) -> float:
        """Delay before the next cycle: `MISMATCH_RECHECK_S` while a first sighting awaits its re-check."""
        return min(regular_s, MISMATCH_RECHECK_S) if self.recheck_due else regular_s

    async def run_once(self) -> ReconcileReport:
        """One cycle; the caller holds the namespace lock."""
        ns = self.ns
        adapter = ns.adapter
        report = ReconcileReport()
        orders = await adapter.open_orders()
        algos = await adapter.open_algo_orders()
        await self._orders(orders, report)
        await self._algos(algos, report)
        await self._cash_flows()

        positions, algos = await self._settled_view()
        exchange_net = net_positions(positions)
        ledger_net = self._ledger_net()
        for symbol in sorted(set(exchange_net) | set(ledger_net)):
            ex, led = exchange_net.get(symbol, Decimal(0)), ledger_net.get(symbol, Decimal(0))
            if ex != led:
                report.add("position", symbol=symbol, exchange=ex, ledger=led)
        self._positions_detail(positions)
        self._wallet(report)

        issues = await self.guard.check_exchange(positions, algos)
        report.stop_invariant_ok = not issues
        await self._liquidation(positions)

        previous = ns.ledger.last_reconcile()
        seen_before = mismatch_subjects(previous.mismatches) if previous is not None else frozenset()
        report.confirmed = bool(mismatch_subjects(report.mismatches) & seen_before)
        self.recheck_due = not report.clean and not report.confirmed
        ns.ledger.insert_reconcile_run(
            clean=report.clean,
            stop_invariant_ok=report.stop_invariant_ok,
            detail=report.detail(),
            mismatches=report.mismatches,
        )
        if report.confirmed:
            detail = report.detail() or "reconcile mismatch"
            ns.alert("reconcile_mismatch", "critical", f"{ns.name} reconcile mismatch", detail)
            kill = ns.ledger.kill_state()
            if kill is not None and kill.state == "killed":
                # A kill already holds (manual, rate limit, ...): the mismatch still becomes its cause unless
                # a stricter one holds. The stored command id is kept (a pending flatten stays pending) and
                # the kill switch's `enforce` re-runs the sequence for the changed row.
                ns.ledger.set_kill_state(
                    "killed",
                    cause="reconcile",
                    reason=detail,
                    command_id=kill.command_id,
                    restart_since=False,
                )
            else:
                await self.kill.engage("reconcile", detail)
        elif not report.clean:
            log.warning(
                "reconcile mismatch seen once, re-checked next cycle",
                extra={"account": ns.name, "mismatches": report.mismatches},
            )
        else:
            ns.resolve("reconcile_mismatch")
            if report.stop_invariant_ok:
                ns.resolve("stop_invariant")
        ns.notify()
        return report

    def _ledger_net(self) -> dict[str, Decimal]:
        return {p.symbol: signed_qty(p) for p in self.ns.ledger.open_positions()}

    async def _settled_view(self) -> tuple[list[ExchangePosition], list[AlgoSnapshot]]:
        """Positions and algo orders (the balance into `ns.balance`) read after the ledger caught up with
        every fill they reflect; a difference left after `CATCH_UP_ROUNDS` catch-ups is reported (it kills
        only when the next run sees it again)."""
        positions, algos = await self._read_view()
        for _ in range(CATCH_UP_ROUNDS):
            exchange_net = net_positions(positions)
            ledger_net = self._ledger_net()
            differing = [
                s
                for s in sorted(set(exchange_net) | set(ledger_net))
                if exchange_net.get(s, Decimal(0)) != ledger_net.get(s, Decimal(0))
            ]
            if not differing:
                break
            for symbol in differing:
                await self.manager.sync_symbol(symbol)  # missed fills first
            positions, algos = await self._read_view()  # the catch-up may hold fills newer than the read
        return positions, algos

    async def _read_view(self) -> tuple[list[ExchangePosition], list[AlgoSnapshot]]:
        """Algo orders, balance, then positions: a fill between two reads always shows in the positions
        (the last read), so it is caught up instead of looking like a missing stop or a wallet gap."""
        adapter = self.ns.adapter
        algos = await adapter.open_algo_orders()
        self.ns.balance = await adapter.balance()
        positions = await adapter.positions()
        return positions, algos

    async def _orders(self, orders: list[OrderSnapshot], report: ReconcileReport) -> None:
        ns = self.ns
        listed = {o.client_id: o for o in orders}
        now = ns.clock()
        for row in ns.ledger.open_orders():
            if row.client_id in listed:
                continue
            snap = await ns.adapter.query_order(row.symbol, row.client_id)
            if snap is not None:
                await self.manager.apply_order_event(OrderEvent(snap))
                if snap.executed_qty > ns.ledger.filled_qty(row.client_id):
                    await self.manager.sync_symbol(row.symbol)
            elif row.status in ("PENDING", "UNKNOWN") and now - row.updated_at > PENDING_GRACE:
                ns.ledger.mark_order(row.client_id, "REJECTED", "never reached the exchange")
            elif row.status not in ("PENDING", "UNKNOWN"):
                report.add("order_missing", client_id=row.client_id, symbol=row.symbol)
        for snap in orders:
            if ns.ledger.order(snap.client_id) is None:
                report.add("foreign_order", client_id=snap.client_id, symbol=snap.symbol)
            else:
                ns.ledger.update_order(snap)

    async def _algos(self, algos: list[AlgoSnapshot], report: ReconcileReport) -> None:
        ns = self.ns
        listed = {a.client_algo_id: a for a in algos}
        now = ns.clock()
        for row in ns.ledger.working_algos():
            if row.client_algo_id in listed:
                continue
            snap = await ns.adapter.query_algo(row.client_algo_id)
            if snap is not None:
                await self.manager.apply_algo_event(AlgoEvent(snap))
            elif now - row.updated_at > PENDING_GRACE:
                ns.ledger.mark_algo(row.client_algo_id, "CANCELED", "absent on the exchange")
        for snap in algos:
            if ns.ledger.algo(snap.client_algo_id) is None:
                report.add("foreign_algo", client_algo_id=snap.client_algo_id, symbol=snap.symbol)
            else:
                ns.ledger.update_algo(snap)

    async def _cash_flows(self) -> None:
        """Income since the last recorded flow (inclusive: flows sharing its timestamp are deduplicated by
        id), never before the wallet anchor (or the look-back when no anchor exists yet)."""
        ns = self.ns
        acct = ns.ledger.exec_account()
        start = acct.anchor_at or (ns.clock() - CASH_FLOW_LOOKBACK)
        last = ns.ledger.last_cash_flow_at()
        if last is not None and last > start:
            start = last
        for flow in await ns.adapter.cash_flows(start):
            ns.ledger.record_cash_flow(flow)

    def _positions_detail(self, positions: list[ExchangePosition]) -> None:
        ledger = self.ns.ledger
        for p in positions:
            row = ledger.position(p.symbol)
            if row is not None:
                ledger.update_position(
                    row.position_id,
                    mark_price=p.mark_price,
                    unrealized_pnl=p.unrealized_pnl,
                    liquidation_price=p.liquidation_price,
                    leverage=p.leverage,
                    margin_type=p.margin_type,
                )

    def _wallet(self, report: ReconcileReport) -> None:
        ns = self.ns
        balance = ns.balance
        if balance is None:
            return
        expected = ns.ledger.expected_wallet()
        if expected is None:
            ns.ledger.set_wallet_anchor(balance.wallet_balance, ns.clock())
            return
        if not wallet_matches(balance.wallet_balance, expected):
            report.add("wallet", exchange=balance.wallet_balance, ledger=expected)

    async def _liquidation(self, positions: list[ExchangePosition]) -> None:
        ns = self.ns
        mult = Decimal(repr(ns.risk().liq_distance_mult))
        for p in positions:
            row = ns.ledger.position(p.symbol)
            if row is None or row.is_hedge_book or p.liquidation_price is None or p.liquidation_price <= 0:
                continue
            stop = self.guard.trigger_for(row)
            if stop is None:
                continue
            stop_distance = abs(row.entry_price - stop)
            liq_distance = abs(row.entry_price - p.liquidation_price)
            if stop_distance == 0 or liq_distance >= mult * stop_distance:
                continue
            # Tighten: the stop must sit at most liq_distance / mult from the entry.
            allowed = liq_distance / mult
            level = row.entry_price - allowed if p.qty > 0 else row.entry_price + allowed
            filters = await ns.symbol_filters(p.symbol)
            if filters is not None:
                level = filters.ceil_price(level) if p.qty > 0 else filters.floor_price(level)
            tightened = await self.guard.tighten(row, level)
            outcome = "stop tightened" if tightened else "stop not tightened (retried next cycle)"
            ns.alert(
                "liquidation_distance",
                "warning",
                f"{ns.name} {p.symbol}: liquidation {p.liquidation_price} closer than "
                f"{mult}x the stop distance, {outcome} to {level}",
                episode=f"{p.symbol}:{ns.clock().date().isoformat()}",
            )
