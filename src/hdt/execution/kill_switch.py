"""Kill switch, pause/resume and the resume rules (phase 09 section 6 and "Sweep round 2").

Fixed order, never a cancel-all that could take the STOPs with it:
1. stop accepting intents: `kill_state = killed` (every exposure-increasing action re-reads it);
2. cancel resting entry orders (`entry`, `entry_ioc`) one by one by client id, confirmed;
3. cancel every take-profit (`tp1`) by `clientAlgoId`. STOPs are never touched: here the `trail` leg is
   the trailed STOP itself (a STOP_MARKET re-placed tighter), so it is kept like `sl`;
4. read `openAlgoOrders` and positions from the exchange: any position without a covering STOP gets one
   at once (2 attempts, else MARKET reduce-only close) before the namespace reports SAFE;
5. `flatten` (optional; its 2-step confirmation happens in the console/Telegram or `hdt-kill`): MARKET
   reduce-only close of every position, and only once a symbol is flat, `algoOpenOrders` cleans up its
   conditional orders. A requested flatten is SAFE only once the exchange shows every position flat.

Resume is refused while the latest reconcile run is not clean, whatever the stored cause (a reconcile
mismatch on a namespace already killed for another cause, or a daily-loss kill on top of a reconcile kill,
must not let the resume skip it); while the kill was caused by the daily loss and a new UTC day has not
started; and while the kill was caused by a reconcile mismatch and no clean reconcile ran after the kill.
A kill on top of a kill keeps the strictest cause (`Ledger.set_kill_state`), so a manual kill never lifts
those blocks, and `since` restarts only on a new detection: re-engaging the stored cause (`enforce` after a
restart or a CLI kill, a retry) keeps it. A successful resume resolves the namespace's open `kill_switch`
alerts, so the next kill pages again.

`enforce` runs the sequence for a `killed` kill_state row this process has not brought to SAFE yet (a new
`since` or a new command id): rows written straight to Postgres by `hdt-kill` (Redis and Telegram may be
down, the namespace may already be killed), rows found after a restart, and kills whose sequence failed
part-way or ended not SAFE (retried with backoff until the report is SAFE). The sequence is idempotent
(cancels by client id, STOPs only where missing), so running it again re-verifies SAFE. `hdt-kill` asks
for flatten through its command id (`hdt-kill-flatten:` prefix), because the row has no other place for
it and execution's Postgres role cannot write `control_commands`.
"""

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Final, Literal, cast, get_args

from hdt.contracts.common import Leg
from hdt.core.clock import ensure_utc
from hdt.db.models.ledger import KillStateRow
from hdt.db.session import transaction
from hdt.execution.actions import cancel_algo, cancel_order_confirmed, close_market, close_sent
from hdt.execution.adapter_base import AlgoSnapshot, ExchangePosition, OrderSnapshot
from hdt.execution.client_ids import parse_client_id
from hdt.execution.order_manager import OrderManager
from hdt.execution.runtime import Namespace
from hdt.execution.stop_guard import StopGuard, covers

log = logging.getLogger(__name__)

KillCause = Literal["manual", "daily_loss", "reconcile", "rate_limit"]
KILL_CAUSES: Final[frozenset[str]] = frozenset(get_args(KillCause))
ENTRY_LEGS: Final[frozenset[Leg]] = frozenset({Leg.ENTRY, Leg.ENTRY_IOC})
CLI_COMMAND_PREFIX: Final[str] = "hdt-kill:"
CLI_FLATTEN_PREFIX: Final[str] = "hdt-kill-flatten:"
RETRY_MIN_S: Final[float] = 2.0
"""First wait before `enforce` re-runs a kill sequence that failed or ended not SAFE (doubles each time)."""
RETRY_MAX_S: Final[float] = 60.0
RESUME_REFUSED_EPISODE: Final[str] = "resume_refused"
"""Alert episode of a refused resume: pages once per kill, beside the open kill alert itself."""


def cli_command_id(*, flatten: bool) -> str:
    """Command id of one `hdt-kill` run (the flatten request travels in its prefix)."""
    return f"{CLI_FLATTEN_PREFIX if flatten else CLI_COMMAND_PREFIX}{uuid.uuid4().hex}"


def flatten_requested(command_id: str | None) -> bool:
    return command_id is not None and command_id.startswith(CLI_FLATTEN_PREFIX)


def is_cli_command(command_id: str | None) -> bool:
    return command_id is not None and command_id.startswith((CLI_COMMAND_PREFIX, CLI_FLATTEN_PREFIX))


@dataclass
class KillReport:
    account: str
    cause: str
    cancelled_entries: int = 0
    cancelled_tps: int = 0
    stops_placed: list[str] = field(default_factory=list)
    closed: list[str] = field(default_factory=list)
    unprotected: list[str] = field(default_factory=list)
    open_entries_left: int = 0
    open_tps_left: int = 0
    flatten: bool = False  # a flatten was requested: SAFE also needs every position flat
    not_flat: list[str] = field(default_factory=list)

    @property
    def safe(self) -> bool:
        return (
            not self.unprotected
            and self.open_entries_left == 0
            and self.open_tps_left == 0
            and not (self.flatten and self.not_flat)
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "account": self.account,
            "cause": self.cause,
            "safe": self.safe,
            "cancelled_entries": self.cancelled_entries,
            "cancelled_tps": self.cancelled_tps,
            "stops_placed": self.stops_placed,
            "closed": self.closed,
            "unprotected": self.unprotected,
            "open_entries_left": self.open_entries_left,
            "open_tps_left": self.open_tps_left,
            "flatten": self.flatten,
            "not_flat": self.not_flat,
        }


def check_safety(
    report: KillReport,
    positions: Sequence[ExchangePosition],
    algos: Sequence[AlgoSnapshot],
    orders: Sequence[OrderSnapshot],
) -> KillReport:
    """Fill the SAFE fields of `report` from the exchange's view (read only): positions without a
    covering STOP, resting entry orders and working take-profits."""
    report.unprotected = [
        p.symbol
        for p in positions
        if p.qty != 0
        and not any(
            a.symbol == p.symbol
            and a.is_working
            and covers(a.side.value, a.order_type, a.qty, a.close_position, p.qty)
            for a in algos
        )
    ]
    report.open_entries_left = sum(
        1 for o in orders if (parsed := parse_client_id(o.client_id)) is not None and parsed.leg in ENTRY_LEGS
    )
    report.open_tps_left = sum(
        1
        for a in algos
        if a.is_working
        and (parsed := parse_client_id(a.client_algo_id)) is not None
        and parsed.leg is Leg.TP1
    )
    return report


@dataclass(frozen=True)
class ResumeDecision:
    allowed: bool
    reason: str


def resume_allowed(
    state: str,
    cause: str | None,
    since: datetime | None,
    now: datetime,
    last_reconcile_clean: bool | None,
    last_reconcile_at: datetime | None,
) -> ResumeDecision:
    """Execution's resume rules (pure). Dates are compared in UTC whatever the timezone of the inputs."""
    if state == "running":
        return ResumeDecision(True, "already running")
    if cause == "daily_loss" and since is not None and ensure_utc(since).date() >= ensure_utc(now).date():
        return ResumeDecision(False, "daily loss kill: resume allowed from the next UTC day")
    if last_reconcile_clean is False:
        return ResumeDecision(False, "the latest reconcile is not clean")
    if cause == "reconcile":
        if last_reconcile_clean is not True or last_reconcile_at is None:
            return ResumeDecision(False, "reconcile kill: the latest reconcile is not clean")
        if since is not None and last_reconcile_at <= since:
            return ResumeDecision(False, "reconcile kill: no clean reconcile since the kill")
    return ResumeDecision(True, "resume rules satisfied")


class KillSwitch:
    def __init__(self, ns: Namespace, manager: OrderManager, guard: StopGuard) -> None:
        self.ns = ns
        self.manager = manager
        self.guard = guard
        self.engaged_since: datetime | None = None  # `since` of the kill this process last brought to SAFE
        self.engaged_command: str | None = None  # and its command id (a repeated kill keeps `since`)
        self._retry_at = float("-inf")  # monotonic time from which `enforce` may re-run the failed kill
        self._retry_s = RETRY_MIN_S
        self._failed: tuple[datetime | None, str | None] | None = None  # (since, command id) not SAFE
        self._flatten_pending = False  # a flatten asked in this process whose sequence is not SAFE yet
        self._flatten_command: str | None = None  # the command id of that flatten

    def _unengaged(self, row: KillStateRow | None) -> bool:
        return (
            row is not None
            and row.state == "killed"
            and row.since is not None
            and (row.since != self.engaged_since or row.command_id != self.engaged_command)
        )

    def _backing_off(self, row: KillStateRow) -> bool:
        """The same kill row failed recently: wait for its retry time (a new kill row runs at once)."""
        return self._failed == (row.since, row.command_id) and time.monotonic() < self._retry_at

    async def enforce(self) -> KillReport | None:
        """Engage a `killed` kill_state row this process has not brought to SAFE yet (takes the namespace
        lock); a sequence that failed or ended not SAFE is re-run with backoff. The stored cause is not a
        new detection: its `since` (the daily-loss resume day) is kept."""
        row = self.ns.ledger.kill_state()
        if row is None or not self._unengaged(row) or self._backing_off(row):
            return None
        async with self.ns.lock:
            row = self.ns.ledger.kill_state()
            if row is None or not self._unengaged(row) or self._backing_off(row):
                return None
            cause = cast(KillCause, row.cause if row.cause in KILL_CAUSES else "manual")
            log.warning(
                "kill state found killed, engaging the kill sequence",
                extra={"account": self.ns.name, "cause": cause, "command_id": row.command_id},
            )
            flatten = flatten_requested(row.command_id) or (
                self._flatten_pending and row.command_id == self._flatten_command
            )
            return await self.engage(
                cause,
                row.reason or "kill state is killed",
                flatten=flatten,
                command_id=row.command_id,
                actor="hdt-kill" if is_cli_command(row.command_id) else "execution",
                restart_since=False,
            )

    async def engage(
        self,
        cause: KillCause,
        reason: str,
        *,
        flatten: bool = False,
        command_id: str | None = None,
        actor: str = "execution",
        restart_since: bool = True,
    ) -> KillReport:
        """Run the fixed kill sequence; the caller holds the namespace lock.

        The kill counts as engaged by this process only once the report is SAFE; until then (an exchange
        error part-way, a position left without STOP) `enforce` re-runs it with backoff. `restart_since`
        is False when this re-engages the stored cause rather than a new detection.
        """
        ns = self.ns
        # 1. stop accepting intents (a kill on a kill keeps the strictest cause)
        row = ns.ledger.set_kill_state(
            "killed", cause=cause, reason=reason, command_id=command_id, restart_since=restart_since
        )
        effective = cast(KillCause, row.cause if row.cause in KILL_CAUSES else "manual")
        report = KillReport(ns.name, effective, flatten=flatten)
        ns.alert("kill_switch", "critical", f"{ns.name} killed ({effective}): {row.reason or reason}")
        try:
            await self._sequence(report, flatten=flatten)
        except Exception:
            self._not_safe(row, flatten=flatten)
            raise
        finally:
            ns.notify()
        self._audit(report, reason=reason, actor=actor, command_id=command_id, flatten=flatten)
        if report.safe:
            self._safe(row)
        else:
            self._not_safe(row, flatten=flatten)
        return report

    def _safe(self, row: KillStateRow) -> None:
        self.engaged_since = row.since
        self.engaged_command = row.command_id
        self._retry_at = float("-inf")
        self._retry_s = RETRY_MIN_S
        self._failed = None
        self._flatten_pending = False
        self._flatten_command = None

    def _not_safe(self, row: KillStateRow, *, flatten: bool) -> None:
        if flatten:
            self._flatten_pending = True
            self._flatten_command = row.command_id
        log.error(
            "kill sequence not SAFE, retrying in %.0f s",
            self._retry_s,
            extra={"account": self.ns.name, "command_id": row.command_id},
        )
        self._failed = (row.since, row.command_id)
        self._retry_at = time.monotonic() + self._retry_s
        self._retry_s = min(self._retry_s * 2, RETRY_MAX_S)

    async def _sequence(self, report: KillReport, *, flatten: bool) -> None:
        """Steps 2-5 of the fixed order, then the exchange-side SAFE check."""
        ns = self.ns
        adapter = ns.adapter
        # 2. cancel regular entry orders by leg classification (ledger + exchange-only ones)
        report.cancelled_entries += await self.manager.cancel_entries()
        for order in await adapter.open_orders():
            parsed = parse_client_id(order.client_id)
            if parsed is not None and parsed.leg in ENTRY_LEGS:
                await cancel_order_confirmed(ns, order.symbol, order.client_id)
                report.cancelled_entries += 1
        # 3. cancel TPs by clientAlgoId; STOPs (sl, trail) untouched
        tp_ids = {
            a.client_algo_id
            for a in await adapter.open_algo_orders()
            if (parsed := parse_client_id(a.client_algo_id)) is not None and parsed.leg is Leg.TP1
        } | {algo.client_algo_id for algo in ns.ledger.working_algos() if algo.leg == Leg.TP1.value}
        for client_algo_id in sorted(tp_ids):
            await cancel_algo(ns, client_algo_id)
            report.cancelled_tps += 1
        # 4. every position must hold a STOP. Conditional orders are read before positions: a STOP that
        # fires between the two reads then shows as a closed position, never as an unprotected one.
        algos = await adapter.open_algo_orders()
        positions = await adapter.positions()
        issues = await self.guard.check_exchange(positions, algos)
        for issue in issues:
            (report.stops_placed if issue.resolved else report.closed).append(issue.symbol)
        # 5. optional flatten
        if flatten:
            for p in await adapter.positions():
                await self._flatten_one(report, p)
        await self._verify(report, flatten=flatten)

    async def _flatten_one(self, report: KillReport, p: ExchangePosition) -> None:
        ns = self.ns
        pos = ns.ledger.position(p.symbol)
        event_id = pos.event_id if pos is not None and pos.event_id else f"kill:{p.symbol}"
        if pos is not None:
            ns.ledger.update_position(pos.position_id, exit_in_progress=True, exit_reason="kill")
        placed_ok = False
        try:
            placed = await close_market(ns, p.symbol, p.qty, event_id=event_id)
            placed_ok = close_sent(placed)  # an EXPIRED MARKET closed nothing
        finally:
            if not placed_ok and pos is not None:
                # the close is not working: the order manager's exits (time stop, trailing) stay armed
                ns.ledger.update_position(pos.position_id, exit_in_progress=False, exit_reason=None)
        if placed_ok:
            report.closed.append(p.symbol)

    async def _verify(self, report: KillReport, *, flatten: bool) -> None:
        adapter = self.ns.adapter
        algos = await adapter.open_algo_orders()
        positions = await adapter.positions()
        orders = await adapter.open_orders()
        if flatten:
            open_symbols = {p.symbol for p in positions if p.qty != 0}
            for symbol in {a.symbol for a in algos} - open_symbols:
                await adapter.cancel_all_algos(symbol)  # only after the symbol's position is closed
            algos = await adapter.open_algo_orders()
            report.not_flat = sorted(open_symbols)
        check_safety(report, positions, algos, orders)
        if report.unprotected:
            self.ns.alert(
                "stop_invariant",
                "critical",
                f"{self.ns.name} kill: positions without STOP: {', '.join(report.unprotected)}",
            )

    def _audit(
        self, report: KillReport, *, reason: str, actor: str, command_id: str | None, flatten: bool
    ) -> None:
        from hdt.configapi.audit import write_audit

        try:
            with transaction(self.ns.sessions) as s:
                write_audit(
                    s,
                    user=actor,
                    ip=None,
                    action="flatten" if flatten else "kill",
                    section="controls",
                    diff={"reason": reason, "command_id": command_id, **report.as_dict()},
                )
        except Exception:
            log.exception("kill audit entry could not be written", extra={"account": self.ns.name})

    def pause(self, reason: str, command_id: str | None = None) -> None:
        current = self.ns.ledger.kill_state()
        if current is not None and current.state == "killed":
            return  # a kill is never downgraded to a pause
        self.ns.ledger.set_kill_state("paused", cause="manual", reason=reason, command_id=command_id)
        self.ns.notify()

    def resume(self, command_id: str | None = None) -> ResumeDecision:
        """Apply the resume rules. A refused resume raises a warning (once per kill); an accepted one
        resolves the open kill alerts so the next kill pages again."""
        ns = self.ns
        row = ns.ledger.kill_state()
        if row is None:
            return ResumeDecision(True, "already running")
        last = ns.ledger.last_reconcile()
        decision = resume_allowed(
            row.state,
            row.cause,
            row.since,
            ns.clock(),
            last.clean if last is not None else None,
            last.ran_at if last is not None else None,
        )
        log.info(
            "resume %s",
            "accepted" if decision.allowed else "refused",
            extra={"account": ns.name, "why": decision.reason},
        )
        if not decision.allowed:
            ns.alert(
                "kill_switch",
                "warning",
                f"{ns.name} resume refused: {decision.reason}",
                episode=RESUME_REFUSED_EPISODE,
            )
        elif row.state != "running":
            ns.ledger.set_kill_state("running", cause=None, reason=None, command_id=command_id)
            ns.resolve("kill_switch")
            ns.resolve("kill_switch", episode=RESUME_REFUSED_EPISODE)
            ns.notify()
        return decision
