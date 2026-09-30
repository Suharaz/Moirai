"""`hdt-kill`: the independent kill path (phase 09 section 6), run over SSH with `docker exec` into the
`execution` container. It needs Postgres (HDT_PG_DSN, role hdt_execution), the lake mount (paper venue)
and vault scope `exec` (Binance namespaces) only: never Redis, Telegram or the console.

For every selected namespace:
1. `kill_state` becomes `killed` (cause `manual`, command id `hdt-kill:<hex>`, or `hdt-kill-flatten:<hex>`
   with `--flatten`): from then on execution refuses every exposure-increasing action of the namespace;
2. when the execution service is alive for the namespace (paper: `paper_state` saved within
   `PAPER_ALIVE`; Binance: a reconcile run within two reconcile intervals) its `KillSwitch.enforce` runs
   the kill sequence within a second, and the CLI watches the exchange until the namespace is SAFE
   (`--wait-s`); a 429 or a transient read error keeps the watch going (a 429 window is waited out). A
   Binance namespace still not SAFE (or not readable) by then is killed by the CLI itself (the exchange is
   the shared truth); a live paper venue is never touched by a second process;
3. otherwise the CLI runs the kill sequence itself (`kill_account`) on its own adapter.
A Binance namespace with a recorded IP ban (HTTP 418) is never contacted: the kill row stands and the
service enforces it after the ban; a ban hit by the CLI itself is recorded for every other process.
The sequence never cancels a STOP leg. The CLI prints every position with its STOP and exits 0 when every
namespace is SAFE, 1 otherwise, 2 on a usage error. `--flatten` needs `--confirm-flatten` (two steps).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Final

import sqlalchemy as sa
from sqlalchemy.orm import Session, sessionmaker

from hdt.contracts.common import Account
from hdt.core.clock import utcnow
from hdt.core.config import RiskFile, StaticConfig, static_config
from hdt.core.logging import configure_logging
from hdt.db.models.ledger import PaperStateRow
from hdt.db.session import make_engine, make_session_factory
from hdt.execution.adapter_base import ExchangeAdapter, ExchangeError, IpBannedError, RateLimitedError
from hdt.execution.binance_client import IP_BAN_DEFAULT_S
from hdt.execution.kill_switch import KillReport, KillSwitch, check_safety, cli_command_id
from hdt.execution.ledger import Ledger
from hdt.execution.namespaces import enabled_accounts
from hdt.execution.order_manager import OrderManager
from hdt.execution.paper import PaperAdapter
from hdt.execution.runtime import Namespace
from hdt.execution.stop_guard import StopGuard, covers
from hdt.execution.venues import NamespaceUnavailableError, exec_vault, open_adapter
from hdt.lake.pit_query import PitQuery
from hdt.risk.pinned import pinned_config as pin_config
from hdt.settings.versions import pin_current

log = logging.getLogger(__name__)

ACTOR: Final[str] = "hdt-kill"
PAPER_ALIVE: Final[timedelta] = timedelta(seconds=10)
RECONCILE_GRACE: Final[timedelta] = timedelta(seconds=15)
POLL_S: Final[float] = 1.0
DEFAULT_WAIT_S: Final[float] = 30.0
ALL: Final[str] = "all"
EXIT_SAFE: Final[int] = 0
EXIT_NOT_SAFE: Final[int] = 1
EXIT_USAGE: Final[int] = 2


@dataclass(frozen=True)
class PositionStop:
    symbol: str
    qty: Decimal  # signed, positive = long
    stop_client_algo_id: str | None  # None: the position has no covering STOP
    trigger: Decimal | None


@dataclass(frozen=True)
class KillOutcome:
    report: KillReport
    stops: list[PositionStop]
    by: str = ACTOR  # who ran the kill sequence: `hdt-kill` or `execution` (the running service)
    note: str | None = None

    def safe(self, *, flatten: bool) -> bool:
        return self.report.safe and not (flatten and self.stops)


async def position_stops(adapter: ExchangeAdapter) -> list[PositionStop]:
    """Every open position with the working STOP that covers it (exchange view)."""
    positions = await adapter.positions()
    algos = [a for a in await adapter.open_algo_orders() if a.is_working]
    out: list[PositionStop] = []
    for p in sorted(positions, key=lambda x: x.symbol):
        if p.qty == 0:
            continue
        stop = next(
            (
                a
                for a in sorted(algos, key=lambda x: x.client_algo_id)
                if a.symbol == p.symbol and covers(a.side.value, a.order_type, a.qty, a.close_position, p.qty)
            ),
            None,
        )
        out.append(
            PositionStop(
                symbol=p.symbol,
                qty=p.qty,
                stop_client_algo_id=stop.client_algo_id if stop is not None else None,
                trigger=stop.trigger_price if stop is not None else None,
            )
        )
    return out


async def observe(account: Account, adapter: ExchangeAdapter) -> KillOutcome:
    """The namespace's SAFE state as the exchange shows it (read only)."""
    report = check_safety(
        KillReport(account.value, "manual"),
        await adapter.positions(),
        await adapter.open_algo_orders(),
        await adapter.open_orders(),
    )
    return KillOutcome(report, await position_stops(adapter), by="execution")


async def kill_account(
    account: Account,
    *,
    sessions: sessionmaker[Session],
    adapter: ExchangeAdapter,
    risk: Callable[[], RiskFile],
    reason: str,
    flatten: bool = False,
    actor: str = ACTOR,
    command_id: str | None = None,
) -> KillOutcome:
    """Run the fixed kill sequence on `adapter` in this process; never touches Redis."""
    ns = Namespace(
        account=account, adapter=adapter, ledger=Ledger(sessions, account), sessions=sessions, risk=risk
    )
    guard = StopGuard(ns)
    manager = OrderManager(ns, guard)
    kill = KillSwitch(ns, manager, guard)
    async with ns.lock:
        report = await kill.engage("manual", reason, flatten=flatten, command_id=command_id, actor=actor)
    if isinstance(adapter, PaperAdapter):
        await adapter.drain(manager)  # the ledger follows the venue's events, as the service would
    return KillOutcome(report, await position_stops(adapter))


# ---------------------------------------------------------------------------- service liveness


def service_alive(sessions: sessionmaker[Session], account: Account, risk: RiskFile) -> bool:
    """Is the execution service running this namespace right now?"""
    now = utcnow()
    ledger = Ledger(sessions, account)
    if account is Account.PAPER:
        with sessions() as s:
            updated = s.scalar(
                sa.select(PaperStateRow.updated_at).where(PaperStateRow.account == Account.PAPER.value)
            )
        return updated is not None and now - updated <= PAPER_ALIVE
    last = ledger.last_reconcile()
    window = timedelta(seconds=2 * risk.reconcile_interval_s) + RECONCILE_GRACE
    return last is not None and now - last.ran_at <= window


async def wait_safe(
    read: Callable[[], Awaitable[KillOutcome]], *, flatten: bool, wait_s: float
) -> KillOutcome | None:
    """Poll `read` until the namespace is SAFE (and flat with `flatten`) or `wait_s` has passed; returns the
    last outcome read, None when no read succeeded.

    A transient read error (HTTP 429, 5xx, a timeout) keeps the watch going. A 429 window is waited out
    before the next read, also past `wait_s`, so a fallback that runs the kill sequence right after never
    contacts the exchange inside it (Binance bans an IP that keeps sending after a 429). An IP ban (418)
    ends the watch at once."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + wait_s
    outcome: KillOutcome | None = None
    while True:
        quiet_s = 0.0  # the exchange must not be contacted for this long (HTTP 429 Retry-After)
        try:
            outcome = await read()
        except IpBannedError:
            raise
        except RateLimitedError as exc:
            quiet_s = exc.retry_after_s
            log.warning("rate limited while watching the kill", extra={"retry_after_s": quiet_s})
        except (ExchangeError, OSError) as exc:
            log.warning("exchange read failed while watching the kill", extra={"error": str(exc)})
        else:
            if outcome.safe(flatten=flatten):
                return outcome
        remaining = deadline - loop.time()
        if remaining <= 0 or quiet_s >= remaining:
            await asyncio.sleep(quiet_s)
            return outcome
        await asyncio.sleep(max(quiet_s, min(POLL_S, remaining)))


# ---------------------------------------------------------------------------- one namespace


@dataclass(frozen=True)
class CliContext:
    static: StaticConfig
    sessions: sessionmaker[Session]
    pit: PitQuery
    risk: RiskFile
    reason: str
    flatten: bool
    wait_s: float


def _record_ban(ledger: Ledger, exc: IpBannedError) -> None:
    """Record a ban the CLI's own client hit, so the service and later runs stay silent too."""
    end = exc.banned_until if exc.banned_until is not None else time.time() + IP_BAN_DEFAULT_S
    ledger.record_ban(datetime.fromtimestamp(end, UTC))


def _ban_refusal(ledger: Ledger, account: Account) -> NamespaceUnavailableError | None:
    banned = ledger.banned_until() if account is not Account.PAPER else None
    if banned is None:
        return None
    return NamespaceUnavailableError(
        f"exchange IP ban recorded until {banned.isoformat()}; not contacted (the kill row stands)"
    )


async def kill_namespace(account: Account, ctx: CliContext) -> KillOutcome:
    command_id = cli_command_id(flatten=ctx.flatten)
    ledger = Ledger(ctx.sessions, account)
    ledger.set_kill_state("killed", cause="manual", reason=ctx.reason, command_id=command_id)
    refusal = _ban_refusal(ledger, account)
    if refusal is not None:  # any request (even a read) would extend the ban
        raise refusal
    alive = service_alive(ctx.sessions, account, ctx.risk)
    vault = None if account is Account.PAPER else exec_vault(ctx.sessions)[0]

    def fresh_adapter() -> ExchangeAdapter:
        return open_adapter(
            account,
            sessions=ctx.sessions,
            pit=ctx.pit,
            risk=ctx.risk,
            binance=ctx.static.binance,
            vault=vault,
        )

    async def run_here(note: str | None) -> KillOutcome:
        refusal = _ban_refusal(ledger, account)
        if refusal is not None:
            raise refusal
        adapter = fresh_adapter()
        try:
            outcome = await kill_account(
                account,
                sessions=ctx.sessions,
                adapter=adapter,
                risk=lambda: ctx.risk,
                reason=ctx.reason,
                flatten=ctx.flatten,
                command_id=command_id,
            )
        except IpBannedError as exc:
            _record_ban(ledger, exc)
            raise
        finally:
            await adapter.aclose()
        return KillOutcome(outcome.report, outcome.stops, note=note)

    if not alive:
        return await run_here("execution is not running this namespace: kill sequence run by hdt-kill")
    if account is Account.PAPER:

        async def read_paper() -> KillOutcome:
            adapter = fresh_adapter()  # re-read `paper_state`: the running service owns the venue
            try:
                return await observe(account, adapter)
            finally:
                await adapter.aclose()

        outcome = await wait_safe(read_paper, flatten=ctx.flatten, wait_s=ctx.wait_s)
        if outcome is not None and outcome.safe(flatten=ctx.flatten):
            return outcome
        if service_alive(ctx.sessions, account, ctx.risk):
            if outcome is None:
                raise NamespaceUnavailableError(
                    f"the paper venue could not be read within {ctx.wait_s:g} s; "
                    "the running service enforces the kill row"
                )
            return KillOutcome(
                outcome.report,
                outcome.stops,
                by="execution",
                note=f"the running service did not reach SAFE within {ctx.wait_s:g} s; "
                "stop the execution container and run hdt-kill again",
            )
        return await run_here("execution stopped while waiting: kill sequence run by hdt-kill")
    adapter = fresh_adapter()

    async def read_exchange() -> KillOutcome:
        try:
            return await observe(account, adapter)
        except IpBannedError as exc:
            _record_ban(ledger, exc)
            raise

    try:
        outcome = await wait_safe(read_exchange, flatten=ctx.flatten, wait_s=ctx.wait_s)
    finally:
        await adapter.aclose()
    if outcome is not None and outcome.safe(flatten=ctx.flatten):
        return outcome
    seen = "not SAFE" if outcome is not None else "exchange not readable (rate limit, errors)"
    return await run_here(f"{seen} {ctx.wait_s:g} s after the kill row: kill sequence run by hdt-kill")


# ---------------------------------------------------------------------------- CLI


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="hdt-kill",
        description=(
            "Kill switch without Redis, Telegram or the console: cancel entries and take-profits, keep "
            "every STOP, place any missing STOP, optionally flatten. Exit 0 = every namespace SAFE."
        ),
    )
    parser.add_argument(
        "--account",
        choices=[a.value for a in Account] + [ALL],
        default=ALL,
        help="namespace to kill (default: every enabled namespace and any namespace holding a position)",
    )
    parser.add_argument("--reason", required=True, help="why (kept in kill_state and the audit log)")
    parser.add_argument("--flatten", action="store_true", help="also close every position at MARKET")
    parser.add_argument("--confirm-flatten", action="store_true", help="second step required with --flatten")
    parser.add_argument(
        "--wait-s",
        type=float,
        default=DEFAULT_WAIT_S,
        help=f"how long to wait for a running execution service to reach SAFE (default {DEFAULT_WAIT_S:g})",
    )
    return parser


def pinned_config(sessions: sessionmaker[Session], static: StaticConfig) -> tuple[RiskFile, Account]:
    """The active risk config and run mode, read from Postgres (no Redis). A stored version that breaks
    the current hard ceilings runs clipped (the running services raise its alert): the kill never fails
    on configuration."""
    with sessions() as s:
        pin = pin_current(s)
    pinned = pin_config(pin, static)
    for problem in pinned.problems:
        log.warning(
            "stored config version breaks the hard ceilings, clipped",
            extra={"section": problem.section.value, "version_id": problem.version_id},
        )
    return pinned.risk, pinned.mode


def select_accounts(choice: str, sessions: sessionmaker[Session], mode: Account) -> list[Account]:
    if choice != ALL:
        return [Account(choice)]
    wanted = set(enabled_accounts(mode))
    wanted |= {a for a in Account if Ledger(sessions, a).open_positions()}
    return sorted(wanted, key=lambda a: a.value)


def render(account: Account, outcome: KillOutcome, *, flatten: bool) -> list[str]:
    report = outcome.report
    status = "SAFE" if outcome.safe(flatten=flatten) else "NOT SAFE"
    lines = [
        f"{account.value}: {status} (sequence run by {outcome.by}; entries cancelled "
        f"{report.cancelled_entries}, take-profits cancelled {report.cancelled_tps}, stops placed "
        f"{len(report.stops_placed)}, closed {len(report.closed)}; entries left {report.open_entries_left}, "
        f"take-profits left {report.open_tps_left})"
    ]
    if outcome.note:
        lines.append(f"  note: {outcome.note}")
    if not outcome.stops:
        lines.append("  no open position")
    for stop in outcome.stops:
        if stop.stop_client_algo_id is None:
            lines.append(f"  {stop.symbol} qty {stop.qty}: NO STOP")
        else:
            lines.append(
                f"  {stop.symbol} qty {stop.qty}: STOP {stop.stop_client_algo_id} trigger {stop.trigger}"
            )
    return lines


async def run(args: argparse.Namespace) -> int:
    static = static_config()
    engine = make_engine()
    sessions = make_session_factory(engine)
    try:
        risk, mode = pinned_config(sessions, static)
        data = static.settings.data
        ctx = CliContext(
            static=static,
            sessions=sessions,
            pit=PitQuery(Path(data.staging_root), Path(data.lake_root)),
            risk=risk,
            reason=args.reason,
            flatten=args.flatten,
            wait_s=args.wait_s,
        )
        all_safe = True
        for account in select_accounts(args.account, sessions, mode):
            try:
                outcome = await kill_namespace(account, ctx)
            except (NamespaceUnavailableError, ExchangeError, OSError) as exc:
                all_safe = False
                print(f"{account.value}: NOT SAFE (cannot reach the exchange: {exc})")
                continue
            all_safe = all_safe and outcome.safe(flatten=args.flatten)
            print("\n".join(render(account, outcome, flatten=args.flatten)))
        return EXIT_SAFE if all_safe else EXIT_NOT_SAFE
    finally:
        engine.dispose()


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:  # --help (0) or a usage error (2)
        return int(exc.code or 0) if isinstance(exc.code, int) else EXIT_USAGE
    if args.flatten and not args.confirm_flatten:
        print(
            "--flatten closes every position at MARKET: run again with --flatten --confirm-flatten",
            file=sys.stderr,
        )
        return EXIT_USAGE
    if args.confirm_flatten and not args.flatten:
        print("--confirm-flatten only confirms --flatten", file=sys.stderr)
        return EXIT_USAGE
    if not args.reason.strip() or args.wait_s < 0:
        print("--reason must not be empty and --wait-s must be >= 0", file=sys.stderr)
        return EXIT_USAGE
    return asyncio.run(run(args))


def cli() -> int:
    """Console entry point `hdt-kill`."""
    configure_logging(ACTOR, stream=sys.stderr)  # stdout carries the SAFE report only
    return main()


if __name__ == "__main__":
    sys.exit(cli())
