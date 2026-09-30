"""Execution service: `python -m hdt.execution.main`.

Env: HDT_PG_DSN (role hdt_execution), HDT_REDIS_URL (ACL user execution), HDT_ORDER_VERIFY_KEYS (trusted
Ed25519 public keys with the namespaces each may sign for), HDT_SCOPE_PRIVATE_KEY_FILE (vault scope `exec`:
the Binance testnet and live keys; only a Binance namespace and the console key tests need it),
HDT_METRICS_PORT (default 9464); each also as `<NAME>_FILE`. The lake is mounted read-only (paper venue,
point-in-time universe). Execution is the only holder of a Binance key and the only caller of signed
endpoints.

Per enabled namespace (pinned run mode: always `paper`, plus `testnet` or `live`), attached and detached
live when the mode changes; a namespace the mode no longer enables stays attached while its ledger holds a
position or a working order (STOPs, exits, hedge and reconcile keep running; Risk refuses new OPENs and
execution rejects new entry legs) and detaches once flat:
- adapter: `PaperAdapter` over the recorded lake, or `BinanceAdapter` on demo-fapi / fapi with the vault
  key. One-way mode is checked first: a Hedge Mode account is refused (critical alert, not started);
- the first RESYNC (reconcile + STOP invariant) runs before any intent is read: directly for paper, on
  every user-stream (re)connect for Binance;
- `orders` consumer (`exec-{account}`) -> `Intake`, started once the namespace is synced;
- the Binance user data stream or the paper venue event loop; the AccountState publisher; the reconciler
  every `reconcile_interval_s` (`MISMATCH_RECHECK_S` after a mismatch seen once); order manager timers
  (entry watchdog, trailing, time stops, hard vetoes, rate-limited stop retries) every second;
  `KillSwitch.enforce` every second, so a kill row written by `hdt-kill` while Redis is down still runs
  the kill sequence; metrics.
Service-wide: config watcher (`config-execution`: risk, binance, mode), `risk_flags` reader
(`exec-flags`), `controls` reader (`exec-controls`: pause / resume / kill / flatten), the vault owner worker
of scope `exec` (console key tests; a rotated key re-attaches its namespace with a new listenKey), the
point-in-time universe (allowlist, coin id -> symbol) and the namespace supervisor: every second,
independent of Redis, it stops an IP-banned namespace (the ban end is recorded on `exec_accounts` for
`hdt-kill` and the key test; the `ip_banned` alert resolves when the namespace attaches again),
re-attaches a namespace whose task ended (critical `namespace_error` alert) and follows the run mode. A
stored risk or mode version that breaks the current hard ceilings runs clipped with a critical
`config_invalid` alert instead of stopping the service. SIGINT / SIGTERM drain every loop.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Final

import sqlalchemy as sa
from nacl.public import PrivateKey
from prometheus_client import Gauge, start_http_server
from redis.asyncio import Redis
from sqlalchemy.orm import Session, sessionmaker

from hdt.contracts.common import Account
from hdt.contracts.risk_flags import RiskFlags
from hdt.contracts.streams import Stream
from hdt.core.alerts import alert
from hdt.core.clock import utcnow
from hdt.core.config import RiskFile, StaticConfig, read_env_value, require_env_value, static_config
from hdt.core.logging import configure_logging
from hdt.core.service import every, install_stop_signals, sleep_or_stop
from hdt.core.streams import RawPayload, StreamConsumer, connect_redis
from hdt.db.models.ledger import KILL_STATES, EquitySnapshotRow
from hdt.db.session import make_engine, make_session_factory, transaction
from hdt.execution.account_state import AccountStatePublisher
from hdt.execution.adapter_base import ExchangeAdapter, ExchangeError, IpBannedError, PositionModeError
from hdt.execution.binance_adapter import BinanceAdapter
from hdt.execution.binance_client import IP_BAN_DEFAULT_S
from hdt.execution.intake import Intake
from hdt.execution.key_check import exec_secret_checks
from hdt.execution.kill_switch import KillSwitch
from hdt.execution.ledger import Ledger
from hdt.execution.namespaces import BINANCE_SECRET, enabled_accounts, exec_group, has_exposure
from hdt.execution.order_manager import OrderManager
from hdt.execution.paper import PaperAdapter
from hdt.execution.reconciler import Reconciler, ReconcileReport
from hdt.execution.resync import Resync
from hdt.execution.runtime import Namespace
from hdt.execution.stop_guard import StopGuard
from hdt.execution.user_stream import UserStream
from hdt.execution.venues import NamespaceUnavailableError, exec_vault, open_adapter
from hdt.execution.verify import Keyring, KeyringError
from hdt.lake.pit_query import PitQuery
from hdt.lake.universe import load_universe
from hdt.ops.metrics import (
    ACCOUNT_DAILY_PNL_FRACTION,
    ACCOUNT_DRAWDOWN,
    ACCOUNT_EQUITY,
    KILL_STATE,
    OPEN_POSITIONS,
    RECONCILE_MISMATCHES,
)
from hdt.risk.limits import daily_pnl_fraction
from hdt.risk.pinned import ConfigProblem, alert_config_problem, pinned_config
from hdt.risk.veto import FlagsBook
from hdt.settings.events import ConfigWatcher, ControlCommand
from hdt.settings.schemas import Section
from hdt.vault.loader import SecretLoader
from hdt.vault.owner import OwnerWorker

log = logging.getLogger(__name__)

SERVICE: Final[str] = "execution"
CONSUMER: Final[str] = "execution"
DEFAULT_METRICS_PORT: Final[str] = "9464"
FLAGS_GROUP: Final[str] = "exec-flags"
CONTROLS_GROUP: Final[str] = "exec-controls"
TICK_INTERVAL_S: Final[float] = 1.0
ENFORCE_INTERVAL_S: Final[float] = 1.0
METRICS_INTERVAL_S: Final[float] = 5.0
SYNC_POLL_S: Final[float] = 0.5
UNIVERSE_REFRESH_S: Final[float] = 300.0
KEY_ROTATION_CHECK_S: Final[float] = 60.0
SUPERVISE_INTERVAL_S: Final[float] = 1.0
ATTACH_RETRY: Final[timedelta] = timedelta(seconds=30)
IP_BAN_FALLBACK: Final[timedelta] = timedelta(seconds=IP_BAN_DEFAULT_S)
"""Stop time after an HTTP 418 whose ban end is unknown (the client's default without `Retry-After`)."""
CONTROL_MAX_AGE: Final[timedelta] = timedelta(minutes=10)

# Account metrics come from the phase 10 catalog (`hdt.ops.metrics`), shared with its dashboards and rules.
NAMESPACE_SYNCED = Gauge(
    "hdt_exec_namespace_synced", "1 while the namespace is synced (0 while RESYNCING)", ["account"]
)
NAMESPACE_ATTACHED = Gauge("hdt_exec_namespace_attached", "1 while the namespace runs", ["account"])


# ---------------------------------------------------------------------------- one namespace


@dataclass
class NamespaceRuntime:
    """The execution components of one namespace, wired to one adapter."""

    ns: Namespace
    guard: StopGuard
    manager: OrderManager
    kill: KillSwitch
    reconciler: Reconciler
    resync: Resync
    intake: Intake
    publisher: AccountStatePublisher

    async def reconcile_once(self) -> ReconcileReport:
        async with self.ns.lock:
            return await self.reconciler.run_once()

    async def tick_once(self) -> None:
        async with self.ns.lock:
            await self.manager.tick(self.ns.clock())


def build_namespace(
    account: Account,
    adapter: ExchangeAdapter,
    *,
    sessions: sessionmaker[Session],
    risk: Callable[[], RiskFile],
    keyring: Keyring,
    redis: Redis | None,
    allowlist: Callable[[], frozenset[str]] = lambda: frozenset(),
    coin_symbol: Callable[[int], str | None] = lambda _c: None,
    clock: Callable[[], datetime] = utcnow,
    run_mode_enabled: Callable[[], bool] = lambda: True,
    account_state_maxlen: int | None = None,
    flags: FlagsBook | None = None,
) -> NamespaceRuntime:
    """Namespace + stop guard + order manager + kill switch + reconciler + re-sync + intake + publisher."""
    if adapter.account is not account:
        raise ValueError(f"adapter of {adapter.account.value} cannot serve namespace {account.value}")
    ns = Namespace(
        account=account,
        adapter=adapter,
        ledger=Ledger(sessions, account),
        sessions=sessions,
        risk=risk,
        allowlist=allowlist,
        coin_symbol=coin_symbol,
        clock=clock,
        run_mode_enabled=run_mode_enabled,
    )
    if flags is not None:
        ns.flags = flags
    guard = StopGuard(ns)
    manager = OrderManager(ns, guard)
    kill = KillSwitch(ns, manager, guard)
    reconciler = Reconciler(ns, manager, guard, kill)
    return NamespaceRuntime(
        ns=ns,
        guard=guard,
        manager=manager,
        kill=kill,
        reconciler=reconciler,
        resync=Resync(ns, reconciler),
        intake=Intake(ns, keyring, manager),
        publisher=AccountStatePublisher(
            ns,
            redis,
            kill,
            interval_s=float(risk().account_state_interval_s),
            maxlen=account_state_maxlen,
        ),
    )


# ---------------------------------------------------------------------------- service parts


class UniverseSymbols:
    """The point-in-time universe: allowlist of `paper` / `live` and the coin id -> Binance symbol map."""

    def __init__(self, pit: PitQuery, clock: Callable[[], datetime] = utcnow) -> None:
        self.pit = pit
        self.clock = clock
        self._symbols: frozenset[str] = frozenset()
        self._by_coin: dict[int, str] = {}

    async def refresh(self) -> bool:
        universe = await asyncio.to_thread(load_universe, self.pit, self.clock())
        if universe is None:
            log.warning("no point-in-time universe in the lake: the allowlist stays as it was")
            return False
        self._symbols = frozenset(m.binance_symbol for m in universe.members)
        self._by_coin = {m.cmc_id: m.binance_symbol for m in universe.members}
        return True

    def symbols(self) -> frozenset[str]:
        return self._symbols

    def coin_symbol(self, coin_id: int) -> str | None:
        return self._by_coin.get(coin_id)


@dataclass
class Attached:
    rt: NamespaceRuntime
    stop: asyncio.Event = field(default_factory=asyncio.Event)
    tasks: list[asyncio.Task[None]] = field(default_factory=list)
    peak_equity: Decimal | None = None
    started_at: datetime = field(default_factory=utcnow)


async def _after_interval(
    stop: asyncio.Event, interval_s: Callable[[], float], fn: Callable[[], Awaitable[object]], what: str
) -> None:
    """`every`, except that the first run waits one interval (the attach sequence has just done it)."""
    if await sleep_or_stop(stop, interval_s()):
        return
    await every(stop, interval_s, fn, what)


def _peak_equity(sessions: sessionmaker[Session], account: Account) -> Decimal | None:
    with sessions() as s:
        value = s.scalar(
            sa.select(sa.func.max(EquitySnapshotRow.equity)).where(EquitySnapshotRow.account == account.value)
        )
    return Decimal(value) if value is not None else None


class ExecutionService:
    def __init__(
        self,
        *,
        static: StaticConfig,
        sessions: sessionmaker[Session],
        redis: Redis,
        keyring: Keyring,
        pit: PitQuery,
        vault: SecretLoader | None,
        owner_key: PrivateKey | None,
        consumer_name: str = CONSUMER,
    ) -> None:
        self.static = static
        self.sessions = sessions
        self.redis = redis
        self.keyring = keyring
        self.pit = pit
        self.vault = vault
        self.owner_key = owner_key
        self.consumer_name = consumer_name
        streams = static.settings.streams
        self.block_ms = streams.block_ms
        self.batch_size = streams.batch_size
        self.reclaim_idle_ms = streams.reclaim_idle_ms
        self.account_state_maxlen = streams.maxlen.get(Stream.ACCOUNT_STATE.value)
        self.flags = FlagsBook.empty()
        self.universe = UniverseSymbols(pit)
        self.watcher = ConfigWatcher(
            redis=redis,
            session_factory=sessions,
            service=SERVICE,
            consumer=consumer_name,
            sections=(Section.RISK, Section.BINANCE, Section.MODE),
            block_ms=self.block_ms,
            reclaim_idle_ms=self.reclaim_idle_ms,
        )
        self.attached: dict[Account, Attached] = {}
        self.refused: dict[Account, str] = {}
        self._refused_key: dict[Account, int | None] = {}  # active key version when it was refused
        self._retry_at: dict[Account, datetime] = {}
        self._crashed: dict[Account, Attached] = {}  # a task of the namespace ended while it should run
        self._held: set[Account] = set()  # attached only for its exposure (the run mode left it)
        self._lifecycle = asyncio.Lock()  # attach / detach / re-attach one at a time
        self._risk: RiskFile = static.risk
        self._mode: Account = Account.PAPER
        self._config_alerted: set[str] = set()  # episodes of clipped stored versions already alerted

    # ------------------------------------------------------------------ config
    def risk(self) -> RiskFile:
        return self._risk

    def mode(self) -> Account:
        return self._mode

    def _load_config(self) -> None:
        """Swap pending versions in (between loop iterations) and re-derive the risk file and run mode. A
        stored version outside the current hard ceilings never stops the service: it runs clipped, with
        one critical alert per version."""
        self.watcher.apply_pending()
        pinned = pinned_config(self.watcher.pin(), self.static)
        self._risk = pinned.risk
        self._mode = pinned.mode
        for problem in pinned.problems:
            if problem.episode not in self._config_alerted and self._alert_config(problem):
                self._config_alerted.add(problem.episode)

    def _alert_config(self, problem: ConfigProblem) -> bool:
        """The `config_invalid` alert of one clipped stored version; False when it could not be written
        (retried on the next config load)."""
        log.critical(
            "stored config version breaks the hard ceilings, running clipped",
            extra={"section": problem.section.value, "version_id": problem.version_id},
        )
        try:
            with transaction(self.sessions) as s:
                alert_config_problem(s, problem, service=SERVICE)
        except Exception:
            log.exception("config alert could not be written", extra={"section": problem.section.value})
            return False
        return True

    async def watch_config(self) -> None:
        """Config poll (Redis); the namespaces follow the new mode within a second (`sync_namespaces`)."""
        await self.watcher.poll_once()
        self._load_config()

    # ------------------------------------------------------------------ namespaces
    def _adapter(self, account: Account) -> ExchangeAdapter:
        return open_adapter(
            account,
            sessions=self.sessions,
            pit=self.pit,
            risk=self.risk(),
            binance=self.static.binance,
            vault=self.vault,
        )

    async def attach(self, account: Account) -> None:
        if account in self.attached or account in self.refused:
            return
        retry_at = self._retry_at.get(account)
        if retry_at is not None and utcnow() < retry_at:
            return
        if account in BINANCE_SECRET:  # a ban recorded by any process (hdt-kill, a previous run, ...)
            banned = await asyncio.to_thread(Ledger(self.sessions, account).banned_until)
            if banned is not None:
                self._retry_at[account] = banned
                log.warning(
                    "exchange IP ban recorded, namespace not started",
                    extra={"account": account.value, "until": banned.isoformat()},
                )
                return
        try:
            att = await self._start_namespace(account)
        except IpBannedError as exc:
            self._banned(account, exc.banned_until)
            return
        except NamespaceUnavailableError as exc:
            self.refused[account] = str(exc)
            self._refused_key[account] = self._key_version(account)
            log.error("namespace refused", extra={"account": account.value, "why": str(exc)})
            self._alert(account, f"{account.value} namespace refused: {exc}")
            return
        except (ExchangeError, OSError) as exc:
            self._retry_at[account] = utcnow() + ATTACH_RETRY
            log.warning(
                "namespace could not start, retrying",
                extra={"account": account.value, "error": type(exc).__name__},
            )
            return
        self._retry_at.pop(account, None)
        self.attached[account] = att
        self._supervise_tasks(account, att)
        att.rt.ns.resolve("namespace_error")
        att.rt.ns.resolve("ip_banned")  # attached again: a later ban pages again
        NAMESPACE_ATTACHED.labels(account.value).set(1)
        log.info(
            "namespace attached",
            extra={"account": account.value, "group": exec_group(account), "stream": Stream.ORDERS.value},
        )

    def _supervise_tasks(self, account: Account, att: Attached) -> None:
        """A namespace task that ends while the namespace should run (an exception, or a loop that
        returned) alerts at once; `sync_namespaces` then re-attaches the namespace."""

        def done(task: asyncio.Task[None]) -> None:
            if att.stop.is_set() or self.attached.get(account) is not att:
                return  # detaching
            exc = None if task.cancelled() else task.exception()
            ended = "cancelled" if task.cancelled() else type(exc).__name__ if exc else "returned"
            log.error(
                "namespace task ended unexpectedly",
                exc_info=exc,
                extra={"account": account.value, "task": task.get_name(), "ended": ended},
            )
            self._crashed[account] = att
            self._alert(account, f"{account.value}: task {task.get_name()} ended ({ended}); re-attaching")

        for task in att.tasks:
            task.add_done_callback(done)

    def _alert(self, account: Account, title: str) -> None:
        try:
            with transaction(self.sessions) as s:
                alert(
                    s,
                    kind="namespace_error",
                    severity="critical",
                    title=title,
                    account=account.value,
                    service=SERVICE,
                )
        except Exception:
            log.exception("alert could not be written", extra={"account": account.value})

    async def _start_namespace(self, account: Account) -> Attached:
        adapter = self._adapter(account)
        try:
            try:
                await adapter.ensure_one_way()
            except PositionModeError as exc:
                raise NamespaceUnavailableError(str(exc)) from exc
            allowlist: Callable[[], frozenset[str]] = self.universe.symbols
            if account is Account.TESTNET:
                # Testnet trades what the demo exchange lists: council decisions (Risk only signs universe
                # coins) and the synthetic driver's symbol, which need not be in the mainnet universe.
                listed = await adapter.exchange_filters()
                tradable = frozenset(s for s, f in listed.items() if f.trading)

                def testnet_symbols() -> frozenset[str]:
                    return tradable

                allowlist = testnet_symbols
            rt = build_namespace(
                account,
                adapter,
                sessions=self.sessions,
                risk=self.risk,
                keyring=self.keyring,
                redis=self.redis,
                allowlist=allowlist,
                coin_symbol=self.universe.coin_symbol,
                account_state_maxlen=self.account_state_maxlen,
                flags=self.flags,
                run_mode_enabled=lambda: account in enabled_accounts(self.mode()),
            )
            if isinstance(adapter, PaperAdapter):
                await rt.resync.run()  # the paper venue has no connection: sync once, now
        except BaseException:
            await adapter.aclose()
            raise
        att = Attached(rt, peak_equity=_peak_equity(self.sessions, account))
        name = account.value
        stop = att.stop
        orders = StreamConsumer(
            redis=self.redis,
            stream=Stream.ORDERS,
            group=exec_group(account),
            consumer=self.consumer_name,
            model=RawPayload,
            handler=rt.intake.handle,
            batch_size=self.batch_size,
            block_ms=self.block_ms,
            reclaim_idle_ms=self.reclaim_idle_ms,
        )
        if isinstance(adapter, PaperAdapter):
            venue: Coroutine[Any, Any, None] = adapter.run(rt.manager, stop)
        elif isinstance(adapter, BinanceAdapter):
            hosts = self.static.binance.demo if account is Account.TESTNET else self.static.binance.live
            stream = UserStream(
                rt.ns,
                adapter.client,
                hosts.ws_private,
                rt.manager,
                rt.resync,
                keepalive_s=float(self.risk().listen_key_keepalive_s),
            )
            venue = stream.run(stop)
        else:  # pragma: no cover - only two adapter kinds exist
            raise TypeError(f"unsupported adapter {type(adapter).__name__}")
        att.tasks = [
            asyncio.create_task(venue, name=f"venue-{name}"),
            asyncio.create_task(self._orders_when_synced(att, orders), name=f"orders-{name}"),
            asyncio.create_task(rt.publisher.run(stop), name=f"account-state-{name}"),
            asyncio.create_task(
                _after_interval(
                    stop,
                    lambda: rt.reconciler.interval_s(float(self.risk().reconcile_interval_s)),
                    lambda: self._reconcile(att),
                    f"{name} reconcile",
                ),
                name=f"reconcile-{name}",
            ),
            asyncio.create_task(
                every(stop, TICK_INTERVAL_S, rt.tick_once, f"{name} order timers"), name=f"tick-{name}"
            ),
            asyncio.create_task(
                every(stop, ENFORCE_INTERVAL_S, rt.kill.enforce, f"{name} kill state"), name=f"enforce-{name}"
            ),
            asyncio.create_task(
                every(stop, METRICS_INTERVAL_S, lambda: self._metrics(att), f"{name} metrics"),
                name=f"metrics-{name}",
            ),
        ]
        return att

    async def _orders_when_synced(self, att: Attached, consumer: StreamConsumer[RawPayload]) -> None:
        """Intents are read only once the namespace is synced (the first RESYNC has completed)."""
        while not att.rt.ns.synced:
            if await sleep_or_stop(att.stop, SYNC_POLL_S):
                return
        await consumer.start()
        await consumer.run(att.stop)

    async def _reconcile(self, att: Attached) -> None:
        report = await att.rt.reconcile_once()
        if not report.clean:
            RECONCILE_MISMATCHES.labels(att.rt.ns.name).inc()

    async def _metrics(self, att: Attached) -> None:
        ns = att.rt.ns
        name = ns.name
        kill = await asyncio.to_thread(ns.ledger.kill_state)
        current = kill.state if kill is not None else "running"
        for state in KILL_STATES:
            KILL_STATE.labels(name, state).set(1 if state == current else 0)
        NAMESPACE_SYNCED.labels(name).set(1 if ns.synced else 0)
        last = att.rt.publisher.last
        if last is None:
            return
        att.peak_equity = last.equity if att.peak_equity is None else max(att.peak_equity, last.equity)
        ACCOUNT_EQUITY.labels(name).set(float(last.equity))
        ACCOUNT_DAILY_PNL_FRACTION.labels(name).set(daily_pnl_fraction(last.equity, last.day_start_equity))
        drawdown = float(last.equity / att.peak_equity - 1) if att.peak_equity > 0 else 0.0
        ACCOUNT_DRAWDOWN.labels(name).set(min(0.0, drawdown))
        OPEN_POSITIONS.labels(name).set(len(last.positions))

    async def detach(self, account: Account) -> None:
        att = self.attached.pop(account, None)
        if att is None:
            return
        att.stop.set()
        await asyncio.gather(*att.tasks, return_exceptions=True)
        await att.rt.ns.adapter.aclose()
        NAMESPACE_ATTACHED.labels(account.value).set(0)
        NAMESPACE_SYNCED.labels(account.value).set(0)
        log.info("namespace detached", extra={"account": account.value})

    def _banned(self, account: Account, until: float | None) -> None:
        """HTTP 418: the namespace stays stopped until the ban ends (no request may extend it) and alerts.
        The ban end is recorded on `exec_accounts` so `hdt-kill` and the key test honour it too."""
        end = datetime.fromtimestamp(until, UTC) if until is not None else utcnow() + IP_BAN_FALLBACK
        self._retry_at[account] = max(end, self._retry_at.get(account, end))
        log.error("IP banned by the exchange", extra={"account": account.value, "until": end.isoformat()})
        try:
            Ledger(self.sessions, account).record_ban(end)
        except Exception:
            log.exception("IP ban could not be recorded", extra={"account": account.value})
        try:
            with transaction(self.sessions) as s:
                alert(
                    s,
                    kind="ip_banned",
                    severity="critical",
                    title=f"{account.value}: IP ban (HTTP 418) until {end.isoformat()}; namespace stopped",
                    account=account.value,
                    service=SERVICE,
                )
        except Exception:
            log.exception("alert could not be written", extra={"account": account.value})

    async def stop_banned(self) -> None:
        """Stop every attached exchange namespace that is IP banned (seen by its own client, or recorded
        by another process such as `hdt-kill`); it re-attaches after the ban."""
        for account in sorted(self.attached, key=lambda a: a.value):
            if account not in BINANCE_SECRET:
                continue  # the paper venue has no exchange to ban it
            ns = self.attached[account].rt.ns
            until = ns.adapter.banned_until()
            if until is None:
                recorded = await asyncio.to_thread(ns.ledger.banned_until)
                until = recorded.timestamp() if recorded is not None else None
            if until is not None:
                self._banned(account, until)
                await self.detach(account)

    async def _restart_crashed(self) -> None:
        """Detach every namespace whose task ended unexpectedly; `sync_namespaces` attaches it again at
        once, or after `ATTACH_RETRY` when it crashed again right after its start."""
        crashed, self._crashed = self._crashed, {}
        for account, att in sorted(crashed.items(), key=lambda item: item[0].value):
            if self.attached.get(account) is not att:
                continue
            await self.detach(account)
            if utcnow() - att.started_at < ATTACH_RETRY:
                self._retry_at[account] = utcnow() + ATTACH_RETRY

    def _has_exposure(self, account: Account) -> bool:
        with self.sessions() as s:
            return has_exposure(s, account)

    def _hold(self, held: set[Account]) -> None:
        for account in sorted(held - self._held, key=lambda a: a.value):
            log.warning(
                "namespace kept attached outside the run mode: it holds positions or working orders",
                extra={"account": account.value, "mode": self.mode().value},
            )
        for account in sorted(self._held - held, key=lambda a: a.value):
            log.info("namespace flat outside the run mode, released", extra={"account": account.value})
        self._held = held

    async def sync_namespaces(self) -> None:
        """Run every second (independent of Redis) and after a config change: stop IP-banned namespaces,
        re-attach crashed ones, attach the run mode's namespaces plus every namespace whose ledger still
        holds a position or a working order (its STOPs, exits and hedge stay managed until it is flat),
        and detach the rest."""
        async with self._lifecycle:
            await self.stop_banned()
            await self._restart_crashed()
            enabled = set(enabled_accounts(self.mode()))
            held = {a for a in Account if a not in enabled and await asyncio.to_thread(self._has_exposure, a)}
            self._hold(held)
            wanted = enabled | held
            for account in sorted(set(self.refused) - wanted, key=lambda a: a.value):
                del self.refused[account]  # a mode change gives a refused namespace a new chance
            for account in sorted(set(self.attached) - wanted, key=lambda a: a.value):
                await self.detach(account)
            for account in sorted(wanted - set(self.attached), key=lambda a: a.value):
                await self.attach(account)

    def _key_version(self, account: Account) -> int | None:
        name = BINANCE_SECRET.get(account)
        return None if self.vault is None or name is None else self.vault.active_version(name)

    async def rotate_keys(self) -> None:
        """Re-attach a Binance namespace whose vault key changed (new client, new listenKey, re-sync); a
        namespace refused since then gets a new chance once another key version is active."""
        if self.vault is None:
            return
        changed = await asyncio.to_thread(self.vault.refresh)
        for account, name in BINANCE_SECRET.items():
            if account in self.refused:
                version = await asyncio.to_thread(self._key_version, account)
                if version != self._refused_key.get(account):
                    log.info("new exchange key for a refused namespace", extra={"account": account.value})
                    del self.refused[account]  # attached again by the next sync_namespaces
                continue
            if name not in changed:
                continue
            if account in self.attached:
                log.info("exchange key rotated, re-attaching", extra={"account": account.value})
                async with self._lifecycle:
                    await self.detach(account)
                    await self.attach(account)

    # ------------------------------------------------------------------ risk_flags / controls
    async def on_flags(self, _msg_id: str, flags: RiskFlags) -> bool:
        # The order manager's timer closes a held position vetoed against its side within a second.
        self.flags.offer(flags)
        return True

    async def on_control(self, _msg_id: str, command: ControlCommand) -> bool:
        age = utcnow() - command.requested_at
        if age > CONTROL_MAX_AGE:
            log.warning(
                "stale control command ignored",
                extra={
                    "command_id": command.command_id,
                    "action": command.action,
                    "age_s": age.total_seconds(),
                },
            )
            return True
        extra = {"account": command.account.value, "action": command.action, "command_id": command.command_id}
        att = self.attached.get(command.account)
        if att is None:
            # The namespace is not running: the row is enforced by KillSwitch.enforce once it runs again
            # (a flatten then runs as a plain kill: nothing can be closed without the namespace).
            ledger = Ledger(self.sessions, command.account)
            current = ledger.kill_state()
            killed = current is not None and current.state == "killed"
            if command.action in ("kill", "flatten"):
                ledger.set_kill_state(
                    "killed", cause="manual", reason=command.reason, command_id=command.command_id
                )
            elif command.action == "pause" and not killed:
                ledger.set_kill_state(
                    "paused", cause="manual", reason=command.reason, command_id=command.command_id
                )
            log.warning("control command for a namespace that is not running", extra=extra)
            return True
        rt = att.rt
        if command.action in ("kill", "flatten"):
            async with rt.ns.lock:
                report = await rt.kill.engage(
                    "manual",
                    command.reason,
                    flatten=command.action == "flatten",
                    command_id=command.command_id,
                    actor=command.requested_by,
                )
            log.warning("kill switch engaged", extra={**extra, "safe": report.safe})
        elif command.action == "pause":
            rt.kill.pause(command.reason, command.command_id)
            log.info("namespace paused", extra=extra)
        else:
            rt.kill.resume(command.command_id)  # a refused resume alerts from the kill switch
        return True

    # ------------------------------------------------------------------ run
    async def run(self, stop: asyncio.Event) -> None:
        await self.watcher.start()
        self._load_config()
        await self.universe.refresh()
        streams = self.static.settings.streams
        readers: list[StreamConsumer[RiskFlags] | StreamConsumer[ControlCommand]] = [
            StreamConsumer(
                redis=self.redis,
                stream=Stream.RISK_FLAGS,
                group=FLAGS_GROUP,
                consumer=self.consumer_name,
                model=RiskFlags,
                handler=self.on_flags,
                batch_size=streams.batch_size,
                block_ms=streams.block_ms,
                reclaim_idle_ms=streams.reclaim_idle_ms,
            ),
            StreamConsumer(
                redis=self.redis,
                stream=Stream.CONTROLS,
                group=CONTROLS_GROUP,
                consumer=self.consumer_name,
                model=ControlCommand,
                handler=self.on_control,
                batch_size=streams.batch_size,
                block_ms=min(streams.block_ms, 1000),
                reclaim_idle_ms=streams.reclaim_idle_ms,
            ),
        ]
        for reader in readers:
            await reader.start()
            log.info("stream reader attached", extra={"stream": str(reader.stream), "group": reader.group})
        await self.sync_namespaces()
        tasks = [asyncio.create_task(reader.run(stop), name=f"read-{reader.group}") for reader in readers]
        tasks += [
            asyncio.create_task(every(stop, 1.0, self.watch_config, "config watcher"), name="config"),
            asyncio.create_task(
                every(stop, SUPERVISE_INTERVAL_S, self.sync_namespaces, "namespace supervisor"),
                name="namespaces",
            ),
            asyncio.create_task(
                _after_interval(stop, lambda: UNIVERSE_REFRESH_S, self.universe.refresh, "universe refresh"),
                name="universe",
            ),
            asyncio.create_task(
                _after_interval(stop, lambda: KEY_ROTATION_CHECK_S, self.rotate_keys, "key rotation"),
                name="key-rotation",
            ),
        ]
        if self.owner_key is not None:
            owner = OwnerWorker(
                scope="exec",
                redis=self.redis,
                session_factory=self.sessions,
                private_key=self.owner_key,
                checks=exec_secret_checks(self.sessions),
                consumer=f"{SERVICE}-1",
                block_ms=streams.block_ms,
                reclaim_idle_ms=streams.reclaim_idle_ms,
                result_maxlen=streams.maxlen.get(Stream.SECRET_TEST_RESULT.value),
            )
            tasks.append(asyncio.create_task(owner.run(stop), name="vault-owner"))
        try:
            await stop.wait()
        finally:
            stop.set()
            # Service loops first: the config watcher must not re-attach a namespace being detached.
            await asyncio.gather(*tasks, return_exceptions=True)
            for account in list(self.attached):
                await self.detach(account)
        log.info("execution service stopped")


async def amain() -> int:
    static = static_config()
    try:
        keyring = Keyring.from_env()
    except KeyringError as exc:
        log.error("execution cannot start: %s", exc)
        return 2
    engine = make_engine()
    sessions = make_session_factory(engine)
    redis: Redis = connect_redis(require_env_value("HDT_REDIS_URL"))
    data = static.settings.data
    pit = PitQuery(Path(data.staging_root), Path(data.lake_root))
    vault, owner_key = exec_vault(sessions)
    start_http_server(int(read_env_value("HDT_METRICS_PORT") or DEFAULT_METRICS_PORT))
    stop = asyncio.Event()
    install_stop_signals(stop)
    service = ExecutionService(
        static=static,
        sessions=sessions,
        redis=redis,
        keyring=keyring,
        pit=pit,
        vault=vault,
        owner_key=owner_key,
    )
    log.info("execution service starting", extra={"trusted_keys": sorted(keyring.keys)})
    try:
        await service.run(stop)
    finally:
        await redis.aclose()
        engine.dispose()
    return 0


def main() -> None:
    configure_logging(SERVICE)
    sys.exit(asyncio.run(amain()))


if __name__ == "__main__":
    main()
