"""Risk service: `python -m hdt.risk.main`.

Env: HDT_PG_DSN (role hdt_risk), HDT_REDIS_URL (ACL user risk), HDT_ORDER_SIGNING_KEY (the Ed25519 signing
key, mounted only here), HDT_METRICS_PORT (default 9464); each also as `<NAME>_FILE`. The lake is mounted
read-only. Risk holds no exchange key and never calls a signed endpoint.

Tasks:
- config watcher (`config-risk`): risk, binance and mode sections, applied at event boundaries (a stored
  version outside the hard ceilings runs clipped with one critical `config_invalid` alert per version,
  `hdt.risk.pinned`); the pinned run mode decides the decision namespaces (`paper`, plus `live` once the
  live gate opens) and a mode change attaches or detaches their consumers without a restart. A namespace
  the mode no longer enables stays attached while its ledger holds a position or a working order
  (`has_exposure`): EXIT/HOLD, veto exits and the hedge book keep running, every OPEN is rejected
  (`mode_disabled`), and it detaches once flat;
- per decision namespace: the `decisions` consumer (`risk-{account}`; a re-attach acks unread, with XACK
  only, the decisions that expired while detached), the outbox relay and the BTC hedge book rebalancer;
- `account_state` reader (`risk-state`, seeded from the stream tail) and `risk_flags` reader
  (`risk-flags`, seeded likewise): a new flag that vetoes a held position's side signs an EXIT; a fresh
  AccountState resolves the namespace's open `account_state_stale` alert;
- CMC WebSocket price vs Binance mark cross-check of held coins: a deviation above
  `price_crosscheck_max_dev` alerts (`price_crosscheck`, one per account, symbol and UTC date), it never
  vetoes or kills (CMC-degraded mode);
- Prometheus metrics; SIGINT / SIGTERM drain every loop and close connections.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Final

from prometheus_client import Gauge, start_http_server
from pydantic import ValidationError
from redis.asyncio import Redis
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from hdt.contracts.account import AccountState
from hdt.contracts.common import Account
from hdt.contracts.risk_flags import RiskFlags
from hdt.contracts.streams import Stream
from hdt.core.alerts import AlertKind, alert, resolve
from hdt.core.clock import utcnow
from hdt.core.config import StaticConfig, read_env_value, require_env_value, static_config
from hdt.core.logging import configure_logging
from hdt.core.service import every, install_stop_signals
from hdt.core.streams import StreamConsumer, connect_redis, ensure_group
from hdt.db.session import make_engine, make_session_factory, transaction
from hdt.execution.namespaces import DECISION_ACCOUNTS, decision_accounts, has_exposure, risk_group
from hdt.lake.pit_query import PitQuery
from hdt.risk.consumer import (
    AccountStateBook,
    DecisionConsumer,
    IntentOutbox,
    LakeMarket,
    RiskRuntime,
    VetoExits,
    stream_consumer,
)
from hdt.risk.hedger import HedgeRebalancer
from hdt.risk.pinned import ConfigProblem, PinnedConfig, alert_config_problem, pinned_config
from hdt.risk.signer import IntentSigner, SigningKeyError
from hdt.risk.veto import FlagsBook
from hdt.settings.events import ConfigWatcher
from hdt.settings.schemas import Section
from hdt.settings.versions import ConfigPin

log = logging.getLogger(__name__)

SERVICE: Final[str] = "risk"
CONSUMER: Final[str] = "risk"
DEFAULT_METRICS_PORT: Final[str] = "9464"
STATE_GROUP: Final[str] = "risk-state"
FLAGS_GROUP: Final[str] = "risk-flags"
SEED_STATE_COUNT: Final[int] = 500
SEED_FLAGS_COUNT: Final[int] = 5000
RELAY_INTERVAL_S: Final[float] = 5.0
CROSSCHECK_INTERVAL_S: Final[float] = 30.0
CROSSCHECK_WINDOW: Final[timedelta] = timedelta(seconds=120)
CROSSCHECK_REALERT: Final[timedelta] = timedelta(minutes=15)
CMC_SOURCE: Final[str] = "cmc"
CMC_WS_ROUTE: Final[str] = "ws_crypto_latest_price"

STATE_AGE = Gauge("hdt_risk_account_state_age_seconds", "Age of the latest AccountState seen", ["account"])
ATTACHED = Gauge("hdt_risk_namespace_attached", "1 while the namespace's decision consumer runs", ["account"])


def cmc_prices(pit: PitQuery, now: datetime, window: timedelta = CROSSCHECK_WINDOW) -> dict[int, Decimal]:
    """Newest CMC WebSocket price per CMC id within `window` (frames `{"type": "data", "data": {cid, p}}`)."""
    out: dict[int, Decimal] = {}
    for record in reversed(pit.series(CMC_SOURCE, CMC_WS_ROUTE, now - window, now, as_of=now)):
        try:
            items = json.loads(record.body())
        except ValueError:
            continue
        for item in reversed(items) if isinstance(items, list) else []:
            frame = item.get("frame") if isinstance(item, dict) else None
            data = frame.get("data") if isinstance(frame, dict) and frame.get("type") == "data" else None
            if not isinstance(data, dict):
                continue
            cid, price = data.get("cid"), data.get("p")
            if isinstance(cid, int) and isinstance(price, int | float) and price > 0 and cid not in out:
                out[cid] = Decimal(repr(float(price)))
    return out


def deviation(cmc_price: Decimal, multiplier: int, mark: Decimal) -> float:
    """Relative gap between CMC's unit price scaled to the contract (x multiplier) and the Binance mark."""
    if mark <= 0:
        return 0.0
    return float(abs(cmc_price * multiplier - mark) / mark)


def _alert_config_problem(sessions: sessionmaker[Session], problem: ConfigProblem) -> bool:
    """Raise the `config_invalid` alert of one clipped stored version; False while Postgres is unavailable."""
    try:
        with transaction(sessions) as s:
            alert_config_problem(s, problem, service=SERVICE)
    except SQLAlchemyError as exc:
        log.warning("config_invalid alert not written", extra={"error": type(exc).__name__})
        return False
    return True


@dataclass
class NamespaceTasks:
    account: Account
    consumer: DecisionConsumer
    veto: VetoExits
    hedger: HedgeRebalancer
    stop: asyncio.Event = field(default_factory=asyncio.Event)
    tasks: list[asyncio.Task[None]] = field(default_factory=list)


class RiskService:
    def __init__(
        self,
        *,
        static: StaticConfig,
        sessions: sessionmaker[Session],
        redis: Redis,
        signer: IntentSigner,
        pit: PitQuery,
        consumer_name: str = CONSUMER,
    ) -> None:
        self.static = static
        self.sessions = sessions
        self.redis = redis
        self.signer = signer
        self.pit = pit
        self.consumer_name = consumer_name
        streams = static.settings.streams
        self.block_ms = streams.block_ms
        self.batch_size = streams.batch_size
        self.reclaim_idle_ms = streams.reclaim_idle_ms
        self.orders_maxlen = streams.maxlen.get(Stream.ORDERS.value)
        self.market = LakeMarket(
            pit,
            btc_cmc_symbol=static.indicators.reserves.btc_symbol,
            window_s=float(static.settings.data.stale_s),
        )
        self.flags = FlagsBook.empty()
        self.states = AccountStateBook()
        self.watcher = ConfigWatcher(
            redis=redis,
            session_factory=sessions,
            service=SERVICE,
            consumer=consumer_name,
            sections=(Section.RISK, Section.BINANCE, Section.MODE),
            block_ms=self.block_ms,
            reclaim_idle_ms=self.reclaim_idle_ms,
        )
        self.namespaces: dict[Account, NamespaceTasks] = {}
        self.winding_down: frozenset[Account] = frozenset()
        self._alerted: dict[tuple[Account, str], datetime] = {}
        self._config_alerted: set[str] = set()  # episodes of clipped stored versions already alerted

    # ------------------------------------------------------------------ config
    def _pinned(self) -> tuple[ConfigPin, PinnedConfig]:
        """Swap pending versions in (event boundary) and derive the risk file and run mode; a stored version
        outside the hard ceilings runs clipped and raises one critical alert per version."""
        self.watcher.apply_pending()
        pin = self.watcher.pin()
        pinned = pinned_config(pin, self.static)
        for problem in pinned.problems:
            if problem.episode not in self._config_alerted and _alert_config_problem(self.sessions, problem):
                self._config_alerted.add(problem.episode)
        return pin, pinned

    def runtime(self) -> RiskRuntime:
        """The pinned config for the next event (pending versions are swapped in at this boundary)."""
        pin, pinned = self._pinned()
        return RiskRuntime(
            pinned.risk, float(self.static.settings.data.stale_s), pin.version_ids(), pinned.mode
        )

    def mode(self) -> Account:
        return self._pinned()[1].mode

    # ------------------------------------------------------------------ namespaces
    def _namespace(self, account: Account) -> NamespaceTasks:
        outbox = IntentOutbox(
            account=account, sessions=self.sessions, redis=self.redis, maxlen=self.orders_maxlen
        )
        consumer = DecisionConsumer(
            account=account,
            sessions=self.sessions,
            redis=self.redis,
            signer=self.signer,
            market=self.market,
            flags=self.flags,
            states=self.states,
            runtime=self.runtime,
            outbox=outbox,
        )

        def coin_symbol(coin_id: int, now: datetime) -> str | None:
            member = self.market.member(coin_id, now)
            return member.binance_symbol if member is not None else None

        veto = VetoExits(
            account=account,
            sessions=self.sessions,
            signer=self.signer,
            states=self.states,
            outbox=outbox,
            coin_symbol=coin_symbol,
            runtime=self.runtime,
        )
        hedger = HedgeRebalancer(
            account=account,
            sessions=self.sessions,
            signer=self.signer,
            states=self.states,
            market=self.market,
            outbox=outbox,
            runtime=self.runtime,
        )
        return NamespaceTasks(account, consumer, veto, hedger)

    async def attach(self, account: Account) -> None:
        if account in self.namespaces:
            return
        ns = self._namespace(account)
        stream = stream_consumer(
            ns.consumer,
            name=self.consumer_name,
            block_ms=self.block_ms,
            batch_size=self.batch_size,
            reclaim_idle_ms=self.reclaim_idle_ms,
        )
        await stream.start()
        await ns.consumer.relay_once()  # rows committed before a crash are published first
        for flags in list(self.flags.by_coin.values()):
            await ns.veto.check(flags)
        name = account.value
        ns.tasks = [
            asyncio.create_task(stream.run(ns.stop), name=f"decisions-{name}"),
            asyncio.create_task(
                every(ns.stop, RELAY_INTERVAL_S, ns.consumer.relay_once, f"{name} outbox relay"),
                name=f"relay-{name}",
            ),
            asyncio.create_task(
                every(ns.stop, self._hedge_interval, lambda: self._hedge(ns), f"{name} hedge book"),
                name=f"hedge-{name}",
            ),
        ]
        self.namespaces[account] = ns
        ATTACHED.labels(name).set(1)
        log.info(
            "decision consumer attached",
            extra={"account": name, "group": risk_group(account), "stream": Stream.DECISIONS.value},
        )

    async def detach(self, account: Account) -> None:
        ns = self.namespaces.pop(account, None)
        if ns is None:
            return
        ns.stop.set()
        await asyncio.gather(*ns.tasks, return_exceptions=True)
        ATTACHED.labels(account.value).set(0)
        log.info("decision consumer detached", extra={"account": account.value})

    async def sync_namespaces(self) -> None:
        """Attach the namespaces the run mode enables; one it no longer enables stays attached (OPEN
        rejected) while it has exposure, and detaches once flat with no working order."""
        enabled = frozenset(decision_accounts(self.mode()))
        others = DECISION_ACCOUNTS - enabled
        try:
            exposed = await asyncio.to_thread(self._exposed, others)
        except SQLAlchemyError as exc:
            # Exposure unknown: never detach a namespace that may still hold positions, attach nothing new.
            log.warning("namespace exposure check failed", extra={"error": type(exc).__name__})
            exposed = others & frozenset(self.namespaces)
        if exposed != self.winding_down:
            log.warning(
                "namespaces winding down",
                extra={
                    "accounts": sorted(a.value for a in exposed),
                    "mode_enables": sorted(a.value for a in enabled),
                },
            )
            self.winding_down = exposed
        wanted = enabled | exposed
        for account in sorted(set(self.namespaces) - wanted, key=lambda a: a.value):
            await self.detach(account)
        for account in sorted(wanted - set(self.namespaces), key=lambda a: a.value):
            await self.attach(account)

    def _exposed(self, accounts: frozenset[Account]) -> frozenset[Account]:
        """The namespaces among `accounts` whose ledger holds a position or a working order."""
        if not accounts:
            return frozenset()
        with self.sessions() as s:
            return frozenset(a for a in accounts if has_exposure(s, a))

    def _hedge_interval(self) -> float:
        return float(self.runtime().risk.account_state_interval_s)

    async def _hedge(self, ns: NamespaceTasks) -> None:
        state = self.states.get(ns.account)
        if state is not None:
            STATE_AGE.labels(ns.account.value).set((utcnow() - state.ts).total_seconds())
        await ns.hedger.run_once()

    # ------------------------------------------------------------------ account_state / risk_flags
    async def seed(self) -> None:
        """Latest AccountState per namespace and latest RiskFlags per coin from the stream tails."""
        await ensure_group(self.redis, Stream.ACCOUNT_STATE, STATE_GROUP, start_id="$")
        await ensure_group(self.redis, Stream.RISK_FLAGS, FLAGS_GROUP, start_id="$")
        states = await self._tail(Stream.ACCOUNT_STATE, SEED_STATE_COUNT)
        for data in states:
            try:
                self.states.offer(AccountState.model_validate_json(data))
            except ValidationError:
                continue
        flags = await self._tail(Stream.RISK_FLAGS, SEED_FLAGS_COUNT)
        for data in flags:
            try:
                self.flags.offer(RiskFlags.model_validate_json(data))
            except ValidationError:
                continue
        log.info(
            "risk state seeded",
            extra={"account_states": len(states), "flags": len(self.flags.by_coin)},
        )

    async def _tail(self, stream: Stream, count: int) -> list[str]:
        rows: Any = await self.redis.xrevrange(stream.value, count=count)
        out: list[str] = []
        for _msg_id, raw in rows or []:
            fields = {_s(k): _s(v) for k, v in raw.items()}
            if "data" in fields:
                out.append(fields["data"])
        return out

    async def on_state(self, _msg_id: str, state: AccountState) -> bool:
        if self.states.offer(state):
            await self._resolve_stale(state)
        return True

    async def _resolve_stale(self, state: AccountState) -> None:
        """A fresh AccountState closes the namespace's open `account_state_stale` alert, so the next
        stale episode pages again."""
        account = state.account
        if not self.states.stale_alerted(account):
            return
        if (utcnow() - state.ts).total_seconds() > self.runtime().risk.account_state_max_age_s:
            return
        self.states.clear_stale_alerted(account)  # before the write: a concurrent new alert re-marks
        try:
            await asyncio.to_thread(self._resolve_alert, "account_state_stale", account)
        except SQLAlchemyError as exc:
            self.states.mark_stale_alerted(account)
            log.warning("account_state_stale not resolved", extra={"error": type(exc).__name__})

    def _resolve_alert(self, kind: AlertKind, account: Account) -> None:
        with transaction(self.sessions) as s:
            resolve(s, kind=kind, account=account.value, service=SERVICE)

    async def on_flags(self, _msg_id: str, flags: RiskFlags) -> bool:
        if self.flags.offer(flags):
            for ns in list(self.namespaces.values()):
                await ns.veto.check(flags)
        return True

    # ------------------------------------------------------------------ price cross-check
    async def crosscheck_once(self) -> int:
        """Alert on held coins whose CMC price deviates from the Binance mark; returns alerts raised."""
        now = utcnow()
        limit = self.runtime().risk.price_crosscheck_max_dev
        prices = await asyncio.to_thread(cmc_prices, self.pit, now)
        if not prices:
            return 0
        raised = 0
        for account in list(self.namespaces):
            state = self.states.get(account)
            if state is None:
                continue
            for p in state.positions:
                if p.qty == 0 or p.is_hedge_book:
                    continue
                member = await asyncio.to_thread(self.market.member_by_symbol, p.symbol, now)
                cmc = prices.get(member.cmc_id) if member is not None else None
                if member is None or cmc is None:
                    continue
                gap = deviation(cmc, member.multiplier, p.mark_price)
                last = self._alerted.get((account, p.symbol))
                if gap <= limit or (last is not None and now - last < CROSSCHECK_REALERT):
                    continue
                self._alerted[(account, p.symbol)] = now
                title = (
                    f"{account.value} {p.symbol}: CMC {cmc * member.multiplier} vs Binance mark "
                    f"{p.mark_price} ({gap:.2%}, limit {limit:.2%})"
                )
                await asyncio.to_thread(self._alert, account, p.symbol, title, now)
                raised += 1
        return raised

    def _alert(self, account: Account, symbol: str, title: str, now: datetime) -> None:
        """One `price_crosscheck` alert per (account, symbol, UTC date)."""
        with transaction(self.sessions) as s:
            alert(
                s,
                kind="price_crosscheck",
                severity="warning",
                title=title,
                detail="positions stay managed on Binance data",
                account=account.value,
                service=SERVICE,
                episode=f"{symbol}:{now.date().isoformat()}",
            )

    # ------------------------------------------------------------------ run
    async def watch_config(self) -> None:
        await self.watcher.poll_once()
        await self.sync_namespaces()

    async def run(self, stop: asyncio.Event) -> None:
        await self.watcher.start()
        await self.seed()
        await self.sync_namespaces()
        streams = self.static.settings.streams
        readers: list[StreamConsumer[Any]] = [
            StreamConsumer(
                redis=self.redis,
                stream=Stream.ACCOUNT_STATE,
                group=STATE_GROUP,
                consumer=self.consumer_name,
                model=AccountState,
                handler=self.on_state,
                batch_size=streams.batch_size,
                block_ms=min(streams.block_ms, 1000),
                reclaim_idle_ms=streams.reclaim_idle_ms,
            ),
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
        ]
        for reader in readers:
            log.info("stream reader attached", extra={"stream": str(reader.stream), "group": reader.group})
        tasks = [asyncio.create_task(reader.run(stop), name=f"read-{reader.group}") for reader in readers]
        tasks.append(
            asyncio.create_task(every(stop, 1.0, self.watch_config, "config watcher"), name="config")
        )
        tasks.append(
            asyncio.create_task(
                every(stop, CROSSCHECK_INTERVAL_S, self.crosscheck_once, "price cross-check"),
                name="crosscheck",
            )
        )
        try:
            await stop.wait()
        finally:
            stop.set()
            # Service loops first: the config watcher must not re-attach a namespace being detached.
            await asyncio.gather(*tasks, return_exceptions=True)
            for account in list(self.namespaces):
                await self.detach(account)
        log.info("risk service stopped")


def _s(value: bytes | str) -> str:
    return value.decode("utf-8") if isinstance(value, bytes) else value


async def amain() -> int:
    static = static_config()
    try:
        signer = IntentSigner.from_env()
    except SigningKeyError as exc:
        log.error("risk cannot start: %s", exc)
        return 2
    engine = make_engine()
    sessions = make_session_factory(engine)
    redis: Redis = connect_redis(require_env_value("HDT_REDIS_URL"))
    data = static.settings.data
    pit = PitQuery(Path(data.staging_root), Path(data.lake_root))
    start_http_server(int(read_env_value("HDT_METRICS_PORT") or DEFAULT_METRICS_PORT))
    stop = asyncio.Event()
    install_stop_signals(stop)
    service = RiskService(static=static, sessions=sessions, redis=redis, signer=signer, pit=pit)
    log.info("risk service starting", extra={"key_id": signer.key_id})
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
