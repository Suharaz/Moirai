"""Per-namespace runtime shared by the execution components (order manager, stop guard, reconciler, ...).

One `Namespace` per enabled account. Components never share state across namespaces: a kill, a mismatch
or a lost user stream in `testnet` leaves `paper` untouched. `lock` serializes every ledger-changing step
of the namespace (exchange events, intents, reconcile, kill switch) so they apply in a single order.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal

from sqlalchemy.orm import Session, sessionmaker

from hdt.contracts.common import Account, OrderSide
from hdt.contracts.risk_flags import RiskFlags
from hdt.core.alerts import AlertKind, Severity, alert, resolve
from hdt.core.clock import utcnow
from hdt.core.config import RiskFile
from hdt.db.session import transaction
from hdt.execution.adapter_base import Balance, ExchangeAdapter
from hdt.execution.filters import SymbolFilters
from hdt.execution.ledger import Ledger
from hdt.risk.veto import FlagsBook

log = logging.getLogger(__name__)
SERVICE = "execution"


@dataclass
class Namespace:
    account: Account
    adapter: ExchangeAdapter
    ledger: Ledger
    sessions: sessionmaker[Session]
    risk: Callable[[], RiskFile]
    allowlist: Callable[[], frozenset[str]] = lambda: frozenset()
    coin_symbol: Callable[[int], str | None] = lambda _coin: None
    clock: Callable[[], datetime] = utcnow
    run_mode_enabled: Callable[[], bool] = lambda: True
    """False while the run mode no longer enables the namespace (attached only for its exposure)."""
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    filters: dict[str, SymbolFilters] = field(default_factory=dict)
    flags: FlagsBook = field(default_factory=FlagsBook.empty)
    balance: Balance | None = None
    synced: bool = False
    changed: asyncio.Event = field(default_factory=asyncio.Event)

    @property
    def name(self) -> str:
        return self.account.value

    def alert(
        self,
        kind: AlertKind,
        severity: Severity,
        title: str,
        detail: str | None = None,
        *,
        episode: str | None = None,
    ) -> None:
        """Raise an alert in its own transaction (an alert failure never breaks the trading path)."""
        try:
            with transaction(self.sessions) as s:
                alert(
                    s,
                    kind=kind,
                    severity=severity,
                    title=title,
                    detail=detail,
                    account=self.name,
                    service=SERVICE,
                    episode=episode,
                )
        except Exception:
            log.exception("alert could not be written", extra={"alert_kind": kind, "account": self.name})

    def resolve(self, kind: AlertKind, *, episode: str | None = None) -> None:
        """Resolve this namespace's open `kind` alert (same `episode` as raised) on the clearing transition,
        so the next occurrence pages again; in its own transaction, a failure is only logged."""
        try:
            with transaction(self.sessions) as s:
                resolve(s, kind=kind, account=self.name, service=SERVICE, episode=episode)
        except Exception:
            log.exception("alert could not be resolved", extra={"alert_kind": kind, "account": self.name})

    def blocked_reason(self) -> str | None:
        """Why exposure-increasing actions are refused right now (None when allowed)."""
        if not self.synced:
            return "namespace is re-syncing"
        kill = self.ledger.kill_state()
        if kill is not None and kill.state != "running":
            return f"namespace {kill.state} ({kill.cause or 'manual'})"
        return None

    def held_reason(self) -> str | None:
        """Why new positions (entry / entry_ioc legs) are refused although the namespace runs: the run mode
        no longer enables it and it stays attached only until flat (protection, exits and the hedge book
        keep working). None while the run mode enables it."""
        if self.run_mode_enabled():
            return None
        return "namespace held for its exposure only: the run mode no longer enables it"

    def vetoed_sides(self, symbol: str, now: datetime) -> frozenset[OrderSide]:
        """Entry sides blocked by unexpired `RiskFlags` of the coin trading as `symbol`."""
        out: set[OrderSide] = set()
        for flags in self.flags.by_coin.values():
            if now > flags.expires_at or self.coin_symbol(flags.coin_id) != symbol:
                continue
            if flags.veto_long:
                out.add(OrderSide.BUY)
            if flags.veto_short:
                out.add(OrderSide.SELL)
        return frozenset(out)

    def flags_for(self, symbol: str) -> RiskFlags | None:
        for flags in self.flags.by_coin.values():
            if self.coin_symbol(flags.coin_id) == symbol:
                return flags
        return None

    def equity(self) -> Decimal | None:
        return self.balance.equity if self.balance is not None else None

    async def symbol_filters(self, symbol: str) -> SymbolFilters | None:
        if symbol not in self.filters:
            self.filters.update(await self.adapter.exchange_filters())
        return self.filters.get(symbol)

    def notify(self) -> None:
        """Something changed: the AccountState publisher emits without waiting for its interval."""
        self.changed.set()
