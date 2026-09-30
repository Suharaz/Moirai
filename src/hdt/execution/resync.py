"""Re-sync on every user-stream (re)connect (phase 09 section 5).

Even a sub-30 s drop can hide a fill, so every (re)connect runs the same sequence:
1. `RESYNCING`: `exec_accounts.sync_state = resyncing`, `Namespace.synced = False`. Every
   exposure-increasing action (entry, IOC fallback, hedge increase) is refused and `AccountState` is not
   emitted, so Risk's 30 s staleness veto blocks new OPENs as well;
2. REST snapshot: `positionRisk`, `openOrders`, `openAlgoOrders`, orders the ledger thinks are open are
   looked up by client id, `userTrades` from the last seen trade id, income since the anchor;
3. the diff is applied to the ledger (fills are idempotent by trade id) and `stop_guard` runs;
4. the push events buffered since the connection opened are applied (`drain`, each event takes the lock);
5. only then `synced`, so intents and timers never act on a ledger missing post-snapshot fills.

Steps 2-3 are exactly one reconciler cycle (a mismatch still found after catching up kills the namespace,
cause `reconcile`, once the previous or the next cycle sees it too). Push events that arrive meanwhile are
buffered by the user stream and applied after the snapshot, which is safe because every ledger write is
idempotent.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable

from hdt.execution.reconciler import Reconciler, ReconcileReport
from hdt.execution.runtime import Namespace

log = logging.getLogger(__name__)


class Resync:
    def __init__(self, ns: Namespace, reconciler: Reconciler) -> None:
        self.ns = ns
        self.reconciler = reconciler

    def begin(self) -> None:
        """Enter RESYNCING (callable without the lock: it only closes the gate)."""
        self.ns.synced = False
        self.ns.ledger.set_sync_state("resyncing")

    async def run(self, drain: Callable[[], Awaitable[None]] | None = None) -> ReconcileReport:
        """One RESYNC. `drain` applies the events buffered meanwhile; it runs without the namespace lock
        (the event sink takes it per event) but before the gate opens."""
        ns = self.ns
        self.begin()
        async with ns.lock:
            report = await self.reconciler.run_once()
        if drain is not None:
            await drain()
        async with ns.lock:
            ns.synced = True
            ns.ledger.set_sync_state("synced")
        log.info(
            "namespace re-synced",
            extra={"account": ns.name, "clean": report.clean, "stop_invariant_ok": report.stop_invariant_ok},
        )
        ns.notify()
        return report
