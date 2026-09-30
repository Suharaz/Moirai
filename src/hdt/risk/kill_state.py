"""Namespace kill state as Risk sees it (table `kill_state`, owned and written by execution).

Execution is the only writer (kill switch, daily loss, reconcile mismatch, operator controls). Risk only
reads it: an active `paused` or `killed` state rejects every OPEN, while EXIT and HOLD still go through.
A namespace without a row has never been started and is treated as running.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import sqlalchemy as sa
from sqlalchemy.orm import Session

from hdt.contracts.common import Account
from hdt.db.models.ledger import KillStateRow

BLOCKING_STATES = frozenset({"paused", "killed"})


@dataclass(frozen=True)
class KillView:
    state: str
    cause: str | None
    reason: str | None
    since: datetime | None

    @property
    def blocks_open(self) -> bool:
        return self.state in BLOCKING_STATES

    def describe(self) -> str:
        why = f" ({self.cause}: {self.reason})" if self.cause or self.reason else ""
        return f"namespace {self.state}{why}"


RUNNING = KillView("running", None, None, None)


def read_kill_state(session: Session, account: Account) -> KillView:
    row = session.scalars(sa.select(KillStateRow).where(KillStateRow.account == account.value)).first()
    if row is None:
        return RUNNING
    return KillView(row.state, row.cause, row.reason, row.since)
