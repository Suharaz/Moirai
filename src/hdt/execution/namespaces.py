"""Account namespaces (`paper | testnet | live`) and the run mode that enables them.

The `paper` execution namespace stays attached while the system runs (it measures the strategy for gate G4
whenever it receives the council decisions, i.e. in `paper` and `live` mode). The pinned run mode
(phase 12 `mode` section) adds its own namespace: `testnet` trades council decisions on the Binance demo
account (owner decision 2026-09-29) and also takes the synthetic lifecycle driver's intents, `live` runs
only after the phase 12 live gate let the mode switch to live. Namespaces never share state: each has its
own ledger rows, equity, ceilings, kill state, consumer groups and `AccountState`.

Consumer groups:
- Risk reads `decisions` with `risk-{account}` for the decision targets of the run mode
  (`decision_accounts`): `paper` -> paper; `testnet` -> testnet only (paper gets no new decision, so gate
  G4 accumulates nothing while the mode is testnet); `live` -> paper and live (paper stays the shadow the
  live results are compared with);
- execution reads `orders` with `exec-{account}` for every enabled namespace.

A namespace the run mode no longer enables stays attached (execution and Risk) while `has_exposure` is
true: its positions keep their STOP management, reconciler, EXIT processing and hedger until it is flat
with no working order; Risk refuses new OPEN decisions for it meanwhile.
"""

from __future__ import annotations

from typing import Final

import sqlalchemy as sa
from sqlalchemy.orm import Session

from hdt.contracts.common import Account
from hdt.db.models.ledger import AlgoOrderRow, OrderRow, PositionRow
from hdt.execution.ledger import OPEN_ORDER_STATES, WORKING_ALGO_STATES

DECISION_ACCOUNTS: Final[frozenset[Account]] = frozenset({Account.PAPER, Account.TESTNET, Account.LIVE})
_DECISION_TARGETS: Final[dict[Account, tuple[Account, ...]]] = {
    Account.PAPER: (Account.PAPER,),
    Account.TESTNET: (Account.TESTNET,),
    Account.LIVE: (Account.PAPER, Account.LIVE),
}
BINANCE_SECRET: Final[dict[Account, str]] = {
    Account.TESTNET: "binance_testnet",
    Account.LIVE: "binance_live",
}


def enabled_accounts(mode: Account) -> tuple[Account, ...]:
    """Namespaces enabled by run mode `mode`: always `paper`, plus the mode's own namespace."""
    ordered = [Account.PAPER]
    if mode is not Account.PAPER:
        ordered.append(mode)
    return tuple(ordered)


def decision_accounts(mode: Account) -> tuple[Account, ...]:
    """Namespaces whose Risk consumer group reads stream `decisions` under run mode `mode`."""
    return _DECISION_TARGETS[mode]


def risk_group(account: Account) -> str:
    return f"risk-{account.value}"


def exec_group(account: Account) -> str:
    return f"exec-{account.value}"


def has_exposure(session: Session, account: Account) -> bool:
    """True while `account`'s ledger holds an open position or a working order (regular or conditional).

    Read by execution and Risk (SELECT on `positions`, `orders`, `algo_orders`) and by the mode policy.
    """
    a = account.value
    position = sa.select(PositionRow.position_id).where(
        PositionRow.account == a, PositionRow.closed_at.is_(None)
    )
    order = sa.select(OrderRow.client_id).where(OrderRow.account == a, OrderRow.status.in_(OPEN_ORDER_STATES))
    algo = sa.select(AlgoOrderRow.client_algo_id).where(
        AlgoOrderRow.account == a, AlgoOrderRow.status.in_(WORKING_ALGO_STATES)
    )
    return bool(session.scalar(sa.select(position.exists() | order.exists() | algo.exists())))
