"""Opening a namespace's exchange: shared by the execution service and `hdt-kill`.

Paper is the simulated venue restored from `paper_state` over the read-only lake. Testnet and live are
Binance USDS-M (demo-fapi / fapi) with the key the vault scope `exec` holds (`exec/binance_testnet`,
`exec/binance_live`); only the execution container has that scope's private key.
"""

from __future__ import annotations

import logging
from decimal import Decimal

from nacl.public import PrivateKey
from sqlalchemy.orm import Session, sessionmaker

from hdt.contracts.common import Account
from hdt.core.config import BinanceFile, RiskFile
from hdt.execution.adapter_base import ExchangeAdapter
from hdt.execution.binance_adapter import BinanceAdapter
from hdt.execution.binance_client import BinanceClient
from hdt.execution.key_check import binance_rest
from hdt.execution.namespaces import BINANCE_SECRET
from hdt.execution.paper import PaperAdapter
from hdt.lake.pit_query import PitQuery
from hdt.vault.loader import SecretIntegrityError, SecretLoader, SecretNotAvailableError
from hdt.vault.scopes import VaultKeyError, load_private_key

log = logging.getLogger(__name__)


class NamespaceUnavailableError(RuntimeError):
    """A namespace cannot start (no vault key, Hedge Mode account, ...)."""


def exec_vault(sessions: sessionmaker[Session]) -> tuple[SecretLoader | None, PrivateKey | None]:
    """Vault scope `exec` (Binance keys, owner checks); None when this process holds no exec key."""
    try:
        key = load_private_key("exec")
    except VaultKeyError as exc:
        log.warning("vault scope exec unavailable, only the paper namespace can run: %s", exc)
        return None, None
    return SecretLoader("exec", sessions, key), key


def open_adapter(
    account: Account,
    *,
    sessions: sessionmaker[Session],
    pit: PitQuery,
    risk: RiskFile,
    binance: BinanceFile,
    vault: SecretLoader | None,
) -> ExchangeAdapter:
    """The namespace's exchange: the paper venue restored from `paper_state`, or Binance (vault key)."""
    if account is Account.PAPER:
        paper = PaperAdapter(
            sessions,
            pit,
            maker_fee=Decimal(repr(risk.fees.maker)),
            taker_fee=Decimal(repr(risk.fees.taker)),
            slippage_bp=Decimal(repr(risk.fees.slippage_bp)),
            starting_equity=Decimal(repr(risk.paper_starting_equity_usd)),
        )
        paper.load()
        return paper
    if vault is None:
        raise NamespaceUnavailableError("vault scope exec is not available (HDT_SCOPE_PRIVATE_KEY_FILE)")
    try:
        secret = vault.get(BINANCE_SECRET[account])
    except (SecretNotAvailableError, SecretIntegrityError) as exc:
        raise NamespaceUnavailableError(f"no usable {BINANCE_SECRET[account]} key: {exc}") from exc
    client = BinanceClient(
        base_url=binance_rest(account, binance),
        api_key=str(secret.values["api_key"]),
        api_secret=str(secret.values["api_secret"]),
        recv_window_ms=binance.recv_window_ms,
        timeout_s=binance.request_timeout_s,
    )
    return BinanceAdapter(account, client)
