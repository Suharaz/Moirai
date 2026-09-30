"""Owner self-test of the `exec/binance_testnet` and `exec/binance_live` secrets (scope `exec` owner).

The console's key test signs `GET /fapi/v3/account` with the candidate key on the namespace's own host
(demo-fapi for testnet, fapi for live). HTTP 200 proves the key, its secret and the IP whitelist work; the
wallet and position count are shown as details. HTTP 401 or Binance codes -2014 (key format), -2015 (key,
IP or permission refused) and -1022 (signature refused) are definitive refusals; anything else (network,
rate limit, 5xx) means the check could not complete.

An exchange IP ban (HTTP 418) recorded for the namespace (`exec_accounts.banned_until`) skips the request,
because any request would extend the ban; a ban answered by the check itself is recorded for every process.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Final

import httpx
from sqlalchemy.orm import Session, sessionmaker

from hdt.contracts.common import Account
from hdt.core.config import BinanceFile, static_config
from hdt.execution.adapter_base import ExchangeError, IpBannedError
from hdt.execution.binance_client import IP_BAN_DEFAULT_S, BinanceClient
from hdt.execution.ledger import Ledger
from hdt.execution.namespaces import BINANCE_SECRET
from hdt.vault.owner import CheckOutcome, SecretCheck
from hdt.vault.scopes import BinanceKeySecret

ACCOUNT_PATH: Final[str] = "/fapi/v3/account"
REFUSED_CODES: Final[frozenset[int]] = frozenset({-2014, -2015, -1022})


def binance_rest(account: Account, binance: BinanceFile) -> str:
    """REST host of a Binance namespace: demo-fapi for testnet, fapi for live."""
    if account is Account.TESTNET:
        return binance.demo.rest
    if account is Account.LIVE:
        return binance.live.rest
    raise ValueError("the paper namespace has no Binance host")


def _decimal(value: Any) -> str | None:
    try:
        return format(Decimal(str(value)), "f") if value is not None else None
    except InvalidOperation:
        return None


async def check_binance_key(
    secret: BinanceKeySecret,
    *,
    base_url: str,
    recv_window_ms: int = 5000,
    timeout_s: float = 10.0,
    transport: httpx.AsyncBaseTransport | None = None,
    on_ip_ban: Callable[[datetime], None] | None = None,
) -> CheckOutcome:
    """Sign one account read with the candidate key; `on_ip_ban` receives the ban end of an HTTP 418."""
    client = BinanceClient(
        base_url=base_url,
        api_key=secret.api_key,
        api_secret=secret.api_secret,
        recv_window_ms=recv_window_ms,
        timeout_s=timeout_s,
        transport=transport,
    )
    try:
        data = await client.signed("GET", ACCOUNT_PATH)
    except IpBannedError as exc:
        end = exc.banned_until if exc.banned_until is not None else time.time() + IP_BAN_DEFAULT_S
        until = datetime.fromtimestamp(end, UTC)
        if on_ip_ban is not None:
            on_ip_ban(until)
        return CheckOutcome("error", f"Binance banned the IP (HTTP 418) until {until.isoformat()}")
    except ExchangeError as exc:
        if exc.http_status == 401 or (exc.code is not None and exc.code in REFUSED_CODES):
            return CheckOutcome(
                "invalid", f"Binance refused the key (code {exc.code}, HTTP {exc.http_status})"
            )
        return CheckOutcome("error", f"{ACCOUNT_PATH} failed: {type(exc).__name__} (code {exc.code})")
    finally:
        await client.aclose()
    if not isinstance(data, Mapping):
        return CheckOutcome("error", f"{ACCOUNT_PATH} returned an unexpected body")
    positions = data.get("positions")
    return CheckOutcome(
        "valid",
        None,
        {
            "host": base_url,
            "total_wallet_balance": _decimal(data.get("totalWalletBalance")),
            "available_balance": _decimal(data.get("availableBalance")),
            "positions": len(positions) if isinstance(positions, list) else None,
        },
    )


def _check_for(account: Account, sessions: sessionmaker[Session]) -> SecretCheck:
    ledger = Ledger(sessions, account)

    async def check(payload: object) -> CheckOutcome:
        if not isinstance(payload, BinanceKeySecret):
            raise TypeError(f"{BINANCE_SECRET[account]} payload must be a BinanceKeySecret")
        banned = ledger.banned_until()
        if banned is not None:
            return CheckOutcome(
                "error", f"exchange IP ban recorded until {banned.isoformat()}; the key was not tested"
            )
        binance = static_config().binance
        return await check_binance_key(
            payload,
            base_url=binance_rest(account, binance),
            recv_window_ms=binance.recv_window_ms,
            timeout_s=binance.request_timeout_s,
            on_ip_ban=ledger.record_ban,
        )

    return check


def exec_secret_checks(sessions: sessionmaker[Session]) -> dict[str, SecretCheck]:
    """The owner checks of scope `exec`, honouring and recording each namespace's exchange IP ban."""
    return {name: _check_for(account, sessions) for account, name in BINANCE_SECRET.items()}
