"""Signed `OrderIntent` builders for the execution tests.

Keys are generated once per test session with the production helper (`hdt.risk.signer.generate`): the Risk
key is trusted for `paper` and `live`, the testnet driver key for `testnet` only, exactly as the keyring
of a deployed execution service.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from decimal import Decimal

from hdt.contracts.common import Account, Leg, OrderSide, OrderType, Side, TimeInForce
from hdt.contracts.order import OrderIntent
from hdt.execution.client_ids import client_id
from hdt.execution.verify import Keyring
from hdt.risk.gate import intent_id_for
from hdt.risk.signer import IntentSigner, generate

RISK_LINE, RISK_ENTRY = generate("risk-test", [Account.PAPER, Account.LIVE])
DRIVER_LINE, DRIVER_ENTRY = generate("driver-test", [Account.TESTNET])
RISK_SIGNER = IntentSigner.from_line(RISK_LINE)
DRIVER_SIGNER = IntentSigner.from_line(DRIVER_LINE)
KEYRING = Keyring.from_json(json.dumps({"keys": [RISK_ENTRY, DRIVER_ENTRY]}))


def signer_for(account: Account) -> IntentSigner:
    return DRIVER_SIGNER if account is Account.TESTNET else RISK_SIGNER


def _base(account: Account, event_id: str, leg: Leg, symbol: str, now: datetime) -> dict[str, object]:
    return {
        "intent_id": intent_id_for(account, event_id, leg, 0),
        "account": account,
        "event_id": event_id,
        "leg": leg,
        "seq": 0,
        "symbol": symbol,
        "client_id": client_id(event_id, leg, 0),
        "created_at": now,
        "key_id": signer_for(account).key_id,
    }


def open_plan(
    *,
    account: Account,
    event_id: str,
    symbol: str,
    side: Side,
    qty: Decimal,
    entry: Decimal,
    stop: Decimal,
    tp1: Decimal,
    now: datetime,
    ioc_price: Decimal | None = None,
    tp_qty: Decimal | None = None,
    trail_distance: Decimal = Decimal("1.50"),
    leverage: int = 3,
    ttl: timedelta | None = None,
    max_entry_distance: Decimal | None = None,
    horizon: timedelta = timedelta(hours=12),
    sign: bool = True,
) -> list[OrderIntent]:
    """The Risk leg plan of one OPEN in publish order: sl, tp1, trail, entry_ioc, entry. Risk signs entry
    legs without `expires_at` (a decision has no time limit); `ttl` adds one, as the testnet driver does."""
    long = side is Side.LONG
    entry_side, exit_side = (OrderSide.BUY, OrderSide.SELL) if long else (OrderSide.SELL, OrderSide.BUY)
    tp_qty = tp_qty if tp_qty is not None else qty / 2
    ioc_price = (
        ioc_price
        if ioc_price is not None
        else (entry * Decimal("1.001") if long else entry * Decimal("0.999"))
    )
    time_stop = now + horizon
    legs = [
        OrderIntent(
            **_base(account, event_id, Leg.SL, symbol, now),
            side=exit_side,
            order_type=OrderType.STOP_MARKET,
            qty=qty,
            trigger_price=stop,
            reduce_only=True,
            expires_at=time_stop,
        ),
        OrderIntent(
            **_base(account, event_id, Leg.TP1, symbol, now),
            side=exit_side,
            order_type=OrderType.TAKE_PROFIT_MARKET,
            qty=tp_qty,
            trigger_price=tp1,
            reduce_only=True,
            expires_at=time_stop,
        ),
        OrderIntent(
            **_base(account, event_id, Leg.TRAIL, symbol, now),
            side=exit_side,
            order_type=OrderType.STOP_MARKET,
            qty=qty - tp_qty,
            trigger_price=tp1,
            price=trail_distance,
            reduce_only=True,
            expires_at=time_stop,
        ),
    ]
    for leg, tif, price in ((Leg.ENTRY_IOC, TimeInForce.IOC, ioc_price), (Leg.ENTRY, TimeInForce.GTX, entry)):
        legs.append(
            OrderIntent(
                **_base(account, event_id, leg, symbol, now),
                side=entry_side,
                order_type=OrderType.LIMIT,
                qty=qty,
                price=price,
                reduce_only=False,
                tif=tif,
                leverage=leverage,
                expires_at=None if ttl is None else now + ttl,
                max_entry_distance=max_entry_distance,
            )
        )
    signer = signer_for(account)
    return [signer.sign(i) for i in legs] if sign else legs


def exit_intent(
    *, account: Account, event_id: str, symbol: str, held_side: Side, qty: Decimal, now: datetime
) -> OrderIntent:
    intent = OrderIntent(
        **_base(account, event_id, Leg.EXIT, symbol, now),
        side=OrderSide.SELL if held_side is Side.LONG else OrderSide.BUY,
        order_type=OrderType.MARKET,
        qty=qty,
        reduce_only=True,
    )
    return signer_for(account).sign(intent)


def hedge_intents(
    *,
    account: Account,
    seq: int,
    delta: Decimal,
    stop: Decimal,
    now: datetime,
    reduce_only: bool = False,
    symbol: str = "BTCUSDT",
) -> list[OrderIntent]:
    """A hedge-book rebalance in publish order: the book's new closePosition stop, then the MARKET `hedge`
    leg for the signed delta (the stop must be known before the hedge fill arrives)."""
    event_id = f"hedge:{account.value}:{seq}"
    side = OrderSide.BUY if delta > 0 else OrderSide.SELL
    book_is_short = delta < 0
    hedge, book_stop = (
        OrderIntent(
            **_base(account, event_id, Leg.HEDGE, symbol, now),
            side=side,
            order_type=OrderType.MARKET,
            qty=abs(delta),
            reduce_only=reduce_only,
            leverage=1,
        ),
        OrderIntent(
            **_base(account, event_id, Leg.SL, symbol, now),
            side=OrderSide.BUY if book_is_short else OrderSide.SELL,
            order_type=OrderType.STOP_MARKET,
            trigger_price=stop,
            reduce_only=False,
            close_position=True,
        ),
    )
    return [signer_for(account).sign(i) for i in (book_stop, hedge)]
