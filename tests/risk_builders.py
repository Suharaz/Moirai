"""Builders for the phase 09 risk tests: candidates, packets, decisions, flags and gate inputs.

Every value is a plain, hand-checkable number so a test can state the expected size or price directly.
Defaults (equity 10 000, LONG 100.00 / stop 98.00 / TP1 104.00, p = 0.65) size to 9.000 at 3x leverage:
risk 10 000 x 0.5% x conf 0.75 = 37.5 USD -> 18.75 units, capped by the 3% margin at 3x (900 USD).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from functools import cache
from typing import Any

from hdt.contracts.account import AccountState, PositionState
from hdt.contracts.candidate import CandidateSet, LevelCandidate
from hdt.contracts.common import Account, AgentName, Intent, Side, TargetType
from hdt.contracts.decision import DecisionMsg
from hdt.contracts.packet import QuantPacket
from hdt.contracts.risk_flags import RiskFlags
from hdt.core.config import RiskFile, static_config
from hdt.execution.filters import SymbolFilters
from hdt.risk.gate import BookInputs, GateConfig, GateInputs, HeldPosition, MarketInputs
from hdt.risk.kill_state import RUNNING, KillView
from hdt.risk.limits import Exposure

NOW = datetime(2026, 9, 28, 12, 0, tzinfo=UTC)
COIN_ID = 5426
SYMBOL = "SOLUSDT"
CID_LONG = "lc_AAAAAAAAAAAAAAAA"
CID_SHORT = "lc_BBBBBBBBBBBBBBBB"
CID_LONG_FAR = "lc_CCCCCCCCCCCCCCCC"
KEY_ID = "risk-test"
FEATURES: dict[str, Any] = {
    "atr_1h": 1.5,
    "open_interest_usd": 500_000_000.0,
    "quote_volume_1h_usd": 200_000_000.0,
    "beta_btc": 1.2,
    "categories": "layer-1,solana-ecosystem",
}


def risk_file(**overrides: Any) -> RiskFile:
    """The pinned default `config/risk.yaml`, optionally with fields replaced."""
    base = static_config().risk
    return base.model_copy(update=overrides) if overrides else base


def symbol_filters(
    symbol: str = SYMBOL,
    *,
    tick: str = "0.01",
    step: str = "0.001",
    min_qty: str = "0.001",
    min_notional: str = "5",
    status: str = "TRADING",
) -> SymbolFilters:
    return SymbolFilters(
        symbol=symbol,
        status=status,
        tick_size=Decimal(tick),
        step_size=Decimal(step),
        min_qty=Decimal(min_qty),
        max_qty=Decimal("1000000"),
        market_step_size=Decimal(step),
        market_min_qty=Decimal(min_qty),
        market_max_qty=Decimal("1000000"),
        min_notional=Decimal(min_notional),
    )


def candidate(
    side: Side = Side.LONG,
    *,
    entry: str = "100.00",
    stop: str | None = None,
    tp1: str | None = None,
    candidate_id: str | None = None,
    tick: str = "0.01",
) -> LevelCandidate:
    long = side is Side.LONG
    stop = stop or ("98.00" if long else "102.00")
    tp1 = tp1 or ("104.00" if long else "96.00")
    e, s, t = Decimal(entry), Decimal(stop), Decimal(tp1)
    return LevelCandidate(
        candidate_id=candidate_id or (CID_LONG if long else CID_SHORT),
        side=side,
        entry=e,
        invalidation=s,
        tp1=t,
        rr=float(abs(t - e) / abs(e - s)),
        tick=Decimal(tick),
    )


def candidate_set(*cands: LevelCandidate, coin_id: int = COIN_ID, as_of: datetime = NOW) -> CandidateSet:
    chosen = cands or (candidate(Side.LONG), candidate(Side.SHORT))
    return CandidateSet(
        coin_id=coin_id,
        as_of=as_of,
        levels_ver="levels-test",
        candidates=tuple(sorted(chosen, key=lambda c: c.candidate_id)),
    )


def packet(
    cs: CandidateSet,
    *,
    coin_id: int = COIN_ID,
    as_of: datetime = NOW,
    features: Mapping[str, Any] | None = None,
) -> QuantPacket:
    return QuantPacket.build(
        agent=AgentName.TECHNICAL,
        coin_id=coin_id,
        as_of=as_of,
        features=dict(FEATURES) | dict(features or {}),
        p_model=0.6,
        candidate_set_sha256=cs.candidate_set_sha256,
        data_quality={},
        universe_date=as_of.date(),
        config_version_ids={"risk": 1},
        feature_ver="feature-test",
        p_model_ver="p-test",
        target_type=TargetType.RAW_12H,
        label_spec_version="label-test",
    )


@cache
def default_candidate_set() -> CandidateSet:
    return candidate_set()


@cache
def default_packet() -> QuantPacket:
    return packet(default_candidate_set())


def decision(
    *,
    intent: Intent = Intent.OPEN,
    side: Side = Side.LONG,
    p: float = 0.65,
    p_side: float | None = None,
    candidate_id: str | None = CID_LONG,
    packet_sha256: str | None = None,
    event_id: str = "evt-0001",
    coin_id: int = COIN_ID,
    as_of: datetime = NOW,
    expires_at: datetime | None = None,
    manager_size: float = 1.0,
    extra: Mapping[str, Any] | None = None,
) -> DecisionMsg:
    """A decision as the council outbox emits it (no `expires_at`: a decision has no time limit);
    `expires_at` adds the optional v1 field, `extra` adds stray fields (ignored by the contract).

    An OPEN without an explicit `packet_sha256` references the default packet (`packet(candidate_set())`).
    """
    computed = p if side is Side.LONG else 1.0 - p
    if packet_sha256 is None and intent is Intent.OPEN:
        packet_sha256 = default_packet().packet_sha256
    payload: dict[str, Any] = {
        "event_id": event_id,
        "coin_id": coin_id,
        "as_of": as_of.isoformat(),
        "intent": intent.value,
        "side": side.value,
        "p": p,
        "p_side": computed if p_side is None else p_side,
        "manager_size": manager_size,
        "candidate_id": candidate_id if intent is Intent.OPEN else None,
        "packet_sha256": packet_sha256,
        "config_version_ids": {"risk": 1},
        "target_type": TargetType.RAW_12H.value,
        "label_spec_version": "label-test",
    }
    if expires_at is not None:
        payload["expires_at"] = expires_at.isoformat()
    return DecisionMsg.model_validate(payload | dict(extra or {}))


def position(
    symbol: str = SYMBOL, qty: str = "9", *, entry: str = "100", hedge: bool = False
) -> PositionState:
    return PositionState(
        symbol=symbol,
        qty=Decimal(qty),
        entry_price=Decimal(entry),
        mark_price=Decimal(entry),
        unrealized_pnl=Decimal(0),
        leverage=3,
        margin_type="ISOLATED",
        is_hedge_book=hedge,
    )


def account_state(
    *,
    account: Account = Account.PAPER,
    equity: str = "10000",
    day_start: str | None = None,
    positions: tuple[PositionState, ...] = (),
    ts: datetime = NOW,
) -> AccountState:
    return AccountState(
        account=account,
        equity=Decimal(equity),
        available=Decimal(equity),
        day_start_equity=Decimal(day_start or equity),
        positions=positions,
        open_orders=(),
        open_algo_orders=(),
        ts=ts,
    )


def flags(
    *,
    coin_id: int = COIN_ID,
    veto_long: bool = False,
    veto_short: bool = False,
    size_mult: float = 1.0,
    age_s: float = 10.0,
    now: datetime = NOW,
    ttl_s: float = 3600.0,
) -> RiskFlags:
    """Flags from a scan `age_s` seconds before `now`, valid for `ttl_s` after it."""
    scanned = now - timedelta(seconds=age_s)
    return RiskFlags(
        coin_id=coin_id,
        as_of=scanned,
        veto_long=veto_long,
        veto_short=veto_short,
        size_mult=size_mult,
        expires_at=scanned + timedelta(seconds=ttl_s),
        scan_fresh_at=scanned,
    )


def market(
    *,
    symbol: str | None = SYMBOL,
    is_btc: bool = False,
    mark: str | None = "100.50",
    mark_age_s: float | None = 1.0,
    book_age_s: float | None = 1.0,
    filters: SymbolFilters | None = None,
    btc_mark: str | None = "60000",
) -> MarketInputs:
    return MarketInputs(
        symbol=symbol,
        is_btc=is_btc,
        filters=filters if filters is not None else symbol_filters(symbol or SYMBOL),
        mark=Decimal(mark) if mark is not None else None,
        mark_age_s=mark_age_s,
        book_age_s=book_age_s,
        btc_mark=Decimal(btc_mark) if btc_mark is not None else None,
    )


def book(
    *,
    state: AccountState | None = None,
    state_age_s: float | None = 5.0,
    held: Mapping[str, HeldPosition] | None = None,
    exposures: tuple[Exposure, ...] = (),
    kill: KillView = RUNNING,
    hedge_qty: str = "0",
) -> BookInputs:
    return BookInputs(
        account_state=state if state is not None else account_state(),
        state_age_s=state_age_s,
        held=dict(held or {}),
        exposures=exposures,
        kill=kill,
        hedge_qty=Decimal(hedge_qty),
    )


def gate_config(
    account: Account = Account.PAPER, risk: RiskFile | None = None, *, opens_enabled: bool = True
) -> GateConfig:
    return GateConfig(
        account=account,
        risk=risk or risk_file(),
        stale_s=180.0,
        key_id=KEY_ID,
        opens_enabled=opens_enabled,
    )


def gate_inputs(
    d: DecisionMsg | None = None,
    *,
    cs: CandidateSet | None = None,
    pkt: QuantPacket | None = None,
    packet_error: str | None = None,
    risk_flags: RiskFlags | None = None,
    mkt: MarketInputs | None = None,
    bk: BookInputs | None = None,
    use_packet: bool = True,
) -> GateInputs:
    """Inputs for `evaluate`; the decision references the packet unless it already names one."""
    if cs is None and pkt is None:
        cs, pkt = default_candidate_set(), default_packet()
    cs = cs or default_candidate_set()
    pkt = pkt or packet(cs)
    d = d or decision(packet_sha256=pkt.packet_sha256)
    return GateInputs(
        decision=d,
        packet=pkt if use_packet else None,
        packet_error=packet_error,
        candidate_set=cs,
        flags=risk_flags if risk_flags is not None else flags(),
        market=mkt or market(),
        book=bk or book(),
    )


def with_decision(inp: GateInputs, d: DecisionMsg) -> GateInputs:
    return replace(inp, decision=d)
