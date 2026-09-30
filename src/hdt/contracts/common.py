"""Shared enums and base classes for the v1 data contracts (Design Contract section 12)."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Annotated

from pydantic import AfterValidator, BaseModel, ConfigDict

from hdt.core.clock import ensure_utc
from hdt.core.ids import canonical_json, canonical_sha256

UtcDatetime = Annotated[datetime, AfterValidator(ensure_utc)]


class AgentName(StrEnum):
    CROWDING = "crowding"
    TECHNICAL = "technical"
    MICRO = "micro"
    FUNDAMENTAL = "fundamental"
    NEWS = "news"
    MACRO = "macro"


class CandidateSource(StrEnum):
    LTX = "LTX"
    MIGRATION = "MIGRATION"
    HOLLOW_HYPE = "HOLLOW_HYPE"
    HELD = "HELD"


class TargetType(StrEnum):
    RAW_12H = "RAW_12H"
    RESID_12H = "RESID_12H"


class Side(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"


class Intent(StrEnum):
    OPEN = "OPEN"
    HOLD = "HOLD"
    EXIT = "EXIT"


class Account(StrEnum):
    PAPER = "paper"
    TESTNET = "testnet"
    LIVE = "live"


class Leg(StrEnum):
    ENTRY = "entry"
    ENTRY_IOC = "entry_ioc"
    SL = "sl"
    TP1 = "tp1"
    TRAIL = "trail"
    EXIT = "exit"
    HEDGE = "hedge"


class OrderSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(StrEnum):
    LIMIT = "LIMIT"
    MARKET = "MARKET"
    STOP_MARKET = "STOP_MARKET"
    TAKE_PROFIT_MARKET = "TAKE_PROFIT_MARKET"


class TimeInForce(StrEnum):
    GTX = "GTX"
    IOC = "IOC"
    GTC = "GTC"


class ClaimKind(StrEnum):
    PACKET = "packet"
    TOOL = "tool"
    URL = "url"


class DirectionHint(StrEnum):
    UP = "up"
    DOWN = "down"
    NONE = "none"


class Tier(StrEnum):
    T0 = "T0"
    T1 = "T1"
    T2 = "T2"
    T3 = "T3"


class DataQualityFlag(StrEnum):
    STALE = "stale"
    OUTLIER = "outlier"
    MISSING_ROUTE = "missing_route"
    RECONCILE_MISMATCH = "reconcile_mismatch"
    LIQ_MISMATCH = "liq_mismatch"
    FUNDING_INTERVAL_UNKNOWN = "funding_interval_unknown"
    FUNDING_INTERVAL_CHANGED = "funding_interval_changed"
    SPIKE_FLOOR_APPLIED = "spike_floor_applied"
    CMC_DEGRADED = "cmc_degraded"


class ContractModel(BaseModel):
    """Internal contract: frozen, unknown fields rejected."""

    model_config = ConfigDict(frozen=True, extra="forbid", use_enum_values=False, allow_inf_nan=False)

    def canonical_bytes(self) -> bytes:
        return canonical_json(self)

    def canonical_sha256(self) -> str:
        return canonical_sha256(self)


class InboundContractModel(ContractModel):
    """Message consumed from another service: unknown fields are ignored and never acted upon."""

    model_config = ConfigDict(frozen=True, extra="ignore", use_enum_values=False, allow_inf_nan=False)
