"""Fail-closed veto (phase 09 section 3). Pure functions; the caller supplies clocks and inputs.

Scan age is `now - scan_fresh_at` in units of the veto scan cycle (`veto_scan_interval_s`):
- <= 1 cycle: `veto_long` / `veto_short` block that side, `size_mult` applies;
- 1-2 cycles: the same, with `size_mult = min(size_mult, 0.5)`;
- > 2 cycles, flags past `expires_at`, or no scan at all: no new LONG; SHORT at most 0.5 size (a stale
  veto still blocks its side: stale data only ever tightens).
Other vetoes: namespace `AccountState` older than `account_state_max_age_s`, coin Binance data older than
`data.stale_s`, an active kill state. Stale CMC data never vetoes (CMC-degraded mode).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from hdt.contracts.common import Side
from hdt.contracts.risk_flags import RiskFlags

STALE_SIZE_CAP = 0.5


@dataclass(frozen=True)
class VetoResult:
    allowed: bool
    size_mult: float
    flags_age_s: float | None
    text: str
    reason: str | None = None


def flags_age_s(flags: RiskFlags | None, now: datetime) -> float | None:
    """Age of the scan behind `flags` (None when absent or past `expires_at`)."""
    if flags is None or now > flags.expires_at:
        return None
    return max(0.0, (now - flags.scan_fresh_at).total_seconds())


def _blocked(flags: RiskFlags, side: Side) -> bool:
    return flags.veto_long if side is Side.LONG else flags.veto_short


def evaluate_open(flags: RiskFlags | None, side: Side, now: datetime, scan_interval_s: float) -> VetoResult:
    """Veto verdict for opening `side` on a coin."""
    age = flags_age_s(flags, now)
    usable = flags if age is not None else None
    cycles = age / scan_interval_s if age is not None else None
    if usable is not None and _blocked(usable, side):
        return VetoResult(
            False, 0.0, age, f"Veto scan blocks {side.value} ({age:.0f} s old)", f"veto_{side.value.lower()}"
        )
    if cycles is not None and usable is not None and cycles <= 2:
        mult = usable.size_mult if cycles <= 1 else min(usable.size_mult, STALE_SIZE_CAP)
        note = "fresh" if cycles <= 1 else "1-2 cycles old, size capped at 0.5"
        return VetoResult(True, mult, age, f"Veto scan {age:.0f} s old ({note}), size_mult {mult:g}")
    # No usable scan within two cycles: fail closed.
    if flags is not None and _blocked(flags, side):
        return VetoResult(
            False, 0.0, age, f"Stale veto scan still blocks {side.value}", f"veto_{side.value.lower()}"
        )
    if side is Side.LONG:
        return VetoResult(False, 0.0, age, "No veto scan within 2 cycles: no new LONG", "veto_stale")
    base = flags.size_mult if flags is not None else 1.0
    mult = min(base, STALE_SIZE_CAP)
    return VetoResult(True, mult, age, f"No veto scan within 2 cycles: SHORT size capped at {mult:g}")


def exit_required(flags: RiskFlags | None, held_side: Side, now: datetime) -> bool:
    """A held position must be closed when an unexpired scan vetoes its side."""
    return flags is not None and now <= flags.expires_at and _blocked(flags, held_side)


def age_ok(ts: datetime | None, now: datetime, max_age_s: float) -> bool:
    return ts is not None and (now - ts).total_seconds() <= max_age_s


@dataclass
class FlagsBook:
    """Latest `RiskFlags` per coin (a message with an older `as_of` never replaces a newer one)."""

    by_coin: dict[int, RiskFlags]

    @classmethod
    def empty(cls) -> FlagsBook:
        return cls({})

    def offer(self, flags: RiskFlags) -> bool:
        held = self.by_coin.get(flags.coin_id)
        if held is not None and held.as_of > flags.as_of:
            return False
        self.by_coin[flags.coin_id] = flags
        return True

    def get(self, coin_id: int) -> RiskFlags | None:
        return self.by_coin.get(coin_id)
