"""LTX (Liquidation Term-Structure Exhaustion) rule, Design Contract section 2.

LONG when Skew4 >= skew4_min, SPIKE >= spike_min, DECAY <= decay_max, FDz >= fdz_min, dOI4 <= doi4_max and
hourly funding dropped by >= funding_drop_min; SHORT is symmetric (Skew4 <= 1 - skew4_min, hourly funding
rose by >= funding_drop_min). Every condition must be known (a null input fails the rule, it never passes).

Contagion block: no LTX on a side while the breadth B of that side (the share of the LTX cross-section,
BTC excluded, with a same-side SPIKE >= breadth_spike_min) is >= b_block and BTC has not flushed on that side
(BTC SPIKE >= btc_flush_spike_min and DECAY <= btc_flush_decay_max with the same skew side).
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Literal

from hdt.core.config import ContagionParams, LtxRule, LtxThresholds

LtxSide = Literal["LONG", "SHORT"]
CONDITIONS = ("skew4", "spike", "decay", "fdz", "doi4", "funding_drop")


@dataclass(frozen=True)
class LtxInputs:
    skew4: float | None
    spike: float | None
    decay: float | None
    fdz: float | None
    doi4: float | None
    funding_drop: float | None

    def values(self) -> dict[str, float | None]:
        return {name: getattr(self, name) for name in CONDITIONS}


@dataclass(frozen=True)
class LtxEval:
    side: LtxSide | None
    strict: dict[str, bool | None]
    loose: dict[str, bool | None]

    @property
    def strict_pass(self) -> bool:
        return self.side is not None and all(v is True for v in self.strict.values())

    @property
    def loose_pass(self) -> bool:
        return self.side is not None and all(v is True for v in self.loose.values())


def side_of(skew4: float | None) -> LtxSide | None:
    """The flushed side: LONG after a long-dominated flush (Skew4 > 0.5), SHORT after a short one."""
    if skew4 is None or skew4 == 0.5:
        return None
    return "LONG" if skew4 > 0.5 else "SHORT"


def check(x: LtxInputs, side: LtxSide, t: LtxThresholds) -> dict[str, bool | None]:
    def known(value: float | None, test: bool) -> bool | None:
        return None if value is None else test

    skew_ok = known(
        x.skew4,
        x.skew4 is not None and (x.skew4 >= t.skew4_min if side == "LONG" else x.skew4 <= 1 - t.skew4_min),
    )
    drop = x.funding_drop if side == "LONG" or x.funding_drop is None else -x.funding_drop
    return {
        "skew4": skew_ok,
        "spike": known(x.spike, x.spike is not None and x.spike >= t.spike_min),
        "decay": known(x.decay, x.decay is not None and x.decay <= t.decay_max),
        "fdz": known(x.fdz, x.fdz is not None and x.fdz >= t.fdz_min),
        "doi4": known(x.doi4, x.doi4 is not None and x.doi4 <= t.doi4_max),
        "funding_drop": known(drop, drop is not None and drop >= t.funding_drop_min),
    }


def evaluate(x: LtxInputs, rule: LtxRule) -> LtxEval:
    side = side_of(x.skew4)
    if side is None:
        empty: dict[str, bool | None] = dict.fromkeys(CONDITIONS)
        return LtxEval(None, empty, dict(empty))
    return LtxEval(side, check(x, side, rule.strict), check(x, side, rule.loose))


@dataclass(frozen=True)
class Breadth:
    long: float | None
    short: float | None
    counted: int

    def of(self, side: LtxSide) -> float | None:
        return self.long if side == "LONG" else self.short


def breadth(coins: Iterable[tuple[float | None, float | None]], params: ContagionParams) -> Breadth:
    """B per side over (SPIKE, Skew4) pairs of the LTX cross-section without BTC."""
    total = n_long = n_short = 0
    for spike, skew4 in coins:
        total += 1
        side = side_of(skew4)
        if spike is None or side is None or spike < params.breadth_spike_min:
            continue
        if side == "LONG":
            n_long += 1
        else:
            n_short += 1
    if total == 0:
        return Breadth(None, None, 0)
    return Breadth(n_long / total, n_short / total, total)


def btc_flushed(
    spike: float | None, decay: float | None, skew4: float | None, side: LtxSide, params: ContagionParams
) -> bool:
    return (
        spike is not None
        and decay is not None
        and side_of(skew4) == side
        and spike >= params.btc_flush_spike_min
        and decay <= params.btc_flush_decay_max
    )


def contagion_blocked(side: LtxSide, b: Breadth, flushed: bool, params: ContagionParams) -> bool | None:
    """True when blocked; None when B is unknown (the rule then cannot pass)."""
    value = b.of(side)
    if value is None:
        return None
    return value >= params.b_block and not flushed
