"""Price validity of an opening entry against the current mark, shared by Risk and Execution.

A decision has no time limit (owner decision 2026-09-28): the only guard between a debate that took long,
or an entry that waited in `orders` while execution was down, and the exchange is this check on the mark
at the moment of the decision (Risk, gate step 5b) and again right before the entry order is sent
(Execution). Pure: no I/O, no clock.

Refused, side-aware (reason codes are the `risk_verdicts.reason` and execution's intent status reason):
- `stop_crossed`: the mark is at or beyond the invalidation (the stop could not be placed);
- `tp_crossed`: the mark is at or beyond the take-profit (the traded move already happened);
- `entry_distance`: the entry is farther than `max_distance` from the mark (Risk signs that distance on
  the entry legs as `max_entry_distance_atr` x ATR 1h, so both services apply the same bound).
"""

from __future__ import annotations

from decimal import Decimal
from typing import Final, NamedTuple

STOP_CROSSED: Final[str] = "stop_crossed"
TP_CROSSED: Final[str] = "tp_crossed"
ENTRY_DISTANCE: Final[str] = "entry_distance"


class EntryRefusal(NamedTuple):
    reason: str
    text: str


def entry_distance_limit(atr: Decimal, max_entry_distance_atr: float) -> Decimal:
    """The `entry_distance` bound in price units: `max_entry_distance_atr` x ATR."""
    return atr * Decimal(repr(max_entry_distance_atr))


def entry_refusal(
    *,
    long: bool,
    mark: Decimal,
    entry: Decimal,
    stop: Decimal,
    tp1: Decimal | None,
    max_distance: Decimal | None,
) -> EntryRefusal | None:
    """The first rule the mark breaks for this entry, or None while the levels still hold.

    `tp1` None (no take-profit leg) and `max_distance` None (an intent signed without it) skip that rule."""
    side = "LONG" if long else "SHORT"
    if (mark <= stop) if long else (mark >= stop):
        return EntryRefusal(
            STOP_CROSSED,
            f"Mark {mark} is already beyond the {side} invalidation {stop}: the stop could not be placed",
        )
    if tp1 is not None and ((mark >= tp1) if long else (mark <= tp1)):
        return EntryRefusal(
            TP_CROSSED,
            f"Mark {mark} is already beyond the {side} take-profit {tp1}: "
            "the move the decision traded has already happened",
        )
    if max_distance is not None and abs(entry - mark) > max_distance:
        return EntryRefusal(
            ENTRY_DISTANCE,
            f"Entry {entry} is {abs(entry - mark)} from mark {mark}, limit {max_distance}",
        )
    return None
