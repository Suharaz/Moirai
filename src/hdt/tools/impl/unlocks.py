"""`unlock_schedule(coin)`: scheduled token unlocks of the event coin around the event time.

The schedule comes from the unlock calendar selected in phase 07 step 1 (`UnlockCalendar`), as it was
recorded at or before the event `as_of`; the tool refuses entries recorded later. It lists unlocks from
`past_days` before to `ahead_days` after `as_of`, oldest first, with the hours until each unlock.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import ClassVar, Final

from pydantic import Field, NonNegativeFloat

from hdt.contracts.common import UtcDatetime
from hdt.tools.base import (
    ShortText,
    Tool,
    ToolArgs,
    ToolBackendError,
    ToolContext,
    ToolData,
    ToolMode,
)
from hdt.tools.ports import UnlockCalendar

MAX_UNLOCKS: Final[int] = 30


class UnlockArgs(ToolArgs):
    coin_id: int | None = Field(default=None, gt=0, description="CMC id; only the event coin (default)")
    ahead_days: int = Field(default=30, ge=1, le=180)
    past_days: int = Field(default=7, ge=0, le=30)


class UnlockEntry(ToolData):
    unlock_at: UtcDatetime
    hours_from_as_of: float = Field(description="negative for unlocks that already happened")
    amount_tokens: NonNegativeFloat | None
    pct_of_circulating: NonNegativeFloat | None
    category: ShortText
    source: ShortText
    recorded_at: UtcDatetime


class UnlockData(ToolData):
    coin_id: int
    as_of: UtcDatetime
    unlocks: tuple[UnlockEntry, ...] = Field(description="oldest first")
    pct_next_7d: NonNegativeFloat | None = Field(
        description="sum of pct_of_circulating over unlocks in the 7 days after as_of (None if unknown)"
    )


class UnlockScheduleTool(Tool[UnlockArgs, UnlockData]):
    name: ClassVar[str] = "unlock_schedule"
    description: ClassVar[str] = (
        "Recorded token unlock schedule of the event coin from past_days before to ahead_days after the "
        "event time, with amounts and the share of circulating supply."
    )
    args_model = UnlockArgs
    data_model = UnlockData

    def __init__(self, mode: ToolMode, calendar: UnlockCalendar) -> None:
        super().__init__(mode)
        self._calendar = calendar

    async def run(self, ctx: ToolContext, args: UnlockArgs) -> UnlockData:
        coin_id = ctx.event_coin(args.coin_id)
        start = ctx.as_of - timedelta(days=args.past_days)
        end = ctx.as_of + timedelta(days=args.ahead_days)
        events = await asyncio.to_thread(self._calendar.unlocks, coin_id, start, end, ctx.as_of)
        entries = []
        for event in events:
            if event.recorded_at > ctx.as_of or event.coin_id != coin_id:
                raise ToolBackendError(
                    "unlock calendar returned an entry recorded after as_of or another coin"
                )
            if not start <= event.unlock_at <= end:
                continue
            entries.append(
                UnlockEntry(
                    unlock_at=event.unlock_at,
                    hours_from_as_of=round((event.unlock_at - ctx.as_of).total_seconds() / 3600, 3),
                    amount_tokens=event.amount_tokens,
                    pct_of_circulating=event.pct_of_circulating,
                    category=event.category or "unspecified",
                    source=event.source,
                    recorded_at=event.recorded_at,
                )
            )
        entries.sort(key=lambda e: (e.unlock_at, e.category, e.source))
        week = [e for e in entries if 0 <= e.hours_from_as_of <= 7 * 24]
        pct = (
            None
            if any(e.pct_of_circulating is None for e in week)
            else sum(e.pct_of_circulating or 0.0 for e in week)
        )
        return UnlockData(
            coin_id=coin_id,
            as_of=ctx.as_of,
            unlocks=tuple(entries[:MAX_UNLOCKS]),
            pct_next_7d=round(pct, 6) if pct is not None else None,
        )
