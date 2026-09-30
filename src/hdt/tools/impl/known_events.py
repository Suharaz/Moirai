"""`known_events(coin, window, as_of)`: news events already known for the event coin.

Reads the News agent's episodic memory (`(news, episodic)`, records `known_event` keyed by `event_key`,
for example `binance_spot_listing:XYZ`), bitemporally: only events whose `known_at` is before the read
time, so a replay of the event on any later day sees exactly what the live meeting saw. Used to tell a
new event from a re-post of an old one.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import ClassVar, Final

from pydantic import Field, PositiveInt

from hdt.contracts.common import AgentName, Tier, UtcDatetime
from hdt.memory.episodic import EpisodicReader
from hdt.memory.store import MemoryStore
from hdt.tools.base import ShortText, Tool, ToolArgs, ToolContext, ToolData, ToolMode

MAX_WINDOW_H: Final[int] = 30 * 24
MAX_EVENTS: Final[int] = 30


class KnownEventsArgs(ToolArgs):
    coin_id: PositiveInt | None = Field(default=None, description="CMC id; only the event coin (default)")
    window_h: int = Field(default=7 * 24, ge=1, le=MAX_WINDOW_H, description="how far back, in hours")
    as_of: UtcDatetime | None = Field(default=None, description="read time; not after the event time")


class KnownEventEntry(ToolData):
    event_key: str
    title: ShortText
    tier: Tier
    known_at: UtcDatetime
    age_h: float
    item_ids: tuple[str, ...]


class KnownEventsData(ToolData):
    coin_id: int
    since: UtcDatetime
    as_of: UtcDatetime
    events: tuple[KnownEventEntry, ...] = Field(description="newest first")


class KnownEventsTool(Tool[KnownEventsArgs, KnownEventsData]):
    name: ClassVar[str] = "known_events"
    description: ClassVar[str] = (
        "News events already known for the event coin (event_key such as binance_spot_listing:XYZ) in the "
        "window before the event time, newest first. An event seen here is not new."
    )
    args_model = KnownEventsArgs
    data_model = KnownEventsData

    def __init__(self, mode: ToolMode, memory: MemoryStore) -> None:
        super().__init__(mode)
        self._memory = memory

    async def run(self, ctx: ToolContext, args: KnownEventsArgs) -> KnownEventsData:
        coin_id = ctx.event_coin(args.coin_id)
        as_of = ctx.read_as_of(args.as_of)
        since = as_of - timedelta(hours=args.window_h)
        reader = EpisodicReader(self._memory, AgentName.NEWS, as_of)
        events = await asyncio.to_thread(reader.known_events, coin_id=coin_id, since=since)
        return KnownEventsData(
            coin_id=coin_id,
            since=since,
            as_of=as_of,
            events=tuple(
                KnownEventEntry(
                    event_key=event.event_key,
                    title=event.title,
                    tier=event.tier,
                    known_at=event.known_at,
                    age_h=round((as_of - event.known_at).total_seconds() / 3600, 3),
                    item_ids=event.item_ids,
                )
                for event in events[:MAX_EVENTS]
            ),
        )
