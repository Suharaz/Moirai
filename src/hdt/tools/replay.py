"""The offline (replay) tool registry.

Built separately from the live one and importing no network client (no httpx, no websockets; a test
checks the import graph in a fresh interpreter). Every tool reads only what was recorded at or before
the event's `as_of` (raw store through `PitQuery`, bitemporal memory, and phase 07 stores behind
`hdt.tools.ports` that the caller must provide as offline readers too). Nothing recorded means
`not_available`, never a live call. The replay `fetch_source` has no fetcher and no raw-store writer.

Only the pieces that are available are registered: a session for an agent that needs a missing tool
fails at creation (`MissingToolError`), so the agent abstains instead of running half-equipped.

`replay_context` builds an agent's `ToolContext` for one round of a recorded event from its stored forecasts
only: the pinned captures are exactly the persisted `capture_manifest` of that round and the earlier ones,
never an in-memory `ToolSession` and never a later round's.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from hdt.contracts.common import AgentName
from hdt.contracts.forecast import AgentForecast, LakeRef
from hdt.core.config import NewsSourcesFile
from hdt.lake.pit_query import PitQuery
from hdt.memory.store import MemoryStore
from hdt.tools.base import Tool, ToolContext, ToolMode
from hdt.tools.impl.dex import DexSecurityTool, LiquidityChangesTool
from hdt.tools.impl.fetch_source import FetchSourceReplayTool
from hdt.tools.impl.known_events import KnownEventsTool
from hdt.tools.impl.news import GetNewsTool
from hdt.tools.impl.official import CheckOfficialTool
from hdt.tools.impl.quant_core import QuantCoreTool
from hdt.tools.impl.snapshot import GetSnapshotTool
from hdt.tools.impl.unlocks import UnlockScheduleTool
from hdt.tools.ports import NewsIndex, OfficialSource, QuantCoreFn, UnlockCalendar
from hdt.tools.registry import ToolRegistry


def build_replay_registry(
    *,
    pit: PitQuery,
    stale_s: int,
    news_sources: NewsSourcesFile,
    quant_core: QuantCoreFn | None = None,
    news: NewsIndex | None = None,
    official: OfficialSource | None = None,
    unlocks: UnlockCalendar | None = None,
    memory: MemoryStore | None = None,
) -> ToolRegistry:
    mode: ToolMode = "replay"
    tools: list[Tool[Any, Any]] = [
        GetSnapshotTool(mode, pit, stale_s=stale_s),
        DexSecurityTool(mode, pit),
        LiquidityChangesTool(mode, pit),
    ]
    if quant_core is not None:
        tools.append(QuantCoreTool(mode, quant_core))
    if news is not None:
        tools.append(GetNewsTool(mode, news, news_sources))
        tools.append(FetchSourceReplayTool(mode, pit, news, news_sources))
    if official is not None:
        tools.append(CheckOfficialTool(mode, official, news_sources))
    if unlocks is not None:
        tools.append(UnlockScheduleTool(mode, unlocks))
    if memory is not None:
        tools.append(KnownEventsTool(mode, memory))
    return ToolRegistry(mode, tools)


def replay_context(agent: AgentName, forecasts: Iterable[AgentForecast], *, round_: int) -> ToolContext:
    """The replay context of `agent` in round `round_` of one recorded event, from the stored forecasts.

    `forecasts` are the event's persisted `AgentForecast`s (any agents, any rounds); only `agent`'s rounds
    up to `round_` are used, so a replayed round can never open a capture first made in a later round.
    `pinned` is the union of their `capture_manifest` in round order, without repeats.
    """
    own = sorted((f for f in forecasts if f.agent == agent and f.round <= round_), key=lambda f: f.round)
    if not any(f.round == round_ for f in own):
        raise ValueError(f"no stored forecast of the {agent.value} agent for round {round_}")
    first = own[0]
    if any((f.event_id, f.coin_id, f.as_of) != (first.event_id, first.coin_id, first.as_of) for f in own):
        raise ValueError(f"stored forecasts of the {agent.value} agent belong to different events")
    pinned: dict[LakeRef, None] = {}
    for forecast in own:
        pinned.update(dict.fromkeys(forecast.capture_manifest))
    return ToolContext("replay", agent, first.event_id, first.coin_id, first.as_of, tuple(pinned))
