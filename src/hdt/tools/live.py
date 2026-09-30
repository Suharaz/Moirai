"""The live tool registry.

Live tools read the same point-in-time sources as replay, so a live meeting and its replay see the same
data. The only tool that calls out is `fetch_source`: it calls the phase 07 sandboxed fetcher (never
the network from this process), then writes the verbatim response to the raw store through the phase 02
`RawStore.append` (the single write path) before the agent sees anything. The DEX tools read what the
recorder captured (the council holds no CMC key); `quant_core` stores its packet insert-only before
returning it.

Only the pieces that are available are registered: a session for an agent that needs a missing tool
fails at creation (`MissingToolError`), so the agent abstains instead of running half-equipped.
"""

from __future__ import annotations

from typing import Any

from hdt.core.config import NewsSourcesFile
from hdt.lake.pit_query import PitQuery
from hdt.lake.raw_store import RawStore
from hdt.memory.store import MemoryStore
from hdt.tools.base import Tool, ToolMode
from hdt.tools.impl.dex import DexSecurityTool, LiquidityChangesTool
from hdt.tools.impl.fetch_source import FetchSourceLiveTool
from hdt.tools.impl.known_events import KnownEventsTool
from hdt.tools.impl.news import GetNewsTool
from hdt.tools.impl.official import CheckOfficialTool
from hdt.tools.impl.quant_core import QuantCoreTool
from hdt.tools.impl.snapshot import GetSnapshotTool
from hdt.tools.impl.unlocks import UnlockScheduleTool
from hdt.tools.ports import FetcherClient, NewsIndex, OfficialSource, QuantCoreFn, UnlockCalendar
from hdt.tools.registry import ToolRegistry


def build_live_registry(
    *,
    pit: PitQuery,
    raw_store: RawStore,
    stale_s: int,
    news_sources: NewsSourcesFile,
    quant_core: QuantCoreFn | None = None,
    news: NewsIndex | None = None,
    fetcher: FetcherClient | None = None,
    official: OfficialSource | None = None,
    unlocks: UnlockCalendar | None = None,
    memory: MemoryStore | None = None,
) -> ToolRegistry:
    mode: ToolMode = "live"
    tools: list[Tool[Any, Any]] = [
        GetSnapshotTool(mode, pit, stale_s=stale_s),
        DexSecurityTool(mode, pit),
        LiquidityChangesTool(mode, pit),
    ]
    if quant_core is not None:
        tools.append(QuantCoreTool(mode, quant_core))
    if news is not None:
        tools.append(GetNewsTool(mode, news, news_sources))
        if fetcher is not None:
            tools.append(
                FetchSourceLiveTool(mode, pit, news, news_sources, fetcher=fetcher, raw_store=raw_store)
            )
    if official is not None:
        tools.append(CheckOfficialTool(mode, official, news_sources))
    if unlocks is not None:
        tools.append(UnlockScheduleTool(mode, unlocks))
    if memory is not None:
        tools.append(KnownEventsTool(mode, memory))
    return ToolRegistry(mode, tools)
