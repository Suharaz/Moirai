"""`quant_core`: the agent's own `QuantPacket` for the event coin at the event `as_of` (phase 03).

No argument comes from the LLM: agent, coin and `as_of` are the context's. The same implementation serves
both modes because phase 03 computes the packet from the lake point-in-time and stores it (insert-only)
before returning it; a replay of the same lake, `as_of` and pins yields the same `packet_sha256`.
"""

from __future__ import annotations

import asyncio
from typing import Any, ClassVar

from pydantic import Field

from hdt.tools.base import (
    NotAvailableError,
    Tool,
    ToolArgs,
    ToolBackendError,
    ToolContext,
    ToolData,
    ToolMode,
)
from hdt.tools.ports import QuantCoreFn


class QuantCoreArgs(ToolArgs):
    """No arguments: the packet is always this agent's, for the event coin at the event as_of."""


class QuantCoreData(ToolData):
    packet_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    packet: dict[str, Any]


class QuantCoreTool(Tool[QuantCoreArgs, QuantCoreData]):
    name: ClassVar[str] = "quant_core"
    description: ClassVar[str] = (
        "Your quant packet for the event coin at the event time: the features of your family, p_model, "
        "data quality flags and the hash of the shared candidate set. Takes no arguments."
    )
    args_model = QuantCoreArgs
    data_model = QuantCoreData

    def __init__(self, mode: ToolMode, quant_core: QuantCoreFn) -> None:
        super().__init__(mode)
        self._quant_core = quant_core

    async def run(self, ctx: ToolContext, args: QuantCoreArgs) -> QuantCoreData:
        try:
            packet = await asyncio.to_thread(self._quant_core, ctx.agent, ctx.coin_id, ctx.as_of)
        except LookupError as exc:
            raise NotAvailableError(str(exc)) from exc
        if (packet.agent, packet.coin_id, packet.as_of) != (ctx.agent, ctx.coin_id, ctx.as_of):
            raise ToolBackendError("quant_core returned a packet for another agent, coin or as_of")
        return QuantCoreData(packet_sha256=packet.packet_sha256, packet=packet.model_dump(mode="json"))
