"""Tool registry (one mode) and the per-agent tool session.

`ToolRegistry(mode, tools)` refuses tools of the other mode; `registry.session(ctx, budget)` refuses a
context of the other mode, so an agent can never reach a live tool while replaying (or the reverse).
A session is bound to one agent in one event: it exposes only that agent's tools (`AGENT_TOOLS`, the
phase 05 tool table), enforces the budget, and records every call as a `ToolCallRecord` for the
decision card. Build registries with `hdt.tools.live.build_live_registry` or
`hdt.tools.replay.build_replay_registry`; this module imports neither.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable, Iterable, Mapping
from types import MappingProxyType
from typing import Any, Final

from pydantic import ValidationError

from hdt.contracts.common import AgentName
from hdt.contracts.forecast import LakeRef, ToolCallRecord
from hdt.core.ids import canonical_sha256
from hdt.tools.base import (
    Tool,
    ToolContext,
    ToolInputError,
    ToolMode,
    ToolOutcomeError,
    ToolResult,
    ToolStatus,
    args_sha256,
    result_id,
    sanitize_text,
)
from hdt.tools.budget import BudgetEnforcer, ToolBudget

log = logging.getLogger(__name__)

AGENT_TOOLS: Final[Mapping[AgentName, frozenset[str]]] = MappingProxyType(
    {
        AgentName.CROWDING: frozenset({"quant_core", "get_snapshot"}),
        AgentName.TECHNICAL: frozenset({"quant_core"}),
        AgentName.MICRO: frozenset({"quant_core"}),
        AgentName.FUNDAMENTAL: frozenset(
            {"quant_core", "dex_security", "liquidity_changes", "unlock_schedule"}
        ),
        AgentName.NEWS: frozenset({"get_news", "fetch_source", "check_official", "known_events"}),
        AgentName.MACRO: frozenset({"quant_core"}),
    }
)
_MESSAGE_CHARS: Final[int] = 500


class RegistryModeError(RuntimeError):
    """A tool or a context of the other mode was offered to a registry."""


class MissingToolError(LookupError):
    """An agent needs a tool whose backend was not provided to the registry."""


class ToolRegistry:
    def __init__(self, mode: ToolMode, tools: Iterable[Tool[Any, Any]]) -> None:
        self.mode: ToolMode = mode
        registered: dict[str, Tool[Any, Any]] = {}
        for tool in tools:
            if tool.mode != mode or mode not in tool.modes:
                raise RegistryModeError(f"{tool.mode} tool {tool.name} offered to a {mode} registry")
            if tool.name in registered:
                raise ValueError(f"tool {tool.name} registered twice")
            registered[tool.name] = tool
        self._tools = MappingProxyType(registered)

    def names(self) -> frozenset[str]:
        return frozenset(self._tools)

    def get(self, name: str) -> Tool[Any, Any]:
        try:
            return self._tools[name]
        except KeyError:
            raise MissingToolError(f"no tool {name} in the {self.mode} registry") from None

    def session(
        self,
        ctx: ToolContext,
        budget: ToolBudget,
        *,
        allowed: Iterable[str] | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> ToolSession:
        """A session for one agent in one event; `allowed` defaults to the agent's tool table row."""
        if ctx.mode != self.mode:
            raise RegistryModeError(f"a {ctx.mode} context cannot use the {self.mode} registry")
        names = frozenset(allowed) if allowed is not None else AGENT_TOOLS[ctx.agent]
        not_for_agent = names - AGENT_TOOLS[ctx.agent]
        if not_for_agent:
            raise ToolInputError(f"tools {sorted(not_for_agent)} are not available to {ctx.agent.value}")
        missing = names - self.names()
        if missing:
            raise MissingToolError(
                f"{self.mode} registry lacks tools {sorted(missing)} for {ctx.agent.value}"
            )
        return ToolSession(self, ctx, BudgetEnforcer(budget, clock), names)


class ToolSession:
    """Calls tools for one agent in one event, under its budget; calls are serialized."""

    def __init__(
        self, registry: ToolRegistry, ctx: ToolContext, budget: BudgetEnforcer, allowed: frozenset[str]
    ) -> None:
        self.ctx = ctx
        self._registry = registry
        self._budget = budget
        self._allowed = allowed
        self._records: list[ToolCallRecord] = []
        self._captures: list[LakeRef] = []
        self._lock = asyncio.Lock()

    @property
    def records(self) -> tuple[ToolCallRecord, ...]:
        """Every call so far, for `AgentForecast.tool_calls` on the decision card."""
        return tuple(self._records)

    @property
    def captures(self) -> tuple[LakeRef, ...]:
        """Lake records this meeting created after `as_of` (fetch_source copies): the council copies them
        into `AgentForecast.capture_manifest`, which replay turns back into `ToolContext.pinned`."""
        return tuple(self._captures)

    @property
    def abstain_reason(self) -> str | None:
        """Set once the budget was exceeded: the agent must abstain with this reason."""
        return self._budget.abstain_reason

    @property
    def budget(self) -> BudgetEnforcer:
        return self._budget

    def tool_names(self) -> tuple[str, ...]:
        return tuple(sorted(self._allowed))

    def json_schemas(self) -> list[dict[str, Any]]:
        return [self._registry.get(name).json_schema() for name in self.tool_names()]

    async def call(self, name: str, arguments: Mapping[str, Any]) -> ToolResult:
        async with self._lock:
            return await self._call(name, dict(arguments))

    async def _call(self, name: str, arguments: dict[str, Any]) -> ToolResult:
        input_sha256 = args_sha256(name, arguments)
        admission = self._budget.admit()
        if not admission.allowed:
            result = ToolResult(tool=name, status="budget_exceeded", message=_budget_message(self._budget))
            self._record(name, input_sha256, result, 0.0)
            return result
        timed_out = False
        try:
            result = await self._run(name, arguments, admission.timeout_s)
        except TimeoutError:
            timed_out = True
            result = ToolResult(tool=name, status="budget_exceeded", message="tool time budget exhausted")
        elapsed = self._budget.finish(timed_out=timed_out)
        self._record(name, input_sha256, result, elapsed)
        return result

    async def _run(self, name: str, arguments: dict[str, Any], timeout_s: float) -> ToolResult:
        try:
            if name not in self._allowed:
                raise ToolInputError(f"tool {name} is not available to the {self.ctx.agent.value} agent")
            tool = self._registry.get(name)
            try:
                args = tool.parse_args(arguments)
            except ValidationError as exc:
                raise ToolInputError(_validation_message(exc)) from None
            data = await asyncio.wait_for(tool.run(self.ctx, args), timeout=timeout_s)
        except ToolOutcomeError as failure:
            return _failure(name, failure.status, str(failure))
        except MissingToolError as exc:
            return _failure(name, "error", str(exc))
        except TimeoutError:
            raise
        except Exception:
            log.exception(
                "tool failed",
                extra={"tool": name, "agent": self.ctx.agent.value, "event_id": self.ctx.event_id},
            )
            return _failure(name, "error", "internal tool error")
        source = getattr(data, "source", None)
        if (
            isinstance(source, LakeRef)
            and source.fetched_at > self.ctx.as_of
            and source not in self._captures
        ):
            self._captures.append(source)
        payload = data.model_dump(mode="json")
        return ToolResult(
            tool=name, status="ok", result_id=result_id(name, canonical_sha256(payload)), data=payload
        )

    def _record(self, name: str, input_sha256: str, result: ToolResult, elapsed_s: float) -> None:
        if result.status == "ok":
            output_sha256 = canonical_sha256(result.data)
        else:
            output_sha256 = canonical_sha256({"status": result.status, "message": result.message})
        self._records.append(
            ToolCallRecord(
                tool=name,
                input_sha256=input_sha256,
                output_sha256=output_sha256,
                status=result.status,
                latency_ms=round(elapsed_s * 1000),
            )
        )


def _failure(name: str, status: ToolStatus, message: str) -> ToolResult:
    return ToolResult(tool=name, status=status, message=sanitize_text(message, _MESSAGE_CHARS))


def _validation_message(exc: ValidationError) -> str:
    problems = [
        f"{'.'.join(str(p) for p in err['loc']) or 'arguments'}: {err['msg']}" for err in exc.errors()
    ]
    return "invalid arguments: " + "; ".join(problems[:5])


def _budget_message(budget: BudgetEnforcer) -> str:
    """Fixed text (no timings): it enters the prompt, which must replay byte-identically."""
    return (
        f"tool budget exceeded (at most {budget.budget.max_calls} calls and "
        f"{budget.budget.max_seconds:g} s of tool time per meeting); the agent must abstain"
    )
