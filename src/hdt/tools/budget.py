"""Per-agent, per-meeting tool budget: at most `max_calls` calls and `max_seconds` of tool time in total.

One enforcer lives for one agent in one council event (all rounds share it). The limits come from the
pinned council config (`tool_budget`). A call beyond `max_calls`, or a call still running when the
cumulative tool time reaches `max_seconds`, exceeds the budget: that call returns `budget_exceeded`, no
further call is admitted, and the agent is forced to abstain. The phase 07 veto scan runs outside this
budget (it is not an agent tool).
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Final

from hdt.core.config import ToolBudgetConfig

ABSTAIN_REASON: Final[str] = "tool_budget_exceeded"


@dataclass(frozen=True)
class ToolBudget:
    max_calls: int
    max_seconds: float

    def __post_init__(self) -> None:
        if self.max_calls < 1:
            raise ValueError("max_calls must be >= 1")
        if not self.max_seconds > 0:
            raise ValueError("max_seconds must be > 0")

    @classmethod
    def from_config(cls, config: ToolBudgetConfig) -> ToolBudget:
        return cls(max_calls=config.max_calls, max_seconds=float(config.max_seconds))


@dataclass(frozen=True)
class Admission:
    allowed: bool
    timeout_s: float
    """Seconds the admitted call may run before the budget is exhausted."""


class BudgetEnforcer:
    """Counts calls and cumulative tool time; `clock` is a monotonic seconds source (injectable)."""

    def __init__(self, budget: ToolBudget, clock: Callable[[], float] = time.monotonic) -> None:
        self.budget = budget
        self._clock = clock
        self._calls = 0
        self._used_s = 0.0
        self._exceeded = False
        self._started_at: float | None = None

    @property
    def calls(self) -> int:
        return self._calls

    @property
    def used_seconds(self) -> float:
        return self._used_s

    @property
    def exceeded(self) -> bool:
        return self._exceeded

    @property
    def abstain_reason(self) -> str | None:
        """Non-None once the budget was exceeded: the agent must abstain with this reason."""
        return ABSTAIN_REASON if self._exceeded else None

    def admit(self) -> Admission:
        """Admit the next call or mark the budget exceeded (a denied call is itself the excess)."""
        if self._started_at is not None:
            raise RuntimeError("a tool call is already in progress for this agent")
        remaining = self.budget.max_seconds - self._used_s
        if self._exceeded or self._calls >= self.budget.max_calls or remaining <= 0:
            self._exceeded = True
            return Admission(allowed=False, timeout_s=0.0)
        self._calls += 1
        self._started_at = self._clock()
        return Admission(allowed=True, timeout_s=remaining)

    def finish(self, *, timed_out: bool = False) -> float:
        """Charge the running call; returns its duration in seconds."""
        if self._started_at is None:
            raise RuntimeError("no tool call in progress")
        elapsed = max(0.0, self._clock() - self._started_at)
        self._started_at = None
        self._used_s += elapsed
        if timed_out or self._used_s > self.budget.max_seconds:
            self._exceeded = True
        return elapsed
