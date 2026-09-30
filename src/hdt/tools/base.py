"""Tool contract shared by the live and the replay registry.

A tool has a `name`, a Pydantic argument model, a Pydantic data model and a `mode` (`live` or `replay`).
Tools never receive the wall clock or a free `as_of` from the LLM: every call runs inside a `ToolContext`
fixed by code for one agent in one council event, and reads are bounded by the event's `as_of`
(look-ahead is blocked at the capability layer, see `hdt.tools.pit`).

Everything a tool returns is schema-typed data. Text that originates outside the system (news titles,
article text, security descriptions) passes through `sanitize_text`: NFC, control/format characters removed
(zero-width and bidi overrides are a prompt-injection vector), blank runs collapsed, length-truncated.
Tool outputs never contain wall-clock values, so the same event replayed on any day yields the same bytes.
"""

from __future__ import annotations

import re
import unicodedata
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, Any, ClassVar, Final, Literal, cast, get_args

from pydantic import AfterValidator

from hdt.contracts.common import AgentName, ContractModel
from hdt.contracts.forecast import LakeRef
from hdt.core.clock import ensure_utc
from hdt.core.ids import canonical_json, canonical_sha256

ToolMode = Literal["live", "replay"]
ToolStatus = Literal["ok", "not_available", "error", "budget_exceeded"]
TOOL_MODES: Final[tuple[ToolMode, ...]] = get_args(ToolMode)

TRUNCATION_MARK: Final[str] = " [truncated]"
_DROPPED_CATEGORIES: Final[frozenset[str]] = frozenset({"Cc", "Cf", "Co", "Cs", "Cn"})
_SPACES: Final = re.compile(r"[ \t]+")
_BLANK_LINES: Final = re.compile(r"\n{3,}")


class ToolOutcomeError(Exception):
    """Base of the expected tool outcomes other than `ok`; the message is shown to the agent."""

    status: ClassVar[ToolStatus] = "error"


class NotAvailableError(ToolOutcomeError):
    """Nothing usable was recorded at or before the event's `as_of` (never a reason to go live)."""

    status: ClassVar[ToolStatus] = "not_available"


class ToolInputError(ToolOutcomeError):
    """The arguments are outside what this agent may read for this event."""


class LookAheadError(ToolInputError):
    """A read after the event's `as_of` was requested."""


class ToolBackendError(ToolOutcomeError):
    """A backend failed or broke its contract (for example returned data recorded after `as_of`)."""


def sanitize_text(value: str, max_chars: int) -> str:
    """Prompt-safe text: NFC, no control/format/private-use characters, collapsed blanks, <= max_chars."""
    if max_chars < len(TRUNCATION_MARK) + 1:
        raise ValueError("max_chars too small")
    normalized = unicodedata.normalize("NFC", value).replace("\r\n", "\n").replace("\r", "\n")
    kept = [ch for ch in normalized if ch in "\n\t" or unicodedata.category(ch) not in _DROPPED_CATEGORIES]
    text = "\n".join(_SPACES.sub(" ", line).strip() for line in "".join(kept).split("\n"))
    text = _BLANK_LINES.sub("\n\n", text).strip()
    if len(text) > max_chars:
        text = text[: max_chars - len(TRUNCATION_MARK)].rstrip() + TRUNCATION_MARK
    return text


def _sanitizer(max_chars: int) -> AfterValidator:
    return AfterValidator(lambda value: sanitize_text(value, max_chars))


ShortText = Annotated[str, _sanitizer(200)]
"""Names, titles, labels."""
Text = Annotated[str, _sanitizer(2000)]
"""Descriptions and summaries."""
ArticleText = Annotated[str, _sanitizer(8000)]
"""Article content: at most 8k characters enter a prompt (phase 07)."""


@dataclass(frozen=True)
class ToolContext:
    """Fixed by code for one agent in one council event; the LLM cannot change any of it.

    `pinned` is the event's immutable capture manifest: lake records the live meeting itself created after
    `as_of` (only `fetch_source` does), taken from the stored `AgentForecast.capture_manifest` of the
    decision card (`hdt.tools.replay.replay_context`). A replay may open exactly those records (verified by
    sha256, event and item) and nothing else recorded after `as_of`; the live meeting starts empty.
    """

    mode: ToolMode
    agent: AgentName
    event_id: str
    coin_id: int
    as_of: datetime
    pinned: tuple[LakeRef, ...] = ()

    def __post_init__(self) -> None:
        if self.mode not in TOOL_MODES:
            raise ValueError(f"unknown tool mode {self.mode!r}")
        if not self.event_id:
            raise ValueError("event_id must not be empty")
        if self.coin_id <= 0:
            raise ValueError("coin_id must be positive")
        object.__setattr__(self, "agent", AgentName(self.agent))
        object.__setattr__(self, "as_of", ensure_utc(self.as_of))

    def read_as_of(self, requested: datetime | None) -> datetime:
        """The instant a tool reads at: the event's `as_of`, or an earlier one; later raises."""
        if requested is None:
            return self.as_of
        requested = ensure_utc(requested)
        if requested > self.as_of:
            raise LookAheadError(
                f"as_of {requested.isoformat()} is after the event as_of {self.as_of.isoformat()}"
            )
        return requested

    def event_coin(self, requested: int | None) -> int:
        """Tools read only the event coin: `requested` must be omitted or equal to it."""
        if requested is not None and requested != self.coin_id:
            raise ToolInputError(f"only the event coin {self.coin_id} can be read, not {requested}")
        return self.coin_id

    def pinned_capture(self, source: str, route: str, key: str) -> LakeRef | None:
        """The manifest entry for one lake record of this event, if the live meeting created it."""
        for ref in self.pinned:
            if (ref.source, ref.route, ref.key) == (source, route, key):
                return ref
        return None


class ToolArgs(ContractModel):
    """Base of tool argument models (frozen, unknown fields rejected)."""


class ToolData(ContractModel):
    """Base of tool data models (frozen, unknown fields rejected)."""


class ToolResult(ContractModel):
    """What the agent sees for one call. `result_id` is what a `tool` claim cites as its `ref`."""

    tool: str
    status: ToolStatus
    result_id: str | None = None
    data: dict[str, Any] | None = None
    message: str | None = None

    def prompt_text(self) -> str:
        """Canonical JSON for the labeled data field of a prompt (identical bytes on every replay)."""
        return canonical_json(self.model_dump(mode="json", exclude_none=True)).decode("utf-8")


def result_id(tool: str, output_sha256: str) -> str:
    return f"{tool}:{output_sha256[:24]}"


def args_sha256(tool: str, args: Mapping[str, Any]) -> str:
    """Hash of a call's input as recorded in `ToolCallRecord.input_sha256`."""
    return canonical_sha256({"tool": tool, "args": dict(args)})


class Tool[A: ToolArgs, D: ToolData](ABC):
    """One agent tool. Subclasses declare the class attributes and implement `run`."""

    name: ClassVar[str]
    description: ClassVar[str]
    args_model: ClassVar[type[ToolArgs]]
    data_model: ClassVar[type[ToolData]]
    modes: ClassVar[frozenset[ToolMode]] = frozenset(TOOL_MODES)

    def __init__(self, mode: ToolMode) -> None:
        if mode not in self.modes:
            raise ValueError(f"tool {self.name} has no {mode} implementation")
        self.mode: ToolMode = mode

    def parse_args(self, raw: Mapping[str, Any]) -> A:
        return cast(A, self.args_model.model_validate(dict(raw)))

    @abstractmethod
    async def run(self, ctx: ToolContext, args: A) -> D:
        """Produce the data, or raise a `ToolOutcomeError` subclass for an expected non-ok outcome."""

    def json_schema(self) -> dict[str, Any]:
        """Function-calling spec: name, description and the JSON schema of the arguments."""
        return {
            "name": self.name,
            "description": self.description,
            "parameters": self.args_model.model_json_schema(),
        }
