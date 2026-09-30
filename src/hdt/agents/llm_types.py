"""The structured LLM call interface shared by every LLM pipeline (council agents, news extractor and
judges, veto scan, reflection).

`hdt.agents.llm_router.LlmRouter` (phase 05) is the only implementation that talks to an LLM gateway
(OpenRouter, or DeepSeek directly per role `gateway`). Other phases depend on the `StructuredLlm` protocol
so their logic is tested with fakes and wired to the router only in their service entry point.

Rules every implementation keeps (Design Contract sections 3, 6, 8; phase 05 Red Team Delta):
- Live: one chat completion on the role's gateway with its structured-output form (OpenRouter
  `response_format` json_schema strict generated from `schema` plus the provider preferences of the role's
  pinned `RoleModelConfig`; DeepSeek `json_object` with the schema in the prompt); the reply is parsed with
  `schema`. Every billed generation is recorded in `llm_calls` (pipeline, role, event_id, requested and
  returned model, provider, generation id, usage, cost); the reply is stored in `llm_cache` under
  `(prompt_hash, model_slug, provider, params_hash)`.
- Replay: the reply comes only from `llm_cache`; a miss raises `LlmCacheMissError`, never a live call.
- A reply that fails `schema` is retried once; a second failure raises `LlmOutputError` (callers abstain).
- Transport, provider, key or budget failures raise `LlmUnavailableError` (callers abstain or fail closed).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal, Protocol

from pydantic import BaseModel

from hdt.contracts.forecast import LlmUsage
from hdt.core.config import LlmRole
from hdt.settings.schemas import RoleModelConfig

LlmPipeline = Literal["council", "news", "reflection"]


class LlmError(RuntimeError):
    """Base class of every structured-call failure."""


class LlmUnavailableError(LlmError):
    """No usable reply: transport, provider, key, price cap or rejected model (callers abstain)."""


class LlmRoleClosedError(LlmUnavailableError):
    """The role's structured-output self-test failed (`reason`): no request is sent (callers abstain)."""

    def __init__(self, role: str, reason: str) -> None:
        super().__init__(f"{role}: role closed ({reason})")
        self.role = role
        self.reason = reason


class LlmOutputError(LlmError):
    """The model replied twice with output that does not match the schema (callers abstain)."""


class LlmCacheMissError(LlmError):
    """Replay asked for a generation that is not in `llm_cache`: a hard error, never a live call."""


@dataclass(frozen=True)
class Generation[T: BaseModel]:
    """One parsed structured reply and its provenance."""

    output: T
    model_slug: str
    """The model requested from the gateway (a pinned slug or DeepSeek model name)."""
    model_returned: str
    """The model the gateway reports as having served the call."""
    provider: str
    """The provider that actually served the call."""
    generation_id: str
    usage: LlmUsage
    prompt_hash: str
    """sha256 (hex) of the canonical request messages."""
    params_hash: str
    """sha256 (hex) of every other request parameter (schema, provider preferences, sampling)."""
    cached: bool
    """True when the reply came from `llm_cache` (replay, or a live repeat of an identical request)."""


class StructuredLlm(Protocol):
    async def structured[T: BaseModel](
        self,
        *,
        role: LlmRole,
        pipeline: LlmPipeline,
        event_id: str | None,
        config: RoleModelConfig,
        system: str,
        user: str,
        schema: type[T],
    ) -> Generation[T]:
        """One structured completion for `role` with the role's pinned model configuration.

        `system` holds only fixed instructions; external text and other agents' output go into `user` inside
        labeled data fields (never concatenated into `system`).
        """
        ...


@dataclass(frozen=True)
class ToolCallRequest:
    """One function call the model asked for in a tool step (`arguments` is the raw JSON text)."""

    call_id: str
    name: str
    arguments: str


@dataclass(frozen=True)
class ToolStep:
    """One reply of the bounded ReAct loop: tool calls to run, or plain content when the model is done."""

    content: str | None
    tool_calls: tuple[ToolCallRequest, ...]
    model_slug: str
    model_returned: str
    provider: str
    generation_id: str
    usage: LlmUsage
    prompt_hash: str
    params_hash: str
    cached: bool
    reasoning_content: str | None = None
    """DeepSeek thinking mode: the chain of thought of this step. The caller must send it back on the
    assistant message of this step in every later request of the loop (DeepSeek refuses the request
    otherwise); None when thinking is off or on OpenRouter."""


class ToolChatLlm(StructuredLlm, Protocol):
    """`StructuredLlm` plus the tool-calling chat step the council agents' ReAct loop needs.

    Same recording, cache and replay rules as `structured`: a tool step is cached under the same key shape
    (the tool specs are part of `params_hash`), so a replayed loop requests the same tools with the same
    arguments and the replay registry returns the same results.
    """

    async def tool_step(
        self,
        *,
        role: LlmRole,
        pipeline: LlmPipeline,
        event_id: str | None,
        config: RoleModelConfig,
        messages: Sequence[Mapping[str, Any]],
        tools: Sequence[Mapping[str, Any]],
    ) -> ToolStep:
        """One chat completion with `tools` (OpenAI function specs: name, description, parameters)."""
        ...
