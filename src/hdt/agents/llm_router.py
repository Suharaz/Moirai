"""`LlmRouter`: the only code that talks to an LLM gateway (phase 05, Design Contract sections 3, 6, 8).

Every LLM pipeline (council agents, news extractor and judges, veto scan, reflection) calls the model
through this router with the role's pinned `RoleModelConfig`, whose `gateway` picks where the request goes:
OpenRouter (the design default) or the DeepSeek API directly (temporary owner decision 2026-09-28).

- OpenRouter: requests go to `https://openrouter.ai/api/v1/chat/completions` through the `openai` SDK with
  the key of vault scope `llm` (`openrouter_api_key`), the role's pinned slug (`openrouter/auto`, other
  OpenRouter routers and every `:` variant such as `:free` or `:online` are refused before any request),
  sampling parameters and provider preferences (`require_parameters`, `allow_fallbacks=false` and
  `data_collection=deny` are enforced here, `zdr`, `order`/`only`, `max_price`). Structured calls send
  `response_format` json_schema strict generated from the Pydantic schema (`strict_json_schema`); tool
  steps send the tool function specs. The cost is OpenRouter's `usage.cost`.
- DeepSeek: requests go to `https://api.deepseek.com/chat/completions` with the key `llm/deepseek_api_key`,
  the role's DeepSeek model name, `max_tokens` and the thinking toggle (`thinking` disabled: `temperature`
  is sent; enabled: `reasoning_effort`, and the chain of thought (`reasoning_content`) of each tool step is
  returned on `ToolStep` so the caller passes it back, as DeepSeek requires within a tool loop). No
  provider object, fallback list or `usage.include` is sent. DeepSeek JSON Output takes no schema, so
  structured calls send `response_format` `json_object` and append the JSON Schema to the system message
  (`json_instruction`); the reply is validated by the same Pydantic parse, and an empty or invalid reply is
  a parse failure like any other. The provider is recorded as `deepseek` and the cost is computed from
  `config/deepseek.yaml` (cache-hit, cache-miss and output tokens at the peak or off-peak rate).
- A reply served by a model other than one of the pinned slugs (the primary or a configured fallback, a
  `-YYYY-MM-DD` / `-YYYYMMDD` date suffix allowed, see `served_slug_pinned`), by an OpenRouter router or a
  variant, or by a provider outside `provider.only` (compared in slug form), is refused (recorded as
  `invalid_output`, never cached, then `LlmUnavailableError`); on DeepSeek a reply must name a DeepSeek
  model. A truncated tool step is recorded, never cached.
- A role closed by its structured-output self-test (`RoleHealth`, attached with `gate_with`) raises
  `LlmRoleClosedError` before any cache read or request, whatever the pipeline.
- Every billed generation is recorded in `llm_calls` (pipeline, role, event, requested and returned model,
  provider, generation id, tokens, cost, prompt and params hashes, latency, status) and counted in
  `hdt_llm_tokens_total` / `hdt_llm_cost_usd_total`; a usable reply is stored in `llm_cache` under
  `(prompt_hash, model_slug, provider routing, params_hash)` in the same transaction. The routing part is
  `deepseek` for DeepSeek requests and the OpenRouter provider routing otherwise, and the DeepSeek params
  carry the gateway, so the two gateways never share a cache entry. When Langfuse is configured every
  generation is also traced there.
- Live calls read the cache first (an identical request is served from it, `cached=True`); replay reads
  only the cache and a miss raises `LlmCacheMissError`, never a live call.
- A structured reply that fails the schema is retried once; the second failure raises `LlmOutputError`.
  Transport, key, provider or model refusals raise `LlmUnavailableError`.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Final, Literal, Protocol

import openai
from openai import AsyncOpenAI
from pydantic import BaseModel, ValidationError
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session, sessionmaker

from hdt.agents.llm_types import (
    Generation,
    LlmCacheMissError,
    LlmOutputError,
    LlmPipeline,
    LlmRoleClosedError,
    LlmUnavailableError,
    ToolCallRequest,
    ToolStep,
)
from hdt.contracts.common import ContractModel
from hdt.contracts.forecast import LlmUsage
from hdt.core.clock import utcnow
from hdt.core.config import LlmRole, read_env_value
from hdt.core.ids import canonical_sha256
from hdt.db.models.llm import LlmCacheRow, LlmCallRow
from hdt.db.session import transaction
from hdt.llm.deepseek import DEEPSEEK_API_BASE, DeepSeekFile, deepseek_config, served_names
from hdt.llm.openrouter_catalog import OPENROUTER_API_BASE
from hdt.ops.metrics import LLM_COST, LLM_TOKENS
from hdt.settings.schemas import Gateway, RoleModelConfig, model_problem, slug_problem
from hdt.vault.loader import SecretIntegrityError, SecretLoader, SecretNotAvailableError
from hdt.vault.redact import redact, register_secret

if TYPE_CHECKING:
    import httpx2
    from langfuse import Langfuse

log = logging.getLogger(__name__)

LLM_SCOPE: Final = "llm"
SECRET_NAMES: Final[Mapping[Gateway, str]] = {
    "openrouter": "openrouter_api_key",
    "deepseek": "deepseek_api_key",
}
"""The `llm` scope vault item holding each gateway's API key."""
GATEWAY_LABELS: Final[Mapping[Gateway, str]] = {"openrouter": "OpenRouter", "deepseek": "DeepSeek"}
RouterMode = Literal["live", "replay"]
ROLE_PIPELINES: Final[Mapping[LlmRole, LlmPipeline]] = {
    "crowding": "council",
    "technical": "council",
    "micro": "council",
    "fundamental": "council",
    "news": "council",
    "macro": "council",
    "news_extractor": "news",
    "news_judge_a": "news",
    "news_judge_b": "news",
    "reflection": "reflection",
}
MAX_ATTEMPTS: Final[int] = 2
"""A structured reply failing its schema is retried once (two generations at most)."""
_ERROR_CHARS: Final[int] = 300
_COMPLETE_TOOL_STEP: Final[frozenset[str | None]] = frozenset({"stop", "tool_calls", None})
"""A tool step ending otherwise (`length`, `content_filter`, `error`) is truncated: recorded, never cached."""
# Keywords dropped from the strict schema sent to providers: not every provider behind OpenRouter accepts
# them in strict mode, and every one of them is enforced by the Pydantic parse of the reply anyway.
_UNSENT_KEYWORDS: Final[frozenset[str]] = frozenset(
    {
        "default",
        "title",
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "minLength",
        "maxLength",
        "minItems",
        "maxItems",
        "pattern",
        "format",
        "uniqueItems",
    }
)
_NAMED_CHILDREN: Final[frozenset[str]] = frozenset({"properties", "$defs", "definitions"})


# --------------------------------------------------------------------------- request shapes


def strict_json_schema(schema: type[BaseModel]) -> dict[str, Any]:
    """The json_schema strict form of `schema`: every object closed, every property listed as required
    (optional fields stay nullable through their `anyOf ... null`), unsupported keywords removed."""
    strict: dict[str, Any] = _strict(schema.model_json_schema())
    return strict


def _strict(node: Any) -> Any:
    if isinstance(node, list):
        return [_strict(item) for item in node]
    if not isinstance(node, dict):
        return node
    out: dict[str, Any] = {}
    for key, value in node.items():
        if key in _UNSENT_KEYWORDS:
            continue
        if key in _NAMED_CHILDREN and isinstance(value, dict):
            out[key] = {name: _strict(child) for name, child in value.items()}
        else:
            out[key] = _strict(value)
    if "$ref" in out:  # strict mode allows no keyword next to a reference
        return {"$ref": out["$ref"]}
    if out.get("type") == "object" or "properties" in out:
        properties = out.setdefault("properties", {})
        out["required"] = list(properties)
        out["additionalProperties"] = False
    return out


def response_format(schema: type[BaseModel]) -> dict[str, Any]:
    return {
        "type": "json_schema",
        "json_schema": {"name": schema.__name__[:64], "strict": True, "schema": strict_json_schema(schema)},
    }


def json_instruction(schema: type[BaseModel]) -> str:
    """The output format paragraph of a DeepSeek structured call. DeepSeek JSON Output
    (`response_format` `json_object`) takes no schema: the prompt must name JSON and show the expected
    shape, so the full Pydantic JSON Schema (constraints included) is written into the system message."""
    rendered = json.dumps(schema.model_json_schema(), sort_keys=True, separators=(",", ":"))
    return (
        "Output format: reply with exactly one JSON object and nothing else (no prose, no code fences). "
        f"The json object must validate against this JSON Schema:\n{rendered}"
    )


def structured_messages(
    config: RoleModelConfig, system: str, user: str, schema: type[BaseModel]
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """The messages and the structured-output request field of a structured call on the role's gateway."""
    if config.gateway == "deepseek":
        system = f"{system}\n\n{json_instruction(schema)}"
        extra: dict[str, Any] = {"response_format": {"type": "json_object"}}
    else:
        extra = {"response_format": response_format(schema)}
    return [{"role": "system", "content": system}, {"role": "user", "content": user}], extra


def provider_routing(config: RoleModelConfig) -> str:
    """The requested routing as the cache key part: `deepseek` on the DeepSeek gateway, otherwise the
    OpenRouter provider routing (`only:...`, `order:...` or `any`)."""
    if config.gateway == "deepseek":
        return "deepseek"
    if config.provider.only:
        return "only:" + ",".join(config.provider.only)
    if config.provider.order:
        return "order:" + ",".join(config.provider.order)
    return "any"


def check_pinned(config: RoleModelConfig) -> None:
    """Refuse, before any request is sent. OpenRouter: routers, floating aliases, every `:` variant
    (`:free`, `:online`, `:nitro`, `:floor`, `:thinking`, ...), and provider preferences other than the fixed
    `allow_fallbacks=false` / `data_collection=deny` (Design Contract section 8). DeepSeek: a name outside
    `DEEPSEEK_MODELS` and any fallback list."""
    if config.gateway == "deepseek":
        problem = model_problem("deepseek", config.model)
        if problem:
            raise LlmUnavailableError(f"model {config.model!r} refused: {problem}")
        if config.fallback_models:
            raise LlmUnavailableError("fallback_models refused: the deepseek gateway serves one model")
        return
    for slug in config.slugs():
        problem = slug_problem(slug) or _variant_problem(slug)
        if problem:
            raise LlmUnavailableError(f"model {slug!r} refused: {problem}")
    if config.allow_fallbacks:
        raise LlmUnavailableError("provider preferences refused: allow_fallbacks must be false")
    if config.data_collection != "deny":
        raise LlmUnavailableError(
            f"provider preferences refused: data_collection must be 'deny', not {config.data_collection!r}"
        )


def _variant_problem(slug: str) -> str | None:
    """OpenRouter `:` variants change routing or attach tools (`:online` web search): never replayable."""
    if ":" in slug:
        return f"OpenRouter variant ':{slug.split(':', 1)[1]}' is not allowed"
    return None


def provider_slug(name: str) -> str:
    """A provider name in slug form: requests take slugs (`amazon-bedrock`, `google-vertex/us-east5`) while
    replies report display names (`Amazon Bedrock`); both sides are compared in this form."""
    out: list[str] = []
    dash = False
    for char in name.strip().casefold():
        if char.isalnum() or char == "/":
            if dash and out and out[-1] != "/":
                out.append("-")
            out.append(char)
            dash = False
        else:
            dash = True
    return "".join(out)


def _provider_allowed(provider: str, only: Sequence[str]) -> bool:
    """Base slug matching as OpenRouter does it: `google-vertex` allows `google-vertex/us-east5`. A reply
    names the provider by display name only (`Google Vertex`, no region), so a regional entry
    (`google-vertex/us-east5`) also allows a reply naming its base provider."""
    served = provider_slug(provider)
    if not served:
        return False
    for entry in only:
        allowed = provider_slug(entry)
        if served == allowed or served.startswith(allowed + "/") or served == allowed.split("/", 1)[0]:
            return True
    return False


_DATE_SUFFIX: Final[re.Pattern[str]] = re.compile(
    r"-(?P<y>[0-9]{4})-?(?P<m>[0-9]{2})-?(?P<d>[0-9]{2})", re.ASCII
)


def _date_suffix(text: str) -> bool:
    """`-YYYY-MM-DD` or `-YYYYMMDD` (ASCII digits) naming a real calendar date."""
    match = _DATE_SUFFIX.fullmatch(text)
    if match is None or (text.count("-") not in (1, 3)):
        return False
    try:
        date(int(match["y"]), int(match["m"]), int(match["d"]))
    except ValueError:
        return False
    return True


def served_slug_pinned(config: RoleModelConfig, model_returned: str) -> bool:
    """Whether `model_returned` is one of the pinned slugs (the primary or a configured fallback).

    Canonicalization rule (the only one): the served slug equals a pinned slug exactly, or is that pinned
    slug followed by one date suffix, `-YYYY-MM-DD` or `-YYYYMMDD` (a dated snapshot of the same model).
    OpenRouter publishes a permanent `canonical_slug` per model in its models API, but the catalog here does
    not carry it, so the served slug is matched against the configured slugs themselves. Another model of
    the same developer never matches.
    """
    for slug in config.slugs():
        if model_returned == slug:
            return True
        if model_returned.startswith(slug) and _date_suffix(model_returned[len(slug) :]):
            return True
    return False


def returned_problem(config: RoleModelConfig, model_returned: str, provider: str) -> str | None:
    """Why a reply must not be used although the gateway served it, or None when it is acceptable.

    OpenRouter: the served model must be one of the pinned slugs (`served_slug_pinned`: exact, or with a
    date suffix), never an OpenRouter router or a variant, and the provider must be inside the
    `provider.only` allowlist. DeepSeek serves only its own models and no provider routing exists; the
    reply must name the requested model or one of its documented aliases (`hdt.llm.deepseek.served_names`),
    so a Flash request never accepts a Pro reply and the reverse.
    """
    if config.gateway == "deepseek":
        accepted = served_names(config.model)
        if model_returned not in accepted:
            return f"served model {model_returned!r} is not {config.model!r} (accepted: {sorted(accepted)})"
        return None
    if "/" not in model_returned:
        return f"served model {model_returned!r} is not a 'developer/model' slug"
    if model_returned.startswith("openrouter/"):
        return f"served by the OpenRouter router {model_returned!r}"
    variant = _variant_problem(model_returned)
    if variant:
        return f"served by {model_returned!r}: {variant}"
    if not served_slug_pinned(config, model_returned):
        return f"served by {model_returned!r}, outside the pinned slugs {list(config.slugs())}"
    if config.provider.only and not _provider_allowed(provider, config.provider.only):
        return f"served by provider {provider!r}, outside provider.only {list(config.provider.only)}"
    return None


class CachedToolCall(ContractModel):
    call_id: str
    name: str
    arguments: str


class CachedReply(ContractModel):
    """The normalized reply stored in `llm_cache.response` (and what every call is parsed from)."""

    content: str | None
    tool_calls: tuple[CachedToolCall, ...] = ()
    model_returned: str
    provider: str
    generation_id: str
    usage: LlmUsage
    finish_reason: str | None = None
    """`stop`, `tool_calls`, `length`, ... (None when the provider reports none)."""
    reasoning_content: str | None = None
    """DeepSeek thinking mode: the chain of thought, passed back within a tool loop (None elsewhere)."""


@dataclass(frozen=True)
class _Request:
    role: LlmRole
    pipeline: LlmPipeline
    event_id: str | None
    config: RoleModelConfig
    messages: list[dict[str, Any]]
    extra: dict[str, Any]
    """`response_format` or `tools` / `tool_choice`: sent as request fields and hashed into params."""
    prompt_hash: str
    params_hash: str
    routing: str

    def key(self) -> CacheKey:
        return CacheKey(
            prompt_hash=self.prompt_hash,
            model_slug=self.config.model,
            provider=self.routing,
            params_hash=self.params_hash,
            role=self.role,
        )


def _params(config: RoleModelConfig, extra: Mapping[str, Any]) -> dict[str, Any]:
    """Every request parameter besides the messages, hashed into `params_hash`. The OpenRouter form is
    unchanged since recording began (replays of recorded meetings keep hitting the cache)."""
    if config.gateway == "deepseek":
        params: dict[str, Any] = {
            "gateway": config.gateway,
            "max_tokens": config.max_tokens,
            "thinking": config.thinking,
            **extra,
        }
        if config.thinking == "disabled":
            params["temperature"] = config.temperature
        return params
    params = {
        "temperature": config.temperature,
        "max_tokens": config.max_tokens,
        "provider": config.provider_preferences(),
        **extra,
    }
    if config.fallback_models:
        params["models"] = list(config.slugs())
    return params


def _sampling(config: RoleModelConfig) -> dict[str, Any]:
    """The gateway-specific request fields (`temperature`, `extra_body`) of one request."""
    if config.gateway == "deepseek":
        if config.thinking == "disabled":
            return {"temperature": config.temperature, "extra_body": {"thinking": {"type": "disabled"}}}
        # Thinking mode ignores temperature: it is not sent.
        return {"extra_body": {"thinking": {"type": "enabled"}, "reasoning_effort": config.thinking}}
    extra_body: dict[str, Any] = {"provider": config.provider_preferences(), "usage": {"include": True}}
    if config.fallback_models:
        extra_body["models"] = list(config.slugs())
    return {"temperature": config.temperature, "extra_body": extra_body}


@dataclass(frozen=True)
class CacheKey:
    prompt_hash: str
    model_slug: str
    provider: str
    """Requested provider routing (`provider_routing`)."""
    params_hash: str
    role: str


@dataclass(frozen=True)
class LlmCallRecord:
    """One `llm_calls` row."""

    generation_id: str
    called_at: datetime
    pipeline: str
    role: str
    event_id: str | None
    model_slug: str
    model_returned: str | None
    provider: str | None
    prompt_tokens: int
    completion_tokens: int
    cost_usd: float | None
    prompt_hash: str
    params_hash: str
    latency_ms: int
    status: str


class LlmStore(Protocol):
    """Where the router records generations and caches replies (blocking; called in worker threads)."""

    def cached(self, key: CacheKey) -> CachedReply | None: ...

    def record(self, call: LlmCallRecord, cache: tuple[CacheKey, CachedReply] | None) -> None:
        """Insert the call row and, when given, the cache entry, in one transaction (both insert-only)."""
        ...


class RoleGate(Protocol):
    """Which roles may call the model (`hdt.agents.model_test.RoleHealth`)."""

    async def closed_reason(self, role: LlmRole, config: RoleModelConfig) -> str | None:
        """None when `role` may run with `config`, otherwise why it is closed."""
        ...


class PgLlmStore:
    """`LlmStore` on the `llm_calls` / `llm_cache` tables (migration 0007_llm)."""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._factory = session_factory

    def cached(self, key: CacheKey) -> CachedReply | None:
        with self._factory() as session:
            response = session.scalar(
                select(LlmCacheRow.response).where(
                    LlmCacheRow.prompt_hash == key.prompt_hash,
                    LlmCacheRow.model_slug == key.model_slug,
                    LlmCacheRow.provider == key.provider,
                    LlmCacheRow.params_hash == key.params_hash,
                )
            )
        return None if response is None else CachedReply.model_validate(response)

    def record(self, call: LlmCallRecord, cache: tuple[CacheKey, CachedReply] | None) -> None:
        with transaction(self._factory) as session:
            session.execute(
                pg_insert(LlmCallRow)
                .values(**asdict(call))
                .on_conflict_do_nothing(index_elements=["generation_id"])
            )
            if cache is not None:
                key, reply = cache
                session.execute(
                    pg_insert(LlmCacheRow)
                    .values(
                        prompt_hash=key.prompt_hash,
                        model_slug=key.model_slug,
                        provider=key.provider,
                        params_hash=key.params_hash,
                        role=key.role,
                        generation_id=reply.generation_id,
                        response=reply.model_dump(mode="json"),
                        created_at=call.called_at,
                    )
                    .on_conflict_do_nothing(
                        index_elements=["prompt_hash", "model_slug", "provider", "params_hash"]
                    )
                )


# --------------------------------------------------------------------------- router


@dataclass
class _Endpoint:
    """One gateway's base URL, key source and SDK client (rebuilt when the key rotates)."""

    label: str
    base_url: str
    api_key: Callable[[], str] | None
    headers: dict[str, str]
    client: AsyncOpenAI | None = None
    client_key: str | None = None


class LlmRouter:
    """`StructuredLlm` + `ToolChatLlm` over the role's gateway; one instance per service and mode.

    `api_key` returns the active `llm/openrouter_api_key` and `deepseek_api_key` the active
    `llm/deepseek_api_key` (each read per request, so a rotated key is used on the next call); a live router
    needs at least one, and a request on a gateway without a key source is unavailable. Replay sends no
    request and needs none. `http_client` is an `httpx2.AsyncClient` handed to the SDK (tests inject a mock
    transport there). `deepseek_prices` defaults to `config/deepseek.yaml`, read on the first DeepSeek
    reply. Records and the cache go to `llm_calls` / `llm_cache` through `session_factory` (`PgLlmStore`)
    unless another `store` is given.
    """

    def __init__(
        self,
        *,
        mode: RouterMode,
        session_factory: sessionmaker[Session] | None = None,
        api_key: Callable[[], str] | None,
        deepseek_api_key: Callable[[], str] | None = None,
        http_client: httpx2.AsyncClient | None = None,
        langfuse: Langfuse | None = None,
        base_url: str = OPENROUTER_API_BASE,
        deepseek_base_url: str = DEEPSEEK_API_BASE,
        deepseek_prices: DeepSeekFile | None = None,
        store: LlmStore | None = None,
    ) -> None:
        if mode not in ("live", "replay"):
            raise ValueError(f"unknown router mode {mode!r}")
        if mode == "live" and api_key is None and deepseek_api_key is None:
            raise ValueError("a live router needs an API key source")
        if store is None:
            if session_factory is None:
                raise ValueError("the router needs a session_factory (llm_calls / llm_cache) or a store")
            store = PgLlmStore(session_factory)
        self.mode: RouterMode = mode
        self._store = store
        self._http_client = http_client
        self._langfuse = langfuse
        self._endpoints: dict[Gateway, _Endpoint] = {
            "openrouter": _Endpoint(
                GATEWAY_LABELS["openrouter"], base_url, api_key, {"X-Title": "Moirai"}
            ),
            "deepseek": _Endpoint(GATEWAY_LABELS["deepseek"], deepseek_base_url, deepseek_api_key, {}),
        }
        self._deepseek_prices = deepseek_prices
        self._client_lock = asyncio.Lock()
        self._gate: RoleGate | None = None

    def gate_with(self, gate: RoleGate | None) -> None:
        """Close roles at the send point: every live call of a role `gate` reports closed raises
        `LlmRoleClosedError` before the cache is read or a request is sent (`RoleHealth` attaches itself)."""
        self._gate = gate

    @classmethod
    def from_env(cls, mode: RouterMode, session_factory: sessionmaker[Session]) -> LlmRouter:
        """The service router: every gateway key from vault scope `llm` (live only), Langfuse from
        `LANGFUSE_*`. A key that was never entered makes only its gateway's requests unavailable."""
        keys: dict[Gateway, Callable[[], str] | None] = {"openrouter": None, "deepseek": None}
        if mode == "live":
            loader = SecretLoader.from_env(LLM_SCOPE, session_factory)
            for gateway, name in SECRET_NAMES.items():
                keys[gateway] = _vault_key(loader, name)
        return cls(
            mode=mode,
            session_factory=session_factory,
            api_key=keys["openrouter"],
            deepseek_api_key=keys["deepseek"],
            langfuse=langfuse_from_env(),
        )

    async def aclose(self) -> None:
        for endpoint in self._endpoints.values():
            if endpoint.client is not None and self._http_client is None:
                await endpoint.client.close()
            endpoint.client = None
        if self._langfuse is not None:
            await asyncio.to_thread(self._langfuse.flush)

    # ------------------------------------------------------------------ public calls

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
        self_test: bool = False,
    ) -> Generation[T]:
        """See `StructuredLlm.structured`. `self_test=True` (live only, the model self-test) skips the role
        gate, always sends the request and stores nothing in the cache."""
        messages, extra = structured_messages(config, system, user, schema)
        request = self._request(role, pipeline, event_id, config, messages, extra)
        if not self_test:
            await self._check_gate(request)
        if not self_test or self.mode == "replay":
            cached = await self._cached(request)
            if cached is not None:
                try:
                    output = _parse(cached, schema)
                except (ValidationError, ValueError) as exc:
                    raise LlmOutputError(f"cached reply for {role} does not match {schema.__name__}") from exc
                return _generation(request, cached, output, cached=True)
        if self.mode == "replay":
            raise _miss(request)
        for attempt in range(1, MAX_ATTEMPTS + 1):
            reply, latency_ms = await self._send(request)
            try:
                output = _parse(reply, schema)
            except (ValidationError, ValueError) as exc:
                await self._record(request, reply, latency_ms, status="invalid_output", cache=False)
                log.warning(
                    "structured reply failed its schema",
                    extra={"role": role, "attempt": attempt, "error": redact(str(exc))[:_ERROR_CHARS]},
                )
                continue
            await self._record(request, reply, latency_ms, status="ok", cache=not self_test)
            return _generation(request, reply, output, cached=False)
        raise LlmOutputError(f"{role}: {MAX_ATTEMPTS} replies did not match {schema.__name__}")

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
        extra = {
            "tools": [{"type": "function", "function": dict(spec)} for spec in tools],
            "tool_choice": "auto",
        }
        request = self._request(role, pipeline, event_id, config, [dict(m) for m in messages], extra)
        await self._check_gate(request)
        reply = await self._cached(request)
        cached = reply is not None
        if reply is None:
            if self.mode == "replay":
                raise _miss(request)
            reply, latency_ms = await self._send(request)
            if reply.finish_reason not in _COMPLETE_TOOL_STEP:
                await self._record(request, reply, latency_ms, status="invalid_output", cache=False)
                raise LlmOutputError(f"{role}: tool step ended with finish_reason {reply.finish_reason!r}")
            await self._record(request, reply, latency_ms, status="ok", cache=True)
        return ToolStep(
            content=reply.content,
            tool_calls=tuple(ToolCallRequest(c.call_id, c.name, c.arguments) for c in reply.tool_calls),
            model_slug=config.model,
            model_returned=reply.model_returned,
            provider=reply.provider,
            generation_id=reply.generation_id,
            usage=reply.usage,
            prompt_hash=request.prompt_hash,
            params_hash=request.params_hash,
            cached=cached,
            reasoning_content=reply.reasoning_content,
        )

    # ------------------------------------------------------------------ internals

    async def _check_gate(self, request: _Request) -> None:
        if self._gate is None or self.mode != "live":
            return
        reason = await self._gate.closed_reason(request.role, request.config)
        if reason is not None:
            raise LlmRoleClosedError(request.role, reason)

    def _request(
        self,
        role: LlmRole,
        pipeline: LlmPipeline,
        event_id: str | None,
        config: RoleModelConfig,
        messages: list[dict[str, Any]],
        extra: dict[str, Any],
    ) -> _Request:
        check_pinned(config)
        return _Request(
            role=role,
            pipeline=pipeline,
            event_id=event_id,
            config=config,
            messages=messages,
            extra=extra,
            prompt_hash=canonical_sha256(messages),
            params_hash=canonical_sha256(_params(config, extra)),
            routing=provider_routing(config),
        )

    async def _cached(self, request: _Request) -> CachedReply | None:
        return await asyncio.to_thread(self._store.cached, request.key())

    async def _openai(self, gateway: Gateway) -> AsyncOpenAI:
        endpoint = self._endpoints[gateway]
        if endpoint.api_key is None:
            raise LlmUnavailableError(f"no {endpoint.label} key source configured")
        try:
            key = await asyncio.to_thread(endpoint.api_key)
        except (SecretNotAvailableError, SecretIntegrityError, KeyError) as exc:
            raise LlmUnavailableError(
                f"{endpoint.label} key unavailable: {type(exc).__name__}: {exc}"
            ) from exc
        if not key:
            raise LlmUnavailableError(f"{endpoint.label} key is empty")
        register_secret(key)
        async with self._client_lock:
            if endpoint.client is None or endpoint.client_key != key:
                if endpoint.client is not None and self._http_client is None:
                    await endpoint.client.close()
                endpoint.client = AsyncOpenAI(
                    api_key=key,
                    base_url=endpoint.base_url,
                    http_client=self._http_client,
                    max_retries=1,
                    default_headers=endpoint.headers or None,
                )
                endpoint.client_key = key
            return endpoint.client

    def _prices(self) -> DeepSeekFile:
        """The DeepSeek price table, checked before a request is sent (a generation must be priceable)."""
        if self._deepseek_prices is None:
            try:
                self._deepseek_prices = deepseek_config()
            except (OSError, ValueError) as exc:  # pydantic.ValidationError is a ValueError
                raise LlmUnavailableError(
                    f"DeepSeek price table config/deepseek.yaml unusable: {type(exc).__name__}"
                ) from exc
        return self._deepseek_prices

    async def _send(self, request: _Request) -> tuple[CachedReply, int]:
        config = request.config
        label = GATEWAY_LABELS[config.gateway]
        prices = self._prices() if config.gateway == "deepseek" else None
        client = await self._openai(config.gateway)
        observation = self._trace_start(request)
        requested_at = utcnow()
        started = time.monotonic()
        try:
            completion = await client.chat.completions.create(
                model=config.model,
                messages=request.messages,  # type: ignore[arg-type]
                max_tokens=config.max_tokens,
                timeout=config.timeout_s,
                **_sampling(config),
                **request.extra,
            )
        except openai.OpenAIError as exc:
            reason = redact(f"{type(exc).__name__}: {exc}")[:_ERROR_CHARS]
            self._trace_end(observation, None, error=reason)
            raise LlmUnavailableError(f"{label} call for {request.role} failed: {reason}") from exc
        latency_ms = round((time.monotonic() - started) * 1000)
        try:
            reply = _normalize(completion)
            if prices is not None:
                reply = _deepseek_reply(reply, completion, prices, config.model, requested_at)
        except LlmUnavailableError as exc:
            self._trace_end(observation, None, error=str(exc))
            raise
        except (AttributeError, TypeError, ValueError) as exc:
            reason = redact(f"{type(exc).__name__}: {exc}")[:_ERROR_CHARS]
            self._trace_end(observation, None, error=reason)
            raise LlmUnavailableError(f"{request.role}: unreadable {label} reply, {reason}") from exc
        problem = returned_problem(config, reply.model_returned, reply.provider)
        if problem:
            self._trace_end(observation, reply, error=problem)
            await self._record(request, reply, latency_ms, status="invalid_output", cache=False)
            raise LlmUnavailableError(f"{request.role}: reply refused, {problem}")
        self._trace_end(observation, reply)
        return reply, latency_ms

    async def _record(
        self, request: _Request, reply: CachedReply, latency_ms: int, *, status: str, cache: bool
    ) -> None:
        labels = {"pipeline": request.pipeline, "role": request.role, "model": request.config.model}
        LLM_TOKENS.labels(**labels, kind="prompt").inc(reply.usage.prompt_tokens)
        LLM_TOKENS.labels(**labels, kind="completion").inc(reply.usage.completion_tokens)
        if reply.usage.cost_usd is not None:
            LLM_COST.labels(**labels).inc(reply.usage.cost_usd)
        try:
            await asyncio.to_thread(self._write, request, reply, latency_ms, status, cache)
        except Exception as exc:
            # The generation is billed (and counted above) but not in `llm_calls`: fail the call so the
            # caller abstains instead of using a reply whose record and cache entry are missing.
            log.exception(
                "llm_calls record failed",
                extra={"role": request.role, "generation_id": reply.generation_id, "status": status},
            )
            raise LlmUnavailableError(
                f"{request.role}: generation {reply.generation_id} not recorded: {type(exc).__name__}"
            ) from exc

    def _write(
        self, request: _Request, reply: CachedReply, latency_ms: int, status: str, cache: bool
    ) -> None:
        call = LlmCallRecord(
            generation_id=reply.generation_id,
            called_at=utcnow(),
            pipeline=request.pipeline,
            role=request.role,
            event_id=request.event_id,
            model_slug=request.config.model,
            model_returned=reply.model_returned,
            provider=reply.provider,
            prompt_tokens=reply.usage.prompt_tokens,
            completion_tokens=reply.usage.completion_tokens,
            cost_usd=reply.usage.cost_usd,
            prompt_hash=request.prompt_hash,
            params_hash=request.params_hash,
            latency_ms=latency_ms,
            status=status,
        )
        self._store.record(call, (request.key(), reply) if cache else None)

    # ------------------------------------------------------------------ Langfuse

    def _trace_start(self, request: _Request) -> Any | None:
        if self._langfuse is None:
            return None
        try:
            return self._langfuse.start_observation(
                name=f"{request.pipeline}.{request.role}",
                as_type="generation",
                model=request.config.model,
                input=request.messages,
                model_parameters={
                    "temperature": request.config.temperature,
                    "max_tokens": request.config.max_tokens,
                },
                metadata={
                    "event_id": request.event_id,
                    "prompt_hash": request.prompt_hash,
                    "params_hash": request.params_hash,
                },
            )
        except Exception:
            log.warning("langfuse trace start failed", exc_info=True)
            return None

    def _trace_end(
        self, observation: Any | None, reply: CachedReply | None, *, error: str | None = None
    ) -> None:
        if observation is None:
            return
        try:
            update: dict[str, Any] = {}
            if reply is not None:
                update["output"] = (
                    reply.content
                    if not reply.tool_calls
                    else [call.model_dump() for call in reply.tool_calls]
                )
                update["usage_details"] = {
                    "input": reply.usage.prompt_tokens,
                    "output": reply.usage.completion_tokens,
                }
                if reply.usage.cost_usd is not None:
                    update["cost_details"] = {"total": reply.usage.cost_usd}
                update["metadata"] = {
                    "provider": reply.provider,
                    "model_returned": reply.model_returned,
                    "generation_id": reply.generation_id,
                }
            if error is not None:
                update["level"] = "ERROR"
                update["status_message"] = error
            observation.update(**update)
            observation.end()
        except Exception:
            log.warning("langfuse trace end failed", exc_info=True)


# --------------------------------------------------------------------------- helpers


def langfuse_from_env() -> Langfuse | None:
    """A Langfuse client when `LANGFUSE_PUBLIC_KEY`, `LANGFUSE_SECRET_KEY` and `LANGFUSE_BASE_URL` are set."""
    public_key = read_env_value("LANGFUSE_PUBLIC_KEY")
    secret_key = read_env_value("LANGFUSE_SECRET_KEY")
    base_url = read_env_value("LANGFUSE_BASE_URL")
    if not (public_key and secret_key and base_url):
        return None
    register_secret(secret_key)
    from langfuse import Langfuse

    return Langfuse(public_key=public_key, secret_key=secret_key, base_url=base_url)


def _vault_key(loader: SecretLoader, name: str) -> Callable[[], str]:
    def api_key() -> str:
        return str(loader.get(name).values["api_key"])

    return api_key


def _normalize(completion: Any) -> CachedReply:
    choices = getattr(completion, "choices", None) or []
    if not choices:
        raise LlmUnavailableError("the gateway returned no choices")
    message = choices[0].message
    calls = tuple(
        CachedToolCall(call_id=call.id, name=call.function.name, arguments=call.function.arguments or "")
        for call in (message.tool_calls or ())
        if getattr(call, "type", "function") == "function"
    )
    extra: dict[str, Any] = dict(getattr(completion, "model_extra", None) or {})
    usage = completion.usage
    usage_extra: dict[str, Any] = dict(getattr(usage, "model_extra", None) or {}) if usage else {}
    cost = usage_extra.get("cost")
    return CachedReply(
        content=message.content,
        tool_calls=calls,
        model_returned=str(completion.model or ""),
        provider=str(extra.get("provider") or ""),
        generation_id=str(completion.id or f"nogen-{uuid.uuid4().hex}"),
        usage=LlmUsage(
            prompt_tokens=int(usage.prompt_tokens) if usage else 0,
            completion_tokens=int(usage.completion_tokens) if usage else 0,
            cost_usd=float(cost) if isinstance(cost, int | float) and not isinstance(cost, bool) else None,
        ),
        finish_reason=str(finish) if (finish := getattr(choices[0], "finish_reason", None)) else None,
    )


def _token_count(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None


def _deepseek_reply(
    reply: CachedReply, completion: Any, prices: DeepSeekFile, model: str, requested_at: datetime
) -> CachedReply:
    """A DeepSeek reply: provider `deepseek`, the cost priced from `prices` (requested model, cache-hit and
    cache-miss prompt tokens, output tokens, peak or off-peak at `requested_at`; prompt tokens without the
    hit/miss split are priced as cache misses, never under-costed) and the thinking-mode chain of thought."""
    usage = getattr(completion, "usage", None)
    cost: float | None = None
    if usage is not None:
        usage_extra: dict[str, Any] = dict(getattr(usage, "model_extra", None) or {})
        hit = _token_count(usage_extra.get("prompt_cache_hit_tokens")) or 0
        miss = _token_count(usage_extra.get("prompt_cache_miss_tokens"))
        if miss is None:
            miss = max(reply.usage.prompt_tokens - hit, 0)
        cost = prices.cost_usd(
            model, cache_hit=hit, cache_miss=miss, output=reply.usage.completion_tokens, at=requested_at
        )
    reasoning = getattr(completion.choices[0].message, "reasoning_content", None)
    return reply.model_copy(
        update={
            "provider": "deepseek",
            "usage": reply.usage.model_copy(update={"cost_usd": cost}),
            "reasoning_content": reasoning if isinstance(reasoning, str) and reasoning else None,
        }
    )


def _parse[T: BaseModel](reply: CachedReply, schema: type[T]) -> T:
    if not reply.content:
        raise ValueError("empty content")
    return schema.model_validate_json(reply.content)


def _generation[T: BaseModel](
    request: _Request, reply: CachedReply, output: T, *, cached: bool
) -> Generation[T]:
    return Generation(
        output=output,
        model_slug=request.config.model,
        model_returned=reply.model_returned,
        provider=reply.provider,
        generation_id=reply.generation_id,
        usage=reply.usage,
        prompt_hash=request.prompt_hash,
        params_hash=request.params_hash,
        cached=cached,
    )


def _miss(request: _Request) -> LlmCacheMissError:
    return LlmCacheMissError(
        f"replay: no cached reply for {request.role} (model {request.config.model}, "
        f"prompt {request.prompt_hash[:12]}, params {request.params_hash[:12]})"
    )
