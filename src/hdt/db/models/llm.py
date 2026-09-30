"""Phase 05 LLM tables: one row per gateway generation (`llm_calls`) and the reply cache (`llm_cache`).

- `llm_calls` is insert-only and written by every service holding the `llm` vault scope (council, news,
  veto scan, scorer) through `hdt.agents.llm_router.LlmRouter`. `model_slug` is the model the router
  requested; `model_returned` and `provider` are what the gateway reported for the generation (`deepseek`
  on the DeepSeek gateway); `cost_usd` is OpenRouter `usage.cost`, or on DeepSeek the token counts priced
  from `config/deepseek.yaml`. `status` is `ok` for a usable reply (tool calls or schema-valid content) and
  `invalid_output` for a reply that failed the structured-output schema (still billed, so still recorded).
  Cache hits are not generations and are never recorded here. The console and the public dashboard read the
  cost columns (`pipeline`, `role`, `event_id`, `model_slug`, tokens, `cost_usd`, `called_at`).
- `llm_cache` is keyed by `(prompt_hash, model_slug, provider, params_hash)` (Design Contract section 6):
  `prompt_hash` = sha256 of the canonical request messages, `model_slug` = requested model, `provider` =
  the requested provider routing (`only:...`, `order:...`, `any`, or `deepseek` for the DeepSeek gateway;
  the provider that actually served is inside `response`), `params_hash` = sha256 of every other request
  parameter. `response` is the normalized reply (`hdt.agents.llm_router.CachedReply`). Replay reads only
  this table; a miss is a hard error.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Final

from sqlalchemy import CheckConstraint, Float, Index, Integer, Text
from sqlalchemy.orm import Mapped, mapped_column

from hdt.db.base import Base

LLM_PIPELINES: Final[tuple[str, ...]] = ("council", "news", "reflection")
LLM_CALL_STATUSES: Final[tuple[str, ...]] = ("ok", "invalid_output")


def _in(column: str, values: tuple[str, ...]) -> str:
    return f"{column} IN ({', '.join(repr(v) for v in values)})"


class LlmCallRow(Base):
    __tablename__ = "llm_calls"
    __table_args__ = (
        CheckConstraint(_in("pipeline", LLM_PIPELINES), name="pipeline"),
        CheckConstraint(_in("status", LLM_CALL_STATUSES), name="status"),
        CheckConstraint("prompt_tokens >= 0 AND completion_tokens >= 0", name="tokens"),
        CheckConstraint("cost_usd IS NULL OR cost_usd >= 0", name="cost"),
        CheckConstraint("latency_ms >= 0", name="latency"),
        Index("ix_llm_calls_called_at", "called_at"),
        Index("ix_llm_calls_event_id", "event_id"),
    )

    generation_id: Mapped[str] = mapped_column(Text, primary_key=True)
    called_at: Mapped[datetime]
    pipeline: Mapped[str] = mapped_column(Text)
    role: Mapped[str] = mapped_column(Text)
    event_id: Mapped[str | None] = mapped_column(Text)
    model_slug: Mapped[str] = mapped_column(Text)
    model_returned: Mapped[str | None] = mapped_column(Text)
    provider: Mapped[str | None] = mapped_column(Text)
    prompt_tokens: Mapped[int] = mapped_column(Integer)
    completion_tokens: Mapped[int] = mapped_column(Integer)
    cost_usd: Mapped[float | None] = mapped_column(Float)
    prompt_hash: Mapped[str] = mapped_column(Text)
    params_hash: Mapped[str] = mapped_column(Text)
    latency_ms: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(Text)


class LlmCacheRow(Base):
    __tablename__ = "llm_cache"

    prompt_hash: Mapped[str] = mapped_column(Text, primary_key=True)
    model_slug: Mapped[str] = mapped_column(Text, primary_key=True)
    provider: Mapped[str] = mapped_column(Text, primary_key=True)
    params_hash: Mapped[str] = mapped_column(Text, primary_key=True)
    role: Mapped[str] = mapped_column(Text)
    generation_id: Mapped[str] = mapped_column(Text)
    response: Mapped[dict[str, Any]]
    created_at: Mapped[datetime]
