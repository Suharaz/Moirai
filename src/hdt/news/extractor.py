"""Step 1 of the 2-step LLM: the extractor (role `news_extractor`) finds the event and a verbatim quote.

External text enters only the `user` message, inside the labeled "Item (data)" field, truncated to the
configured character bound (at most 8k); `system` holds only the fixed instructions of
`prompts/extractor.md`. Code then checks that the quote is really in the stored copy (`quote_found`);
a quote that is not found drops the item's evidence (status `quote_failed`).
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import Any, Final, Literal

from pydantic import BaseModel, ConfigDict, Field

from hdt.agents.llm_types import Generation, StructuredLlm
from hdt.core.ids import canonical_json, canonical_sha256
from hdt.news.classes import EventClass
from hdt.news.coins import CoinRef
from hdt.news.store import StoredItem
from hdt.settings.schemas import RoleModelConfig

PROMPTS_DIR: Final[Path] = Path(__file__).resolve().parent / "prompts"
EXTRACTOR_ROLE: Final = "news_extractor"


@cache
def prompt(name: str) -> str:
    return (PROMPTS_DIR / f"{name}.md").read_bytes().decode("utf-8").replace("\r\n", "\n")


@cache
def prompt_hash(name: str) -> str:
    return canonical_sha256({"prompt": prompt(name)})


class ExtractorOutput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    has_event: bool
    event_class: EventClass
    event_slug: str = Field(pattern=r"^[a-z0-9_]{1,40}$")
    direction: Literal["up", "down", "none"]
    quote: str = Field(max_length=400)
    event_time: str | None = Field(max_length=40)


@dataclass(frozen=True)
class ItemContext:
    item: StoredItem
    coins: tuple[CoinRef, ...]
    text: str
    """The stored copy (title, summary, content) the quote check reads."""
    text_max_chars: int

    def data(self) -> dict[str, Any]:
        return {
            "item_id": self.item.item_id,
            "source_name": self.item.source_name,
            "domain": self.item.domain,
            "published_at": self.item.published_at.isoformat() if self.item.published_at else None,
            "coins": [{"coin_id": c.coin_id, "symbol": c.symbol, "name": c.name} for c in self.coins],
            "text": self.text[: self.text_max_chars],
        }


def extractor_user(ctx: ItemContext) -> str:
    return (
        "## Item (data)\n"
        f"{canonical_json(ctx.data()).decode('utf-8')}\n\n"
        "## Task\nExtract the event of this item about the listed coins as specified."
    )


async def extract(
    llm: StructuredLlm, config: RoleModelConfig, ctx: ItemContext
) -> Generation[ExtractorOutput]:
    """One extractor call; `LlmError` subclasses propagate to the caller (it decides abstain or retry)."""
    return await llm.structured(
        role=EXTRACTOR_ROLE,
        pipeline="news",
        event_id=ctx.item.item_id,
        config=config,
        system=prompt("extractor"),
        user=extractor_user(ctx),
        schema=ExtractorOutput,
    )
