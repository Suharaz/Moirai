"""Step 2 of the 2-step LLM: two judges from different model developers (roles `news_judge_a`,
`news_judge_b`; waived while both run on the deepseek gateway, owner decision 2026-09-28) classify the
item independently; they disagree -> the item's verdict is `disagree`.

The judges' `verified_tier` is recorded for audit only: the tier used anywhere is computed by code
(`hdt.news.tiering`). Numbers the verdict keeps: hardness and novelty are the mean of the two judges,
confidence the lower one, event time the earlier stated one.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final, Literal

from pydantic import BaseModel, ConfigDict, Field

from hdt.agents.llm_types import Generation, StructuredLlm
from hdt.core.config import LlmRole
from hdt.core.ids import canonical_json
from hdt.news.classes import EventClass
from hdt.news.extractor import ExtractorOutput, ItemContext, prompt
from hdt.news.sources import parse_time
from hdt.settings.schemas import RoleModelConfig

JUDGE_ROLES: Final[tuple[LlmRole, LlmRole]] = ("news_judge_a", "news_judge_b")


class JudgeOutput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    event_class: EventClass
    direction: Literal["up", "down", "none"]
    hardness: float = Field(ge=0, le=1)
    novelty: float = Field(ge=0, le=1)
    verified_tier: Literal["T0", "T1", "T2", "T3", "UNVERIFIED"]
    event_time: str | None = Field(max_length=40)
    confidence: float = Field(ge=0, le=1)


@dataclass(frozen=True)
class JudgePair:
    a: JudgeOutput
    b: JudgeOutput

    @property
    def agreed(self) -> bool:
        return self.a.event_class == self.b.event_class and self.a.direction == self.b.direction

    @property
    def classes(self) -> frozenset[str]:
        return frozenset({self.a.event_class, self.b.event_class})

    def hardness(self) -> float:
        return round((self.a.hardness + self.b.hardness) / 2, 4)

    def novelty(self) -> float:
        return round((self.a.novelty + self.b.novelty) / 2, 4)

    def confidence(self) -> float:
        return min(self.a.confidence, self.b.confidence)

    def event_time(self, fallback: str | None, earliest: datetime, latest: datetime) -> datetime | None:
        """The earlier time a judge stated (else the extractor's), ignoring any time outside
        `[earliest, latest]`: model output is untrusted and must never widen a window or reach the far
        ends of the calendar."""
        stated = [
            t
            for t in (parse_time(self.a.event_time), parse_time(self.b.event_time))
            if t is not None and earliest <= t <= latest
        ]
        if stated:
            return min(stated)
        extracted = parse_time(fallback)
        return extracted if extracted is not None and earliest <= extracted <= latest else None


def judge_user(ctx: ItemContext, extracted: ExtractorOutput) -> str:
    event: dict[str, Any] = {
        "event_class": extracted.event_class,
        "event_slug": extracted.event_slug,
        "direction": extracted.direction,
        "quote": extracted.quote,
        "event_time": extracted.event_time,
    }
    return (
        "## Item (data)\n"
        f"{canonical_json(ctx.data()).decode('utf-8')}\n\n"
        "## Extracted event (data)\n"
        f"{canonical_json(event).decode('utf-8')}\n\n"
        "## Task\nJudge this item and the extracted event independently, as specified."
    )


async def judge_pair(
    llm: StructuredLlm,
    configs: tuple[RoleModelConfig, RoleModelConfig],
    ctx: ItemContext,
    extracted: ExtractorOutput,
) -> tuple[Generation[JudgeOutput], Generation[JudgeOutput]]:
    """Both judges concurrently; the first `LlmError` propagates (the caller abstains or retries)."""
    user = judge_user(ctx, extracted)
    system = prompt("judge")
    a, b = await asyncio.gather(
        *(
            llm.structured(
                role=role,
                pipeline="news",
                event_id=ctx.item.item_id,
                config=config,
                system=system,
                user=user,
                schema=JudgeOutput,
            )
            for role, config in zip(JUDGE_ROLES, configs, strict=True)
        )
    )
    return a, b
