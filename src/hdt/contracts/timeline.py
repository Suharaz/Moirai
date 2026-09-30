"""Decision-card timeline entries: the JSON array in `decision_cards.timeline`, written by the council.

Producer contract: every entry names the pipeline `stage` it belongs to. The admin console and the public
snapshot show every entry whose stage is known (`TIMELINE_STAGES`, risk checks, sizing, verdicts and
orders included); an entry without a known stage is dropped from the public card. A card publishes at most
the last `PUBLIC_TIMELINE_MAX` entries (the snapshot schema caps `timeline` at 50 items), so a long
timeline is truncated from the start, never rejected.
"""

from __future__ import annotations

from typing import Final, Literal, get_args

from pydantic import Field

from hdt.contracts.common import ContractModel, UtcDatetime

TimelineStage = Literal["scanner", "news", "council", "manager", "risk", "orders", "outcome", "scoring"]
TimelineTone = Literal["", "pos", "neg", "warn"]
TIMELINE_STAGES: Final[frozenset[str]] = frozenset(get_args(TimelineStage))
PUBLIC_TIMELINE_MAX: Final[int] = 50  # = `maxItems` of `decision_card.timeline` in snapshot_schema.json


class DecisionTimelineEntry(ContractModel):
    """One line of a decision card timeline (`risk`: checks, sizing, verdict; `orders`: execution)."""

    at: UtcDatetime
    stage: TimelineStage
    text: str = Field(min_length=1, max_length=4000)
    tone: TimelineTone = ""
