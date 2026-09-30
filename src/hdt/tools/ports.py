"""Backends the tools depend on that other phases implement. Tools enforce the point-in-time contract
themselves: every item a backend returns must have been recorded at or before the `as_of` it was asked
for, otherwise the tool fails with `ToolBackendError` instead of passing look-ahead data to an agent.

- `QuantCoreFn`: phase 03 `QuantCore.quant_core(agent, coin_id, as_of, pins)` with the event's
  `QuantPins` bound by the caller (blocking; tools run it in a worker thread). It raises `LookupError`
  (`CoinNotInUniverseError`) when the coin is not in the point-in-time universe.
- `NewsIndex`: news items ingested by the phase 07 news worker (`item_id`, the coins they map to,
  `ingested_at`).
- `FetcherClient`: the phase 07 sandboxed fetcher on `net_fetch`. The caller passes the `item_id` and
  the item's ingested URL; the fetcher follows <= 3 redirects under its SSRF guard and returns the bytes,
  their sha256 and the redirect chain. The caller (the live `fetch_source` tool) writes the raw store.
- `OfficialSource`: official exchange announcements recorded by phase 07 (Binance announcements are the
  only T0 listing source).
- `UnlockCalendar`: token unlock schedule (source selected in phase 07 step 1).
- `NewsAssessor`: the phase 07 evidence pipeline's view of one coin at `as_of` (the `NewsForecast` adapter
  input of the News agent, phase 05): the regime-rule `p_model` (0.5 when no mode applies), the mode, and
  the code-verified evidence behind it. Built only from records known at or before `as_of` (stored items,
  extractor/judge verdicts, official checks, attention), so replay reproduces it without live calls.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import datetime
from typing import Final, Literal, Protocol

from pydantic import Field, NonNegativeFloat, PositiveInt

from hdt.contracts.common import AgentName, ContractModel, DirectionHint, Tier, UtcDatetime
from hdt.contracts.packet import QuantPacket

ITEM_ID_PATTERN: Final[str] = r"^[A-Za-z0-9_.:-]{1,64}$"
HTTP_URL_PATTERN: Final[str] = r"^https?://[^\s]{1,2040}$"

QuantCoreFn = Callable[[AgentName, int, datetime], QuantPacket]


class NewsItem(ContractModel):
    item_id: str = Field(pattern=ITEM_ID_PATTERN)
    coin_ids: tuple[PositiveInt, ...] = Field(min_length=1)
    title: str = Field(min_length=1)
    url: str = Field(pattern=HTTP_URL_PATTERN)
    source_name: str = Field(min_length=1)
    published_at: UtcDatetime | None = None
    ingested_at: UtcDatetime
    summary: str | None = None


class NewsIndex(Protocol):
    def items(self, coin_id: int, since: datetime, as_of: datetime, limit: int) -> Sequence[NewsItem]:
        """Items mapped to `coin_id` with `since <= ingested_at <= as_of`, newest first."""
        ...

    def item(self, item_id: str, as_of: datetime) -> NewsItem | None:
        """The item if it was ingested at or before `as_of`."""
        ...


class FetchedDocument(ContractModel):
    item_id: str = Field(pattern=ITEM_ID_PATTERN)
    requested_url: str = Field(pattern=HTTP_URL_PATTERN)
    final_url: str = Field(pattern=HTTP_URL_PATTERN)
    redirect_chain: tuple[str, ...] = Field(default=(), description="every URL after the requested one")
    http_status: int = Field(ge=100, le=599)
    content_type: str = ""
    body: bytes
    body_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class FetcherClient(Protocol):
    async def fetch(self, item_id: str, url: str) -> FetchedDocument: ...


class Announcement(ContractModel):
    exchange: str = Field(min_length=1)
    title: str = Field(min_length=1)
    url: str = Field(pattern=HTTP_URL_PATTERN)
    catalog: str = ""
    published_at: UtcDatetime
    recorded_at: UtcDatetime


class OfficialSource(Protocol):
    def announcements(self, exchange: str, since: datetime, as_of: datetime) -> Sequence[Announcement]:
        """Announcements of `exchange` published since `since` and recorded at or before `as_of`."""
        ...


class UnlockEvent(ContractModel):
    coin_id: PositiveInt
    unlock_at: UtcDatetime
    amount_tokens: NonNegativeFloat | None = None
    pct_of_circulating: NonNegativeFloat | None = None
    category: str = ""
    source: str = Field(min_length=1)
    recorded_at: UtcDatetime


class UnlockCalendar(Protocol):
    def unlocks(self, coin_id: int, start: datetime, end: datetime, as_of: datetime) -> Sequence[UnlockEvent]:
        """Unlocks of `coin_id` scheduled in `[start, end]`, as recorded at or before `as_of`."""
        ...


NewsMode = Literal["fade_hype", "fade_panic", "ride", "none", "abstain"]


class NewsEvidence(ContractModel):
    """One judged news event behind an assessment; every field is computed or checked by code."""

    item_id: str = Field(pattern=ITEM_ID_PATTERN)
    event_key: str = Field(min_length=1, max_length=200)
    event_class: str = Field(min_length=1, max_length=40)
    direction: DirectionHint
    tier: Tier = Field(description="from news_sources.yaml + check_official after redirects, never an LLM")
    official: bool
    hardness: float = Field(ge=0, le=1)
    novelty: float = Field(ge=0, le=1)
    quote: str = Field(min_length=1, max_length=280, description="verbatim, found in the stored copy by code")
    known_at: UtcDatetime


class NewsAssessment(ContractModel):
    coin_id: PositiveInt
    as_of: UtcDatetime
    p_model: float = Field(gt=0, lt=1, description="regime-rule probability of up; 0.5 when mode is none")
    mode: NewsMode
    abstain_reason: str | None = Field(default=None, max_length=200)
    evidence: tuple[NewsEvidence, ...] = Field(default=(), max_length=20)
    rule_version: str = Field(min_length=1)


class NewsAssessor(Protocol):
    def assess(self, coin_id: int, as_of: datetime) -> NewsAssessment:
        """The assessment of `coin_id` from records known at or before `as_of` (blocking; call it in a worker
        thread from async code)."""
        ...
