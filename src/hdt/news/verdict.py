"""One item through the evidence pipeline: extractor -> quote check -> two judges -> code tier and
`check_official` -> known-event dedupe. The result is one insert-only `news_verdicts` row per
(item, rule_version), stamped with `processed_at` (the time it became known).

Statuses:
- `agreed`: both judges gave the same class and direction (the only status that is evidence);
- `disagree`: class or direction differ -> the News agent abstains on it, the veto scan applies a soft
  veto when either judge said EXPLOIT or DELIST;
- `no_event`: the extractor found no event;
- `quote_failed`: the extractor's quote is not in the stored copy (hallucination guard);
- `llm_failed`: a model replied with invalid output twice, both judges were served by the same model
  developer (a fallback made them correlated, so their agreement proves nothing), or the item could not
  be processed at all (`failed_step` `internal`: one poisoned item never stops a cycle).
A transport / budget failure (`LlmUnavailableError`, `LlmCacheMissError`) or a role closed by its
structured-output self-test stores nothing: the item is retried on the next cycle while it is inside the
processing window (the veto scan soft-vetoes it meanwhile).
Times a model states are untrusted: an event time outside `[news time - EVENT_TIME_PAST, processed_at +
EVENT_TIME_AHEAD]` is ignored.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Final, Literal, Protocol, cast

import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError

from hdt.agents.llm_types import LlmCacheMissError, LlmOutputError, LlmUnavailableError, StructuredLlm
from hdt.contracts.common import Tier
from hdt.core.clock import ensure_utc
from hdt.core.config import LlmRole, NewsSourcesFile
from hdt.memory.episodic import KnownEvent, KnownEventWrite
from hdt.news.coins import CoinRef
from hdt.news.config import NewsFile
from hdt.news.extractor import ItemContext, extract, prompt_hash
from hdt.news.judge import JudgePair, judge_pair
from hdt.news.official import ItemEvidence, OfficialCheck, check_official
from hdt.news.onchain import OnchainState
from hdt.news.store import VERDICTS, StoredItem, insert_rows
from hdt.news.text import item_text, quote_found
from hdt.news.tiering import code_tier
from hdt.settings.schemas import DEEPSEEK_DEVELOPER, RoleModelConfig, slug_developer
from hdt.tools.ports import Announcement, UnlockEvent

log = logging.getLogger(__name__)

Processor = Literal["news", "veto_scan"]
KnownEventSink = Callable[[KnownEvent], KnownEventWrite]
KNOWN_NOVELTY_CAP: Final[float] = 0.2
"""Novelty of an item whose event was already known (another article reported it first)."""
EVENT_TIME_PAST: Final[timedelta] = timedelta(days=30)
EVENT_TIME_AHEAD: Final[timedelta] = timedelta(days=365)
_SYMBOL_CHARS: Final = re.compile(r"[^A-Za-z0-9_.-]")


class RoleGate(Protocol):
    """`hdt.agents.model_test.RoleHealth`: None when the role may run with `config`, else why not."""

    async def closed_reason(self, role: LlmRole, config: RoleModelConfig) -> str | None: ...


@dataclass(frozen=True)
class RoleConfigs:
    extractor: RoleModelConfig
    judge_a: RoleModelConfig
    judge_b: RoleModelConfig

    def named(self) -> tuple[tuple[LlmRole, RoleModelConfig], ...]:
        return (
            ("news_extractor", self.extractor),
            ("news_judge_a", self.judge_a),
            ("news_judge_b", self.judge_b),
        )


def item_evidence(item: StoredItem) -> ItemEvidence:
    return ItemEvidence(item.url, item_text(item.title, item.summary, item.content), item.news_time)


def _no_onchain(_coin_id: int, _start: datetime, _end: datetime) -> OnchainState | None:
    return None


@dataclass(frozen=True)
class OfficialEvidence:
    """What `check_official` may use at one `as_of` (everything recorded at or before it)."""

    announcements: Sequence[Announcement] = ()
    unlocks: Sequence[UnlockEvent] = ()
    unlock_tier: str = "T3"
    """Tier of the configured unlock calendar (what `check_official` accepts for UNLOCK)."""
    unlock_tiers: Mapping[str, str] = field(default_factory=dict)
    """Recorded tier per unlock source (the veto scan's large-unlock rule)."""
    onchain: Callable[[int, datetime, datetime], OnchainState | None] = field(default=_no_onchain)

    def check(
        self,
        event_class: str | None,
        coin: CoinRef,
        *,
        as_of: datetime,
        item: StoredItem,
        sources: NewsSourcesFile,
        config: NewsFile,
    ) -> OfficialCheck:
        state = None
        end = ensure_utc(as_of)
        if event_class == "EXPLOIT":
            start = min(item.news_time, end) - timedelta(hours=config.modes.incident_lookback_h)
            state = self.onchain(coin.coin_id, start, end)
        return check_official(
            event_class,
            coin,
            as_of=end,
            item=item_evidence(item),
            announcements=self.announcements,
            sources=sources,
            onchain=state,
            drain_usd_min=config.modes.onchain_drain_usd_min,
            unlocks=self.unlocks,
            unlock_tier=self.unlock_tier,
            unlock_ahead=timedelta(hours=config.veto.unlock_ahead_h),
        )


def event_key(slug: str, coins: Sequence[CoinRef], when: datetime) -> str:
    """`<slug>:<SYMBOLS>.<YYYYMMDD>`: the day keeps a later, separate event of the same kind distinct."""
    symbols = sorted({_SYMBOL_CHARS.sub("", c.symbol) or str(c.coin_id) for c in coins})
    subject = "-".join(symbols)[:60] or "coin"
    return f"{slug[:48]}:{subject}.{ensure_utc(when):%Y%m%d}"


def base_row(
    item: StoredItem, rule_version: str, processed_at: datetime, processor: Processor
) -> dict[str, Any]:
    return {
        "item_id": item.item_id,
        "rule_version": rule_version,
        "processed_at": processed_at,
        "processor": processor,
        "coin_ids": list(item.coin_ids),
        "status": "no_event",
        "event_key": None,
        "event_class": None,
        "direction": None,
        "hardness": None,
        "novelty": None,
        "confidence": None,
        "event_time": None,
        "quote": None,
        "quote_ok": False,
        "domain": item.domain,
        "tier": item.tier.value,
        "official": False,
        "official_ref": None,
        "class_a": None,
        "class_b": None,
        "direction_a": None,
        "direction_b": None,
        "known_event": None,
        "detail": {"prompts": {"extractor": prompt_hash("extractor"), "judge": prompt_hash("judge")}},
    }


@dataclass(frozen=True)
class VerdictJob:
    item: StoredItem
    coins: tuple[CoinRef, ...]
    official: OfficialEvidence
    processed_at: datetime
    processor: Processor


class ItemJudge:
    """Runs the 2-step LLM on items and builds their verdict rows (pure apart from the LLM calls)."""

    def __init__(
        self,
        llm: StructuredLlm,
        roles: RoleConfigs,
        *,
        config: NewsFile,
        sources: NewsSourcesFile,
        known_events: KnownEventSink | None = None,
        health: RoleGate | None = None,
    ) -> None:
        self._llm = llm
        self._roles = roles
        self._config = config
        self._sources = sources
        self._known = known_events
        self._health = health

    async def closed_roles(self) -> dict[str, str]:
        """Roles whose structured-output self-test failed (role -> reason); empty when all may run."""
        if self._health is None:
            return {}
        closed: dict[str, str] = {}
        for role, config in self._roles.named():
            reason = await self._health.closed_reason(role, config)
            if reason is not None:
                closed[role] = reason
        return closed

    async def verdict(self, job: VerdictJob) -> dict[str, Any] | None:
        """The verdict row of `job.item`, or None when the models were unavailable or a role is closed
        (retry later). An unexpected failure on this item becomes its `llm_failed` row (`internal`), so
        one poisoned item never stops the cycle."""
        closed = await self.closed_roles()
        if closed:
            log.warning("news roles closed", extra={"item_id": job.item.item_id, "roles": sorted(closed)})
            return None
        try:
            row = await self._judged(job)
        except Exception as exc:  # any fault in this item's untrusted data or the model output
            log.exception("news item could not be processed", extra={"item_id": job.item.item_id})
            row = base_row(job.item, self._config.rule_version, job.processed_at, job.processor)
            return clean_row(_failed(row, "internal", f"{type(exc).__name__}: {exc}"))
        if row is not None and row["status"] == "agreed":
            self._apply_known(row, job)
        return None if row is None else clean_row(row)

    async def _judged(self, job: VerdictJob) -> dict[str, Any] | None:
        item = job.item
        row = base_row(item, self._config.rule_version, job.processed_at, job.processor)
        text = item_text(item.title, item.summary, item.content)
        ctx = ItemContext(item, job.coins, text, self._config.ingest.llm_text_max_chars)
        try:
            extracted = await extract(self._llm, self._roles.extractor, ctx)
        except LlmOutputError as exc:
            return _failed(row, "extractor", str(exc))
        except (LlmUnavailableError, LlmCacheMissError) as exc:
            log.warning("extractor unavailable", extra={"item_id": item.item_id, "error": str(exc)})
            return None
        out = extracted.output
        row["detail"]["extractor"] = {
            "model": extracted.model_returned,
            "generation_id": extracted.generation_id,
            "class": out.event_class,
            "direction": out.direction,
        }
        if not out.has_event or out.event_class == "NO_EVENT":
            return row
        quote = out.quote.strip()
        # The model reads at most `llm_text_max_chars`; the quote must be in that part of the stored copy.
        if not quote_found(quote, ctx.text[: ctx.text_max_chars]):
            row["status"] = "quote_failed"
            row["detail"]["rejected_quote"] = quote[:280]
            return row
        row["quote"] = quote[:280]
        row["quote_ok"] = True
        try:
            a, b = await judge_pair(self._llm, (self._roles.judge_a, self._roles.judge_b), ctx, out)
        except LlmOutputError as exc:
            return _failed(row, "judge", str(exc))
        except (LlmUnavailableError, LlmCacheMissError) as exc:
            log.warning("judges unavailable", extra={"item_id": item.item_id, "error": str(exc)})
            return None
        pair = JudgePair(a.output, b.output)
        row.update(
            class_a=pair.a.event_class,
            class_b=pair.b.event_class,
            direction_a=pair.a.direction,
            direction_b=pair.b.direction,
        )
        row["detail"]["judges"] = {
            "a": {"model": a.model_returned, "generation_id": a.generation_id, "tier": pair.a.verified_tier},
            "b": {"model": b.model_returned, "generation_id": b.generation_id, "tier": pair.b.verified_tier},
        }
        shared = same_developer(
            (self._roles.judge_a, a.model_returned), (self._roles.judge_b, b.model_returned)
        )
        if shared is not None:
            return _failed(row, "judge_developer", f"both judges were served by developer {shared!r}")
        earliest = item.news_time - EVENT_TIME_PAST
        latest = ensure_utc(job.processed_at) + EVENT_TIME_AHEAD
        when = pair.event_time(out.event_time, earliest, latest)
        row["event_time"] = when
        row["event_key"] = event_key(out.event_slug, job.coins, when or item.news_time)
        if not pair.agreed:
            row["status"] = "disagree"
            return row
        event_class = pair.a.event_class
        row.update(
            status="agreed" if event_class != "NO_EVENT" else "no_event",
            event_class=event_class,
            direction=pair.a.direction,
            hardness=pair.hardness(),
            novelty=pair.novelty(),
            confidence=pair.confidence(),
        )
        if event_class == "NO_EVENT":
            return row
        self._apply_official(row, job, event_class)
        return row

    def _apply_official(self, row: dict[str, Any], job: VerdictJob, event_class: str) -> None:
        checks = {
            coin.coin_id: job.official.check(
                event_class,
                coin,
                as_of=job.processed_at,
                item=job.item,
                sources=self._sources,
                config=self._config,
            )
            for coin in job.coins
        }
        confirmed = sorted(coin_id for coin_id, check in checks.items() if check.official)
        row["official"] = bool(confirmed)
        row["official_ref"] = next((checks[c].ref for c in confirmed), None)
        row["tier"] = code_tier(
            job.item.url,
            self._sources,
            event_class=event_class,
            official=bool(confirmed),
            recorded_announcement=job.item.source_kind == "binance",
        ).value
        row["detail"]["official_coins"] = confirmed
        row["detail"]["official_basis"] = {str(c): check.basis for c, check in checks.items()}

    def _apply_known(self, row: dict[str, Any], job: VerdictJob) -> None:
        if self._known is None:
            return
        written = self._known(
            KnownEvent(
                known_at=job.processed_at,
                event_key=row["event_key"],
                coin_ids=tuple(job.item.coin_ids[:20]),
                title=job.item.title,
                tier=Tier(row["tier"]),
                item_ids=(job.item.item_id,),
            )
        )
        row["known_event"] = written.status
        if written.status != "new":
            row["novelty"] = min(row["novelty"], KNOWN_NOVELTY_CAP)
            row["detail"]["known_as"] = written.key


def served_developer(config: RoleModelConfig, returned: str) -> str | None:
    """The developer that served a judge's reply: `deepseek` on the deepseek gateway (it serves only its
    own models), else the developer of the returned slug; None when an OpenRouter reply names none (the
    router already refused any reply outside the role's pinned slugs)."""
    if config.gateway == "deepseek":
        return DEEPSEEK_DEVELOPER
    return slug_developer(returned).casefold() if "/" in returned else None


def same_developer(judge_a: tuple[RoleModelConfig, str], judge_b: tuple[RoleModelConfig, str]) -> str | None:
    """The developer both judges were served by, None when they differ. Each judge is its role config and
    the model it returned. Owner decision 2026-09-28, the same waiver as `ModelsSection._judges_differ`:
    when BOTH judge configs are on the deepseek gateway the rule does not apply (one developer serves
    them); with any judge on OpenRouter it is enforced, a DeepSeek judge counting as developer `deepseek`."""
    (config_a, returned_a), (config_b, returned_b) = judge_a, judge_b
    if config_a.gateway == config_b.gateway == "deepseek":
        return None
    developer = served_developer(config_a, returned_a)
    return (
        developer if developer is not None and developer == served_developer(config_b, returned_b) else None
    )


def _failed(row: dict[str, Any], step: str, error: str) -> dict[str, Any]:
    row["status"] = "llm_failed"
    row["detail"]["failed_step"] = step
    row["detail"]["error"] = error[:300]
    return row


_C0: Final[re.Pattern[str]] = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


def clean_row[T](value: T) -> T:
    """`value` with C0 control characters (NUL first: Postgres text and JSONB reject it) removed from
    every string, recursively; model-derived text is stored, so one reply must not make its row
    unwritable."""
    out: Any = value
    if isinstance(value, str):
        out = _C0.sub("", value)
    elif isinstance(value, dict):
        out = {k: clean_row(v) for k, v in value.items()}
    elif isinstance(value, (list, tuple)):
        out = type(value)(clean_row(v) for v in value)
    return cast("T", out)


def store_verdict(engine: sa.Engine, row: dict[str, Any], job: VerdictJob, rule_version: str) -> bool:
    """Insert `row`; when the database refuses it, store a minimal `llm_failed` row (no detail, no
    model text) instead so the item is never retried forever and the next items still get judged.
    Returns whether a row was written."""
    try:
        with engine.begin() as conn:
            return bool(insert_rows(conn, VERDICTS, [row]))
    except DBAPIError as exc:
        log.exception("verdict row refused by the database", extra={"item_id": job.item.item_id})
        fallback = base_row(job.item, rule_version, job.processed_at, job.processor)
        fallback.update(status="llm_failed", detail={"failed_step": "store", "error": type(exc).__name__})
        with engine.begin() as conn:
            return bool(insert_rows(conn, VERDICTS, [fallback]))
