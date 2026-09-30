"""Durable side effects of a meeting: the blind commit, the decision card transaction and the outbox relay.

`PgDecisionStore.emit` writes, in ONE transaction: the `decision_cards` row, every `decision_forecasts` and
`decision_claims` row, the `decision_outbox` row (when something is sent to Risk) and the shadow-lesson
forecasts (through the phase 08 sink). A second emit of the same event writes nothing and reports whether
the stored card has the same content hash. After the commit, one episodic memory record per agent is written
(`known_at` = the stored card's `created_at`, so a retry writes the identical record).

`DecisionOutbox.relay_once` XADDs every pending `DecisionMsg` to `decisions` and marks it published, however
long after the candidate it was decided (a decision has no time limit, owner decision 2026-09-28). A crash
between the XADD and the mark re-sends that one message; Risk deduplicates on `event_id`.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import date, datetime
from typing import Any

import sqlalchemy as sa
from redis.asyncio import Redis
from sqlalchemy.dialects.postgresql import JSONB

from hdt.contracts.decision import DecisionMsg
from hdt.contracts.streams import Stream
from hdt.core.clock import utcnow
from hdt.core.streams import publish
from hdt.council.commit import CommitMismatchError, RoundCommit
from hdt.council.graph import DecisionRecord
from hdt.council.ports import ShadowForecastSink
from hdt.db.dedupe import insert_once
from hdt.db.models.decision import (
    DecisionCardRow,
    DecisionClaimRow,
    DecisionCommitRow,
    DecisionForecastRow,
    DecisionOutboxRow,
)
from hdt.memory.episodic import DecisionEpisode, EpisodicWriter
from hdt.memory.store import MemoryStore
from hdt.ops.metrics import COUNCIL_EVENTS, COUNCIL_ROUND1_CONSENSUS

log = logging.getLogger(__name__)

AUDIT_USER = "council"
AUDIT_ACTION = "council_round1_commit"


def _table(model: Any) -> sa.Table:
    table: sa.Table = model.__table__
    return table


class PgDecisionStore:
    """`DecisionStore` over the council role's engine."""

    def __init__(
        self,
        engine: sa.Engine,
        *,
        shadow_sink: ShadowForecastSink | None = None,
        memory: MemoryStore | None = None,
        clock: Callable[[], datetime] = utcnow,
    ) -> None:
        self.engine = engine
        self.shadow_sink = shadow_sink
        self.episodes = EpisodicWriter(memory) if memory is not None else None
        self.clock = clock

    # ------------------------------------------------------------------ blind commit

    def commit_round(self, commit: RoundCommit, at: datetime) -> None:
        """Insert the per-agent hashes and the audit-log entry once; a different stored commit raises."""
        with self.engine.begin() as conn:
            stored = conn.execute(
                sa.select(
                    DecisionCommitRow.agent, DecisionCommitRow.forecast_sha256, DecisionCommitRow.round_sha256
                ).where(
                    DecisionCommitRow.event_id == commit.event_id, DecisionCommitRow.round == commit.round
                )
            ).all()
            if stored:
                hashes = {row.agent: row.forecast_sha256 for row in stored}
                rounds = {row.round_sha256 for row in stored}
                if hashes != dict(commit.forecasts) or rounds != {commit.round_sha256}:
                    raise CommitMismatchError(
                        f"event {commit.event_id} round {commit.round}: stored commit differs from this run"
                    )
                return
            conn.execute(
                sa.insert(DecisionCommitRow),
                [
                    {
                        "event_id": commit.event_id,
                        "round": commit.round,
                        "agent": agent,
                        "forecast_sha256": digest,
                        "round_sha256": commit.round_sha256,
                        "committed_at": at,
                    }
                    for agent, digest in sorted(commit.forecasts.items())
                ],
            )
            # Plain INSERT: the council may append to the audit log but not read it (no RETURNING id).
            conn.execute(
                sa.text(
                    'INSERT INTO audit_log (at, "user", ip, action, section, diff_redacted) '
                    "VALUES (:at, :user, NULL, :action, 'council', :diff)"
                ).bindparams(sa.bindparam("diff", type_=JSONB)),
                {
                    "at": at,
                    "user": AUDIT_USER,
                    "action": AUDIT_ACTION,
                    "diff": {
                        "event_id": commit.event_id,
                        "round": commit.round,
                        "round_sha256": commit.round_sha256,
                        "forecasts": dict(sorted(commit.forecasts.items())),
                    },
                },
            )

    # ------------------------------------------------------------------ decision

    def emit(self, record: DecisionRecord) -> bool:
        """One transaction for the card, its rows, the outbox and the shadow forecasts; True when written."""
        now = self.clock()
        with self.engine.begin() as conn:
            card = {
                **record.card,
                "as_of": datetime.fromisoformat(record.card["as_of"]),
                "universe_date": date.fromisoformat(record.card["universe_date"]),
                "created_at": now,
            }
            written = insert_once(conn, _table(DecisionCardRow), card)
            if written:
                event_id = record.event_id
                if record.forecasts:
                    conn.execute(
                        sa.insert(DecisionForecastRow),
                        [{"event_id": event_id, **row} for row in record.forecasts],
                    )
                if record.claims:
                    conn.execute(
                        sa.insert(DecisionClaimRow), [{"event_id": event_id, **row} for row in record.claims]
                    )
                if record.decision is not None:
                    msg = record.decision
                    conn.execute(
                        sa.insert(DecisionOutboxRow).values(
                            event_id=event_id,
                            payload=msg.model_dump(mode="json"),
                            as_of=msg.as_of,
                            status="pending",
                            created_at=now,
                            published_at=None,
                            stream_id=None,
                        )
                    )
                if self.shadow_sink is not None:
                    for agent, lesson_id, forecast in record.shadow:
                        self.shadow_sink.record(
                            conn, event_id=event_id, agent=agent, lesson_id=lesson_id, forecast=forecast
                        )
            stored = conn.execute(
                sa.select(DecisionCardRow.card_sha256, DecisionCardRow.created_at).where(
                    DecisionCardRow.event_id == record.event_id
                )
            ).one()
        if stored.card_sha256 != record.card["card_sha256"]:
            log.error(
                "stored decision card differs from this run; keeping the stored card",
                extra={"event_id": record.event_id},
            )
        if written:
            source = str(record.card["source"])
            COUNCIL_EVENTS.labels(source, str(record.card["outcome"])).inc()
            if record.card["params"].get("round1_consensus"):
                COUNCIL_ROUND1_CONSENSUS.labels(source).inc()
        self._episodes(record, stored.created_at)
        return written

    def _episodes(self, record: DecisionRecord, known_at: datetime) -> None:
        if self.episodes is None:
            return
        intent = record.decision.intent if record.decision is not None else None
        side = record.decision.side if record.decision is not None else None
        for agent, forecast in record.final.items():
            skill = forecast.skill_commit
            episode = DecisionEpisode(
                known_at=max(known_at, forecast.as_of),
                event_id=record.event_id,
                agent=forecast.agent,
                agent_version=forecast.agent_version[:64],
                coin_id=forecast.coin_id,
                as_of=forecast.as_of,
                source=record.card["source"],
                target_type=forecast.target_type,
                council_intent=intent,
                council_side=side if intent is not None else None,
                abstain=forecast.abstain,
                p_used=None if forecast.abstain else forecast.p_used,
                candidate_id=forecast.candidate_id,
                summary=forecast.reason,
                packet_sha256=forecast.packet_sha256,
                skill_commit=skill if skill is not None and _is_commit(skill) else None,
                tool_calls=forecast.tool_calls,
            )
            self.episodes.record_decision(episode)
            log.debug("episode recorded", extra={"event_id": record.event_id, "agent": agent})


def _is_commit(value: str) -> bool:
    return len(value) == 40 and all(ch in "0123456789abcdef" for ch in value)


class DecisionOutbox:
    """Relays pending `decision_outbox` rows to stream `decisions` (idempotent on restart)."""

    def __init__(
        self,
        *,
        engine: sa.Engine,
        redis: Redis,
        maxlen: int | None = None,
        clock: Callable[[], datetime] = utcnow,
    ) -> None:
        self.engine = engine
        self.redis = redis
        self.maxlen = maxlen
        self.clock = clock
        self._lock = asyncio.Lock()

    def _pending(self) -> list[tuple[str, dict[str, Any]]]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                sa.select(DecisionOutboxRow.event_id, DecisionOutboxRow.payload)
                .where(DecisionOutboxRow.status == "pending")
                .order_by(DecisionOutboxRow.created_at, DecisionOutboxRow.event_id)
            ).all()
        return [(row.event_id, dict(row.payload)) for row in rows]

    def _mark(self, event_id: str, **values: Any) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                sa.update(DecisionOutboxRow)
                .where(DecisionOutboxRow.event_id == event_id, DecisionOutboxRow.status == "pending")
                .values(**values)
            )

    async def relay_once(self) -> int:
        """XADD every pending decision in write order; returns how many were published."""
        published = 0
        async with self._lock:
            for event_id, payload in await asyncio.to_thread(self._pending):
                msg = DecisionMsg.model_validate(payload)
                stream_id = await publish(self.redis, Stream.DECISIONS, msg, maxlen=self.maxlen)
                await asyncio.to_thread(
                    self._mark, event_id, status="published", published_at=self.clock(), stream_id=stream_id
                )
                published += 1
        if published:
            log.info("decisions relayed", extra={"count": published})
        return published
