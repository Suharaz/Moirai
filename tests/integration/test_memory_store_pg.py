"""Memory store on Postgres: PostgresStore works on the Alembic schema under the service roles, rows are
append-only, and row-level security keeps each writer to the records it owns."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg
import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config

from hdt.contracts.common import AgentName, CandidateSource, TargetType, Tier
from hdt.memory.episodic import DecisionEpisode, EpisodicReader, EpisodicWriter, KnownEvent, OutcomeRecord
from hdt.memory.lessons import LessonEvent, LessonLog, LessonTemplate
from hdt.memory.store import memory_namespace, open_postgres_store

pytestmark = [pytest.mark.pg, pytest.mark.integration]

ROOT = Path(__file__).resolve().parents[2]
T0 = datetime(2026, 2, 1, tzinfo=UTC)


def decision(agent: AgentName = AgentName.CROWDING) -> DecisionEpisode:
    return DecisionEpisode(
        event_id="evt-1",
        agent=agent,
        agent_version="v1",
        coin_id=5,
        as_of=T0,
        known_at=T0 + timedelta(seconds=3),
        source=CandidateSource.LTX,
        target_type=TargetType.RAW_12H,
        council_intent=None,
        abstain=True,
    )


def test_memory_store_roles_rls_and_append_only(
    fresh_database: Callable[[], AbstractContextManager[str]], pg_role_url: Callable[[str, str], str]
) -> None:
    with fresh_database() as url:
        cfg = Config(str(ROOT / "alembic.ini"))
        cfg.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
        command.upgrade(cfg, "head")

        with open_postgres_store(pg_role_url(url, "hdt_council")) as council:
            assert EpisodicWriter(council).record_decision(decision()) is True
            assert EpisodicWriter(council).record_decision(decision()) is False  # idempotent
            with pytest.raises(psycopg.errors.InsufficientPrivilege):  # outcomes belong to the scorer
                EpisodicWriter(council).record_outcome(
                    OutcomeRecord.resolved(
                        as_of=T0,
                        horizon_h=12,
                        event_id="evt-1",
                        agent=AgentName.CROWDING,
                        coin_id=5,
                        target_type=TargetType.RAW_12H,
                        label=1,
                    )
                )

        with open_postgres_store(pg_role_url(url, "hdt_scorer")) as scorer:
            EpisodicWriter(scorer).record_outcome(
                OutcomeRecord.resolved(
                    as_of=T0,
                    horizon_h=12,
                    event_id="evt-1",
                    agent=AgentName.CROWDING,
                    coin_id=5,
                    target_type=TargetType.RAW_12H,
                    label=1,
                )
            )
            template = LessonTemplate(
                title="Late cascades",
                when_text="SPIKE above 6",
                observation="reverted late",
                adjustment="wait more",
            )
            lesson = LessonLog(scorer, clock=lambda: T0).propose(
                AgentName.CROWDING, template, actor="reflection"
            )
            with pytest.raises(psycopg.errors.InsufficientPrivilege):  # approval belongs to config-api
                scorer.append(
                    memory_namespace(AgentName.CROWDING, "lessons"),
                    f"{lesson.lesson_id}:review",
                    LessonEvent(
                        lesson_id=lesson.lesson_id,
                        agent=AgentName.CROWDING,
                        action="approved",
                        actor="x",
                        known_at=T0 + timedelta(seconds=1),
                    ),
                )
            with pytest.raises(psycopg.errors.InsufficientPrivilege):  # decisions belong to the council
                EpisodicWriter(scorer).record_decision(decision(AgentName.MACRO))

        with open_postgres_store(pg_role_url(url, "hdt_news")) as news:
            event = KnownEvent(
                event_key="binance_spot_listing:AAA",
                coin_ids=(5,),
                title="Binance lists AAA",
                tier=Tier.T0,
                known_at=T0,
            )
            assert EpisodicWriter(news).record_known_event(event).status == "new"

        with open_postgres_store(pg_role_url(url, "hdt_council")) as reader:
            episodes = EpisodicReader(reader, AgentName.CROWDING, T0 + timedelta(hours=13)).recent()
            assert len(episodes) == 1
            assert episodes[0].outcome is not None
            assert (
                EpisodicReader(reader, AgentName.CROWDING, T0 + timedelta(hours=11)).recent()[0].outcome
                is None
            )
            assert (
                LessonLog(reader).lessons_at(AgentName.CROWDING, T0 + timedelta(seconds=1))[0].state
                == "shadow"
            )

        engine = sa.create_engine(url)  # the migrator owns the table; the trigger still refuses changes
        try:
            for statement in (
                "DELETE FROM store",
                "UPDATE store SET value = '{}'::jsonb",
                "TRUNCATE store",
            ):
                with pytest.raises(sa.exc.DBAPIError, match="append-only"), engine.begin() as conn:
                    conn.execute(sa.text(statement))
            with engine.begin() as conn:
                assert conn.execute(sa.text("SELECT count(*) FROM store")).scalar_one() == 4
                with pytest.raises(sa.exc.IntegrityError), conn.begin_nested():  # memory never expires
                    conn.execute(sa.text("UPDATE store SET ttl_minutes = 5"))
        finally:
            engine.dispose()
