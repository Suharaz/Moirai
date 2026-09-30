"""Scorer on Postgres (migration 0010_scoring), running as `hdt_scorer`: labels, rescoring, served parameters
under the council role, episodic outcomes, the claims audit, Reflection and the lesson A/B, and the grants."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx2
import numpy as np
import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy.orm import sessionmaker
from tests.unit.scoring.mark_lake import AS_OF, COIN, END, RISE, build_lake, kline_hour

from hdt.agents.llm_router import LlmRouter
from hdt.contracts.common import AgentName, TargetType
from hdt.contracts.forecast import AgentForecast
from hdt.core.config import scanner_config, scoring_config, static_config
from hdt.db.models.decision import DecisionCardRow, DecisionClaimRow, DecisionForecastRow
from hdt.db.models.ledger import PositionRow
from hdt.db.models.scoring import ClaimsAuditFlagRow, ScoringLabelRow, StackingModelRow
from hdt.db.models.settings import AgentVersionRow
from hdt.lake.pit_query import PitQuery
from hdt.lake.raw_store import RawStore
from hdt.memory.episodic import outcome_key
from hdt.memory.lessons import LessonLog, LessonTemplate
from hdt.memory.store import memory_namespace, open_postgres_store
from hdt.scoring import claims_audit, reflection
from hdt.scoring import service as scoring_service
from hdt.scoring import store as scoring_store
from hdt.scoring.params import PgParamsSource
from hdt.scoring.resolver import LabelOutcome
from hdt.scoring.resolver import resolve as real_resolve
from hdt.scoring.service import Scorer
from hdt.scoring.shadow import MemoryShadowLessons, PgShadowForecastSink
from hdt.scoring.stacking import LightGbmStacker, StackSample
from hdt.scoring.stacking import _train as train_stacker
from hdt.scoring.store import load_flags
from hdt.settings.schemas import RoleModelConfig

pytestmark = [pytest.mark.pg, pytest.mark.integration]

ROOT = Path(__file__).resolve().parents[2]
AGENTS = tuple(a.value for a in AgentName)
SKILL = {"crowding": 0.8, "technical": 0.55, "micro": 0.6, "fundamental": 0.5, "news": 0.5, "macro": 0.45}
MODEL = "anthropic/claude-sonnet-4.5"


@dataclass
class Db:
    url: str
    admin: sa.Engine
    role: Callable[[str], sa.Engine]


@pytest.fixture
def db(
    fresh_database: Callable[[], AbstractContextManager[str]], pg_role_url: Callable[[str, str], str]
) -> Iterator[Db]:
    with fresh_database() as url:
        cfg = Config(str(ROOT / "alembic.ini"))
        cfg.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
        command.upgrade(cfg, "head")
        engines: dict[str, sa.Engine] = {}

        def role(name: str) -> sa.Engine:
            if name not in engines:
                engines[name] = sa.create_engine(pg_role_url(url, name))
            return engines[name]

        admin = sa.create_engine(url)
        try:
            yield Db(url, admin, role)
        finally:
            for engine in (*engines.values(), admin):
                engine.dispose()


def forecast(
    event_id: str, agent: str, as_of: datetime, p: float | None, *, target: str = "RAW_12H", round_: int = 1
) -> AgentForecast:
    return AgentForecast(
        agent=AgentName(agent),
        agent_version="1",
        event_id=event_id,
        round=round_,
        coin_id=1027,
        as_of=as_of,
        target_type=TargetType(target),
        label_spec_version="lbl1",
        p_model=None if p is None else round(0.5 + (p - 0.5) * 0.5, 6),
        p_llm=p,
        p_used=p,
        abstain=p is None,
        abstain_reason="no edge" if p is None else None,
        reason=f"{agent} read of {event_id}",
    )


def insert_event(
    conn: sa.Connection,
    event_id: str,
    as_of: datetime,
    p_by_agent: dict[str, float | None],
    *,
    y: int | None,
    symbol: str = COIN,
    target: str = "RAW_12H",
    unscored: bool = False,
    resolved_at: datetime | None = None,
) -> None:
    conn.execute(
        sa.insert(DecisionCardRow.__table__),
        {
            "event_id": event_id,
            "coin_id": 1027,
            "symbol": symbol,
            "as_of": as_of,
            "source": "LTX",
            "outcome": "NO_TRADE",
            "side": None,
            "manager_size": 0.0,
            "p_pooled": 0.5,
            "disagreement": 0.1,
            "rounds": 1,
            "stop_reason": "consensus",
            "candidate_id": None,
            "candidate_set_sha256": None,
            "packet_sha256": None,
            "target_type": target,
            "label_spec_version": "lbl1",
            "config_version_ids": {},
            "universe_date": as_of.date(),
            "summary": "test card",
            "consensus": [],
            "manager_rule": [],
            "timeline": [],
            "unscored": unscored,
            "shadow_only": False,
            "held_side": None,
            "intent": None,
            "round1_sha256": "a" * 64,
            "params": {},
            "agents": {},
            "hard_evidence_version": "h1",
            "card_sha256": "b" * 64,
            "created_at": as_of,
        },
    )
    for agent, p in p_by_agent.items():
        f = forecast(event_id, agent, as_of, p, target=target)
        conn.execute(
            sa.insert(DecisionForecastRow.__table__),
            {
                "event_id": event_id,
                "agent": agent,
                "round": 1,
                "forecast": f.model_dump(mode="json"),
                "submitted": f.model_dump(mode="json"),
                "revision": None,
                "stance": "ABSTAIN" if p is None else ("LONG" if p > 0.5 else "SHORT"),
                "weight_norm": None,
                "commit_sha256": "c" * 64,
            },
        )
    if y is not None:
        conn.execute(
            sa.insert(ScoringLabelRow.__table__),
            {
                "event_id": event_id,
                "coin_id": 1027,
                "symbol": symbol,
                "as_of": as_of,
                "target_type": target,
                "label_spec_version": "lbl1",
                "horizon_h": 12,
                "status": "resolved",
                "y": y,
                "label_value": 0.01 if y else -0.01,
                "coin_return": 0.01 if y else -0.01,
                "btc_return": None,
                "beta_btc": None,
                "atr": None,
                "regime": "trend_high_vol",
                "barrier": None,
                "barrier_y": None,
                "resolved_at": resolved_at or as_of + timedelta(hours=13),
            },
        )


def history(conn: sa.Connection, n: int, *, end: datetime, prefix: str = "ev", seed: int = 5) -> list[str]:
    """`n` labeled events every 3 h ending 12 h before `end`; agents right with probability SKILL."""
    rng = np.random.default_rng(seed)
    ids = []
    for i in range(n):
        as_of = end - timedelta(hours=12 + 3 * (n - i))
        y = int(rng.random() < 0.5)
        ps: dict[str, float | None] = {}
        for agent in AGENTS:
            p = 0.7 if rng.random() < SKILL[agent] else 0.3
            ps[agent] = p if y else 1 - p
        event_id = f"{prefix}-{i:04d}"
        insert_event(conn, event_id, as_of, ps, y=y)
        ids.append(event_id)
    return ids


def make_scorer(db: Db, tmp_path: Path, memory: Any, now: datetime) -> Scorer:
    return make_scorer_on(db, build_lake(tmp_path), memory, now)


def make_scorer_on(db: Db, store: RawStore, memory: Any, now: datetime) -> Scorer:
    static = static_config()
    return Scorer(
        sessions=sessionmaker(db.role("hdt_scorer")),
        static=static,
        scanner=scanner_config(),
        scoring=scoring_config(),
        pit=PitQuery(store.staging_root, store.lake_root),
        memory=memory,
        council=lambda: static.council,
        models=lambda: None,
        llm=None,
        clock=lambda: now,
    )


def count(engine: sa.Engine, sql: str) -> int:
    with engine.connect() as conn:
        return int(conn.execute(sa.text(sql)).scalar_one())


def test_scorer_labels_rescoring_and_served_params(db: Db, tmp_path: Path) -> None:
    now = END + timedelta(hours=1)
    with db.admin.begin() as conn:
        history(conn, 80, end=AS_OF + timedelta(hours=12))
        insert_event(conn, "due-raw", AS_OF, {a: 0.7 for a in AGENTS}, y=None)
        insert_event(conn, "due-unscored", AS_OF, {a: 0.7 for a in AGENTS}, y=None, unscored=True)
    scorer_dsn = sa.engine.make_url(db.role("hdt_scorer").url).render_as_string(hide_password=False)
    with open_postgres_store(scorer_dsn) as memory:
        scorer = make_scorer(db, tmp_path, memory, now)
        assert scorer.resolve_due() == 1
        with db.admin.connect() as conn:
            label = conn.execute(
                sa.text(
                    "SELECT status, y, label_value, barrier FROM scoring_labels WHERE event_id = 'due-raw'"
                )
            ).one()
            assert label.status == "resolved"
            assert label.y == 1
            assert label.label_value == pytest.approx(0.02)
            assert (
                conn.execute(
                    sa.text("SELECT count(*) FROM scoring_labels WHERE event_id = 'due-unscored'")
                ).scalar_one()
                == 0
            )

        result, _ = scorer.rescore()
        assert len(result.scored) == 81 * 7
        tables = (
            "scored_forecasts",
            "weight_history",
            "calibration_stats",
            "calibration_bins",
            "scoring_params",
        )
        first = {t: count(db.admin, f"SELECT count(*) FROM {t}") for t in tables}
        assert first["scoring_params"] == 1
        assert first["scored_forecasts"] == 81 * 7
        scorer.rescore()
        assert {t: count(db.admin, f"SELECT count(*) FROM {t}") for t in tables} == first

        served = PgParamsSource(db.role("hdt_council"), council=static_config().council)
        raw = served.load(TargetType.RAW_12H, "lbl1")
        assert raw.params_version == 1
        weights = raw.weights("trend_high_vol", AGENTS)
        assert max(weights, key=lambda a: weights[a]) == "crowding"
        assert served.load(TargetType.RESID_12H, "lbl1").params_version == 0

        assert scorer.record_outcomes() == 81 * 6
        assert scorer.record_outcomes() == 0
        stored = memory.get(
            memory_namespace(AgentName.CROWDING, "episodic"),
            outcome_key("due-raw"),
            as_of=END + timedelta(days=1),
        )
        assert stored is not None
        assert stored.value["label"] == 1
        assert stored.value["record_type"] == "outcome"

    council = db.role("hdt_council")
    with council.connect() as conn, pytest.raises(sa.exc.ProgrammingError, match="permission denied"):
        conn.execute(sa.text("UPDATE scored_forecasts SET p = 0.5"))
    publisher = db.role("hdt_publisher_ro")
    assert count(publisher, "SELECT count(*) FROM calibration_bins") == first["calibration_bins"]
    with publisher.connect() as conn, pytest.raises(sa.exc.ProgrammingError, match="permission denied"):
        conn.execute(sa.text("SELECT count(*) FROM lesson_shadow_forecasts"))
    assert count(db.role("hdt_telegram"), "SELECT count(*) FROM weight_history") == first["weight_history"]


def test_claims_audit_flags_once_and_review_restores(db: Db) -> None:
    now = datetime(2026, 3, 10, tzinfo=UTC)
    # (claims, wrong, reject reason): only penalized reasons are wrong claims; `repeated` is not (S6).
    rejected = {
        "crowding": (30, 3, "value_mismatch"),
        "technical": (30, 1, "value_mismatch"),
        "micro": (30, 3, "repeated"),
        "news": (10, 5, "quote_not_found"),
    }
    with db.admin.begin() as conn:
        insert_event(conn, "audit-1", now - timedelta(days=1), {}, y=None)
        insert_event(conn, "audit-old", now - timedelta(days=9), {}, y=None)
        for agent, (claims, bad, reason) in rejected.items():
            for i in range(claims):
                for event_id in ("audit-1", "audit-old"):
                    wrong = i < bad or event_id == "audit-old"
                    conn.execute(
                        sa.insert(DecisionClaimRow.__table__),
                        {
                            "event_id": event_id,
                            "round": 1,
                            "shared_id": f"{agent}-{i}",
                            "source_agent": agent,
                            "original_claim_id": f"c{i}",
                            "claim_sha256": f"{agent}{i:02d}".ljust(64, "0"),
                            "claim": {"text": "claim"},
                            "reject_reason": reason if wrong else None,
                            "penalty": wrong and reason != "repeated",
                            "shared": True,
                        },
                    )
    sessions = sessionmaker(db.role("hdt_scorer"))
    params = scoring_config().claims_audit
    with sessions() as session, session.begin():
        flagged = claims_audit.audit(session, params, now=now)
    assert [(f.agent, f.claims, f.rejected) for f in flagged] == [("crowding", 30, 3)]
    with sessions() as session, session.begin():
        assert claims_audit.audit(session, params, now=now + timedelta(hours=1)) == []
    with db.admin.connect() as conn:
        assert (
            conn.execute(sa.text("SELECT count(*) FROM alerts WHERE kind = 'claims_audit'")).scalar_one() == 1
        )
        windows = load_flags(conn)
    assert [(w.agent, w.end) for w in windows] == [("crowding", None)]
    reviewed_at = now + timedelta(days=1)
    with sessions() as session, session.begin():
        assert claims_audit.review(session, "crowding", by="operator", note="checked", now=reviewed_at)
    with sessions() as session, session.begin():
        assert not claims_audit.review(
            session, "crowding", by="operator", note="again", now=now + timedelta(days=2)
        )
    # The reviewed claims cannot re-flag the agent: its window restarts at the review (S7).
    with sessions() as session, session.begin():
        assert claims_audit.audit(session, params, now=reviewed_at + timedelta(minutes=5)) == []
    # New penalized claims after the review flag it again.
    with db.admin.begin() as conn:
        insert_event(conn, "audit-new", reviewed_at + timedelta(hours=1), {}, y=None)
        for i in range(30):
            conn.execute(
                sa.insert(DecisionClaimRow.__table__),
                {
                    "event_id": "audit-new",
                    "round": 1,
                    "shared_id": f"crowding-new-{i}",
                    "source_agent": "crowding",
                    "original_claim_id": f"n{i}",
                    "claim_sha256": f"crowdingnew{i:02d}".ljust(64, "0"),
                    "claim": {"text": "claim"},
                    "reject_reason": "value_mismatch" if i < 3 else None,
                    "penalty": i < 3,
                    "shared": True,
                },
            )
    with sessions() as session, session.begin():
        again = claims_audit.audit(session, params, now=reviewed_at + timedelta(hours=2))
    assert [(f.agent, f.claims, f.rejected) for f in again] == [("crowding", 30, 3)]


def lesson_llm(sent: list[dict[str, Any]], reply: dict[str, str]) -> httpx2.AsyncClient:
    def handler(request: httpx2.Request) -> httpx2.Response:
        sent.append(json.loads(request.content))
        return httpx2.Response(
            200,
            json={
                "id": f"gen-reflect-{len(sent)}",
                "object": "chat.completion",
                "created": 1,
                "model": MODEL,
                "provider": "Anthropic",
                "choices": [
                    {
                        "index": 0,
                        "finish_reason": "stop",
                        "message": {"role": "assistant", "content": json.dumps(reply)},
                    }
                ],
                "usage": {"prompt_tokens": 90, "completion_tokens": 40, "total_tokens": 130, "cost": 0.001},
            },
        )

    return httpx2.AsyncClient(transport=httpx2.MockTransport(handler))


def close_trade(conn: sa.Connection, event_id: str, at: datetime) -> None:
    conn.execute(
        sa.insert(PositionRow.__table__),
        {
            "account": "testnet",
            "event_id": event_id,
            "symbol": COIN,
            "side": "LONG",
            "qty": 0,
            "max_qty": 1,
            "entry_price": 100,
            "unrealized_pnl": 0,
            "is_hedge_book": False,
            "opened_at": at,
            "closed_at": at + timedelta(hours=6),
            "exit_price": 99,
            "exit_reason": "stop",
            "exit_notional": 99,
            "exit_qty": 1,
            "realized_pnl": -1,
            "fees_funding_usd": 0,
            "r_multiple": -1.0,
            "exit_in_progress": False,
            "tp1_done": False,
            "updated_at": at + timedelta(hours=6),
        },
    )


async def test_reflection_proposes_one_shadow_lesson_and_the_ab_decides(db: Db, tmp_path: Path) -> None:
    now = datetime(2026, 4, 1, tzinfo=UTC)
    start = now - timedelta(days=30)
    with db.admin.begin() as conn:
        # Trade 1 and 2: macro is the most wrong agent (p 0.9 on a down move).
        for event_id, hours in (("trade-1", 0), ("trade-2", 3)):
            as_of = start + timedelta(hours=hours)
            ps: dict[str, float | None] = {a: 0.45 for a in AGENTS}
            ps["macro"] = 0.9
            insert_event(conn, event_id, as_of, ps, y=0)
            close_trade(conn, event_id, as_of)
    scorer_engine = db.role("hdt_scorer")
    sessions = sessionmaker(scorer_engine)
    scorer_dsn = sa.engine.make_url(scorer_engine.url).render_as_string(hide_password=False)
    sent: list[dict[str, Any]] = []
    reply = {
        "title": "Macro overreach in quiet tape",
        "when_text": "Macro regime reads risk-on while the coin trades in a tight range",
        "observation": "The agent leaned on the macro tailwind although price momentum was flat",
        "adjustment": "Cap conviction when the coin shows no momentum confirmation over the last day",
    }
    router = LlmRouter(
        mode="live",
        api_key=lambda: "sk-or-test",
        http_client=lesson_llm(sent, reply),
        session_factory=sessions,
    )
    config = RoleModelConfig.model_validate(
        {
            "model": MODEL,
            "temperature": 0.2,
            "max_tokens": 600,
            "timeout_s": 30,
            "provider": {"only": ["Anthropic"]},
        }
    )
    params = scoring_config().reflection
    with open_postgres_store(scorer_dsn) as memory:
        scorer = make_scorer(db, tmp_path, memory, now)
        scorer.rescore()
        lessons = LessonLog(memory)
        with sessions() as session:
            trades = reflection.closed_trades(session.connection())
        assert [t.event_id for t in trades] == ["trade-1", "trade-2"]
        statuses = []
        for trade in trades:
            statuses.append(
                await reflection.reflect(
                    sessions, trade, llm=router, lessons=lessons, config=config, params=params, now=now
                )
            )
        assert statuses == ["proposed", "skipped_shadow_busy"]
        assert len(sent) == 1
        user_prompt = sent[0]["messages"][-1]["content"]
        assert "macro" in user_prompt
        shadow = lessons.shadow(AgentName.MACRO, reflection.LATEST)
        assert shadow is not None
        assert shadow.state == "shadow"
        with sessions() as session:
            assert reflection.closed_trades(session.connection()) == []
        shadows = MemoryShadowLessons(memory)
        assert shadows.shadow(AgentName.MACRO, datetime.now(UTC)) is not None

        # 60 A/B pairs: the lesson makes macro better calibrated on every event.
        sink = PgShadowForecastSink()
        rng = np.random.default_rng(11)
        with db.admin.begin() as conn:
            for i in range(60):
                as_of = start + timedelta(days=1, hours=3 * i)
                y = int(rng.random() < 0.5)
                insert_event(conn, f"ab-{i:03d}", as_of, {"macro": 0.8 if y == 0 else 0.2}, y=y)
        with db.role("hdt_council").begin() as conn:
            for i in range(60):
                as_of = start + timedelta(days=1, hours=3 * i)
                better = forecast(f"ab-{i:03d}", "macro", as_of, 0.5)
                sink.record(
                    conn,
                    event_id=f"ab-{i:03d}",
                    agent=AgentName.MACRO,
                    lesson_id=shadow.lesson_id,
                    forecast=better,
                )
                sink.record(
                    conn,
                    event_id=f"ab-{i:03d}",
                    agent=AgentName.MACRO,
                    lesson_id=shadow.lesson_id,
                    forecast=better,
                )
        with sessions() as session:
            done = reflection.run_ab(session.connection(), lessons, params, now=now)
        assert done == [shadow.lesson_id]
        decided = lessons.shadow(AgentName.MACRO, reflection.LATEST)
        assert decided is not None
        assert decided.ab is not None
        assert decided.ab.n == 60
        assert decided.ab.improved
        assert shadows.shadow(AgentName.MACRO, datetime.now(UTC)) is None
        with db.role("hdt_console_ro").connect() as conn:
            row = conn.execute(
                sa.text("SELECT agent, state, ab_n FROM lessons WHERE lesson_id = :id"),
                {"id": shadow.lesson_id},
            ).one()
        assert (row.agent, row.state, row.ab_n) == ("macro", "shadow", 60)

        # A lesson that makes news worse on every paired event is retired by the A/B itself.
        worse = lessons.propose(AgentName.NEWS, LessonTemplate.model_validate(reply), actor=reflection.ACTOR)
        with db.admin.begin() as conn:
            for i in range(60):
                event_id = f"ab-{i:03d}"
                y = conn.execute(
                    sa.text("SELECT y FROM scoring_labels WHERE event_id = :e"), {"e": event_id}
                ).scalar_one()
                as_of = start + timedelta(days=1, hours=3 * i)
                f = forecast(event_id, "news", as_of, 0.55 if y else 0.45)
                conn.execute(
                    sa.insert(DecisionForecastRow.__table__),
                    {
                        "event_id": event_id,
                        "agent": "news",
                        "round": 1,
                        "forecast": f.model_dump(mode="json"),
                        "submitted": f.model_dump(mode="json"),
                        "revision": None,
                        "stance": "NEUTRAL",
                        "weight_norm": None,
                        "commit_sha256": "c" * 64,
                    },
                )
        with db.role("hdt_council").begin() as conn:
            for i in range(60):
                event_id = f"ab-{i:03d}"
                as_of = start + timedelta(days=1, hours=3 * i)
                sink.record(
                    conn,
                    event_id=event_id,
                    agent=AgentName.NEWS,
                    lesson_id=worse.lesson_id,
                    forecast=forecast(event_id, "news", as_of, 0.5),
                )
        with sessions() as session:
            assert reflection.run_ab(session.connection(), lessons, params, now=now) == [worse.lesson_id]
        assert lessons.shadow(AgentName.NEWS, reflection.LATEST) is None
        retired = next(
            x for x in lessons.lessons_at(AgentName.NEWS, reflection.LATEST) if x.lesson_id == worse.lesson_id
        )
        assert retired.state == "retired"
        assert retired.ab is not None
        assert not retired.ab.improved
    await router.aclose()


def test_scoring_migration_downgrades_and_upgrades_cleanly(db: Db) -> None:
    cfg = Config(str(ROOT / "alembic.ini"))
    cfg.set_main_option("sqlalchemy.url", db.url.replace("%", "%%"))
    command.downgrade(cfg, "0009_council")
    with db.admin.connect() as conn:
        left = conn.execute(
            sa.text(
                "SELECT count(*) FROM pg_class "
                "WHERE relname IN ('scored_forecasts', 'lessons', 'scoring_labels')"
            )
        ).scalar_one()
    assert left == 0
    command.upgrade(cfg, "head")
    assert count(db.role("hdt_console_ro"), "SELECT count(*) FROM lessons") == 0


def label_row(engine: sa.Engine, event_id: str) -> Any:
    with engine.connect() as conn:
        return conn.execute(
            sa.text("SELECT status, y, error, resolved_at FROM scoring_labels WHERE event_id = :id"),
            {"id": event_id},
        ).one_or_none()


def test_a_poisoned_card_is_contained_alerted_and_retried(
    db: Db, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """S2: the oldest due cards raise; the newer one is still labeled, the bad ones are recorded `error`
    under one integrity alert (M-6: no alert per card), then re-resolved after the retry interval once they
    no longer raise. A retry that ends pending is throttled by the interval like any other attempt."""
    bad_as_of = AS_OF - timedelta(minutes=30)
    bad_ids = {"poisoned": bad_as_of, "poisoned-2": bad_as_of - timedelta(minutes=15)}
    with db.admin.begin() as conn:
        for event_id, as_of in bad_ids.items():
            insert_event(conn, event_id, as_of, {a: 0.7 for a in AGENTS}, y=None)
        insert_event(conn, "healthy", AS_OF, {a: 0.7 for a in AGENTS}, y=None)
    real_packets = scoring_store.event_packets

    def poisoned_packets(conn: sa.Connection, coin_id: int, as_of: datetime) -> Any:
        if as_of in bad_ids.values():
            raise ValueError("committed packet hash mismatch")
        return real_packets(conn, coin_id, as_of)

    attempts: list[str] = []

    def still_pending(view: Any, request: Any, **kwargs: Any) -> Any:
        attempts.append(request.event_id)
        return LabelOutcome(request, "pending", None, None, None, None, None, None)

    def open_integrity() -> int:
        return count(
            db.admin, "SELECT count(*) FROM alerts WHERE kind = 'integrity_error' AND resolved_at IS NULL"
        )

    monkeypatch.setattr(scoring_store, "event_packets", poisoned_packets)
    now = END + timedelta(hours=1)
    interval = timedelta(seconds=scoring_config().resolver.missing_retry_interval_s + 60)
    scorer_dsn = sa.engine.make_url(db.role("hdt_scorer").url).render_as_string(hide_password=False)
    with open_postgres_store(scorer_dsn) as memory:
        assert make_scorer(db, tmp_path / "a", memory, now).resolve_due() == 1
        assert label_row(db.admin, "healthy").status == "resolved"
        for event_id in bad_ids:
            bad = label_row(db.admin, event_id)
            assert bad.status == "error"
            assert "hash mismatch" in bad.error
        assert open_integrity() == 1
        # Inside the retry interval nothing is attempted again.
        assert make_scorer(db, tmp_path / "b", memory, now + timedelta(minutes=30)).resolve_due() == 0
        monkeypatch.setattr(scoring_store, "event_packets", real_packets)
        monkeypatch.setattr(scoring_service, "resolve", still_pending)
        later = now + interval
        assert make_scorer(db, tmp_path / "c", memory, later).resolve_due() == 0
        assert sorted(attempts) == sorted(bad_ids)
        assert {label_row(db.admin, e).resolved_at for e in bad_ids} == {later}
        assert make_scorer(db, tmp_path / "d", memory, later + timedelta(minutes=30)).resolve_due() == 0
        assert sorted(attempts) == sorted(bad_ids)  # throttled: no second attempt inside the interval
        assert open_integrity() == 1  # errors still pending inside the window
        monkeypatch.setattr(scoring_service, "resolve", real_resolve)
        assert make_scorer(db, tmp_path / "e", memory, later + interval).resolve_due() == 2
    for event_id in bad_ids:
        fixed = label_row(db.admin, event_id)
        assert (fixed.status, fixed.y, fixed.error) == ("resolved", 1, None)
    assert open_integrity() == 0


def test_a_missing_label_is_re_resolved_after_a_backfill(db: Db, tmp_path: Path) -> None:
    """S1: the end mark of a 06:37 card is absent (no WS, no kline yet) -> `missing`; once the on-time kline
    record lands in the lake the label resolves on the next attempt inside the retry window, never outside
    it (M-4: a backfill fetched after `t + kline_fetch_lag + max_mark_gap` is not read)."""
    as_of = AS_OF + timedelta(minutes=37)
    with db.admin.begin() as conn:
        insert_event(conn, "late-end", as_of, {a: 0.7 for a in AGENTS}, y=None)
    resolver = scoring_config().resolver
    missing_at = as_of + timedelta(hours=12 + resolver.missing_after_h, minutes=1)
    lake = build_lake(tmp_path)
    scorer_dsn = sa.engine.make_url(db.role("hdt_scorer").url).render_as_string(hide_password=False)
    with open_postgres_store(scorer_dsn) as memory:
        assert make_scorer_on(db, lake, memory, missing_at).resolve_due() == 1
        assert label_row(db.admin, "late-end").status == "missing"
        lake.append_many([kline_hour(s, END, END + timedelta(hours=1)) for s in RISE])
        beyond = as_of + timedelta(hours=12 + resolver.missing_retry_h, minutes=1)
        assert make_scorer_on(db, lake, memory, beyond).resolve_due() == 0
        assert label_row(db.admin, "late-end").status == "missing"
        retry_at = missing_at + timedelta(seconds=resolver.missing_retry_interval_s + 60)
        assert make_scorer_on(db, lake, memory, retry_at).resolve_due() == 1
    resolved = label_row(db.admin, "late-end")
    assert (resolved.status, resolved.y, resolved.error) == ("resolved", 1, None)


def test_rescore_failure_keeps_outcomes_and_reflection_pending(db: Db, tmp_path: Path) -> None:
    """S3: a resolved label without scored rows is neither marked recorded nor reflected upon."""
    now = END + timedelta(hours=1)
    with db.admin.begin() as conn:
        insert_event(conn, "unscored-yet", AS_OF - timedelta(hours=2), {a: 0.7 for a in AGENTS}, y=1)
        close_trade(conn, "unscored-yet", AS_OF - timedelta(hours=2))
    scorer_dsn = sa.engine.make_url(db.role("hdt_scorer").url).render_as_string(hide_password=False)

    class BrokenScorer(Scorer):
        def rescore(self) -> Any:
            raise RuntimeError("rescore failed")

    with open_postgres_store(scorer_dsn) as memory:
        base = make_scorer(db, tmp_path, memory, now)
        broken = BrokenScorer(
            sessions=base.sessions,
            static=base.static,
            scanner=base.scanner,
            scoring=base.scoring,
            pit=base.pit,
            memory=memory,
            council=base.council,
            models=base.models,
            llm=None,
            clock=lambda: now,
        )
        asyncio.run(broken.cycle())
        assert base.record_outcomes() == 0
        with base.sessions() as session:
            assert reflection.closed_trades(session.connection()) == []
        assert (
            count(db.admin, "SELECT count(*) FROM scoring_labels WHERE outcomes_recorded_at IS NOT NULL") == 0
        )
        base.rescore()
        assert base.record_outcomes() == 6
        with base.sessions() as session:
            assert [t.event_id for t in reflection.closed_trades(session.connection())] == ["unscored-yet"]
    assert count(db.admin, "SELECT count(*) FROM reflection_runs") == 0


def stored_stacker() -> tuple[str, dict[str, Any]]:
    rng = np.random.default_rng(3)
    samples = [
        StackSample(
            event_id=f"s{i}",
            as_of=AS_OF + timedelta(hours=i),
            horizon_h=12,
            p_by_agent={a: float(rng.uniform(0.2, 0.8)) for a in AGENTS},
            regime=None,
            p_pooled=0.5,
            barrier_y=int(rng.random() < 0.5),
        )
        for i in range(80)
    ]
    booster = train_stacker(AGENTS, samples, scoring_config().stacking)
    return LightGbmStacker(booster, AGENTS).to_stored()


def test_served_params_follow_the_newest_stacker_versions_and_flags(db: Db, tmp_path: Path) -> None:
    """S4: a newer disabled stacker disables stacking. S5: a new agent version and an open claims flag
    reach the council at load, before any label. I3: a load pinned at `at` is point in time."""
    now = END + timedelta(hours=1)
    with db.admin.begin() as conn:
        history(conn, 80, end=AS_OF + timedelta(hours=12))
    scorer_dsn = sa.engine.make_url(db.role("hdt_scorer").url).render_as_string(hide_password=False)
    with open_postgres_store(scorer_dsn) as memory:
        make_scorer(db, tmp_path, memory, now).rescore()
    served = PgParamsSource(db.role("hdt_council"), council=static_config().council)
    before = served.load(TargetType.RAW_12H, "lbl1")
    assert before.stacker is None
    assert before.a("macro") < 1.0
    assert before.weights("trend_high_vol", AGENTS)["crowding"] > 0.20

    model, features = stored_stacker()
    t1, t2 = now + timedelta(hours=1), now + timedelta(hours=2)
    with db.admin.begin() as conn:
        for through, created, enabled in ((AS_OF, t1, True), (AS_OF + timedelta(hours=1), t2, False)):
            conn.execute(
                sa.insert(StackingModelRow.__table__),
                {
                    "target_type": "RAW_12H",
                    "label_spec_version": "lbl1",
                    "trained_through": through,
                    "n": 300,
                    "oos_n": 100,
                    "oos_logloss_stack": 0.6 if enabled else 0.7,
                    "oos_logloss_pool": 0.65,
                    "enabled": enabled,
                    "features": features,
                    "model": model if enabled else None,
                    "created_at": created,
                },
            )
    assert served.load(TargetType.RAW_12H, "lbl1").stacker is None
    pinned = served.load(TargetType.RAW_12H, "lbl1", at=t1 + timedelta(minutes=1))
    assert pinned.stacker is not None
    assert pinned.stacker_trained_through == AS_OF
    assert pinned.params_version == before.params_version
    with pytest.raises(LookupError):
        served.load(TargetType.RAW_12H, "lbl1", params_version=before.params_version + 1)
    with pytest.raises(LookupError):
        served.load(TargetType.RAW_12H, "lbl1", at=now - timedelta(hours=1), params_version=1)

    registered = now + timedelta(hours=3)
    with db.admin.begin() as conn:
        for agent in ("crowding", "macro"):
            conn.execute(
                sa.insert(AgentVersionRow.__table__),
                {
                    "agent": agent,
                    "version": 2,
                    "model_slug": MODEL,
                    "provider_prefs_hash": "p" * 64,
                    "prompt_hash": "h" * 64,
                    "skill_commit": "c" * 40,
                    "config_version_id": None,
                    "created_by": "council",
                    "created_at": registered,
                },
            )
        conn.execute(
            sa.insert(ClaimsAuditFlagRow.__table__),
            {
                "agent": "technical",
                "raised_at": registered,
                "window_start": registered - timedelta(days=7),
                "window_end": registered,
                "claims": 30,
                "rejected": 3,
                "rate": 0.1,
                "reviewed_at": None,
                "reviewed_by": None,
                "review_note": None,
            },
        )
    council = static_config().council.weights
    live = served.load(TargetType.RAW_12H, "lbl1")
    assert live.params_version == before.params_version
    weights = live.weights("trend_high_vol", AGENTS)
    assert weights["crowding"] <= council.new_version_cap + 1e-9
    assert live.ceilings["technical"] == scoring_config().claims_audit.flagged_ceiling
    assert weights["technical"] <= live.ceilings["technical"] + 1e-9
    assert (live.a("macro"), live.r("macro")) == (council.a_initial, council.r_initial)
    assert live.a("technical") == before.a("technical")
    earlier = served.load(TargetType.RAW_12H, "lbl1", at=registered - timedelta(minutes=1))
    assert earlier.weights("trend_high_vol", AGENTS) == before.weights("trend_high_vol", AGENTS)
    assert earlier.a("macro") == before.a("macro")
    with db.admin.begin() as conn:
        conn.execute(
            sa.text(
                "UPDATE claims_audit_flags SET reviewed_at = :at, reviewed_by = 'operator', "
                "review_note = 'ok'"
            ),
            {"at": registered + timedelta(hours=1)},
        )
    reviewed = served.load(TargetType.RAW_12H, "lbl1")
    assert reviewed.ceilings["technical"] == before.ceilings["technical"]
    assert reviewed.ceilings["crowding"] == council.new_version_cap


def test_pinned_params_replay_ignores_rows_committed_after_the_load(db: Db, tmp_path: Path) -> None:
    """I-6: a stacker, an agent version and a claims flag stamped before `at` but committed after the
    council's load must not change a replay that passes the original view's pins."""
    now = END + timedelta(hours=1)
    with db.admin.begin() as conn:
        history(conn, 80, end=AS_OF + timedelta(hours=12))
    scorer_dsn = sa.engine.make_url(db.role("hdt_scorer").url).render_as_string(hide_password=False)
    with open_postgres_store(scorer_dsn) as memory:
        make_scorer(db, tmp_path, memory, now).rescore()
    served = PgParamsSource(db.role("hdt_council"), council=static_config().council)
    at = now + timedelta(hours=2)
    original = served.load(TargetType.RAW_12H, "lbl1", at=at)
    assert original.stacker is None
    assert original.pins["stacker_trained_through"] is None
    assert "technical" not in original.pins["flagged"]
    assert original.pins["live_versions"].get("macro", 0) < 9
    pins = json.loads(json.dumps(original.pins))

    stamped = at - timedelta(seconds=30)
    model, features = stored_stacker()
    with db.admin.begin() as conn:
        conn.execute(
            sa.insert(StackingModelRow.__table__),
            {
                "target_type": "RAW_12H",
                "label_spec_version": "lbl1",
                "trained_through": AS_OF,
                "n": 300,
                "oos_n": 100,
                "oos_logloss_stack": 0.6,
                "oos_logloss_pool": 0.65,
                "enabled": True,
                "features": features,
                "model": model,
                "created_at": stamped,
            },
        )
        conn.execute(
            sa.insert(AgentVersionRow.__table__),
            {
                "agent": "macro",
                "version": 9,
                "model_slug": MODEL,
                "provider_prefs_hash": "p" * 64,
                "prompt_hash": "h" * 64,
                "skill_commit": "c" * 40,
                "config_version_id": None,
                "created_by": "council",
                "created_at": stamped,
            },
        )
        conn.execute(
            sa.insert(ClaimsAuditFlagRow.__table__),
            {
                "agent": "technical",
                "raised_at": stamped,
                "window_start": stamped - timedelta(days=7),
                "window_end": stamped,
                "claims": 30,
                "rejected": 3,
                "rate": 0.1,
                "reviewed_at": None,
                "reviewed_by": None,
                "review_note": None,
            },
        )
    unpinned = served.load(TargetType.RAW_12H, "lbl1", at=at, params_version=original.params_version)
    assert unpinned.stacker is not None  # the time filter alone now sees the late rows
    replay = served.load(TargetType.RAW_12H, "lbl1", at=at, params_version=original.params_version, pins=pins)
    assert replay == original
    assert replay.stacker is None
    assert replay.a("macro") == original.a("macro")
    assert replay.ceilings["technical"] == original.ceilings["technical"]

    with_stacker = served.load(TargetType.RAW_12H, "lbl1", at=at + timedelta(minutes=1))
    assert with_stacker.stacker is not None
    again = served.load(TargetType.RAW_12H, "lbl1", at=at, pins=json.loads(json.dumps(with_stacker.pins)))
    assert again.stacker_trained_through == AS_OF
    assert again.ceilings == with_stacker.ceilings
    with pytest.raises(ValueError, match="malformed"):
        served.load(TargetType.RAW_12H, "lbl1", at=at, pins={"flagged": []})
