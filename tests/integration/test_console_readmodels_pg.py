"""Console read models on the migrated schema, read as `hdt_console_ro` (council, LLM, scoring, lessons).

Every monitoring panel backed by a table of migrations 0007-0010 (or the `lessons` view) runs its real SQL
through the real grants: a missing column, table or grant would turn the panel `Unavailable`.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from tests.reader_rows import (
    agent_forecast,
    calibration_rows,
    card_row,
    claim_row,
    forecast_row,
    insert,
    llm_call_row,
    scored_row,
    weight_row,
)

from hdt.console.readmodels import Available, DecisionFilters, ReadModels, ReadResult
from hdt.contracts.candidate import CandidateSet, LevelCandidate
from hdt.contracts.common import AgentName, ClaimKind, Side
from hdt.contracts.forecast import Claim
from hdt.db.models.decision import DecisionCardRow, DecisionClaimRow, DecisionForecastRow
from hdt.db.models.llm import LlmCallRow
from hdt.db.models.scoring import (
    CalibrationBinRow,
    CalibrationStatRow,
    GateProgressRow,
    ScoredForecastRow,
    WeightHistoryRow,
)
from hdt.db.models.settings import GateFlagRow
from hdt.memory.lessons import AbResult, LessonLog, LessonTemplate
from hdt.memory.store import open_postgres_store

pytestmark = [pytest.mark.pg, pytest.mark.integration]

ROOT = Path(__file__).resolve().parents[2]
# The read models window on the wall clock (today, 30 days), so the seed is relative to it.
NOW = datetime.now(UTC).replace(microsecond=0)
TODAY = NOW.replace(hour=0, minute=0, second=0)
CARD = "EVT-CARD"
HELD = "EVT-HELD"
CARD_AS_OF = NOW - timedelta(hours=20)
LEVEL_A = "lc_" + "A" * 16
LEVEL_B = "lc_" + "B" * 16
TARGET = "RESID_12H"


def _level(candidate_id: str, side: Side, entry: str, stop: str, tp1: str) -> LevelCandidate:
    rr = float(abs(Decimal(tp1) - Decimal(entry)) / abs(Decimal(entry) - Decimal(stop)))
    return LevelCandidate(
        candidate_id=candidate_id,
        side=side,
        entry=Decimal(entry),
        invalidation=Decimal(stop),
        tp1=Decimal(tp1),
        rr=rr,
        tick=Decimal("0.001"),
    )


def _seed(conn: sa.Connection) -> None:
    cset = CandidateSet(
        coin_id=1,
        as_of=CARD_AS_OF,
        levels_ver="v1",
        candidates=(
            _level(LEVEL_A, Side.LONG, "1.742", "1.701", "1.804"),
            _level(LEVEL_B, Side.SHORT, "1.742", "1.783", "1.680"),
        ),
    )
    conn.execute(
        sa.text(
            "INSERT INTO candidate_sets (candidate_set_sha256, coin_id, as_of, payload, created_at) "
            "VALUES (:s, 1, :t, CAST(:p AS jsonb), :t)"
        ),
        {"s": cset.candidate_set_sha256, "t": CARD_AS_OF, "p": cset.model_dump_json()},
    )
    timeline = [
        {
            "at": CARD_AS_OF.isoformat(),
            "stage": "scanner",
            "text": "Scanner emitted a LTX candidate",
            "tone": "",
        },
        {"at": CARD_AS_OF.isoformat(), "stage": "council", "text": "Round 2 closed", "tone": "warn"},
    ]
    insert(
        conn,
        DecisionCardRow,
        [
            card_row(
                CARD,
                "OPUSDT",
                CARD_AS_OF,
                candidate_id=LEVEL_A,
                candidate_set_sha256=cset.candidate_set_sha256,
                config_version_ids={"risk": 7, "council": 3},
                timeline=timeline,
                intent="OPEN",
            ),
            card_row(
                HELD,
                "ARBUSDT",
                NOW - timedelta(hours=10),
                source="HELD",
                outcome="HOLD",
                held_side="LONG",
                intent="HOLD",
                manager_size=0.0,
                unscored=True,
            ),
        ],
    )
    forecasts = []
    for round_, ps in ((1, {"crowding": 0.62, "technical": 0.58}), (2, {"crowding": 0.64, "technical": 0.6})):
        for agent, p in ps.items():
            f = agent_forecast(CARD, agent, CARD_AS_OF, p, round_=round_, candidate_id=LEVEL_A)
            forecasts.append(forecast_row(f, stance="LONG", weight_norm=0.3 if agent == "crowding" else 0.2))
    insert(conn, DecisionForecastRow, forecasts)
    verified = Claim(
        claim_id="t1", kind=ClaimKind.PACKET, ref="rsi_1h", statement="RSI 1h is 71", verified=True
    )
    fabricated = Claim(claim_id="c1", kind=ClaimKind.PACKET, ref="oi_z", statement="OI z-score is 9")
    insert(
        conn,
        DecisionClaimRow,
        [
            claim_row(CARD, 1, "C-001", "technical", verified),
            claim_row(CARD, 1, "C-002", "crowding", fabricated, reject_reason="value_mismatch"),
        ],
    )
    calls = [
        ("g1", "council", "crowding", CARD, 0.012),
        ("g2", "council", "technical", CARD, 0.010),
        ("g3", "news", "news_extractor", None, 0.004),
    ]
    insert(
        conn,
        LlmCallRow,
        [
            llm_call_row(
                gen,
                TODAY,
                pipeline=pipeline,
                role=role,
                event_id=event,
                model_slug="openai/gpt-4.1-mini",
                prompt_tokens=1000,
                completion_tokens=200,
                cost_usd=cost,
            )
            for gen, pipeline, role, event, cost in calls
        ],
    )
    scored_at = NOW - timedelta(hours=1)
    insert(
        conn,
        ScoredForecastRow,
        [
            scored_row(CARD, "pooled", as_of=CARD_AS_OF, hit=True, scored_at=scored_at, p=0.65),
            scored_row(CARD, "crowding", as_of=CARD_AS_OF, hit=True, scored_at=scored_at, p=0.62),
            scored_row(CARD, "technical", as_of=CARD_AS_OF, hit=True, scored_at=scored_at, p=0.58),
        ],
    )
    insert(
        conn,
        WeightHistoryRow,
        [
            weight_row(
                "crowding", NOW - timedelta(days=2), w=0.18, a=0.9, r=0.6, coverage=0.95, forecasts=200
            ),
            weight_row(
                "crowding",
                NOW - timedelta(days=1),
                w=0.2,
                w_capped=0.19,
                a=0.9,
                r=0.6,
                coverage=0.97,
                forecasts=214,
                agent_version=2,
            ),
            weight_row(
                "technical",
                NOW - timedelta(days=1),
                w=0.15,
                a=0.8,
                r=0.5,
                coverage=0.9,
                forecasts=120,
                agent_version=None,
            ),
        ],
    )
    for agent in ("pooled", "crowding"):
        bins, stats = calibration_rows(agent, NOW - timedelta(days=1), z=0.84, ece=0.041, n=187)
        insert(conn, CalibrationBinRow, [bins])
        insert(conn, CalibrationStatRow, [stats])
    insert(
        conn,
        GateProgressRow,
        [
            {
                "gate": "G1",
                "check_key": key,
                "label": label,
                "value": value,
                "target": target,
                "met": False,
                "updated_at": NOW,
            }
            for key, label, value, target in (
                ("progress", "LTX edge on the lake", 41, 120),
                ("calibration", "Calibration samples", 20, 100),
            )
        ],
    )
    insert(
        conn,
        GateFlagRow,
        [
            {"gate": "G1", "passed": passed, "evidence_ref": "r", "decided_by": "scorer", "decided_at": at}
            for passed, at in ((False, NOW - timedelta(days=2)), (True, NOW - timedelta(days=1)))
        ],
    )
    conn.execute(
        sa.text(
            "INSERT INTO agent_versions (agent, version, model_slug, provider_prefs_hash, created_by, "
            "created_at) VALUES ('crowding', 1, 'openai/gpt-4.1-mini', 'h', 'admin', :v1), "
            "('crowding', 2, 'openai/gpt-4.1', 'h', 'admin', :v2)"
        ),
        {"v1": NOW - timedelta(days=40), "v2": NOW - timedelta(days=5)},
    )


def _lessons(url: str) -> None:
    """One lesson awaiting review (improving A/B) and one approved, written as the lesson event log."""
    ab = AbResult(n=60, logloss_with=0.61, logloss_without=0.69, ci_low=-0.12, ci_high=-0.03)
    with open_postgres_store(url) as memory:
        log = LessonLog(memory)
        pending = log.propose(
            AgentName.MACRO,
            LessonTemplate(
                title="Macro overreach in quiet tape",
                when_text="BTC realized vol is in its lowest decile",
                observation="Macro leaned hard on the regime flag",
                adjustment="Pull the reading toward the base rate",
            ),
            actor="reflection",
        )
        log.record_ab(AgentName.MACRO, pending.lesson_id, ab, actor="scorer")
        active = log.propose(
            AgentName.NEWS,
            LessonTemplate(
                title="Listing rumours without an official post",
                when_text="A listing claim has no exchange announcement",
                observation="News treated a rumour as confirmed",
                adjustment="Discount unconfirmed listing claims",
            ),
            actor="reflection",
        )
        log.record_ab(AgentName.NEWS, active.lesson_id, ab, actor="scorer")
        log.approve(AgentName.NEWS, active.lesson_id, actor="admin", note="clear improvement")


@pytest.fixture(scope="module")
def models(
    fresh_database: Callable[[], AbstractContextManager[str]], pg_role_url: Callable[[str, str], str]
) -> Iterator[ReadModels]:
    with fresh_database() as url:
        cfg = Config(str(ROOT / "alembic.ini"))
        cfg.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
        command.upgrade(cfg, "head")
        admin = sa.create_engine(url)
        with admin.begin() as conn:
            _seed(conn)
        admin.dispose()
        _lessons(url)
        engine = sa.create_engine(pg_role_url(url, "hdt_console_ro"))
        try:
            yield ReadModels(engine)
        finally:
            engine.dispose()


def _value[T](result: ReadResult[T]) -> T:
    assert isinstance(result, Available), result
    return result.value


def test_decision_list_joins_scores_and_filters(models: ReadModels) -> None:
    page = _value(models.decisions("paper", DecisionFilters(), limit=10, offset=0))
    assert page.total == 2
    assert [(r.event_id, r.scoring, r.scoring_kind) for r in page.rows] == [
        (HELD, "Not scored", "mute"),
        (CARD, "Correct (up)", "pos"),
    ]
    held = _value(models.decisions("paper", DecisionFilters(source="HELD"), limit=10, offset=0))
    assert [(r.event_id, r.outcome, r.manager_size) for r in held.rows] == [(HELD, "HOLD", 0.0)]


def test_decision_card_reads_forecasts_claims_levels_and_usage(models: ReadModels) -> None:
    card = _value(models.decision_card(CARD, "paper"))
    assert card is not None
    assert card.config_version == 7
    assert [(f.round, f.agent) for f in card.forecasts] == [
        (1, "crowding"),
        (1, "technical"),
        (2, "crowding"),
        (2, "technical"),
    ]
    assert [(r.agent, r.before, r.after) for r in card.revisions()] == [
        ("crowding", 0.62, 0.64),
        ("technical", 0.58, 0.6),
    ]
    assert [(c.shared_id, c.claim.verified, c.reject_reason) for c in card.claims] == [
        ("C-001", True, None),
        ("C-002", False, "value_mismatch"),
    ]
    assert card.levels is not None
    assert [(lv.candidate_id, lv.vote, lv.chosen) for lv in card.levels] == [
        (LEVEL_A, pytest.approx(0.5), True),
        (LEVEL_B, None, False),
    ]
    assert card.usage is not None
    assert (card.usage.calls, card.usage.agents) == (2, 2)
    assert card.usage.cost_usd == pytest.approx(0.022)
    assert [(t.stage, t.text) for t in card.timeline] == [
        ("scanner", "Scanner emitted a LTX candidate"),
        ("council", "Round 2 closed"),
    ]
    assert (card.verdict, card.fills) == (None, ())
    held = _value(models.decision_card(HELD, "paper"))
    assert held is not None
    assert held.levels is None
    assert held.levels_problem is not None
    assert _value(models.decision_card("EVT-NONE", "paper")) is None


def test_scoring_gates_and_costs(models: ReadModels) -> None:
    hits = _value(models.hit_rate())
    assert (hits.scored, hits.hits) == (1, 1)
    (gate,) = _value(models.gates())
    assert gate.gate == "G1"
    assert gate.progress is not None
    assert gate.progress.value == 41
    assert [c.check_key for c in gate.checks] == ["calibration"]
    assert gate.passed is True
    window = _value(models.llm_cost())
    assert window.calls == 3
    assert window.cost_usd == pytest.approx(0.026)
    costs = _value(models.cost_overview())
    assert costs.today.calls == 3
    assert (costs.by_day[-1].day, costs.by_day[-1].calls) == (TODAY.date(), 3)
    assert costs.council_events_30d == 1
    assert costs.council_cost_30d == pytest.approx(0.022)
    assert [(key, cost) for key, _label, cost in costs.by_pipeline] == [
        ("council", pytest.approx(0.022)),
        ("news", pytest.approx(0.004)),
        ("reflection", 0.0),
    ]
    assert [(r.role, r.models, r.prompt_tokens) for r in costs.by_role] == [
        ("crowding", ("openai/gpt-4.1-mini",), 1000),
        ("technical", ("openai/gpt-4.1-mini",), 1000),
        ("news_extractor", ("openai/gpt-4.1-mini",), 1000),
    ]


def test_agents_weights_and_calibration(models: ReadModels) -> None:
    rows = {r.agent: r for r in _value(models.agents(TARGET))}
    crowding, technical = rows["crowding"], rows["technical"]
    assert (crowding.version, crowding.model_slug) == (2, "openai/gpt-4.1")
    assert (crowding.w, crowding.w_capped, crowding.forecasts) == (0.2, 0.19, 214)
    assert crowding.log_loss_30d == pytest.approx(-math.log(0.62))
    assert (crowding.rejected_claims, technical.rejected_claims) == (1.0, 0.0)
    assert crowding.llm_cost_30d == pytest.approx(0.012)
    assert technical.llm_cost_30d == pytest.approx(0.010)
    assert (technical.version, technical.w, technical.forecasts) == (None, 0.15, 120)
    assert rows["macro"].w is None
    points, changes = _value(models.weight_series(TARGET))
    assert [(p.agent, p.w) for p in points if p.agent == "crowding"] == [
        ("crowding", 0.18),
        ("crowding", 0.19),
    ]
    assert [(c.agent, c.version) for c in changes] == [("crowding", 2)]
    calibration = _value(models.calibration(TARGET, "pooled"))
    assert calibration is not None
    assert (calibration.spiegelhalter_z, calibration.ece, calibration.n) == (0.84, 0.041, 187)
    assert [(b.bin_index, b.n) for b in calibration.bins] == [(0, 187)]
    assert _value(models.calibration(TARGET, "macro")) is None
    assert _value(models.calibration_agents(TARGET)) == ("pooled", "crowding")


def test_lessons_board_reads_the_lessons_view(models: ReadModels) -> None:
    board = _value(models.lessons())
    (awaiting,) = board.awaiting
    assert (awaiting.agent, awaiting.state, awaiting.ab_n) == ("macro", "shadow", 60)
    assert (awaiting.ab_logloss_with, awaiting.ab_ci_high) == (0.61, -0.03)
    assert awaiting.title == "Macro overreach in quiet tape"
    (active,) = board.active
    assert (active.agent, active.decided_by, active.review_note) == ("news", "admin", "clear improvement")
    assert active.decided_at is not None
    assert board.in_shadow == ()
    assert board.retired == ()
    assert models.lessons_nav_label() == "Lessons (1)"
