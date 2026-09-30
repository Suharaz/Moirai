"""Phase 11 on Postgres (migration 0011_golive), running as `hdt_scorer`: the one-way evaluation split that
can only start in the future, the weekly job writing `gate_progress` only, the one-shot final G4 evaluation
(`golive_g4_finals` + `gate_flags`, read back by the config-api live gate) over the whole paper account, the
pinned council rules of the replay, the weekly report readers and the Telegram digest."""

from __future__ import annotations

import hashlib
import importlib.util
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy.orm import sessionmaker

from hdt.configapi.routes.mode import read_live_gate
from hdt.core.config import scanner_config, scoring_config, static_config
from hdt.db.models.decision import DecisionCardRow
from hdt.db.models.golive import GoLiveG4FinalRow
from hdt.db.models.ledger import EquitySnapshotRow, FillRow, OrderRow, PositionRow
from hdt.db.models.ops import AlertRow
from hdt.db.models.scoring import GateProgressRow, ScoringLabelRow
from hdt.db.models.settings import ConfigVersionRow, GateFlagRow
from hdt.golive import final
from hdt.golive.ablation import AgentPath, ReplayEvent
from hdt.golive.data import daily_pnl, load_finals, load_replay_events, load_splits
from hdt.golive.g4 import G4Thresholds, write_final, write_progress
from hdt.golive.split import set_split
from hdt.golive.weekly import (
    GoLiveContext,
    calibration_summary,
    compute_g4,
    costs_summary,
    final_blockers,
    iso_week,
    live_comparison,
    render_markdown,
    replay_report,
    send_report,
    weights_summary,
)
from hdt.settings.schemas import COUNCIL_FIELDS
from reader_rows import card_row, insert

pytestmark = [pytest.mark.pg, pytest.mark.integration]

ROOT = Path(__file__).resolve().parents[2]
EQUITY = 10_000.0
PINS = {"crowding": {"model_slug": "openai/gpt-4.1-mini", "agent_version": "1"}}
STATIC = {"scanner": "a" * 64, "scoring": "b" * 64, "council": "c" * 64}


@dataclass
class Db:
    admin: sa.Engine
    role: Callable[[str], sa.Engine]
    url: Callable[[str], str]


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
            yield Db(admin, role, lambda name: pg_role_url(url, name))
        finally:
            for engine in (*engines.values(), admin):
                engine.dispose()


def _db_now(db: Db) -> datetime:
    with db.admin.connect() as conn:
        value: datetime = conn.execute(sa.text("SELECT now()")).scalar_one()
    return value.astimezone(UTC)


def _next_midnight(t: datetime) -> datetime:
    return datetime.combine(t.date(), datetime.min.time(), tzinfo=UTC) + timedelta(days=1)


def _ctx(session: Any, min_entries: int = 3) -> GoLiveContext:
    ctx = GoLiveContext.load(session, static_config(), scoring_config(), scanner_config(), resamples=200)
    return replace(ctx, thresholds=G4Thresholds(min_entries=min_entries))


def _order(event_id: str, at: datetime) -> dict[str, Any]:
    return {
        "account": "paper",
        "client_id": f"c-{event_id}",
        "event_id": event_id,
        "intent_id": None,
        "symbol": "OPUSDT",
        "leg": "entry",
        "side": "BUY",
        "order_type": "LIMIT",
        "tif": "IOC",
        "price": Decimal("1.0"),
        "qty": Decimal("1000"),
        "executed_qty": Decimal("1000"),
        "avg_price": Decimal("1.0"),
        "reduce_only": False,
        "status": "FILLED",
        "exchange_order_id": None,
        "fill_model": "book",
        "created_at": at,
        "updated_at": at,
        "expires_at": None,
        "last_error": None,
    }


def _label(event_id: str, target: str, at: datetime, resolved: datetime) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "coin_id": 1,
        "symbol": "OPUSDT",
        "as_of": at,
        "target_type": target,
        "label_spec_version": "lbl1",
        "horizon_h": 12,
        "status": "resolved",
        "y": 1,
        "label_value": 0.01,
        "resolved_at": resolved,
    }


def _position(event_id: str, at: datetime, closed: datetime) -> dict[str, Any]:
    return {
        "account": "paper",
        "event_id": event_id,
        "symbol": "OPUSDT",
        "side": "LONG",
        "qty": Decimal("0"),
        "max_qty": Decimal("1000"),
        "entry_price": Decimal("1.0"),
        "mark_price": None,
        "unrealized_pnl": Decimal("0"),
        "is_hedge_book": False,
        "opened_at": at,
        "closed_at": closed,
        "exit_notional": Decimal("0"),
        "exit_qty": Decimal("1000"),
        "realized_pnl": Decimal("0"),
        "fees_funding_usd": Decimal("0"),
        "exit_in_progress": False,
        "tp1_done": False,
        "updated_at": closed,
    }


def _snapshot(ts: datetime, equity: float) -> dict[str, Any]:
    value = Decimal(str(equity))
    return {
        "account": "paper",
        "ts": ts,
        "equity": value,
        "available": value,
        "day_start_equity": Decimal(str(EQUITY)),
        "wallet_balance": value,
        "unrealized_pnl": Decimal("0"),
        "open_positions": 0,
    }


def _seed_window(conn: sa.Connection, start: datetime, *, council: int | None = None, n: int = 3) -> None:
    """`n` scored RAW and `n` scored RESID cards, one per hour from `start` + 1 h, each with a filled paper
    entry and a resolved label, plus one unscored RAW card whose position loses 15% of the account."""
    pins: dict[str, int] = {"models": 1, **({"council": council} if council is not None else {})}
    cards = []
    labels = []
    orders = []
    for target, prefix in (("RAW_12H", "raw"), ("RESID_12H", "resid")):
        for i in range(n):
            at = start + timedelta(hours=1 + i)
            event_id = f"{prefix}-{i}"
            cards.append(
                card_row(event_id, "OPUSDT", at, target_type=target, config_version_ids=pins, agents=PINS)
            )
            labels.append(_label(event_id, target, at, at + timedelta(hours=12)))
            orders.append(_order(event_id, at))
    unscored_at = start + timedelta(minutes=30)
    cards.append(
        card_row(
            "raw-unscored",
            "OPUSDT",
            unscored_at,
            target_type="RAW_12H",
            unscored=True,
            config_version_ids=pins,
            agents=PINS,
        )
    )
    orders.append(_order("raw-unscored", unscored_at))
    insert(conn, DecisionCardRow, cards)
    insert(conn, ScoringLabelRow, labels)
    insert(conn, OrderRow, orders)
    closed = start + timedelta(hours=6)
    position_id = conn.execute(
        sa.insert(PositionRow)
        .values(_position("raw-unscored", unscored_at, closed))
        .returning(PositionRow.position_id)
    ).scalar_one()
    insert(
        conn,
        FillRow,
        [
            {
                "account": "paper",
                "symbol": "OPUSDT",
                "trade_id": 1,
                "client_id": "c-exit",
                "exchange_order_id": None,
                "event_id": "raw-unscored",
                "leg": "exit",
                "side": "SELL",
                "price": Decimal("0.85"),
                "qty": Decimal("1000"),
                "fee_usd": Decimal("0"),
                "fee_asset": "USDT",
                "maker": False,
                "realized_pnl": Decimal("-1500"),
                "fill_model": "book",
                "position_id": position_id,
                "filled_at": closed,
            }
        ],
    )
    insert(
        conn,
        EquitySnapshotRow,
        [
            _snapshot(start - timedelta(minutes=1), EQUITY),
            _snapshot(start + timedelta(hours=3), EQUITY - 1_500.0),
            _snapshot(closed, EQUITY - 1_500.0),
        ],
    )


def _set_splits(db: Db, start: datetime, targets: tuple[str, ...] = ("RAW_12H", "RESID_12H")) -> None:
    now = _db_now(db)
    with db.role("hdt_scorer").begin() as conn:
        for target in targets:
            assert set_split(conn, target, start, by="owner", reason="8 weeks of calibration", now=now)


def _load_script(name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_split_is_insert_only_and_never_in_the_past(db: Db) -> None:
    scorer = db.role("hdt_scorer")
    now = _db_now(db)
    start = _next_midnight(now)
    with pytest.raises(ValueError, match="in the past"), scorer.begin() as conn:
        set_split(conn, "RAW_12H", now - timedelta(days=1), by="owner", reason="late", now=now)
    # the database refuses a backdated split even when the code is bypassed
    with pytest.raises(IntegrityError, match="eval_start_not_past"), scorer.begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO golive_eval_splits (target_type, eval_start, decided_by, reason) "
                "VALUES ('RAW_12H', now() - interval '1 day', 'owner', 'late')"
            )
        )
    # and the scorer cannot supply its own creation time
    with pytest.raises(ProgrammingError, match="permission denied"), scorer.begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO golive_eval_splits (target_type, eval_start, decided_by, reason, created_at) "
                "VALUES ('RAW_12H', now() - interval '1 day', 'owner', 'late', now() - interval '2 days')"
            )
        )
    with scorer.begin() as conn:
        assert set_split(conn, "RAW_12H", start, by="owner", reason="8 weeks of calibration", now=now) == 1
        assert (
            set_split(conn, "RAW_12H", start + timedelta(days=7), by="owner", reason="moved", now=now) is None
        )
        assert load_splits(conn) == {"RAW_12H": start}
    with pytest.raises(ProgrammingError, match="permission denied"), scorer.begin() as conn:
        conn.execute(sa.text("UPDATE golive_eval_splits SET eval_start = now()"))
    # bypassing the code, the database still refuses a second split before the first one's final
    with pytest.raises(IntegrityError, match="never moved"), scorer.begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO golive_eval_splits (target_type, eval_start, decided_by, reason) "
                "VALUES ('RAW_12H', now() + interval '9 days', 'owner', 'moved')"
            )
        )
    # and the scorer cannot pick the generation itself
    with pytest.raises(ProgrammingError, match="permission denied"), scorer.begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO golive_eval_splits (target_type, generation, eval_start, decided_by, reason) "
                "VALUES ('RESID_12H', 5, now() + interval '9 days', 'owner', 'skip')"
            )
        )
    with pytest.raises(ValueError, match="decided_by and reason"), scorer.begin() as conn:
        set_split(conn, "RESID_12H", start, by=" ", reason="x", now=now)
    with db.role("hdt_console_ro").connect() as conn:
        assert load_splits(conn) == {"RAW_12H": start}


def test_a_held_transaction_cannot_backdate_a_split(db: Db) -> None:
    """Review M-1: `created_at` is the clock at the insert, not the start of the transaction."""
    with db.role("hdt_scorer").connect() as conn:
        conn.execute(sa.text("SELECT pg_sleep(1.5)"))
        with pytest.raises(IntegrityError, match="eval_start_not_past"):
            conn.execute(
                sa.text(
                    "INSERT INTO golive_eval_splits (target_type, eval_start, decided_by, reason) "
                    "VALUES ('RAW_12H', clock_timestamp() - interval '1 second', 'owner', 'held')"
                )
            )


def test_final_g4_counts_the_whole_account_and_runs_once(db: Db) -> None:
    start = _next_midnight(_db_now(db))
    _set_splits(db, start)
    with db.admin.begin() as conn:
        _seed_window(conn, start)
    now = start + timedelta(days=10)
    sessions = sessionmaker(db.role("hdt_scorer"))
    with sessions() as session:
        conn = session.connection()
        ctx = _ctx(session)
        assert final_blockers(conn, ctx, now=start + timedelta(days=1)) != []
        assert final_blockers(conn, ctx, now=now) == []
        result, ablations = compute_g4(conn, ctx, now=now, final=True)
        session.rollback()
    assert result.required == ("RAW_12H", "RESID_12H")
    raw = result.targets["RAW_12H"]
    assert (raw.eval_start, raw.eval_end, raw.sealed) == (start, start + timedelta(days=1), True)
    checks = {c.key: c for c in raw.checks}
    assert checks["n_entries"].value == 3.0
    assert checks["pinned"].met is True
    # the unscored card's loss is part of RAW's PnL and of the account curve (unrealized snapshots included)
    assert checks["net_pnl"].value == pytest.approx(-1_500.0)
    assert checks["max_drawdown"].met is False
    account = {c.key: c for c in result.account}
    assert account["max_drawdown"].value == pytest.approx(0.15)
    assert account["max_drawdown"].met is False
    assert not result.passed
    assert set(ablations) == {"RAW_12H", "RESID_12H"}

    with sessions() as session, session.begin():
        write_final(
            session.connection(), result, "reports/golive/g4/final-1.json#sha256=ab", static_pins=STATIC
        )
    with pytest.raises(IntegrityError), sessions() as session, session.begin():
        write_final(
            session.connection(), result, "reports/golive/g4/final-2.json#sha256=cd", static_pins=STATIC
        )
    with db.role("hdt_console_ro").connect() as conn:
        flags = conn.execute(sa.select(GateFlagRow.passed, GateFlagRow.evidence_ref)).all()
        finals = load_finals(conn)
        progress = {
            r.check_key: r
            for r in conn.execute(sa.select(GateProgressRow).where(GateProgressRow.gate == "G4"))
        }
    assert [tuple(f) for f in flags] == [(False, "reports/golive/g4/final-1.json#sha256=ab")]
    assert set(finals) == {"RAW_12H", "RESID_12H"}
    assert all(not f.gate_passed for f in finals.values())
    assert progress["overall"].met is False
    assert progress["account.max_drawdown"].met is False
    with sessionmaker(db.role("hdt_configapi"))() as session:
        gate = read_live_gate(session)
    assert gate.g4 is not None
    assert gate.g4["passed"] is False
    with sessions() as session:
        blockers = final_blockers(session.connection(), _ctx(session), now=now)
    assert any("already recorded" in b for b in blockers)

    # review M-2: the weekly interim run after the final leaves the recorded rows as the final wrote them
    with sessions() as session, session.begin():
        conn = session.connection()
        interim, _ = compute_g4(conn, _ctx(session), now=now + timedelta(days=1))
        write_progress(conn, interim, decided=load_finals(conn).keys())
    with db.role("hdt_console_ro").connect() as conn:
        after = {
            r.check_key: r
            for r in conn.execute(sa.select(GateProgressRow).where(GateProgressRow.gate == "G4"))
        }
    assert after["overall"].met is False
    assert after["account.max_drawdown"].met is False
    assert after["RAW_12H.max_drawdown"].met is False
    assert after["RAW_12H.ablation"].met == progress["RAW_12H.ablation"].met

    # a new split of both target types is the re-evaluation path: the finals of the old ones stop counting
    with sessions() as session, session.begin():
        conn = session.connection()
        later = now + timedelta(days=2)
        for target in ("RAW_12H", "RESID_12H"):
            assert (
                set_split(conn, target, _next_midnight(later), by="owner", reason="re-evaluate", now=later)
                == 2
            )
        assert load_finals(conn) == {}
        assert final_blockers(conn, _ctx(session), now=later) != []


def test_a_narrower_target_list_cannot_pass_the_gate(db: Db) -> None:
    start = _next_midnight(_db_now(db))
    _set_splits(db, start)
    with db.admin.begin() as conn:
        _seed_window(conn, start)
    sessions = sessionmaker(db.role("hdt_scorer"))
    with sessions() as session:
        conn = session.connection()
        result, _ = compute_g4(
            conn, _ctx(session), now=start + timedelta(days=10), required=["RAW_12H"], final=True
        )
        session.rollback()
    assert result.required == ("RAW_12H", "RESID_12H")
    assert not result.targets["RESID_12H"].passed
    assert not result.passed
    with sessions() as session, session.begin():
        write_final(
            session.connection(), result, "reports/golive/g4/final.json#sha256=ab", static_pins=STATIC
        )
    with db.role("hdt_console_ro").connect() as conn:
        assert conn.execute(sa.select(GateFlagRow.passed)).scalars().all() == [False]


def test_final_refuses_an_unsealed_window_or_a_missing_split(db: Db) -> None:
    start = _next_midnight(_db_now(db))
    _set_splits(db, start, ("RAW_12H",))
    with db.admin.begin() as conn:
        _seed_window(conn, start)
    sessions = sessionmaker(db.role("hdt_scorer"))
    with sessions() as session:
        conn = session.connection()
        blockers = final_blockers(conn, _ctx(session), now=start + timedelta(days=10))
        unsealed = final_blockers(conn, _ctx(session, min_entries=150), now=start + timedelta(days=10))
        interim, _ = compute_g4(conn, _ctx(session), now=start + timedelta(days=10))
    assert any(b.startswith("RESID_12H: no calibration / evaluation split") for b in blockers)
    assert any("window not sealed" in b for b in unsealed)
    with pytest.raises(ValueError, match="final"), sessions() as session, session.begin():
        write_final(session.connection(), interim, "x", static_pins=STATIC)


def test_final_command_records_once_with_evidence_it_never_overwrites(
    db: Db, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # a split set weeks ago (as the owner did then); 150 paper entries per target type since
    start = _next_midnight(_db_now(db)) - timedelta(days=30)
    with db.admin.begin() as conn:
        for target in ("RAW_12H", "RESID_12H"):
            conn.execute(
                sa.text(
                    "INSERT INTO golive_eval_splits (target_type, eval_start, decided_by, reason, "
                    "created_at) VALUES (:t, :s, 'owner', 'test', :c)"
                ),
                {"t": target, "s": start, "c": start - timedelta(days=1)},
            )
        _seed_window(conn, start, n=150)
    monkeypatch.setenv("HDT_PG_DSN", db.url("hdt_scorer"))

    assert final.main(["run", "--dry-run", "--reports", str(tmp_path)]) == 0
    assert not (tmp_path / "g4").exists()
    capsys.readouterr()
    assert final.main(["run", "--reports", str(tmp_path)]) == 0
    (evidence,) = (tmp_path / "g4").iterdir()
    text = evidence.read_text(encoding="utf-8")
    assert '"stage": "final"' in text
    assert '"static_pins"' in text
    with db.role("hdt_console_ro").connect() as conn:
        flags = conn.execute(sa.select(GateFlagRow.passed, GateFlagRow.evidence_ref)).all()
        finals = load_finals(conn)
    assert len(flags) == 1
    assert flags[0].passed is False
    assert (
        flags[0].evidence_ref == f"{evidence.as_posix()}#sha256={hashlib.sha256(text.encode()).hexdigest()}"
    )
    assert {t: (f.eval_start, f.eval_end) for t, f in finals.items()} == {
        "RAW_12H": (start, start + timedelta(days=7)),
        "RESID_12H": (start, start + timedelta(days=7)),
    }
    # the second run is refused: no new flag, no new evidence file
    assert final.main(["run", "--reports", str(tmp_path)]) == 2
    assert "already recorded" in capsys.readouterr().err
    assert list((tmp_path / "g4").iterdir()) == [evidence]
    with db.role("hdt_console_ro").connect() as conn:
        assert conn.execute(sa.select(sa.func.count()).select_from(GateFlagRow)).scalar_one() == 1
        pins: list[dict[str, Any] | None] = list(conn.execute(sa.select(GoLiveG4FinalRow.pins)).scalars())
    assert all(p is not None and set(p["static"]) == {"scanner", "scoring", "council"} for p in pins)

    # review M-2: the next weekly report keeps the final's rows, it never turns them back into interim
    weekly = _load_script("weekly_report")
    assert weekly.main(["--skip-event-study", "--no-telegram", "--reports", str(tmp_path / "w")]) == 0
    with db.role("hdt_console_ro").connect() as conn:
        overall = conn.execute(
            sa.select(GateProgressRow.met).where(
                GateProgressRow.gate == "G4", GateProgressRow.check_key == "overall"
            )
        ).scalar_one()
    assert overall is False


def test_weekly_report_writes_progress_but_never_a_gate_flag(
    db: Db, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    start = _next_midnight(_db_now(db))
    _set_splits(db, start)
    monkeypatch.setenv("HDT_PG_DSN", db.url("hdt_scorer"))
    weekly = _load_script("weekly_report")
    assert weekly.main(["--skip-event-study", "--no-telegram", "--reports", str(tmp_path)]) == 0
    with db.role("hdt_console_ro").connect() as conn:
        assert conn.execute(sa.select(sa.func.count()).select_from(GateFlagRow)).scalar_one() == 0
        assert conn.execute(sa.select(sa.func.count()).select_from(GoLiveG4FinalRow)).scalar_one() == 0
        overall = conn.execute(
            sa.select(GateProgressRow.met).where(
                GateProgressRow.gate == "G4", GateProgressRow.check_key == "overall"
            )
        ).scalar_one()
    assert overall is None
    assert not (tmp_path / "g4").exists()
    # a dry run writes no progress either
    with db.admin.begin() as conn:
        conn.execute(sa.delete(GateProgressRow))
    assert weekly.main(["--skip-event-study", "--dry-run", "--reports", str(tmp_path)]) == 0
    with db.role("hdt_console_ro").connect() as conn:
        assert conn.execute(sa.select(sa.func.count()).select_from(GateProgressRow)).scalar_one() == 0


def test_replay_uses_the_council_rules_pinned_on_each_card(db: Db) -> None:
    static = static_config()
    payload = static.council.model_dump(mode="json", include=set(COUNCIL_FIELDS))
    payload["supermajority"] = 0.8
    assert static.council.supermajority != 0.8
    with db.admin.begin() as conn:
        version = conn.execute(
            sa.insert(ConfigVersionRow)
            .values(
                section="council",
                payload_json=payload,
                payload_sha256="c" * 64,
                schema_version=1,
                author="owner",
                reason="test",
                created_at=datetime(2026, 1, 1, tzinfo=UTC),
                parent_id=None,
            )
            .returning(ConfigVersionRow.id)
        ).scalar_one()
        start = datetime(2026, 1, 5, tzinfo=UTC)
        _seed_window(conn, start, council=version)
        insert(
            conn,
            DecisionCardRow,
            [card_row("raw-yaml", "OPUSDT", start + timedelta(hours=5), target_type="RAW_12H")],
        )
        insert(conn, ScoringLabelRow, [_label("raw-yaml", "RAW_12H", start + timedelta(hours=5), start)])
    with db.role("hdt_scorer").connect() as conn:
        events = {e.event_id: e for e in load_replay_events(conn, static=static)}
    assert events["raw-0"].rules is not None
    assert events["raw-0"].rules.supermajority == pytest.approx(0.8)
    assert events["raw-0"].config_version_ids == {"models": 1, "council": version}
    assert events["raw-0"].agent_pins == {"crowding": ("openai/gpt-4.1-mini", "1")}
    assert events["raw-yaml"].rules is not None
    assert events["raw-yaml"].rules.supermajority == pytest.approx(static.council.supermajority)


def test_hedge_book_pnl_is_shared_by_the_alt_positions_it_hedged(db: Db) -> None:
    start = datetime(2026, 1, 5, tzinfo=UTC)
    opened = start + timedelta(hours=1)
    with db.admin.begin() as conn:
        rows = [
            _position("raw-0", opened, opened + timedelta(hours=4)),
            _position("resid-0", opened, opened + timedelta(hours=2)),
            {
                **_position("hedge-0", opened, opened + timedelta(hours=4)),
                "event_id": None,
                "is_hedge_book": True,
            },
        ]
        ids = [
            conn.execute(sa.insert(PositionRow).values(r).returning(PositionRow.position_id)).scalar_one()
            for r in rows
        ]
        insert(
            conn,
            FillRow,
            [
                {
                    "account": "paper",
                    "symbol": "BTCUSDT",
                    "trade_id": 7,
                    "client_id": "c-hedge",
                    "exchange_order_id": None,
                    "event_id": None,
                    "leg": "hedge",
                    "side": "BUY",
                    "price": Decimal("60000"),
                    "qty": Decimal("0.01"),
                    "fee_usd": Decimal("0"),
                    "fee_asset": "USDT",
                    "maker": False,
                    "realized_pnl": Decimal("300"),
                    "fill_model": "book",
                    "position_id": ids[2],
                    "filled_at": opened + timedelta(hours=4),
                }
            ],
        )
    with db.role("hdt_scorer").connect() as conn:
        pnl = daily_pnl(
            conn,
            "paper",
            start=start,
            end=start + timedelta(days=1),
            event_targets={"raw-0": "RAW_12H", "resid-0": "RESID_12H"},
        )
    # equal notionals, RAW open 4 h and RESID 2 h of the hedge's 4 h: two thirds to RAW, one third to RESID
    assert pnl.by_target["RAW_12H"][start.date()] == pytest.approx(200.0)
    assert pnl.by_target["RESID_12H"][start.date()] == pytest.approx(100.0)
    assert pnl.unattributed == pytest.approx(0.0)


def _event(i: int, as_of: datetime) -> ReplayEvent:
    s = 1 if i % 2 == 0 else -1
    agents = tuple(
        AgentPath(name, 0.5 + 0.1 * s, 0.5 + 0.2 * s, 0.5, 0.5) for name in ("crowding", "news", "technical")
    )
    return ReplayEvent(
        f"g{i}",
        1,
        as_of,
        "LTX",
        "RAW_12H",
        "v1",
        1 if s > 0 else 0,
        2,
        "max_rounds",
        0.5 + 0.1 * s,
        0.3,
        "log_pool",
        1,
        {"crowding": 1.0, "news": 1.0, "technical": 1.0},
        {},
        {},
        0.0,
        {},
        (0.02, 0.98),
        agents,
    )


def test_weekly_pipeline_reads_every_source_as_the_scorer(db: Db) -> None:
    start = _next_midnight(_db_now(db))
    _set_splits(db, start, ("RAW_12H",))
    scorer = db.role("hdt_scorer")
    now = start + timedelta(days=60, hours=1)
    sessions = sessionmaker(scorer)
    with sessions() as session:
        conn = session.connection()
        ctx = _ctx(session, min_entries=150)
        assert load_replay_events(conn, static=ctx.static, end=now) == []
        events = [_event(i, start - timedelta(days=20) + timedelta(hours=10 * i)) for i in range(30)]
        events += [_event(100 + i, start + timedelta(hours=10 * i)) for i in range(30)]
        g4, ablations = compute_g4(conn, ctx, now=now, events=events)
        replay = replay_report(conn, ctx, now=now, events=events)
        costs = costs_summary(conn, since=now - timedelta(days=7), until=now)
        live = live_comparison(conn, ctx)
        weights, calibration = weights_summary(conn), calibration_summary(conn)
        write_progress(conn, g4)
        session.rollback()
    # no paper orders exist: N is 0 and the window stays open; the weekly job computes no evaluation ablation
    raw = g4.targets["RAW_12H"]
    assert next(c for c in raw.checks if c.key == "n_entries").value == 0.0
    assert next(c for c in raw.checks if c.key == "ablation").met is None
    assert raw.sealed is False
    assert g4.missing_split == ()
    assert not g4.passed
    assert ablations == {}
    # the calibration set is the 30 events before the split
    assert replay.ablations["RAW_12H"].n_events == 30
    assert replay.proposals
    assert costs["llm_total_usd"] == 0.0
    assert costs["paper_funding_usd"] == 0.0
    assert live is None
    assert weights == []
    assert calibration == []

    report = {
        "week": iso_week(now),
        "generated_at": now.isoformat(),
        "report_path": "reports/golive/weekly/x.md",
        "g4": g4.to_json(),
        "g4_finals": [],
        "replay": replay.to_json(),
        "event_study": None,
        "live": None,
        "costs": costs,
        "notes": replay.notes,
        "gates": [],
        "weights": weights,
        "calibration": calibration,
    }
    markdown = render_markdown(report)
    assert "## Gate G4 interim" in markdown
    assert "Final evaluation: not run yet." in markdown
    with sessions() as session, session.begin():
        alert_id = send_report(session, report)
    assert alert_id is not None
    with sessions() as session, session.begin():
        assert send_report(session, report) is None
    with db.admin.connect() as conn:
        alert = conn.execute(sa.select(AlertRow).where(AlertRow.alert_id == alert_id)).one()
    assert (alert.kind, alert.severity, alert.service, alert.dedupe_key) == (
        "weekly_report",
        "info",
        "scorer",
        iso_week(now),
    )
    assert "G4 final: not run yet" in (alert.detail or "")
