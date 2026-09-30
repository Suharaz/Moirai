"""public-publisher against a real Postgres, read through the `hdt_publisher_ro` role.

Every source table comes from the migrations (council 0009, LLM 0007, scoring 0010, ledger, candidate sets
and settings), seeded with rows that satisfy their real NOT NULL and CHECK constraints, and read with the
grants the migrations give `hdt_publisher_ro`. The test exercises the real SQL, the real grants (column-level
on the order tables) and the allowlist end to end.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

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

from hdt.contracts.common import ClaimKind
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
from hdt.public.allowlist import check_snapshot
from hdt.public.publisher import BLOB_PREFIX, LATEST_KEY, MANIFEST_PREFIX, Publisher, SnapshotWriter
from hdt.public.store import ObjectNotFoundError

pytestmark = [pytest.mark.pg, pytest.mark.integration]

ROOT = Path(__file__).resolve().parents[2]
NOW = datetime(2026, 9, 27, 12, 0, tzinfo=UTC)
LEVEL_A = "lc_" + "A" * 16
LEVEL_B = "lc_" + "B" * 16
POSITION_SQL = sa.text(
    "INSERT INTO positions (account, event_id, symbol, side, qty, max_qty, entry_price, mark_price, "
    "unrealized_pnl, leverage, margin_type, liquidation_price, stop_price, initial_stop_price, "
    "tp1_price, is_hedge_book, opened_at, time_stop_at, closed_at, exit_price, exit_reason, "
    "exit_notional, exit_qty, realized_pnl, fees_funding_usd, r_multiple, exit_in_progress, tp1_done, "
    "updated_at) VALUES (:account, :event_id, :symbol, :side, :qty, :max_qty, :entry, :mark, :upnl, 3, "
    "'ISOLATED', :liq, :stop, :stop, :tp1, :hedge, :opened, :time_stop, :closed, :exit, :reason, "
    ":exit_notional, :exit_qty, :pnl, :fees, :r, false, false, :opened)"
)
POSITION_DEFAULTS: dict[str, Any] = {
    "mark": None,
    "upnl": None,
    "time_stop": None,
    "closed": None,
    "exit": None,
    "reason": None,
    "pnl": None,
    "fees": None,
    "r": None,
    "hedge": False,
}
ORDER_SQL = sa.text(
    "INSERT INTO orders (account, client_id, event_id, symbol, leg, side, order_type, price, qty, "
    "executed_qty, avg_price, reduce_only, status, created_at, updated_at) VALUES ('testnet', :client_id, "
    "'EVT-OPEN', 'SOLUSDT', :leg, :side, :type, :price, 5, :executed, :avg, :reduce_only, :status, :at, :at)"
)
ALGO_ORDER_SQL = sa.text(
    "INSERT INTO algo_orders (account, client_algo_id, event_id, symbol, leg, side, order_type, "
    "trigger_price, close_position, reduce_only, working_type, status, created_at, updated_at) VALUES "
    "('testnet', 's-abc123', 'EVT-OPEN', 'SOLUSDT', 'sl', 'SELL', 'STOP_MARKET', 142.0, true, true, "
    "'MARK_PRICE', 'NEW', :at, :at)"
)
FILL_SQL = sa.text(
    "INSERT INTO fills (account, symbol, trade_id, client_id, side, price, qty, realized_pnl, filled_at) "
    "VALUES ('testnet', 'SOLUSDT', :trade_id, 'e-abc123', 'BUY', :price, :qty, 0, :at)"
)
RISK_SQL = sa.text(
    "INSERT INTO risk_verdicts (account, event_id, coin_id, symbol, decision_intent, effective_intent, "
    "verdict, reason, intent_id, entry_client_id, sizing, checks, config_version_ids, decided_at) VALUES "
    "(:account, 'EVT-OPEN', 1, 'SOLUSDT', 'OPEN', :effective, :verdict, :reason, 'int-secret1', "
    ":entry, '{\"risk_usd\": 80}', '[]', '{}', :at)"
)


def _position(conn: sa.Connection, **values: Any) -> None:
    row = POSITION_DEFAULTS | values
    closed = row["closed"] is not None
    conn.execute(
        POSITION_SQL,
        row
        | {
            # The ledger zeroes `qty` on close and keeps the largest size in `max_qty`.
            "qty": 0 if closed else row["qty"],
            "max_qty": row["qty"],
            "upnl": row["upnl"] or 0,
            "pnl": row["pnl"] or 0,
            "fees": row["fees"] or 0,
            "exit_notional": row["exit"] * row["qty"] if closed else 0,
            "exit_qty": row["qty"] if closed else 0,
        },
    )


def _closed_op_trade(conn: sa.Connection, account: str, event_id: str) -> None:
    """A closed OPUSDT trade with every level set (stop 1.701, TP1 1.804, liquidation 1.2)."""
    _position(
        conn,
        account=account,
        event_id=event_id,
        symbol="OPUSDT",
        side="LONG",
        qty=100,
        entry=1.742,
        liq=1.2,
        stop=1.701,
        tp1=1.804,
        opened=NOW - timedelta(hours=40),
        closed=NOW - timedelta(hours=28),
        exit=1.7838,
        reason="time_stop",
        pnl=16.4,
        fees=-0.62,
        r=1.02,
    )


class MemoryStore:
    """In-memory stand-in for the S3 bucket (same put/get/delete/list semantics as `PublicStore`)."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.puts: list[str] = []

    def put(
        self, key: str, body: bytes, *, content_type: str = "application/json", cache_control: str = ""
    ) -> None:
        self.objects[key] = body
        self.puts.append(key)

    def get(self, key: str) -> bytes:
        try:
            return self.objects[key]
        except KeyError:
            raise ObjectNotFoundError(key, status=404) from None

    def delete(self, key: str) -> None:
        if key not in self.objects:
            raise ObjectNotFoundError(key, status=404)
        del self.objects[key]

    def list(self, prefix: str) -> list[str]:
        return sorted(k for k in self.objects if k.startswith(prefix))


def _card(event_id: str, symbol: str, as_of: datetime, **extra: Any) -> dict[str, Any]:
    # Every entry with a known `stage` is published (risk and order lines included); free text without a
    # stage and entries with a malformed timestamp are dropped.
    timeline = [
        {"at": as_of.isoformat(), "stage": "scanner", "text": "Scanner emitted a LTX candidate"},
        {"at": as_of.isoformat(), "stage": "risk", "text": "Risk: approved, sizing 0.8% of equity"},
        {"at": as_of.isoformat(), "text": "Orders sent: entry and STOP at 1.701", "tone": ""},
        {"at": as_of.isoformat(), "stage": "orders", "text": "Verdict: APPROVED", "tone": "pos"},
        {"at": "yesterday", "stage": "council", "text": "Round 2 closed", "tone": ""},
    ]
    return card_row(event_id, symbol, as_of, timeline=timeline, **extra)


def _seed(conn: sa.Connection) -> None:
    ins = conn.execute
    for hours in range(0, 24 * 40, 6):
        ts = NOW - timedelta(hours=hours)
        ins(
            sa.text(
                "INSERT INTO equity_snapshots (account, ts, equity, available, day_start_equity, "
                "wallet_balance, unrealized_pnl, open_positions) "
                "VALUES ('testnet', :ts, :eq, :eq, 10000, :eq, 0, 0)"
            ),
            {"ts": ts, "eq": 10000 + (24 * 40 - hours) * 0.5},
        )
    # Paper keeps trading internally but is never public: its equity and its verdict below stay private.
    ins(
        sa.text(
            "INSERT INTO equity_snapshots (account, ts, equity, available, day_start_equity, "
            "wallet_balance, unrealized_pnl, open_positions) "
            "VALUES ('paper', :ts, 10000, 10000, 10000, 10000, 0, 0)"
        ),
        {"ts": NOW - timedelta(minutes=1)},
    )
    _position(
        conn,
        account="testnet",
        event_id="EVT-OPEN",
        symbol="SOLUSDT",
        side="LONG",
        qty=5,
        entry=147.8,
        mark=150.1,
        upnl=11.59,
        liq=99.0,
        stop=142.0,
        tp1=160.0,
        opened=NOW - timedelta(hours=14),
        time_stop=NOW - timedelta(hours=2),
    )
    _position(
        conn,
        account="testnet",
        event_id=None,
        symbol="BTCUSDT",
        side="SHORT",
        qty=0.01,
        entry=65000,
        mark=64900,
        upnl=1.0,
        liq=90000,
        stop=None,
        tp1=None,
        hedge=True,
        opened=NOW - timedelta(days=3),
    )
    # The entry fill and its protective STOP are public; the rejected exit never reached the book. The
    # entry carries no `avg_price` (execution sets it only from a REST read-back): its fills give 147.6.
    opened = NOW - timedelta(hours=14)
    for order in (
        {
            "client_id": "e-abc123",
            "leg": "entry",
            "side": "BUY",
            "type": "LIMIT",
            "price": 147.8,
            "executed": 5,
            "avg": None,
            "reduce_only": False,
            "status": "FILLED",
            "at": opened,
        },
        {
            "client_id": "x-rejected1",
            "leg": "exit",
            "side": "SELL",
            "type": "MARKET",
            "price": None,
            "executed": 0,
            "avg": None,
            "reduce_only": True,
            "status": "REJECTED",
            "at": NOW - timedelta(hours=1),
        },
    ):
        ins(ORDER_SQL, order)
    for trade_id, price, qty in ((1, 147.0, 2), (2, 148.0, 3)):
        ins(FILL_SQL, {"trade_id": trade_id, "price": price, "qty": qty, "at": opened})
    ins(ALGO_ORDER_SQL, {"at": opened + timedelta(seconds=1)})
    ins(
        RISK_SQL,
        {
            "account": "testnet",
            "effective": "OPEN",
            "verdict": "approved",
            "reason": None,
            "entry": "e-abc123",
            "at": opened,
        },
    )
    ins(
        RISK_SQL,
        {
            "account": "paper",
            "effective": None,
            "verdict": "rejected",
            "reason": "max_open_positions",
            "entry": None,
            "at": opened,
        },
    )
    # Operational reasons (kill switch, position ceilings) stay private: published as `operational_limit`.
    ins(
        RISK_SQL,
        {
            "account": "live",
            "effective": None,
            "verdict": "rejected",
            "reason": "kill_state",
            "entry": None,
            "at": opened,
        },
    )
    _closed_op_trade(conn, "testnet", "EVT-CLOSED")
    candidate_payload = {
        "coin_id": 1,
        "as_of": (NOW - timedelta(hours=40)).isoformat(),
        "levels_ver": "v1",
        "candidates": [
            {
                "candidate_id": LEVEL_A,
                "side": "LONG",
                "entry": "1.742",
                "invalidation": "1.701",
                "tp1": "1.804",
                "rr": float((1.804 - 1.742) / (1.742 - 1.701)),
                "tick": "0.001",
            },
            {
                "candidate_id": LEVEL_B,
                "side": "SHORT",
                "entry": "1.742",
                "invalidation": "1.783",
                "tp1": "1.680",
                "rr": float((1.742 - 1.680) / (1.783 - 1.742)),
                "tick": "0.001",
            },
        ],
    }
    ins(
        sa.text(
            "INSERT INTO candidate_sets (candidate_set_sha256, coin_id, as_of, payload, created_at) "
            "VALUES ('set-closed', 1, :t, :p, :t)"
        ),
        {"t": NOW - timedelta(hours=40), "p": json.dumps(candidate_payload)},
    )
    cards = [
        _card(
            "EVT-CLOSED",
            "OPUSDT",
            NOW - timedelta(hours=40),
            candidate_id=LEVEL_A,
            candidate_set_sha256="set-closed",
        ),
        _card("EVT-OPEN", "SOLUSDT", NOW - timedelta(hours=15)),
        _card("EVT-YOUNG", "ARBUSDT", NOW - timedelta(hours=2)),
        _card("EVT-UNLABELED", "ARBUSDT", NOW - timedelta(hours=30)),
        # A held-coin re-evaluation: HOLD of the held side, size 0, never scored.
        _card(
            "EVT-HELD",
            "ARBUSDT",
            NOW - timedelta(hours=20),
            source="HELD",
            outcome="HOLD",
            side="LONG",
            held_side="LONG",
            intent="HOLD",
            manager_size=0.0,
            unscored=True,
        ),
        _card(
            "EVT-SOL-EARLIER",
            "SOLUSDT",
            NOW - timedelta(hours=13),
            outcome="NO_TRADE",
            side=None,
            manager_size=0.0,
        ),
        _card("EVT-SOL-BEFORE-OPEN", "SOLUSDT", NOW - timedelta(hours=30)),
    ]
    insert(conn, DecisionCardRow, cards)
    as_of = {str(c["event_id"]): c["as_of"] for c in cards}
    # The scorer writes a row only once the 12 h label is resolved: EVT-UNLABELED has none yet.
    insert(
        conn,
        ScoredForecastRow,
        [
            scored_row(e, "pooled", as_of=as_of[e], hit=hit, scored_at=NOW - timedelta(hours=1))
            for e, hit in [
                ("EVT-CLOSED", True),
                ("EVT-OPEN", True),
                ("EVT-YOUNG", False),
                ("EVT-SOL-EARLIER", True),
                ("EVT-SOL-BEFORE-OPEN", False),
            ]
        ],
    )
    forecast = agent_forecast(
        "EVT-CLOSED",
        "crowding",
        as_of["EVT-CLOSED"],
        0.62,
        p_model=0.6,
        p_llm=0.63,
        candidate_id=LEVEL_A,
        reason="private rationale",
        prompt_hash="d" * 64,
        model_slug="openai/gpt-4.1-mini",
    )
    insert(conn, DecisionForecastRow, [forecast_row(forecast, stance="LONG", weight_norm=0.24)])
    claim = Claim(
        claim_id="x", kind=ClaimKind.PACKET, ref="rsi_1h", statement="RSI 1h is 71", verified=True, hard=False
    )
    insert(conn, DecisionClaimRow, [claim_row("EVT-CLOSED", 2, "C-001", "technical", claim)])
    insert(
        conn,
        LlmCallRow,
        [
            llm_call_row(
                "g1",
                NOW - timedelta(hours=40),
                pipeline="council",
                role="crowding",
                event_id="EVT-CLOSED",
                model_slug="openai/gpt-4.1-mini",
                prompt_tokens=1000,
                completion_tokens=200,
                cost_usd=0.012,
            ),
            llm_call_row(
                "g2",
                NOW - timedelta(minutes=5),
                pipeline="news",
                role="news_extractor",
                event_id=None,
                model_slug="google/gemini-2.5-flash",
                prompt_tokens=5000,
                completion_tokens=300,
                cost_usd=0.004,
            ),
        ],
    )
    insert(
        conn,
        WeightHistoryRow,
        [weight_row("crowding", NOW - timedelta(days=1), w=0.2, a=0.9, r=0.6, coverage=0.97, forecasts=214)],
    )
    bins, stats = calibration_rows("pooled", NOW - timedelta(days=1), z=0.84, ece=0.041, n=187)
    insert(conn, CalibrationBinRow, [bins])
    insert(conn, CalibrationStatRow, [stats])
    insert(
        conn,
        GateProgressRow,
        [
            {
                "gate": "G1",
                "check_key": "progress",
                "label": "LTX edge on the lake",
                "value": 41,
                "target": 120,
                "met": False,
                "updated_at": NOW,
            },
            {
                "gate": "G1",
                "check_key": "calibration",
                "label": "Calibration samples",
                "value": 20,
                "target": 100,
                "met": False,
                "updated_at": NOW,
            },
        ],
    )
    ins(
        sa.text(
            "INSERT INTO agent_versions (agent, version, model_slug, provider_prefs_hash, created_by, "
            "created_at) VALUES ('crowding', 2, 'openai/gpt-4.1-mini', 'h', 'admin', :t)"
        ),
        {"t": NOW - timedelta(days=5)},
    )


@pytest.fixture(scope="module")
def seeded_url(fresh_database: Callable[[], AbstractContextManager[str]]) -> Iterator[str]:
    with fresh_database() as url:
        cfg = Config(str(ROOT / "alembic.ini"))
        cfg.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
        command.upgrade(cfg, "head")
        admin = sa.create_engine(url)
        with admin.begin() as conn:
            _seed(conn)
        admin.dispose()
        yield url


@pytest.fixture
def built(seeded_url: str, pg_role_url: Callable[[str, str], str]) -> dict[str, Any]:
    engine = sa.create_engine(pg_role_url(seeded_url, "hdt_publisher_ro"))
    try:
        return Publisher(engine, SnapshotWriter(MemoryStore(), retention=timedelta(hours=1))).build(NOW)  # type: ignore[arg-type]
    finally:
        engine.dispose()


def test_snapshot_from_database_passes_validation(built: dict[str, Any]) -> None:
    check_snapshot(built)


def test_every_decision_is_published_at_once(built: dict[str, Any]) -> None:
    listed = {d["event_id"]: d for d in built["decisions"]}
    # Newest first, young, unscored and open events included.
    assert list(listed) == [
        "EVT-YOUNG",
        "EVT-SOL-EARLIER",
        "EVT-OPEN",
        "EVT-HELD",
        "EVT-UNLABELED",
        "EVT-SOL-BEFORE-OPEN",
        "EVT-CLOSED",
    ]
    assert listed["EVT-UNLABELED"]["hit"] is None
    assert listed["EVT-YOUNG"]["hit"] is False
    assert [c["event_id"] for c in built["decision_cards"]] == list(listed)


def test_open_positions_carry_levels_and_decision(built: dict[str, Any]) -> None:
    (testnet,) = built["accounts"]
    assert testnet["account"] == "testnet"
    positions = {p["symbol"]: p for p in testnet["open_positions"]}
    sol = positions["SOLUSDT"]
    assert (sol["event_id"], sol["stop_price"], sol["tp1_price"], sol["liquidation_price"]) == (
        "EVT-OPEN",
        142.0,
        160.0,
        99.0,
    )
    assert positions["BTCUSDT"]["event_id"] is None
    assert positions["BTCUSDT"]["qty"] == 0.01
    (closed,) = testnet["closed_trades"]
    assert (closed["stop_price"], closed["tp1_price"], closed["liquidation_price"]) == (1.701, 1.804, 1.2)
    assert closed["qty"] == 100  # the size traded (max_qty), not the zeroed qty of a closed position
    assert testnet["trade_stats_30d"]["closed"] == 1
    assert testnet["today_pnl"] == pytest.approx(testnet["equity"] - 10000)


def test_orders_list_accepted_orders_without_identifiers(built: dict[str, Any]) -> None:
    (testnet,) = built["accounts"]
    assert [(o["leg"], o["order_type"], o["status"], o["price"]) for o in testnet["orders"]] == [
        ("sl", "STOP_MARKET", "NEW", 142.0),
        ("entry", "LIMIT", "FILLED", 147.8),
    ]
    stop, entry = testnet["orders"]
    assert (stop["executed_qty"], stop["avg_price"], stop["reduce_only"]) == (None, None, True)
    assert (entry["executed_qty"], entry["side"]) == (5, "BUY")
    assert entry["avg_price"] == pytest.approx(147.6)
    blob = json.dumps(built)
    for identifier in ("e-abc123", "s-abc123", "x-rejected1"):
        assert identifier not in blob


def test_decision_card_shows_the_risk_review_and_orders_of_its_event(built: dict[str, Any]) -> None:
    card = next(c for c in built["decision_cards"] if c["event_id"] == "EVT-OPEN")
    assert [
        (r["account"], r["council_action"], r["applied_action"], r["result"], r["reason"])
        for r in card["risk"]
    ] == [
        ("live", "OPEN", None, "rejected", "operational_limit"),
        ("testnet", "OPEN", "OPEN", "approved", None),
    ]
    assert [(o["account"], o["leg"], o["status"]) for o in card["orders"]] == [
        ("testnet", "entry", "FILLED"),
        ("testnet", "sl", "NEW"),
    ]
    assert card["orders"][0]["avg_price"] == pytest.approx(147.6)
    blob = json.dumps(card)
    for private in ("int-secret1", "e-abc123", "risk_usd", "x-rejected1", "kill_state", "max_open_positions"):
        assert private not in blob
    assert next(c for c in built["decision_cards"] if c["event_id"] == "EVT-YOUNG")["orders"] == []


def test_decision_card_content_is_public_grade(built: dict[str, Any]) -> None:
    card = next(c for c in built["decision_cards"] if c["event_id"] == "EVT-CLOSED")
    assert [t["text"] for t in card["timeline"]] == [
        "Scanner emitted a LTX candidate",
        "Risk: approved, sizing 0.8% of equity",
        "Verdict: APPROVED",
    ]
    assert [(lv["candidate_id"], lv["chosen"]) for lv in card["levels"]] == [
        (LEVEL_A, True),
        (LEVEL_B, False),
    ]
    (trade,) = card["trades"]
    assert (trade["symbol"], trade["exit_reason"]) == ("OPUSDT", "time_stop")
    (forecast,) = card["forecasts"]
    assert forecast["p_used"] == pytest.approx(0.62)
    assert "reason" not in forecast
    assert "prompt_hash" not in forecast
    assert card["usage"] == {"cost_usd": pytest.approx(0.012), "calls": 1, "agents": 1}
    opened = next(c for c in built["decision_cards"] if c["event_id"] == "EVT-OPEN")
    (open_trade,) = opened["trades"]
    assert (open_trade["closed_at"], open_trade["stop_price"]) == (None, 142.0)
    held = next(c for c in built["decision_cards"] if c["event_id"] == "EVT-HELD")
    assert held["levels"] == []
    assert held["trades"] == []
    blob = json.dumps(built)
    for secret_field in ("config_version_ids", "entry_client_id", "e-abc123", "private rationale"):
        assert secret_field not in blob


def test_writer_dedupes_sections_and_prunes_old_versions(built: dict[str, Any]) -> None:
    snapshot = built
    store = MemoryStore()
    writer = SnapshotWriter(store, retention=timedelta(hours=1))  # type: ignore[arg-type]
    first = writer.publish(snapshot, NOW)
    assert first.blobs_written == 7
    changed = dict(snapshot) | {
        "generated_at": "2026-09-27T12:01:00.000000Z",
        "scoring": {"scored_30d": 9, "hits_30d": 5},
    }
    second = writer.publish(changed, NOW + timedelta(minutes=1))
    assert second.blobs_written == 1
    pointer = json.loads(store.get(LATEST_KEY))
    assert pointer["manifest"] == second.manifest_key
    manifest = json.loads(store.get(second.manifest_key))
    assert json.loads(store.get(f"{BLOB_PREFIX}{manifest['sections']['scoring']}.json")) == changed["scoring"]
    third = writer.publish(snapshot, NOW + timedelta(hours=2))
    assert store.list(MANIFEST_PREFIX) == [third.manifest_key]
    referenced = set(json.loads(store.get(third.manifest_key))["sections"].values())
    assert {k.removeprefix(BLOB_PREFIX).removesuffix(".json") for k in store.list(BLOB_PREFIX)} == referenced


def test_missing_sources_publish_empty_sections(
    fresh_database: Callable[[], AbstractContextManager[str]], pg_role_url: Callable[[str, str], str]
) -> None:
    # A source the publisher role cannot read (not granted, or not migrated yet) is invisible in
    # information_schema: only the sections built on it degrade; every other section still publishes.
    with fresh_database() as url:
        cfg = Config(str(ROOT / "alembic.ini"))
        cfg.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
        command.upgrade(cfg, "head")
        admin = sa.create_engine(url)
        with admin.begin() as conn:
            _seed(conn)
            conn.execute(sa.text("REVOKE SELECT ON llm_calls, scored_forecasts FROM hdt_publisher_ro"))
            conn.execute(sa.text("REVOKE SELECT (trigger_price) ON algo_orders FROM hdt_publisher_ro"))
        admin.dispose()
        engine = sa.create_engine(pg_role_url(url, "hdt_publisher_ro"))
        try:
            snapshot = Publisher(
                engine,
                SnapshotWriter(MemoryStore(), retention=timedelta(hours=1)),  # type: ignore[arg-type]
            ).build(NOW)
        finally:
            engine.dispose()
    check_snapshot(snapshot)
    assert snapshot["costs"] is None
    assert snapshot["scoring"] is None
    assert snapshot["decisions"] == []
    assert snapshot["decision_cards"] == []
    (testnet,) = snapshot["accounts"]
    assert testnet["account"] == "testnet"
    # The algo stop is not readable in full any more, so only the plain orders are listed.
    assert [o["leg"] for o in testnet["orders"]] == ["entry"]
    resid = next(v for v in snapshot["agents"] if v["target_type"] == "RESID_12H")
    crowding = next(a for a in resid["agents"] if a["agent"] == "crowding")
    assert crowding["w"] == pytest.approx(0.2)
    assert crowding["llm_cost_30d"] is None
    assert crowding["log_loss_30d"] is None
    assert [g["gate"] for g in snapshot["gates"]] == ["G1"]


def test_testnet_and_live_are_published_and_cards_follow_their_positions(
    fresh_database: Callable[[], AbstractContextManager[str]], pg_role_url: Callable[[str, str], str]
) -> None:
    # One decision runs on testnet and live: the testnet trade is closed while the live position is still
    # open. Both are public at once, and the decision card is rebuilt until the event is closed everywhere.
    # The same decision's paper trade is never published.
    with fresh_database() as url:
        cfg = Config(str(ROOT / "alembic.ini"))
        cfg.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
        command.upgrade(cfg, "head")
        admin = sa.create_engine(url)
        with admin.begin() as conn:
            for account in ("live", "testnet", "paper"):
                conn.execute(
                    sa.text(
                        "INSERT INTO equity_snapshots (account, ts, equity, available, day_start_equity, "
                        "wallet_balance, unrealized_pnl, open_positions) "
                        "VALUES (:a, :ts, 10000, 10000, 10000, 10000, 0, 0)"
                    ),
                    {"a": account, "ts": NOW - timedelta(minutes=1)},
                )
            _closed_op_trade(conn, "testnet", "EVT-SPLIT")
            _closed_op_trade(conn, "paper", "EVT-SPLIT")
            _position(
                conn,
                account="live",
                event_id="EVT-SPLIT",
                symbol="OPUSDT",
                side="LONG",
                qty=40,
                entry=1.742,
                liq=1.2,
                stop=1.701,
                tp1=1.804,
                opened=NOW - timedelta(hours=40),
            )
            as_of = NOW - timedelta(hours=41)
            insert(conn, DecisionCardRow, [_card("EVT-SPLIT", "OPUSDT", as_of)])
            insert(
                conn,
                ScoredForecastRow,
                [
                    scored_row(
                        "EVT-SPLIT", "pooled", as_of=as_of, hit=True, scored_at=NOW - timedelta(hours=29)
                    )
                ],
            )
        engine = sa.create_engine(pg_role_url(url, "hdt_publisher_ro"))
        publisher = Publisher(
            engine,
            SnapshotWriter(MemoryStore(), retention=timedelta(hours=1)),  # type: ignore[arg-type]
        )
        try:
            snapshot = publisher.build(NOW)
            with admin.begin() as conn:
                conn.execute(
                    sa.text("UPDATE positions SET closed_at = :t, qty = 0 WHERE account = 'live'"),
                    {"t": NOW - timedelta(minutes=5)},
                )
            after_close = publisher.build(NOW)
        finally:
            engine.dispose()
            admin.dispose()
    check_snapshot(snapshot)
    accounts = {a["account"]: a for a in snapshot["accounts"]}
    assert [a["account"] for a in snapshot["accounts"]] == ["testnet", "live"]
    assert [t["event_id"] for t in accounts["testnet"]["closed_trades"]] == ["EVT-SPLIT"]
    (live_open,) = accounts["live"]["open_positions"]
    assert (live_open["event_id"], live_open["stop_price"]) == ("EVT-SPLIT", 1.701)
    (card,) = snapshot["decision_cards"]
    assert [(t["account"], t["closed_at"] is None) for t in card["trades"]] == [
        ("live", True),
        ("testnet", False),
    ]
    (card_after,) = after_close["decision_cards"]
    assert [(t["account"], t["closed_at"] is None) for t in card_after["trades"]] == [
        ("live", False),
        ("testnet", False),
    ]


def test_a_fresh_held_card_picks_up_the_risk_verdict_written_after_it(
    fresh_database: Callable[[], AbstractContextManager[str]], pg_role_url: Callable[[str, str], str]
) -> None:
    # A held re-evaluation is unscored, so it is "scored" at once, and its event holds no position (the
    # position carries the original event): only the settle delay keeps it from being cached before Risk.
    with fresh_database() as url:
        cfg = Config(str(ROOT / "alembic.ini"))
        cfg.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
        command.upgrade(cfg, "head")
        admin = sa.create_engine(url)
        card = _card(
            "EVT-REVIEW",
            "ARBUSDT",
            NOW - timedelta(minutes=2),
            source="HELD",
            outcome="NO_TRADE",
            side=None,
            held_side="LONG",
            intent="EXIT",
            manager_size=0.0,
            unscored=True,
        )
        with admin.begin() as conn:
            insert(conn, DecisionCardRow, [card])
        engine = sa.create_engine(pg_role_url(url, "hdt_publisher_ro"))
        publisher = Publisher(
            engine,
            SnapshotWriter(MemoryStore(), retention=timedelta(hours=1)),  # type: ignore[arg-type]
        )
        try:
            before = publisher.build(NOW)
            with admin.begin() as conn:
                conn.execute(
                    sa.text(
                        "INSERT INTO risk_verdicts (account, event_id, coin_id, symbol, decision_intent, "
                        "effective_intent, verdict, checks, config_version_ids, decided_at) VALUES "
                        "('testnet', 'EVT-REVIEW', 1, 'ARBUSDT', 'EXIT', 'EXIT', 'approved', '[]', '{}', :at)"
                    ),
                    {"at": NOW - timedelta(minutes=1)},
                )
            after = publisher.build(NOW)
        finally:
            engine.dispose()
            admin.dispose()
    assert before["decision_cards"][0]["risk"] == []
    (review,) = after["decision_cards"][0]["risk"]
    assert (review["council_action"], review["applied_action"], review["result"]) == (
        "EXIT",
        "EXIT",
        "approved",
    )
