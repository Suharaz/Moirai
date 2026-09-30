"""public-publisher: builds the allowlisted public snapshot every 60 s and writes it to `public-store`.

Entry point: `python -m hdt.public.publisher [--once]` (service `public-publisher`, Postgres role
`hdt_publisher_ro` which is read-only and granted only the tables below, S3 user with write access to the
public bucket only).

Cycle:
1. Read Postgres (one read-only transaction, same table and column names as the admin console) and
   project every row through the per-object allowlist (`allowlist.project`). Sources that do not exist yet
   (phases not deployed) or are not granted are published as empty / null sections.
2. Publish everything at once for every account (owner decision 2026-09-30, `allowlist`): each decision
   card as soon as the council writes it, open positions with their levels, closed trades and every order
   except the rejected ones. Validate the whole snapshot (`allowlist.check_snapshot`: schema + forbidden
   names + finite numbers). A snapshot that fails, or whose rows cannot be projected (malformed
   timestamps, non-finite numbers), is not published (metric `hdt_public_snapshot_rejected_total`, log,
   and the Prometheus `PublicSnapshotStale` alert fires once the last good snapshot is older than 5
   minutes).
3. Write immutable objects: each top-level section as a content-addressed blob `blobs/<sha256>.json`
   (unchanged sections are not rewritten), a manifest `manifests/<ts>-<sha256>.json` listing the section
   hashes, then overwrite the pointer `latest.json`. Manifests older than the retention and blobs no
   retained manifest references are deleted.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import logging
import os
import signal
import threading
import time
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any, Final

from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from hdt.contracts.candidate import CandidateSet
from hdt.contracts.timeline import PUBLIC_TIMELINE_MAX, TIMELINE_STAGES
from hdt.core.clock import ensure_utc, utcnow
from hdt.ops import metrics
from hdt.public.allowlist import (
    PUBLIC_ACCOUNTS,
    SCHEMA_VERSION,
    SECTIONS,
    SnapshotRejectedError,
    check_snapshot,
    project,
    public_risk_reason,
)
from hdt.public.store import ObjectNotFoundError, PublicStore, StoreConfig, StoreError
from hdt.vault.redact import redact

log = logging.getLogger(__name__)

SERVICE: Final[str] = "public-publisher"
INTERVAL_S: Final[float] = 60.0
RETENTION_ENV: Final[str] = "HDT_PUBLIC_RETENTION_HOURS"
DEFAULT_RETENTION_HOURS: Final[int] = 24
LATEST_KEY: Final[str] = "latest.json"
MANIFEST_PREFIX: Final[str] = "manifests/"
BLOB_PREFIX: Final[str] = "blobs/"
MAX_DECISIONS: Final[int] = 1000
MAX_CLOSED_TRADES: Final[int] = 2000
MAX_ORDERS: Final[int] = 2000  # per account, = `maxItems` of `account.orders` in snapshot_schema.json
MAX_CARD_ORDERS: Final[int] = 100  # per decision card, = `maxItems` of `decision_card.orders`
# A card is cached only once its decision is at least this old: Risk and execution write within seconds.
CARD_SETTLE: Final[timedelta] = timedelta(hours=1)
WEIGHT_HISTORY_DAYS: Final[int] = 60
TARGET_TYPES: Final[tuple[str, ...]] = ("RESID_12H", "RAW_12H")
AGENTS: Final[tuple[str, ...]] = ("crowding", "technical", "micro", "fundamental", "news", "macro")

# Tables and the columns the publisher reads (subset of the console read contract).
SOURCES: Final[dict[str, tuple[str, ...]]] = {
    "equity_snapshots": ("account", "ts", "equity", "day_start_equity"),
    "positions": (
        "account",
        "event_id",
        "symbol",
        "side",
        "qty",
        "max_qty",
        "entry_price",
        "mark_price",
        "unrealized_pnl",
        "leverage",
        "margin_type",
        "liquidation_price",
        "stop_price",
        "tp1_price",
        "is_hedge_book",
        "opened_at",
        "time_stop_at",
        "closed_at",
        "exit_price",
        "exit_reason",
        "realized_pnl",
        "fees_funding_usd",
        "r_multiple",
    ),
    # `client_id` only joins an order to its fills; it is never projected.
    "orders": (
        "account",
        "client_id",
        "event_id",
        "symbol",
        "leg",
        "side",
        "order_type",
        "price",
        "qty",
        "executed_qty",
        "avg_price",
        "reduce_only",
        "status",
        "created_at",
        "updated_at",
    ),
    "fills": ("account", "client_id", "price", "qty"),
    "algo_orders": (
        "account",
        "event_id",
        "symbol",
        "leg",
        "side",
        "order_type",
        "trigger_price",
        "qty",
        "reduce_only",
        "status",
        "created_at",
        "updated_at",
    ),
    "decision_cards": (
        "event_id",
        "symbol",
        "as_of",
        "source",
        "outcome",
        "side",
        "manager_size",
        "p_pooled",
        "disagreement",
        "rounds",
        "stop_reason",
        "candidate_id",
        "candidate_set_sha256",
        "target_type",
        "summary",
        "consensus",
        "manager_rule",
        "timeline",
        "unscored",
    ),
    "decision_forecasts": (
        "event_id",
        "agent",
        "round",
        "forecast",
        "stance",
        "weight_norm",
        "commit_sha256",
    ),
    "decision_claims": ("event_id", "round", "shared_id", "source_agent", "claim", "reject_reason"),
    "candidate_sets": ("candidate_set_sha256", "payload"),
    "scored_forecasts": ("event_id", "agent", "target_type", "hit", "label", "log_loss", "scored_at"),
    "llm_calls": (
        "called_at",
        "pipeline",
        "role",
        "event_id",
        "model_slug",
        "prompt_tokens",
        "completion_tokens",
        "cost_usd",
    ),
    "weight_history": (
        "target_type",
        "agent",
        "agent_version",
        "as_of",
        "w",
        "w_capped",
        "a",
        "r",
        "coverage",
        "forecasts",
    ),
    "agent_versions": ("agent", "version", "model_slug", "created_at"),
    "calibration_bins": (
        "target_type",
        "agent",
        "computed_at",
        "bin_index",
        "bin_lo",
        "bin_hi",
        "n",
        "mean_p",
        "observed_rate",
    ),
    "calibration_stats": ("target_type", "agent", "computed_at", "spiegelhalter_z", "ece", "n"),
    "gate_progress": ("gate", "check_key", "label", "value", "target", "met"),
    "gate_flags": ("gate", "passed", "decided_at"),
    "risk_verdicts": (
        "account",
        "event_id",
        "decision_intent",
        "effective_intent",
        "verdict",
        "reason",
        "decided_at",
    ),
}
# Non-rejected orders (rejected ones never reached the book), each query followed by a scope on `account`
# / `event_id`. `avg_price` falls back to the volume-weighted price of the order's fills: execution sets it
# only when it reads the order back over REST. Stop / take-profit orders carry their trigger price.
ORDERS_SELECT: Final[str] = (
    "SELECT account, event_id, symbol, leg, side, order_type, price, qty, executed_qty, avg_price, "
    "reduce_only, status, created_at, updated_at FROM orders WHERE status <> 'REJECTED' AND "
)
ORDERS_WITH_FILL_PRICE_SELECT: Final[str] = (
    "SELECT o.account, o.event_id, o.symbol, o.leg, o.side, o.order_type, o.price, o.qty, o.executed_qty, "
    "coalesce(o.avg_price, f.avg_price) AS avg_price, o.reduce_only, o.status, o.created_at, o.updated_at "
    "FROM orders o LEFT JOIN LATERAL (SELECT sum(fl.price * fl.qty) / nullif(sum(fl.qty), 0) AS avg_price "
    "FROM fills fl WHERE fl.account = o.account AND fl.client_id = o.client_id) f ON true "
    "WHERE o.status <> 'REJECTED' AND "
)
ALGO_ORDERS_SELECT: Final[str] = (
    " UNION ALL SELECT account, event_id, symbol, leg, side, order_type, trigger_price, qty, NULL, NULL, "
    "reduce_only, status, created_at, updated_at FROM algo_orders WHERE status <> 'REJECTED' AND "
)
ACCOUNT_SCOPE: Final[str] = "account = :a"
EVENTS_SCOPE: Final[str] = "event_id = ANY(:ids) AND account = ANY(:accounts)"
# Keeps each event's newest `:l` rows of an order union, oldest first.
EVENT_ORDERS_HEAD: Final[str] = (
    "SELECT * FROM (SELECT u.*, row_number() OVER (PARTITION BY event_id ORDER BY created_at DESC) "
    "AS newest FROM ("
)
EVENT_ORDERS_TAIL: Final[str] = ") u) r WHERE newest <= :l ORDER BY created_at"


# --------------------------------------------------------------------------- JSON helpers


def iso(value: datetime) -> str:
    return ensure_utc(value).astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def jsonable(value: Any) -> Any:
    """Postgres values -> JSON values (Decimal -> float, datetime -> ISO UTC `Z`, date -> ISO)."""
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return iso(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(k): jsonable(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [jsonable(v) for v in value]
    return value


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode()


def _clean_text(value: Any) -> str | None:
    return None if value is None else redact(str(value))


def _float(value: Any) -> float | None:
    return None if value is None else float(value)


# --------------------------------------------------------------------------- builder


class SnapshotBuilder:
    """Builds one snapshot inside one read-only transaction."""

    def __init__(self, session: Session, now: datetime, card_cache: dict[str, dict[str, Any]]) -> None:
        self._s = session
        self._now = now
        self._cache = card_cache
        self._available = self._available_sources()

    def _available_sources(self) -> set[str]:
        rows = self._s.execute(
            text(
                "SELECT table_name, column_name FROM information_schema.columns "
                "WHERE table_schema = current_schema() AND table_name = ANY(:t)"
            ),
            {"t": list(SOURCES)},
        )
        columns: dict[str, set[str]] = defaultdict(set)
        for table, column in rows:
            columns[str(table)].add(str(column))
        available = {t for t, needed in SOURCES.items() if set(needed) <= columns.get(t, set())}
        missing = sorted(set(SOURCES) - available)
        if missing:
            log.info("public sources not available yet", extra={"missing": missing})
        return available

    def has(self, *tables: str) -> bool:
        return all(t in self._available for t in tables)

    def _rows(self, sql: str, params: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
        return [dict(row._mapping) for row in self._s.execute(text(sql), dict(params or {}))]

    # ----------------------------------------------------------------- accounts

    def accounts(self) -> list[dict[str, Any]]:
        if not self.has("equity_snapshots"):
            return []
        present = {
            r["account"]
            for r in self._rows(
                "SELECT DISTINCT account FROM equity_snapshots WHERE account = ANY(:a)",
                {"a": list(PUBLIC_ACCOUNTS)},
            )
        }
        return [self._account(a) for a in PUBLIC_ACCOUNTS if a in present]

    def _account(self, account: str) -> dict[str, Any]:
        p = {"a": account}
        latest = self._rows(
            "SELECT ts, equity, day_start_equity FROM equity_snapshots WHERE account = :a "
            "ORDER BY ts DESC LIMIT 1",
            p,
        )[0]
        ts = ensure_utc(latest["ts"])
        equity = float(latest["equity"])
        day_start = float(latest["day_start_equity"])
        base = self._rows(
            "SELECT equity FROM equity_snapshots WHERE account = :a AND ts <= :t ORDER BY ts DESC LIMIT 1",
            {"a": account, "t": ts - timedelta(days=30)},
        ) or self._rows("SELECT equity FROM equity_snapshots WHERE account = :a ORDER BY ts LIMIT 1", p)
        base_equity = float(base[0]["equity"])
        peak = self._rows("SELECT max(equity) AS peak FROM equity_snapshots WHERE account = :a", p)[0]["peak"]
        peak_f = float(peak) if peak is not None else equity
        hourly = self._rows(
            "SELECT DISTINCT ON (date_trunc('hour', ts)) ts, equity FROM equity_snapshots "
            "WHERE account = :a AND ts >= :s ORDER BY date_trunc('hour', ts), ts DESC",
            {"a": account, "s": self._now - timedelta(days=30)},
        )
        daily = self._rows(
            "SELECT DISTINCT ON (date_trunc('day', ts)) ts, equity FROM equity_snapshots "
            "WHERE account = :a ORDER BY date_trunc('day', ts), ts DESC",
            p,
        )
        row = {
            "account": account,
            "equity_as_of": iso(ts),
            "equity": equity,
            "day_start_equity": day_start,
            "today_pnl": equity - day_start,
            "today_pnl_fraction": (equity - day_start) / day_start if day_start else None,
            "change_30d": equity - base_equity,
            "change_30d_fraction": (equity - base_equity) / base_equity if base_equity else None,
            "drawdown_fraction": min(0.0, equity / peak_f - 1.0) if peak_f > 0 else None,
            "equity_hourly_30d": [
                project({"t": iso(r["ts"]), "equity": float(r["equity"])}, "equity_point") for r in hourly
            ],
            "equity_daily": [
                project({"t": iso(r["ts"]), "equity": float(r["equity"])}, "equity_point") for r in daily
            ],
            "open_positions": [],
            "closed_trades": [],
            "orders": self._orders(account),
            "trade_stats_30d": None,
        }
        if self.has("positions"):
            row["open_positions"] = [
                self._open_position(r)
                for r in self._rows(
                    "SELECT * FROM positions WHERE account = :a AND closed_at IS NULL "
                    "ORDER BY is_hedge_book, opened_at",
                    p,
                )
            ]
            row["closed_trades"] = [
                self._closed_trade(r)
                for r in self._rows(
                    "SELECT * FROM positions WHERE account = :a AND closed_at IS NOT NULL "
                    "AND NOT is_hedge_book ORDER BY closed_at DESC LIMIT :l",
                    p | {"l": MAX_CLOSED_TRADES},
                )
            ]
            stats = self._rows(
                "SELECT count(*) AS closed, count(*) FILTER (WHERE realized_pnl > 0) AS wins, "
                "coalesce(sum(realized_pnl), 0) AS realized_pnl, "
                "coalesce(sum(fees_funding_usd), 0) AS fees_funding_usd, "
                "avg(r_multiple) AS avg_r FROM positions "
                "WHERE account = :a AND closed_at >= :s AND NOT is_hedge_book",
                p | {"s": self._now - timedelta(days=30)},
            )[0]
            row["trade_stats_30d"] = project(jsonable(stats), "trade_stats")
        return project(row, "account")

    def _orders(self, account: str) -> list[dict[str, Any]]:
        """The newest orders of the account: entries and exits, plus the stop / take-profit orders."""
        union = self._order_union(ACCOUNT_SCOPE)
        if union is None:
            return []
        rows = self._rows(union + " ORDER BY created_at DESC LIMIT :l", {"a": account, "l": MAX_ORDERS})
        return [project(jsonable(r), "order") for r in rows]

    def _order_union(self, scope: str) -> str | None:
        """Orders and algo orders within `scope` (a module constant), or None without the orders table."""
        if not self.has("orders"):
            return None
        sql = (ORDERS_WITH_FILL_PRICE_SELECT if self.has("fills") else ORDERS_SELECT) + scope
        if self.has("algo_orders"):
            sql += ALGO_ORDERS_SELECT + scope
        return sql

    def _event_orders(self, ids: list[str]) -> dict[str, list[dict[str, Any]]]:
        """Each event's own orders across the public accounts: its newest `MAX_CARD_ORDERS`, oldest first."""
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        union = self._order_union(EVENTS_SCOPE)
        if not ids or union is None:
            return grouped
        sql = EVENT_ORDERS_HEAD + union + EVENT_ORDERS_TAIL
        for row in self._rows(sql, {"ids": ids, "accounts": list(PUBLIC_ACCOUNTS), "l": MAX_CARD_ORDERS}):
            grouped[str(row["event_id"])].append(project(jsonable(row), "card_order"))
        return grouped

    @staticmethod
    def _open_position(row: Mapping[str, Any]) -> dict[str, Any]:
        out = project(jsonable(row), "open_position")
        out["qty"] = abs(float(row["qty"]))
        return out

    @staticmethod
    def _closed_trade(row: Mapping[str, Any]) -> dict[str, Any]:
        """A closed trade; `qty` is the largest size the position reached (its `qty` is zero once closed)."""
        return project(jsonable(row), "closed_trade") | {"qty": abs(float(row["max_qty"]))}

    # ----------------------------------------------------------------- gates and scoring

    def gates(self) -> list[dict[str, Any]] | None:
        if not self.has("gate_progress"):
            return None
        rows = self._rows(
            "SELECT gate, check_key, label, value, target, met FROM gate_progress ORDER BY gate, check_key"
        )
        flags: dict[str, dict[str, Any]] = {}
        if self.has("gate_flags"):
            for flag_row in self._rows(
                "SELECT DISTINCT ON (gate) gate, passed, decided_at FROM gate_flags "
                "ORDER BY gate, decided_at DESC"
            ):
                flags[str(flag_row["gate"])] = flag_row
        grouped: dict[str, dict[str, Any]] = {}
        for row in rows:
            gate = str(row["gate"])
            entry = grouped.setdefault(
                gate, {"gate": gate, "label": None, "value": None, "target": None, "met": None, "checks": []}
            )
            if row["check_key"] == "progress":
                entry.update(
                    label=row["label"],
                    value=_float(row["value"]),
                    target=_float(row["target"]),
                    met=row["met"],
                )
            else:
                entry["checks"].append(project(jsonable(row), "gate_check"))
        out = []
        for gate, entry in grouped.items():
            flag: Mapping[str, Any] | None = flags.get(gate)
            entry["passed"] = flag["passed"] if flag else None
            entry["decided_at"] = iso(flag["decided_at"]) if flag and flag["decided_at"] else None
            out.append(project(entry, "gate"))
        return out

    def scoring(self) -> dict[str, Any] | None:
        if not self.has("scored_forecasts"):
            return None
        row = self._rows(
            "SELECT count(*) AS n, count(*) FILTER (WHERE hit) AS hits FROM scored_forecasts "
            "WHERE agent = 'pooled' AND scored_at >= :s AND hit IS NOT NULL",
            {"s": self._now - timedelta(days=30)},
        )[0]
        return project({"scored_30d": int(row["n"]), "hits_30d": int(row["hits"])}, "scoring")

    # ----------------------------------------------------------------- decisions

    def decisions(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Every decision card, newest first, as soon as the council writes it.

        A card only changes through its score and its positions, so it is cached once final (scored, or
        an unscored held re-evaluation, and no position of its event still open) and rebuilt every cycle
        until then.
        """
        if not self.has("decision_cards", "scored_forecasts"):
            return [], []
        rows = self._rows(
            "SELECT d.*, s.hit, s.label FROM decision_cards d "
            "LEFT JOIN LATERAL (SELECT hit, label FROM scored_forecasts f WHERE f.event_id = d.event_id "
            "AND f.agent = 'pooled' ORDER BY f.scored_at DESC LIMIT 1) s ON true "
            "ORDER BY d.as_of DESC, d.event_id DESC LIMIT :l",
            {"l": MAX_DECISIONS},
        )
        listed = [project(jsonable(r) | {"unscored": bool(r["unscored"])}, "decision") for r in rows]
        ids = [str(r["event_id"]) for r in rows]
        open_events = self._open_events(ids)
        missing = [r for r in rows if str(r["event_id"]) not in self._cache]
        built = self._cards(missing) if missing else {}
        for row in missing:
            if final_card(row, open_events, self._now):
                self._cache[str(row["event_id"])] = built[str(row["event_id"])]
        for stale in set(self._cache) - set(ids):
            del self._cache[stale]
        cards = [self._cache.get(event_id) or built[event_id] for event_id in ids]
        return listed, cards

    def _open_events(self, ids: list[str]) -> set[str]:
        if not ids or not self.has("positions"):
            return set()
        return {
            str(r["event_id"])
            for r in self._rows(
                "SELECT DISTINCT event_id FROM positions WHERE closed_at IS NULL AND event_id = ANY(:ids)",
                {"ids": ids},
            )
        }

    def _cards(self, rows: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
        ids = [str(r["event_id"]) for r in rows]
        forecasts = self._grouped(
            "decision_forecasts",
            "SELECT * FROM decision_forecasts WHERE event_id = ANY(:ids) ORDER BY round, agent",
            ids,
        )
        claims = self._grouped(
            "decision_claims",
            "SELECT * FROM decision_claims WHERE event_id = ANY(:ids) ORDER BY round, shared_id",
            ids,
        )
        trades = self._grouped(
            "positions",
            "SELECT * FROM positions WHERE event_id = ANY(:ids) AND account = ANY(:accounts) "
            "AND NOT is_hedge_book ORDER BY account, opened_at",
            ids,
        )
        risk = self._grouped(
            "risk_verdicts",
            "SELECT account, event_id, decision_intent, effective_intent, verdict, reason, decided_at "
            "FROM risk_verdicts WHERE event_id = ANY(:ids) AND account = ANY(:accounts) ORDER BY account",
            ids,
        )
        orders = self._event_orders(ids)
        usage: dict[str, dict[str, Any]] = {}
        if self.has("llm_calls"):
            for used in self._rows(
                "SELECT event_id, coalesce(sum(cost_usd), 0) AS cost_usd, count(*) AS calls, "
                "count(DISTINCT role) FILTER (WHERE pipeline = 'council') AS agents "
                "FROM llm_calls WHERE event_id = ANY(:ids) GROUP BY event_id",
                {"ids": ids},
            ):
                usage[str(used["event_id"])] = project(jsonable(used), "usage")
        sets: dict[str, Any] = {}
        hashes = sorted({str(r["candidate_set_sha256"]) for r in rows if r.get("candidate_set_sha256")})
        if hashes and self.has("candidate_sets"):
            for found in self._rows(
                "SELECT candidate_set_sha256, payload FROM candidate_sets "
                "WHERE candidate_set_sha256 = ANY(:h)",
                {"h": hashes},
            ):
                sets[str(found["candidate_set_sha256"])] = found["payload"]
        out: dict[str, dict[str, Any]] = {}
        for row in rows:
            event_id = str(row["event_id"])
            card_trades = [project(jsonable(t), "card_trade") for t in trades.get(event_id, [])]
            levels = self._levels(row, sets.get(str(row.get("candidate_set_sha256"))))
            card = jsonable(dict(row)) | {
                "summary": _clean_text(row.get("summary")),
                "unscored": bool(row["unscored"]),
                "consensus": [_check(c) for c in _json_list(row.get("consensus"))],
                "manager_rule": [_check(c) for c in _json_list(row.get("manager_rule"))],
                "timeline": public_timeline(_json_list(row.get("timeline"))),
                "forecasts": [_forecast(f) for f in forecasts.get(event_id, [])],
                "claims": [c for c in (_claim(k) for k in claims.get(event_id, [])) if c is not None],
                "levels": levels,
                "trades": card_trades,
                "risk": [_risk_result(r) for r in risk.get(event_id, [])],
                "orders": orders.get(event_id, []),
                "usage": usage.get(event_id),
            }
            out[event_id] = project(card, "decision_card")
        return out

    def _grouped(self, table: str, sql: str, ids: list[str]) -> dict[str, list[dict[str, Any]]]:
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        if not self.has(table):
            return grouped
        for row in self._rows(sql, {"ids": ids, "accounts": list(PUBLIC_ACCOUNTS)}):
            grouped[str(row["event_id"])].append(row)
        return grouped

    @staticmethod
    def _levels(card: Mapping[str, Any], payload: Any) -> list[dict[str, Any]]:
        if payload is None:
            return []
        try:
            candidate_set = CandidateSet.model_validate(payload)
        except ValidationError:
            log.warning("candidate set does not validate", extra={"event_id": card.get("event_id")})
            return []
        chosen = card.get("candidate_id")
        return [
            project(
                {
                    "candidate_id": c.candidate_id,
                    "side": c.side.value,
                    "entry": float(c.entry),
                    "invalidation": float(c.invalidation),
                    "tp1": float(c.tp1),
                    "rr": float(c.rr),
                    "chosen": c.candidate_id == chosen,
                },
                "level",
            )
            for c in candidate_set.candidates
        ]

    # ----------------------------------------------------------------- agents

    def agents(self) -> list[dict[str, Any]] | None:
        if not self.has("weight_history"):
            return None
        versions: dict[str, Mapping[str, Any]] = {}
        changes: list[dict[str, Any]] = []
        since_hist = self._now - timedelta(days=WEIGHT_HISTORY_DAYS)
        if self.has("agent_versions"):
            for row in self._rows(
                "SELECT DISTINCT ON (agent) agent, version, model_slug FROM agent_versions "
                "ORDER BY agent, version DESC"
            ):
                versions[str(row["agent"])] = row
            changes = [
                project(jsonable(r), "version_change")
                for r in self._rows(
                    "SELECT agent, version, created_at AS at FROM agent_versions "
                    "WHERE created_at >= :s AND version > 1 ORDER BY created_at",
                    {"s": since_hist},
                )
            ]
        since30 = self._now - timedelta(days=30)
        rejected: dict[str, float] = {}
        if self.has("decision_claims", "decision_cards"):
            for row in self._rows(
                "SELECT k.source_agent, count(*) AS n, "
                "count(*) FILTER (WHERE NOT coalesce((k.claim->>'verified')::boolean, false)) AS bad "
                "FROM decision_claims k JOIN decision_cards d ON d.event_id = k.event_id WHERE d.as_of >= :s "
                "GROUP BY k.source_agent",
                {"s": since30},
            ):
                if row["n"]:
                    rejected[str(row["source_agent"])] = int(row["bad"]) / int(row["n"])
        cost: dict[str, float] = {}
        if self.has("llm_calls"):
            for row in self._rows(
                "SELECT role, sum(cost_usd) AS cost FROM llm_calls "
                "WHERE called_at >= :s AND pipeline = 'council' "
                "GROUP BY role",
                {"s": since30},
            ):
                cost[str(row["role"])] = float(row["cost"] or 0)
        views = []
        for target in TARGET_TYPES:
            latest = {
                str(r["agent"]): r
                for r in self._rows(
                    "SELECT DISTINCT ON (agent) * FROM weight_history WHERE target_type = :t "
                    "ORDER BY agent, as_of DESC",
                    {"t": target},
                )
            }
            log_loss: dict[str, float] = {}
            if self.has("scored_forecasts"):
                for row in self._rows(
                    "SELECT agent, avg(log_loss) AS ll FROM scored_forecasts "
                    "WHERE target_type = :t AND scored_at >= :s "
                    "GROUP BY agent",
                    {"t": target, "s": since30},
                ):
                    if row["ll"] is not None:
                        log_loss[str(row["agent"])] = float(row["ll"])
            agents = []
            for agent in AGENTS:
                weight = latest.get(agent, {})
                version = versions.get(agent, {})
                agents.append(
                    project(
                        {
                            "agent": agent,
                            "version": version.get("version", weight.get("agent_version")),
                            "model_slug": version.get("model_slug"),
                            "w": _float(weight.get("w")),
                            "w_capped": _float(weight.get("w_capped")),
                            "a": _float(weight.get("a")),
                            "r": _float(weight.get("r")),
                            "coverage": _float(weight.get("coverage")),
                            "forecasts": weight.get("forecasts"),
                            "log_loss_30d": log_loss.get(agent),
                            "rejected_claims_fraction": rejected.get(agent),
                            "llm_cost_30d": cost.get(agent),
                        },
                        "agent",
                    )
                )
            history = [
                project(
                    {
                        "agent": r["agent"],
                        "day": ensure_utc(r["as_of"]).date().isoformat(),
                        "w": float(r["w"]),
                    },
                    "weight_point",
                )
                for r in self._rows(
                    "SELECT DISTINCT ON (agent, date_trunc('day', as_of)) agent, as_of, "
                    "coalesce(w_capped, w) AS w "
                    "FROM weight_history WHERE target_type = :t AND as_of >= :s "
                    "ORDER BY agent, date_trunc('day', as_of), as_of DESC",
                    {"t": target, "s": since_hist},
                )
                if r["w"] is not None
            ]
            views.append(
                project(
                    {
                        "target_type": target,
                        "agents": agents,
                        "weight_history": history,
                        "version_changes": changes,
                        "calibration": self._calibration(target),
                    },
                    "agents_view",
                )
            )
        return views

    def _calibration(self, target: str) -> list[dict[str, Any]]:
        if not self.has("calibration_bins"):
            return []
        latest = self._rows(
            "SELECT agent, max(computed_at) AS computed_at FROM calibration_bins "
            "WHERE target_type = :t GROUP BY agent "
            "ORDER BY agent",
            {"t": target},
        )
        out = []
        for row in latest:
            agent, computed_at = str(row["agent"]), row["computed_at"]
            bins = self._rows(
                "SELECT bin_lo, bin_hi, n, mean_p, observed_rate FROM calibration_bins "
                "WHERE target_type = :t AND agent = :g AND computed_at = :c ORDER BY bin_index",
                {"t": target, "g": agent, "c": computed_at},
            )
            stats: Mapping[str, Any] = {}
            if self.has("calibration_stats"):
                found = self._rows(
                    "SELECT spiegelhalter_z, ece, n FROM calibration_stats "
                    "WHERE target_type = :t AND agent = :g "
                    "ORDER BY computed_at DESC LIMIT 1",
                    {"t": target, "g": agent},
                )
                stats = found[0] if found else {}
            out.append(
                project(
                    {
                        "agent": agent,
                        "computed_at": iso(computed_at),
                        "spiegelhalter_z": _float(stats.get("spiegelhalter_z")),
                        "ece": _float(stats.get("ece")),
                        "n": stats.get("n"),
                        "bins": [project(jsonable(b), "calibration_bin") for b in bins],
                    },
                    "calibration",
                )
            )
        return out

    # ----------------------------------------------------------------- costs

    def costs(self) -> dict[str, Any] | None:
        if not self.has("llm_calls"):
            return None
        today = datetime.combine(self._now.date(), datetime.min.time(), tzinfo=UTC)
        since = today - timedelta(days=29)
        day = self._rows(
            "SELECT coalesce(sum(cost_usd), 0) AS cost_usd, count(*) AS calls FROM llm_calls "
            "WHERE called_at >= :s",
            {"s": today},
        )[0]
        month = self._rows(
            "SELECT coalesce(sum(cost_usd), 0) AS cost_usd, count(*) AS calls, "
            "coalesce(sum(cost_usd) FILTER (WHERE pipeline = 'council'), 0) AS council_cost_usd, "
            "count(DISTINCT event_id) FILTER (WHERE pipeline = 'council') AS council_events "
            "FROM llm_calls WHERE called_at >= :s",
            {"s": since},
        )[0]
        by_day = {
            (r["day"].date() if isinstance(r["day"], datetime) else r["day"]): r
            for r in self._rows(
                "SELECT date_trunc('day', called_at AT TIME ZONE 'UTC') AS day, sum(cost_usd) AS cost_usd, "
                "count(*) AS calls FROM llm_calls WHERE called_at >= :s GROUP BY 1",
                {"s": since},
            )
        }
        daily = []
        for offset in range(30):
            d = (since + timedelta(days=offset)).date()
            found = by_day.get(d)
            daily.append(
                project(
                    {
                        "day": d.isoformat(),
                        "cost_usd": float(found["cost_usd"] or 0) if found else 0.0,
                        "calls": int(found["calls"]) if found else 0,
                    },
                    "cost_day",
                )
            )
        pipelines = [
            project(jsonable(r), "cost_pipeline")
            for r in self._rows(
                "SELECT pipeline, coalesce(sum(cost_usd), 0) AS cost_usd FROM llm_calls "
                "WHERE called_at >= :s "
                "GROUP BY pipeline ORDER BY pipeline",
                {"s": since},
            )
        ]
        roles = [
            project(jsonable(r) | {"models": sorted(str(m) for m in (r["models"] or []) if m)}, "cost_role")
            for r in self._rows(
                "SELECT role, array_agg(DISTINCT model_slug) AS models, count(*) AS calls, "
                "coalesce(sum(prompt_tokens), 0) AS prompt_tokens, "
                "coalesce(sum(completion_tokens), 0) AS completion_tokens, "
                "coalesce(sum(cost_usd), 0) AS cost_usd FROM llm_calls WHERE called_at >= :s "
                "GROUP BY role ORDER BY role",
                {"s": since},
            )
        ]
        models = [
            project(jsonable(r), "cost_model")
            for r in self._rows(
                "SELECT model_slug, count(*) AS calls, coalesce(sum(prompt_tokens), 0) AS prompt_tokens, "
                "coalesce(sum(completion_tokens), 0) AS completion_tokens, "
                "coalesce(sum(cost_usd), 0) AS cost_usd "
                "FROM llm_calls WHERE called_at >= :s AND model_slug IS NOT NULL "
                "GROUP BY model_slug ORDER BY model_slug",
                {"s": since},
            )
        ]
        return project(
            {
                "today": project(jsonable(day), "cost_today"),
                "last_30d": project(jsonable(month), "cost_30d"),
                "daily": daily,
                "by_pipeline": pipelines,
                "by_role": roles,
                "by_model": models,
            },
            "costs",
        )

    # ----------------------------------------------------------------- whole snapshot

    def build(self) -> dict[str, Any]:
        decisions, cards = self.decisions()
        return project(
            {
                "schema_version": SCHEMA_VERSION,
                "generated_at": iso(self._now),
                "accounts": self.accounts(),
                "gates": self.gates(),
                "scoring": self.scoring(),
                "decisions": decisions,
                "decision_cards": cards,
                "agents": self.agents(),
                "costs": self.costs(),
            },
            "snapshot",
        )


def final_card(row: Mapping[str, Any], open_events: set[str], now: datetime) -> bool:
    """A card no later cycle can change: scored (or an unscored held re-evaluation), nothing still open, and
    older than `CARD_SETTLE`, so the Risk verdicts and orders that follow a decision are on it."""
    scored = bool(row.get("unscored")) or row.get("hit") is not None
    settled = ensure_utc(row["as_of"]) <= now - CARD_SETTLE
    return scored and settled and str(row.get("event_id")) not in open_events


def public_timeline(entries: Iterable[Any]) -> list[dict[str, Any]]:
    """The last `PUBLIC_TIMELINE_MAX` entries whose `stage` is known (`hdt.contracts.timeline`).

    A malformed `at` is skipped. Older entries beyond the cap are dropped, so one long card can never fail
    the schema (`maxItems`) and take the whole snapshot down.
    """
    out: list[dict[str, Any]] = []
    for entry in entries:
        if not isinstance(entry, Mapping) or not entry.get("text") or not entry.get("at"):
            continue
        stage = entry.get("stage")
        if not isinstance(stage, str) or stage not in TIMELINE_STAGES:
            continue
        at = entry["at"]
        try:
            at_iso = (
                iso(at)
                if isinstance(at, datetime)
                else iso(datetime.fromisoformat(str(at).replace("Z", "+00:00")))
            )
        except (ValueError, OverflowError):
            log.warning("timeline entry with a malformed timestamp skipped")
            continue
        tone = entry.get("tone") or ""
        out.append(
            project(
                {
                    "at": at_iso,
                    "text": redact(str(entry["text"])),
                    "tone": tone if tone in ("pos", "neg", "warn") else "",
                },
                "timeline_entry",
            )
        )
    return out[-PUBLIC_TIMELINE_MAX:]


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            return []
    return value if isinstance(value, list) else []


def _json_map(value: Any) -> Mapping[str, Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            return {}
    return value if isinstance(value, Mapping) else {}


def _check(value: Any) -> dict[str, Any]:
    item = _json_map(value)
    passed = item.get("passed")
    return project(
        {"passed": passed if isinstance(passed, bool) else None, "text": redact(str(item.get("text", "")))},
        "check",
    )


def _forecast(row: Mapping[str, Any]) -> dict[str, Any]:
    forecast = _json_map(row.get("forecast"))
    return project(
        {
            "agent": row["agent"],
            "round": row["round"],
            "model_slug": forecast.get("model_slug"),
            "abstain": bool(forecast.get("abstain", False)),
            "p_model": _float(forecast.get("p_model")),
            "p_llm": _float(forecast.get("p_llm")),
            "p_used": _float(forecast.get("p_used")),
            "stance": row.get("stance"),
            "candidate_id": forecast.get("candidate_id"),
            "weight_norm": _float(row.get("weight_norm")),
            "commit_sha256": row.get("commit_sha256"),
        },
        "forecast",
    )


def _risk_result(row: Mapping[str, Any]) -> dict[str, Any]:
    """What Risk made of the council decision on one account (renamed: see `risk_result` in the allowlist)."""
    return project(
        jsonable(
            {
                "account": row["account"],
                "council_action": row["decision_intent"],
                "applied_action": row["effective_intent"],
                "result": row["verdict"],
                "reason": public_risk_reason(row.get("reason")),
                "decided_at": row["decided_at"],
            }
        ),
        "risk_result",
    )


def _claim(row: Mapping[str, Any]) -> dict[str, Any] | None:
    claim = _json_map(row.get("claim"))
    statement = claim.get("statement")
    if not statement:
        return None
    return project(
        {
            "round": row["round"],
            "shared_id": str(row["shared_id"]),
            "kind": claim.get("kind"),
            "tier": claim.get("tier"),
            "statement": redact(str(statement)),
            "verified": bool(claim.get("verified", False)),
            "hard": bool(claim.get("hard", False)),
            "reject_reason": _clean_text(row.get("reject_reason")),
        },
        "claim",
    )


# --------------------------------------------------------------------------- writer


@dataclass(frozen=True)
class PublishResult:
    manifest_key: str
    generated_at: str
    bytes_written: int
    blobs_written: int


class SnapshotWriter:
    """Writes content-addressed blobs, a manifest and the `latest.json` pointer; prunes old versions."""

    def __init__(self, store: PublicStore, *, retention: timedelta) -> None:
        self._store = store
        self._retention = retention
        self._known_blobs: set[str] | None = None
        self._manifest_blobs: dict[str, set[str]] = {}

    def _load_state(self) -> None:
        self._known_blobs = {
            k.removeprefix(BLOB_PREFIX).removesuffix(".json") for k in self._store.list(BLOB_PREFIX)
        }
        for key in self._store.list(MANIFEST_PREFIX):
            try:
                manifest = json.loads(self._store.get(key))
                self._manifest_blobs[key] = set(manifest["sections"].values())
            except (StoreError, ValueError, KeyError, TypeError, AttributeError):
                log.warning("unreadable manifest in public store", extra={"key": key})
                self._manifest_blobs[key] = set()

    def publish(self, snapshot: Mapping[str, Any], now: datetime) -> PublishResult:
        if self._known_blobs is None:
            self._load_state()
        assert self._known_blobs is not None
        sections: dict[str, str] = {}
        written = 0
        blobs = 0
        for name in SECTIONS:
            body = canonical_bytes(snapshot[name])
            digest = hashlib.sha256(body).hexdigest()
            sections[name] = digest
            if digest not in self._known_blobs:
                self._store.put(
                    f"{BLOB_PREFIX}{digest}.json", body, cache_control="public, max-age=31536000, immutable"
                )
                self._known_blobs.add(digest)
                written += len(body)
                blobs += 1
        manifest = {
            "schema_version": snapshot["schema_version"],
            "generated_at": snapshot["generated_at"],
            "sections": sections,
        }
        manifest_body = canonical_bytes(manifest)
        manifest_key = (
            f"{MANIFEST_PREFIX}{now.astimezone(UTC).strftime('%Y%m%dT%H%M%SZ')}-"
            f"{hashlib.sha256(manifest_body).hexdigest()[:16]}.json"
        )
        self._store.put(manifest_key, manifest_body, cache_control="public, max-age=31536000, immutable")
        self._manifest_blobs[manifest_key] = set(sections.values())
        pointer = canonical_bytes(
            {
                "manifest": manifest_key,
                "generated_at": snapshot["generated_at"],
                "schema_version": SCHEMA_VERSION,
            }
        )
        self._store.put(LATEST_KEY, pointer, cache_control="no-cache")
        self.prune(now, keep=manifest_key)
        return PublishResult(manifest_key, str(snapshot["generated_at"]), written + len(manifest_body), blobs)

    def prune(self, now: datetime, *, keep: str) -> int:
        """Delete manifests older than the retention (never `keep`) and unreferenced blobs."""
        cutoff = (now - self._retention).astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
        expired = [
            key
            for key in self._manifest_blobs
            if key != keep and key.removeprefix(MANIFEST_PREFIX)[:16] < cutoff
        ]
        deleted = 0
        for key in expired:
            with contextlib.suppress(ObjectNotFoundError):
                self._store.delete(key)
            del self._manifest_blobs[key]
            deleted += 1
        if not expired or self._known_blobs is None:
            return deleted
        referenced = set().union(*self._manifest_blobs.values()) if self._manifest_blobs else set()
        for digest in sorted(self._known_blobs - referenced):
            with contextlib.suppress(ObjectNotFoundError):
                self._store.delete(f"{BLOB_PREFIX}{digest}.json")
            self._known_blobs.discard(digest)
            deleted += 1
        return deleted


# --------------------------------------------------------------------------- service


class Publisher:
    def __init__(self, engine: Engine, writer: SnapshotWriter) -> None:
        self._engine = engine
        self._writer = writer
        self._cards: dict[str, dict[str, Any]] = {}

    def build(self, now: datetime) -> dict[str, Any]:
        with Session(self._engine) as session, session.begin():
            session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            return SnapshotBuilder(session, now, self._cards).build()

    def run_once(self) -> PublishResult | None:
        """Build, check and publish one snapshot; None when the snapshot was rejected.

        Database and store errors propagate (the caller logs them and retries next cycle). Rows the
        builder cannot project (ValueError / TypeError: malformed timestamps, non-finite numbers,
        unexpected nulls) reject the snapshot like a failed check instead of crashing the service.
        """
        now = utcnow()
        try:
            snapshot = self.build(now)
            check_snapshot(snapshot)
        except SnapshotRejectedError as exc:
            self._reject(exc.problems)
            return None
        except (ValueError, TypeError) as exc:
            log.exception("public snapshot could not be built")
            self._reject([f"build failed: {type(exc).__name__}"])
            return None
        result = self._writer.publish(snapshot, now)
        metrics.PUBLIC_SNAPSHOT_LAST_SUCCESS.set(time.time())
        metrics.PUBLIC_SNAPSHOT_BYTES.set(len(canonical_bytes(snapshot)))
        log.info(
            "public snapshot published",
            extra={
                "manifest": result.manifest_key,
                "blobs_written": result.blobs_written,
                "bytes": result.bytes_written,
            },
        )
        return result

    def _reject(self, problems: list[str]) -> None:
        metrics.PUBLIC_SNAPSHOT_REJECTED.inc()
        self._cards.clear()
        log.error("public snapshot rejected, not published", extra={"problems": problems[:20]})


def retention_from_env() -> timedelta:
    raw = os.environ.get(RETENTION_ENV)
    hours = int(raw) if raw else DEFAULT_RETENTION_HOURS
    if hours < 1:
        raise ValueError(f"{RETENTION_ENV} must be >= 1")
    return timedelta(hours=hours)


def main(argv: list[str] | None = None) -> int:
    from hdt.core.config import static_config
    from hdt.core.logging import configure_logging
    from hdt.db.session import make_engine
    from hdt.ops.metrics import serve_metrics

    parser = argparse.ArgumentParser(
        prog="python -m hdt.public.publisher", description="public snapshot publisher"
    )
    parser.add_argument("--once", action="store_true", help="publish one snapshot and exit")
    args = parser.parse_args(argv)
    configure_logging(SERVICE, static_config().settings.logging.level)
    if not args.once:
        serve_metrics(SERVICE)
    engine = make_engine(pool_size=1)
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    with PublicStore(StoreConfig.from_env()) as store:
        publisher = Publisher(engine, SnapshotWriter(store, retention=retention_from_env()))
        while True:
            started = time.monotonic()
            try:
                result = publisher.run_once()
            except (SQLAlchemyError, StoreError):
                log.exception("public snapshot cycle failed")
                result = None
            if args.once:
                engine.dispose()
                return 0 if result is not None else 1
            if stop.wait(max(1.0, INTERVAL_S - (time.monotonic() - started))):
                break
    engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
