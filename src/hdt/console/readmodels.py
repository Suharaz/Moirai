"""Read models for the console monitoring pages (read-only Postgres role `hdt_console_ro`).

The console never writes to Postgres. Monitoring pages read the tables owned by other phases; when a
backing table (or a required column) does not exist yet, the read returns `Unavailable` naming the
missing source instead of raising, and the page renders an explicit empty state. Availability is checked
against `information_schema` for every `ReadModels` instance (one per script run), so a page lights up as
soon as the owning phase migrates and grants SELECT to `hdt_console_ro`.

Console read contract: `SOURCES` lists every table and column the console reads and the phase that owns
it. Tables named by the plan keep their exact names; the others are the console's documented contract for
the owning phase (`docs/system-architecture.md`, "Console read contract (phase 12)"). JSON columns that
carry a contract (`AgentForecast`, `Claim`, `CandidateSet`, `DecisionTimelineEntry`) are validated with it.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Final, Literal

import sqlalchemy as sa
from pydantic import BaseModel, ConfigDict, ValidationError
from sqlalchemy import Engine
from sqlalchemy.exc import SQLAlchemyError

from hdt.contracts.candidate import CandidateSet
from hdt.contracts.forecast import AgentForecast, Claim
from hdt.contracts.timeline import DecisionTimelineEntry, TimelineStage
from hdt.core.clock import ensure_utc, utcnow
from hdt.db.models.ingest import NOT_ON_PLAN_ERROR

# --------------------------------------------------------------------------- results


@dataclass(frozen=True)
class Source:
    """One backing table of a read model."""

    table: str
    owner: str
    columns: tuple[str, ...]
    purpose: str

    def label(self) -> str:
        return f"table {self.table} ({self.owner})"


@dataclass(frozen=True)
class Missing:
    source: Source
    missing_columns: tuple[str, ...] = ()

    def describe(self) -> str:
        if self.missing_columns:
            return f"{self.source.label()}: missing columns {', '.join(self.missing_columns)}"
        return (
            f"{self.source.label()} does not exist yet or is not granted to hdt_console_ro "
            f"({self.source.purpose})"
        )


@dataclass(frozen=True)
class Unavailable:
    """The data behind a panel cannot be read yet; `reasons` name every missing source."""

    reasons: tuple[str, ...]

    @property
    def available(self) -> bool:
        return False


@dataclass(frozen=True)
class Available[T]:
    value: T

    @property
    def available(self) -> bool:
        return True


type ReadResult[T] = Available[T] | Unavailable


class RowShapeError(ValueError):
    """A stored row does not match the console read contract (e.g. a JSON contract column)."""


# --------------------------------------------------------------------------- engine access


class ReadModelBase:
    """Availability checks shared by every read model. `engine` uses the read-only console role."""

    def __init__(self, engine: Engine | None, *, unavailable_reason: str = "") -> None:
        self._engine = engine
        self._unavailable_reason = unavailable_reason
        self._columns: dict[str, frozenset[str]] | None = None

    def _catalog(self) -> dict[str, frozenset[str]]:
        """table -> columns visible to this role in the current schema (one query per instance)."""
        if self._columns is None:
            assert self._engine is not None
            query = sa.text(
                "SELECT table_name, column_name FROM information_schema.columns "
                "WHERE table_schema = current_schema()"
            )
            catalog: dict[str, set[str]] = {}
            with self._engine.connect() as conn:
                for table, column in conn.execute(query):
                    catalog.setdefault(str(table), set()).add(str(column))
            self._columns = {k: frozenset(v) for k, v in catalog.items()}
        return self._columns

    def missing(self, sources: Sequence[Source]) -> list[Missing]:
        catalog = self._catalog()
        out: list[Missing] = []
        for source in sources:
            present = catalog.get(source.table)
            if present is None:
                out.append(Missing(source))
                continue
            absent = tuple(c for c in source.columns if c not in present)
            if absent:
                out.append(Missing(source, absent))
        return out

    def read[T](self, sources: Sequence[Source], query: Callable[[sa.Connection], T]) -> ReadResult[T]:
        """Run `query` when every source exists with its columns; otherwise name what is missing."""
        if self._engine is None:
            reason = self._unavailable_reason or "The console database connection is not configured."
            return Unavailable((reason,))
        try:
            gaps = self.missing(sources)
            if gaps:
                return Unavailable(tuple(g.describe() for g in gaps))
            with self._engine.connect() as conn:
                return Available(query(conn))
        except SQLAlchemyError as exc:
            return Unavailable((f"The console database could not be read ({type(exc).__name__}).",))
        except RowShapeError as exc:
            return Unavailable((str(exc),))


def rows(conn: sa.Connection, sql: str, params: Mapping[str, object] | None = None) -> list[sa.RowMapping]:
    return list(conn.execute(sa.text(sql), dict(params or {})).mappings())


def one(conn: sa.Connection, sql: str, params: Mapping[str, object] | None = None) -> sa.RowMapping | None:
    return conn.execute(sa.text(sql), dict(params or {})).mappings().first()


SOURCES: Final[dict[str, Source]] = {}


def register(source: Source) -> Source:
    SOURCES[source.table] = source
    return source


# --------------------------------------------------------------------------- value helpers


def _f(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, int | float | Decimal):
        result = float(value)
        return result if math.isfinite(result) else None
    raise RowShapeError(f"expected a number, got {type(value).__name__}")


def _fz(value: object) -> float:
    number = _f(value)
    return 0.0 if number is None else number


def _i(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, int | Decimal | float):
        return int(value)
    raise RowShapeError(f"expected an integer, got {type(value).__name__}")


def _iz(value: object) -> int:
    number = _i(value)
    return 0 if number is None else number


def _s(value: object) -> str | None:
    return None if value is None else str(value)


def _dt(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return ensure_utc(value)
    raise RowShapeError(f"expected a timestamp, got {type(value).__name__}")


def _date(value: object) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return ensure_utc(value).date()
    if isinstance(value, date):
        return value
    raise RowShapeError(f"expected a date, got {type(value).__name__}")


def _b(value: object) -> bool | None:
    return None if value is None else bool(value)


def _json_list(value: object, what: str) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    raise RowShapeError(f"{what} must be a JSON array")


def logit(p: float) -> float:
    p = min(max(p, 1e-9), 1 - 1e-9)
    return math.log(p / (1 - p))


# --------------------------------------------------------------------------- the console read contract

KILL_STATE = register(
    Source(
        "kill_state",
        "phase 09 execution",
        ("account", "state", "reason", "updated_at"),
        "kill and pause state per account",
    )
)
EQUITY = register(
    Source(
        "equity_snapshots",
        "phase 09 execution",
        ("account", "ts", "equity", "available", "day_start_equity"),
        "AccountState equity history per account",
    )
)
POSITIONS = register(
    Source(
        "positions",
        "phase 09 execution",
        (
            "account",
            "event_id",
            "symbol",
            "side",
            "qty",
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
        "position ledger per account, open and closed",
    )
)
ORDERS = register(
    Source(
        "orders",
        "phase 09 execution",
        (
            "account",
            "client_id",
            "event_id",
            "symbol",
            "leg",
            "side",
            "order_type",
            "tif",
            "price",
            "qty",
            "executed_qty",
            "status",
            "created_at",
            "expires_at",
        ),
        "regular orders per account",
    )
)
ALGO_ORDERS = register(
    Source(
        "algo_orders",
        "phase 09 execution",
        (
            "account",
            "client_algo_id",
            "event_id",
            "symbol",
            "leg",
            "side",
            "order_type",
            "trigger_price",
            "qty",
            "close_position",
            "link_id",
            "status",
            "created_at",
        ),
        "STOP / TP algo orders per account",
    )
)
FILLS = register(
    Source(
        "fills",
        "phase 09 execution",
        (
            "account",
            "trade_id",
            "client_id",
            "symbol",
            "leg",
            "side",
            "price",
            "qty",
            "fee_usd",
            "maker",
            "filled_at",
        ),
        "fills per account",
    )
)
HEDGE_BOOK = register(
    Source(
        "hedge_book",
        "phase 09 execution",
        ("account", "symbol", "target_qty", "actual_qty", "beta", "updated_at"),
        "BTC hedge book per account",
    )
)
RECONCILE = register(
    Source(
        "reconcile_runs",
        "phase 09 execution",
        ("account", "ran_at", "clean", "stop_invariant_ok", "detail"),
        "reconciler runs and the STOP invariant",
    )
)
RISK_VERDICTS = register(
    Source(
        "risk_verdicts",
        "phase 09 risk",
        (
            "account",
            "event_id",
            "verdict",
            "reason",
            "intent_id",
            "signature_ok",
            "entry_client_id",
            "stop_client_algo_id",
            "sizing",
            "checks",
            "decided_at",
        ),
        "risk verdict per account and decision",
    )
)
DECISION_CARDS = register(
    Source(
        "decision_cards",
        "phase 06 council",
        (
            "event_id",
            "coin_id",
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
            "config_version_ids",
            "universe_date",
            "summary",
            "consensus",
            "manager_rule",
            "timeline",
            "unscored",
            "created_at",
        ),
        "decision card per council event",
    )
)
DECISION_FORECASTS = register(
    Source(
        "decision_forecasts",
        "phase 06 council",
        ("event_id", "agent", "round", "forecast", "stance", "weight_norm", "commit_sha256"),
        "AgentForecast per agent and round with its commit hash",
    )
)
DECISION_CLAIMS = register(
    Source(
        "decision_claims",
        "phase 06 council",
        ("event_id", "round", "shared_id", "source_agent", "claim", "reject_reason"),
        "anonymized shared claims with verification",
    )
)
CANDIDATE_SETS = register(
    Source(
        "candidate_sets",
        "phase 03 quant core",
        ("candidate_set_sha256", "coin_id", "as_of", "payload"),
        "the shared level candidate set per coin and as_of",
    )
)
LLM_CALLS = register(
    Source(
        "llm_calls",
        "phase 05 LLM agents",
        (
            "generation_id",
            "called_at",
            "pipeline",
            "role",
            "event_id",
            "model_slug",
            "prompt_tokens",
            "completion_tokens",
            "cost_usd",
        ),
        "OpenRouter generation records (usage.cost) per call",
    )
)
AGENT_VERSIONS = register(
    Source(
        "agent_versions",
        "phase 12 settings",
        ("agent", "version", "model_slug", "created_at"),
        "agent versions created by model changes",
    )
)
WEIGHT_HISTORY = register(
    Source(
        "weight_history",
        "phase 08 scoring",
        (
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
        "w / a / r history per agent and target_type",
    )
)
SCORED_FORECASTS = register(
    Source(
        "scored_forecasts",
        "phase 08 scoring",
        ("event_id", "agent", "target_type", "p", "label", "hit", "log_loss", "scored_at"),
        "scored round-1 forecasts per agent, agent 'pooled' for the council",
    )
)
CALIBRATION_BINS = register(
    Source(
        "calibration_bins",
        "phase 08 scoring",
        (
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
        "reliability diagram bins",
    )
)
CALIBRATION_STATS = register(
    Source(
        "calibration_stats",
        "phase 08 scoring",
        ("target_type", "agent", "computed_at", "spiegelhalter_z", "ece", "n"),
        "calibration statistics",
    )
)
LESSONS = register(
    Source(
        "lessons",
        "phase 08 reflection",
        (
            "lesson_id",
            "agent",
            "title",
            "state",
            "when_text",
            "observation",
            "adjustment",
            "created_at",
            "shadow_since",
            "ab_completed_at",
            "ab_n",
            "ab_logloss_with",
            "ab_logloss_without",
            "ab_ci_low",
            "ab_ci_high",
            "decided_by",
            "decided_at",
            "review_note",
        ),
        "Reflection lessons with their A/B result and review state",
    )
)
GATE_PROGRESS = register(
    Source(
        "gate_progress",
        "phase 11 shadow run",
        ("gate", "check_key", "label", "value", "target", "met", "updated_at"),
        "G1 / G4 progress and checks",
    )
)
GATE_FLAGS = register(
    Source(
        "gate_flags",
        "phase 12 settings",
        ("gate", "passed", "evidence_ref", "decided_by", "decided_at"),
        "gate pass decisions",
    )
)
ALERTS = register(
    Source(
        "alerts",
        "phase 10 ops",
        ("alert_id", "raised_at", "kind", "severity", "title", "detail", "resolved_at"),
        "operational alerts",
    )
)
CMC_CREDIT_USAGE = register(
    Source(
        "cmc_credit_usage",
        "phase 02 ingestion",
        ("date", "route", "credits", "calls"),
        "CMC credits per day and route",
    )
)
CMC_KEY_INFO = register(
    Source(
        "cmc_key_info",
        "phase 02 ingestion",
        (
            "checked_at",
            "credits_used_cycle",
            "credit_limit_cycle",
            "cycle_start",
            "cycle_end",
            "credits_used_today",
            "governor_daily_budget",
        ),
        "CMC /v1/key/info snapshots and the governor budget",
    )
)
CMC_GOVERNOR_DAYS = register(
    Source(
        "cmc_governor_days",
        "phase 02 ingestion",
        ("date", "budget"),
        "the governor's daily credit budget history",
    )
)
ROUTE_HEALTH = register(
    Source(
        "route_health",
        "phase 02 ingestion",
        (
            "route_key",
            "sort_order",
            "route",
            "source",
            "cadence_label",
            "last_success_at",
            "cycles_late",
            "status",
            "last_error",
            "credits_per_day",
            "consumers",
        ),
        "per-route freshness",
    )
)
WS_HEALTH = register(
    Source(
        "ws_health",
        "phase 02 ingestion",
        ("connection", "status", "gaps_24h", "connected_since", "last_message_at"),
        "WebSocket connection health",
    )
)
LAKE_STATS = register(
    Source(
        "lake_stats",
        "phase 02 ingestion",
        (
            "as_of",
            "lake_bytes",
            "disk_used_fraction",
            "retention_days",
            "merkle_date",
            "merkle_root",
            "object_locked",
        ),
        "lake size and the daily Merkle root",
    )
)

# --------------------------------------------------------------------------- shared constants

AGENTS: Final[tuple[str, ...]] = ("crowding", "technical", "micro", "fundamental", "news", "macro")
AGENT_LABELS: Final[dict[str, str]] = {
    "crowding": "Crowding",
    "technical": "Technical",
    "micro": "Microstructure",
    "fundamental": "Fundamental",
    "news": "News",
    "macro": "Macro",
    "pooled": "Pooled",
}
ROLE_LABELS: Final[dict[str, str]] = {
    **{k: v for k, v in AGENT_LABELS.items() if k != "pooled"},
    "news_extractor": "News extractor",
    "news_judge_a": "News judge A",
    "news_judge_b": "News judge B",
    "reflection": "Reflection",
}
ROLE_ORDER: Final[tuple[str, ...]] = (*AGENTS, "news_extractor", "news_judge_a", "news_judge_b", "reflection")
PIPELINES: Final[tuple[tuple[str, str], ...]] = (
    ("council", "Council agents"),
    ("news", "News pipeline (extractor + 2 judges)"),
    ("reflection", "Reflection"),
)
WORKING_ALGO: Final[tuple[str, ...]] = ("NEW", "WORKING")
OPEN_ORDER: Final[tuple[str, ...]] = ("NEW", "PARTIALLY_FILLED")
_EQUITY_SERIES_SQL: Final[dict[str, str]] = {
    "hour": (
        "SELECT DISTINCT ON (date_trunc('hour', ts)) ts, equity FROM equity_snapshots "
        "WHERE account = :a AND (CAST(:since AS timestamptz) IS NULL OR ts >= :since) "
        "ORDER BY date_trunc('hour', ts), ts DESC"
    ),
    "day": (
        "SELECT DISTINCT ON (date_trunc('day', ts)) ts, equity FROM equity_snapshots "
        "WHERE account = :a AND (CAST(:since AS timestamptz) IS NULL OR ts >= :since) "
        "ORDER BY date_trunc('day', ts), ts DESC"
    ),
}
# Decision filters: every value is bound; an unset filter binds NULL and matches all rows. The WHERE text
# is repeated verbatim in both statements so each stays a single literal (no SQL string composition).
_DECISION_COUNT_SQL: Final[str] = (
    "SELECT count(*) FROM decision_cards d "
    "WHERE (CAST(:q AS text) IS NULL OR d.event_id ILIKE :q OR d.symbol ILIKE :q) "
    "AND (CAST(:src AS text) IS NULL OR d.source = :src) "
    "AND (CAST(:out AS text) IS NULL OR d.outcome = :out) "
    "AND (CAST(:d0 AS timestamptz) IS NULL OR (d.as_of >= :d0 AND d.as_of < :d1))"
)
_DECISION_PAGE_SQL: Final[str] = (
    "SELECT d.event_id, d.as_of, d.symbol, d.source, d.outcome, d.manager_size, d.p_pooled, "
    "d.disagreement, d.rounds, d.unscored, v.verdict, v.reason, s.hit, s.label "
    "FROM decision_cards d "
    "LEFT JOIN risk_verdicts v ON v.event_id = d.event_id AND v.account = :acct "
    "LEFT JOIN scored_forecasts s ON s.event_id = d.event_id AND s.agent = 'pooled' "
    "WHERE (CAST(:q AS text) IS NULL OR d.event_id ILIKE :q OR d.symbol ILIKE :q) "
    "AND (CAST(:src AS text) IS NULL OR d.source = :src) "
    "AND (CAST(:out AS text) IS NULL OR d.outcome = :out) "
    "AND (CAST(:d0 AS timestamptz) IS NULL OR (d.as_of >= :d0 AND d.as_of < :d1)) "
    "ORDER BY d.as_of DESC, d.event_id DESC LIMIT :l OFFSET :o"
)
NEW_VERSION_FORECASTS: Final[int] = 50

Account = Literal["paper", "testnet", "live"]
RunState = Literal["running", "paused", "killed", "unknown"]


def agent_label(agent: str) -> str:
    return AGENT_LABELS.get(agent, agent)


def role_label(role: str) -> str:
    return ROLE_LABELS.get(role, role)


# --------------------------------------------------------------------------- result types


@dataclass(frozen=True)
class RunStatus:
    state: RunState
    label: str
    detail: str


@dataclass(frozen=True)
class EquityPoint:
    ts: datetime
    equity: float
    drawdown: float


@dataclass(frozen=True)
class AccountSummary:
    ts: datetime
    equity: float
    available: float
    day_start_equity: float
    equity_30d_ago: float | None
    peak_equity: float

    @property
    def today_pnl(self) -> float:
        return self.equity - self.day_start_equity

    @property
    def today_pnl_fraction(self) -> float | None:
        return self.today_pnl / self.day_start_equity if self.day_start_equity else None

    @property
    def change_30d(self) -> float | None:
        return None if self.equity_30d_ago is None else self.equity - self.equity_30d_ago

    @property
    def change_30d_fraction(self) -> float | None:
        if self.equity_30d_ago is None or not self.equity_30d_ago:
            return None
        return (self.equity - self.equity_30d_ago) / self.equity_30d_ago

    @property
    def drawdown(self) -> float:
        """Current drawdown from the running peak, as a non-positive fraction."""
        return (self.equity - self.peak_equity) / self.peak_equity if self.peak_equity else 0.0


@dataclass(frozen=True)
class OpenPosition:
    symbol: str
    side: str
    qty: float
    entry_price: float
    mark_price: float | None
    unrealized_pnl: float
    leverage: int | None
    margin_type: str | None
    liquidation_price: float | None
    stop_price: float | None
    tp1_price: float | None
    stop_working: bool
    is_hedge_book: bool
    opened_at: datetime | None
    time_stop_at: datetime | None
    event_id: str | None


@dataclass(frozen=True)
class ClosedTrade:
    closed_at: datetime
    symbol: str
    side: str
    entry_price: float
    exit_price: float | None
    stop_price: float | None
    r_multiple: float | None
    realized_pnl: float
    fees_funding_usd: float
    exit_reason: str | None
    event_id: str | None


@dataclass(frozen=True)
class TradeStats:
    closed: int
    wins: int
    realized_pnl: float
    fees_funding: float
    avg_r: float | None

    @property
    def win_rate(self) -> float | None:
        return self.wins / self.closed if self.closed else None


@dataclass(frozen=True)
class OpenOrder:
    client_id: str
    symbol: str
    leg: str
    order_type: str
    tif: str | None
    price: float | None
    qty: float
    executed_qty: float
    status: str
    created_at: datetime | None
    expires_at: datetime | None


@dataclass(frozen=True)
class AlgoOrder:
    client_algo_id: str
    symbol: str
    leg: str
    order_type: str
    trigger_price: float | None
    close_position: bool
    link_id: str | None
    status: str


@dataclass(frozen=True)
class Fill:
    filled_at: datetime
    symbol: str
    leg: str
    side: str
    price: float
    qty: float
    fee_usd: float | None
    maker: bool | None


@dataclass(frozen=True)
class HedgeBook:
    symbol: str
    target_qty: float
    actual_qty: float
    beta: float | None
    updated_at: datetime | None


@dataclass(frozen=True)
class ReconcileStatus:
    ran_at: datetime
    clean: bool
    stop_invariant_ok: bool
    detail: str | None


@dataclass(frozen=True)
class DecisionRow:
    event_id: str
    as_of: datetime
    symbol: str
    source: str
    outcome: str
    manager_size: float | None
    p_pooled: float | None
    disagreement: float | None
    rounds: int | None
    risk_verdict: str | None
    risk_reason: str | None
    scoring: str
    scoring_kind: Literal["pos", "neg", "mute"]


@dataclass(frozen=True)
class DecisionPage:
    rows: tuple[DecisionRow, ...]
    total: int


@dataclass(frozen=True)
class DecisionFilters:
    query: str = ""
    source: str = ""
    outcome: str = ""
    day: date | None = None


@dataclass(frozen=True)
class CheckLine:
    passed: bool | None
    text: str


@dataclass(frozen=True)
class TimelineEntry:
    at: datetime
    stage: TimelineStage
    text: str
    tone: Literal["", "pos", "neg", "warn"] = ""


@dataclass(frozen=True)
class ForecastRow:
    agent: str
    round: int
    forecast: AgentForecast
    stance: str | None
    weight_norm: float | None
    commit_sha256: str | None


@dataclass(frozen=True)
class SharedClaim:
    shared_id: str
    round: int
    claim: Claim
    reject_reason: str | None


@dataclass(frozen=True)
class Revision:
    agent: str
    before: float
    after: float
    cites: tuple[str, ...]

    @property
    def delta_logit(self) -> float:
        return logit(self.after) - logit(self.before)


@dataclass(frozen=True)
class LevelRow:
    candidate_id: str
    side: str
    entry: float
    invalidation: float
    tp1: float
    rr: float
    vote: float | None
    chosen: bool


class _Loose(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)


class SizingBreakdown(_Loose):
    """`risk_verdicts.sizing` (JSON written by Risk): the full position sizing chain."""

    equity: float
    equity_age_s: float | None = None
    risk_pct: float
    p_side: float | None = None
    conf: float
    manager_size: float
    flags_size_mult: float
    flags_age_s: float | None = None
    mode: str
    mode_size_multiplier: float
    risk_usd: float
    entry: float
    stop: float
    stop_distance: float
    qty: float
    base_asset: str = ""
    notional: float
    leverage: int
    margin: float
    margin_cap_fraction: float
    liquidation_price: float | None = None
    liquidation_distance_ratio: float | None = None
    hedge_qty: float | None = None
    hedge_beta: float | None = None


class _CheckJson(_Loose):
    passed: bool | None
    text: str


@dataclass(frozen=True)
class RiskVerdict:
    account: str
    verdict: str
    reason: str | None
    intent_id: str | None
    signature_ok: bool | None
    entry_client_id: str | None
    stop_client_algo_id: str | None
    sizing: SizingBreakdown | None
    checks: tuple[CheckLine, ...]
    decided_at: datetime | None


@dataclass(frozen=True)
class LlmUsageSummary:
    cost_usd: float
    calls: int
    agents: int


@dataclass(frozen=True)
class DecisionCard:
    event_id: str
    coin_id: int
    symbol: str
    as_of: datetime
    source: str
    outcome: str
    side: str | None
    manager_size: float | None
    p_pooled: float | None
    disagreement: float | None
    rounds: int | None
    stop_reason: str | None
    candidate_id: str | None
    target_type: str | None
    config_version_ids: dict[str, int]
    universe_date: date | None
    summary: str
    consensus: tuple[CheckLine, ...]
    manager_rule: tuple[CheckLine, ...]
    timeline: tuple[TimelineEntry, ...]
    unscored: bool
    forecasts: tuple[ForecastRow, ...]
    claims: tuple[SharedClaim, ...]
    levels: tuple[LevelRow, ...] | None
    levels_problem: str | None
    usage: LlmUsageSummary | None
    verdict: RiskVerdict | None
    fills: tuple[Fill, ...]

    @property
    def config_version(self) -> int | None:
        return max(self.config_version_ids.values()) if self.config_version_ids else None

    def round_forecasts(self, round_no: int) -> list[ForecastRow]:
        return [f for f in self.forecasts if f.round == round_no]

    def revisions(self) -> list[Revision]:
        by_agent: dict[str, list[ForecastRow]] = {}
        for row in self.forecasts:
            by_agent.setdefault(row.agent, []).append(row)
        out: list[Revision] = []
        for agent in sorted(by_agent, key=_agent_sort):
            ordered = sorted(by_agent[agent], key=lambda r: r.round)
            first, last = ordered[0], ordered[-1]
            if last.round == 1 or first.forecast.p_used is None or last.forecast.p_used is None:
                continue
            out.append(
                Revision(agent, first.forecast.p_used, last.forecast.p_used, last.forecast.cited_claim_ids)
            )
        return out


def _agent_sort(agent: str) -> int:
    return AGENTS.index(agent) if agent in AGENTS else len(AGENTS)


@dataclass(frozen=True)
class GateProgressLine:
    check_key: str
    label: str
    value: float | None
    target: float | None
    met: bool | None


@dataclass(frozen=True)
class GateView:
    gate: str
    progress: GateProgressLine | None
    checks: tuple[GateProgressLine, ...]
    passed: bool | None
    decided_at: datetime | None


@dataclass(frozen=True)
class AlertRow:
    raised_at: datetime
    kind: str
    severity: str
    title: str
    detail: str | None


@dataclass(frozen=True)
class HitRate:
    scored: int
    hits: int

    @property
    def rate(self) -> float | None:
        return self.hits / self.scored if self.scored else None


@dataclass(frozen=True)
class CostWindow:
    cost_usd: float
    calls: int


@dataclass(frozen=True)
class CostDay:
    day: date
    cost_usd: float
    calls: int


@dataclass(frozen=True)
class RoleCost:
    role: str
    models: tuple[str, ...]
    calls: int
    prompt_tokens: int
    completion_tokens: int
    cost_usd: float


@dataclass(frozen=True)
class CostOverview:
    today: CostWindow
    last_30d: CostWindow
    council_events_30d: int
    council_cost_30d: float
    by_day: tuple[CostDay, ...]
    by_pipeline: tuple[tuple[str, str, float], ...]
    by_role: tuple[RoleCost, ...]

    @property
    def cost_per_event(self) -> float | None:
        return self.council_cost_30d / self.council_events_30d if self.council_events_30d else None


@dataclass(frozen=True)
class AgentRow:
    agent: str
    version: int | None
    model_slug: str | None
    w: float | None
    w_capped: float | None
    a: float | None
    r: float | None
    coverage: float | None
    forecasts: int | None
    log_loss_30d: float | None
    rejected_claims: float | None
    llm_cost_30d: float

    @property
    def capped(self) -> bool:
        return (
            self.version is not None
            and self.version > 1
            and self.forecasts is not None
            and self.forecasts < NEW_VERSION_FORECASTS
        )


@dataclass(frozen=True)
class WeightPoint:
    agent: str
    day: date
    w: float


@dataclass(frozen=True)
class VersionChange:
    agent: str
    version: int
    at: datetime


@dataclass(frozen=True)
class CalibrationBin:
    bin_index: int
    bin_lo: float
    bin_hi: float
    n: int
    mean_p: float | None
    observed_rate: float | None


@dataclass(frozen=True)
class Calibration:
    agent: str
    computed_at: datetime | None
    bins: tuple[CalibrationBin, ...]
    spiegelhalter_z: float | None
    ece: float | None
    n: int | None


@dataclass(frozen=True)
class CreditStatus:
    checked_at: datetime
    used: int
    limit: int
    cycle_start: datetime
    cycle_end: datetime
    used_today: int | None
    daily_budget: int | None

    @property
    def fraction(self) -> float:
        return self.used / self.limit if self.limit else 0.0

    @property
    def cycle_day(self) -> int:
        return max(1, (self.checked_at - self.cycle_start).days + 1)

    @property
    def days_left(self) -> int:
        return max(0, math.ceil((self.cycle_end - self.checked_at).total_seconds() / 86400))

    @property
    def projection_fraction(self) -> float:
        """Linear projection of usage at the billing anchor from the pace so far in the cycle."""
        return project_credits(self.used, self.limit, self.cycle_start, self.cycle_end, self.checked_at)


def project_credits(used: int, limit: int, start: datetime, end: datetime, now: datetime) -> float:
    total = (end - start).total_seconds()
    elapsed = (now - start).total_seconds()
    if limit <= 0 or total <= 0:
        return 0.0
    if elapsed <= 0:
        return used / limit
    projected = used * min(total / elapsed, total / max(elapsed, 1.0))
    return projected / limit


@dataclass(frozen=True)
class CreditDay:
    day: date
    credits: int
    budget: int | None


ROUTE_NOT_ON_PLAN: Final[str] = "not on current plan"
"""Displayed status of a CMC route the key's plan refuses (1006): empty until the plan is upgraded."""
ROUTE_PROBLEM_STATUSES: Final[frozenset[str]] = frozenset({"late", "failing"})


@dataclass(frozen=True)
class RouteHealthRow:
    route_key: str
    route: str
    source: str
    cadence_label: str
    last_success_at: datetime | None
    cycles_late: int
    status: str
    """`route_health.status`, or `ROUTE_NOT_ON_PLAN` for a route disabled by a 1006 plan refusal."""
    credits_per_day: int | None
    consumers: str
    last_error: str | None


@dataclass(frozen=True)
class RouteCounts:
    ok: int
    late_or_failing: int
    """Routes `late` or `failing`; a route not on the current plan is never counted here."""
    not_on_plan: tuple[str, ...]


def route_status(status: str, last_error: str | None) -> str:
    if status == "disabled" and last_error is not None and last_error.startswith(NOT_ON_PLAN_ERROR):
        return ROUTE_NOT_ON_PLAN
    return status


def route_counts(rows: Sequence[RouteHealthRow]) -> RouteCounts:
    return RouteCounts(
        ok=sum(1 for r in rows if r.status == "ok"),
        late_or_failing=sum(1 for r in rows if r.status in ROUTE_PROBLEM_STATUSES),
        not_on_plan=tuple(r.route for r in rows if r.status == ROUTE_NOT_ON_PLAN),
    )


@dataclass(frozen=True)
class WsHealthRow:
    connection: str
    status: str
    gaps_24h: int
    connected_since: datetime | None
    last_message_at: datetime | None


@dataclass(frozen=True)
class LakeStatus:
    as_of: datetime
    lake_bytes: int
    disk_used_fraction: float | None
    retention_days: int | None
    merkle_date: date | None
    merkle_root: str | None
    object_locked: bool | None


@dataclass(frozen=True)
class Lesson:
    lesson_id: str
    agent: str
    title: str
    state: str
    when_text: str
    observation: str
    adjustment: str
    created_at: datetime
    shadow_since: datetime | None
    ab_completed_at: datetime | None
    ab_n: int | None
    ab_logloss_with: float | None
    ab_logloss_without: float | None
    ab_ci_low: float | None
    ab_ci_high: float | None
    decided_by: str | None
    decided_at: datetime | None
    review_note: str | None

    @property
    def awaiting_review(self) -> bool:
        return self.state == "shadow" and self.ab_completed_at is not None


@dataclass(frozen=True)
class LessonBoard:
    awaiting: tuple[Lesson, ...]
    in_shadow: tuple[Lesson, ...]
    active: tuple[Lesson, ...]
    retired: tuple[Lesson, ...]


# --------------------------------------------------------------------------- row mapping


def _open_position(r: sa.RowMapping) -> OpenPosition:
    return OpenPosition(
        symbol=str(r["symbol"]),
        side=str(r["side"]),
        qty=abs(_fz(r["qty"])),
        entry_price=_fz(r["entry_price"]),
        mark_price=_f(r["mark_price"]),
        unrealized_pnl=_fz(r["unrealized_pnl"]),
        leverage=_i(r["leverage"]),
        margin_type=_s(r["margin_type"]),
        liquidation_price=_f(r["liquidation_price"]),
        stop_price=_f(r["stop_price"]),
        tp1_price=_f(r["tp1_price"]),
        stop_working=bool(r["stop_working"]),
        is_hedge_book=bool(r["is_hedge_book"]),
        opened_at=_dt(r["opened_at"]),
        time_stop_at=_dt(r["time_stop_at"]),
        event_id=_s(r["event_id"]),
    )


def _closed_trade(r: sa.RowMapping) -> ClosedTrade:
    closed_at = _dt(r["closed_at"])
    assert closed_at is not None
    return ClosedTrade(
        closed_at=closed_at,
        symbol=str(r["symbol"]),
        side=str(r["side"]),
        entry_price=_fz(r["entry_price"]),
        exit_price=_f(r["exit_price"]),
        stop_price=_f(r["stop_price"]),
        r_multiple=_f(r["r_multiple"]),
        realized_pnl=_fz(r["realized_pnl"]),
        fees_funding_usd=_fz(r["fees_funding_usd"]),
        exit_reason=_s(r["exit_reason"]),
        event_id=_s(r["event_id"]),
    )


def _fill(r: sa.RowMapping) -> Fill:
    filled_at = _dt(r["filled_at"])
    assert filled_at is not None
    return Fill(
        filled_at=filled_at,
        symbol=str(r["symbol"]),
        leg=str(r["leg"]),
        side=str(r["side"]),
        price=_fz(r["price"]),
        qty=_fz(r["qty"]),
        fee_usd=_f(r["fee_usd"]),
        maker=_b(r["maker"]),
    )


def _checks(value: object, what: str) -> tuple[CheckLine, ...]:
    try:
        return tuple(
            CheckLine(c.passed, c.text)
            for c in (_CheckJson.model_validate(i) for i in _json_list(value, what))
        )
    except ValidationError as exc:
        raise RowShapeError(
            f"{what} does not match the check-list shape: {exc.error_count()} errors"
        ) from None


def _scoring(
    unscored: bool, hit: bool | None, label: object, as_of: datetime, now: datetime
) -> tuple[str, Literal["pos", "neg", "mute"]]:
    if unscored:
        return "Not scored", "mute"
    if hit is None:
        due = as_of + timedelta(hours=12)
        return ("Pending 12 h", "mute") if now < due else ("Awaiting label", "mute")
    move = f" ({label})" if label is not None else ""
    return (f"Correct{move}", "pos") if hit else (f"Wrong{move}", "neg")


# --------------------------------------------------------------------------- read models


class ReadModels(ReadModelBase):
    """Typed reads for the monitoring pages. Every method returns `Available` or `Unavailable`."""

    # ------------------------------------------------------------ shell hooks (never raise)

    def run_status(self, account: str) -> RunStatus:
        result = self.read(
            [KILL_STATE],
            lambda c: one(
                c,
                "SELECT state, reason, updated_at FROM kill_state WHERE account = :a "
                "ORDER BY updated_at DESC LIMIT 1",
                {"a": account},
            ),
        )
        if isinstance(result, Unavailable):
            return RunStatus("unknown", "Status unknown", "; ".join(result.reasons))
        row = result.value
        if row is None:
            return RunStatus("unknown", "Status unknown", f"No kill_state row for the {account} account yet.")
        state = str(row["state"])
        when = _dt(row["updated_at"])
        since = f" since {when:%Y-%m-%d %H:%M} UTC" if when else ""
        reason = f": {row['reason']}" if row["reason"] else ""
        if state == "running":
            return RunStatus("running", "Running", f"{account} account running{since}")
        if state == "paused":
            return RunStatus("paused", "Paused", f"{account} account paused{since}{reason}")
        if state == "killed":
            return RunStatus("killed", "KILLED", f"{account} account killed{since}{reason}")
        return RunStatus("unknown", "Status unknown", f"Unknown kill_state value {state!r}")

    def lessons_nav_label(self) -> str:
        result = self.read(
            [LESSONS],
            lambda c: _iz(
                c.execute(
                    sa.text(
                        "SELECT count(*) FROM lessons WHERE state = 'shadow' AND ab_completed_at IS NOT NULL"
                    )
                ).scalar_one()
            ),
        )
        if isinstance(result, Available) and result.value > 0:
            return f"Lessons ({result.value})"
        return "Lessons"

    # ------------------------------------------------------------ account and positions

    def account_summary(self, account: str) -> ReadResult[AccountSummary | None]:
        def query(c: sa.Connection) -> AccountSummary | None:
            latest = one(
                c,
                "SELECT ts, equity, available, day_start_equity FROM equity_snapshots WHERE account = :a "
                "ORDER BY ts DESC LIMIT 1",
                {"a": account},
            )
            if latest is None:
                return None
            ts = _dt(latest["ts"])
            assert ts is not None
            before = one(
                c,
                "SELECT equity FROM equity_snapshots WHERE account = :a AND ts <= :t "
                "ORDER BY ts DESC LIMIT 1",
                {"a": account, "t": ts - timedelta(days=30)},
            )
            if before is None:
                before = one(
                    c,
                    "SELECT equity FROM equity_snapshots WHERE account = :a ORDER BY ts LIMIT 1",
                    {"a": account},
                )
            peak: object = c.execute(
                sa.text("SELECT max(equity) FROM equity_snapshots WHERE account = :a"), {"a": account}
            ).scalar_one()
            return AccountSummary(
                ts=ts,
                equity=_fz(latest["equity"]),
                available=_fz(latest["available"]),
                day_start_equity=_fz(latest["day_start_equity"]),
                equity_30d_ago=_f(before["equity"]) if before is not None else None,
                peak_equity=_fz(peak),
            )

        return self.read([EQUITY], query)

    def equity_series(self, account: str, days: int | None) -> ReadResult[tuple[EquityPoint, ...]]:
        """Hourly (7 and 30 days) or daily (all) closing equity with the running drawdown."""
        sql = _EQUITY_SERIES_SQL["hour" if days is not None and days <= 30 else "day"]

        def query(c: sa.Connection) -> tuple[EquityPoint, ...]:
            since = utcnow() - timedelta(days=days) if days is not None else None
            data = rows(c, sql, {"a": account, "since": since})
            peak = -math.inf
            points: list[EquityPoint] = []
            for r in data:
                equity = _fz(r["equity"])
                peak = max(peak, equity)
                ts = _dt(r["ts"])
                assert ts is not None
                points.append(EquityPoint(ts, equity, (equity - peak) / peak if peak > 0 else 0.0))
            return tuple(points)

        return self.read([EQUITY], query)

    def open_positions(self, account: str) -> ReadResult[tuple[OpenPosition, ...]]:
        sql = (
            "SELECT p.*, EXISTS (SELECT 1 FROM algo_orders g WHERE g.account = p.account "
            "AND g.symbol = p.symbol AND g.order_type = 'STOP_MARKET' AND g.status = ANY(:working)) "
            "AS stop_working FROM positions p WHERE p.account = :a AND p.closed_at IS NULL "
            "ORDER BY p.is_hedge_book, p.opened_at"
        )
        params = {"a": account, "working": list(WORKING_ALGO)}
        return self.read(
            [POSITIONS, ALGO_ORDERS], lambda c: tuple(_open_position(r) for r in rows(c, sql, params))
        )

    def closed_trades(
        self, account: str, *, limit: int, offset: int
    ) -> ReadResult[tuple[tuple[ClosedTrade, ...], int]]:
        def query(c: sa.Connection) -> tuple[tuple[ClosedTrade, ...], int]:
            total = _iz(
                c.execute(
                    sa.text(
                        "SELECT count(*) FROM positions WHERE account = :a AND closed_at IS NOT NULL "
                        "AND NOT is_hedge_book"
                    ),
                    {"a": account},
                ).scalar_one()
            )
            data = rows(
                c,
                "SELECT * FROM positions WHERE account = :a AND closed_at IS NOT NULL AND NOT is_hedge_book "
                "ORDER BY closed_at DESC LIMIT :l OFFSET :o",
                {"a": account, "l": limit, "o": offset},
            )
            return tuple(_closed_trade(r) for r in data), total

        return self.read([POSITIONS], query)

    def trade_stats(self, account: str, days: int = 30) -> ReadResult[TradeStats]:
        def query(c: sa.Connection) -> TradeStats:
            r = one(
                c,
                "SELECT count(*) AS n, count(*) FILTER (WHERE realized_pnl > 0) AS wins, "
                "coalesce(sum(realized_pnl), 0) AS pnl, coalesce(sum(fees_funding_usd), 0) AS fees, "
                "avg(r_multiple) AS avg_r FROM positions WHERE account = :a AND closed_at >= :since "
                "AND NOT is_hedge_book",
                {"a": account, "since": utcnow() - timedelta(days=days)},
            )
            assert r is not None
            return TradeStats(_iz(r["n"]), _iz(r["wins"]), _fz(r["pnl"]), _fz(r["fees"]), _f(r["avg_r"]))

        return self.read([POSITIONS], query)

    def open_orders(self, account: str) -> ReadResult[tuple[OpenOrder, ...]]:
        def query(c: sa.Connection) -> tuple[OpenOrder, ...]:
            data = rows(
                c,
                "SELECT * FROM orders WHERE account = :a AND status = ANY(:open) ORDER BY created_at DESC",
                {"a": account, "open": list(OPEN_ORDER)},
            )
            return tuple(
                OpenOrder(
                    client_id=str(r["client_id"]),
                    symbol=str(r["symbol"]),
                    leg=str(r["leg"]),
                    order_type=str(r["order_type"]),
                    tif=_s(r["tif"]),
                    price=_f(r["price"]),
                    qty=_fz(r["qty"]),
                    executed_qty=_fz(r["executed_qty"]),
                    status=str(r["status"]),
                    created_at=_dt(r["created_at"]),
                    expires_at=_dt(r["expires_at"]),
                )
                for r in data
            )

        return self.read([ORDERS], query)

    def algo_orders(self, account: str) -> ReadResult[tuple[AlgoOrder, ...]]:
        def query(c: sa.Connection) -> tuple[AlgoOrder, ...]:
            data = rows(
                c,
                "SELECT * FROM algo_orders WHERE account = :a AND status = ANY(:working) "
                "ORDER BY symbol, leg, created_at",
                {"a": account, "working": list(WORKING_ALGO)},
            )
            return tuple(
                AlgoOrder(
                    client_algo_id=str(r["client_algo_id"]),
                    symbol=str(r["symbol"]),
                    leg=str(r["leg"]),
                    order_type=str(r["order_type"]),
                    trigger_price=_f(r["trigger_price"]),
                    close_position=bool(r["close_position"]),
                    link_id=_s(r["link_id"]),
                    status=str(r["status"]),
                )
                for r in data
            )

        return self.read([ALGO_ORDERS], query)

    def recent_fills(self, account: str, limit: int = 50) -> ReadResult[tuple[Fill, ...]]:
        return self.read(
            [FILLS],
            lambda c: tuple(
                _fill(r)
                for r in rows(
                    c,
                    "SELECT * FROM fills WHERE account = :a ORDER BY filled_at DESC LIMIT :l",
                    {"a": account, "l": limit},
                )
            ),
        )

    def hedge_book(self, account: str) -> ReadResult[HedgeBook | None]:
        def query(c: sa.Connection) -> HedgeBook | None:
            r = one(
                c,
                "SELECT * FROM hedge_book WHERE account = :a ORDER BY updated_at DESC LIMIT 1",
                {"a": account},
            )
            if r is None:
                return None
            return HedgeBook(
                str(r["symbol"]),
                _fz(r["target_qty"]),
                _fz(r["actual_qty"]),
                _f(r["beta"]),
                _dt(r["updated_at"]),
            )

        return self.read([HEDGE_BOOK], query)

    def reconcile_status(self, account: str) -> ReadResult[ReconcileStatus | None]:
        def query(c: sa.Connection) -> ReconcileStatus | None:
            r = one(
                c,
                "SELECT * FROM reconcile_runs WHERE account = :a ORDER BY ran_at DESC LIMIT 1",
                {"a": account},
            )
            if r is None:
                return None
            ran_at = _dt(r["ran_at"])
            assert ran_at is not None
            return ReconcileStatus(ran_at, bool(r["clean"]), bool(r["stop_invariant_ok"]), _s(r["detail"]))

        return self.read([RECONCILE], query)

    # ------------------------------------------------------------ decisions

    @staticmethod
    def _decision_params(filters: DecisionFilters) -> dict[str, object]:
        """Bind values for `_DECISION_WHERE`; an unset filter binds NULL and matches every row."""
        start = (
            datetime(filters.day.year, filters.day.month, filters.day.day, tzinfo=utcnow().tzinfo)
            if filters.day is not None
            else None
        )
        return {
            "q": f"%{filters.query.strip()}%" if filters.query else None,
            "src": filters.source or None,
            "out": filters.outcome or None,
            "d0": start,
            "d1": start + timedelta(days=1) if start is not None else None,
        }

    def decisions(
        self, account: str, filters: DecisionFilters, *, limit: int, offset: int
    ) -> ReadResult[DecisionPage]:
        params = self._decision_params(filters)

        def query(c: sa.Connection) -> DecisionPage:
            total = _iz(c.execute(sa.text(_DECISION_COUNT_SQL), params).scalar_one())
            data = rows(c, _DECISION_PAGE_SQL, {**params, "acct": account, "l": limit, "o": offset})
            now = utcnow()
            out: list[DecisionRow] = []
            for r in data:
                as_of = _dt(r["as_of"])
                assert as_of is not None
                text, kind = _scoring(bool(r["unscored"]), _b(r["hit"]), r["label"], as_of, now)
                out.append(
                    DecisionRow(
                        event_id=str(r["event_id"]),
                        as_of=as_of,
                        symbol=str(r["symbol"]),
                        source=str(r["source"]),
                        outcome=str(r["outcome"]),
                        manager_size=_f(r["manager_size"]),
                        p_pooled=_f(r["p_pooled"]),
                        disagreement=_f(r["disagreement"]),
                        rounds=_i(r["rounds"]),
                        risk_verdict=_s(r["verdict"]),
                        risk_reason=_s(r["reason"]),
                        scoring=text,
                        scoring_kind=kind,
                    )
                )
            return DecisionPage(tuple(out), total)

        return self.read([DECISION_CARDS, RISK_VERDICTS, SCORED_FORECASTS], query)

    def decision_card(self, event_id: str, account: str) -> ReadResult[DecisionCard | None]:
        def query(c: sa.Connection) -> DecisionCard | None:
            d = one(c, "SELECT * FROM decision_cards WHERE event_id = :e", {"e": event_id})
            if d is None:
                return None
            forecasts: list[ForecastRow] = []
            for r in rows(
                c,
                "SELECT * FROM decision_forecasts WHERE event_id = :e ORDER BY round, agent",
                {"e": event_id},
            ):
                try:
                    forecast = AgentForecast.model_validate(r["forecast"])
                except ValidationError as exc:
                    raise RowShapeError(
                        f"decision_forecasts {event_id} {r['agent']} round {r['round']} does not match "
                        f"AgentForecast ({exc.error_count()} errors)"
                    ) from None
                forecasts.append(
                    ForecastRow(
                        str(r["agent"]),
                        _iz(r["round"]),
                        forecast,
                        _s(r["stance"]),
                        _f(r["weight_norm"]),
                        _s(r["commit_sha256"]),
                    )
                )
            forecasts.sort(key=lambda f: (f.round, _agent_sort(f.agent)))
            claims: list[SharedClaim] = []
            for r in rows(
                c,
                "SELECT * FROM decision_claims WHERE event_id = :e ORDER BY round, shared_id",
                {"e": event_id},
            ):
                try:
                    claim = Claim.model_validate(r["claim"])
                except ValidationError as exc:
                    raise RowShapeError(
                        f"decision_claims {event_id} {r['shared_id']} does not match Claim "
                        f"({exc.error_count()} errors)"
                    ) from None
                claims.append(
                    SharedClaim(str(r["shared_id"]), _iz(r["round"]), claim, _s(r["reject_reason"]))
                )

            side = _s(d["side"])
            levels, levels_problem = self._levels(c, d, forecasts, side)
            usage_row = one(
                c,
                "SELECT coalesce(sum(cost_usd), 0) AS cost, count(*) AS calls, count(DISTINCT role) FILTER "
                "(WHERE pipeline = 'council') AS agents FROM llm_calls WHERE event_id = :e",
                {"e": event_id},
            )
            usage = (
                LlmUsageSummary(_fz(usage_row["cost"]), _iz(usage_row["calls"]), _iz(usage_row["agents"]))
                if usage_row is not None and _iz(usage_row["calls"]) > 0
                else None
            )
            v = one(
                c,
                "SELECT * FROM risk_verdicts WHERE event_id = :e AND account = :a",
                {"e": event_id, "a": account},
            )
            verdict = self._verdict(v) if v is not None else None
            fills = tuple(
                _fill(r)
                for r in rows(
                    c,
                    "SELECT f.* FROM fills f "
                    "JOIN orders o ON o.client_id = f.client_id AND o.account = f.account "
                    "WHERE o.event_id = :e AND f.account = :a ORDER BY f.filled_at",
                    {"e": event_id, "a": account},
                )
            )
            try:
                timeline = tuple(
                    TimelineEntry(t.at, t.stage, t.text, t.tone)
                    for t in (
                        DecisionTimelineEntry.model_validate(i) for i in _json_list(d["timeline"], "timeline")
                    )
                )
            except ValidationError as exc:
                raise RowShapeError(
                    f"decision_cards.timeline of {event_id} is malformed ({exc.error_count()} errors)"
                ) from None
            as_of = _dt(d["as_of"])
            assert as_of is not None
            raw_ids = d["config_version_ids"] or {}
            if not isinstance(raw_ids, dict):
                raise RowShapeError(f"decision_cards.config_version_ids of {event_id} must be an object")
            return DecisionCard(
                event_id=str(d["event_id"]),
                coin_id=_iz(d["coin_id"]),
                symbol=str(d["symbol"]),
                as_of=as_of,
                source=str(d["source"]),
                outcome=str(d["outcome"]),
                side=side,
                manager_size=_f(d["manager_size"]),
                p_pooled=_f(d["p_pooled"]),
                disagreement=_f(d["disagreement"]),
                rounds=_i(d["rounds"]),
                stop_reason=_s(d["stop_reason"]),
                candidate_id=_s(d["candidate_id"]),
                target_type=_s(d["target_type"]),
                config_version_ids={str(k): int(v) for k, v in raw_ids.items()},
                universe_date=_date(d["universe_date"]),
                summary=str(d["summary"] or ""),
                consensus=_checks(d["consensus"], "decision_cards.consensus"),
                manager_rule=_checks(d["manager_rule"], "decision_cards.manager_rule"),
                timeline=timeline,
                unscored=bool(d["unscored"]),
                forecasts=tuple(forecasts),
                claims=tuple(claims),
                levels=levels,
                levels_problem=levels_problem,
                usage=usage,
                verdict=verdict,
                fills=fills,
            )

        return self.read(
            [
                DECISION_CARDS,
                DECISION_FORECASTS,
                DECISION_CLAIMS,
                CANDIDATE_SETS,
                LLM_CALLS,
                RISK_VERDICTS,
                ORDERS,
                FILLS,
            ],
            query,
        )

    @staticmethod
    def _levels(
        c: sa.Connection, d: sa.RowMapping, forecasts: Sequence[ForecastRow], side: str | None
    ) -> tuple[tuple[LevelRow, ...] | None, str | None]:
        sha = d["candidate_set_sha256"]
        if sha is None:
            return None, "This event has no candidate set (HOLD / EXIT re-evaluations carry no levels)."
        r = one(c, "SELECT payload FROM candidate_sets WHERE candidate_set_sha256 = :s", {"s": sha})
        if r is None:
            return None, f"Candidate set {str(sha)[:12]} is not stored in candidate_sets."
        try:
            cset = CandidateSet.model_validate(r["payload"])
        except ValidationError as exc:
            raise RowShapeError(
                f"candidate_sets {str(sha)[:12]} does not match CandidateSet ({exc.error_count()} errors)"
            ) from None
        last_round = max((f.round for f in forecasts), default=1)
        final = [f for f in forecasts if f.round == last_round]
        chosen = d["candidate_id"]
        out: list[LevelRow] = []
        for cand in cset.candidates:
            same_side = side is None or cand.side.value == side
            vote = (
                sum(f.weight_norm or 0.0 for f in final if f.forecast.candidate_id == cand.candidate_id)
                if same_side
                else None
            )
            out.append(
                LevelRow(
                    cand.candidate_id,
                    cand.side.value,
                    float(cand.entry),
                    float(cand.invalidation),
                    float(cand.tp1),
                    cand.rr,
                    vote,
                    cand.candidate_id == chosen,
                )
            )
        return tuple(out), None

    @staticmethod
    def _verdict(v: sa.RowMapping) -> RiskVerdict:
        sizing: SizingBreakdown | None = None
        if v["sizing"] is not None:
            try:
                sizing = SizingBreakdown.model_validate(v["sizing"])
            except ValidationError as exc:
                raise RowShapeError(
                    f"risk_verdicts.sizing of {v['event_id']} is malformed ({exc.error_count()} errors)"
                ) from None
        return RiskVerdict(
            account=str(v["account"]),
            verdict=str(v["verdict"]),
            reason=_s(v["reason"]),
            intent_id=_s(v["intent_id"]),
            signature_ok=_b(v["signature_ok"]),
            entry_client_id=_s(v["entry_client_id"]),
            stop_client_algo_id=_s(v["stop_client_algo_id"]),
            sizing=sizing,
            checks=_checks(v["checks"], "risk_verdicts.checks"),
            decided_at=_dt(v["decided_at"]),
        )

    # ------------------------------------------------------------ overview

    def hit_rate(self, days: int = 30, agent: str = "pooled") -> ReadResult[HitRate]:
        def query(c: sa.Connection) -> HitRate:
            r = one(
                c,
                "SELECT count(*) AS n, count(*) FILTER (WHERE hit) AS hits FROM scored_forecasts "
                "WHERE agent = :g AND scored_at >= :since",
                {"g": agent, "since": utcnow() - timedelta(days=days)},
            )
            assert r is not None
            return HitRate(_iz(r["n"]), _iz(r["hits"]))

        return self.read([SCORED_FORECASTS], query)

    def gates(self) -> ReadResult[tuple[GateView, ...]]:
        def query(c: sa.Connection) -> tuple[GateView, ...]:
            lines: dict[str, list[GateProgressLine]] = {}
            for r in rows(c, "SELECT * FROM gate_progress ORDER BY gate, check_key"):
                lines.setdefault(str(r["gate"]), []).append(
                    GateProgressLine(
                        str(r["check_key"]), str(r["label"]), _f(r["value"]), _f(r["target"]), _b(r["met"])
                    )
                )
            flags = {
                str(r["gate"]): r
                for r in rows(
                    c,
                    "SELECT DISTINCT ON (gate) gate, passed, decided_at FROM gate_flags "
                    "ORDER BY gate, decided_at DESC",
                )
            }
            out: list[GateView] = []
            for gate in sorted(set(lines) | set(flags)):
                items = lines.get(gate, [])
                progress = next((i for i in items if i.check_key == "progress"), None)
                flag = flags.get(gate)
                out.append(
                    GateView(
                        gate=gate,
                        progress=progress,
                        checks=tuple(i for i in items if i.check_key != "progress"),
                        passed=_b(flag["passed"]) if flag is not None else None,
                        decided_at=_dt(flag["decided_at"]) if flag is not None else None,
                    )
                )
            return tuple(out)

        return self.read([GATE_PROGRESS, GATE_FLAGS], query)

    def open_alerts(self, limit: int = 20) -> ReadResult[tuple[AlertRow, ...]]:
        def query(c: sa.Connection) -> tuple[AlertRow, ...]:
            out: list[AlertRow] = []
            for r in rows(
                c,
                "SELECT * FROM alerts WHERE resolved_at IS NULL ORDER BY raised_at DESC LIMIT :l",
                {"l": limit},
            ):
                raised = _dt(r["raised_at"])
                assert raised is not None
                out.append(
                    AlertRow(raised, str(r["kind"]), str(r["severity"]), str(r["title"]), _s(r["detail"]))
                )
            return tuple(out)

        return self.read([ALERTS], query)

    # ------------------------------------------------------------ AI costs

    def llm_cost(self, days: int = 30) -> ReadResult[CostWindow]:
        def query(c: sa.Connection) -> CostWindow:
            r = one(
                c,
                "SELECT coalesce(sum(cost_usd), 0) AS cost, count(*) AS calls "
                "FROM llm_calls WHERE called_at >= :s",
                {"s": utcnow() - timedelta(days=days)},
            )
            assert r is not None
            return CostWindow(_fz(r["cost"]), _iz(r["calls"]))

        return self.read([LLM_CALLS], query)

    def cost_overview(self) -> ReadResult[CostOverview]:
        def query(c: sa.Connection) -> CostOverview:
            now = utcnow()
            today = datetime(now.year, now.month, now.day, tzinfo=now.tzinfo)
            since = today - timedelta(days=29)
            t = one(
                c,
                "SELECT coalesce(sum(cost_usd), 0) AS cost, count(*) AS calls "
                "FROM llm_calls WHERE called_at >= :s",
                {"s": today},
            )
            m = one(
                c,
                "SELECT coalesce(sum(cost_usd), 0) AS cost, count(*) AS calls, "
                "coalesce(sum(cost_usd) FILTER (WHERE pipeline = 'council'), 0) AS council_cost, "
                "count(DISTINCT event_id) FILTER (WHERE pipeline = 'council') AS events "
                "FROM llm_calls WHERE called_at >= :s",
                {"s": since},
            )
            assert t is not None
            assert m is not None
            days = {
                _date(r["day"]): r
                for r in rows(
                    c,
                    "SELECT (called_at AT TIME ZONE 'UTC')::date AS day, sum(cost_usd) AS cost, "
                    "count(*) AS calls FROM llm_calls WHERE called_at >= :s GROUP BY 1",
                    {"s": since},
                )
            }
            by_day = tuple(
                CostDay(
                    (since + timedelta(days=i)).date(),
                    _fz(days[(since + timedelta(days=i)).date()]["cost"])
                    if (since + timedelta(days=i)).date() in days
                    else 0.0,
                    _iz(days[(since + timedelta(days=i)).date()]["calls"])
                    if (since + timedelta(days=i)).date() in days
                    else 0,
                )
                for i in range(30)
            )
            pipes = {
                str(r["pipeline"]): _fz(r["cost"])
                for r in rows(
                    c,
                    "SELECT pipeline, sum(cost_usd) AS cost FROM llm_calls "
                    "WHERE called_at >= :s GROUP BY pipeline",
                    {"s": since},
                )
            }
            by_pipeline = tuple((key, label, pipes.get(key, 0.0)) for key, label in PIPELINES)
            roles: list[RoleCost] = []
            for r in rows(
                c,
                "SELECT role, array_agg(DISTINCT model_slug) AS models, count(*) AS calls, "
                "coalesce(sum(prompt_tokens), 0) AS pin, coalesce(sum(completion_tokens), 0) AS pout, "
                "coalesce(sum(cost_usd), 0) AS cost FROM llm_calls WHERE called_at >= :s GROUP BY role",
                {"s": since},
            ):
                models = tuple(sorted(str(x) for x in (r["models"] or []) if x is not None))
                roles.append(
                    RoleCost(
                        str(r["role"]), models, _iz(r["calls"]), _iz(r["pin"]), _iz(r["pout"]), _fz(r["cost"])
                    )
                )
            roles.sort(key=lambda x: ROLE_ORDER.index(x.role) if x.role in ROLE_ORDER else len(ROLE_ORDER))
            return CostOverview(
                today=CostWindow(_fz(t["cost"]), _iz(t["calls"])),
                last_30d=CostWindow(_fz(m["cost"]), _iz(m["calls"])),
                council_events_30d=_iz(m["events"]),
                council_cost_30d=_fz(m["council_cost"]),
                by_day=by_day,
                by_pipeline=by_pipeline,
                by_role=tuple(roles),
            )

        return self.read([LLM_CALLS], query)

    # ------------------------------------------------------------ agents

    def agents(self, target_type: str) -> ReadResult[tuple[AgentRow, ...]]:
        def query(c: sa.Connection) -> tuple[AgentRow, ...]:
            since = utcnow() - timedelta(days=30)
            versions = {
                str(r["agent"]): r
                for r in rows(
                    c,
                    "SELECT DISTINCT ON (agent) agent, version, model_slug FROM agent_versions "
                    "ORDER BY agent, version DESC",
                )
            }
            weights = {
                str(r["agent"]): r
                for r in rows(
                    c,
                    "SELECT DISTINCT ON (agent) * FROM weight_history WHERE target_type = :t "
                    "ORDER BY agent, as_of DESC",
                    {"t": target_type},
                )
            }
            losses = {
                str(r["agent"]): _f(r["ll"])
                for r in rows(
                    c,
                    "SELECT agent, avg(log_loss) AS ll FROM scored_forecasts WHERE target_type = :t "
                    "AND scored_at >= :s GROUP BY agent",
                    {"t": target_type, "s": since},
                )
            }
            rejected = {
                str(r["source_agent"]): (_iz(r["bad"]) / _iz(r["n"])) if _iz(r["n"]) else None
                for r in rows(
                    c,
                    "SELECT k.source_agent, count(*) AS n, "
                    "count(*) FILTER (WHERE NOT coalesce((k.claim->>'verified')::boolean, false)) AS bad "
                    "FROM decision_claims k JOIN decision_cards d ON d.event_id = k.event_id "
                    "WHERE d.as_of >= :s GROUP BY k.source_agent",
                    {"s": since},
                )
            }
            costs = {
                str(r["role"]): _fz(r["cost"])
                for r in rows(
                    c,
                    "SELECT role, sum(cost_usd) AS cost FROM llm_calls "
                    "WHERE called_at >= :s AND pipeline = 'council' GROUP BY role",
                    {"s": since},
                )
            }
            out: list[AgentRow] = []
            for agent in AGENTS:
                v, w = versions.get(agent), weights.get(agent)
                out.append(
                    AgentRow(
                        agent=agent,
                        version=_i(v["version"]) if v is not None else None,
                        model_slug=_s(v["model_slug"]) if v is not None else None,
                        w=_f(w["w"]) if w is not None else None,
                        w_capped=_f(w["w_capped"]) if w is not None else None,
                        a=_f(w["a"]) if w is not None else None,
                        r=_f(w["r"]) if w is not None else None,
                        coverage=_f(w["coverage"]) if w is not None else None,
                        forecasts=_i(w["forecasts"]) if w is not None else None,
                        log_loss_30d=losses.get(agent),
                        rejected_claims=rejected.get(agent),
                        llm_cost_30d=costs.get(agent, 0.0),
                    )
                )
            return tuple(out)

        return self.read(
            [AGENT_VERSIONS, WEIGHT_HISTORY, SCORED_FORECASTS, DECISION_CLAIMS, DECISION_CARDS, LLM_CALLS],
            query,
        )

    def weight_series(
        self, target_type: str, days: int = 60
    ) -> ReadResult[tuple[tuple[WeightPoint, ...], tuple[VersionChange, ...]]]:
        def query(c: sa.Connection) -> tuple[tuple[WeightPoint, ...], tuple[VersionChange, ...]]:
            since = utcnow() - timedelta(days=days)
            points: list[WeightPoint] = []
            for r in rows(
                c,
                "SELECT DISTINCT ON (agent, date_trunc('day', as_of)) agent, as_of, "
                "coalesce(w_capped, w) AS w "
                "FROM weight_history WHERE target_type = :t AND as_of >= :s "
                "ORDER BY agent, date_trunc('day', as_of), as_of DESC",
                {"t": target_type, "s": since},
            ):
                day = _date(r["as_of"])
                assert day is not None
                points.append(WeightPoint(str(r["agent"]), day, _fz(r["w"])))
            changes: list[VersionChange] = []
            for r in rows(
                c,
                "SELECT agent, version, created_at FROM agent_versions "
                "WHERE created_at >= :s AND version > 1 "
                "ORDER BY created_at",
                {"s": since},
            ):
                at = _dt(r["created_at"])
                assert at is not None
                changes.append(VersionChange(str(r["agent"]), _iz(r["version"]), at))
            return tuple(points), tuple(changes)

        return self.read([WEIGHT_HISTORY, AGENT_VERSIONS], query)

    def calibration(self, target_type: str, agent: str) -> ReadResult[Calibration | None]:
        def query(c: sa.Connection) -> Calibration | None:
            stats = one(
                c,
                "SELECT * FROM calibration_stats WHERE target_type = :t AND agent = :g "
                "ORDER BY computed_at DESC LIMIT 1",
                {"t": target_type, "g": agent},
            )
            latest: object = c.execute(
                sa.text(
                    "SELECT max(computed_at) FROM calibration_bins WHERE target_type = :t AND agent = :g"
                ),
                {"t": target_type, "g": agent},
            ).scalar_one()
            if stats is None and latest is None:
                return None
            bins = (
                tuple(
                    CalibrationBin(
                        _iz(r["bin_index"]),
                        _fz(r["bin_lo"]),
                        _fz(r["bin_hi"]),
                        _iz(r["n"]),
                        _f(r["mean_p"]),
                        _f(r["observed_rate"]),
                    )
                    for r in rows(
                        c,
                        "SELECT * FROM calibration_bins "
                        "WHERE target_type = :t AND agent = :g AND computed_at = :c "
                        "ORDER BY bin_index",
                        {"t": target_type, "g": agent, "c": latest},
                    )
                )
                if latest is not None
                else ()
            )
            return Calibration(
                agent=agent,
                computed_at=_dt(latest)
                if latest is not None
                else (_dt(stats["computed_at"]) if stats else None),
                bins=bins,
                spiegelhalter_z=_f(stats["spiegelhalter_z"]) if stats is not None else None,
                ece=_f(stats["ece"]) if stats is not None else None,
                n=_i(stats["n"]) if stats is not None else None,
            )

        return self.read([CALIBRATION_BINS, CALIBRATION_STATS], query)

    def calibration_agents(self, target_type: str) -> ReadResult[tuple[str, ...]]:
        def query(c: sa.Connection) -> tuple[str, ...]:
            names = {
                str(r["agent"])
                for r in rows(
                    c,
                    "SELECT DISTINCT agent FROM calibration_bins WHERE target_type = :t",
                    {"t": target_type},
                )
            }
            ordered = ["pooled", *AGENTS]
            return tuple(sorted(names, key=lambda n: ordered.index(n) if n in ordered else len(ordered)))

        return self.read([CALIBRATION_BINS], query)

    # ------------------------------------------------------------ data and credits

    def credit_status(self) -> ReadResult[CreditStatus | None]:
        def query(c: sa.Connection) -> CreditStatus | None:
            r = one(c, "SELECT * FROM cmc_key_info ORDER BY checked_at DESC LIMIT 1")
            if r is None:
                return None
            checked, start, end = _dt(r["checked_at"]), _dt(r["cycle_start"]), _dt(r["cycle_end"])
            assert checked is not None
            assert start is not None
            assert end is not None
            return CreditStatus(
                checked_at=checked,
                used=_iz(r["credits_used_cycle"]),
                limit=_iz(r["credit_limit_cycle"]),
                cycle_start=start,
                cycle_end=end,
                used_today=_i(r["credits_used_today"]),
                daily_budget=_i(r["governor_daily_budget"]),
            )

        return self.read([CMC_KEY_INFO], query)

    def credit_days(self, since: date) -> ReadResult[tuple[CreditDay, ...]]:
        def query(c: sa.Connection) -> tuple[CreditDay, ...]:
            used = {
                _date(r["date"]): _iz(r["credits"])
                for r in rows(
                    c,
                    "SELECT date, sum(credits) AS credits FROM cmc_credit_usage "
                    "WHERE date >= :s GROUP BY date",
                    {"s": since},
                )
            }
            budget = {
                _date(r["date"]): _i(r["budget"])
                for r in rows(c, "SELECT date, budget FROM cmc_governor_days WHERE date >= :s", {"s": since})
            }
            days = sorted(d for d in set(used) | set(budget) if d is not None)
            return tuple(CreditDay(d, used.get(d, 0), budget.get(d)) for d in days)

        return self.read([CMC_CREDIT_USAGE, CMC_GOVERNOR_DAYS], query)

    def route_health(self) -> ReadResult[tuple[RouteHealthRow, ...]]:
        return self.read(
            [ROUTE_HEALTH],
            lambda c: tuple(
                RouteHealthRow(
                    route_key=str(r["route_key"]),
                    route=str(r["route"]),
                    source=str(r["source"]),
                    cadence_label=str(r["cadence_label"]),
                    last_success_at=_dt(r["last_success_at"]),
                    cycles_late=_iz(r["cycles_late"]),
                    status=route_status(str(r["status"]), _s(r["last_error"])),
                    credits_per_day=_i(r["credits_per_day"]),
                    consumers=str(r["consumers"] or ""),
                    last_error=_s(r["last_error"]),
                )
                for r in rows(c, "SELECT * FROM route_health ORDER BY sort_order, route_key")
            ),
        )

    def ws_health(self) -> ReadResult[tuple[WsHealthRow, ...]]:
        return self.read(
            [WS_HEALTH],
            lambda c: tuple(
                WsHealthRow(
                    str(r["connection"]),
                    str(r["status"]),
                    _iz(r["gaps_24h"]),
                    _dt(r["connected_since"]),
                    _dt(r["last_message_at"]),
                )
                for r in rows(c, "SELECT * FROM ws_health ORDER BY connection")
            ),
        )

    def lake_status(self) -> ReadResult[LakeStatus | None]:
        def query(c: sa.Connection) -> LakeStatus | None:
            r = one(c, "SELECT * FROM lake_stats ORDER BY as_of DESC LIMIT 1")
            if r is None:
                return None
            as_of = _dt(r["as_of"])
            assert as_of is not None
            return LakeStatus(
                as_of,
                _iz(r["lake_bytes"]),
                _f(r["disk_used_fraction"]),
                _i(r["retention_days"]),
                _date(r["merkle_date"]),
                _s(r["merkle_root"]),
                _b(r["object_locked"]),
            )

        return self.read([LAKE_STATS], query)

    # ------------------------------------------------------------ lessons

    def lessons(self) -> ReadResult[LessonBoard]:
        def query(c: sa.Connection) -> LessonBoard:
            items: list[Lesson] = []
            for r in rows(c, "SELECT * FROM lessons ORDER BY created_at DESC"):
                created = _dt(r["created_at"])
                assert created is not None
                items.append(
                    Lesson(
                        lesson_id=str(r["lesson_id"]),
                        agent=str(r["agent"]),
                        title=str(r["title"]),
                        state=str(r["state"]),
                        when_text=str(r["when_text"] or ""),
                        observation=str(r["observation"] or ""),
                        adjustment=str(r["adjustment"] or ""),
                        created_at=created,
                        shadow_since=_dt(r["shadow_since"]),
                        ab_completed_at=_dt(r["ab_completed_at"]),
                        ab_n=_i(r["ab_n"]),
                        ab_logloss_with=_f(r["ab_logloss_with"]),
                        ab_logloss_without=_f(r["ab_logloss_without"]),
                        ab_ci_low=_f(r["ab_ci_low"]),
                        ab_ci_high=_f(r["ab_ci_high"]),
                        decided_by=_s(r["decided_by"]),
                        decided_at=_dt(r["decided_at"]),
                        review_note=_s(r["review_note"]),
                    )
                )
            return LessonBoard(
                awaiting=tuple(i for i in items if i.awaiting_review),
                in_shadow=tuple(i for i in items if i.state == "shadow" and not i.awaiting_review),
                active=tuple(i for i in items if i.state == "active"),
                retired=tuple(i for i in items if i.state == "retired"),
            )

        return self.read([LESSONS], query)


@dataclass(frozen=True)
class Panel[T]:
    """Convenience for pages: a titled read result."""

    title: str
    result: ReadResult[T]
    extra: tuple[str, ...] = field(default_factory=tuple)
