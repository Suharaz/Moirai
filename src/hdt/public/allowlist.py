"""Public snapshot allowlist and publication rules (Design Contract section 10, phase 10).

Owner decision 2026-09-30: the public dashboard shows everything about trading, at once, for the testnet and
live accounts: each council session with its forecasts, claims and full timeline as soon as the card is
written, open positions with their stop, take-profit and liquidation levels, and every order that was not
rejected (entries, exits, stop and take-profit orders). There is no publication delay. Paper is not public
(owner decision, same day): no paper equity, position, trade, order or Risk verdict is published.

What stays private is operational, not trading: secrets, configuration, CMC credits, alerts, reconcile
and audit state, and order / algo / client identifiers. Three independent gates enforce it; a snapshot is
written only when all of them pass:

1. Projection: the publisher copies only the fields named in `FIELDS` for each object type (explicit
   allowlist per page); nothing is copied by default.
2. Schema: `snapshot_schema.json` (JSON Schema 2020-12, `additionalProperties: false` on every object)
   rejects any field outside the allowlist. `format: date-time` is enforced (RFC 3339 with an explicit
   offset, checked without extra dependencies).
3. Rules (`check_snapshot`): a forbidden-name scan over every key and finite numbers only.

`FIELDS` and the schema are kept identical by a unit test.
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Iterator, Mapping
from datetime import datetime
from functools import cache
from importlib import resources
from typing import Any, Final

from jsonschema import Draft202012Validator, FormatChecker

SCHEMA_VERSION: Final[int] = 2
PUBLIC_ACCOUNTS: Final[tuple[str, ...]] = ("testnet", "live")
# Top-level snapshot sections, each stored as one content-addressed blob in the public store.
SECTIONS: Final[tuple[str, ...]] = (
    "accounts",
    "gates",
    "scoring",
    "decisions",
    "decision_cards",
    "agents",
    "costs",
)

# --------------------------------------------------------------------------- per-object allowlists

FIELDS: Final[dict[str, tuple[str, ...]]] = {
    "snapshot": (
        "schema_version",
        "generated_at",
        "accounts",
        "gates",
        "scoring",
        "decisions",
        "decision_cards",
        "agents",
        "costs",
    ),
    # Overview + Positions (one per published account)
    "account": (
        "account",
        "equity_as_of",
        "equity",
        "day_start_equity",
        "today_pnl",
        "today_pnl_fraction",
        "change_30d",
        "change_30d_fraction",
        "drawdown_fraction",
        "equity_hourly_30d",
        "equity_daily",
        "open_positions",
        "closed_trades",
        "orders",
        "trade_stats_30d",
    ),
    "equity_point": ("t", "equity"),
    # Open positions, levels and decision link included.
    "open_position": (
        "event_id",
        "symbol",
        "side",
        "qty",
        "entry_price",
        "mark_price",
        "unrealized_pnl",
        "stop_price",
        "tp1_price",
        "liquidation_price",
        "leverage",
        "margin_type",
        "is_hedge_book",
        "opened_at",
        "time_stop_at",
    ),
    # Closed trades: full detail.
    "closed_trade": (
        "event_id",
        "symbol",
        "side",
        "qty",
        "opened_at",
        "closed_at",
        "entry_price",
        "exit_price",
        "stop_price",
        "tp1_price",
        "liquidation_price",
        "r_multiple",
        "realized_pnl",
        "fees_funding_usd",
        "exit_reason",
    ),
    # Every order except the rejected ones (they never reached the book): entry and exit orders, and the
    # stop / take-profit orders, whose `price` is the trigger price. No order, algo or client id.
    "order": (
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
    "trade_stats": ("closed", "wins", "realized_pnl", "fees_funding_usd", "avg_r"),
    "gate": ("gate", "label", "value", "target", "met", "checks", "passed", "decided_at"),
    "gate_check": ("label", "value", "target", "met"),
    "scoring": ("scored_30d", "hits_30d"),
    # Decisions list
    "decision": (
        "event_id",
        "as_of",
        "symbol",
        "source",
        "outcome",
        "manager_size",
        "p_pooled",
        "disagreement",
        "rounds",
        "unscored",
        "hit",
        "label",
    ),
    # Decision card, with the complete timeline, what Risk decided per account and the event's orders.
    "decision_card": (
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
        "target_type",
        "summary",
        "unscored",
        "hit",
        "label",
        "consensus",
        "manager_rule",
        "timeline",
        "forecasts",
        "claims",
        "levels",
        "trades",
        "risk",
        "orders",
        "usage",
    ),
    "check": ("passed", "text"),
    "timeline_entry": ("at", "text", "tone"),
    "forecast": (
        "agent",
        "round",
        "model_slug",
        "abstain",
        "p_model",
        "p_llm",
        "p_used",
        "stance",
        "candidate_id",
        "weight_norm",
        "commit_sha256",
    ),
    "claim": ("round", "shared_id", "kind", "tier", "statement", "verified", "hard", "reject_reason"),
    "level": ("candidate_id", "side", "entry", "invalidation", "tp1", "rr", "chosen"),
    "card_trade": (
        "account",
        "symbol",
        "side",
        "opened_at",
        "closed_at",
        "entry_price",
        "exit_price",
        "stop_price",
        "tp1_price",
        "r_multiple",
        "realized_pnl",
        "fees_funding_usd",
        "exit_reason",
    ),
    # `risk_verdicts` renamed: `intent` and `verdict` stay forbidden key terms everywhere else.
    "risk_result": ("account", "council_action", "applied_action", "result", "reason", "decided_at"),
    "card_order": (
        "account",
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
    "usage": ("cost_usd", "calls", "agents"),
    # Agents
    "agents_view": ("target_type", "agents", "weight_history", "version_changes", "calibration"),
    "agent": (
        "agent",
        "version",
        "model_slug",
        "w",
        "w_capped",
        "a",
        "r",
        "coverage",
        "forecasts",
        "log_loss_30d",
        "rejected_claims_fraction",
        "llm_cost_30d",
    ),
    "weight_point": ("agent", "day", "w"),
    "version_change": ("agent", "version", "at"),
    "calibration": ("agent", "computed_at", "spiegelhalter_z", "ece", "n", "bins"),
    "calibration_bin": ("bin_lo", "bin_hi", "n", "mean_p", "observed_rate"),
    # AI costs
    "costs": ("today", "last_30d", "daily", "by_pipeline", "by_role", "by_model"),
    "cost_today": ("cost_usd", "calls"),
    "cost_30d": ("cost_usd", "calls", "council_cost_usd", "council_events"),
    "cost_day": ("day", "cost_usd", "calls"),
    "cost_pipeline": ("pipeline", "cost_usd"),
    "cost_role": ("role", "models", "calls", "prompt_tokens", "completion_tokens", "cost_usd"),
    "cost_model": ("model_slug", "calls", "prompt_tokens", "completion_tokens", "cost_usd"),
}

# `risk_verdicts.reason` codes about the decision or the market, published as is. Every other code (the
# kill switch, run mode, AccountState age, equity, sizing ceilings, malformed messages) names operational
# state that stays private and is published as `PRIVATE_RISK_REASON`: the key gates never see values.
PUBLIC_RISK_REASONS: Final[frozenset[str]] = frozenset(
    {
        "no_position",
        "integrity",
        "candidate_unknown",
        "candidate_side",
        "sign_mismatch",
        "btc_reserved",
        "symbol_unknown",
        "symbol_not_trading",
        "tick",
        "rr",
        "stop_crossed",
        "tp_crossed",
        "entry_distance",
        "stale_data",
        "missing_data",
        "veto_long",
        "veto_short",
        "veto_stale",
        "bad_levels",
        "too_small",
    }
)
PRIVATE_RISK_REASON: Final[str] = "operational_limit"


def public_risk_reason(reason: str | None) -> str | None:
    """The published form of a Risk reason code: itself when public, else the generic operational value."""
    if reason is None:
        return None
    return reason if reason in PUBLIC_RISK_REASONS else PRIVATE_RISK_REASON


# Defense in depth: names that must never appear anywhere in a snapshot, whatever the schema says. Keys are
# first folded to snake_case (`clientOrderId` -> `client_order_id`); a term must start a segment, so
# `entry_client_id` and `cmc_credits` are caught while `spiegelhalter_z` is not.
FORBIDDEN_KEY: Final[re.Pattern[str]] = re.compile(
    r"(?:^|_)(client_(algo_|order_)?id|order_id|algo|link_id|intent|signature|secret|passw|api_?key|"
    r"bot_token|credential|private|dsn|credit|config|reconcile|audit|alert|verdict|sizing|kill|halt|"
    r"account_state|notional|margin_cap|risk_usd|risk_pct)"
)
_CAMEL_BOUNDARY: Final[re.Pattern[str]] = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")


def forbidden_key(key: str) -> bool:
    """True when `key` (snake_case, camelCase or any case) names a field that is never public."""
    return FORBIDDEN_KEY.search(_CAMEL_BOUNDARY.sub("_", key).lower().replace("-", "_")) is not None


def project(row: Mapping[str, Any], kind: str) -> dict[str, Any]:
    """Copy exactly the allowlisted fields of `kind` (missing ones become None)."""
    return {name: row.get(name) for name in FIELDS[kind]}


# --------------------------------------------------------------------------- validation


class SnapshotRejectedError(ValueError):
    """The snapshot must not be published; `problems` lists every reason found."""

    def __init__(self, problems: list[str]) -> None:
        super().__init__("; ".join(problems[:10]) + (" ..." if len(problems) > 10 else ""))
        self.problems = problems


@cache
def snapshot_schema() -> dict[str, Any]:
    text = resources.files("hdt.public").joinpath("snapshot_schema.json").read_text(encoding="utf-8")
    schema: dict[str, Any] = json.loads(text)
    Draft202012Validator.check_schema(schema)
    return schema


@cache
def snapshot_validator() -> Draft202012Validator:
    """Schema validator with the strict RFC 3339 `date-time` check (publisher and dashboard reader)."""
    checker = FormatChecker()
    checker.checks("date-time")(_is_date_time)
    return Draft202012Validator(snapshot_schema(), format_checker=checker)


# RFC 3339 `date-time`: full date, `T`, full time with optional fraction, `Z` or a numeric offset.
_RFC3339: Final[re.Pattern[str]] = re.compile(
    r"^\d{4}-\d{2}-\d{2}[Tt]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[Zz]|[+-]\d{2}:\d{2})$"
)


def _is_date_time(value: object) -> bool:
    """Strict `date-time` format check (formats apply to strings only)."""
    if not isinstance(value, str):
        return True
    return _RFC3339.match(value) is not None and _parse_ts(value) is not None


def _parse_ts(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00").replace("z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def _walk_values(value: Any, path: str = "$") -> Iterator[tuple[Any, str]]:
    if isinstance(value, Mapping):
        for key, child in value.items():
            yield from _walk_values(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from _walk_values(child, f"{path}[{index}]")
    else:
        yield value, path


def _walk_keys(value: Any, path: str = "$") -> Iterator[tuple[str, str]]:
    if isinstance(value, Mapping):
        for key, child in value.items():
            yield str(key), f"{path}.{key}"
            yield from _walk_keys(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from _walk_keys(child, f"{path}[{index}]")


def _schema_problems(snapshot: Mapping[str, Any]) -> list[str]:
    errors = sorted(snapshot_validator().iter_errors(snapshot), key=lambda e: list(e.absolute_path))
    return [f"schema: {e.json_path}: {e.message}" for e in errors]


def _forbidden_key_problems(snapshot: Mapping[str, Any]) -> list[str]:
    return [f"forbidden field {key!r} at {path}" for key, path in _walk_keys(snapshot) if forbidden_key(key)]


def _non_finite_problems(snapshot: Mapping[str, Any]) -> list[str]:
    return [
        f"non-finite number at {path}"
        for value, path in _walk_values(snapshot)
        if isinstance(value, float) and not math.isfinite(value)
    ]


def check_snapshot(snapshot: Mapping[str, Any]) -> None:
    """Raise `SnapshotRejectedError` unless the snapshot may be published as is."""
    problems = _schema_problems(snapshot)
    problems += _forbidden_key_problems(snapshot)
    problems += _non_finite_problems(snapshot)
    if _parse_ts(snapshot.get("generated_at")) is None:
        problems.append("generated_at missing or not a UTC timestamp")
    if problems:
        raise SnapshotRejectedError(problems)


def schema_object_fields(schema: Mapping[str, Any]) -> dict[str, set[str]]:
    """Property names per `$defs` object (plus the root as `snapshot`), for the drift test."""
    out = {"snapshot": set(schema.get("properties", {}))}
    for name, definition in schema.get("$defs", {}).items():
        if isinstance(definition, Mapping) and "properties" in definition:
            out[name] = set(definition["properties"])
    return out
