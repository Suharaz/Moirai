"""Rows of the council, LLM and scoring tables with every NOT NULL column of migrations 0007-0010.

Reader tests (public publisher, console read models, Telegram bot) seed these with the admin engine and read
them back under the reader's own role, so the reader SQL runs against the migrated schema and its grants.
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import Any

import sqlalchemy as sa

from hdt.contracts.common import AgentName, TargetType
from hdt.contracts.forecast import AgentForecast, Claim
from hdt.db.base import Base

LABEL_SPEC = "lbl1"
TARGET = "RESID_12H"


def insert(conn: sa.Connection, model: type[Base], rows: Iterable[Mapping[str, Any]]) -> None:
    conn.execute(sa.insert(model), [dict(r) for r in rows])


def card_row(event_id: str, symbol: str, as_of: datetime, **extra: Any) -> dict[str, Any]:
    """A LONG decision card as `hdt.council.graph.build_record` writes it (override any column)."""
    row: dict[str, Any] = {
        "event_id": event_id,
        "coin_id": 1,
        "symbol": symbol,
        "as_of": as_of,
        "source": "LTX",
        "outcome": "LONG",
        "side": "LONG",
        "manager_size": 1.0,
        "p_pooled": 0.65,
        "disagreement": 0.2,
        "rounds": 2,
        "stop_reason": "no_new_claims",
        "candidate_id": None,
        "candidate_set_sha256": None,
        "packet_sha256": None,
        "target_type": TARGET,
        "label_spec_version": LABEL_SPEC,
        "config_version_ids": {"risk": 7},
        "universe_date": as_of.date(),
        "summary": "The blind round already had a 2/3 weighted majority.",
        "consensus": [{"passed": True, "text": "Quorum: 6 agents with an opinion"}],
        "manager_rule": [{"passed": None, "text": "Manager not needed"}],
        "timeline": [],
        "unscored": False,
        "shadow_only": False,
        "held_side": None,
        "intent": None,
        "round1_sha256": "a" * 64,
        "params": {},
        "agents": {},
        "hard_evidence_version": "h1",
        "card_sha256": "b" * 64,
        "created_at": as_of,
    }
    row.update(extra)
    return row


def agent_forecast(
    event_id: str, agent: str, as_of: datetime, p: float, *, round_: int = 1, **extra: Any
) -> AgentForecast:
    """A non-abstaining forecast (p_model = p_llm = p_used = p unless overridden)."""
    fields: dict[str, Any] = {
        "agent": AgentName(agent),
        "agent_version": "1",
        "event_id": event_id,
        "round": round_,
        "coin_id": 1,
        "as_of": as_of,
        "target_type": TargetType(TARGET),
        "label_spec_version": LABEL_SPEC,
        "p_model": p,
        "p_llm": p,
        "p_used": p,
        "abstain": False,
    }
    fields.update(extra)
    return AgentForecast(**fields)


def forecast_row(forecast: AgentForecast, *, stance: str, weight_norm: float | None) -> dict[str, Any]:
    body = forecast.model_dump(mode="json")
    return {
        "event_id": forecast.event_id,
        "agent": forecast.agent.value,
        "round": forecast.round,
        "forecast": body,
        "submitted": body,
        "revision": None,
        "stance": stance,
        "weight_norm": weight_norm,
        "commit_sha256": hashlib.sha256(forecast.commit_bytes()).hexdigest(),
    }


def claim_row(
    event_id: str,
    round_: int,
    shared_id: str,
    source_agent: str,
    claim: Claim,
    *,
    reject_reason: str | None = None,
) -> dict[str, Any]:
    body = claim.model_dump(mode="json")
    return {
        "event_id": event_id,
        "round": round_,
        "shared_id": shared_id,
        "source_agent": source_agent,
        "original_claim_id": claim.claim_id,
        "claim_sha256": hashlib.sha256(claim.model_dump_json().encode()).hexdigest(),
        "claim": body,
        "reject_reason": reject_reason,
        "penalty": reject_reason is not None,
        "shared": reject_reason is None,
    }


def scored_row(
    event_id: str, agent: str, *, as_of: datetime, hit: bool, scored_at: datetime, p: float = 0.6
) -> dict[str, Any]:
    """A final score: the scorer writes a row only once the label is resolved (hit is never NULL)."""
    y = int((p >= 0.5) == hit)
    return {
        "event_id": event_id,
        "agent": agent,
        "target_type": TARGET,
        "label_spec_version": LABEL_SPEC,
        "coin_id": 1,
        "as_of": as_of,
        "agent_version": None,
        "p": p,
        "p_model": None,
        "label": "up" if y else "down",
        "y": y,
        "hit": hit,
        "log_loss": -math.log(p if y else 1 - p),
        "regime": None,
        "scored_at": scored_at,
    }


def llm_call_row(
    generation_id: str,
    called_at: datetime,
    *,
    pipeline: str,
    role: str,
    event_id: str | None,
    model_slug: str,
    prompt_tokens: int,
    completion_tokens: int,
    cost_usd: float | None,
) -> dict[str, Any]:
    return {
        "generation_id": generation_id,
        "called_at": called_at,
        "pipeline": pipeline,
        "role": role,
        "event_id": event_id,
        "model_slug": model_slug,
        "model_returned": model_slug,
        "provider": "test-provider",
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "cost_usd": cost_usd,
        "prompt_hash": "d" * 64,
        "params_hash": "e" * 64,
        "latency_ms": 900,
        "status": "ok",
    }


def weight_row(
    agent: str,
    as_of: datetime,
    *,
    w: float,
    a: float,
    r: float,
    coverage: float,
    forecasts: int,
    agent_version: int | None = 1,
    w_capped: float | None = None,
) -> dict[str, Any]:
    return {
        "target_type": TARGET,
        "label_spec_version": LABEL_SPEC,
        "agent": agent,
        "as_of": as_of,
        "agent_version": agent_version,
        "w": w,
        "w_capped": w if w_capped is None else w_capped,
        "a": a,
        "r": r,
        "coverage": coverage,
        "forecasts": forecasts,
        "events": forecasts,
    }


def calibration_rows(
    agent: str, computed_at: datetime, *, z: float | None, ece: float, n: int
) -> tuple[dict[str, Any], dict[str, Any]]:
    """(one calibration bin, the matching calibration stats row)."""
    key = {
        "target_type": TARGET,
        "label_spec_version": LABEL_SPEC,
        "agent": agent,
        "computed_at": computed_at,
    }
    bin_row = key | {
        "bin_index": 0,
        "bin_lo": 0.5,
        "bin_hi": 0.6,
        "n": n,
        "mean_p": 0.55,
        "observed_rate": 0.57,
    }
    return bin_row, key | {"spiegelhalter_z": z, "ece": ece, "n": n}
