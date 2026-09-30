"""The public snapshot allowlist: every forbidden-field case is rejected and never reaches the store.

`fixtures/snapshot_valid.json` is real `public-publisher` output (built from a seeded database by
`tests/integration/test_public_publisher.py`, equity series trimmed). Each case below mutates one thing in a
copy and asserts `check_snapshot` refuses it and `Publisher.run_once` writes nothing.
"""

from __future__ import annotations

import copy
import json
from collections.abc import Callable
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from prometheus_client import REGISTRY

from hdt.public.allowlist import (
    FIELDS,
    SnapshotRejectedError,
    check_snapshot,
    forbidden_key,
    schema_object_fields,
    snapshot_schema,
)
from hdt.public.publisher import Publisher, SnapshotWriter
from hdt.public.store import ObjectNotFoundError

FIXTURE = Path(__file__).parent / "fixtures" / "snapshot_valid.json"

Snapshot = dict[str, Any]


def _valid() -> Snapshot:
    data: Snapshot = json.loads(FIXTURE.read_text(encoding="utf-8"))
    return data


class MemoryStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def put(
        self, key: str, body: bytes, *, content_type: str = "application/json", cache_control: str = ""
    ) -> None:
        self.objects[key] = body

    def get(self, key: str) -> bytes:
        try:
            return self.objects[key]
        except KeyError:
            raise ObjectNotFoundError(key, status=404) from None

    def delete(self, key: str) -> None:
        self.objects.pop(key)

    def list(self, prefix: str) -> list[str]:
        return sorted(k for k in self.objects if k.startswith(prefix))


class FixedPublisher(Publisher):
    """`Publisher` whose database read is replaced by a prepared snapshot; publish path unchanged."""

    def __init__(self, snapshot: Snapshot, store: MemoryStore) -> None:
        super().__init__(engine=None, writer=SnapshotWriter(store, retention=timedelta(hours=24)))  # type: ignore[arg-type]
        self._fixed = snapshot

    def build(self, now: datetime) -> Snapshot:
        return self._fixed


def _account(s: Snapshot) -> Snapshot:
    account: Snapshot = s["accounts"][0]
    return account


def _card(s: Snapshot, event_id: str) -> Snapshot:
    card: Snapshot = next(c for c in s["decision_cards"] if c["event_id"] == event_id)
    return card


def _add(path: Callable[[Snapshot], Snapshot], key: str, value: Any = "x") -> Callable[[Snapshot], None]:
    def mutate(s: Snapshot) -> None:
        path(s)[key] = value

    return mutate


def _root(s: Snapshot) -> Snapshot:
    return s


def _open_position(s: Snapshot) -> Snapshot:
    position: Snapshot = _account(s)["open_positions"][0]
    return position


def _closed_trade(s: Snapshot) -> Snapshot:
    trade: Snapshot = _account(s)["closed_trades"][0]
    return trade


def _order(s: Snapshot) -> Snapshot:
    order: Snapshot = _account(s)["orders"][0]
    return order


def _forecast(s: Snapshot) -> Snapshot:
    forecast: Snapshot = _card(s, "EVT-CLOSED")["forecasts"][0]
    return forecast


def _card_trade(s: Snapshot) -> Snapshot:
    trade: Snapshot = _card(s, "EVT-CLOSED")["trades"][0]
    return trade


def _agent(s: Snapshot) -> Snapshot:
    agent: Snapshot = s["agents"][0]["agents"][0]
    return agent


def _costs(s: Snapshot) -> Snapshot:
    costs: Snapshot = s["costs"]
    return costs


REJECTED: dict[str, Callable[[Snapshot], None]] = {
    # Anything not in the allowlist, at the root or nested.
    "extra_root_field": _add(_root, "note"),
    "extra_account_field": _add(_account, "available"),
    "extra_open_position_field": _add(_open_position, "notes"),
    "extra_forecast_field": _add(_forecast, "reason", "private rationale"),
    "extra_prompt_hash": _add(_forecast, "prompt_hash", "d" * 64),
    "extra_agent_field": _add(_agent, "model_slug_history"),
    "extra_order_field": _add(_order, "note"),
    # Order identifiers anywhere.
    "order_client_id": _add(_order, "client_id", "e-abc123"),
    "order_exchange_order_id": _add(_order, "exchange_order_id", 987654321),
    "order_algo_id": _add(_order, "algo_id", 1234),
    "client_id": _add(_closed_trade, "client_id", "e-abc123"),
    "entry_client_id": _add(_card_trade, "entry_client_id", "e-abc123"),
    "order_id": _add(_closed_trade, "order_id", 123456),
    "client_algo_id": _add(_card_trade, "client_algo_id", "s-abc123"),
    "binance_camel_case_order_id": _add(_closed_trade, "clientOrderId", "e-abc123"),
    "intent_signature": _add(_card_trade, "intent_signature", "ed25519:..."),
    # Config, secrets, credits and risk internals.
    "config_version_ids": _add(lambda s: _card(s, "EVT-CLOSED"), "config_version_ids", {"risk": 7}),
    "api_key": _add(_root, "api_key", "k"),
    "binance_camel_case_api_key": _add(_costs, "apiKey", "k"),
    "bot_token": _add(_root, "bot_token", "t"),
    "cmc_credits": _add(_costs, "cmc_credits", 3000),
    "credit_limit": _add(_root, "credit_limit_cycle", 100000),
    "sizing": _add(lambda s: _card(s, "EVT-CLOSED"), "sizing", {"risk_usd": 12.0}),
    "risk_verdict": _add(lambda s: _card(s, "EVT-CLOSED"), "verdict", "APPROVED"),
    "kill_state": _add(_account, "kill_state", "running"),
    "reconcile": _add(_account, "reconcile_clean", True),
    "daily_loss_config": _add(_account, "config_daily_loss_limit", 0.02),
    "private_key": _add(_root, "private_key", "-----BEGIN"),
    # Type and shape errors.
    "wrong_schema_version": lambda s: s.update(schema_version=1),
    "unknown_account": lambda s: _account(s).update(account="demo"),
    # Paper is never public: not as an account, nor on a card's Risk review, orders or trades.
    "paper_account": lambda s: _account(s).update(account="paper"),
    "paper_card_risk": lambda s: _card(s, "EVT-OPEN")["risk"][0].update(account="paper"),
    "paper_card_order": lambda s: _card(s, "EVT-OPEN")["orders"][0].update(account="paper"),
    "paper_card_trade": lambda s: _card_trade(s).update(account="paper"),
    "order_side_long": lambda s: _order(s).update(side="LONG"),
    "negative_open_qty_sign_leak": lambda s: _open_position(s).update(qty=-5.0),
    "generated_at_missing": lambda s: s.pop("generated_at"),
    # Strict `date-time`: not a date, no offset, date only, basic ISO format.
    "opened_at_not_a_date": lambda s: _open_position(s).update(opened_at="not a date"),
    "opened_at_without_offset": lambda s: _open_position(s).update(opened_at="2026-09-26T22:00:00"),
    "equity_as_of_date_only": lambda s: _account(s).update(equity_as_of="2026-09-26"),
    "closed_at_basic_format": lambda s: _closed_trade(s).update(closed_at="20260926T220000Z"),
    # Non-finite numbers (the canonical JSON encoder refuses them).
    "nan_equity": lambda s: _account(s).update(equity=float("nan")),
    "infinite_fee": lambda s: _closed_trade(s).update(fees_funding_usd=float("inf")),
}


@pytest.fixture
def valid() -> Snapshot:
    return _valid()


def test_fixture_is_publishable(valid: Snapshot) -> None:
    check_snapshot(valid)
    store = MemoryStore()
    assert FixedPublisher(valid, store).run_once() is not None
    assert "latest.json" in store.objects


@pytest.mark.parametrize("case", sorted(REJECTED))
def test_forbidden_case_is_rejected_and_not_published(valid: Snapshot, case: str) -> None:
    REJECTED[case](valid)
    with pytest.raises(SnapshotRejectedError) as info:
        check_snapshot(valid)
    assert info.value.problems
    store = MemoryStore()
    assert FixedPublisher(valid, store).run_once() is None
    assert store.objects == {}


def test_rejection_keeps_the_previous_snapshot_live(valid: Snapshot) -> None:
    store = MemoryStore()
    assert FixedPublisher(valid, store).run_once() is not None
    before = dict(store.objects)
    poisoned = copy.deepcopy(valid)
    _open_position(poisoned)["client_id"] = "e-abc123"
    assert FixedPublisher(poisoned, store).run_once() is None
    assert store.objects == before


class BrokenPublisher(FixedPublisher):
    def __init__(self, error: Exception, store: MemoryStore) -> None:
        super().__init__(_valid(), store)
        self._error = error

    def build(self, now: datetime) -> Snapshot:
        raise self._error


@pytest.mark.parametrize(
    "error", [ValueError("Invalid isoformat string: 'yesterday'"), TypeError("float() argument")]
)
def test_unprojectable_rows_reject_the_snapshot_instead_of_crashing(error: Exception) -> None:
    before = REGISTRY.get_sample_value("hdt_public_snapshot_rejected_total") or 0.0
    store = MemoryStore()
    assert BrokenPublisher(error, store).run_once() is None
    assert store.objects == {}
    assert REGISTRY.get_sample_value("hdt_public_snapshot_rejected_total") == before + 1


@pytest.mark.parametrize(
    ("key", "expected"),
    [
        ("client_id", True),
        ("entry_client_id", True),
        ("clientAlgoId", True),
        ("ORDER_ID", True),
        ("cmc_credits", True),
        ("apikey", True),
        ("spiegelhalter_z", False),
        ("hit", False),
        ("realized_pnl", False),
        ("fees_funding_usd", False),
    ],
)
def test_forbidden_key_matches_segments_not_substrings(key: str, expected: bool) -> None:
    assert forbidden_key(key) is expected


def test_no_allowlisted_field_is_itself_forbidden() -> None:
    assert [(kind, f) for kind, fields in FIELDS.items() for f in fields if forbidden_key(f)] == []


def test_schema_and_allowlist_do_not_drift() -> None:
    schema = snapshot_schema()
    assert schema_object_fields(schema) == {kind: set(fields) for kind, fields in FIELDS.items()}
    for name, definition in schema["$defs"].items():
        if "properties" in definition:
            assert definition.get("additionalProperties") is False, name
            assert set(definition["required"]) == set(definition["properties"]), name
    assert schema["additionalProperties"] is False
