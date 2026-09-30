"""The dashboard reads what the publisher wrote, refuses tampered content, imports no trading code."""

from __future__ import annotations

import json
import subprocess
import sys
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from hdt.public.publisher import BLOB_PREFIX, LATEST_KEY, SnapshotWriter
from hdt.public.store import ObjectNotFoundError, StoreError
from hdt.public_dashboard import data

FIXTURE = Path(__file__).parent / "fixtures" / "snapshot_valid.json"
GENERATED_AT = datetime(2026, 9, 27, 12, 0, tzinfo=UTC)


class MemoryStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.fail = False

    def put(
        self, key: str, body: bytes, *, content_type: str = "application/json", cache_control: str = ""
    ) -> None:
        self.objects[key] = body

    def get(self, key: str) -> bytes:
        if self.fail:
            raise StoreError("public store unreachable (ConnectError)")
        try:
            return self.objects[key]
        except KeyError:
            raise ObjectNotFoundError(key, status=404) from None

    def delete(self, key: str) -> None:
        self.objects.pop(key)

    def list(self, prefix: str) -> list[str]:
        return sorted(k for k in self.objects if k.startswith(prefix))


@pytest.fixture
def store(monkeypatch: pytest.MonkeyPatch) -> Iterator[MemoryStore]:
    memory = MemoryStore()
    monkeypatch.setattr(data, "_store", lambda: memory)
    data._blob.clear()
    data._pointer_manifest.clear()
    yield memory
    data._blob.clear()
    data._pointer_manifest.clear()


def _snapshot() -> dict[str, Any]:
    snapshot: dict[str, Any] = json.loads(FIXTURE.read_text(encoding="utf-8"))
    return snapshot


def test_round_trip_through_the_writer(store: MemoryStore) -> None:
    snapshot = _snapshot()
    SnapshotWriter(store, retention=timedelta(hours=24)).publish(snapshot, GENERATED_AT)  # type: ignore[arg-type]
    loaded = data.load_snapshot()
    assert loaded.data == snapshot
    assert loaded.generated_at == GENERATED_AT
    assert not loaded.is_stale(GENERATED_AT + timedelta(minutes=5))
    assert loaded.is_stale(GENERATED_AT + timedelta(minutes=5, seconds=1))


def test_tampered_blob_is_refused(store: MemoryStore) -> None:
    result = SnapshotWriter(store, retention=timedelta(hours=24)).publish(_snapshot(), GENERATED_AT)  # type: ignore[arg-type]
    manifest = json.loads(store.objects[result.manifest_key])
    key = f"{BLOB_PREFIX}{manifest['sections']['scoring']}.json"
    store.objects[key] = json.dumps({"scored_30d": 999, "hits_30d": 999}).encode()
    with pytest.raises(data.SnapshotUnavailableError, match="content hash"):
        data.load_snapshot()


def test_content_outside_the_schema_is_refused(store: MemoryStore) -> None:
    snapshot = _snapshot()
    snapshot["accounts"][0]["open_positions"][0]["client_id"] = "e-abc123"
    SnapshotWriter(store, retention=timedelta(hours=24)).publish(snapshot, GENERATED_AT)  # type: ignore[arg-type]
    with pytest.raises(data.SnapshotUnavailableError, match="schema"):
        data.load_snapshot()


@pytest.mark.parametrize("opened_at", ["2026-09-26T22:00:00", "not a date", "20260926T220000Z"])
def test_malformed_date_time_is_refused_on_read(store: MemoryStore, opened_at: str) -> None:
    # The reader applies the publisher's strict RFC 3339 check: a default FormatChecker skips
    # `date-time` without rfc3339-validator installed, so these used to load.
    snapshot = _snapshot()
    snapshot["accounts"][0]["open_positions"][0]["opened_at"] = opened_at
    SnapshotWriter(store, retention=timedelta(hours=24)).publish(snapshot, GENERATED_AT)  # type: ignore[arg-type]
    with pytest.raises(data.SnapshotUnavailableError, match="schema"):
        data.load_snapshot()


def test_missing_or_unreachable_store_is_unavailable(store: MemoryStore) -> None:
    with pytest.raises(data.SnapshotUnavailableError):
        data.load_snapshot()
    SnapshotWriter(store, retention=timedelta(hours=24)).publish(_snapshot(), GENERATED_AT)  # type: ignore[arg-type]
    store.objects[LATEST_KEY] = b'{"manifest": "manifests/missing.json"}'
    with pytest.raises(data.SnapshotUnavailableError):
        data.load_snapshot()
    store.fail = True
    with pytest.raises(data.SnapshotUnavailableError):
        data.load_snapshot()


def test_dashboard_imports_no_trading_code() -> None:
    probe = "import sys, hdt.public_dashboard.app\nprint('\\n'.join(sorted(sys.modules)))\n"
    loaded = set(
        subprocess.run(  # noqa: S603 - fixed interpreter and script
            [sys.executable, "-c", probe], check=True, capture_output=True, text=True
        ).stdout.split()
    )
    hdt_modules = {m for m in loaded if m == "hdt" or m.startswith("hdt.")}
    allowed = {
        "hdt",
        "hdt.brand",
        "hdt.console",
        "hdt.console.theme",
        "hdt.public",
        "hdt.public.allowlist",
        "hdt.public.store",
    }
    assert {m for m in hdt_modules if not m.startswith("hdt.public_dashboard")} <= allowed
    assert not loaded & {
        "sqlalchemy",
        "redis",
        "psycopg",
        "asyncpg",
        "nacl",
        "cryptography.hazmat.primitives.asymmetric.ed25519",
    }
