"""Snapshot loading for the public dashboard (read-only S3 credentials, nothing else).

`latest.json` -> manifest -> one immutable blob per section. Blobs are content-addressed, so they are
cached for the life of the process; the pointer is re-read every 30 s. The assembled snapshot is validated
with the publisher's own validator (same JSON schema, same strict `date-time` check) before anything is
shown.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Final

import streamlit as st

from hdt.public.allowlist import SECTIONS, snapshot_validator
from hdt.public.store import PublicStore, StoreConfig, StoreError

STALE_AFTER: Final[timedelta] = timedelta(minutes=5)


class SnapshotUnavailableError(RuntimeError):
    """No valid snapshot can be shown (store unreachable, nothing published yet, or invalid content)."""


@dataclass(frozen=True)
class LoadedSnapshot:
    data: dict[str, Any]
    generated_at: datetime

    def age(self, now: datetime | None = None) -> timedelta:
        return (now or datetime.now(UTC)) - self.generated_at

    def is_stale(self, now: datetime | None = None) -> bool:
        return self.age(now) > STALE_AFTER


@st.cache_resource
def _store() -> PublicStore:
    return PublicStore(StoreConfig.from_env())


@st.cache_data(max_entries=64, show_spinner=False)
def _blob(digest: str) -> Any:
    body = _store().get(f"blobs/{digest}.json")
    if hashlib.sha256(body).hexdigest() != digest:
        raise SnapshotUnavailableError("a snapshot section does not match its content hash")
    return json.loads(body)


@st.cache_data(ttl=30, show_spinner=False)
def _pointer_manifest() -> dict[str, Any]:
    store = _store()
    pointer = json.loads(store.get("latest.json"))
    manifest = json.loads(store.get(str(pointer["manifest"])))
    if not isinstance(manifest, dict) or not isinstance(manifest.get("sections"), dict):
        raise SnapshotUnavailableError("malformed snapshot manifest")
    return manifest


def assemble(manifest: dict[str, Any], sections: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": manifest["schema_version"],
        "generated_at": manifest["generated_at"],
        **sections,
    }


def load_snapshot() -> LoadedSnapshot:
    try:
        manifest = _pointer_manifest()
        sections = {name: _blob(str(manifest["sections"][name])) for name in SECTIONS}
    except (StoreError, KeyError, ValueError, TypeError) as exc:
        raise SnapshotUnavailableError(type(exc).__name__) from None
    snapshot = assemble(manifest, sections)
    if next(snapshot_validator().iter_errors(snapshot), None) is not None:
        raise SnapshotUnavailableError("the snapshot does not match the public schema")
    generated = datetime.fromisoformat(str(snapshot["generated_at"]).replace("Z", "+00:00"))
    return LoadedSnapshot(snapshot, generated.astimezone(UTC))
