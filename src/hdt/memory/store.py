"""Bitemporal, append-only access to the LangGraph Store.

Production uses `langgraph.store.postgres.PostgresStore` on the `store` table that Alembic migration
`0004_memory` creates (with row-level security per writer and an append-only trigger); services never
call `PostgresStore.setup()` and hold no DDL rights. Tests may use LangGraph's `InMemoryStore`, which
implements the same `BaseStore` API and filter operators.

Every stored value carries `known_at` (ISO) and `known_at_key`: microseconds since the Unix epoch as a
20-digit zero-padded string. Fixed-width digit strings order identically as text (PostgresStore compares
`value->>field` as text) and as numbers (InMemoryStore compares with `float`), so the bound
`known_at < as_of` is pushed down to the store and re-checked here before anything is returned.
Records are never updated or deleted: `append` is idempotent for an identical value and refuses a
different value under an existing key.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Final, Literal

from langgraph.store.base import BaseStore

from hdt.contracts.common import AgentName, ContractModel, UtcDatetime
from hdt.core.clock import ensure_utc

log = logging.getLogger(__name__)

MemoryKind = Literal["episodic", "lessons"]
KNOWN_AT_KEY: Final[str] = "known_at_key"
PAGE_SIZE: Final[int] = 200
_EPOCH: Final[datetime] = datetime(1970, 1, 1, tzinfo=UTC)
_KEY_WIDTH: Final[int] = 20


class MemoryConflictError(RuntimeError):
    """A different record already exists under this key (memory is append-only)."""


def memory_namespace(agent: AgentName | str, kind: MemoryKind) -> tuple[str, str]:
    return (AgentName(agent).value, kind)


def time_key(ts: datetime) -> str:
    """Sortable fixed-width key of an aware UTC instant (microsecond precision)."""
    delta = ensure_utc(ts) - _EPOCH
    micros = (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds
    if micros < 0:
        raise ValueError("instants before 1970 are not supported")
    return f"{micros:0{_KEY_WIDTH}d}"


class MemoryRecord(ContractModel):
    """Base of every memory value: `known_at` is when the system learned the fact (transaction time)."""

    record_type: str
    known_at: UtcDatetime

    def to_value(self) -> dict[str, Any]:
        value = self.model_dump(mode="json")
        value[KNOWN_AT_KEY] = time_key(self.known_at)
        return value


@dataclass(frozen=True)
class StoredRecord:
    namespace: tuple[str, ...]
    key: str
    value: dict[str, Any]
    """The record as written, without the internal `known_at_key`."""


def _strip(value: Mapping[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in value.items() if k != KNOWN_AT_KEY}


class MemoryStore:
    """Append-only writes and `as_of`-bounded reads over any LangGraph `BaseStore`."""

    def __init__(self, store: BaseStore) -> None:
        self._store = store

    def exists(self, namespace: tuple[str, str], key: str) -> bool:
        """Whether any record exists under `key` (writers only: no `as_of` bound)."""
        return self._store.get(namespace, key, refresh_ttl=False) is not None

    def append(self, namespace: tuple[str, str], key: str, record: MemoryRecord) -> bool:
        """Write `record` under `key`; False when the identical record is already there."""
        if not key:
            raise ValueError("memory key must not be empty")
        value = record.to_value()
        existing = self._store.get(namespace, key, refresh_ttl=False)
        if existing is not None:
            if dict(existing.value) == value:
                return False
            raise MemoryConflictError(f"memory record {'.'.join(namespace)}/{key} already exists")
        self._store.put(namespace, key, value, index=False)
        return True

    def get(self, namespace: tuple[str, str], key: str, *, as_of: datetime) -> StoredRecord | None:
        """The record under `key` if it was known strictly before `as_of`."""
        item = self._store.get(namespace, key, refresh_ttl=False)
        if item is None or not _known_before(item.value, time_key(as_of)):
            return None
        return StoredRecord(tuple(item.namespace), item.key, _strip(item.value))

    def query(
        self,
        namespace: tuple[str, str],
        *,
        as_of: datetime,
        record_type: str,
        since: datetime | None = None,
        equals: Mapping[str, Any] | None = None,
    ) -> list[StoredRecord]:
        """Records of `record_type` with `since <= known_at < as_of`, oldest first (ties by key)."""
        upper = time_key(as_of)
        bounds: dict[str, str] = {"$lt": upper}
        if since is not None:
            bounds["$gte"] = time_key(since)
        filters: dict[str, Any] = {"record_type": record_type, KNOWN_AT_KEY: bounds, **(equals or {})}
        found: dict[str, StoredRecord] = {}
        offset = 0
        while True:
            page = self._store.search(
                namespace, filter=filters, limit=PAGE_SIZE, offset=offset, refresh_ttl=False
            )
            for item in page:
                if tuple(item.namespace) != namespace or not _known_before(item.value, upper):
                    continue
                if since is not None and str(item.value.get(KNOWN_AT_KEY, "")) < bounds["$gte"]:
                    continue
                found[item.key] = StoredRecord(tuple(item.namespace), item.key, _strip(item.value))
            if len(page) < PAGE_SIZE:
                break
            offset += PAGE_SIZE
        return sorted(found.values(), key=lambda r: (time_key(_known_at(r.value)), r.key))


def _known_at(value: Mapping[str, Any]) -> datetime:
    return ensure_utc(datetime.fromisoformat(str(value["known_at"])))


def _known_before(value: Mapping[str, Any], upper_key: str) -> bool:
    key = value.get(KNOWN_AT_KEY)
    if not isinstance(key, str) or len(key) != _KEY_WIDTH or "known_at" not in value:
        log.warning("memory record without a valid known_at_key ignored")
        return False
    # The stored key must match the record's own known_at: a mismatch would defeat the pushed-down bound.
    if time_key(_known_at(value)) != key:
        log.warning("memory record whose known_at_key disagrees with known_at ignored")
        return False
    return key < upper_key


def libpq_dsn(dsn: str) -> str:
    """psycopg/libpq connection string from a SQLAlchemy-style DSN (`postgresql+psycopg://`)."""
    for prefix in ("postgresql+psycopg://", "postgres+psycopg://"):
        if dsn.startswith(prefix):
            return "postgresql://" + dsn[len(prefix) :]
    return dsn


@contextmanager
def open_postgres_store(dsn: str) -> Iterator[MemoryStore]:
    """A `MemoryStore` on PostgresStore for the calling service's role (the schema is Alembic's)."""
    from langgraph.store.postgres import PostgresStore

    with PostgresStore.from_conn_string(libpq_dsn(dsn)) as store:
        yield MemoryStore(store)
