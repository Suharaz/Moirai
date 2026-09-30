"""Append-only raw store: staging JSONL per open hour, compacted to Parquet when the hour closes.

There is no update API and records are never deleted individually. The only removal is `expire_raw`: whole
compacted partitions of the raw depth routes older than the retention window (90 days by default,
Design Contract risk handling), after their day's Merkle manifest has been written.

Records are partitioned by `fetched_at` (the recorder's receive time), so a record can only
land in an open hour; appending to an hour that is already compacted is refused.
Compaction verifies the whole chain, writes Parquet to a temporary name and renames it atomically;
it is idempotent, so a crash between the rename and the staging removal is repaired on the next run.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Final

import pyarrow as pa
import pyarrow.parquet as pq

from hdt.core.clock import ensure_utc, utcnow
from hdt.core.ids import canonical_json, sha256_hex
from hdt.lake.schemas import (
    META_COUNT,
    META_HEAD,
    PARQUET_SCHEMA,
    Capture,
    LakeIntegrityError,
    Partition,
    RawRecord,
    build_record,
    verify_chain,
)

log = logging.getLogger(__name__)

STAGING_FILE: Final[str] = "records.jsonl"
LAKE_FILE: Final[str] = "data.parquet"
ROOTS_DIR: Final[str] = "roots"


class ClosedPartitionError(RuntimeError):
    """The target hour is already compacted; the lake is immutable."""


@dataclass
class _ChainState:
    next_seq: int
    head: str


class RawStore:
    def __init__(self, staging_root: Path, lake_root: Path, *, close_grace: timedelta = timedelta(minutes=2)):
        self.staging_root = staging_root
        self.lake_root = lake_root
        self.close_grace = close_grace
        self._lock = threading.Lock()
        self._chains: dict[Partition, _ChainState] = {}

    # ------------------------------------------------------------------ paths

    def staging_path(self, partition: Partition) -> Path:
        return self.staging_root / partition.relpath / STAGING_FILE

    def lake_path(self, partition: Partition) -> Path:
        return self.lake_root / partition.relpath / LAKE_FILE

    # ------------------------------------------------------------------ writes

    def append(self, capture: Capture) -> RawRecord:
        return self.append_many([capture])[0]

    def append_many(self, captures: Iterable[Capture]) -> list[RawRecord]:
        """Chain and durably append captures (one fsync per touched partition)."""
        written: list[RawRecord] = []
        with self._lock:
            lines: dict[Partition, list[bytes]] = {}
            for capture in captures:
                partition = Partition.of(capture.source, capture.route, capture.fetched_at)
                state = self._state(partition)
                record = build_record(capture, seq=state.next_seq, prev_hash=state.head)
                state.next_seq, state.head = record.seq + 1, record.record_hash
                lines.setdefault(partition, []).append(record.to_json_line())
                written.append(record)
            for partition, chunk in lines.items():
                path = self.staging_path(partition)
                path.parent.mkdir(parents=True, exist_ok=True)
                with path.open("ab") as fh:
                    fh.write(b"".join(chunk))
                    fh.flush()
                    os.fsync(fh.fileno())
        return written

    def _state(self, partition: Partition) -> _ChainState:
        state = self._chains.get(partition)
        if state is not None:
            return state
        if self.lake_path(partition).exists():
            raise ClosedPartitionError(f"{partition.relpath} is already compacted")
        last = _recover_tail(self.staging_path(partition))
        if last is None:
            state = _ChainState(0, partition.genesis)
        else:
            record = RawRecord.from_mapping(json.loads(last))
            state = _ChainState(record.seq + 1, record.record_hash)
        self._chains[partition] = state
        return state

    # ------------------------------------------------------------------ compaction

    def open_partitions(self) -> list[Partition]:
        return sorted(_partitions_under(self.staging_root, STAGING_FILE))

    def compact_closed(self, now: datetime | None = None) -> list[Partition]:
        """Compact every staging partition whose hour ended at least `close_grace` ago."""
        cutoff = ensure_utc(now or utcnow()) - self.close_grace
        done = []
        for partition in self.open_partitions():
            if partition.end <= cutoff:
                self.compact(partition)
                done.append(partition)
        return done

    def compact(self, partition: Partition) -> str:
        with self._lock:
            staging = self.staging_path(partition)
            records = list(read_staging(staging))
            head = verify_chain(partition, records)
            target = self.lake_path(partition)
            if target.exists():
                existing = self.read_partition(partition)
                if verify_chain(partition, existing) != head:
                    raise LakeIntegrityError(
                        f"{partition.relpath}: staging differs from the compacted partition; not overwriting"
                    )
            else:
                _write_parquet(target, records, head)
            staging.unlink()
            _remove_empty_dirs(staging.parent, self.staging_root)
            self._chains.pop(partition, None)
            log.info(
                "partition compacted",
                extra={"partition": partition.relpath, "records": len(records), "head": head},
            )
            return head

    # ------------------------------------------------------------------ reads and integrity

    def read_partition(self, partition: Partition) -> list[RawRecord]:
        target = self.lake_path(partition)
        if target.exists():
            table = pq.read_table(target, schema=PARQUET_SCHEMA)
            return [RawRecord.from_mapping(row) for row in table.to_pylist()]
        return list(read_staging(self.staging_path(partition)))

    def verify(self, partition: Partition) -> str:
        """Re-verify a partition; for compacted ones also check the head stored in the file metadata."""
        head = verify_chain(partition, self.read_partition(partition))
        target = self.lake_path(partition)
        if target.exists():
            meta = pq.read_metadata(target).metadata or {}
            if meta.get(META_HEAD, b"").decode("ascii") != head:
                raise LakeIntegrityError(f"{partition.relpath}: head hash differs from the file metadata")
        return head

    def daily_root(self, day: date) -> dict[str, object]:
        """Merkle root over the compacted partitions of one UTC day; refuses while any hour is open."""
        if any(p.day == day for p in self.open_partitions()):
            raise ClosedPartitionError(f"{day.isoformat()} still has open partitions")
        leaves = sorted(
            f"{p.relpath}:{self.verify(p)}"
            for p in _partitions_under(self.lake_root, LAKE_FILE)
            if p.day == day
        )
        root = merkle_root([sha256_hex(leaf) for leaf in leaves])
        manifest: dict[str, object] = {
            "date": day.isoformat(),
            "partitions": len(leaves),
            "merkle_root": root,
            "generated_at": utcnow(),
        }
        path = self.lake_root / ROOTS_DIR / f"date={day.isoformat()}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write(path, canonical_json(manifest))
        return manifest

    def expire_raw(self, routes: Iterable[tuple[str, str]], before: date) -> list[Partition]:
        """Delete compacted partitions of `routes` ((source, route) pairs) dated before `before`.

        Only days whose Merkle manifest exists are removed, so the daily root still proves what was held.
        """
        wanted = set(routes)
        removed: list[Partition] = []
        for partition in sorted(_partitions_under(self.lake_root, LAKE_FILE)):
            if (partition.source, partition.route) not in wanted or partition.day >= before:
                continue
            manifest = self.lake_root / ROOTS_DIR / f"date={partition.day.isoformat()}.json"
            if not manifest.exists():
                continue
            path = self.lake_path(partition)
            path.unlink()
            _remove_empty_dirs(path.parent, self.lake_root)
            removed.append(partition)
        if removed:
            log.info(
                "raw partitions expired", extra={"partitions": len(removed), "before": before.isoformat()}
            )
        return removed

    def lake_bytes(self) -> int:
        return sum(
            f.stat().st_size
            for root in (self.lake_root, self.staging_root)
            for f in root.rglob("*")
            if f.is_file()
        )


def merkle_root(leaf_hashes: list[str]) -> str:
    if not leaf_hashes:
        return sha256_hex(b"")
    level = leaf_hashes
    while len(level) > 1:
        if len(level) % 2:
            level = [*level, level[-1]]
        level = [sha256_hex(level[i] + level[i + 1]) for i in range(0, len(level), 2)]
    return level[0]


# --------------------------------------------------------------------------- helpers


def _partitions_under(root: Path, filename: str) -> Iterator[Partition]:
    for path in root.glob(f"source=*/route=*/date=*/hour=*/{filename}"):
        hour_dir = path.parent
        parts = {seg.split("=", 1)[0]: seg.split("=", 1)[1] for seg in hour_dir.relative_to(root).parts}
        yield Partition(
            parts["source"], parts["route"], date.fromisoformat(parts["date"]), int(parts["hour"])
        )


def read_staging(path: Path) -> Iterator[RawRecord]:
    """Complete staging records in append order; a torn final line (write in flight) is skipped."""
    if not path.exists():
        return
    with path.open("rb") as fh:
        for number, line in enumerate(fh, start=1):
            if not line.endswith(b"\n"):
                # A torn final write never completed its fsync; it was never acknowledged to the caller.
                log.warning("ignoring torn staging line", extra={"path": str(path), "line": number})
                return
            yield RawRecord.from_mapping(json.loads(line))


def _recover_tail(path: Path) -> bytes | None:
    """Return the last complete staging line, truncating a torn final write first.

    A line without its newline never completed `append_many` (no fsync, never acknowledged), so it is
    dropped; otherwise the next append would be glued onto it.
    """
    if not path.exists():
        return None
    size = path.stat().st_size
    with path.open("r+b") as fh:
        pos, buf = size, b""
        while pos > 0:
            step = min(1 << 16, pos)
            pos -= step
            fh.seek(pos)
            buf = fh.read(step) + buf
            newline = buf.rfind(b"\n")
            if newline == -1:
                continue
            previous = buf.rfind(b"\n", 0, newline)
            if previous == -1 and pos > 0:
                continue
            valid = pos + newline + 1
            if valid < size:
                log.warning("truncating torn staging tail", extra={"path": str(path), "bytes": size - valid})
                fh.truncate(valid)
            return buf[previous + 1 : newline]
        if size:
            log.warning("truncating torn staging tail", extra={"path": str(path), "bytes": size})
            fh.truncate(0)
    return None


def _write_parquet(target: Path, records: list[RawRecord], head: str) -> None:
    table = pa.Table.from_pylist(
        [
            {**r.header(), "body_zstd": r.body_zstd, "prev_hash": r.prev_hash, "record_hash": r.record_hash}
            for r in records
        ],
        schema=PARQUET_SCHEMA,
    ).replace_schema_metadata({META_HEAD: head.encode("ascii"), META_COUNT: str(len(records)).encode()})
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(".parquet.tmp")
    pq.write_table(table, tmp, compression="zstd")
    with tmp.open("r+b") as fh:  # a writable handle: Windows refuses fsync on read-only handles
        os.fsync(fh.fileno())
    os.replace(tmp, target)


def _atomic_write(path: Path, data: bytes) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("wb") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def _remove_empty_dirs(start: Path, stop: Path) -> None:
    current = start
    while current != stop and current.is_dir() and not any(current.iterdir()):
        current.rmdir()
        current = current.parent
