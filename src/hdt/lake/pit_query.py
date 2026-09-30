"""Point-in-time reads over the raw lake: only records with `fetched_at <= as_of` are ever returned.

Reads compacted Parquet partitions and the open staging files of the current hours alike, so live and
replay callers see the same data for the same `as_of`. A torn staging line (a write still in flight in
the recorder) is ignored.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import pyarrow.parquet as pq

from hdt.core.clock import ensure_utc
from hdt.lake.raw_store import LAKE_FILE, STAGING_FILE, read_staging
from hdt.lake.schemas import PARQUET_SCHEMA, Partition, RawRecord

DEFAULT_LOOKBACK = timedelta(days=8)


class PitQuery:
    def __init__(self, staging_root: Path, lake_root: Path) -> None:
        self.staging_root = staging_root
        self.lake_root = lake_root

    def latest(
        self,
        source: str,
        route: str,
        as_of: datetime,
        *,
        key: str = "",
        lookback: timedelta = DEFAULT_LOOKBACK,
    ) -> RawRecord | None:
        """Newest record at or before `as_of` within `lookback` (None when nothing was recorded)."""
        as_of = ensure_utc(as_of)
        for partition in reversed(self._partitions(source, route, as_of - lookback, as_of)):
            found = self._read(partition, key, as_of - lookback, as_of)
            if found:
                return max(found, key=lambda r: (r.fetched_at, r.seq))
        return None

    def series(
        self, source: str, route: str, start: datetime, end: datetime, *, as_of: datetime, key: str = ""
    ) -> list[RawRecord]:
        """Records with `start <= fetched_at <= min(end, as_of)`, oldest first."""
        lo, hi = ensure_utc(start), min(ensure_utc(end), ensure_utc(as_of))
        if hi < lo:
            return []
        out: list[RawRecord] = []
        for partition in self._partitions(source, route, lo, hi):
            out.extend(self._read(partition, key, lo, hi))
        return sorted(out, key=lambda r: (r.fetched_at, r.seq))

    def _partitions(self, source: str, route: str, lo: datetime, hi: datetime) -> list[Partition]:
        """Hour partitions that exist on disk (staging or lake) and overlap `[lo, hi]`, oldest first.

        Listing directories instead of probing every hour keeps long lookbacks cheap.
        """
        found = _partitions_on_disk(self.staging_root, source, route, lo, hi)
        found |= _partitions_on_disk(self.lake_root, source, route, lo, hi)
        return sorted(found, key=lambda p: p.start)

    def _read(self, partition: Partition, key: str, lo: datetime, hi: datetime) -> list[RawRecord]:
        lake = self.lake_root / partition.relpath / LAKE_FILE
        if lake.exists():
            table = pq.read_table(
                lake,
                schema=PARQUET_SCHEMA,
                filters=[("key", "=", key), ("fetched_at", ">=", lo), ("fetched_at", "<=", hi)],
            )
            return [RawRecord.from_mapping(row) for row in table.to_pylist()]
        staging = self.staging_root / partition.relpath / STAGING_FILE
        return [r for r in read_staging(staging) if r.key == key and lo <= r.fetched_at <= hi]


def _partitions_on_disk(root: Path, source: str, route: str, lo: datetime, hi: datetime) -> set[Partition]:
    base = root / f"source={source}" / f"route={route}"
    found: set[Partition] = set()
    if not base.is_dir():
        return found
    for day_dir in base.glob("date=*"):
        day = date.fromisoformat(day_dir.name.split("=", 1)[1])
        if not lo.date() <= day <= hi.date():
            continue
        for hour_dir in day_dir.glob("hour=*"):
            partition = Partition(source, route, day, int(hour_dir.name.split("=", 1)[1]))
            if partition.end > lo and partition.start <= hi:
                found.add(partition)
    return found
