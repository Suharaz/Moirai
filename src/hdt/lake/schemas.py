"""Raw lake record, partition layout and the per-partition hash chain.

Layout (hive style, readable by DuckDB/Polars/pyarrow):
    <root>/source=<source>/route=<route>/date=YYYY-MM-DD/hour=HH/
Staging holds one append-only `records.jsonl` per open partition; the lake holds one `data.parquet` per
closed partition (zstd), with the chain head in the Parquet key-value metadata.

Chain: `record_hash = sha256(prev_hash || "\\n" || canonical_json(header))`, where the header is every
field except the compressed body (the body is covered by `body_sha256`). The first `prev_hash` of a
partition is `sha256("genesis:" + partition path)`, so partitions verify independently.
"""

from __future__ import annotations

import base64
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any, Final, Literal

import pyarrow as pa
import zstandard

from hdt.core.clock import ensure_utc
from hdt.core.ids import canonical_json, canonical_sha256, sha256_hex

Source = Literal["cmc", "binance", "news", "derived"]
SOURCES: Final[tuple[Source, ...]] = ("cmc", "binance", "news", "derived")
_ROUTE_RE: Final = re.compile(r"^[a-z0-9_]{1,64}$")
# `key` is a column, never a path segment: any Unicode word character is fine (provider symbols such as
# a non-ASCII Binance ticker must not abort a capture), control characters and separators are not.
_KEY_RE: Final = re.compile(r"^[\w.:-]{0,64}$")
_ZSTD_LEVEL: Final[int] = 6
CMC_OHLCV_BACKFILL_ROUTE: Final[str] = "ohlcv_backfill"
"""Lake route of the one-off per-coin history call of CMC route #7 (`ohlcv_historical`): its own partitions,
so the "backfilled yet?" lookup never scans every hourly #7 partition."""

PARQUET_SCHEMA: Final[pa.Schema] = pa.schema(
    [
        pa.field("seq", pa.int64(), nullable=False),
        pa.field("source", pa.string(), nullable=False),
        pa.field("route", pa.string(), nullable=False),
        pa.field("key", pa.string(), nullable=False),
        pa.field("params_hash", pa.string(), nullable=False),
        pa.field("params_json", pa.string(), nullable=False),
        pa.field("fetched_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("status_ts", pa.timestamp("us", tz="UTC"), nullable=True),
        pa.field("http_status", pa.int32(), nullable=False),
        pa.field("body_sha256", pa.string(), nullable=False),
        pa.field("body_zstd", pa.binary(), nullable=False),
        pa.field("prev_hash", pa.string(), nullable=False),
        pa.field("record_hash", pa.string(), nullable=False),
    ]
)
META_HEAD: Final[bytes] = b"hdt.head_hash"
META_COUNT: Final[bytes] = b"hdt.count"


class LakeIntegrityError(RuntimeError):
    """A stored record or partition does not match its hash chain."""


@dataclass(frozen=True, order=True)
class Partition:
    source: str
    route: str
    day: date
    hour: int

    @classmethod
    def of(cls, source: str, route: str, fetched_at: datetime) -> Partition:
        ts = ensure_utc(fetched_at)
        return cls(source, route, ts.date(), ts.hour)

    @property
    def relpath(self) -> str:
        return f"source={self.source}/route={self.route}/date={self.day.isoformat()}/hour={self.hour:02d}"

    @property
    def start(self) -> datetime:
        return datetime(self.day.year, self.day.month, self.day.day, self.hour, tzinfo=UTC)

    @property
    def end(self) -> datetime:
        return self.start + timedelta(hours=1)

    @property
    def genesis(self) -> str:
        return sha256_hex(f"genesis:{self.relpath}")


@dataclass(frozen=True)
class Capture:
    """One verbatim response as received, before it is chained and stored."""

    source: Source
    route: str
    fetched_at: datetime
    http_status: int
    body: bytes
    params: Mapping[str, Any] | None = None
    key: str = ""
    status_ts: datetime | None = None

    def __post_init__(self) -> None:
        if self.source not in SOURCES:
            raise ValueError(f"unknown source {self.source!r}")
        if not _ROUTE_RE.fullmatch(self.route):
            raise ValueError(f"route must match {_ROUTE_RE.pattern}: {self.route!r}")
        if not _KEY_RE.fullmatch(self.key):
            raise ValueError(f"key must match {_KEY_RE.pattern}: {self.key!r}")
        ensure_utc(self.fetched_at)
        if self.status_ts is not None:
            ensure_utc(self.status_ts)


@dataclass(frozen=True)
class RawRecord:
    seq: int
    source: str
    route: str
    key: str
    params_hash: str
    params_json: str
    fetched_at: datetime
    status_ts: datetime | None
    http_status: int
    body_sha256: str
    body_zstd: bytes
    prev_hash: str
    record_hash: str

    def header(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "source": self.source,
            "route": self.route,
            "key": self.key,
            "params_hash": self.params_hash,
            "params_json": self.params_json,
            "fetched_at": self.fetched_at,
            "status_ts": self.status_ts,
            "http_status": self.http_status,
            "body_sha256": self.body_sha256,
        }

    def expected_hash(self) -> str:
        return chain_hash(self.prev_hash, self.header())

    def body(self) -> bytes:
        """Decompressed body, verified against `body_sha256`."""
        raw = zstandard.ZstdDecompressor().decompress(self.body_zstd)
        if sha256_hex(raw) != self.body_sha256:
            raise LakeIntegrityError(f"body hash mismatch in {self.source}/{self.route} seq {self.seq}")
        return raw

    def to_json_line(self) -> bytes:
        return canonical_json({**self.header(), **self._tail()}) + b"\n"

    def _tail(self) -> dict[str, Any]:
        return {
            "body_zstd": base64.b64encode(self.body_zstd).decode("ascii"),
            "prev_hash": self.prev_hash,
            "record_hash": self.record_hash,
        }

    @classmethod
    def from_mapping(cls, row: Mapping[str, Any]) -> RawRecord:
        body = row["body_zstd"]
        return cls(
            seq=int(row["seq"]),
            source=str(row["source"]),
            route=str(row["route"]),
            key=str(row["key"]),
            params_hash=str(row["params_hash"]),
            params_json=str(row["params_json"]),
            fetched_at=_as_utc(row["fetched_at"]),
            status_ts=_as_utc(row["status_ts"]) if row["status_ts"] is not None else None,
            http_status=int(row["http_status"]),
            body_sha256=str(row["body_sha256"]),
            body_zstd=base64.b64decode(body) if isinstance(body, str) else bytes(body),
            prev_hash=str(row["prev_hash"]),
            record_hash=str(row["record_hash"]),
        )


def chain_hash(prev_hash: str, header: Mapping[str, Any]) -> str:
    return sha256_hex(prev_hash.encode("ascii") + b"\n" + canonical_json(dict(header)))


def build_record(capture: Capture, *, seq: int, prev_hash: str) -> RawRecord:
    params = dict(capture.params or {})
    fields: dict[str, Any] = {
        "seq": seq,
        "source": capture.source,
        "route": capture.route,
        "key": capture.key,
        "params_hash": canonical_sha256(params),
        "params_json": canonical_json(params).decode("utf-8"),
        "fetched_at": ensure_utc(capture.fetched_at),
        "status_ts": ensure_utc(capture.status_ts) if capture.status_ts else None,
        "http_status": capture.http_status,
        "body_sha256": sha256_hex(capture.body),
    }
    return RawRecord(
        **fields,
        body_zstd=zstandard.ZstdCompressor(level=_ZSTD_LEVEL).compress(capture.body),
        prev_hash=prev_hash,
        record_hash=chain_hash(prev_hash, fields),
    )


def verify_chain(partition: Partition, records: list[RawRecord]) -> str:
    """Check order, links and hashes of a whole partition; return the head hash."""
    prev = partition.genesis
    for expected_seq, record in enumerate(records):
        if record.seq != expected_seq:
            raise LakeIntegrityError(f"{partition.relpath}: seq {record.seq} where {expected_seq} expected")
        if (record.source, record.route) != (partition.source, partition.route):
            raise LakeIntegrityError(f"{partition.relpath}: record {record.seq} belongs to another route")
        if Partition.of(record.source, record.route, record.fetched_at) != partition:
            raise LakeIntegrityError(
                f"{partition.relpath}: record {record.seq} is outside the partition hour"
            )
        if record.prev_hash != prev or record.expected_hash() != record.record_hash:
            raise LakeIntegrityError(f"{partition.relpath}: hash chain broken at seq {record.seq}")
        prev = record.record_hash
    return prev


def _as_utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        return ensure_utc(value)
    return ensure_utc(datetime.fromisoformat(str(value)))
