"""Raw lake: point-in-time reads never see the future, tampering is detected, closed hours are immutable."""

from __future__ import annotations

import json
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from hdt.lake.pit_query import PitQuery
from hdt.lake.raw_store import ClosedPartitionError, RawStore
from hdt.lake.schemas import Capture, LakeIntegrityError, Partition

T0 = datetime(2026, 9, 27, 10, 0, tzinfo=UTC)


def _capture(offset_s: float, body: bytes = b"{}", key: str = "", route: str = "quotes") -> Capture:
    return Capture(
        source="cmc",
        route=route,
        fetched_at=T0 + timedelta(seconds=offset_s),
        http_status=200,
        body=body,
        params={"id": 1},
        key=key,
    )


def _stores(root: Path) -> tuple[RawStore, PitQuery]:
    return RawStore(root / "staging", root / "lake"), PitQuery(root / "staging", root / "lake")


@settings(max_examples=40, deadline=None)
@given(
    offsets=st.lists(st.integers(min_value=0, max_value=4 * 3600), min_size=1, max_size=30),
    probes=st.lists(st.integers(min_value=-600, max_value=5 * 3600), min_size=1, max_size=8),
    compact_after_s=st.integers(min_value=0, max_value=5 * 3600),
)
def test_pit_never_returns_future_records(
    offsets: list[int], probes: list[int], compact_after_s: int
) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        store, pit = _stores(Path(tmp))
        for i, offset in enumerate(sorted(offsets)):
            store.append(_capture(offset, body=str(i).encode()))
        store.compact_closed(T0 + timedelta(seconds=compact_after_s))
        for probe in probes:
            as_of = T0 + timedelta(seconds=probe)
            visible = [o for o in offsets if T0 + timedelta(seconds=o) <= as_of]
            latest = pit.latest("cmc", "quotes", as_of)
            if not visible:
                assert latest is None
                continue
            assert latest is not None
            assert latest.fetched_at == T0 + timedelta(seconds=max(visible))
            series = pit.series(
                "cmc", "quotes", T0 - timedelta(hours=1), T0 + timedelta(hours=6), as_of=as_of
            )
            assert all(r.fetched_at <= as_of for r in series)
            assert len(series) == len(visible)


def test_latest_filters_by_key_and_returns_the_verbatim_body(tmp_path: Path) -> None:
    store, pit = _stores(tmp_path)
    store.append(_capture(10, body=b'{"btc":1}', key="1"))
    store.append(_capture(20, body=b'{"eth":1}', key="1027"))
    record = pit.latest("cmc", "quotes", T0 + timedelta(minutes=1), key="1")
    assert record is not None
    assert record.body() == b'{"btc":1}'


def test_appending_to_a_compacted_hour_is_refused(tmp_path: Path) -> None:
    store, _ = _stores(tmp_path)
    store.append(_capture(10))
    assert store.compact_closed(T0 + timedelta(hours=2)) == [Partition.of("cmc", "quotes", T0)]
    fresh = RawStore(tmp_path / "staging", tmp_path / "lake")
    with pytest.raises(ClosedPartitionError):
        fresh.append(_capture(20))


def test_open_hour_is_not_compacted(tmp_path: Path) -> None:
    store, _ = _stores(tmp_path)
    store.append(_capture(10))
    assert store.compact_closed(T0 + timedelta(minutes=61)) == []  # inside the close grace period
    assert store.open_partitions() == [Partition.of("cmc", "quotes", T0)]


def test_chain_continues_after_restart(tmp_path: Path) -> None:
    store, _ = _stores(tmp_path)
    store.append(_capture(1))
    restarted = RawStore(tmp_path / "staging", tmp_path / "lake")
    second = restarted.append(_capture(2))
    assert second.seq == 1
    restarted.verify(Partition.of("cmc", "quotes", T0))


def test_torn_staging_tail_is_dropped_before_the_next_append(tmp_path: Path) -> None:
    store, _ = _stores(tmp_path)
    store.append(_capture(1))
    partition = Partition.of("cmc", "quotes", T0)
    with store.staging_path(partition).open("ab") as fh:
        fh.write(b'{"seq":1,"torn')
    restarted = RawStore(tmp_path / "staging", tmp_path / "lake")
    assert restarted.append(_capture(2)).seq == 1
    restarted.verify(partition)


def test_tampered_staging_body_is_detected(tmp_path: Path) -> None:
    store, _ = _stores(tmp_path)
    store.append_many([_capture(1, body=b"a"), _capture(2, body=b"b")])
    partition = Partition.of("cmc", "quotes", T0)
    path = store.staging_path(partition)
    lines = path.read_bytes().splitlines()
    first = json.loads(lines[0])
    first["http_status"] = 500
    path.write_bytes(json.dumps(first).encode() + b"\n" + lines[1] + b"\n")
    with pytest.raises(LakeIntegrityError):
        store.verify(partition)
    with pytest.raises(LakeIntegrityError):
        store.compact(partition)


def test_tampered_parquet_partition_is_detected(tmp_path: Path) -> None:
    store, _ = _stores(tmp_path)
    store.append_many([_capture(1, body=b"a"), _capture(2, body=b"b")])
    partition = Partition.of("cmc", "quotes", T0)
    store.compact(partition)
    target = store.lake_path(partition)
    table = pq.read_table(target)
    rows = table.to_pylist()
    rows[1]["params_json"] = '{"id":2}'
    pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), target)
    with pytest.raises(LakeIntegrityError):
        store.verify(partition)


def test_compaction_is_idempotent_after_a_crash_before_staging_removal(tmp_path: Path) -> None:
    store, _ = _stores(tmp_path)
    store.append(_capture(1))
    partition = Partition.of("cmc", "quotes", T0)
    staged = store.staging_path(partition).read_bytes()
    head = store.compact(partition)
    store.staging_path(partition).parent.mkdir(parents=True, exist_ok=True)
    store.staging_path(partition).write_bytes(staged)  # crash left the staging file behind
    assert store.compact(partition) == head
    assert not store.staging_path(partition).exists()


def test_daily_root_requires_closed_hours_and_changes_with_content(tmp_path: Path) -> None:
    store, _ = _stores(tmp_path)
    store.append(_capture(1))
    with pytest.raises(ClosedPartitionError):
        store.daily_root(T0.date())
    store.compact_closed(T0 + timedelta(hours=2))
    root_a = store.daily_root(T0.date())["merkle_root"]

    other, _ = _stores(tmp_path / "other")
    other.append(_capture(1, body=b"different"))
    other.compact_closed(T0 + timedelta(hours=2))
    assert other.daily_root(T0.date())["merkle_root"] != root_a


def test_expire_raw_removes_only_old_depth_days_that_have_a_merkle_root(tmp_path: Path) -> None:
    store, pit = _stores(tmp_path)
    for day in range(3):
        store.append(_capture(day * 86_400, route="ws_depth"))
        store.append(_capture(day * 86_400, route="quotes"))
    store.compact_closed(T0 + timedelta(days=3))
    store.daily_root(T0.date())  # only day 0 and day 1 get a manifest
    store.daily_root((T0 + timedelta(days=1)).date())
    removed = store.expire_raw([("cmc", "ws_depth")], before=(T0 + timedelta(days=3)).date())
    assert [p.day for p in removed] == [T0.date(), (T0 + timedelta(days=1)).date()]
    after = T0 + timedelta(days=4)
    assert pit.series("cmc", "ws_depth", T0, after, as_of=after) == pit.series(
        "cmc", "ws_depth", T0 + timedelta(days=2), after, as_of=after
    )
    assert len(pit.series("cmc", "ws_depth", T0, after, as_of=after)) == 1  # day 2: no manifest yet
    assert len(pit.series("cmc", "quotes", T0, after, as_of=after)) == 3  # other routes untouched


def test_non_ascii_provider_keys_are_recorded_and_read_back(tmp_path: Path) -> None:
    store, pit = _stores(tmp_path)
    store.append(_capture(1, key="币安人生USDT"))
    assert pit.latest("cmc", "quotes", T0 + timedelta(seconds=2), key="币安人生USDT") is not None
    store.compact_closed(T0 + timedelta(hours=2))
    assert pit.latest("cmc", "quotes", T0 + timedelta(hours=2), key="币安人生USDT") is not None
    with pytest.raises(ValueError, match="key must match"):
        _capture(1, key="a/b")
