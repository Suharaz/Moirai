"""On-chain incident summary (hdt.news.onchain) over recorded CMC DEX captures, point in time."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from hdt.lake.pit_query import PitQuery
from hdt.lake.raw_store import RawStore
from hdt.lake.schemas import Capture
from hdt.news.onchain import onchain_state
from hdt.tools.impl.dex import LIQUIDITY_ROUTE, SECURITY_ROUTE

COIN = 777
HEADLINE = datetime(2026, 9, 1, 10, 20, tzinfo=UTC)


def _ms(at: datetime) -> int:
    return int(at.timestamp() * 1000)


def _record(store: RawStore, route: str, at: datetime, data: Any) -> None:
    body = json.dumps({"status": {"error_code": 0}, "data": data}).encode()
    store.append(Capture("cmc", route, at, 200, body, params={"address": "0xabc"}, key=str(COIN)))


def _change(at: datetime, kind: str, usd: float, tx: str) -> dict[str, Any]:
    return {"ts": _ms(at), "tp": kind, "tu": usd, "txId": tx}


def test_drain_before_the_headline_counts_across_captures(tmp_path: Path) -> None:
    store = RawStore(tmp_path / "staging", tmp_path / "lake")
    pit = PitQuery(tmp_path / "staging", tmp_path / "lake")
    drain = _change(HEADLINE - timedelta(minutes=20), "remove", 600_000.0, "0xdrain")
    # The earlier capture lists the drain; the later one lists it again plus a small add (counted once).
    _record(store, LIQUIDITY_ROUTE, HEADLINE - timedelta(minutes=5), {"lcs": [drain]})
    later = [drain, _change(HEADLINE + timedelta(minutes=5), "add", 1_000.0, "0xadd")]
    _record(store, LIQUIDITY_ROUTE, HEADLINE + timedelta(minutes=10), {"lcs": later})
    as_of = HEADLINE + timedelta(minutes=30)

    after_only = onchain_state(pit, COIN, HEADLINE, as_of)
    assert after_only is not None
    assert (after_only.removals_usd, after_only.changes_seen) == (0.0, 1)

    around = onchain_state(pit, COIN, HEADLINE - timedelta(hours=24), as_of)
    assert around is not None
    assert (around.removals_usd, around.changes_seen) == (600_000.0, 2)
    assert around.incident(250_000.0)
    assert [c.fetched_at for c in around.liquidity] == [
        HEADLINE - timedelta(minutes=5),
        HEADLINE + timedelta(minutes=10),
    ]
    assert around.refs[0].fetched_at == HEADLINE - timedelta(minutes=5)  # the capture listing the drain


def test_security_only_capture_has_no_liquidity_data(tmp_path: Path) -> None:
    store = RawStore(tmp_path / "staging", tmp_path / "lake")
    pit = PitQuery(tmp_path / "staging", tmp_path / "lake")
    _record(store, SECURITY_ROUTE, HEADLINE + timedelta(minutes=10), [{"securityItems": []}])
    state = onchain_state(pit, COIN, HEADLINE - timedelta(hours=24), HEADLINE + timedelta(minutes=30))
    assert state is not None
    assert (state.changes_seen, state.liquidity, state.incident(250_000.0)) == (0, (), False)


def _window(state: Any) -> bool:
    return bool(state.covered_after(HEADLINE, HEADLINE + timedelta(minutes=30)))


def test_empty_post_headline_capture_does_not_borrow_older_data(tmp_path: Path) -> None:
    store = RawStore(tmp_path / "staging", tmp_path / "lake")
    pit = PitQuery(tmp_path / "staging", tmp_path / "lake")
    old_add = _change(HEADLINE - timedelta(hours=11), "add", 500.0, "0xold")
    _record(store, LIQUIDITY_ROUTE, HEADLINE - timedelta(hours=10), {"lcs": [old_add]})
    _record(store, LIQUIDITY_ROUTE, HEADLINE + timedelta(minutes=10), {"lcs": []})
    _record(store, LIQUIDITY_ROUTE, HEADLINE + timedelta(minutes=12), {})  # vendor glitch
    state = onchain_state(pit, COIN, HEADLINE - timedelta(hours=24), HEADLINE + timedelta(minutes=40))
    assert state is not None
    assert state.changes_seen == 1
    assert not state.incident(250_000.0)
    assert not _window(state)


def test_full_page_after_the_headline_does_not_cover_the_headline(tmp_path: Path) -> None:
    store = RawStore(tmp_path / "staging", tmp_path / "lake")
    pit = PitQuery(tmp_path / "staging", tmp_path / "lake")
    busy = [_change(HEADLINE + timedelta(seconds=i + 1), "add", 10.0, f"0x{i}") for i in range(100)]
    _record(store, LIQUIDITY_ROUTE, HEADLINE + timedelta(minutes=10), {"lcs": busy})
    state = onchain_state(pit, COIN, HEADLINE - timedelta(hours=24), HEADLINE + timedelta(minutes=40))
    assert state is not None
    assert not _window(state)

    reaching = [_change(HEADLINE - timedelta(hours=2), "add", 10.0, "0xpre"), *busy[:5]]
    _record(store, LIQUIDITY_ROUTE, HEADLINE + timedelta(minutes=15), {"lcs": reaching})
    state = onchain_state(pit, COIN, HEADLINE - timedelta(hours=24), HEADLINE + timedelta(minutes=40))
    assert state is not None
    assert _window(state)


def test_unrepresentable_change_time_is_skipped(tmp_path: Path) -> None:
    store = RawStore(tmp_path / "staging", tmp_path / "lake")
    pit = PitQuery(tmp_path / "staging", tmp_path / "lake")
    bad = [{"ts": 1.7e18, "tp": "remove", "tu": 1e6}, {"ts": 1e30, "tp": "remove", "tu": 1e6}]
    _record(store, LIQUIDITY_ROUTE, HEADLINE + timedelta(minutes=10), {"lcs": bad})
    state = onchain_state(pit, COIN, HEADLINE - timedelta(hours=24), HEADLINE + timedelta(minutes=40))
    assert state is not None
    assert state.changes_seen == 0
