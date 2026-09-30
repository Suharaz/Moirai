"""Fail-closed veto (phase 09 section 3): scan age in cycles, stale AccountState, stale Binance data, kill.

Success criterion: no fresh `RiskFlags` -> no LONG is opened.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from hdt.contracts.common import Intent, Side
from hdt.risk.gate import HeldPosition, evaluate
from hdt.risk.kill_state import KillView
from hdt.risk.veto import FlagsBook, age_ok, evaluate_open, exit_required
from risk_builders import (
    CID_SHORT,
    NOW,
    account_state,
    book,
    candidate_set,
    decision,
    flags,
    gate_config,
    gate_inputs,
    market,
    packet,
    position,
    with_decision,
)

CYCLE = 300.0


def test_no_scan_at_all_blocks_long_and_halves_short() -> None:
    long = evaluate_open(None, Side.LONG, NOW, CYCLE)
    assert not long.allowed
    assert long.reason == "veto_stale"
    short = evaluate_open(None, Side.SHORT, NOW, CYCLE)
    assert short.allowed
    assert short.size_mult == 0.5


def test_expired_flags_count_as_absent() -> None:
    expired = flags(age_s=100, ttl_s=60)  # expired 40 s ago although the scan is < 1 cycle old
    assert not evaluate_open(expired, Side.LONG, NOW, CYCLE).allowed
    assert evaluate_open(expired, Side.SHORT, NOW, CYCLE).size_mult == 0.5


def test_fresh_scan_applies_vetoes_and_size_mult() -> None:
    fresh = flags(age_s=CYCLE, veto_long=True, size_mult=0.8)
    blocked = evaluate_open(fresh, Side.LONG, NOW, CYCLE)
    assert not blocked.allowed
    assert blocked.reason == "veto_long"
    allowed = evaluate_open(fresh, Side.SHORT, NOW, CYCLE)
    assert allowed.allowed
    assert allowed.size_mult == 0.8


def test_scan_one_to_two_cycles_old_caps_size_at_half() -> None:
    older = flags(age_s=1.5 * CYCLE, size_mult=0.9)
    verdict = evaluate_open(older, Side.LONG, NOW, CYCLE)
    assert verdict.allowed
    assert verdict.size_mult == 0.5
    assert evaluate_open(flags(age_s=1.5 * CYCLE, size_mult=0.3), Side.LONG, NOW, CYCLE).size_mult == 0.3


def test_scan_older_than_two_cycles_fails_closed() -> None:
    stale = flags(age_s=2 * CYCLE + 1, size_mult=0.9)
    assert evaluate_open(stale, Side.LONG, NOW, CYCLE).reason == "veto_stale"
    assert evaluate_open(stale, Side.SHORT, NOW, CYCLE).size_mult == 0.5
    stale_short_veto = flags(age_s=3 * CYCLE, veto_short=True)
    assert not evaluate_open(stale_short_veto, Side.SHORT, NOW, CYCLE).allowed


def test_held_position_vetoed_against_its_side_must_exit() -> None:
    assert exit_required(flags(veto_long=True), Side.LONG, NOW)
    assert not exit_required(flags(veto_long=True), Side.SHORT, NOW)
    assert not exit_required(flags(veto_long=True, age_s=100, ttl_s=60), Side.LONG, NOW)
    assert not exit_required(None, Side.LONG, NOW)


def test_flags_book_keeps_the_newest_scan_per_coin() -> None:
    fb = FlagsBook.empty()
    newer = flags(age_s=10, veto_long=True)
    older = flags(age_s=200)
    assert fb.offer(newer)
    assert not fb.offer(older)
    assert fb.get(newer.coin_id) == newer


def test_age_ok_boundaries() -> None:
    assert age_ok(NOW - timedelta(seconds=30), NOW, 30)
    assert not age_ok(NOW - timedelta(seconds=31), NOW, 30)
    assert not age_ok(None, NOW, 30)


# ------------------------------------------------------------------ through the gate


def test_gate_opens_no_long_without_a_fresh_scan() -> None:
    inp = gate_inputs(risk_flags=flags(age_s=2 * CYCLE + 5))
    res = evaluate(inp, gate_config(), NOW)
    assert res.verdict == "rejected"
    assert res.reason == "veto_stale"
    assert res.intents == ()


def test_gate_sizes_a_short_at_half_without_a_fresh_scan() -> None:
    pkt = packet(candidate_set())
    short_d = decision(side=Side.SHORT, p=0.35, candidate_id=CID_SHORT, packet_sha256=pkt.packet_sha256)
    stale = gate_inputs(short_d, pkt=pkt, risk_flags=flags(age_s=10 * CYCLE))
    res = evaluate(stale, gate_config(), NOW)
    assert res.verdict == "approved"
    assert res.sizing is not None
    assert res.sizing["flags_size_mult"] == 0.5


@pytest.mark.parametrize("age", [31.0, None])
def test_gate_rejects_open_on_stale_account_state_and_alerts(age: float | None) -> None:
    inp = gate_inputs(bk=book(state_age_s=age))
    res = evaluate(inp, gate_config(), NOW)
    assert res.reason == "account_state_stale"
    assert res.alert is not None
    assert res.alert.kind == "account_state_stale"
    assert res.intents == ()


def test_gate_rejects_open_on_stale_binance_data() -> None:
    for mkt in (market(mark_age_s=181), market(book_age_s=181), market(book_age_s=None)):
        res = evaluate(gate_inputs(mkt=mkt), gate_config(), NOW)
        assert res.reason == "stale_data"


def test_gate_active_kill_state_blocks_open_but_exit_still_passes() -> None:
    killed = KillView("killed", "reconcile", "mismatch", NOW - timedelta(minutes=5))
    assert evaluate(gate_inputs(bk=book(kill=killed)), gate_config(), NOW).reason == "kill_state"
    held = {"SOLUSDT": HeldPosition(Side.LONG, position().qty)}
    exit_d = decision(intent=Intent.EXIT, candidate_id=None)
    inp = with_decision(
        gate_inputs(
            bk=book(
                kill=killed,
                held=held,
                state_age_s=None,
                state=account_state(positions=(position(),)),
            ),
            risk_flags=flags(age_s=10 * CYCLE, veto_long=True),
            mkt=market(mark_age_s=None, book_age_s=None),
        ),
        exit_d,
    )
    res = evaluate(inp, gate_config(), NOW)
    assert res.verdict == "approved"
    assert res.effective_intent is Intent.EXIT
    assert [i.leg.value for i in res.intents] == ["exit"]
