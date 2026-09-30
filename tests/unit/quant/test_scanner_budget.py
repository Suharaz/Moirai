"""Scanner grid, event budget and account-state selection."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from hdt.contracts.account import AccountState, PositionState
from hdt.contracts.common import Account, CandidateSource, TargetType
from hdt.core.config import scanner_config
from hdt.core.streams import encode
from hdt.quant.scanner import Evaluation, apply_budget, grid_as_of, latest_account_state

T0 = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)


def _ev(coin_id: int, klass: str | None, score: float | None) -> Evaluation:
    return Evaluation(
        coin_id=coin_id,
        rule=CandidateSource.HELD if klass == "held" else CandidateSource.LTX,
        side="LONG",
        conditions={},
        strict_pass=klass in ("strict", "held"),
        loose_pass=klass is not None,
        contagion_blocked=False,
        score=score,
        target_type=TargetType.RESID_12H,
        emit_class=klass,  # type: ignore[arg-type]
    )


def test_grid_is_fixed_and_idempotent_within_a_slot() -> None:
    cfg = scanner_config()
    slot = grid_as_of(T0 + timedelta(seconds=cfg.run_delay_s + 1), cfg)
    assert grid_as_of(slot + timedelta(seconds=cfg.cadence_s - 1), cfg) == slot
    assert grid_as_of(slot + timedelta(seconds=cfg.cadence_s), cfg) == slot + timedelta(seconds=cfg.cadence_s)
    assert grid_as_of(slot, cfg) == slot


def test_budget_never_drops_held_and_drops_superset_before_strict() -> None:
    held = _ev(1, "held", None)
    strict_hi, strict_lo = _ev(2, "strict", 9.0), _ev(3, "strict", 5.0)
    superset = _ev(4, "superset", 50.0)
    evaluations = [superset, strict_lo, held, strict_hi]
    apply_budget(evaluations, emitted_today=0, budget=2)
    assert held.emitted
    assert strict_hi.emitted
    assert strict_lo.dropped_budget
    assert superset.dropped_budget
    assert not any(ev.emitted and ev.dropped_budget for ev in evaluations)


def test_budget_exhausted_earlier_in_the_day_still_emits_held() -> None:
    held, strict = _ev(1, "held", None), _ev(2, "strict", 9.0)
    apply_budget([held, strict], emitted_today=24, budget=24)
    assert held.emitted
    assert strict.dropped_budget


def _state(account: Account, symbol: str) -> AccountState:
    return AccountState(
        account=account,
        equity=Decimal(1000),
        available=Decimal(900),
        day_start_equity=Decimal(1000),
        positions=(
            PositionState(
                symbol=symbol,
                qty=Decimal(1),
                entry_price=Decimal(10),
                mark_price=Decimal(10),
                unrealized_pnl=Decimal(0),
                leverage=2,
                margin_type="ISOLATED",
            ),
        ),
        open_orders=(),
        open_algo_orders=(),
        ts=T0,
    )


def test_latest_account_state_picks_the_running_namespace_and_skips_garbage() -> None:
    entries = [
        ("3-0", {b"data": b"not json"}),
        ("2-0", {k.encode(): v.encode() for k, v in encode(_state(Account.LIVE, "AUSDT")).items()}),
        ("1-0", encode(_state(Account.PAPER, "BUSDT"))),
    ]
    paper = latest_account_state(entries, Account.PAPER)
    assert paper is not None
    assert paper.positions[0].symbol == "BUSDT"
    live = latest_account_state(entries, Account.LIVE)
    assert live is not None
    assert live.positions[0].symbol == "AUSDT"
    assert latest_account_state(entries[:1], Account.PAPER) is None
