"""Execution intake (phase 09 section 5): every intent is verified, recorded once and alerted on refusal.

Success criterion: unsigned, wrong signature, or a `paper` intent sent into `live` is rejected + alert; a
signed intent exceeding Execution's own ceiling is rejected; nothing reaches the exchange in either case.
The replay key is `(account, intent_id)` and only a verified intent may hold it: an untrusted payload is
audited and never re-drives, shadows or burns a recorded intent (review cycle 1, C1 proofs 1-3). A
recorded `accepted` duplicate is re-driven only while its action is unfinished (review cycle 2, I1); a
namespace held only for its exposure refuses new positions (m8); any untrusted payload can be audited
(m7); a routine refusal never masks the critical alert of a forged intent (review cycle 2 P10, C3-R).
"""

from __future__ import annotations

import logging
import time
import uuid
from datetime import timedelta
from decimal import Decimal

import pytest
import sqlalchemy as sa
from sqlalchemy import Engine

from alert_logs import raised as _raised
from fake_exchange import SOL, Harness, ledger_sessions, make_harness, open_position
from hdt.contracts.common import Account, Leg, Side
from hdt.contracts.order import OrderIntent
from hdt.db.models.ledger import ProcessedIntentRow
from hdt.db.models.ops import AlertRow
from hdt.db.models.settings import AuditLogRow
from hdt.execution import actions
from hdt.execution.intake import AUDIT_ACTION, AUDIT_PAYLOAD_MAX
from hdt.risk.gate import intent_id_for
from intent_builders import KEYRING, RISK_SIGNER, exit_intent, open_plan

pytestmark = [pytest.mark.pg, pytest.mark.integration]


@pytest.fixture(autouse=True)
def _no_retry_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(actions, "CANCEL_CONFIRM_BACKOFF_S", 0.0)
    monkeypatch.setattr(actions, "UNKNOWN_VERIFY_DELAY_S", 0.0)


async def _harness(pg_engine: Engine, account: Account) -> Harness:
    h = make_harness(ledger_sessions(pg_engine), account, KEYRING)
    await h.start()
    return h


def _plan(
    h: Harness,
    account: Account | None = None,
    *,
    qty: str = "9",
    sign: bool = True,
    event_id: str = "evt-in",
    stop: str = "98.00",
    ttl: timedelta | None = None,
) -> list[OrderIntent]:
    return open_plan(
        account=account or h.ns.account,
        event_id=event_id,
        symbol=SOL,
        side=Side.LONG,
        qty=Decimal(qty),
        entry=Decimal("100.00"),
        stop=Decimal(stop),
        tp1=Decimal("104.00"),
        ioc_price=Decimal("100.10"),
        now=h.clock(),
        sign=sign,
        ttl=ttl,
    )


def _row(h: Harness, intent_id: str) -> ProcessedIntentRow | None:
    with h.ns.sessions() as s:
        return s.get(ProcessedIntentRow, (h.ns.name, intent_id))


def _audit_mark(h: Harness) -> int:
    with h.ns.sessions() as s:
        return int(s.scalar(sa.select(sa.func.coalesce(sa.func.max(AuditLogRow.id), 0))) or 0)


def _rejections(h: Harness, since_id: int) -> list[tuple[str, str]]:
    """(intent_id, reason) of the untrusted payloads this namespace audited after `since_id`."""
    with h.ns.sessions() as s:
        rows = s.scalars(
            sa.select(AuditLogRow)
            .where(AuditLogRow.id > since_id, AuditLogRow.action == AUDIT_ACTION)
            .order_by(AuditLogRow.id)
        ).all()
    return [
        (r.diff_redacted["intent_id"], r.diff_redacted["reason"])
        for r in rows
        if r.diff_redacted["account"] == h.ns.name
    ]


def _alerts(caplog: pytest.LogCaptureFixture) -> list[tuple[str, int]]:
    return [(r.alert_kind, r.levelno) for r in caplog.records if _raised(r)]  # type: ignore[attr-defined]


async def test_unsigned_intent_is_audited_rejected_and_never_takes_the_replay_key(
    pg_engine: Engine, caplog: pytest.LogCaptureFixture
) -> None:
    h = await _harness(pg_engine, Account.PAPER)
    mark = _audit_mark(h)
    unsigned = _plan(h, sign=False)[0]
    assert await h.send([unsigned]) == ["rejected"]
    assert _row(h, unsigned.intent_id) is None
    assert _rejections(h, mark) == [(unsigned.intent_id, "unsigned")]
    assert _alerts(caplog) == [("intent_signature", logging.CRITICAL)]
    assert h.exchange.calls == []


async def test_tampered_intent_is_rejected_and_the_genuine_plan_still_goes_through(
    pg_engine: Engine, caplog: pytest.LogCaptureFixture
) -> None:
    h = await _harness(pg_engine, Account.PAPER)
    mark = _audit_mark(h)
    plan = _plan(h)
    tampered = plan[-1].model_copy(update={"qty": Decimal("90")})
    assert await h.send([tampered]) == ["rejected"]
    assert _rejections(h, mark) == [(tampered.intent_id, "bad_signature")]
    assert h.exchange.calls == []
    assert await h.send(plan) == ["accepted"] * 5  # the tampered copy burnt nothing
    assert _alerts(caplog) == [("intent_signature", logging.CRITICAL)]


async def test_paper_intent_sent_into_live_is_rejected_with_a_critical_alert(
    pg_engine: Engine, caplog: pytest.LogCaptureFixture
) -> None:
    live = await _harness(pg_engine, Account.LIVE)
    mark = _audit_mark(live)
    paper_sl = _plan(live, Account.PAPER)[0]
    assert await live.intake.process(paper_sl.model_dump(mode="json")) == "rejected"
    relabelled = OrderIntent.model_validate(
        {
            **paper_sl.model_dump(),
            "account": Account.LIVE,
            "intent_id": intent_id_for(Account.LIVE, paper_sl.event_id, paper_sl.leg, paper_sl.seq),
        }
    )
    assert await live.send([relabelled]) == ["rejected"]
    assert _rejections(live, mark) == [
        (paper_sl.intent_id, "wrong_account"),
        (relabelled.intent_id, "bad_signature"),
    ]
    assert _row(live, relabelled.intent_id) is None
    assert _alerts(caplog) == [("intent_signature", logging.CRITICAL)] * 2
    assert live.exchange.calls == []


async def test_signed_entry_over_executions_own_ceiling_is_rejected(
    pg_engine: Engine, caplog: pytest.LogCaptureFixture
) -> None:
    h = await _harness(pg_engine, Account.PAPER)
    plan = _plan(h, qty="10.5")  # 1 050 USD > 10% of the 10 000 equity
    assert await h.send(plan) == ["accepted", "accepted", "accepted", "rejected", "rejected"]
    row = _row(h, plan[-1].intent_id)
    assert row is not None
    assert "execution cap" in (row.reason or "")
    assert ("intent_signature", logging.WARNING) in _alerts(caplog)
    assert h.exchange.mutating_calls("place_order") == []


async def test_expired_entry_is_void_on_arrival(pg_engine: Engine) -> None:
    h = await _harness(pg_engine, Account.PAPER)
    plan = _plan(h, ttl=timedelta(minutes=10))  # an entry expiry is set only by the testnet driver
    h.clock.advance(11 * 60)
    assert await h.send(plan) == ["accepted", "accepted", "accepted", "void", "void"]
    assert h.exchange.mutating_calls("place_order") == []


async def test_malformed_intent_is_audited_rejected_with_a_critical_alert(
    pg_engine: Engine, caplog: pytest.LogCaptureFixture
) -> None:
    h = await _harness(pg_engine, Account.PAPER)
    mark = _audit_mark(h)
    payload = {**_plan(h)[-1].model_dump(mode="json"), "qty": "-1"}
    assert await h.intake.process(payload, msg_id="1-0") == "rejected"
    assert _row(h, payload["intent_id"]) is None
    [(intent_id, reason)] = _rejections(h, mark)
    assert intent_id == payload["intent_id"]
    assert reason.startswith("malformed")
    assert _alerts(caplog) == [("intent_signature", logging.CRITICAL)]


async def test_accepted_plan_is_acted_on_exactly_once(pg_engine: Engine) -> None:
    h = await _harness(pg_engine, Account.PAPER)
    plan = _plan(h)
    assert await h.send(plan) == ["accepted"] * 5
    assert await h.send(plan) == ["duplicate"] * 5
    entries = [c for c in h.exchange.mutating_calls("place_order") if c.endswith(plan[-1].client_id)]
    assert entries == [f"place_order:{plan[-1].client_id}"]


async def test_unsigned_payload_reusing_an_accepted_intent_id_is_not_re_driven(
    pg_engine: Engine, caplog: pytest.LogCaptureFixture
) -> None:
    """C1 proof 1: an unsigned entry under the accepted `sl` intent_id must not reach the venue."""
    h = await _harness(pg_engine, Account.PAPER)
    plan = _plan(h)
    assert await h.send(plan) == ["accepted"] * 5
    forged = _plan(h, sign=False, event_id="evt-forged")[-1].model_copy(
        update={"intent_id": plan[0].intent_id}
    )
    assert await h.intake.process(forged.model_dump(mode="json")) == "rejected"
    assert not [c for c in h.exchange.calls if forged.client_id in c]
    row = _row(h, plan[0].intent_id)
    assert row is not None
    assert (row.leg, row.status) == (Leg.SL.value, "accepted")  # the recorded sl is untouched
    assert _alerts(caplog) == [("intent_signature", logging.CRITICAL)]


async def test_intent_id_recorded_by_another_namespace_is_not_a_replay_here(
    pg_engine: Engine, caplog: pytest.LogCaptureFixture
) -> None:
    """C1 proof 2: a testnet intent_id replayed into `live` in an unsigned stop must place nothing."""
    sessions = ledger_sessions(pg_engine)
    testnet = make_harness(sessions, Account.TESTNET, KEYRING)
    live = make_harness(sessions, Account.LIVE, KEYRING)
    await testnet.start()
    await live.start()
    testnet_sl = _plan(testnet)[0]  # signed by the testnet driver key
    assert await testnet.send([testnet_sl]) == ["accepted"]
    forged_stop = _plan(live, sign=False, event_id="evt-forged", stop="1.00")[0].model_copy(
        update={"intent_id": testnet_sl.intent_id}
    )
    assert await live.intake.process(forged_stop.model_dump(mode="json")) == "rejected"
    assert live.exchange.calls == []
    assert _row(live, testnet_sl.intent_id) is None
    assert _alerts(caplog) == [("intent_signature", logging.CRITICAL)]


async def test_unsigned_copy_sent_first_does_not_burn_the_genuine_intent(
    pg_engine: Engine, caplog: pytest.LogCaptureFixture
) -> None:
    """C1 proof 3: an unsigned EXIT under the victim's intent_id must not turn the real EXIT into a
    duplicate."""
    h = await _harness(pg_engine, Account.PAPER)
    real = exit_intent(
        account=Account.PAPER,
        event_id="evt-in",
        symbol=SOL,
        held_side=Side.LONG,
        qty=Decimal("9"),
        now=h.clock(),
    )
    poison = real.model_copy(update={"signature": ""})
    assert await h.intake.process(poison.model_dump(mode="json")) == "rejected"
    assert await h.send([real]) == ["accepted"]  # not a duplicate of the rejected copy
    row = _row(h, real.intent_id)
    assert row is not None
    assert row.leg == Leg.EXIT.value
    assert _alerts(caplog) == [("intent_signature", logging.CRITICAL)]


async def test_signed_intent_reusing_a_recorded_id_with_other_content_is_rejected(
    pg_engine: Engine, caplog: pytest.LogCaptureFixture
) -> None:
    h = await _harness(pg_engine, Account.PAPER)
    mark = _audit_mark(h)
    plan = _plan(h)
    assert await h.send(plan) == ["accepted"] * 5
    before = _row(h, plan[-1].intent_id)
    assert before is not None
    resigned = RISK_SIGNER.sign(plan[-1].model_copy(update={"qty": Decimal("5"), "signature": ""}))
    assert await h.send([resigned]) == ["rejected"]
    after = _row(h, plan[-1].intent_id)
    assert after is not None
    assert (after.status, after.reason, after.payload) == (before.status, before.reason, before.payload)
    assert _rejections(h, mark) == [(plan[-1].intent_id, "intent_id_reused")]
    places = [c for c in h.exchange.mutating_calls("place_order") if c.endswith(plan[-1].client_id)]
    assert places == [f"place_order:{plan[-1].client_id}"]
    assert _alerts(caplog) == [("intent_signature", logging.CRITICAL)]


def _tag() -> str:
    return uuid.uuid4().hex[:10]


def _open_alerts(h: Harness, kind: str) -> dict[str, str]:
    """dedupe_key -> severity of the open `kind` alerts (the outbox table is shared by the whole run)."""
    with h.ns.sessions() as s:
        rows = s.execute(
            sa.select(AlertRow.dedupe_key, AlertRow.severity).where(
                AlertRow.kind == kind, AlertRow.resolved_at.is_(None), AlertRow.is_test.is_(False)
            )
        ).all()
    return {key: severity for key, severity in rows}


async def test_a_routine_refusal_never_masks_the_critical_alert_of_a_forged_intent(pg_engine: Engine) -> None:
    """Review cycle 2 (P10) C3-R: limit refusals page per intent, forged messages per stream message."""
    h = await _harness(pg_engine, Account.PAPER)
    tag = _tag()
    first = _plan(h, qty="10.5", event_id=f"evt-cap1-{tag}")
    second = _plan(h, qty="10.5", event_id=f"evt-cap2-{tag}")
    assert (await h.send(first))[-1] == "rejected"
    assert (await h.send(second))[-1] == "rejected"
    forged = _plan(h, sign=False, event_id=f"evt-forged-{tag}")[0]
    msg_id = f"{time.time_ns()}-0"
    assert await h.intake.process(forged.model_dump(mode="json"), msg_id=msg_id) == "rejected"
    alerts = _open_alerts(h, "intent_signature")
    assert alerts[f"paper:{first[-1].intent_id}"] == "warning"
    assert alerts[f"paper:{second[-1].intent_id}"] == "warning"
    assert alerts[f"paper:{msg_id}"] == "critical"


async def test_an_accepted_entry_interrupted_before_its_order_is_re_driven_once(pg_engine: Engine) -> None:
    h = await _harness(pg_engine, Account.PAPER)
    plan = _plan(h, event_id=f"evt-crash-{_tag()}")
    assert await h.send(plan[:-1]) == ["accepted"] * 4
    entry = plan[-1]
    assert h.ledger.record_intent(entry, status="accepted")  # the process died before acting on it
    assert await h.send([entry]) == ["duplicate"]  # re-delivered: no order row yet, re-driven
    assert await h.send([entry]) == ["duplicate"]  # the order exists now: nothing more
    places = [c for c in h.exchange.mutating_calls("place_order") if c.endswith(entry.client_id)]
    assert places == [f"place_order:{entry.client_id}"]


async def test_a_redelivered_stop_leg_never_resets_the_trailed_stop(pg_engine: Engine) -> None:
    """Review cycle 2 I1 (regular legs): re-driving an old sl / tp1 / trail re-ran `_init_plan`."""
    h = await _harness(pg_engine, Account.PAPER)
    plan = await open_position(h, event_id=f"evt-trail-{_tag()}")
    pos = h.ledger.position(SOL)
    assert pos is not None
    assert pos.stop_price == Decimal("98.00")
    h.ledger.update_position(pos.position_id, stop_price=Decimal("99.50"))  # trailed since
    calls = len(h.exchange.calls)
    assert await h.send(plan[:3]) == ["duplicate"] * 3
    pos = h.ledger.position(SOL)
    assert pos is not None
    assert pos.stop_price == Decimal("99.50")
    assert h.exchange.calls[calls:] == []


async def test_a_redelivered_exit_whose_close_was_sent_never_touches_a_newer_position(
    pg_engine: Engine,
) -> None:
    """Review cycle 2 I1: the recorded EXIT stayed `accepted` (crash before its status update) and is
    re-delivered after a new position opened on the same symbol: it must neither close the new position
    nor mark it as exiting (which would stop its exit management)."""
    h = await _harness(pg_engine, Account.PAPER)
    tag = _tag()
    await open_position(h, event_id=f"evt-a-{tag}")
    exit_ = exit_intent(
        account=Account.PAPER,
        event_id=f"evt-x-{tag}",
        symbol=SOL,
        held_side=Side.LONG,
        qty=Decimal("9"),
        now=h.clock(),
    )
    assert await h.send([exit_]) == ["accepted"]
    await h.pump()
    assert h.ledger.position(SOL) is None
    h.ledger.set_intent_status(exit_.intent_id, "accepted")
    await open_position(h, event_id=f"evt-b-{tag}")
    assert await h.send([exit_]) == ["duplicate"]
    await h.pump()
    pos = h.ledger.position(SOL)
    assert pos is not None
    assert (pos.qty, pos.exit_in_progress, pos.exit_reason) == (Decimal("9"), False, None)
    closes = [c for c in h.exchange.mutating_calls("place_order") if c.endswith(exit_.client_id)]
    assert closes == [f"place_order:{exit_.client_id}"]


async def test_a_namespace_held_only_for_its_exposure_refuses_new_entries_and_audits_them(
    pg_engine: Engine, caplog: pytest.LogCaptureFixture
) -> None:
    """Review cycle 2 m8: the run mode left `testnet`; it stays attached for its position only."""
    h = await _harness(pg_engine, Account.TESTNET)
    enabled = False
    h.ns.run_mode_enabled = lambda: enabled
    held = h.ns.held_reason()
    assert held is not None
    mark = _audit_mark(h)
    plan = _plan(h, event_id=f"evt-held-{_tag()}")
    assert await h.send(plan) == ["accepted"] * 3 + ["rejected"] * 2
    row = _row(h, plan[-1].intent_id)
    assert row is not None
    assert row.reason == held
    assert _rejections(h, mark) == [(plan[-2].intent_id, held), (plan[-1].intent_id, held)]
    assert h.exchange.mutating_calls("place_order") == []
    assert _alerts(caplog) == []  # a namespace rule, not a signature or limit problem
    enabled = True  # the run mode enables testnet again (the testnet driver trades in testnet mode)
    assert await h.send(_plan(h, event_id=f"evt-back-{_tag()}")) == ["accepted"] * 5


async def test_untrusted_payloads_that_jsonb_cannot_store_are_still_audited(pg_engine: Engine) -> None:
    """Review cycle 2 m7: NUL characters, NaN and a huge payload must not make the audit write fail."""
    h = await _harness(pg_engine, Account.PAPER)
    mark = _audit_mark(h)
    unsigned = _plan(h, sign=False, event_id=f"evt-nul-{_tag()}")[0].model_dump(mode="json")
    odd = {**unsigned, "symbol": "SOL\u0000USDT", "note\u0000": float("nan")}
    assert await h.intake.process(odd, msg_id=f"{time.time_ns()}-0") == "rejected"
    huge = {**unsigned, "blob": "x" * (AUDIT_PAYLOAD_MAX * 3)}
    assert await h.intake.process(huge, msg_id=f"{time.time_ns()}-1") == "rejected"
    with h.ns.sessions() as s:
        rows = s.scalars(
            sa.select(AuditLogRow)
            .where(AuditLogRow.id > mark, AuditLogRow.action == AUDIT_ACTION)
            .order_by(AuditLogRow.id)
        ).all()
    stored = [r.diff_redacted["payload"] for r in rows]
    assert len(stored) == 2
    assert stored[0]["symbol"] == "SOLUSDT"
    assert stored[0]["note"] == "nan"
    assert stored[1]["truncated"] is True
    assert stored[1]["chars"] > AUDIT_PAYLOAD_MAX * 3
    assert len(stored[1]["head"]) < AUDIT_PAYLOAD_MAX + 64  # the head only (log redaction may mark it)
