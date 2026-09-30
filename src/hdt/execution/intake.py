"""Intake of stream `orders` for one namespace (consumer group `exec-{account}`).

All namespaces share the stream; each group only acts on messages whose `account` is its own (the account
is inside the signed bytes, so re-labelling a paper intent as live breaks its signature). Per message:
1. parse as `OrderIntent`, then verify the signature: valid, `key_id` known and allowed for this namespace
   (`verify_intent`). A malformed or unverified payload is rejected with a Critical alert (one per stream
   message) and written to `audit_log` (keyed by its stream message id, the payload stripped of NUL and
   capped); it never touches `processed_intents`, so it can neither burn a real intent's replay key nor
   re-drive a recorded one;
2. replay: an `intent_id` already recorded for this namespace (`processed_intents` is keyed by
   `(account, intent_id)`) is not processed again. The duplicate must carry exactly the recorded signed
   bytes (else rejected like an unverified payload, the recorded row untouched); an `accepted` one is
   re-driven from the recorded payload only while its action is provably unfinished (`_unfinished`: no
   order row for its client id yet, or a `PENDING` one), so a stale re-delivery never re-places an old
   stop or re-initialises a position's protective plan;
3. a namespace the run mode no longer enables (attached only for its exposure) refuses new positions:
   entry and entry_ioc legs are rejected and audited;
4. execution's own hard limits (`hdt.execution.limits`) and the protective-leg geometry of entries (a
   refusal raises a warning per intent);
5. recorded `accepted` and handed to the order manager.
`XACK` only after the durable write (intent row or audit entry): the handler returns True after it.
"""

from __future__ import annotations

import json
import logging
import math
import uuid
from decimal import Decimal
from typing import Any, Final

from pydantic import ValidationError

from hdt.contracts.common import Leg
from hdt.contracts.order import OrderIntent
from hdt.core.streams import RawPayload
from hdt.db.session import transaction
from hdt.execution.limits import check_intent, check_protective_geometry
from hdt.execution.order_manager import OrderManager, limit_context
from hdt.execution.runtime import SERVICE, Namespace
from hdt.execution.verify import Keyring, verify_intent

log = logging.getLogger(__name__)

AUDIT_ACTION: Final[str] = "intent_rejected"
AUDIT_SECTION: Final[str] = "orders"
AUDIT_PAYLOAD_MAX: Final[int] = 16_384
"""Characters of an untrusted payload kept in `audit_log` (JSON text); a longer one is stored truncated."""
AUDIT_DEPTH_MAX: Final[int] = 16
AUDIT_TEXT_MAX: Final[int] = 256
"""Characters kept of the payload's `intent_id` in the audit entry."""
PENDING: Final[str] = "PENDING"
OPEN_LEGS: Final[frozenset[Leg]] = frozenset({Leg.ENTRY, Leg.ENTRY_IOC})
PLAN_LEGS: Final[frozenset[Leg]] = frozenset({Leg.SL, Leg.TP1, Leg.TRAIL})


def _audit_safe(value: Any, depth: int = 0) -> Any:
    """`value` storable in JSONB: NUL characters removed, unencodable surrogates replaced, non-finite floats
    as text, nesting bounded (an untrusted payload must never make its own audit write fail)."""
    if isinstance(value, str):
        return value.replace("\x00", "").encode("utf-8", "replace").decode("utf-8")
    if isinstance(value, bool) or value is None or isinstance(value, int):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if depth >= AUDIT_DEPTH_MAX:
        return "<nested too deep>"
    if isinstance(value, dict):
        return {_audit_safe(str(k)): _audit_safe(v, depth + 1) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [_audit_safe(v, depth + 1) for v in value]
    return _audit_safe(str(value))


def audit_payload(payload: Any) -> Any:
    """The sanitized payload, or its first `AUDIT_PAYLOAD_MAX` characters of JSON text when longer."""
    safe = _audit_safe(payload)
    text = json.dumps(safe, ensure_ascii=False, separators=(",", ":"))
    if len(text) <= AUDIT_PAYLOAD_MAX:
        return safe
    return {"truncated": True, "chars": len(text), "head": text[:AUDIT_PAYLOAD_MAX]}


class Intake:
    def __init__(self, ns: Namespace, keyring: Keyring, manager: OrderManager) -> None:
        self.ns = ns
        self.keyring = keyring
        self.manager = manager

    async def handle(self, msg_id: str, raw: RawPayload) -> bool:
        payload = raw.root
        if payload.get("account") != self.ns.name:
            return True  # another namespace's intent
        async with self.ns.lock:
            await self.process(payload, msg_id=msg_id)
        return True

    async def process(self, payload: dict[str, Any], *, msg_id: str | None = None) -> str:
        """Returns the resulting status (`accepted`, `rejected`, `void`, `duplicate`). Lock held."""
        ns = self.ns
        try:
            intent = OrderIntent.model_validate(payload)
        except ValidationError as exc:
            self._reject_untrusted(
                payload,
                msg_id,
                f"malformed: {exc.error_count()} errors",
                f"{ns.name}: malformed OrderIntent rejected",
            )
            return "rejected"
        verdict = verify_intent(intent, self.keyring, ns.account)
        if not verdict.ok:
            self._reject_untrusted(
                payload,
                msg_id,
                verdict.reason or "unverified",
                f"{ns.name}: OrderIntent {intent.leg.value} {intent.symbol} rejected ({verdict.reason})",
            )
            return "rejected"

        recorded = ns.ledger.intent_row(intent.intent_id)
        if recorded is not None:
            stored = OrderIntent.model_validate(recorded.payload)
            if stored.signing_bytes() != intent.signing_bytes():
                self._reject_untrusted(
                    payload,
                    msg_id,
                    "intent_id_reused",
                    f"{ns.name}: signed OrderIntent reuses recorded intent_id {intent.intent_id} "
                    "with different content",
                )
                return "rejected"
            if recorded.status == "accepted" and self._unfinished(stored):
                log.info(
                    "re-driving an interrupted intent",
                    extra={"account": ns.name, "intent_id": stored.intent_id, "leg": stored.leg.value},
                )
                await self.manager.act(stored)
            return "duplicate"

        now = ns.clock()
        if (
            intent.leg in (Leg.ENTRY, Leg.ENTRY_IOC, Leg.HEDGE)
            and intent.expires_at
            and now > intent.expires_at
        ):
            ns.ledger.record_intent(intent, status="void", reason="expired on arrival")
            return "void"

        held = ns.held_reason() if intent.leg in OPEN_LEGS else None
        if held is not None:
            ns.ledger.record_intent(intent, status="rejected", reason=held)
            log.warning(
                "new position refused on a held namespace",
                extra={"account": ns.name, "intent_id": intent.intent_id, "leg": intent.leg.value},
            )
            self._audit_rejection(payload, msg_id, held)
            return "rejected"

        problem = await self._limits(intent)
        if problem is not None:
            ns.ledger.record_intent(intent, status="rejected", reason=problem)
            if not problem.startswith("namespace"):
                ns.alert(
                    "intent_signature",
                    "warning",
                    f"{ns.name}: signed {intent.leg.value} {intent.symbol} refused by execution limits",
                    problem,
                    episode=intent.intent_id,
                )
            return "rejected"
        if not ns.ledger.record_intent(intent, status="accepted"):
            return "duplicate"
        await self.manager.act(intent)
        return "accepted"

    def _unfinished(self, intent: OrderIntent) -> bool:
        """Whether the action of a recorded `accepted` intent may have been interrupted (a crash between the
        intent row and the exchange request), so re-driving it is safe and needed.

        - entry, exit, hedge and the hedge-book stop send exactly one exchange order under the intent's
          client id: unfinished while that order has no row yet, or only a `PENDING` one (the request may
          not have reached the exchange; the order manager re-checks and re-sends idempotently);
        - an sl / tp1 / trail plan leg is copied onto the position of its event: unfinished only while that
          position is open without the leg (a later re-drive would reset a trailed or tightened stop);
        - entry_ioc is the entry's fallback plan, read by the entry watchdog: never re-driven."""
        ledger = self.ns.ledger
        if intent.leg is Leg.ENTRY_IOC:
            return False
        if intent.leg is Leg.SL and intent.close_position:
            algo = ledger.algo(intent.client_id)
            return algo is None or algo.status == PENDING
        if intent.leg in PLAN_LEGS:
            pos = ledger.position(intent.symbol)
            if pos is None or pos.event_id != intent.event_id:
                return False  # the fill that opens the position copies the plan from the ledger
            if intent.leg is Leg.SL:
                return pos.initial_stop_price is None
            if intent.leg is Leg.TP1:
                return pos.tp1_price is None and not pos.tp1_done
            return pos.trail_distance is None
        order = ledger.order(intent.client_id)
        return order is None or order.status == PENDING

    def _reject_untrusted(self, payload: dict[str, Any], msg_id: str | None, reason: str, title: str) -> None:
        """Audit + Critical alert for a payload execution cannot trust; nothing else is written.

        The audit write is not guarded: if it fails the message stays un-acked and is retried. The alert
        episode is the stream message, so every forged or malformed message pages (never deduplicated
        into an earlier, possibly routine, `intent_signature` alert)."""
        self._write_audit(payload, msg_id, reason)
        self.ns.alert(
            "intent_signature",
            "critical",
            title,
            f"{reason} (stream message {msg_id or 'n/a'})",
            episode=msg_id if msg_id is not None else uuid.uuid4().hex,
        )

    def _audit_rejection(self, payload: dict[str, Any], msg_id: str | None, reason: str) -> None:
        """Audit entry of a verified intent refused by a namespace rule; the intent row already records the
        refusal, so an audit failure is only logged."""
        try:
            self._write_audit(payload, msg_id, reason)
        except Exception:
            log.exception("intent refusal audit entry could not be written", extra={"account": self.ns.name})

    def _write_audit(self, payload: dict[str, Any], msg_id: str | None, reason: str) -> None:
        from hdt.configapi.audit import write_audit

        ns = self.ns
        with transaction(ns.sessions) as s:
            write_audit(
                s,
                user=SERVICE,
                ip=None,
                action=AUDIT_ACTION,
                section=AUDIT_SECTION,
                diff={
                    "account": ns.name,
                    "msg_id": msg_id,
                    "intent_id": _audit_safe(str(payload.get("intent_id") or ""))[:AUDIT_TEXT_MAX],
                    "reason": reason,
                    "payload": audit_payload(payload),
                },
            )

    async def _limits(self, intent: OrderIntent) -> str | None:
        ns = self.ns
        ledger = ns.ledger
        mark: Decimal | None = None
        if intent.leg is Leg.HEDGE:
            mark = (await ns.adapter.mark_prices([intent.symbol])).get(intent.symbol)
        ctx = limit_context(
            ns,
            intent,
            ns.clock(),
            entries_for_event=ledger.count_event_leg(intent.event_id, intent.leg)
            if intent.leg in (Leg.ENTRY, Leg.ENTRY_IOC)
            else 0,
        )
        result = check_intent(intent, ctx, mark)
        if not result.ok:
            return result.reason
        if intent.leg in (Leg.ENTRY, Leg.ENTRY_IOC):
            for leg in (Leg.SL, Leg.TP1):
                protective = ledger.event_leg(intent.event_id, leg)
                if protective is not None:
                    problem = check_protective_geometry(intent, protective)
                    if problem is not None:
                        return problem
        return None
