"""Operational alerts raised by the trading services (risk, execution).

Alerts are a Postgres outbox owned by phase 10 (`hdt.ops.alerts.raise_alert`, table `alerts`): the
caller writes the alert inside its own transaction and the Telegram bot delivers it. When that module is
not installed in the running image the alert is still emitted as a structured log line at the matching
level, so no alert is ever silently dropped. Every alert is logged either way, once: this module owns the
log record of the alerts and resolutions it forwards (the outbox is called with `emit_log=False`).
"""

from __future__ import annotations

import importlib
import logging
from typing import Any, Final, Literal

from sqlalchemy.orm import Session

log = logging.getLogger(__name__)

Severity = Literal["critical", "warning", "info"]
AlertKind = Literal[
    "reconcile_mismatch",
    "stop_invariant",
    "kill_switch",
    "daily_loss_warning",
    "intent_signature",
    "integrity_error",
    "account_state_stale",
    "price_crosscheck",
    "ip_banned",
    "liquidation_distance",
    "namespace_error",
    "config_invalid",
    "entry_refused",
    "claims_audit",
    "council_event_failed",
]
_LEVELS: Final[dict[str, int]] = {
    "critical": logging.CRITICAL,
    "warning": logging.WARNING,
    "info": logging.INFO,
}
_OPS_MODULE: Final[str] = "hdt.ops.alerts"


def _ops_function(name: str) -> Any | None:
    try:
        module = importlib.import_module(_OPS_MODULE)
    except ModuleNotFoundError as exc:
        if exc.name not in (_OPS_MODULE, "hdt.ops"):
            raise
        return None
    return getattr(module, name, None)


def dedupe_key(account: str | None, episode: str | None) -> str:
    """Outbox dedupe key: one open alert per `(kind, account)`, or per `(kind, account, episode)`."""
    return (account or "") if episode is None else f"{account or ''}:{episode}"


def alert(
    session: Session | None,
    *,
    kind: AlertKind,
    severity: Severity,
    title: str,
    detail: str | None = None,
    account: str | None = None,
    service: str,
    episode: str | None = None,
) -> str | None:
    """Raise one alert, deduplicated per open `(kind, account[, episode])` by the outbox.

    `episode` scopes the dedupe key to one occurrence (for example the UTC date of a daily-loss warning),
    so a later episode pages again even while an earlier one is still open. Returns the alert id, or
    `None` when the same alert is already open or the outbox is unavailable.
    """
    key = dedupe_key(account, episode)
    log.log(
        _LEVELS[severity],
        "alert: %s",
        title,
        extra={
            "alert_kind": kind,
            "severity": severity,
            "account": account,
            "detail": detail,
            "dedupe_key": key,
        },
    )
    raise_alert = _ops_function("raise_alert")
    if raise_alert is None or session is None:
        return None
    result = raise_alert(
        session,
        kind=kind,
        severity=severity,
        title=title,
        detail=detail,
        dedupe_key=key,
        service=service,
        account=account,
        emit_log=False,
    )
    return None if result is None else str(result)


def resolve(
    session: Session | None,
    *,
    kind: AlertKind,
    account: str | None,
    service: str,
    episode: str | None = None,
) -> None:
    """Resolve the open alert raised by `alert(kind=kind, account=account, episode=episode)`.

    Called on the clearing transition (kill switch resumed, clean reconcile, ...) so the next occurrence
    pages again. A no-op when the outbox is unavailable, `session` is None or nothing is open.
    """
    resolve_alert = _ops_function("resolve_alert")
    if resolve_alert is None or session is None:
        return
    count = resolve_alert(session, kind=kind, dedupe_key=dedupe_key(account, episode), emit_log=False)
    if count:
        log.info(
            "alert resolved: %s",
            kind,
            extra={"alert_kind": kind, "account": account, "service": service, "episode": episode},
        )


def open_episodes(session: Session | None, *, kind: AlertKind, account: str | None) -> list[str]:
    """Episodes of the open `kind` alerts of `account` (dedupe keys `{account}:{episode}`), sorted.

    Lets a caller resolve episodes it no longer tracks (for example daily-loss warnings of earlier UTC
    dates after a restart). `[]` when the outbox is unavailable or `session` is None.
    """
    open_dedupe_keys = _ops_function("open_dedupe_keys")
    if open_dedupe_keys is None or session is None:
        return []
    prefix = f"{account or ''}:"
    keys: set[str] = open_dedupe_keys(session, kind)
    return sorted(key.removeprefix(prefix) for key in keys if key.startswith(prefix))
