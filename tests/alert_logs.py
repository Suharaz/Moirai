"""Log records of raised alerts, for tests that assert which alerts a component raised.

`hdt.core.alerts.alert` logs exactly one record per raised alert (message `alert: <title>`, `alert_kind`
extra; the outbox it forwards to does not log again) and `hdt.core.alerts.resolve` one record per
resolution (`alert resolved: <kind>`, same extra). Only the first kind counts as a raise.
"""

from __future__ import annotations

import logging


def raised(record: logging.LogRecord, kind: str | None = None) -> bool:
    """True for the record of a raised alert (of `kind` when given)."""
    if not hasattr(record, "alert_kind") or not record.getMessage().startswith("alert: "):
        return False
    return kind is None or record.alert_kind == kind
