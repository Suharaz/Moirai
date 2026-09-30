"""Mandatory log redaction, installed on every handler by `hdt.core.logging`.

Removes API keys, secrets, passwords, DSN credentials, Binance `signature`, `listenKey`, CMC/Binance key
headers, bearer tokens, Telegram bot tokens, OpenRouter keys and Ed25519 signatures. A field counts as
secret when its name contains a secret keyword (`api_key`, `secret`, `signature`, `listenkey`, `token`,
`password`, `private_key`, `sealed_blob`, `dsn`, `authorization`, `credential`), so prefixed names such as
`CMC_API_KEY` or `HDT_PG_PASSWORD_RISK` are covered. Secrets decrypted or loaded at runtime are also
registered with `register_secret` and redacted by exact match.
"""

from __future__ import annotations

import logging
import re
import threading
from typing import Any

MASK = "[REDACTED]"

_KEYWORD = (
    r"(?:x-cmc_pro_api_key|x-mbx-apikey|api[_-]?key|apikey|api[_-]?secret|secret|signature|listen[_-]?key|"
    r"token(?!s)|password|passwd|private[_-]?key|sealed[_-]?blob|dsn|authorization|credential)"
)
# A field name that contains a secret keyword, e.g. `cmc_api_key`, `HDT_PG_PASSWORD_RISK`, `listenKey`.
_NAME = rf"[A-Za-z0-9_\-]*{_KEYWORD}[A-Za-z0-9_\-]*"

_PATTERNS: tuple[re.Pattern[str], ...] = (
    # URL userinfo: scheme://user:PASSWORD@host
    re.compile(r"([a-zA-Z][a-zA-Z0-9+.\-]*://[^:/\s@]+:)([^@\s/]+)(@)"),
    # "name": "value"  /  'name': 'value'  (JSON and Python reprs; escaped quotes stay inside the value)
    re.compile(rf"""(?i)(["']{_NAME}["']\s*:\s*")((?:[^"\\]|\\.)*)(")"""),
    re.compile(rf"""(?i)(["']{_NAME}["']\s*:\s*')((?:[^'\\]|\\.)*)(')"""),
    # name="value" / name='value' (keyword reprs such as pydantic __repr__)
    re.compile(rf"""(?i)((?<![A-Za-z0-9_]){_NAME}=")((?:[^"\\]|\\.)*)(")"""),
    re.compile(rf"""(?i)((?<![A-Za-z0-9_]){_NAME}=')((?:[^'\\]|\\.)*)(')"""),
    # name=value in query strings, form bodies, env dumps
    re.compile(rf"(?i)((?<![A-Za-z0-9_]){_NAME}=)([^&\s\"',;\\]+)"),
    # name: value (HTTP headers, YAML)
    re.compile(rf"(?im)((?<![A-Za-z0-9_\"']){_NAME}\s*:\s*)(?:bearer\s+)?([^\s,;\"'{{}}\[\]]+)"),
    # Bearer tokens anywhere
    re.compile(r"(?i)(bearer\s+)([A-Za-z0-9._~+/=-]{8,})"),
    # Binance user-stream URLs carry the listenKey in the path
    re.compile(r"(/(?:private/)?ws/)([A-Za-z0-9]{20,})"),
    # Telegram bot token, bare or inside api.telegram.org/bot<token>/
    re.compile(r"((?:bot)?)(\d{6,12}:[A-Za-z0-9_-]{30,})"),
    # OpenRouter keys
    re.compile(r"()(sk-or-(?:v\d+-)?[A-Za-z0-9]{20,})"),
)
_SECRET_KEY = re.compile(rf"(?i)^{_NAME}$")

_registered: set[str] = set()
_lock = threading.Lock()


def register_secret(value: str) -> None:
    """Redact `value` by exact match from now on (values shorter than 6 chars are ignored)."""
    if value and len(value) >= 6:
        with _lock:
            _registered.add(value)


def is_secret_name(name: str) -> bool:
    return bool(_SECRET_KEY.match(name))


def _substitute(match: re.Match[str]) -> str:
    tail = match.group(3) if match.lastindex == 3 else ""
    return match.group(1) + MASK + tail


def redact(text: str) -> str:
    for pattern in _PATTERNS:
        text = pattern.sub(_substitute, text)
    with _lock:
        secrets = sorted(_registered, key=len, reverse=True)
    for secret in secrets:
        if secret in text:
            text = text.replace(secret, MASK)
    return text


def redact_value(key: str, value: Any) -> Any:
    """Redact a structured value: secret-named keys are masked, strings are scrubbed, containers recurse."""
    if is_secret_name(key) and value not in (None, ""):
        return MASK
    if isinstance(value, str):
        return redact(value)
    if isinstance(value, dict):
        return {str(k): redact_value(str(k), v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [redact_value(key, v) for v in value]
    if isinstance(value, int | float | bool) or value is None:
        return value
    return redact(repr(value))


class RedactionFilter(logging.Filter):
    """Rewrites the formatted message, exception text and extra fields so nothing leaks a secret."""

    _STD = frozenset(vars(logging.makeLogRecord({})).keys()) | {"message", "asctime"}

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            message = record.getMessage()
        except Exception:  # bad format args: never fall back to printing raw args
            message = f"{record.msg!s} (unformattable log arguments dropped)"
        record.msg = redact(message)
        record.args = None
        if record.exc_info and not record.exc_text:
            record.exc_text = logging.Formatter().formatException(record.exc_info)
        if record.exc_text:
            record.exc_text = redact(record.exc_text)
        record.exc_info = None
        for key, value in list(vars(record).items()):
            if key not in self._STD and not key.startswith("_"):
                setattr(record, key, redact_value(key, value))
        return True
