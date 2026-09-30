"""TOTP (RFC 6238, 6 digits, 30 s) with replay protection, and sealing of TOTP seeds at rest.

A code is accepted for the current step and one step either side; its step counter must be greater than
the last accepted counter of the user, so a code can never be used twice (login and step-up alike).
TOTP seeds are stored encrypted with a key derived from the session secret (bootstrap secret).
"""

from __future__ import annotations

import hashlib
import hmac
import re
from datetime import datetime
from typing import Final

import pyotp
from nacl.exceptions import CryptoError
from nacl.secret import SecretBox

TOTP_DIGITS: Final[int] = 6
TOTP_STEP_S: Final[int] = 30
TOTP_WINDOW: Final[int] = 1
ISSUER: Final[str] = "hdt-console"
_CODE_RE: Final = re.compile(r"^\d{6}$")


class TotpSealError(RuntimeError):
    """The stored TOTP seed cannot be opened (wrong session secret)."""


def new_totp_secret() -> str:
    return pyotp.random_base32(length=32)


def provisioning_uri(secret: str, username: str) -> str:
    return pyotp.TOTP(secret, digits=TOTP_DIGITS, interval=TOTP_STEP_S).provisioning_uri(
        name=username, issuer_name=ISSUER
    )


def _box(session_secret: bytes) -> SecretBox:
    key = hmac.new(session_secret, b"hdt/totp-seed-box/v1", hashlib.sha256).digest()
    return SecretBox(key)


def seal_totp_secret(secret: str, session_secret: bytes) -> bytes:
    return bytes(_box(session_secret).encrypt(secret.encode("ascii")))


def open_totp_secret(sealed: bytes, session_secret: bytes) -> str:
    try:
        return _box(session_secret).decrypt(sealed).decode("ascii")
    except CryptoError as exc:
        raise TotpSealError("cannot open the stored TOTP seed (session secret changed?)") from exc


def totp_code(secret: str, at: datetime) -> str:
    return pyotp.TOTP(secret, digits=TOTP_DIGITS, interval=TOTP_STEP_S).at(at)


def verify_totp(secret: str, code: str, *, now: datetime, last_counter: int | None) -> int | None:
    """Return the accepted step counter, or None (wrong code, malformed code, or replayed step)."""
    if not _CODE_RE.fullmatch(code or ""):
        return None
    totp = pyotp.TOTP(secret, digits=TOTP_DIGITS, interval=TOTP_STEP_S)
    current = totp.timecode(now)
    for counter in range(current - TOTP_WINDOW, current + TOTP_WINDOW + 1):
        if last_counter is not None and counter <= last_counter:
            continue
        if hmac.compare_digest(totp.generate_otp(counter), code):
            return counter
    return None
