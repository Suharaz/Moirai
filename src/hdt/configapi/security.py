"""Session cookie, CSRF token and client IP helpers.

The cookie `hdt_session` carries a random token; the database stores only HMAC(session secret, token),
so a database leak cannot be replayed as a session. The CSRF token is derived from the same token with
a different HMAC label: it is stable for the session, never stored, and checked in constant time.
"""

from __future__ import annotations

import hashlib
import hmac
import ipaddress
import secrets
from datetime import timedelta
from typing import Final

from fastapi import Request, Response

from hdt.configapi.context import IpNetwork

SESSION_COOKIE: Final[str] = "hdt_session"
CSRF_HEADER: Final[str] = "X-CSRF-Token"
SESSION_IDLE: Final[timedelta] = timedelta(minutes=30)
STEP_UP_TTL: Final[timedelta] = timedelta(minutes=5)
MAX_FAILED_ATTEMPTS: Final[int] = 5
LOCKOUT: Final[timedelta] = timedelta(minutes=15)
MUTATING_METHODS: Final[frozenset[str]] = frozenset({"POST", "PUT", "PATCH", "DELETE"})


def _mac(secret: bytes, label: bytes, token: str) -> str:
    return hmac.new(secret, label + token.encode("utf-8"), hashlib.sha256).hexdigest()


def new_session_token() -> str:
    return secrets.token_urlsafe(32)


def session_token_hash(secret: bytes, token: str) -> str:
    return _mac(secret, b"hdt/session/v1:", token)


def csrf_token_for(secret: bytes, token: str) -> str:
    return _mac(secret, b"hdt/csrf/v1:", token)


def csrf_matches(secret: bytes, token: str, presented: str | None) -> bool:
    return presented is not None and hmac.compare_digest(csrf_token_for(secret, token), presented)


def set_session_cookie(response: Response, token: str) -> None:
    response.set_cookie(SESSION_COOKIE, token, httponly=True, secure=True, samesite="strict", path="/")


def clear_session_cookie(response: Response) -> None:
    response.delete_cookie(SESSION_COOKIE, httponly=True, secure=True, samesite="strict", path="/")


def client_ip(request: Request, trusted: tuple[IpNetwork, ...]) -> str | None:
    """The peer address, or the right-most untrusted `X-Forwarded-For` hop when the peer is trusted."""
    peer = request.client.host if request.client else None
    if peer is None or not _is_trusted(peer, trusted):
        return peer
    forwarded = request.headers.get("x-forwarded-for")
    if not forwarded:
        return peer
    hops = [hop.strip() for hop in forwarded.split(",") if hop.strip()]
    for hop in reversed(hops):
        if not _is_trusted(hop, trusted):
            return hop if _is_ip(hop) else peer
    return hops[0] if hops and _is_ip(hops[0]) else peer


def _is_ip(value: str) -> bool:
    try:
        ipaddress.ip_address(value)
    except ValueError:
        return False
    return True


def _is_trusted(value: str, trusted: tuple[IpNetwork, ...]) -> bool:
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        return False
    return any(address in network for network in trusted)
