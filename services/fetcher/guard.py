"""SSRF guard of the sandboxed article fetcher (phase 07, design contract section 10).

Standard library only (the fetcher image installs no third-party package). Every URL the fetcher is about
to request, the first one and every redirect target, passes `check_url` and then `resolve_checked`:

- scheme http or https only, no user info, default port only (80 / 443: the egress proxy allows nothing
  else), a host name made of DNS labels (IP literals in any form, dotted, decimal, hex, octal, IPv6, are
  refused, also when written with full-width or other compatibility digits: the numeric check runs again
  on the IDNA (NFKC) form, which is the name actually sent; single-label and internal names such as
  `localhost` or `metadata.google.internal` are refused too); the request carries only ASCII (the IDNA
  host and a percent-encoded path);
- every address the name resolves to must be a public unicast address: loopback, private, link-local
  (169.254.169.254 metadata included), CGNAT, documentation (TEST-NETs), multicast, reserved,
  unspecified, IPv6 unique-local and addresses embedding one of those (IPv4-mapped, 6to4, Teredo, NAT64
  64:ff9b::/96) are refused; one bad address refuses the whole name, so a mixed answer cannot be used;
- DNS rebinding: in direct mode the connection is opened to exactly the address that was checked (the
  caller never resolves the name again). Behind the egress proxy the fetcher cannot open a socket to the
  destination itself and, on the internal fetch network, usually cannot resolve the name either: it
  refuses a name that resolves here to a forbidden address, and the proxy, which performs the only
  connection, is the address authority (deploy/squid `to_private`, the same ranges as `check_address`).
"""

from __future__ import annotations

import ipaddress
import re
import socket
import unicodedata
from collections.abc import Callable
from dataclasses import dataclass
from typing import Final
from urllib.parse import quote, urlsplit

Resolver = Callable[[str, int], list[str]]

DEFAULT_PORTS: Final[dict[str, int]] = {"http": 80, "https": 443}
MAX_URL_LENGTH: Final[int] = 2048
BLOCKED_HOST_NAMES: Final[frozenset[str]] = frozenset(
    {
        "localhost",
        "localhost.localdomain",
        "ip6-localhost",
        "ip6-loopback",
        "metadata",
        "metadata.google.internal",
        "instance-data",
    }
)
BLOCKED_SUFFIXES: Final[tuple[str, ...]] = (
    ".localhost",
    ".local",
    ".internal",
    ".intranet",
    ".lan",
    ".home.arpa",
    ".arpa",
)
_LABEL: Final = re.compile(r"^(?!-)[a-z0-9-]{1,63}(?<!-)$")
_NUMERIC_HOST: Final = re.compile(r"^[0-9a-fx.]+$")
_PATH_SAFE: Final[str] = "/%:@!$&'()*+,;=-._~"
_QUERY_SAFE: Final[str] = _PATH_SAFE + "?"
_NAT64: Final = ipaddress.IPv6Network("64:ff9b::/96")
"""Well-known NAT64 prefix: the last 32 bits are the IPv4 destination."""


class BlockedError(Exception):
    """The request was refused by the guard; `reason` is a fixed, low-cardinality label."""

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason


@dataclass(frozen=True)
class Target:
    url: str
    scheme: str
    host: str
    """The ASCII (IDNA) host name, as sent in the Host header and to the proxy."""
    port: int
    request_target: str
    """Path plus query, as sent in the request line (never empty, ASCII only)."""

    @property
    def absolute_form(self) -> str:
        """The request line target of a plain-http GET sent through the proxy."""
        return f"{self.scheme}://{self.host}{self.request_target}"


def check_url(url: str) -> Target:
    """Static checks of one URL (no DNS); raises `BlockedError`."""
    if len(url) > MAX_URL_LENGTH or any(ch.isspace() or ord(ch) < 0x20 for ch in url):
        raise BlockedError("bad_url", "URL too long or contains blanks / control characters")
    try:
        parts = urlsplit(url)
        port = parts.port
    except ValueError as exc:
        raise BlockedError("bad_url", f"unparseable URL: {exc}") from exc
    scheme = parts.scheme.lower()
    if scheme not in DEFAULT_PORTS:
        raise BlockedError("scheme", f"scheme {scheme or '(none)'} is not http or https")
    if parts.username is not None or parts.password is not None or "@" in parts.netloc:
        raise BlockedError("userinfo", "user info in the URL is refused")
    if port is not None and port != DEFAULT_PORTS[scheme]:
        raise BlockedError("port", f"port {port} is not the default port of {scheme}")
    host = check_host_name((parts.hostname or "").rstrip(".").lower())
    target = quote(parts.path or "/", safe=_PATH_SAFE)
    if parts.query:
        target = f"{target}?{quote(parts.query, safe=_QUERY_SAFE)}"
    return Target(url, scheme, host, DEFAULT_PORTS[scheme], target)


def check_host_name(host: str) -> str:
    """Static checks of a host name; returns its ASCII (IDNA) form."""
    if not host:
        raise BlockedError("hostname", "URL has no host")
    folded = unicodedata.normalize("NFKC", host)
    if ":" in folded or _numeric(folded):
        raise BlockedError("ip_literal", "IP literals are refused; use a host name")
    try:
        ascii_host = folded.encode("idna").decode("ascii").lower()
    except UnicodeError as exc:
        raise BlockedError("hostname", "host name is not a valid IDNA name") from exc
    if _numeric(ascii_host):
        raise BlockedError("ip_literal", "IP literals are refused; use a host name")
    labels = ascii_host.split(".")
    if len(labels) < 2 or not all(_LABEL.match(label) for label in labels):
        raise BlockedError("hostname", f"{host} is not a public DNS name")
    if labels[-1].isdigit():
        raise BlockedError("ip_literal", "numeric top-level label")
    if ascii_host in BLOCKED_HOST_NAMES or ascii_host.endswith(BLOCKED_SUFFIXES):
        raise BlockedError("hostname", f"{host} is an internal name")
    return ascii_host


def _numeric(host: str) -> bool:
    return bool(_NUMERIC_HOST.match(host)) and _looks_numeric(host)


def _looks_numeric(host: str) -> bool:
    """Dotted / decimal / hex / octal IPv4 forms that resolvers accept (e.g. 2130706433, 0x7f.1)."""
    return all(label == "" or label.isdigit() or label.startswith("0x") for label in host.split("."))


def _embedded(ip: ipaddress.IPv6Address) -> ipaddress.IPv4Address | None:
    if ip.ipv4_mapped is not None:
        return ip.ipv4_mapped
    if ip.sixtofour is not None:
        return ip.sixtofour
    if ip.teredo is not None:
        return ip.teredo[1]
    if ip in _NAT64:
        return ipaddress.IPv4Address(int(ip) & 0xFFFFFFFF)
    return None


def check_address(address: str) -> None:
    """Refuse any address that is not public unicast (including embedded IPv4 forms)."""
    try:
        ip = ipaddress.ip_address(address.split("%", 1)[0])
    except ValueError as exc:
        raise BlockedError("private_address", f"unparseable address {address!r}") from exc
    if isinstance(ip, ipaddress.IPv6Address):
        inner = _embedded(ip)
        if inner is not None:
            check_address(str(inner))
    if not ip.is_global or ip.is_multicast or ip.is_reserved or ip.is_unspecified:
        raise BlockedError("private_address", f"{ip} is not a public unicast address")


def system_resolver(host: str, port: int) -> list[str]:
    infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM, proto=socket.IPPROTO_TCP)
    seen: list[str] = []
    for info in infos:
        address = str(info[4][0])
        if address not in seen:
            seen.append(address)
    return seen


def resolve_checked(target: Target, resolver: Resolver) -> str:
    """Resolve once and check every address; returns the address to connect to."""
    try:
        addresses = resolver(target.host, target.port)
    except (OSError, UnicodeError) as exc:
        raise BlockedError("dns_failure", f"{target.host} did not resolve") from exc
    if not addresses:
        raise BlockedError("dns_failure", f"{target.host} resolved to nothing")
    for address in addresses:
        check_address(address)
    return addresses[0]
