"""Sandboxed article fetcher (phase 07): `POST /fetch {"item_id", "url"}` on port 8080 (net_fetch only).

Standard library only; runs as a non-root user on a read-only filesystem with no volume and no secret.
It downloads exactly one URL (the ingested URL of a news item, chosen by the caller) and returns the
verbatim body, its sha256 and the redirect chain; it never parses, renders or executes the content (no JS)
and never writes anything: the caller (the council's live `fetch_source` tool) stores the capture.

Limits (design contract section 10): http/https only; the SSRF guard (`guard.py`) runs on the first URL and
on every redirect target; at most 3 redirects; at most 5 MB on the wire and 5 MB after decompression
(gzip / deflate only, decompressed incrementally with a hard output bound, so a compression bomb stops at
the limit; a corrupt stream is refused); a body shorter than its Content-Length or a compressed stream
that never ends is a truncated download (upstream error, never data); a hard 10 s deadline for the whole
request chain, name resolution, proxy CONNECT and TLS handshake included (a watchdog closes the socket,
so a server that drips bytes cannot hold the fetch open). `content_type` is returned bounded and
printable only. At most `MAX_CONCURRENT` requests are served at once (more are refused at accept) and a
client socket idles at most `CLIENT_TIMEOUT_S`.

Answers:
- 200 `{"item_id", "requested_url", "final_url", "redirect_chain", "http_status", "content_type",
  "body_b64", "body_sha256"}` (any upstream status: 404 and 5xx are data too);
- 422 `{"error": "blocked", "reason"}` when the guard, a limit or the egress proxy refused the fetch;
- 502 `{"error": "upstream", "reason"}` when the destination could not be reached or the download broke;
- 400 for a malformed request. `GET /healthz` answers 200.

Behind the egress proxy (`HTTPS_PROXY` / `HTTP_PROXY`, set by docker-compose) HTTPS goes through a CONNECT
tunnel and plain HTTP as an absolute-form GET; the proxy is the only address this process connects to and
the guard's name checks still run on every hop. On the internal fetch network names normally do not
resolve here, so the proxy is the address authority (deploy/squid `to_private`). A proxy refusal is a
block, never data: a CONNECT answered with 403, and a plain-http answer the proxy generated itself
(`X-Squid-Error` header; 403 means denied, anything else is an upstream error).
"""

from __future__ import annotations

import base64
import contextlib
import hashlib
import http.client
import json
import logging
import os
import re
import socket
import ssl
import sys
import threading
import time
import zlib
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Final, NoReturn
from urllib.parse import urljoin, urlsplit

from guard import BlockedError, Resolver, Target, check_url, resolve_checked, system_resolver

log = logging.getLogger("hdt.fetcher")

MAX_REDIRECTS: Final[int] = 3
MAX_BYTES: Final[int] = 5_000_000
TIMEOUT_S: Final[float] = 10.0
MAX_REQUEST_BYTES: Final[int] = 4096
READ_CHUNK: Final[int] = 65536
MAX_CONCURRENT: Final[int] = 16
CLIENT_TIMEOUT_S: Final[float] = 15.0
CONTENT_TYPE_MAX: Final[int] = 128
USER_AGENT: Final[str] = "hdt-fetcher/1 (+research; no-js)"
ITEM_ID: Final = re.compile(r"^[A-Za-z0-9_.:-]{1,64}$")
REDIRECT_STATUSES: Final[frozenset[int]] = frozenset({301, 302, 303, 307, 308})
PROXY_ERROR_HEADER: Final[str] = "x-squid-error"
_PROXY_ERROR_VALUE: Final = re.compile(r"^ERR_[A-Z_]+(?: |$)")
"""The form Squid gives its own error pages (`ERR_ACCESS_DENIED 0`)."""
_ASCII_DIGITS: Final = re.compile(r"^[0-9]+$")
_NOT_PRINTABLE: Final = re.compile(r"[^\x20-\x7e]")


@dataclass(frozen=True)
class Limits:
    max_redirects: int = MAX_REDIRECTS
    max_bytes: int = MAX_BYTES
    timeout_s: float = TIMEOUT_S


@dataclass(frozen=True)
class Proxy:
    host: str
    port: int

    @classmethod
    def from_url(cls, url: str) -> Proxy:
        parts = urlsplit(url)
        if parts.scheme != "http" or not parts.hostname:
            raise ValueError(f"unsupported proxy URL {url!r}")
        return cls(parts.hostname, parts.port or 3128)


@dataclass(frozen=True)
class Proxies:
    http: Proxy | None = None
    https: Proxy | None = None

    @classmethod
    def from_env(cls) -> Proxies:
        def one(*names: str) -> Proxy | None:
            for name in names:
                value = os.environ.get(name)
                if value:
                    return Proxy.from_url(value)
            return None

        return cls(http=one("HTTP_PROXY", "http_proxy"), https=one("HTTPS_PROXY", "https_proxy"))

    def for_scheme(self, scheme: str) -> Proxy | None:
        return self.https if scheme == "https" else self.http


@dataclass(frozen=True)
class FetchResult:
    requested_url: str
    final_url: str
    redirect_chain: tuple[str, ...]
    http_status: int
    content_type: str
    body: bytes
    body_sha256: str


class UpstreamError(Exception):
    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason


@dataclass
class _Deadline:
    """Hard wall-clock bound: at expiry the watchdog shuts the current socket down."""

    seconds: float
    started: float = field(default_factory=time.monotonic)
    expired: bool = False
    _sock: socket.socket | None = None
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _timer: threading.Timer | None = None

    def __post_init__(self) -> None:
        self._timer = threading.Timer(self.seconds, self._expire)
        self._timer.daemon = True
        self._timer.start()

    def remaining(self) -> float:
        left = self.seconds - (time.monotonic() - self.started)
        if left <= 0 or self.expired:
            raise BlockedError("timeout", f"fetch exceeded {self.seconds:g} s")
        return left

    def watch(self, sock: socket.socket | None) -> None:
        with self._lock:
            self._sock = sock
            if self.expired and sock is not None:
                _shutdown(sock)

    def _expire(self) -> None:
        with self._lock:
            self.expired = True
            if self._sock is not None:
                _shutdown(self._sock)

    def cancel(self) -> None:
        if self._timer is not None:
            self._timer.cancel()


def _shutdown(sock: socket.socket) -> None:
    with contextlib.suppress(OSError):
        sock.shutdown(socket.SHUT_RDWR)


def _resolve_within(resolver: Resolver, host: str, port: int, deadline: _Deadline) -> list[str]:
    """`resolver(host, port)` bounded by the deadline (a blocking lookup cannot be interrupted, so it runs
    in a daemon thread that is abandoned at expiry)."""
    answer: list[list[str]] = []
    failure: list[Exception] = []

    def lookup() -> None:
        try:
            answer.append(resolver(host, port))
        except Exception as exc:  # handed to the caller below
            failure.append(exc)

    worker = threading.Thread(target=lookup, name="fetcher-dns", daemon=True)
    worker.start()
    worker.join(deadline.remaining())
    if worker.is_alive():
        raise BlockedError("timeout", f"name resolution exceeded the {deadline.seconds:g} s deadline")
    if failure:
        raise failure[0]
    return answer[0]


def content_type(raw: str) -> str:
    """The Content-Type as returned: printable ASCII only, at most `CONTENT_TYPE_MAX` characters."""
    return _NOT_PRINTABLE.sub("", raw).strip()[:CONTENT_TYPE_MAX]


class _Decoder:
    """Content-Encoding decoder with a hard bound on the decoded size."""

    def __init__(self, encoding: str, max_bytes: int) -> None:
        encoding = encoding.strip().lower()
        if encoding in ("", "identity"):
            self._z: Any = None
        elif encoding in ("gzip", "x-gzip"):
            self._z = zlib.decompressobj(wbits=16 + zlib.MAX_WBITS)
        elif encoding == "deflate":
            self._z = _PENDING_DEFLATE  # zlib-wrapped or raw: decided on the first two bytes
        else:
            raise BlockedError("encoding", f"unsupported content encoding {encoding!r}")
        self._max = max_bytes
        self._fed = False
        self.out = bytearray()

    def feed(self, data: bytes) -> None:
        if self._z is None:
            self._append(data)
            return
        if self._z is _PENDING_DEFLATE:
            if not data:
                return
            self._z = zlib.decompressobj(wbits=zlib.MAX_WBITS if _zlib_header(data) else -zlib.MAX_WBITS)
        self._fed = self._fed or bool(data)
        while data:
            try:
                chunk = self._z.decompress(data, self._max + 1 - len(self.out))
            except zlib.error as exc:
                raise BlockedError("encoding", f"corrupt compressed body: {exc}") from exc
            self._append(chunk)
            data = self._z.unconsumed_tail
            if not chunk and not data:
                break

    def finish(self) -> bytes:
        if self._z is _PENDING_DEFLATE:
            return bytes(self.out)  # a declared deflate body with no bytes at all
        if self._z is not None:
            try:
                self._append(self._z.flush(self._max + 1 - len(self.out)))
            except zlib.error as exc:
                raise BlockedError("encoding", f"corrupt compressed body: {exc}") from exc
            if self._fed and not self._z.eof:
                raise UpstreamError("truncated", "the compressed body ended before its end of stream")
        return bytes(self.out)

    def _append(self, chunk: bytes) -> None:
        self.out.extend(chunk)
        if len(self.out) > self._max:
            raise BlockedError("too_large", f"decoded body larger than {self._max} bytes")


_PENDING_DEFLATE: Final = object()


def _zlib_header(data: bytes) -> bool:
    """`data` starts with a zlib (RFC 1950) header; servers also send raw deflate (RFC 1951) as `deflate`."""
    return len(data) >= 2 and data[0] & 0x0F == 8 and (data[0] << 8 | data[1]) % 31 == 0


def redirect_location(raw: str) -> str:
    """The Location as the server sent it: http.client decodes header bytes as latin-1, so a UTF-8 value
    arrives as mojibake; recover the UTF-8 text when the bytes are valid UTF-8."""
    try:
        return raw.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return raw


class Fetcher:
    def __init__(
        self,
        *,
        limits: Limits | None = None,
        resolver: Resolver = system_resolver,
        proxies: Proxies | None = None,
        ssl_context: ssl.SSLContext | None = None,
    ) -> None:
        self.limits = limits or Limits()
        self._resolver = resolver
        self._proxies = proxies or Proxies()
        self._ssl = ssl_context or ssl.create_default_context()

    def fetch(self, url: str) -> FetchResult:
        deadline = _Deadline(self.limits.timeout_s)
        try:
            result = self._follow(url, deadline)
            deadline.remaining()  # a fetch that finished past its deadline is a timeout, not data
            return result
        except OSError as exc:
            # Every socket timeout is set to the time left, so a socket timeout is the deadline too (it can
            # fire a moment before the watchdog flags it).
            if deadline.expired or isinstance(exc, TimeoutError):
                raise BlockedError("timeout", f"fetch exceeded {self.limits.timeout_s:g} s") from exc
            raise UpstreamError("connect", f"{type(exc).__name__}: {exc}") from exc
        except http.client.HTTPException as exc:
            if deadline.expired:
                raise BlockedError("timeout", f"fetch exceeded {self.limits.timeout_s:g} s") from exc
            raise UpstreamError("protocol", f"{type(exc).__name__}: {exc}") from exc
        finally:
            deadline.cancel()

    def _follow(self, url: str, deadline: _Deadline) -> FetchResult:
        chain: list[str] = []
        current = url
        while True:
            target = check_url(current)
            status, headers, body = self._request(target, deadline)
            location = headers.get("location")
            if status in REDIRECT_STATUSES and location:
                if len(chain) >= self.limits.max_redirects:
                    raise BlockedError(
                        "too_many_redirects", f"more than {self.limits.max_redirects} redirects"
                    )
                try:
                    current = urljoin(current, redirect_location(location).strip())
                except ValueError as exc:
                    raise BlockedError("bad_url", f"unparseable redirect location: {exc}") from exc
                chain.append(current)
                continue
            return FetchResult(
                requested_url=url,
                final_url=current,
                redirect_chain=tuple(chain),
                http_status=status,
                content_type=content_type(headers.get("content-type", "")),
                body=body,
                body_sha256=hashlib.sha256(body).hexdigest(),
            )

    def _request(self, target: Target, deadline: _Deadline) -> tuple[int, dict[str, str], bytes]:
        proxy = self._proxies.for_scheme(target.scheme)
        conn = self._connect(target, proxy, deadline)
        try:
            via_proxy = proxy is not None and target.scheme == "http"
            request_target = target.absolute_form if via_proxy else target.request_target
            conn.putrequest("GET", request_target, skip_host=True, skip_accept_encoding=True)
            conn.putheader("Host", target.host)
            conn.putheader("User-Agent", USER_AGENT)
            conn.putheader("Accept", "text/html,application/xhtml+xml,text/plain;q=0.9,*/*;q=0.5")
            conn.putheader("Accept-Encoding", "gzip, deflate")
            conn.putheader("Connection", "close")
            conn.endheaders()
            self._set_timeout(conn, deadline)
            response = conn.getresponse()
            headers = {key.lower(): value for key, value in response.getheaders()}
            if via_proxy and _proxy_error(response.status, headers.get(PROXY_ERROR_HEADER)):
                _raise_proxy_error(response.status, headers[PROXY_ERROR_HEADER])
            if response.status in REDIRECT_STATUSES and headers.get("location"):
                return response.status, headers, b""
            length = (headers.get("content-length") or "").strip()
            if _ASCII_DIGITS.match(length) and int(length) > self.limits.max_bytes:
                raise BlockedError("too_large", f"body larger than {self.limits.max_bytes} bytes")
            decoder = _Decoder(headers.get("content-encoding", ""), self.limits.max_bytes)
            received = 0
            while True:
                self._set_timeout(conn, deadline)
                chunk = response.read1(READ_CHUNK)
                if not chunk:
                    break
                received += len(chunk)
                if received > self.limits.max_bytes:
                    raise BlockedError("too_large", f"body larger than {self.limits.max_bytes} bytes")
                decoder.feed(chunk)
            if response.length:
                # http.client counts down the declared Content-Length; bytes still owed mean a cut download.
                raise UpstreamError("truncated", f"body ended {response.length} bytes before its length")
            return response.status, headers, decoder.finish()
        finally:
            deadline.watch(None)
            conn.close()

    def _connect(
        self, target: Target, proxy: Proxy | None, deadline: _Deadline
    ) -> http.client.HTTPConnection:
        conn: http.client.HTTPConnection
        if proxy is None:
            address = resolve_checked(
                target, lambda host, port: _resolve_within(self._resolver, host, port, deadline)
            )
            sock = self._open(address, target.port, deadline)
            try:
                if target.scheme == "https":
                    sock = self._tls(sock, target, deadline)
                    conn = http.client.HTTPSConnection(target.host, target.port, context=self._ssl)
                else:
                    conn = http.client.HTTPConnection(target.host, target.port)
            except BaseException:
                sock.close()
                raise
            conn.sock = sock
            return conn
        # Behind the proxy: the name must not resolve to a forbidden address when it resolves here at all
        # (inside the sandbox network it normally does not; the proxy's `to_private` is then the address
        # authority); the proxy enforces the address it connects to.
        try:
            addresses = _resolve_within(self._resolver, target.host, target.port, deadline)
        except (OSError, UnicodeError):
            addresses = []
        if addresses:
            resolve_checked(target, lambda _host, _port: addresses)
        # The proxy's own name is looked up under the deadline too (never inside create_connection).
        proxy_addresses = _resolve_within(system_resolver, proxy.host, proxy.port, deadline)
        if not proxy_addresses:
            raise UpstreamError("proxy", f"the egress proxy name {proxy.host!r} did not resolve")
        sock = self._open(proxy_addresses[0], proxy.port, deadline)
        try:
            if target.scheme == "https":
                self._tunnel(sock, target, deadline)
                sock = self._tls(sock, target, deadline)
                conn = http.client.HTTPSConnection(target.host, target.port, context=self._ssl)
            else:
                conn = http.client.HTTPConnection(proxy.host, proxy.port)
        except BaseException:
            sock.close()
            raise
        conn.sock = sock
        return conn

    @staticmethod
    def _open(host: str, port: int, deadline: _Deadline) -> socket.socket:
        sock = socket.create_connection((host, port), timeout=deadline.remaining())
        deadline.watch(sock)
        return sock

    def _tls(self, sock: socket.socket, target: Target, deadline: _Deadline) -> socket.socket:
        """TLS over `sock`, the handshake itself under the watchdog (the wrapped socket is watched first)."""
        tls = self._ssl.wrap_socket(sock, server_hostname=target.host, do_handshake_on_connect=False)
        try:
            deadline.watch(tls)
            tls.settimeout(deadline.remaining())
            tls.do_handshake()
        except BaseException:
            tls.close()  # wrap_socket detached `sock`: the wrapper owns the descriptor now
            raise
        return tls

    @staticmethod
    def _tunnel(sock: socket.socket, target: Target, deadline: _Deadline) -> None:
        """CONNECT through the proxy on the watched socket; a refusal is `proxy_denied`."""
        authority = f"{target.host}:{target.port}"
        request = f"CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\nUser-Agent: {USER_AGENT}\r\n\r\n"
        sock.settimeout(deadline.remaining())
        sock.sendall(request.encode("ascii"))
        response = http.client.HTTPResponse(sock, method="CONNECT")
        try:
            response.begin()
            status, error = response.status, response.getheader(PROXY_ERROR_HEADER) or ""
        finally:
            response.close()  # the buffered reader only; the socket stays open for TLS
        if status != 200:
            _raise_proxy_error(status, error or f"CONNECT answered {status}")

    @staticmethod
    def _set_timeout(conn: http.client.HTTPConnection, deadline: _Deadline) -> None:
        if conn.sock is not None:
            conn.sock.settimeout(deadline.remaining())


def _proxy_error(status: int, value: str | None) -> bool:
    """An error page of the egress proxy on a plain-http GET: Squid answers an error status with its
    `X-Squid-Error: ERR_...` header. An origin's own header on a success, or in another form, is data."""
    return value is not None and status >= 400 and _PROXY_ERROR_VALUE.match(value.strip()) is not None


def _raise_proxy_error(status: int, error: str) -> NoReturn:
    """A refusal by the egress proxy: 403 is a denial (block), anything else could not be reached."""
    detail = content_type(error) or f"status {status}"
    if status == 403:
        raise BlockedError("proxy_denied", f"the egress proxy denied the destination ({detail})")
    raise UpstreamError("proxy", f"the egress proxy could not reach the destination ({detail})")


def result_json(item_id: str, result: FetchResult) -> dict[str, Any]:
    return {
        "item_id": item_id,
        "requested_url": result.requested_url,
        "final_url": result.final_url,
        "redirect_chain": list(result.redirect_chain),
        "http_status": result.http_status,
        "content_type": result.content_type,
        "body_b64": base64.b64encode(result.body).decode("ascii"),
        "body_sha256": result.body_sha256,
    }


def handle_fetch(fetcher: Fetcher, raw: bytes) -> tuple[int, dict[str, Any]]:
    """(status, JSON answer) of one `POST /fetch` body."""
    try:
        request = json.loads(raw)
    except ValueError:
        return 400, {"error": "bad_request", "reason": "body is not JSON"}
    if not isinstance(request, dict) or set(request) != {"item_id", "url"}:
        return 400, {"error": "bad_request", "reason": "expected exactly item_id and url"}
    item_id, url = request["item_id"], request["url"]
    if not isinstance(item_id, str) or not ITEM_ID.match(item_id) or not isinstance(url, str):
        return 400, {"error": "bad_request", "reason": "invalid item_id or url"}
    try:
        result = fetcher.fetch(url)
    except BlockedError as exc:
        log.warning("fetch blocked item=%s reason=%s: %s", item_id, exc.reason, exc)
        return 422, {"error": "blocked", "reason": exc.reason}
    except UpstreamError as exc:
        log.warning("fetch failed item=%s reason=%s: %s", item_id, exc.reason, exc)
        return 502, {"error": "upstream", "reason": exc.reason}
    except Exception:
        # A bug or an input no rule foresaw: answered, never a dropped connection.
        log.exception("fetch failed item=%s: unexpected error", item_id)
        return 502, {"error": "upstream", "reason": "internal"}
    log.info(
        "fetched item=%s status=%s redirects=%d bytes=%d",
        item_id,
        result.http_status,
        len(result.redirect_chain),
        len(result.body),
    )
    return 200, result_json(item_id, result)


class BoundedServer(ThreadingHTTPServer):
    """At most `max_concurrent` requests in flight: a connection beyond that is closed at accept."""

    daemon_threads = True

    def __init__(
        self,
        address: tuple[str, int],
        handler: type[BaseHTTPRequestHandler],
        max_concurrent: int = MAX_CONCURRENT,
    ) -> None:
        super().__init__(address, handler)
        self.max_concurrent = max_concurrent
        self._slots = threading.BoundedSemaphore(max_concurrent)

    def process_request(self, request: Any, client_address: Any) -> None:
        if not self._slots.acquire(blocking=False):
            log.warning("fetcher busy: %d requests in flight, connection refused", self.max_concurrent)
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self._slots.release()
            raise

    def process_request_thread(self, request: Any, client_address: Any) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._slots.release()


def make_handler(fetcher: Fetcher) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "hdt-fetcher"
        sys_version = ""
        timeout = CLIENT_TIMEOUT_S

        def do_GET(self) -> None:
            if self.path == "/healthz":
                self._send(200, {"status": "ok"})
            else:
                self._send(404, {"error": "not_found"})

        def do_POST(self) -> None:
            if self.path != "/fetch":
                self._send(404, {"error": "not_found"})
                return
            length = self.headers.get("Content-Length", "")
            if not length.isdigit() or int(length) > MAX_REQUEST_BYTES:
                self._send(400, {"error": "bad_request", "reason": "missing or oversized body"})
                return
            status, answer = handle_fetch(fetcher, self.rfile.read(int(length)))
            self._send(status, answer)

        def _send(self, status: int, answer: dict[str, Any]) -> None:
            data = json.dumps(answer).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, format: str, *args: Any) -> None:
            log.debug(format, *args)

    return Handler


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    if hasattr(os, "geteuid") and os.geteuid() == 0:
        log.error("refusing to run as root")
        sys.exit(2)
    port = int(os.environ.get("HDT_FETCHER_PORT", "8080"))
    handler = make_handler(Fetcher(proxies=Proxies.from_env()))
    server = BoundedServer(("0.0.0.0", port), handler)  # noqa: S104
    log.info("fetcher listening on %d", port)
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
