"""SSRF guard of the sandboxed fetcher (services/fetcher/guard.py, stdlib only)."""

from __future__ import annotations

import base64
import gzip
import json
import socket
import sys
import threading
import time
import zlib
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "services" / "fetcher"))

import app
from app import BoundedServer, Fetcher, Limits, Proxies, Proxy, _Decoder, handle_fetch, make_handler
from guard import BlockedError, check_address, check_url, resolve_checked


@pytest.mark.parametrize(
    ("url", "reason"),
    [
        ("file:///etc/passwd", "scheme"),
        ("gopher://example.com/", "scheme"),
        ("ftp://example.com/x", "scheme"),
        ("http://user:pw@example.com/", "userinfo"),
        ("http://example.com:8080/", "port"),
        ("https://example.com:22/", "port"),
        ("http://127.0.0.1/", "ip_literal"),
        ("http://169.254.169.254/latest/meta-data/", "ip_literal"),
        ("http://[::1]/", "ip_literal"),
        ("http://2130706433/", "ip_literal"),
        ("http://0x7f.1/", "ip_literal"),
        ("http://localhost/", "hostname"),
        ("http://metadata.google.internal/", "hostname"),
        ("http://redis/", "hostname"),
        ("http://printer.local/", "hostname"),
        ("http://\uff11\uff12\uff17.\uff10.\uff10.0x1/", "ip_literal"),  # full-width digits
        ("http://127\u30020\u30020\u30021/", "ip_literal"),  # ideographic full stops
    ],
)
def test_static_url_checks_refuse(url: str, reason: str) -> None:
    with pytest.raises(BlockedError) as err:
        check_url(url)
    assert err.value.reason == reason


@pytest.mark.parametrize(
    "address",
    [
        "127.0.0.1",
        "10.1.2.3",
        "172.16.0.1",
        "192.168.1.1",
        "169.254.169.254",
        "100.64.0.1",
        "0.0.0.0",  # noqa: S104 - an address the guard must refuse
        "224.0.0.1",
        "::1",
        "fd00::1",
        "fe80::1",
        "::ffff:127.0.0.1",
        "2002:7f00:0001::",
        "192.0.2.10",
        "198.51.100.10",
        "203.0.113.10",
        "64:ff9b::a00:1",
    ],
)
def test_private_addresses_refused(address: str) -> None:
    with pytest.raises(BlockedError) as err:
        check_address(address)
    assert err.value.reason == "private_address"


def test_public_name_resolves_to_checked_address() -> None:
    target = check_url("https://www.coindesk.com/markets/x?a=1")
    assert resolve_checked(target, lambda _h, _p: ["93.184.216.34"]) == "93.184.216.34"
    assert target.request_target == "/markets/x?a=1"


def test_mixed_dns_answer_refused_rebinding() -> None:
    target = check_url("https://evil.example.com/")
    with pytest.raises(BlockedError):
        resolve_checked(target, lambda _h, _p: ["93.184.216.34", "10.0.0.5"])


def test_redirect_to_private_host_is_blocked() -> None:
    answers = {"good.example.com": ["93.184.216.34"], "bad.example.com": ["169.254.169.254"]}
    fetcher = Fetcher(limits=Limits(), resolver=lambda host, _p: answers[host])
    real_request = fetcher._request
    hops: list[str] = []

    def first_hop_redirects(target, deadline):  # type: ignore[no-untyped-def]
        hops.append(target.host)
        if target.host == "good.example.com":
            return 302, {"location": "http://bad.example.com/x"}, b""
        return real_request(target, deadline)  # real path: the guard runs before any socket is opened

    fetcher._request = first_hop_redirects  # type: ignore[method-assign]
    with pytest.raises(BlockedError) as err:
        fetcher.fetch("https://good.example.com/story")
    assert err.value.reason == "private_address"
    assert hops == ["good.example.com", "bad.example.com"]


def test_too_many_redirects_refused() -> None:
    fetcher = Fetcher(limits=Limits(max_redirects=3), resolver=lambda _h, _p: ["93.184.216.34"])
    fetcher._request = lambda _t, _d: (302, {"location": "/again"}, b"")  # type: ignore[method-assign,assignment]
    with pytest.raises(BlockedError) as err:
        fetcher.fetch("https://loop.example.com/start")
    assert err.value.reason == "too_many_redirects"


def test_gzip_bomb_stops_at_limit() -> None:
    bomb = gzip.compress(b"\0" * 2_000_000)
    decoder = _Decoder("gzip", 100_000)
    with pytest.raises(BlockedError) as err:
        decoder.feed(bomb)
    assert err.value.reason == "too_large"


def test_handle_fetch_rejects_malformed_and_blocked() -> None:
    fetcher = Fetcher(resolver=lambda _h, _p: ["10.0.0.1"])
    assert handle_fetch(fetcher, b"not json")[0] == 400
    assert handle_fetch(fetcher, b'{"item_id": "a", "url": "x", "extra": 1}')[0] == 400
    status, answer = handle_fetch(fetcher, b'{"item_id": "rss:abc", "url": "http://127.0.0.1/"}')
    assert (status, answer["reason"]) == (422, "ip_literal")
    status, answer = handle_fetch(fetcher, b'{"item_id": "rss:abc", "url": "http://internal.example.com/"}')
    assert (status, answer["reason"]) == (422, "private_address")


class FakeProxy:
    """A local stand-in for the egress proxy: records each request head, answers with canned bytes."""

    def __init__(self, answer: bytes, *, hang: bool = False) -> None:
        self.answer = answer
        self.hang = hang
        self.requests: list[bytes] = []
        self._sock = socket.create_server(("127.0.0.1", 0))
        self.port = self._sock.getsockname()[1]
        self._stop = threading.Event()
        threading.Thread(target=self._serve, daemon=True).start()

    def _serve(self) -> None:
        while not self._stop.is_set():
            try:
                conn, _ = self._sock.accept()
            except OSError:
                return
            with conn:
                head = b""
                while b"\r\n\r\n" not in head:
                    chunk = conn.recv(4096)
                    if not chunk:
                        break
                    head += chunk
                self.requests.append(head)
                if self.hang:
                    self._stop.wait(5)
                    continue
                conn.sendall(self.answer)

    def fetcher(self, *, timeout_s: float = 5.0) -> Fetcher:
        proxy = Proxy("127.0.0.1", self.port)
        return Fetcher(
            limits=Limits(timeout_s=timeout_s),
            resolver=_no_dns,
            proxies=Proxies(http=proxy, https=proxy),
        )

    def close(self) -> None:
        self._stop.set()
        self._sock.close()


def _no_dns(host: str, _port: int) -> list[str]:
    raise socket.gaierror(f"{host}: no name resolution on the internal fetch network")


def _fetch(proxy: FakeProxy, url: str, *, timeout_s: float = 5.0) -> tuple[int, dict[str, Any]]:
    try:
        body = json.dumps({"item_id": "rss:abc", "url": url}).encode()
        status, answer = handle_fetch(proxy.fetcher(timeout_s=timeout_s), body)
        return int(status), dict(answer)
    finally:
        proxy.close()


def _http(status: str, headers: dict[str, str], body: bytes) -> bytes:
    lines = [f"HTTP/1.1 {status}", *(f"{k}: {v}" for k, v in headers.items()), "", ""]
    return "\r\n".join(lines).encode() + body


def test_proxy_denial_of_plain_http_is_a_block_not_data() -> None:
    denied = _http(
        "403 Forbidden", {"X-Squid-Error": "ERR_ACCESS_DENIED 0", "Content-Length": "6"}, b"denied"
    )
    assert _fetch(FakeProxy(denied), "http://internal.example.com/") == (
        422,
        {"error": "blocked", "reason": "proxy_denied"},
    )
    failed = _http("503 Service Unavailable", {"X-Squid-Error": "ERR_DNS_FAIL 0", "Content-Length": "0"}, b"")
    assert _fetch(FakeProxy(failed), "http://gone.example.com/") == (
        502,
        {"error": "upstream", "reason": "proxy"},
    )


def test_origin_403_through_the_proxy_stays_data() -> None:
    status, answer = _fetch(
        FakeProxy(_http("403 Forbidden", {"Content-Length": "2"}, b"no")), "http://a.example.com/"
    )
    assert (status, answer["http_status"]) == (200, 403)


def test_proxy_refusing_the_connect_tunnel_is_a_block() -> None:
    denied = _http("403 Forbidden", {"X-Squid-Error": "ERR_ACCESS_DENIED 0", "Content-Length": "0"}, b"")
    proxy = FakeProxy(denied)
    assert _fetch(proxy, "https://internal.example.com/x")[1]["reason"] == "proxy_denied"
    assert proxy.requests[0].startswith(b"CONNECT internal.example.com:443 HTTP/1.1\r\n")


def test_truncated_body_is_an_upstream_error_not_data() -> None:
    cut = _http("200 OK", {"Content-Length": "100"}, b"0123456789")
    assert _fetch(FakeProxy(cut), "http://a.example.com/") == (
        502,
        {"error": "upstream", "reason": "truncated"},
    )
    whole = gzip.compress(b"<html>" + b"x" * 5000 + b"</html>")
    cut_gzip = _http("200 OK", {"Content-Encoding": "gzip", "Content-Length": "40"}, whole[:40])
    assert _fetch(FakeProxy(cut_gzip), "http://a.example.com/")[1]["reason"] == "truncated"


def test_corrupt_gzip_is_refused_not_a_dropped_connection() -> None:
    corrupt = _http("200 OK", {"Content-Encoding": "gzip", "Content-Length": "12"}, b"not gzip!!!!")
    assert _fetch(FakeProxy(corrupt), "http://a.example.com/") == (
        422,
        {"error": "blocked", "reason": "encoding"},
    )


def test_malformed_redirect_location_is_refused() -> None:
    bad = _http("302 Found", {"Location": "http://[::1", "Content-Length": "0"}, b"")
    assert _fetch(FakeProxy(bad), "http://a.example.com/") == (422, {"error": "blocked", "reason": "bad_url"})


def test_unicode_host_and_path_are_sent_as_ascii() -> None:
    ok = _http(
        "200 OK", {"Content-Type": "text/html\x01; charset=utf-8" + "x" * 500, "Content-Length": "2"}, b"ok"
    )
    proxy = FakeProxy(ok)
    status, answer = _fetch(proxy, "http://b\u00fccher.example.com/caf\u00e9?q=\u00e9")
    assert status == 200
    assert proxy.requests[0].startswith(
        b"GET http://xn--bcher-kva.example.com/caf%C3%A9?q=%C3%A9 HTTP/1.1\r\n"
        b"Host: xn--bcher-kva.example.com\r\n"
    )
    assert answer["content_type"].startswith("text/html; charset=utf-8")
    assert len(answer["content_type"]) == 128


def test_deadline_bounds_a_proxy_that_never_answers_connect() -> None:
    started = time.monotonic()
    status, answer = _fetch(FakeProxy(b"", hang=True), "https://a.example.com/", timeout_s=0.5)
    assert (status, answer["reason"]) == (422, "timeout")
    assert time.monotonic() - started < 3


def test_deadline_bounds_name_resolution() -> None:
    def slow(_host: str, _port: int) -> list[str]:
        time.sleep(3)
        return ["93.184.216.34"]

    started = time.monotonic()
    body = b'{"item_id": "rss:abc", "url": "https://slow.example.com/"}'
    status, answer = handle_fetch(Fetcher(limits=Limits(timeout_s=0.3), resolver=slow), body)
    assert (status, answer["reason"]) == (422, "timeout")
    assert time.monotonic() - started < 2


def test_server_refuses_connections_beyond_its_bound() -> None:
    release = threading.Event()

    class Slow(Fetcher):
        def fetch(self, url: str) -> Any:
            release.wait(5)
            raise BlockedError("timeout", "held")

    server = BoundedServer(("127.0.0.1", 0), make_handler(Slow()), max_concurrent=1)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    port = server.server_address[1]
    body = b'{"item_id": "rss:abc", "url": "https://a.example.com/"}'
    request = b"POST /fetch HTTP/1.1\r\nHost: f\r\nContent-Length: %d\r\n\r\n%s" % (len(body), body)
    try:
        first = socket.create_connection(("127.0.0.1", port), timeout=5)
        first.sendall(request)
        time.sleep(0.3)
        second = socket.create_connection(("127.0.0.1", port), timeout=5)
        second.sendall(request)
        try:
            refused = second.recv(100)
        except ConnectionError:  # closed at accept with unread data: some stacks reset instead of FIN
            refused = b""
        assert refused == b""  # closed at accept: the one slot is taken
        assert first.recv(100).startswith(b"HTTP/1.0 422")
    finally:
        release.set()
        server.shutdown()
        server.server_close()


def test_origin_squid_error_header_is_data_not_a_proxy_denial() -> None:
    spoof = _http("200 OK", {"X-Squid-Error": "ERR_ACCESS_DENIED 0", "Content-Length": "2"}, b"ok")
    status, answer = _fetch(FakeProxy(spoof), "http://a.example.com/")
    assert (status, answer["http_status"]) == (200, 200)
    odd = _http("403 Forbidden", {"X-Squid-Error": "nope", "Content-Length": "2"}, b"no")
    status, answer = _fetch(FakeProxy(odd), "http://a.example.com/")
    assert (status, answer["http_status"]) == (200, 403)


def test_raw_deflate_body_is_decoded() -> None:
    packer = zlib.compressobj(wbits=-zlib.MAX_WBITS)
    raw = packer.compress(b"<html>raw deflate</html>") + packer.flush()
    answer = _http("200 OK", {"Content-Encoding": "deflate", "Content-Length": str(len(raw))}, raw)
    status, out = _fetch(FakeProxy(answer), "http://a.example.com/")
    assert status == 200
    assert base64.b64decode(out["body_b64"]) == b"<html>raw deflate</html>"
    wrapped = zlib.compress(b"<p>zlib</p>")
    decoder = _Decoder("deflate", 1000)
    decoder.feed(wrapped)
    assert decoder.finish() == b"<p>zlib</p>"


def test_unicode_digit_content_length_is_not_an_internal_error() -> None:
    odd = b"HTTP/1.1 200 OK\r\nContent-Length: \xb2\r\n\r\nok"  # latin-1 superscript two: isdigit, not int
    status, _ = _fetch(FakeProxy(odd), "http://a.example.com/")
    assert status == 200


def test_utf8_redirect_location_is_followed_as_sent() -> None:
    fetcher = Fetcher(limits=Limits(), resolver=lambda _h, _p: ["93.184.216.34"])
    targets: list[str] = []

    def hops(target, _deadline):  # type: ignore[no-untyped-def]
        targets.append(target.request_target)
        if len(targets) == 1:
            # http.client hands header bytes over as latin-1: the UTF-8 "/caf\u00e9" arrives as mojibake.
            return 302, {"location": "/caf\u00e9".encode().decode("latin-1")}, b""
        return 200, {}, b"ok"

    fetcher._request = hops  # type: ignore[method-assign]
    assert fetcher.fetch("https://a.example.com/start").final_url == "https://a.example.com/caf\u00e9"
    assert targets[1] == "/caf%C3%A9"


def test_socket_is_closed_when_the_connection_setup_fails() -> None:
    denied = _http("403 Forbidden", {"X-Squid-Error": "ERR_ACCESS_DENIED 0", "Content-Length": "0"}, b"")
    proxy = FakeProxy(denied)
    fetcher = proxy.fetcher()
    opened: list[socket.socket] = []
    real_open = Fetcher._open

    def tracked(host: str, port: int, deadline: Any) -> socket.socket:
        sock = real_open(host, port, deadline)
        opened.append(sock)
        return sock

    fetcher._open = tracked  # type: ignore[method-assign]
    try:
        with pytest.raises(BlockedError):
            fetcher.fetch("https://a.example.com/")
    finally:
        proxy.close()
    assert len(opened) == 1
    assert opened[0].fileno() == -1


def test_proxy_name_lookup_is_bounded_by_the_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    def slow(_host: str, _port: int) -> list[str]:
        time.sleep(3)
        return ["127.0.0.1"]

    monkeypatch.setattr(app, "system_resolver", slow)
    proxy = Proxy("egress-fetch", 3128)
    fetcher = Fetcher(
        limits=Limits(timeout_s=0.3), resolver=_no_dns, proxies=Proxies(http=proxy, https=proxy)
    )
    started = time.monotonic()
    body = b'{"item_id": "rss:abc", "url": "https://a.example.com/"}'
    status, answer = handle_fetch(fetcher, body)
    assert (status, answer["reason"]) == (422, "timeout")
    assert time.monotonic() - started < 2


def test_fetch_finishing_past_its_deadline_is_a_timeout() -> None:
    fetcher = Fetcher(limits=Limits(timeout_s=0.2), resolver=lambda _h, _p: ["93.184.216.34"])

    def late(_target, _deadline):  # type: ignore[no-untyped-def]
        time.sleep(0.4)
        return 200, {}, b"late"

    fetcher._request = late  # type: ignore[method-assign]
    with pytest.raises(BlockedError) as err:
        fetcher.fetch("https://a.example.com/")
    assert err.value.reason == "timeout"
