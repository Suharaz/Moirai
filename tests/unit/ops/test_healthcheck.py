"""Container healthcheck: `--fresh` turns a live /metrics endpoint with a stale heartbeat unhealthy."""

from __future__ import annotations

import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from hdt.ops.healthcheck import check

GAUGE = "hdt_telegram_poll_heartbeat_timestamp_seconds"
NOW = 1_800_000_000.0


class _Metrics(BaseHTTPRequestHandler):
    body = b""

    def do_GET(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.end_headers()
        self.wfile.write(type(self).body)

    def log_message(self, format: str, *args: object) -> None:
        return


@pytest.fixture
def metrics_url() -> Iterator[str]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Metrics)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}/metrics"
    finally:
        server.shutdown()
        server.server_close()


def _serve(heartbeat: float | None) -> None:
    lines = [f"# TYPE {GAUGE} gauge"]
    if heartbeat is not None:
        lines.append(f"{GAUGE} {heartbeat!r}")
    lines.append('hdt_telegram_loop_errors_total{stage="iteration"} 3.0')
    _Metrics.body = ("\n".join(lines) + "\n").encode()


@pytest.mark.parametrize(
    ("heartbeat", "healthy"),
    [(NOW - 179, True), (NOW - 181, False), (0.0, False), (None, False)],
    ids=["fresh", "stale", "never_set", "absent"],
)
def test_heartbeat_freshness(metrics_url: str, heartbeat: float | None, healthy: bool) -> None:
    _serve(heartbeat)
    assert check(metrics_url, fresh_gauge=GAUGE, max_age_s=180, now=NOW)[0] is healthy


def test_plain_probe_ignores_the_heartbeat(metrics_url: str) -> None:
    _serve(NOW - 10_000)
    assert check(metrics_url, now=NOW) == (True, "HTTP 200")
