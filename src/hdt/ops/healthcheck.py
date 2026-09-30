"""Container healthcheck probe: `python -m hdt.ops.healthcheck <url> [--timeout S] [--fresh G --max-age S]`.

Used by the `HEALTHCHECK` of every service image (services/*/Dockerfile). Exit code 0 when the URL answers
HTTP 2xx within the timeout, 1 otherwise. Only loopback http URLs are accepted and proxies are never used,
so the probe cannot be pointed at another container or leak through the egress proxy. Standard library
only, so the probe starts fast and never imports service code.

`--fresh GAUGE --max-age S` additionally reads the Prometheus text exposition at the URL and requires the
unlabelled gauge `GAUGE` (a Unix timestamp heartbeat, for example
`hdt_telegram_poll_heartbeat_timestamp_seconds`) to be at most `S` seconds old: a process whose metrics
server is up but whose main loop is stuck or dead is reported unhealthy.
"""

from __future__ import annotations

import argparse
import math
import re
import sys
import time
import urllib.error
import urllib.request
from typing import Final
from urllib.parse import urlsplit

LOOPBACK_HOSTS: Final[frozenset[str]] = frozenset({"127.0.0.1", "localhost", "::1"})
DEFAULT_TIMEOUT_S: Final[float] = 5.0
MAX_BODY_BYTES: Final[int] = 4 * 1024 * 1024
METRIC_NAME: Final[re.Pattern[str]] = re.compile(r"^[a-zA-Z_:][a-zA-Z0-9_:]*$")


def gauge_value(exposition: str, name: str) -> float | None:
    """Value of the unlabelled sample `name` in a Prometheus text exposition, or None when absent."""
    prefix = f"{name} "
    for line in exposition.splitlines():
        if line.startswith(prefix):
            try:
                value = float(line[len(prefix) :].split()[0])
            except (IndexError, ValueError):
                return None
            return value if math.isfinite(value) else None
    return None


def check(
    url: str,
    timeout_s: float = DEFAULT_TIMEOUT_S,
    *,
    fresh_gauge: str | None = None,
    max_age_s: float | None = None,
    now: float | None = None,
) -> tuple[bool, str]:
    """(healthy, reason) for one GET of a loopback http URL, plus the optional heartbeat freshness."""
    parts = urlsplit(url)
    if parts.scheme != "http" or parts.hostname not in LOOPBACK_HOSTS:
        return False, "only http://127.0.0.1, http://localhost or http://[::1] URLs are probed"
    if fresh_gauge is not None and (
        not METRIC_NAME.match(fresh_gauge) or max_age_s is None or max_age_s <= 0
    ):
        return False, "--fresh needs a metric name and a positive --max-age"
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(url, timeout=timeout_s) as response:
            status = int(response.status)
            body = response.read(MAX_BODY_BYTES) if fresh_gauge is not None else b""
    except urllib.error.HTTPError as exc:
        return False, f"HTTP {exc.code}"
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        return False, type(exc).__name__
    if not 200 <= status < 300:
        return False, f"HTTP {status}"
    if fresh_gauge is None or max_age_s is None:
        return True, f"HTTP {status}"
    value = gauge_value(body.decode("utf-8", errors="replace"), fresh_gauge)
    if value is None or value <= 0:
        return False, f"{fresh_gauge} not reported yet"
    age = (time.time() if now is None else now) - value
    if age > max_age_s:
        return False, f"{fresh_gauge} is {age:.0f} s old (max {max_age_s:g} s)"
    return True, f"HTTP {status}, {fresh_gauge} {max(age, 0.0):.0f} s old"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m hdt.ops.healthcheck", description=__doc__.splitlines()[0]
    )
    parser.add_argument("url")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_S)
    parser.add_argument("--fresh", metavar="GAUGE", help="heartbeat gauge (Unix time) that must be recent")
    parser.add_argument("--max-age", type=float, metavar="SECONDS", help="maximum age of the --fresh gauge")
    args = parser.parse_args(argv)
    if (args.fresh is None) != (args.max_age is None):
        parser.error("--fresh and --max-age go together")
    healthy, reason = check(args.url, args.timeout, fresh_gauge=args.fresh, max_age_s=args.max_age)
    if not healthy:
        print(f"unhealthy: {reason}", file=sys.stderr)
    return 0 if healthy else 1


if __name__ == "__main__":
    raise SystemExit(main())
