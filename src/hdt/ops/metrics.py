"""Prometheus exporter and the shared metric catalog.

Every service calls `serve_metrics("<service>")` once at startup: it starts the prometheus_client HTTP
exporter on `HDT_METRICS_PORT` (default 9464) inside the container, on `net_data` only; Prometheus is the
only scraper and no metrics port is ever published on the host.

Metric objects are process-global. Metrics owned by one module are defined next to the code (the recorder
credit meter defines `hdt_cmc_*`); the cross-service metrics that `deploy/grafana/dashboards` and
`deploy/prometheus/alerts.yml` rely on are defined once here so every producer uses the same names and
labels. Owner service per metric is given in each help text:

| Concept (phase 10 metrics list) | Metric | Producer |
|:--|:--|:--|
| latency per route | `hdt_route_latency_seconds{source,route}` | recorder |
| WS gaps | `hdt_ws_gaps_total{connection}` | recorder |
| projected CMC credits | `hdt_cmc_credit_projection_fraction` (credit_meter.py) | recorder |
| LLM tokens / cost | `hdt_llm_tokens_total`, `hdt_llm_cost_usd_total` | council, news, scorer |
| event count | `hdt_council_events_total{source,outcome}` | council |
| agreement rate | `hdt_council_round1_consensus_total` / events | council |
| failed meetings | `hdt_council_events_failed_total` | council |
| scanner relay failures | `hdt_scanner_relay_failures_total` | council |
| w / a / r per agent | `hdt_agent_weight{agent,target_type,param}` | scorer |
| PnL, drawdown, position count | `hdt_account_*`, `hdt_open_positions` | execution |
| reconcile mismatches | `hdt_reconcile_mismatches_total{account}` | execution |
| entries refused by the price guard | `hdt_entries_refused_total{account,reason}` | execution |
| fetcher blocks | `hdt_fetch_blocked_total{reason}` | live `fetch_source` tool (council) |
| telegram-bot loop alive | `hdt_telegram_poll_heartbeat_timestamp_seconds` | telegram-bot |
| public snapshot freshness | `hdt_public_snapshot_last_success_timestamp_seconds` | public-publisher |
"""

from __future__ import annotations

import logging
import os
from typing import Final

from prometheus_client import Counter, Gauge, Histogram, Info, start_http_server

log = logging.getLogger(__name__)

METRICS_PORT_ENV: Final[str] = "HDT_METRICS_PORT"
DEFAULT_METRICS_PORT: Final[int] = 9464
METRICS_ADDR_ENV: Final[str] = "HDT_METRICS_ADDR"

SERVICE_INFO = Info("hdt_service", "Service identity of this process")

# recorder
ROUTE_LATENCY = Histogram(
    "hdt_route_latency_seconds",
    "Recorder request latency per data route (producer: recorder)",
    ["source", "route"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0),
)
WS_GAPS = Counter(
    "hdt_ws_gaps_total", "WebSocket sequence gaps detected (producer: recorder)", ["connection"]
)

# council / news / scorer (LLM via OpenRouter, figures from `usage`)
LLM_TOKENS = Counter(
    "hdt_llm_tokens_total",
    "LLM tokens from OpenRouter usage (producer: council, news, scorer); kind=prompt|completion",
    ["pipeline", "role", "model", "kind"],
)
LLM_COST = Counter(
    "hdt_llm_cost_usd_total",
    "LLM cost in USD from OpenRouter usage.cost (producer: council, news, scorer)",
    ["pipeline", "role", "model"],
)
COUNCIL_EVENTS = Counter(
    "hdt_council_events_total",
    "Council events by source and outcome (producer: council)",
    ["source", "outcome"],
)
COUNCIL_ROUND1_CONSENSUS = Counter(
    "hdt_council_round1_consensus_total",
    "Council events whose blind round already had a 2/3 weighted majority (producer: council)",
    ["source"],
)
COUNCIL_EVENTS_FAILED = Counter(
    "hdt_council_events_failed_total",
    "Council meetings marked failed after their last attempt (producer: council)",
)
SCANNER_RELAY_FAILURES = Counter(
    "hdt_scanner_relay_failures_total",
    "Emitted scanner candidates whose XADD or published mark failed; retried next run (producer: council)",
)

# scorer
AGENT_WEIGHT = Gauge(
    "hdt_agent_weight",
    "Learned agent parameters (producer: scorer); param=w|a|r",
    ["agent", "target_type", "param"],
)

# execution (contract agreed with phase 09; names are stable)
ACCOUNT_EQUITY = Gauge("hdt_account_equity_usd", "Account equity in USD (producer: execution)", ["account"])
ACCOUNT_DAILY_PNL_FRACTION = Gauge(
    "hdt_account_daily_pnl_fraction",
    "PnL since 00:00 UTC as a fraction of day-start equity (producer: execution)",
    ["account"],
)
ACCOUNT_DRAWDOWN = Gauge(
    "hdt_account_drawdown_fraction",
    "Drawdown from the equity peak, <= 0 (producer: execution)",
    ["account"],
)
OPEN_POSITIONS = Gauge("hdt_open_positions", "Open positions (producer: execution)", ["account"])
RECONCILE_MISMATCHES = Counter(
    "hdt_reconcile_mismatches_total",
    "Reconcile runs that found a mismatch (producer: execution)",
    ["account"],
)
ENTRIES_REFUSED = Counter(
    "hdt_entries_refused_total",
    "Entries not sent: the mark had invalidated their levels or was unavailable (producer: execution)",
    ["account", "reason"],
)
KILL_STATE = Gauge(
    "hdt_kill_state", "1 for the current run state of the account (producer: execution)", ["account", "state"]
)

# fetcher caller (the fetcher itself has no route to Prometheus). Every reason is exported at 0 from import
# time, so a missing series means missing instrumentation, not "nothing blocked".
FETCH_BLOCK_REASONS: Final[tuple[str, ...]] = (
    "wrong_item",
    "sha256_mismatch",
    "too_many_redirects",
    "final_url_mismatch",
    "non_http_redirect",
    "redirect_added_query",
)
FETCH_BLOCKED = Counter(
    "hdt_fetch_blocked_total",
    "fetch_source answers refused by the fetch contract check "
    "(producer: the live fetch_source tool, council)",
    ["reason"],
)
for _reason in FETCH_BLOCK_REASONS:
    FETCH_BLOCKED.labels(reason=_reason)

# public publisher
PUBLIC_SNAPSHOT_LAST_SUCCESS = Gauge(
    "hdt_public_snapshot_last_success_timestamp_seconds",
    "Unix time of the last public snapshot written to public-store (producer: public-publisher)",
)
PUBLIC_SNAPSHOT_REJECTED = Counter(
    "hdt_public_snapshot_rejected_total",
    "Snapshots refused by the allowlist / schema / delay validation (producer: public-publisher)",
)
PUBLIC_SNAPSHOT_BYTES = Gauge(
    "hdt_public_snapshot_bytes", "Size of the last published snapshot (producer: public-publisher)"
)

# telegram-bot
TELEGRAM_COMMANDS = Counter(
    "hdt_telegram_commands_total",
    "Telegram commands handled (producer: telegram-bot)",
    ["command", "outcome"],
)
TELEGRAM_POLL_ERRORS = Counter(
    "hdt_telegram_poll_errors_total", "getUpdates failures (producer: telegram-bot)"
)
TELEGRAM_POLL_HEARTBEAT = Gauge(
    "hdt_telegram_poll_heartbeat_timestamp_seconds",
    "Unix time the bot loop last finished an iteration (poll handled, success or handled failure; "
    "also while waiting for the vault secret). Stale means the loop is stuck (producer: telegram-bot)",
)
TELEGRAM_LOOP_ERRORS = Counter(
    "hdt_telegram_loop_errors_total",
    "Bot loop iterations or messages that failed with an unexpected error (producer: telegram-bot)",
    ["stage"],
)
TELEGRAM_WEBHOOK_SET = Gauge(
    "hdt_telegram_webhook_set", "1 when getWebhookInfo reports a webhook URL (producer: telegram-bot)"
)
ALERTS_DELIVERED = Counter(
    "hdt_alerts_delivered_total", "Alert notifications sent to Telegram (producer: telegram-bot)", ["phase"]
)
ALERTS_DELIVERY_FAILURES = Counter(
    "hdt_alerts_delivery_failures_total", "Alert notifications that failed to send (producer: telegram-bot)"
)


def metrics_port() -> int:
    raw = os.environ.get(METRICS_PORT_ENV)
    if raw is None or raw == "":
        return DEFAULT_METRICS_PORT
    port = int(raw)
    if not 1 <= port <= 65535:
        raise ValueError(f"{METRICS_PORT_ENV} must be a TCP port, got {raw!r}")
    return port


def serve_metrics(service: str) -> int:
    """Start the exporter once per process (idempotent per port); returns the port."""
    port = metrics_port()
    addr = os.environ.get(METRICS_ADDR_ENV) or "0.0.0.0"  # noqa: S104 - container-internal, net_data only
    SERVICE_INFO.info({"service": service})
    if port in _started:
        return port
    start_http_server(port, addr=addr)
    _started.add(port)
    log.info("metrics exporter listening", extra={"port": port, "service": service})
    return port


_started: set[int] = set()
