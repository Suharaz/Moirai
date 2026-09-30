"""Prometheus series of the news worker and the veto scan (exported by `hdt.ops.metrics.serve_metrics`)."""

from __future__ import annotations

from prometheus_client import Counter, Gauge

INGESTED = Counter("hdt_news_items_ingested_total", "News items stored (producer: news)", ["source", "kind"])
SOURCE_ERRORS = Counter(
    "hdt_news_source_errors_total", "Failed news source polls (producer: news)", ["source"]
)
SOURCE_LAST_SUCCESS = Gauge(
    "hdt_news_source_last_success_timestamp_seconds",
    "Unix time of the last successful poll per source (producer: news)",
    ["source"],
)
VERDICTS = Counter(
    "hdt_news_verdicts_total", "Item verdicts written (producer: news, veto_scan)", ["processor", "status"]
)
CANDIDATES_PUBLISHED = Counter(
    "hdt_news_candidates_total", "HOLLOW_HYPE candidates published (producer: news)", ["mode"]
)
VETO_FLAGS = Counter("hdt_veto_scan_flags_total", "RiskFlags published (producer: veto_scan)", ["kind"])
VETO_SCAN_LAST = Gauge(
    "hdt_veto_scan_last_timestamp_seconds", "Unix time of the last completed veto scan (producer: veto_scan)"
)
