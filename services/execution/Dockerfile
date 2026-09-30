# execution: only caller of signed Binance endpoints, ledger, reconcile, kill switch (phase 09).
# Command (docker-compose.yml): python -m hdt.execution.main. Scope `exec` + OrderIntent verify keys. Independent kill path: `docker compose exec execution hdt-kill` (docs/runbook.md section 6).
FROM base
LABEL org.opencontainers.image.title="hdt-execution"
ENV HDT_METRICS_PORT=9464
EXPOSE 9464
HEALTHCHECK --interval=30s --timeout=10s --start-period=90s --retries=3 \
    CMD ["python", "-m", "hdt.ops.healthcheck", "http://127.0.0.1:9464/metrics"]
