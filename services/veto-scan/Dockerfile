# veto-scan: independent news veto scan, only producer of `risk_flags` (phase 07).
# Command (docker-compose.yml): python -m hdt.news.veto_scan. Scopes `data` + `llm`.
FROM base
LABEL org.opencontainers.image.title="hdt-veto-scan"
ENV HDT_METRICS_PORT=9464
EXPOSE 9464
HEALTHCHECK --interval=30s --timeout=10s --start-period=90s --retries=3 \
    CMD ["python", "-m", "hdt.ops.healthcheck", "http://127.0.0.1:9464/metrics"]
