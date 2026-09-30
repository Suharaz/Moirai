# risk: risk gate, sizing, limits, veto, OrderIntent signing (phase 09).
# Command (docker-compose.yml): python -m hdt.risk.main. Holds the OrderIntent Ed25519 signing key; no egress.
FROM base
LABEL org.opencontainers.image.title="hdt-risk"
ENV HDT_METRICS_PORT=9464
EXPOSE 9464
HEALTHCHECK --interval=30s --timeout=10s --start-period=90s --retries=3 \
    CMD ["python", "-m", "hdt.ops.healthcheck", "http://127.0.0.1:9464/metrics"]
