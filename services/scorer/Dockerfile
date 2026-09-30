# scorer: resolver, learning of w/a/r, calibration, reflection (phase 08).
# Command (docker-compose.yml): python -m hdt.scoring.main. Scope `llm`.
FROM base
LABEL org.opencontainers.image.title="hdt-scorer"
ENV HDT_METRICS_PORT=9464
EXPOSE 9464
HEALTHCHECK --interval=30s --timeout=10s --start-period=90s --retries=3 \
    CMD ["python", "-m", "hdt.ops.healthcheck", "http://127.0.0.1:9464/metrics"]
