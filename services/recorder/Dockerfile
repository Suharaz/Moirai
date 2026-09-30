# recorder: CMC + Binance public ingestion into the immutable lake (phase 02).
# Command (docker-compose.yml): python -m hdt.ingest.scheduler. Writes /app/data (named volume lake_data).
FROM base
LABEL org.opencontainers.image.title="hdt-recorder"
ENV HDT_METRICS_PORT=9102
EXPOSE 9102
HEALTHCHECK --interval=30s --timeout=10s --start-period=90s --retries=3 \
    CMD ["python", "-m", "hdt.ops.healthcheck", "http://127.0.0.1:9102/metrics"]
