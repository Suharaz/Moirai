# public-publisher: allowlisted public snapshot writer (phase 10).
# Command (docker-compose.yml): python -m hdt.public.publisher. Read-only DB role; write user of the public-store bucket; networks net_data, net_store_write.
FROM base
LABEL org.opencontainers.image.title="hdt-public-publisher"
ENV HDT_METRICS_PORT=9464
EXPOSE 9464
HEALTHCHECK --interval=30s --timeout=10s --start-period=90s --retries=3 \
    CMD ["python", "-m", "hdt.ops.healthcheck", "http://127.0.0.1:9464/metrics"]
