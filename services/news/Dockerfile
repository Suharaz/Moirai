# news: news ingestion, fetch through the sandboxed fetcher, extractor and judges (phase 07).
# Command (docker-compose.yml): python -m hdt.news.main. Scopes `data` + `llm`; networks net_data, net_trace, net_proxy
# (not net_fetch: news reaches the fetcher through the council, design contract section 10).
FROM base
LABEL org.opencontainers.image.title="hdt-news"
ENV HDT_METRICS_PORT=9464
EXPOSE 9464
HEALTHCHECK --interval=30s --timeout=10s --start-period=90s --retries=3 \
    CMD ["python", "-m", "hdt.ops.healthcheck", "http://127.0.0.1:9464/metrics"]
