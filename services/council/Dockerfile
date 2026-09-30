# council: scanner, six agents, LangGraph council, decision relay (phases 03, 05, 06).
# Command (docker-compose.yml): python -m hdt.council.main. Scope `llm`; networks net_data, net_fetch, net_trace, net_proxy.
FROM base
LABEL org.opencontainers.image.title="hdt-council"
ENV HDT_METRICS_PORT=9464
EXPOSE 9464
HEALTHCHECK --interval=30s --timeout=10s --start-period=90s --retries=3 \
    CMD ["python", "-m", "hdt.ops.healthcheck", "http://127.0.0.1:9464/metrics"]
