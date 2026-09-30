# dashboard: public read-only performance dashboard behind Caddy (phase 10).
# Command (docker-compose.yml): streamlit run src/hdt/public_dashboard/app.py. Script-level Streamlit
# config: src/hdt/public_dashboard/.streamlit/config.toml. Reads only public-store (read-only S3 user); networks net_edge, net_store_read.
FROM base
LABEL org.opencontainers.image.title="hdt-dashboard"
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0 \
    STREAMLIT_SERVER_PORT=8501 \
    STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false
EXPOSE 8501
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD ["python", "-m", "hdt.ops.healthcheck", "http://127.0.0.1:8501/app/_stcore/health"]
