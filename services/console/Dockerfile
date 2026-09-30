# console: Streamlit admin console, tunnel-only, password + TOTP through config-api (phase 12).
# Command (docker-compose.yml): streamlit run src/hdt/console/app.py. Script-level Streamlit config:
# src/hdt/console/.streamlit/config.toml.
FROM base
LABEL org.opencontainers.image.title="hdt-console"
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0 \
    STREAMLIT_SERVER_PORT=8501 \
    STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false
EXPOSE 8501
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD ["python", "-m", "hdt.ops.healthcheck", "http://127.0.0.1:8501/_stcore/health"]
