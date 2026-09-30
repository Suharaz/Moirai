# config-api: FastAPI admin API (auth, TOTP, CSRF, config versions, sealed secrets) (phase 12).
# Command (docker-compose.yml): python -m hdt.configapi.main. Holds no scope private key.
FROM base
LABEL org.opencontainers.image.title="hdt-config-api"
ENV HDT_CONFIGAPI_HOST=0.0.0.0 \
    HDT_CONFIGAPI_PORT=8081
EXPOSE 8081
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD ["python", "-m", "hdt.ops.healthcheck", "http://127.0.0.1:8081/api/health"]
