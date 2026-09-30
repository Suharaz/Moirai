# telegram-bot: operator commands, alert delivery, alert rule monitor (phase 10).
# Command (docker-compose.yml): python -m hdt.ops.telegram_bot. Scope `ops` + /resume TOTP seed.
FROM base
LABEL org.opencontainers.image.title="hdt-telegram-bot"
ENV HDT_METRICS_PORT=9464
EXPOSE 9464
HEALTHCHECK --interval=30s --timeout=10s --start-period=90s --retries=3 \
    CMD ["python", "-m", "hdt.ops.healthcheck", "http://127.0.0.1:9464/metrics", \
         "--fresh", "hdt_telegram_poll_heartbeat_timestamp_seconds", "--max-age", "180"]
