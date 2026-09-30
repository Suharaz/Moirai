# migrate: one-shot `alembic upgrade head` as role hdt_migrator before any service starts.
# Command (docker-compose.yml): alembic upgrade head. Every service that uses Postgres waits for it to
# complete successfully.
FROM base
LABEL org.opencontainers.image.title="hdt-migrate"
