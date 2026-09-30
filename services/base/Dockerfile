# Shared runtime image of every Python service (compose service `hdt-base`, never started itself).
# Service images build `FROM base` (compose additional context `service:hdt-base`) and add their own
# port, healthcheck and labels; the command is set in docker-compose.yml.
#
# - dependencies come only from the hash-pinned lock: `uv sync --frozen --no-dev` (the `fetcher` group
#   is not installed; it belongs to the sandboxed fetcher image)
# - base images are pinned by digest
# - the image runs as the unprivileged user `hdt` (10001) and works on a read-only root filesystem:
#   bytecode is compiled at build time, HOME and temporary files live on the /tmp tmpfs
# - /app/data (lake + staging) is owned by `hdt`, so a new named volume mounted there inherits it
ARG PYTHON_IMAGE=python:3.12.14-slim-trixie@sha256:f77ac9e44ae96ef2c90b8053ea08c31f8be030f824196b0ae4db6d462c84e51f

FROM ghcr.io/astral-sh/uv:0.6.9@sha256:cbc016e49b55190e17bfd0b89a1fdc1a54e0a54a8f737dfacc72eca9ad078338 AS uv

FROM ${PYTHON_IMAGE} AS builder
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PYTHON=/usr/local/bin/python3.12 \
    UV_PROJECT_ENVIRONMENT=/app/.venv
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY README.md alembic.ini ./
COPY src ./src
RUN uv sync --frozen --no-dev

FROM ${PYTHON_IMAGE} AS runtime
# libgomp1: OpenMP runtime required by the LightGBM wheel.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 hdt \
    && useradd --uid 10001 --gid 10001 --no-create-home --home-dir /nonexistent --shell /usr/sbin/nologin hdt
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/src /app/src
COPY README.md alembic.ini ./
COPY config ./config
COPY scripts ./scripts
ENV PATH=/app/.venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HDT_CONFIG_DIR=/app/config \
    HOME=/tmp \
    TZ=UTC
RUN python -m compileall -q /app/src /app/scripts \
    && install -d -o hdt -g hdt -m 0755 /app/data /app/data/lake /app/data/staging
USER hdt
