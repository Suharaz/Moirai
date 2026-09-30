# fetcher: sandboxed original-article fetcher (phase 07, design contract section 10).
# Standard library only: no third-party package, no project code, no config, no secret. Compose runs it on
# net_fetch alone (council -> fetcher -> egress-fetch), read-only root filesystem, no volume, capped pids.
# The same digest-pinned Python image as services/base.
ARG PYTHON_IMAGE=python:3.12.14-slim-trixie@sha256:f77ac9e44ae96ef2c90b8053ea08c31f8be030f824196b0ae4db6d462c84e51f

FROM ${PYTHON_IMAGE}
LABEL org.opencontainers.image.title="hdt-fetcher"
RUN groupadd --gid 10002 fetcher \
    && useradd --uid 10002 --gid 10002 --no-create-home --home-dir /nonexistent --shell /usr/sbin/nologin fetcher
WORKDIR /app
COPY services/fetcher/guard.py services/fetcher/app.py ./
RUN python -m compileall -q /app
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HOME=/tmp \
    TZ=UTC \
    HDT_FETCHER_PORT=8080
USER fetcher
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["python", "-c", "import http.client,sys; c=http.client.HTTPConnection('127.0.0.1', 8080, timeout=3); c.request('GET', '/healthz'); sys.exit(0 if c.getresponse().status == 200 else 1)"]
CMD ["python", "/app/app.py"]
