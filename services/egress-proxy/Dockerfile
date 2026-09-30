# egress-proxy: Squid allowlist proxy, the only container on a network with a route to the internet
# (besides caddy for ACME). Configuration: deploy/squid/squid.conf + deploy/squid/allowlist/, mounted
# read-only at /etc/hdt-squid by docker-compose.yml. Runs as the unprivileged `proxy` user with no cache
# directory; /run/hdt-squid is a tmpfs, so the root filesystem can be read-only.
FROM debian:trixie-slim@sha256:a99cfc517144bc59b1978475ec53b46ecabec7e43635402ee5b77cc54cd1b20a
RUN apt-get update \
    && apt-get install -y --no-install-recommends squid ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && squid -v
COPY --chmod=0755 services/egress-proxy/entrypoint.sh /usr/local/bin/egress-proxy-entrypoint
LABEL org.opencontainers.image.title="hdt-egress-proxy"
USER proxy
EXPOSE 3128
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD ["bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/3128"]
ENTRYPOINT ["/usr/local/bin/egress-proxy-entrypoint"]
