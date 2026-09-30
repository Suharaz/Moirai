# caddy: the only published port (443). Built with xcaddy to add rate limiting and an HTTP cache:
# - github.com/mholt/caddy-ratelimit v0.1.0      (per-client-IP request rate limit)
# - github.com/caddyserver/cache-handler v0.17.0 (HTTP cache for the dashboard's static assets)
# - github.com/darkweak/storages/otter/caddy v0.0.20 (bounded in-memory storage for that cache)
# Runs as the unprivileged user `caddy` (10001) and listens on 8443 inside the container.
FROM caddy:2.11.4-builder@sha256:369218c81ca6d6af249981221b3a5c764d886dd5b058f51d144066de13f2418d AS build
RUN xcaddy build v2.11.4 \
    --with github.com/mholt/caddy-ratelimit@v0.1.0 \
    --with github.com/caddyserver/cache-handler@v0.17.0 \
    --with github.com/darkweak/storages/otter/caddy@v0.0.20

FROM caddy:2.11.4@sha256:0c994536bddb66445885237f1a5dcc1916bccea922661c76b4e9fc24061f9b52
COPY --from=build /usr/bin/caddy /usr/bin/caddy
# Only unprivileged ports are used, so drop the base image's cap_net_bind_service file capability:
# under cap_drop ALL + no-new-privileges the kernel refuses to exec a binary that carries it.
RUN addgroup -S -g 10001 caddy \
    && adduser -S -D -H -u 10001 -G caddy -s /sbin/nologin caddy \
    && install -d -o caddy -g caddy -m 0700 /data/caddy /config/caddy \
    && setcap -r /usr/bin/caddy \
    && test -z "$(getcap /usr/bin/caddy)" \
    && test "$(caddy list-modules | grep -cE '^http\.handlers\.(rate_limit|cache)$')" -eq 2
LABEL org.opencontainers.image.title="hdt-caddy"
USER caddy
EXPOSE 8443
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD ["wget", "-q", "-O", "/dev/null", "http://127.0.0.1:8081/health"]
