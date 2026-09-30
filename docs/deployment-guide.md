# Deployment Guide

## 1. Development setup
Prerequisites: uv (installs Python 3.12), Docker with compose v2, git.
```bash
uv python install 3.12
uv sync --frozen                                  # installs runtime + dev dependencies from the hashed lock
cp .env.example .env                              # fill bootstrap values for local dev only
uv run python scripts/check_env.py                # prints present/missing per variable, never values
docker compose -f docker-compose.dev.yml up -d    # Postgres 16 (WAL archiving) + Redis 7 (AOF everysec, ACL)
uv run alembic upgrade head                       # business tables
uv run pytest                                     # unit + contract + pg/redis tests
uv run python scripts/check_language.py           # English-only check
uv run pre-commit install                         # detect-secrets, ruff, language check
```
Dev Postgres creates one role per service (`deploy/postgres/init/`); dev Redis loads a per-service ACL file generated from `deploy/redis/users.acl.template`.

## 2. Secrets model
| Kind | Examples | Where it lives | Who can read it |
|:--|:--|:--|:--|
| Bootstrap | per-role Postgres DSNs, Redis ACL users, scope private keys, session secret, OrderIntent Ed25519 signing key, TLS | sops-encrypted file (`secrets/*.enc.yaml`, age), decrypted at deploy time and mounted as docker secrets | only the service that needs each file |
| Application | CMC key, OpenRouter key, Binance testnet/live key + secret, Telegram token + allowed user ids, news feed key | Postgres `secrets` table, sealed in the browser with the scope public key (PyNaCl/libsodium SealedBox) | only the scope owner: `data` -> recorder, news; `llm` -> council, scorer, news; `exec` -> execution; `ops` -> telegram-bot |
Rules: never send secrets over chat; `.env` is for local development only and is git-ignored; config-api and console hold no private key; there is no endpoint to read a secret back.

Scope key pairs are generated with `scripts/gen_scope_keys.py` (phase 12): private keys go into the sops file, public keys into `config/vault_public_keys.yaml`. The first admin user is created on the VPS with `scripts/admin_create.py`, which prints the TOTP URI to the terminal only.

## 3. Production (phase 10)
### 3.1 Target
- Linux VPS in Tokyo or Singapore, 4 vCPU / 8 GB RAM to start, disk >= 200 GB, NTP enabled, Docker Engine with compose v2.
- Docker Compose (`docker-compose.yml`) with every image pinned by digest; services and networks per `docs/system-architecture.md` section 2.
- Firewall: only `443` on the public interface. SSH (key-only, fail2ban), console (`:8501`), Grafana (`:3000`) and Langfuse (`:3100`) bind to the Tailscale address only; config-api has no host port (the console is its only client).
- Public edge (`deploy/caddy/Caddyfile`): `/` serves the static landing page from `web/landing` (mounted read-only into caddy at `/srv/landing`, so the VPS checkout must include it); `/app` and `/app/*` proxy to the public dashboard, which Streamlit serves under `baseUrlPath = "app"` (health at `/app/_stcore/health`). Rebuild the `dashboard` image together with the Caddy change: an older image still serves the dashboard at `/` and passes its own old healthcheck, so Compose reports it healthy while every `/app/*` request Caddy forwards to it fails.
- Egress only through the allowlist proxies (`deploy/squid`): each client has a fixed address and its own destination list (CMC, Binance, OpenRouter, Telegram, news sources, the backup bucket); execution egresses from the VPS IP whitelisted on the Binance key. `risk` has no egress at all.
- Durability: Postgres WAL archiving + PITR, Redis AOF everysec, client-side encrypted (age) backups with write-only credentials, lake sync to object storage, monthly restore drill (`docs/runbook.md` section 14).
- The recorder is deployed first (right after phase 02) so the lake starts accumulating; CMC derivatives data has no history, so any unrecorded day is lost.

### 3.2 First deployment
```bash
# Host: Docker, Tailscale (tailscale up), ufw allow in on tailscale0; ufw allow 443/tcp; ufw default deny incoming
sudo git clone <repo> /opt/hdt && cd /opt/hdt
cp .env.prod.example .env.prod                      # hostname, ACME e-mail, Tailscale IP, backup target, age recipients
uv run python scripts/gen_scope_keys.py              # scope key pairs (once; commit config/vault_public_keys.yaml)
uv run python -m hdt.risk.signer generate --key-id risk-1 --accounts paper,testnet,live # phase 09 keys:
uv run python -m hdt.risk.signer generate --key-id driver-1 --accounts testnet          # see below
uv run python scripts/gen_prod_secrets.py            # every random secret + list of missing operator files
docker compose --env-file .env.prod build
docker compose --env-file .env.prod up -d           # services whose code exists; later phases add --profile ...
docker compose --env-file .env.prod ps              # every service healthy
docker compose --env-file .env.prod exec telegram-bot python -m hdt.ops.alerts test   # one alert per kind
```
Operator-provided files in `secrets/` (the generator lists the missing ones):
- `scope_private_key.<service>.json`: written by `scripts/gen_scope_keys.py`.
- `order_signing_key` (risk) and `testnet_driver_signing_key`: the first line printed by `python -m hdt.risk.signer generate` for each key; `order_verify_keys.json` (execution): `{"keys": [<risk entry>, <driver entry>]}` from the second lines.
- `backup_s3_access_key`, `backup_s3_secret_key`: a key that may only put objects into the backup bucket. Uploads are create-only (`If-None-Match: *`); configure the bucket to require that header and to apply object lock plus retention. The read key and the age identity for restores stay offline. `HDT_BACKUP_S3_ENDPOINT` must be `https://<DNS name>` with no port or port 443 (egress-proxy refuses to start otherwise) and `HDT_BACKUP_HOUR_UTC` a decimal hour 0-23 (the backup service refuses to start otherwise).
- `alertmanager_deadman_url`: the ping URL of an external dead-man check (default provider hc-ping.com, allowlisted in `deploy/squid/allowlist/deadman.txt`); Alertmanager pings it every minute with the always-firing `Watchdog` (`docs/runbook.md` section 9).
- Files are mode 0444 inside a 0700 `secrets/` directory (the generator sets this): containers run as unprivileged users. Keep an encrypted copy with sops (`secrets/*.enc.yaml`, age) and never commit plaintext.

Compose profiles: `council` (council), `news` (news, veto-scan, fetcher, egress-fetch), `scoring` (scorer), `trading` (risk, execution). Enable a profile only after its phase is merged. `testnet-driver` is a one-shot CLI in its own profile, never restarted and never scraped: `docker compose --env-file .env.prod --profile testnet-driver run --rm testnet-driver ...`.
Network rules kept by `tests/integration/test_public_isolation.py`: `net_fetch` holds only the council, the fetcher and egress-fetch (news reaches the fetcher through the council); fixed addresses (Squid clients, monitoring listeners) lie below `.128` and every such network hands out dynamic addresses from `.128/25` only; every container has `mem_limit` and `pids_limit` (defaults 512m / 256 in `x-hardened`, per-service overrides for Postgres, Redis, the council, ClickHouse and the dashboard, sized for the 8 GB host); Redis runs with `--maxmemory 800mb`, langfuse-redis with 400mb.
Monitoring: Prometheus scrapes the services, `postgres-exporter` (role `hdt_monitor`), `redis-exporter` (ACL user `monitor`), `blackbox` (config-api, console, egress-proxy) and `blackbox-edge` (caddy, dashboard; on `net_edge` and `net_probe_edge` only). Exporters, probers and Alertmanager listen only on their fixed monitoring address. Their passwords come from `scripts/gen_prod_secrets.py`; an existing cluster needs `docker compose exec postgres bash /docker-entrypoint-initdb.d/02-ops-roles.sh` once (`docs/runbook.md` section 13).
LLM services receive `LANGFUSE_BASE_URL` (`http://langfuse-web:3000`) and `LANGFUSE_PUBLIC_KEY_FILE` / `LANGFUSE_SECRET_KEY_FILE` (read with `read_env_value`); Langfuse creates the project with those keys on first start.

### 3.3 Operations
Incidents, backups, restore drills and upgrades: `docs/runbook.md`.

### 3.4 Shared host behind its own web server
On a shared VPS whose own web server already owns 80/443 (other projects on the same host, no Tailscale), Moirai runs as its own compose project in `/opt/<project>` and changes nothing outside it except one reverse-proxy site.
```bash
cd /opt/<project>
DC="docker compose -p <project> --env-file .env.prod -f docker-compose.yml -f docker-compose.behind-proxy.yml"
$DC --profile council --profile news --profile scoring --profile trading --profile testnet-driver build
$DC up -d <service ...>          # always --force-recreate after an image tag change, never `restart`
```
- `.env.prod`: `HDT_TAILSCALE_IP=127.0.0.1` (console, Langfuse and Grafana on loopback, reached by SSH tunnel), `HDT_GRAFANA_URL` on the loopback Grafana port, `HDT_ACME_EMAIL` is required but unused (Caddy runs with `auto_https off`). With the backup service off, `HDT_BACKUP_TARGET` / `HDT_BACKUP_AGE_RECIPIENTS` still need placeholder values for interpolation.
- Services you do not start (for example `telegram-bot`, `backup`, `alertmanager`) need no secret files; compose only needs them when those services are started.
- Host web server site `<your-host>`: reverse proxy `/` to the loopback port published by `docker-compose.behind-proxy.yml`, with WebSocket upgrade (the dashboard needs `/app/_stcore/stream`), an HTTPS certificate and an HTTP to HTTPS redirect.
- Data move from another host: stop every writer there first; restore with `migrate` to head on an empty database, then a data-only `pg_restore --disable-triggers --single-transaction` of a dump taken with `--data-only --exclude-table-data=alembic_version --exclude-table-data=checkpoint_migrations` (migrations seed `checkpoint_migrations`); compare exact row counts of every table. The lake goes into the `<project>_lake_data` volume (owner uid 10001). The old host's `session_secret` must come along, or the admin TOTP seeds cannot be opened; the vault needs the same scope private keys.
- Console admins are created inside config-api: `docker compose -p <project> exec config-api python scripts/admin_create.py <username>`.

## 4. Run modes
- `paper` always runs when the system runs and is the only namespace that counts toward gate G4. It receives the council decisions in `paper` and `live` mode, not in `testnet` mode.
- `testnet` trades the council decisions on the Binance demo account (`demo-fapi.binance.com`) instead of paper (owner decision 2026-09-29): Risk sizes each OPEN from the demo `AccountState` equity with `size_multiplier.testnet`, and signs with the Risk key, which must be granted `testnet` in `order_verify_keys.json`. Testnet results never count toward G4, and G4 accumulates nothing while the mode is `testnet`. Demo prices and liquidity differ from mainnet, so the price guards (Risk on the mainnet mark, execution on the demo mark) can reject an entry the paper venue would take.
  The synthetic order lifecycle driver also trades the testnet namespace. Offline, it runs against the fake exchange in the default test suite. The phase 09 acceptance on the demo account (20 consecutive lifecycles through the execution service, STOP p99 <= 2 s) is `HDT_TESTNET_API_KEY=... HDT_TESTNET_API_SECRET=... uv run pytest -m testnet` (also read from the dev `.env`); without the keys it is collected and skipped. The demo account must be in One-way mode and flat on the driver's symbol, so run the driver while no council position is open on that symbol.
- `live` is enabled only through the console live gate (G4 flag, valid live key with trade-only permission and IP whitelist, completed go-live checklist, TOTP) with `size_multiplier` <= 1 (default 0.25).

## 5. Emergency procedures
- Kill switch: console, Telegram (`/kill`, 2 steps), or the independent `hdt-kill` CLI over SSH (`docker exec` into `execution`), which works when Redis and Telegram are down.
- Resume is refused after a daily-loss kill within the same UTC day, and whatever the kill cause while the latest reconcile run is not clean.
- Runbook: `docs/runbook.md` covers dead routes, credit overrun, reconcile mismatch, exchange maintenance, stale public snapshots, host resources, backups, the monthly restore drill and ledger rebuild (restore + reconciler replay of `userTrades` / `allOrders` / `allAlgoOrders`).
