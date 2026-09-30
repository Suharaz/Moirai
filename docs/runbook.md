# Runbook

Operational procedures for the production stack (`docker-compose.yml`, phase 10). Every alert that reaches
Telegram names an alert kind (`hdt.ops.alerts.AlertKind`); section 2 maps each kind to its procedure.

## 1. Access and conventions
- Reach the VPS over Tailscale only: `ssh ops@<tailscale-name>`. Console `http://<tailscale-ip>:8501`,
  Grafana `:3000`, Langfuse `:3100`. The public IP answers only on `443` (public dashboard).
- The stack lives in `/opt/hdt` (the git checkout). All commands below use
  `alias dc='docker compose --env-file .env.prod'` from that directory; add
  `--profile council --profile news --profile scoring --profile trading` once those services are deployed.
- State of every container: `dc ps -a` (look for `unhealthy`, `restarting`, `exited`). Logs are JSON lines:
  `dc logs --since 30m <service>`.
- Grafana dashboards: "HDT ops" (alerts, host, snapshot, backups, Telegram, fetcher), "HDT recorder"
  (routes, latency, WebSocket gaps, CMC credits, lake disk), "HDT trading" (account, council, agents, LLM cost).
- Test the alert path end to end after any change to the bot or its token:
  `dc exec telegram-bot python -m hdt.ops.alerts test` (one test alert per kind; `--kind <kind>` for one).
- Never paste secrets into chat, tickets or this runbook. Application keys are entered only on the console
  API keys page; bootstrap secrets are files in `secrets/` (section 12).

## 2. Alert kinds
| Kind | Section |
|:--|:--|
| `route_dead` | 3 Dead route |
| `cmc_credit_projection`, `cmc_halt` | 4 CMC credit overrun |
| `reconcile_mismatch`, `stop_invariant`, `account_state_stale` | 5 Reconcile mismatch |
| `kill_switch`, `daily_loss_warning` | 6 Kill switch |
| `ip_banned`, `price_crosscheck`, exchange maintenance notices | 7 Exchange maintenance and bans |
| `public_snapshot_stale` | 8 Public snapshot stale |
| `service_down`, `prometheus_unreachable` | 9 Service down |
| `ram_usage`, `disk_usage` | 10 Host resources |
| `fetcher_blocks` | 11 Fetcher blocks |
| `backup_stale` | 13 Backups |
| `telegram_webhook`, `telegram_polling`, `telegram_recipient_unreachable` | 9 Service down (telegram-bot) |
| `telegram_totp_lockout`, `config_invalid` | 6 Kill switch |
| `intent_signature`, `integrity_error`, `namespace_error`, `liquidation_distance` | 6 Kill switch, then the service log named in the alert |
| `claims_audit` | 11a Claims audit |
| `council_event_failed` | 11b Council delivery |
| `entry_refused` | 11c Entry refused by execution's price guard |
| `weekly_report` | informational: the weekly go-live digest (`scripts/weekly_report.py`); no action |

Episode alerts (`intent_signature`, `integrity_error`, `price_crosscheck`, `liquidation_distance`,
`config_invalid`, `entry_refused`) report one intent, decision, symbol-day or config version and nothing
resolves them: the Telegram bot's alert monitor closes them 24 h after delivery without a RESOLVED message,
so `/status` and the console count only recent ones. An undelivered critical stays open until it is
delivered. An escalated alert (a higher severity raised for the same key) pages again with
`escalated at <time>` and the time it was first raised.

## 3. Dead route (`route_dead`)
A recorder route missed more than 3 cycles. Unrecorded CMC derivatives data cannot be backfilled, so act
within the hour.
1. Grafana "HDT recorder": which route, since when, and whether every route of the same source failed.
2. `dc logs --since 1h recorder | grep -i <route>`: HTTP status, CMC `error_code`, timeouts.
3. Egress: `dc logs --since 1h egress-proxy | grep 'client=10.231.11.10'` shows the recorder's requests;
   `status=TCP_DENIED` means the host is not in `deploy/squid/allowlist/recorder.txt`.
4. One route only with an HTTP 4xx: the endpoint or plan changed; compare with `docs/api-endpoints-design.md`.
   Whole source down: check the provider status page; the recorder retries on its own. A CMC `1006` (the
   key's plan does not include the endpoint) never raises `route_dead`: see "Routes not on the current plan".
5. Recorder stuck or crash-looping: `dc restart recorder`. It resumes every schedule; hourly compaction
   and backfill of Binance klines catch up automatically.
6. Record the gap (route, UTC window, cause) in `docs/project-changelog.md` so later studies can exclude it.

## 4. CMC credit overrun (`cmc_credit_projection`, `cmc_halt`)
- Projection above 85 % of the monthly cycle: the governor already sheds context and DEX routes first and
  derivatives last. Grafana "HDT recorder" > CMC credits shows which routes spend the most.
  Run `dc exec recorder python scripts/pilot_credit_report.py` for the per-route comparison with the
  Design Contract estimate and the ordered cadence proposals.
- Lower a cadence through the console (configuration is versioned; the recorder applies the new version at
  its next cycle boundary). Never raise the budget above the plan limit.
- `cmc_halt` (error 1009 / 1010 / 1011): the key hit its hard cap or IP limit and every CMC route stopped
  without retry. Check the CMC dashboard; resume only after the cycle resets or the plan is changed, by
  saving the key again on the console API keys page (the recorder re-reads `/v1/key/info`).
- Routes not on the current plan: the console Data & credits page lists CMC routes shown as "not on current
  plan" (`route_health.status = disabled`, `last_error` "not on current CMC plan (1006): ..."). This is
  expected, not an outage: no alert, no failure count, no credits, and their data stays empty (the agents
  that need it abstain). Nothing to do; after a plan upgrade each route's next scheduled call succeeds and
  it returns to `ok` by itself (at most one cadence later; one day for daily and `once` routes). Saving a
  new key version on the console API keys page re-runs them within 15 s.

## 5. Reconcile mismatch (`reconcile_mismatch`, `stop_invariant`, `account_state_stale`)
Execution found the exchange and the ledger disagree (or a position without its protective stop) and
engaged the kill switch for that account.
1. Do not resume. Resume is refused anyway until the latest reconcile is clean.
2. Console > Operations shows the reconcile report: symbol, field, ledger value, exchange value.
3. The reconciler repairs the ledger from the exchange on every run (positionRisk, openOrders,
   openAlgoOrders, userTrades since the last seen trade id, cash flows): it runs at execution start,
   after every user-stream reconnect and every `risk.reconcile_interval_s`. `dc restart execution`
   forces a fresh run; check that the next report is clean.
4. Ledger rebuild (the ledger itself is damaged, e.g. after a bad migration or disk fault):
   1. Kill the account (section 6) and stop `execution` and `risk`: `dc stop execution risk`.
   2. Restore the database to just before the damage into a scratch cluster (section 14, base + WAL with
      `--target-time`), and compare the ledger tables with production.
   3. Copy the good rows back with a reviewed SQL script run as `hdt_migrator`, then `dc start risk
      execution`: the reconciler replays `userTrades` and the open orders / algo orders from the exchange
      (`allOrders` / `allAlgoOrders` history) on top of the restored state.
   4. Resume only after a clean reconcile report (console or Telegram `/resume`, 2 steps + TOTP).
5. If the exchange shows a position without a stop, place the stop by hand on Binance first; correctness of
   the ledger comes second to the open risk.

## 6. Kill switch
- Engage from the console, from Telegram (`/kill`, confirm within 60 s), or over SSH when Redis or Telegram
  are down: `dc exec execution hdt-kill --account {paper,testnet,live} --reason "<text>" [--flatten]`
  (`python -m hdt.execution.kill_cli`, phase 09). The CLI talks to Postgres and the exchange directly and
  writes the audit log.
- `daily_loss_warning` fires at -1.5 % of day-start equity; the kill switch engages by itself at the daily
  loss limit and cannot be resumed within the same UTC day.
- Resume only through the console or Telegram `/resume` (2 steps + TOTP from the Telegram TOTP seed); execution
  refuses it after a same-day daily-loss kill, and whatever the recorded kill cause while the latest
  reconcile run is not clean (no clean run after the last mismatch).
- `telegram_totp_lockout`: repeated wrong TOTP codes locked Telegram `/resume`. If nobody on the team
  typed them, treat the Telegram account as compromised: keep every account killed, revoke the bot token
  with BotFather, rotate the Telegram TOTP seed, and resume from the console only.
- `config_invalid` (critical): a stored risk or mode configuration version lies outside the ceilings of
  the running code (for example after an upgrade lowered a ceiling). The services keep running with the
  values clipped to the ceilings. Review the version on the console and save a new valid one.
- `intent_signature` critical, `integrity_error`, `namespace_error`: an order intent failed verification
  or crossed account namespaces. Treat as a possible compromise: kill every account, keep the containers
  for inspection (`dc stop`, not `down`), and read the audit log before anything else. Each forged or
  malformed stream message pages on its own. An `intent_signature` warning is routine: a correctly
  signed intent that execution's own limits refused (the alert names the limit); check the risk limits
  if it repeats.

## 7. Exchange maintenance and bans (`ip_banned`, `price_crosscheck`)
- Scheduled Binance maintenance: pause new entries before the window (`/pause` or the console); open
  positions keep their exchange-side stops. The recorder logs WebSocket gaps during the window (Grafana "WS
  gaps"); they are expected.
- After the window: wait for `hdt_recorder_route_up` to return to 1 for every Binance route, check that
  the next reconcile is clean, then `/resume`.
- `ip_banned` (HTTP 418 / 429 with ban): every signed call stops until the ban expires. Do not restart in a
  loop; each attempt extends the ban. Find the burst in `dc logs execution` and fix it before resuming.
- `price_crosscheck`: mark price from the exchange and the recorded price disagree beyond tolerance. Check
  the recorder (section 3) and the exchange status; entries stay blocked until they agree.

## 8. Public snapshot stale (`public_snapshot_stale`)
The public dashboard shows the last good snapshot with a stale banner; nothing unsafe is exposed.
1. `dc logs --since 30m public-publisher`: a rejected snapshot names the field that failed the allowlist or
   the JSON schema. Never loosen `src/hdt/public/allowlist.py` to make it pass; fix the source data or the
   read model, test with `tests/unit/public/test_snapshot_allowlist.py`, then redeploy.
2. Store unreachable: `dc ps public-store public-store-init`; `dc up -d public-store-init` recreates the
   bucket and users idempotently if the volume was replaced.
   The bucket is versioned with a GOVERNANCE object lock (`HDT_PUBLIC_LOCK_DAYS`, default 1 day) and a
   lifecycle rule, so an overwritten or pruned snapshot survives as an old version. If
   `public-store-init` stops with `bucket hdt-public has no object lock`, the bucket predates that
   requirement (lock can only be chosen at creation): `dc stop public-publisher dashboard public-store`,
   `docker volume rm hdt_public_store_data`, `dc up -d public-store public-store-init public-publisher
   dashboard`; the publisher rewrites every snapshot within a minute.
3. Postgres unreachable: section 9.
4. Check the page from outside: `curl -sS -o /dev/null -w '%{http_code}\n' https://<public-hostname>/`.

Paper is not public (owner decision 2026-09-30): `PUBLIC_ACCOUNTS` is testnet and live, and the schema rejects
a `paper` account anywhere in a snapshot. This closed the residual risk accepted in review cycle 3 m6 (a paper
exit, visible from the paper equity jump, revealing the STOP level of the same event still open on testnet or
live). Never add paper back to `PUBLIC_ACCOUNTS` without re-opening that review.

## 9. Service down (`service_down`, `prometheus_unreachable`, Telegram bot kinds)
1. `dc ps -a <service>`; `dc logs --since 15m <service>`.
2. Crash on start: configuration or secret problem (missing file in `secrets/`, wrong DSN); fix and
   `dc up -d <service>`. Restarting services never loses state: every consumer resumes from its Redis
   consumer group and every writer is idempotent.
3. Postgres or Redis down: every dependent service stops making progress. `dc logs postgres` /
   `dc logs redis`; disk full is the usual cause (section 10).
4. `prometheus_unreachable`: metric alerts (RAM, disk, fetcher, snapshot, backups) are not evaluated until
   Prometheus is back: `dc restart prometheus`.
5. `telegram_webhook`: a webhook was set on the bot token (the bot polls; a webhook means someone else holds
   the token). Rotate the token with BotFather, save it on the console API keys page, then
   `dc restart telegram-bot`.
6. Probes and exporters: services without their own metrics are checked by `blackbox` (config-api,
   console, egress-proxy, which must answer and deny: the probe asks Squid for
   `http://egress-denied.invalid/`, which is never resolved and never leaves the host, and expects 403)
   and `blackbox-edge` (caddy, dashboard); `postgres-exporter` (Postgres role `hdt_monitor`,
   `pg_monitor` only) and `redis-exporter` (ACL user `monitor`, INFO/PING/SLOWLOG only) raise
   `PostgresDown` / `RedisDown`. A failing probe names the service in the alert.
   Exposure kept on purpose (net_monitor only: Prometheus, Grafana, node-exporter, Alertmanager and the
   exporters): `redis-exporter` runs with `-disable-scrape-endpoint`, so it scrapes nothing but its own
   Redis. `postgres-exporter` still answers `/probe?target=...` (no flag disables it): a caller on
   net_monitor can make it connect to a Postgres it names (no `auth_module` is configured). `blackbox`
   `/probe` can be pointed at any URL with its two modules: GET only, and the answer holds the probe
   result (success, status code, timings), never the body. Keep Grafana, the only interactive service on
   net_monitor, admin-only and patched.
7. Telegram bot stalled or failing (`TelegramBotStalled`, `TelegramBotErrors`, `AlertDeliveryFailing`):
   the container healthcheck and the stall alert read the loop heartbeat; `AlertDeliveryFailing` fires
   when notifications kept failing to send for 30 minutes (Telegram unreachable through egress-proxy, a
   revoked token, or no allowed user reachable at all). Telegram alerts may not arrive while they fire,
   so these rules stop the Watchdog (below) and the external dead-man check pages out of band.
   Use `hdt-kill` over SSH if a kill is needed, then `dc logs telegram-bot` and `dc restart telegram-bot`.
8. `telegram_recipient_unreachable` (warning): Telegram refused messages to one allowed user (400/403:
   the user never opened a private chat with the bot, blocked it, or deleted the account). The other
   users still receive every alert; that user is skipped until a send to them succeeds, which resolves
   this alert. Fix: the user sends `/start` to the bot (or unblocks it); remove users who left from the
   allowed list on the console.
9. Dead-man check: Prometheus fires `Watchdog` all the time and Alertmanager sends it every minute
   through egress-proxy to the URL in `secrets/alertmanager_deadman_url` (default provider
   hc-ping.com, allowlisted in `deploy/squid/allowlist/deadman.txt`; another provider needs its host
   there). Configure the check at the provider with a period of 1 min and a grace of 5 min, and route its
   alert to a channel that does not depend on this host. A page from it means Prometheus,
   Alertmanager, egress-proxy, the host, or the Telegram bot (while the rules of item 7 fire) is down.

## 10. Host resources (`ram_usage`, `disk_usage`)
- RAM above 85 %: `docker stats --no-stream` for the largest containers. Langfuse (ClickHouse, web,
  worker) is the first to move: stand it up on a second VPS with the same `langfuse-*` services, point
  `LANGFUSE_BASE_URL` of the LLM services at it over Tailscale, and remove the local Langfuse services.
- Disk at 80 % / 90 %:
  1. `df -h` and `docker system df -v`: the lake (`hdt_lake_data`), Postgres (`hdt_pg_data`),
     `hdt_pg_wal_archive` (grows only when backups fail: section 13), Prometheus (capped at 20 GB / 90 d),
     ClickHouse.
  2. Raw depth older than 90 days is expired by the recorder; confirm the expiry job runs (recorder log).
  3. `docker image prune` removes superseded images. Never delete lake partitions by hand.

## 11. Fetcher blocks (`fetcher_blocks`)
More than 20 article fetches were refused in an hour.
1. `dc logs --since 1h egress-fetch | grep TCP_DENIED` lists refused destinations; the fetcher log gives the
   guard reason (private IP, redirect limit, size, type).
2. A legitimate new news domain: review it against `config/news_sources.yaml` tiering, add it to
   `deploy/squid/allowlist/fetcher.txt`, then `dc restart egress-fetch`.
3. Many blocks to private or metadata addresses: someone is feeding crafted URLs. Keep the block, find the
   source items in the news tables and mark the source untrusted.

## 11a. Claims audit (`claims_audit`)
An agent had more than 5 % of its claims rejected by the council verifier over 7 days (at least 20 claims).
Its weight ceiling is lowered to 0.20 until the flag is reviewed.
1. Console, Agents page: open the agent's rejected claims and their reject reasons.
2. Typical causes: a prompt or model change that makes the agent quote numbers loosely, a stale packet field,
   or a news source whose stored copies changed.
3. After fixing or accepting the cause, record the review (restores the normal ceiling):
   `dc exec scorer python -m hdt.scoring.claims_audit review --agent <agent> --by <you> --note "<why>"`.

## 11b. Council delivery (`council_event_failed`)
- A decision has no time limit (owner decision 2026-09-28): the council waits for the debate result, the
  relay publishes a decision however late, and Risk judges an OPEN by the current mark against its levels.
  The former `council_decision_expired` alert, its metric and the outbox `expired` status are gone. A slow
  debate therefore shows up as Risk rejections `stop_crossed`, `tp_crossed` or `entry_distance` on the
  decision card (the market moved while the council met), not as an alert. Delivery stuck in the outbox shows
  as `decision_outbox` rows left `pending`: check `dc logs --since 1h council` (relay errors, Redis) and
  Redis health; the relay retries every second and Risk dedupes on `event_id`, so a restart is safe.
- `council_event_failed`: a meeting used all its attempts (crashes included). `dc logs --since 1h council`
  gives the exception; the event stays `failed`, is not scored and does not block the next event.
- Stored risk config versions saved before 2026-09-28 still carry `decision_ttl_s`. Versions are immutable,
  so the key is dropped whenever a version is read (one info log `stored risk config carries retired keys`,
  never `config_invalid`), is not shown by the config API and is never written by a save or rollback.

## 11c. Entry refused by execution's price guard (`entry_refused`)
- Execution re-checks every entry (and its IOC fallback) against the current mark right before sending it,
  with Risk's rule (`hdt.core.entry_guard`). The reason is in the alert title, in
  `processed_intents.reason` of the voided `entry` / `entry_ioc` intents and in
  `hdt_entries_refused_total{account,reason}`: `stop_crossed`, `tp_crossed`, `entry_distance` (the market
  moved since Risk approved it, typically after execution was down or lagging on `orders`), `no_mark`
  (the adapter returned no mark: check `dc logs --since 1h execution` for premiumIndex errors or, in paper,
  the recorder's mark feed), `no_stop_plan` (the event's `sl` intent is missing: a Risk bug, check its log).
- No action is needed for the market reasons: nothing was sent and the protective legs of that event never
  act without a position. Risk keeps counting an approved entry execution has not handled yet in its
  portfolio limits for up to 1 day (`PENDING_LOOKBACK`) or until execution marks it done, void or rejected;
  a long execution outage therefore holds that reserved risk until execution returns.

## 11d. LLM gateway: switching roles between OpenRouter and DeepSeek
Each LLM role (6 agents, news extractor, 2 news judges, reflection) carries its own `gateway` in the
`models` config section: `openrouter` (the default and the long-term gateway) or `deepseek` (the temporary
gateway, owner decision 2026-09-28). Switching every role to DeepSeek-V4.1-Flash (`deepseek-flash`):
1. Console, API keys page: pick "DeepSeek (llm)", paste the key, seal it, then press Test. The council calls
   `GET https://api.deepseek.com/user/balance`; `valid` needs `is_available` true (a positive balance).
2. Egress: `api.deepseek.com` is in `deploy/squid/allowlist/llm.txt`; after pulling this version run
   `dc up -d egress-proxy` (or `dc restart egress-proxy`) so Squid loads it.
3. Console, Models page, section "Switch every role to DeepSeek": model `deepseek-flash`, thinking
   `disabled`. Configured roles keep their temperature, `max_tokens` and `timeout_s`; the three inputs apply
   to roles not configured yet. Press "Request model tests for every role", then "Refresh model tests" until
   every role shows `status` `done` and `schema_valid` true.
4. Enter a reason and press "Save the draft as a new models version". Config-api refuses the save with
   `model_test_required` when a changed role has no passing test in the last 24 h.
5. The services apply the change at the next event boundary and `RoleHealth` re-tests every changed role; a
   failing role is closed (its agent abstains with `model_self_test_failed`) and raises `config_invalid`.

Notes:
- Thinking (`low`, `high`, `max`) makes DeepSeek ignore `temperature`, counts the chain of thought in
  `max_tokens` and is slower: raise `max_tokens` and `timeout_s` of those roles in the same version.
- DeepSeek has no provider routing, fallbacks, ZDR or price caps; a DeepSeek role must leave `provider`,
  `zdr`, `max_price` and `fallback_models` unset. Both news judges may run on DeepSeek (the different
  developer rule applies only when a judge is on OpenRouter).
- `llm_calls.provider` is `deepseek`; `cost_usd` is priced from `config/deepseek.yaml` (peak rate on UTC
  weekdays 01:00-04:00 and 06:00-10:00, off-peak otherwise; Chinese public holidays are billed off-peak by
  DeepSeek but priced at peak here, so the recorded cost can only over-state). When DeepSeek changes its
  prices, update the file with the new `prices_read_on` date and redeploy.
- Back to OpenRouter: in the Models page config editor set each role's `gateway` to `openrouter` with a
  pinned `developer/model` slug (and its provider options), test every role, then save.

## 12. Secrets
- `secrets/` on the VPS holds the decrypted docker secrets (directory 0700, files 0444). Store them
  encrypted with sops (age) as `secrets/*.enc.yaml`; decrypt at deploy time only.
- `uv run python scripts/gen_prod_secrets.py` creates every random secret and the files derived from them;
  it lists the operator-provided files that are missing (scope keys, OrderIntent keys, backup key,
  `alertmanager_deadman_url`). Each file is written atomically (temporary file, fsync, rename).
- The script stops on an existing empty secret file (`... exists but is empty`) instead of generating a
  new value: restore the file from the encrypted copy, or delete it to rotate on purpose (below).
- Rotate one value: delete its file (and, for a Postgres or Redis password, change it in the running store
  first: `ALTER ROLE ... PASSWORD` / `ACL SETUSER`), run the script again, then `dc up -d --force-recreate`
  the services that mount it.

## 13. Backups (`backup_stale`)
What runs (service `backup`, `deploy/backup/backup.sh`):
- WAL: Postgres archives each completed segment (at least every 2 min) into `pg_wal_archive`; the backup
  daemon encrypts (age) and uploads it every 60 s, then deletes the local copy. One WAL run at a time
  (`wal.lock` in the state volume; a manual `hdt-backup wal` waits for the daemon's). The segment being
  uploaded is recorded first (`wal_in_flight`), so an upload that was stored but whose answer was lost,
  or that a stop or crash interrupted, counts as shipped on the next run instead of stalling WAL
  shipping. The service gets 120 s to stop (`stop_grace_period`).
- Daily at `HDT_BACKUP_HOUR_UTC` (a UTC hour, 0 to 23; an invalid value stops the service at start, see
  `dc logs backup`): physical base backup, logical `pg_dump`, and the lake (each closed file once,
  content-addressed, plus a manifest per run). Every uploaded set is listed in `shipped.tsv` in the
  state volume.
- Each daily step (`base`, `logical`, `lake`) runs and is recorded on its own (`done.<kind>` in the
  backup state volume, dated with the UTC day the step started): a failed step leaves no work files, is
  retried with backoff (5 min, doubling up to 2 h, `retry.<kind>`) and never reruns the steps that
  already succeeded that day. WAL shipping runs in its own loop and is never blocked by a daily step.
- `hdt_backup` is created by `deploy/postgres/init/02-ops-roles.sh` with REPLICATION and BYPASSRLS
  (`pg_dump` must read the RLS tables `secrets` and `store`). A cluster initialised before that script
  existed gets the role with `dc exec postgres bash /docker-entrypoint-initdb.d/02-ops-roles.sh`
  (idempotent; also creates `hdt_monitor`), then `dc restart backup postgres-exporter`.
- Uploads use a write-only key and are create-only (`If-None-Match: *`): an object that already exists
  is never replaced. Configure the bucket to require that header and to apply object lock plus a
  retention period (provider policy); an upload refused with HTTP 412 means the object exists and the
  step fails instead of overwriting it.
- Postgres archives a segment with `deploy/postgres/archive-wal.sh` (copy, fsync, rename): a segment
  already archived with the same content counts as done, a different content fails the archive.
Alert handling:
1. `dc logs --since 1h backup` names the failing step. `BackupStepFailing` fires when a step's last
   failure is newer than its last success for 15 minutes; `BackupNeverSucceeded` when a daily step has
   never succeeded for 30 hours.
2. Upload refused (HTTP 403): the key or bucket policy changed. Connection refused through the proxy: the
   S3 host must equal `HDT_BACKUP_S3_ENDPOINT` (egress-proxy renders its allowlist from it at start). That
   value must be `https://<DNS name>` with no port or port 443: egress-proxy refuses to start with an IP
   address or another port (`dc logs egress-proxy`), which Squid would deny anyway.
3. Run a step by hand: `dc exec backup hdt-backup wal` (or `base`, `logical`, `lake`).
4. `hdt_backup_wal_pending_files` keeps growing: WAL is not leaving the host and point-in-time recovery
   would lose data; this also fills the disk. Fix before anything else.
5. `wal/<NAME>.zst.age already exists but was not shipped from here`: the bucket holds that segment but
   this host has no record of uploading it (the backup state volume was recreated, another deployment
   writes the same `HDT_BACKUP_TARGET` prefix, or someone else uploaded it). WAL shipping stops at that
   segment until it is settled:
   1. On the VPS: `dc exec backup sha256sum /var/lib/postgresql/wal_archive/<NAME>`.
   2. On the restore machine (section 14, `$R` as defined there):
      `$R hdt/backup:drill bash -c 'hdt-restore wal <NAME> /tmp/segment && sha256sum /tmp/segment'`.
   3. Same hash: the object is this segment. Delete the local copy
      (`dc exec backup rm /var/lib/postgresql/wal_archive/<NAME>`) and run `dc exec backup hdt-backup wal`;
      shipping continues with the next segment.
   4. Different hash, or the restore fails: the object is not this host's. Keep the segment. Unless
      another deployment is known to share the prefix, treat it as tampering with the backup bucket and
      rotate the backup write key (section 12). Point `HDT_BACKUP_TARGET` at a new prefix, `dc up -d
      backup`, and take a base backup at once (`dc exec backup hdt-backup base`): recovery from the new
      prefix starts at that base backup.

## 14. Restore drill (monthly, and for any real restore)
Run on a separate machine (never on the production VPS) that holds the offline age identity and the
READ-only key of the backup bucket. Build the image from the same commit: `docker build -f
services/backup/Dockerfile -t hdt/backup:drill .`
```bash
cat > restore.env <<'EOF'
HDT_BACKUP_TARGET=s3://<bucket>/<prefix>
HDT_BACKUP_S3_ENDPOINT=https://<endpoint>
HDT_BACKUP_S3_REGION=<region>
HDT_RESTORE_S3_ACCESS_KEY_FILE=/keys/read_access_key
HDT_RESTORE_S3_SECRET_KEY_FILE=/keys/read_secret_key
HDT_RESTORE_AGE_IDENTITY_FILE=/keys/identity.txt
EOF
R="docker run --rm --env-file restore.env -v $PWD/keys:/keys:ro"
$R hdt/backup:drill hdt-restore latest base                         # newest complete ids
$R hdt/backup:drill hdt-restore list logical/

# 1. Physical + WAL into a scratch volume, optionally to a point in time, then start it.
docker volume create hdt_drill_pg
docker run -d --name hdt-drill --env-file restore.env -v $PWD/keys:/keys:ro \
  -v hdt_drill_pg:/var/lib/postgresql/data hdt/backup:drill bash -c \
  "hdt-restore base latest /var/lib/postgresql/data [--target-time '2026-10-01 12:00:00+00'] \
   && exec postgres -D /var/lib/postgresql/data"
docker exec hdt-drill psql -U postgres -d hdt -c 'SELECT pg_is_in_recovery()'   # f when done

# 2. Logical dump into a scratch database of that server (or any Postgres 16).
docker exec hdt-drill createdb -U postgres hdt_logical
docker run --rm --network container:hdt-drill --env-file restore.env -v $PWD/keys:/keys:ro \
  hdt/backup:drill hdt-restore logical latest postgresql://postgres@127.0.0.1/hdt_logical

# 3. Lake into a scratch directory.
docker run --rm --env-file restore.env -v $PWD/keys:/keys:ro -v $PWD/lake:/restore \
  hdt/backup:drill hdt-restore lake latest /restore
```
`latest` is the newest backup id that is complete (SHA256SUMS lists exactly the expected objects and
all of them exist) and not in the future (more than `HDT_RESTORE_MAX_CLOCK_SKEW_S`, default 300 s);
an explicit id that is incomplete is refused. During WAL replay `hdt-restore wal` exits 1 only when a
segment was never shipped (end of WAL); any other error (network, decrypt, checksum, configuration)
exits 255, which stops recovery instead of promoting a database with missing transactions: fix the
cause and start the server again.

Pass criteria: every command exits 0 (checksums and backup manifest verified), the restored server leaves
recovery, row counts of the main tables match the production figures at the backup time, and the lake
tree matches the manifest. Record the drill on the production host, which clears
`RestoreDrillOverdue` (fires 35 days after the last recorded drill; `RestoreDrillNeverRecorded` after
48 h without any):
`dc exec backup hdt-backup restore-tested <base id> <logical id> <lake id>`. It accepts only ids of sets
this host uploaded (`shipped.tsv` in the backup state volume; sets uploaded before that record existed do
not count, so drill on a newer set). Also record the date, backup ids, durations and any issue in
`docs/project-changelog.md`, then
`docker rm -f hdt-drill && docker volume rm hdt_drill_pg`.
The same flow is automated against a throwaway cluster in `tests/integration/test_backup_restore.py`.

## 15. Deploying a new version
1. `git pull` in `/opt/hdt`; read `docs/project-changelog.md` for migration notes.
2. `dc build` (images are pinned by digest; builds are reproducible from `uv.lock`).
3. `dc up -d`: `migrate` runs `alembic upgrade head` before any service starts.
4. `dc ps` until every service is `healthy`; send `dc exec telegram-bot python -m hdt.ops.alerts test
   --kind service_down` to confirm alert delivery.
