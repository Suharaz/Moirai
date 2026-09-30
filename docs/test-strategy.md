# Test Strategy

How the AI council system is verified before any real money is at risk. This is a plan: it states what each test layer proves, in which order the layers run, what environment each needs, and which exit criteria gate the next step. It does not replace the phase files; every criterion below traces back to the Design Contract (`plans/.../research/00-design-contract.md`, section 9) or a phase success criterion.

## 1. Principles
- **Money safety first.** Tests that protect capital (STOP invariant, Risk signature, hard ceilings, kill switch, no LLM numbers in orders) run on every commit and block merges. Nothing reaches `live` without gate G4 plus the console checklist and TOTP.
- **Point in time or it does not count.** Every feature, label and replay test reads the lake with `fetched_at <= as_of`. A test that can see the future is a defect, not a flaky test.
- **Determinism.** Injected clock, seeded randomness, canonical JSON hashes, LLM cache in replay (a cache miss is a hard error). The same input must give the same decision card hash.
- **Real behaviour, not wiring.** Tests assert consumer-visible outcomes (orders refused, labels, weights, alerts), boundaries and invariants. No tests of mock echoes, copies or source text.
- **Staged exposure.** unit -> contract -> integration (dev Docker) -> replay on the recorded lake -> recorder soak and credit pilot -> testnet lifecycle -> paper shadow -> live 1/4 size. Each stage has exit criteria; failing one sends work back, never forward.
- **Statistics are pre-registered.** G1 and G4 thresholds, horizons and evaluation sets are fixed before the data is looked at (`docs/g1-preregistration.md`, Design Contract section 9). Calibration and evaluation sets are split by time; the final evaluation runs once.

## 2. Test layers

| Layer | Location | Environment | Runs | Proves |
|:--|:--|:--|:--|:--|
| Unit | `tests/unit/**` | none (pure Python, tmp dirs) | every commit, pre-commit + CI | math, parsers, rules, bounds, state machines |
| Property | `tests/unit/**` with Hypothesis | none | every commit | invariants over generated inputs (PIT, ticks, sizing) |
| Contract | `tests/contract/**` | none | every commit | data contracts, anti-manipulation, no LLM numbers, agent output bounds |
| Integration | `tests/integration/**` (markers `pg`, `redis`, `integration`) | dev Docker stack (Postgres 16, Redis 7) on 127.0.0.1 | every commit in CI with services; locally before merge | migrations == models, role grants, Redis ACL matrix, outbox atomicity, checkpoint resume, isolation |
| Replay | `tests/replay/**` + `scripts/ablation_replay.py` | recorded lake (fixture or real) | per merge touching council/quant/scoring; nightly on the real lake | determinism, no look-ahead, cache-miss hard error, no network |
| Network | tests marked `network` | real public endpoints, real keys where needed | manual, deselected by default (`-m network`) | live API shapes and labels (keyless/Startup+), testnet order lifecycle |
| Soak | recorder on the VPS | production stack | once before shadow, again after major ingest changes | 72 h continuity, credit pilot |
| Shadow | whole system, `paper` + `testnet` | production stack | 8+ weeks | G4 statistics |
| Live | `live` 1/4 size | production stack, limited sub-account | 4+ weeks | live vs paper tolerances |

Default command: `uv run pytest -q` (unit + contract + integration with the dev stack; `network` deselected). CI also runs `ruff format --check`, `ruff check`, `mypy`, `scripts/check_language.py`, `uv lock --check`, `pip-audit`, `detect-secrets`.

## 3. Unit and property tests by component

### 3.1 Data ingestion and lake (phase 02)
- Raw store: hash chain tamper detection, closed hours immutable, compaction idempotent, daily Merkle root refuses open hours, raw depth expiry only after a manifest exists (`tests/unit/lake/test_raw_store.py`).
- PIT query: Hypothesis property "any `as_of` never returns a record fetched after it"; staging and Parquet reads agree; non-ASCII provider keys round-trip.
- CMC client: every response recorded (including errors that charged credits), 1008 retried with backoff, 1009/1010/1011 halt with no retry, keyless -> keyed fallback only with budget.
- Credit governor and meter: budget from the billing anchor, shed order context/DEX -> attention -> price -> derivatives, UTC day reset, 7-day run-rate projection and the 85 % alert, WebSocket credits accrue 0.025 per message.
- Collectors: route #2 targets from the month's first route #1 record, paged lists stored under `<key>:p<n>`, has_more paging, shedding by class, a 1010 halts every later job before any HTTP call.
- Binance: weight header tracking, 418/429 backoff, depth `pu` gap, aggTrade id jump, markPrice time jump, 24 h reconnect before the limit.
- Symbol map and universe: 1000/1M prefixes, ticker collisions to the best rank, overrides, untradable symbols excluded, universe sorted by OI USD, LTX = top 60.
- Pilot report: monthly projection, ordered cadence proposals above 80 %.

### 3.2 Quant core (phase 03)
- Liquidation semantics: coin absent from a complete list = 0, incomplete page = unknown, SPIKE denominator floor, forceOrder lower bound.
- Funding: per-hour normalization, interval change 8 h -> 4 h at the same hourly cost is not a drop, inferred interval precedence.
- LTX strict/loose rules incl. a null condition failing the rule, long/short symmetry, contagion block at B >= 0.40 until BTC flushes.
- Levels (property): tick-aligned (LONG floors, SHORT ceils), R:R >= min_rr after rounding, entry/stop/TP ordering per side.
- p_model: unfitted models return 0.5, clipping, missing feature = z 0; packet determinism; the cache never serves another agent or another config pin.
- Scanner budget: HELD never dropped, superset dropped first, reruns of an `as_of` emit nothing new.
- G1 statistics on synthetic series with known answers: bootstrap CI coverage, power / N_min, fee + slippage + funding netting.

### 3.3 Tools, memory, skills (phase 04)
- Replay registry opens no socket for a full replayed day (socket monkeypatch raises).
- Memory as_of property test: no record with `known_at > as_of` is ever returned; lesson state rebuilt from the append-only chain.
- Budget enforcer: 8 calls / 45 s then forced abstain.
- Replaying day 4 and day 13 of the same event gives identical prompts.

### 3.4 LLM agents (phase 05)
- Output bounds contract: `|Δlogit| > 0.5` clipped by code, candidate outside the set or on the wrong side rejected, claims pointing to missing packet fields rejected, code-assigned `verified/hard/tier` ignore LLM values.
- Router (fake OpenRouter): json_schema strict request, provider prefs, `openrouter/auto` and `:free` rejected, cache hit, replay cache miss is a hard error, failing structured-output self-test fails the role closed, two parse failures abstain, cost recorded in `llm_calls`.
- Router (fake DeepSeek): JSON Output body (`json_object`, schema in the system message, thinking toggle, no provider prefs), empty or invalid replies retried once then `LlmOutputError`, cost priced from the price table (cache hit/miss), thinking tool steps return `reasoning_content` and the runner passes it back, gateways never share a cache entry.

### 3.5 Council (phase 06)
- Consensus tables: quorum 4, 2/3 of w-tilde, the momentum group counts as one vote (3 weak LONG vs 2 strong SHORT -> no consensus).
- Aggregate: log opinion pool with r-mixing, clip [0.02, 0.98], abstainers excluded.
- Manager: consensus size 1.0; after 3 rounds p >= 0.60 / <= 0.40 with matching sign and D below threshold gives 0.5; otherwise NO_TRADE; D value-range test.
- Verifier and hard evidence: fabricated number, repeated claim, URL claim without quote, fabricated quote on an attacker page, correct packet claim that is not in the hard list cannot unlock a flip, an opposite-direction hard claim cannot unlock a flip, all URL claims count as one piece of evidence.
- Commit: an edit after the round-1 commit is detected.

### 3.6 News evidence (phase 07)
- SSRF guard: 127.0.0.1, 169.254.169.254, 10.x, redirect to an internal IP, DNS rebinding, > 3 redirects, > 5 MB, > 10 s, gzip bomb.
- Dedupe: rewritten articles about one event collapse (exact, MinHash >= 0.8, event_key).
- Modes table incl. "Fade panic without independent refuting evidence -> abstain", judge disagreement -> abstain, EXPLOIT/DELIST disagreement -> soft veto.
- Quote check, tiering from the post-redirect domain, fake "official" page, project denial during an exploit.

### 3.7 Scoring and learning (phase 08)
- Synthetic oracle (6 agents): the oracle reaches w >= 0.30 within 150 independent forecasts; noise agents stay <= 1/6 + 0.03; eta and alpha chosen here.
- Sleeping experts: an agent abstaining on 100 % of events keeps its relative share.
- Resolver: RAW_12H vs RESID_12H labels differ correctly; no data after `as_of + horizon`; markPriceKlines fallback; no silent drops.
- Idempotency: rescoring the full history twice gives identical w/a/r/calibration tables.
- Calibration and Hedge tables never mix target types.

### 3.8 Risk and execution (phase 09)
- Sizing chain, conf mapping, notional and margin caps, liquidation >= 1.5x stop distance.
- Hard ceilings in code (leverage <= 3 isolated, risk <= 0.5 %, <= 4 positions, 1 per coin, same-direction risk <= 1.5 % incl. hedge, daily loss -2 % kill) cannot be exceeded by config or messages.
- Veto fail-closed on stale scans, AccountState older than 30 s vetoes, duplicate decisions ignored; no decision TTL (owner decision 2026-09-28): an OPEN decided long after its candidate is refused only when the mark is beyond the stop or TP1 or too far from the entry.
- Ed25519 signature: unsigned or wrongly signed intents refused by execution; `test_no_llm_numbers`: no order field originates from LLM output.
- Client ids: deterministic, <= 36 chars, separate algo namespace.

### 3.9 Ops, public dashboard, console (phases 10, 12)
- Snapshot allowlist: every forbidden field (stop/TP/liquidation levels on open positions, order ids, config, keys, credits, alerts, audit) rejected; decisions only once scored, >= 12 h old and closed; the previous snapshot stays live after a rejection.
- Alerts: dedupe per (kind, key), notification retry, rules (dead routes, credit projection, CMC halt, lake disk).
- Telegram: allowlisted private chat only, 2-step kill, 2-step + TOTP resume with replay protection, daily-loss kill not resumable the same UTC day.
- Console: auth, CSRF, step-up, ceilings via direct API calls, version pins, live gate refused without G4/checklist/TOTP, sealed scopes (config-api cannot decrypt).

## 4. Contract tests
- Round-trip of every v1 contract through canonical JSON and JSONB with a stable sha256.
- Anti-manipulation end to end with fake agents (flip without claims, repeated claims, fabricated numbers, attacker quote).
- Agent output bounds, no LLM numbers in orders, Risk signature on every OrderIntent.
- Snapshot schema vs allowlist drift.

## 5. Integration tests (dev Docker stack)
- Migrations: upgrade to head == SQLAlchemy models, then downgrade/upgrade round trip.
- Role grants: each service role can do exactly its matrix (e.g. council cannot UPDATE packets, risk cannot INSERT them, telegram audit inserts work).
- Redis ACL matrix: XADD/consume per service, destructive commands denied everywhere, recorder read-only XREVRANGE on `candidates` and `account_state`.
- Outbox atomicity: decision card + forecasts + claims + outbox row in one transaction; relay delivers at least once to `decisions` with a stable `event_id`; Risk dedupes on `processed_decisions`, so the effect is exactly one entry (tested across a crash between XADD and the mark).
- Council checkpoint: kill the process mid round 2; resuming by `thread_id = event_id` completes the same event with the same card hash.
- Scanner end to end on a synthetic lake with Postgres + Redis.
- Execution against a fake exchange: user stream loss blocks new actions until REST re-sync; STOP re-placed idempotently; kill switch never cancels STOP legs.
- Public isolation: parsed compose file proves the dashboard and publisher have no network path to Postgres, Redis, config-api or execution; only Caddy publishes 443; fetcher only on `net_fetch`.

## 6. Replay and point-in-time validation
- Build a 3-day lake fixture from the first real recording that contains a liquidation cascade; freeze it (Merkle root in the fixture manifest).
- Replay the full pipeline (scanner -> quant packets -> council with cached LLM outputs -> risk -> paper engine) twice: identical decision card hashes, identical orders.
- Look-ahead audit: append records after `as_of` to the fixture and assert no feature, packet, label or decision changes.
- Network audit: replay under a socket guard; any connection attempt fails the run.
- Cache discipline: delete one cached LLM response; replay must fail hard at that call.

## 7. Recorder soak (72 h) and CMC credit pilot (7 days)
Preconditions: VPS stack up, CMC Startup key entered on the console and `valid`, Binance public access, disk alert wired.
- **72 h soak exit criteria:** no core route more than 2 consecutive cycles late (`route_health`; routes "not on current CMC plan" (1006, `disabled`) are excluded and listed in the soak report), WS gaps logged and counted (`ws_health.gaps_24h`), every closed hour compacted, daily Merkle roots produced, no orphaned staging files, memory and file handles flat, restart during the soak resumes without duplicate records.
- **Credit pilot exit criteria:** `scripts/pilot_credit_report.py --days 7` projects <= 80 % of the real monthly quota from `/v1/key/info`; the governor never reaches a hard cap; per-route actual vs design ratio reviewed; if above 80 %, apply the proposals in order (route #2 peripheral cadence, route #3 watchlist size, route #30 off, route #6 cadence) and rerun the pilot.
- Confirm every Startup+/keyless label with a real call and record it in `docs/api-endpoints-design.md` section 6.

## 8. Gate G1 (event study before trusting the LLM layer)
- Run `scripts/g1_event_study.py --start <first recorded day> [--end <instant>]` weekly on the recorded lake. It evaluates the scanner rules point in time at every 5-minute slot, keeps one event per coin per non-overlapping 12 h window across both rules, and computes the preregistered 12 h event study (4/8/24 h reference only) net of taker fee twice and slippage twice (LTX adds the beta hedge leg), with a UTC-day block bootstrap 95 % CI and the power-based N_min from the non-event sigma (`docs/g1-preregistration.md`, amendment 1). Funding is not netted: the preregistered metric deducts only the fee and slippage round trip.
- The study has no dry-run or `--write` flag: every run writes `reports/g1/<end date>.json`, `reports/g1/<end date>.md` and `reports/g1/latest.json`. `latest.json` records the study the gate state was computed on (start, end, bootstrap seed and resamples, rule, label and feature versions, and `config_pins`: sha256 of the scanner file, indicators, CMC routes, fees and staleness limit).
- Determinism check before trusting a report: rerun with the same `--start/--end` and compare the JSON byte for byte.
- Outcomes (both rules must pass): `insufficient_data` / `insufficient_power` -> keep recording, phases 05-08 stay unbuilt / blocked; `failed` -> replan the core strategy; `passed` -> `scripts/fit_p_model.py`, then review the version files in `reports/p_model/`, then build phases 05-08 and start the shadow run.
- `scripts/fit_p_model.py` takes no window option. It refuses (exit 2) unless `latest.json` reads `passed` with a complete study record, the JSON report it names holds the same status, and the recorded seed, versions and config pins equal the current ones; it then fits on exactly the study's `[start, end)` (`tests/unit/quant/test_fit_p_model.py`). The fit is per agent and target type, walk-forward over blocks of whole `as_of` instants, each test block trained only on earlier rows whose 12 h label resolved before the block starts (purged by the label horizon).
- First run (2026-09-27, window 00:00 to 23:00 UTC, 83 slots with a recorded universe): `insufficient_data`, zero strict or loose events, every baseline label immature.
- The preregistration is never edited after results exist; a change needs a new dated file. The one correction so far (2026-09-28) fixed the provenance paragraph of amendment 1 to state when it was committed; no rule changed.

## 9. Testnet lifecycle (order mechanics only)
Preconditions: Binance demo key entered on the console, demo WS routing probed.
- 20 synthetic lifecycles via the testnet driver: entry, STOP placed within 2 s of a fill, TP, emulated OCO cancel, trailing, exit, IOC sized by `executedQty`.
- Failure drills: unplug the network mid-position, drop the user stream, restart execution with an open position, kill switch from Telegram and from `hdt-kill` over SSH. Every drill ends with a STOP on every open position and a clean reconcile.
- Testnet results never count toward G4, including the council decisions traded on the demo account in run mode `testnet` (owner decision 2026-09-29).

## 10. Paper shadow run and gate G4
- Whole system in `paper` on mainnet data plus the testnet driver, model slugs pinned for the whole window, weekly report via Telegram.
- Calibration set and final evaluation set split by time; thresholds (Manager D, Hedge eta, LTX thresholds) tuned only on the calibration set.
- **G4 exit criteria on the final evaluation set, per `target_type`, after fees:** >= 150 independent entry orders; Spiegelhalter |Z| < 1.96 and the calibration slope 95 % CI contains 1 (ECE reported only); daily PnL including no-trade days with annualized Sharpe > 1.5 and PSR(0) >= 0.95; max drawdown < 10 %; every retained component (debate, LLM adjustment, each agent, consensus threshold) wins a paired bootstrap log-loss ablation with Holm correction. Numeric variants via replay ablation; debate-dependent variants as forward shadow branches.
- Record every keep/disable decision with its figures in `docs/project-changelog.md`.

## 11. Live 1/4 size
- Go-live checklist (`docs/go-live-checklist.md`): trade-only key with IP whitelist, sub-account with limited capital, kill switch tested live with a minimum order, backups restored into scratch, alerts delivered.
- 4 weeks at 1/4 size: compare live vs paper on the same signals with minimum N for fill rate, entry price and stop slippage; stop scaling if slippage exceeds 2x the paper assumption. Re-measure at each size step.

## 12. Security and isolation tests
- Secrets: sealed per scope, config-api holds only public keys, no service logs a key (log grep in CI over integration logs), redaction of key, secret, signature, listenKey and the CMC header.
- Network: external port scan shows only 443; console, config-api, Grafana and Langfuse only through the tunnel; egress only through the allowlist proxy.
- Fetcher: SSRF suite plus container checks (non-root, read-only fs, no volume, no route to `net_data`).
- Prompt injection: external text only in labeled data fields, truncated; instruction-like strings filtered from shared claims.
- Supply chain: hash-pinned lock, pip-audit/osv-scanner, detect-secrets, pinned CI actions and image digests.
- Least privilege under the real roles: tests that connect as the Redis admin or the Postgres superuser hide ACL and grant regressions. Every security-relevant path keeps at least one test authenticated as its production role (risk attach under the template `risk` ACL user, logical backup as `hdt_backup`, alert raise and denial of delivery columns as a raiser such as `hdt_news`, publisher reads as `hdt_publisher_ro`).
- Monitoring rules: promtool tests include unrelated-job series, because every process importing `hdt.ops.metrics` exports the shared gauges at 0.

## 13. Execution order and ownership
```text
every commit:  lint + types + language + unit + property + contract + integration (dev stack)
per merge:     replay determinism + look-ahead audit on the fixture lake
VPS day 0-3:   72 h recorder soak
VPS day 0-7:   CMC credit pilot -> cadence decision
weekly:        G1 event study until passed or failed
after G1 pass: fit p_model -> shadow start; testnet lifecycle drills in parallel
shadow wk 2-3: preliminary event study recheck
shadow wk 4+:  weekly ablation + calibration on the calibration set
G4 pass:       go-live checklist -> live 1/4 size, 4 weeks -> scale by owner decision
```

## 14. Known gaps that need real inputs
- Real CMC Startup key: keyed route labels, credit pilot, `/v1/key/info` projection.
- OpenRouter key and chosen model slugs: agent self-tests, 1-day replay parse error rate < 1 %.
- Binance demo and live keys: testnet lifecycle drills, live kill switch test.
- Weeks of recorded lake: G1, p_model fit, the 3-day cascade fixture.
- 300 human-labeled news items: phase 07 precision/recall criteria.
- A VPS: soak, port scan, backups restore, public isolation against the running stack.
