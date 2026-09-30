# System Architecture

Source of truth: Design Contract v2 (`plans/260927-0641-ai-derivatives-trading-council/research/00-design-contract.md`). Flow diagram and route catalog: `visuals/ai-trading-flow.html` (workspace root).

## 1. End-to-end flow
```text
CMC (30 routes) ─┐
Binance public ──┼─> recorder (immutable lake: staging -> hourly Parquet, hash chain, Merkle root)
News sources ────┘        │
                          ▼
             pit_query(as_of)  (fetched_at <= as_of only)
                          │
        quant core: features, per-agent p_model, shared candidate set, QuantPacket (Postgres, sha256)
                          │
        scanner ──XADD──> stream `candidates`  <──XADD── news (HOLLOW_HYPE)
                          │
        council (LangGraph): load_context -> Send(6 agents) -> commit_round1 -> check_consensus
                             -> [debate <= 3 rounds: share_claims -> verify_claims -> revise]
                             -> aggregate -> manager -> emit_decision (outbox, same tx as decision card)
                          │ relay XADD DecisionMsg
                          ▼
        stream `decisions` ──cg risk-{account}──> risk (dedupe, packet sha256, side, levels vs mark, veto, sizing, sign Ed25519)
                          │ XADD OrderIntent
                          ▼
        stream `orders` ──cg exec-{account}──> execution (verify, own limits, adapter paper|demo|live)
                          │ XADD AccountState
                          ▼
        stream `account_state` -> risk, scanner (HELD), console, public-publisher (allowlisted)
                          │
        scorer: resolver at as_of + 12 h -> labels -> w (Hedge), a, r, calibration -> next council events
```

## 2. Services and networks
| Service | Role | Networks | Secrets |
|:--|:--|:--|:--|
| `postgres` | Business tables, decision cards, ledger, config versions, sealed secrets | `net_data` | bootstrap |
| `redis` | Streams (AOF everysec, per-service ACL) | `net_data` | bootstrap |
| `recorder` | CMC + Binance public ingestion, credit meter/governor | `net_data`, `net_proxy` | scope `data` private key |
| `council` | Scanner, agents, council graph, relay | `net_data`, `net_fetch`, `net_trace`, `net_proxy` | scope `llm` private key |
| `news` | News ingestion, extractor and judges, HOLLOW_HYPE candidates; reads the lake read-only | `net_data`, `net_trace`, `net_proxy` (not on `net_fetch`: only the council's live `fetch_source` reaches the fetcher) | scope `data`, `llm` |
| `veto-scan` | Independent news veto scan -> `risk_flags` for every universe coin each cycle; reads the lake read-only | `net_data`, `net_trace`, `net_proxy` | scope `data`, `llm` |
| `fetcher` | Sandboxed article fetch by `item_id` | `net_fetch` only (egress via `egress-fetch`) | none |
| `scorer` | Resolver, learning, reflection | `net_data`, `net_trace`, `net_proxy` | scope `llm` private key |
| `risk` | Risk rules, signs `OrderIntent` | `net_data` only (no egress) | OrderIntent Ed25519 private key |
| `execution` | Only caller of signed Binance endpoints | `net_data`, `net_proxy` (VPS IP whitelisted on the key) | scope `exec` private key, Ed25519 public keys |
| `testnet-driver` | Synthetic testnet order lifecycle | `net_data`, `net_proxy` | own Ed25519 key (testnet only) |
| `config-api` | FastAPI: auth, TOTP, validation, ceilings, audit | `net_data`, `net_console` (no host port) | session secret, scope public keys only |
| `console` | Streamlit admin console | `net_data`, `net_console`, `net_admin` (Tailscale `:8501`) | none (read-only DB user) |
| `telegram-bot` | Alerts and commands | `net_data`, `net_proxy` | scope `ops` private key, `/resume` TOTP seed |
| `public-publisher` | Builds allowlisted JSON snapshots | `net_data`, `net_store_write` | read-only DB role, store write user |
| `public-store` | Immutable snapshot bucket (MinIO) | `net_store_write`, `net_store_read` | root user (init only) |
| `dashboard` | Public read-only Streamlit dashboard | `net_edge`, `net_store_read` | store read-only user (GetObject only) |
| `caddy` | Public HTTPS `:443`, rate limit, static cache | `net_ingress` (443), `net_edge` | ACME state |
| `prometheus`, `node-exporter`, `grafana` | Metrics, alert rules, dashboards | `net_data` + `net_monitor`; Grafana `net_monitor` + `net_admin` (Tailscale `:3000`) | Grafana admin password |
| `postgres-exporter`, `redis-exporter`, `blackbox`, `blackbox-edge`, `alertmanager` | Database and Redis metrics (roles `hdt_monitor`, ACL user `monitor`), HTTP probes of internal services and of caddy/dashboard (`net_probe_edge`), dead-man Watchdog to an external check | `net_monitor` (+ `net_data` for the exporters, `net_probe_edge` for `blackbox-edge`, `net_proxy` for `alertmanager`) | monitor passwords, dead-man URL |
| `langfuse-*` | Self-hosted tracing on its own Postgres/Redis/ClickHouse/MinIO | `net_langfuse`, `net_trace`, `net_admin` (Tailscale `:3100`) | `langfuse.env` |
| `egress-proxy`, `egress-fetch` | Squid allowlists per client (CMC, Binance, OpenRouter, DeepSeek, Telegram, news, backup bucket) | `net_proxy` / `net_fetch` + `net_egress` | none |
| `backup` | age-encrypted WAL, base, logical and lake backups | `net_data`, `net_proxy` | backup role, write-only bucket key |

Only `443` is public. Console, Grafana and Langfuse publish on the Tailscale address only; SSH is Tailscale-only; config-api is reached only from the console. Every network is `internal` except `net_ingress` (caddy), `net_egress` (the two proxies) and `net_admin` (IP masquerading off). `tests/integration/test_public_isolation.py` asserts these rules on the compose file and, with Docker, by connecting from the dashboard's and the fetcher's networks.

## 3. Data contracts (v1, `src/hdt/contracts/`)
| Contract | Main fields | Producer -> Consumer |
|:--|:--|:--|
| `Candidate` | coin_id, as_of, source {LTX, MIGRATION, HOLLOW_HYPE, HELD}, score, rule_version, target_type, label_spec_version | scanner (03), news (07) -> `candidates` -> trigger (06) |
| `LevelCandidate` | candidate_id, side, entry, invalidation, tp1, rr, tick | levels (03) -> packet |
| `QuantPacket` | agent, coin_id, as_of, features, p_model, candidate_set_sha256, data_quality, universe_date, config_version_ids, feature_ver, p_model_ver, packet_sha256 | quant_core (03) -> agents (05), verifier (06), risk (09) via Postgres |
| `Claim` | claim_id, kind {packet, tool, url}, ref, statement, value, quote, direction_hint; code-assigned verified, hard, tier, domain_url | agents (05) -> verifier/share (06) -> scoring (08) |
| `AgentForecast` | agent, agent_version, event_id, round, p_model, p_llm, p_used, abstain, abstain_reason, candidate_id, claims, cited_claim_ids, packet_sha256, prompt_hash, model_slug, provider, skill_commit | agents (05) -> council (06), scoring (08) |
| `RiskFlags` | coin_id, as_of, veto_long, veto_short, size_mult, evidence_ref, expires_at, scan_fresh_at | veto scan (07) -> `risk_flags` -> risk, execution |
| `DecisionMsg` | schema_version, event_id, coin_id, as_of, intent {OPEN, HOLD, EXIT}, side, p, p_side, manager_size, candidate_id, packet_sha256, config_version_ids, expires_at (optional, always null since 2026-09-28, ignored by Risk) | outbox (06) -> `decisions` -> risk (09) |
| `OrderIntent` | intent_id, account {paper, testnet, live}, event_id, leg {entry, entry_ioc, sl, tp1, trail, exit, hedge}, seq, symbol, side, qty, price, trigger_price, reduce_only, tif, client_id, max_entry_distance (entry legs, optional, since 2026-09-28), key_id, signature | risk (09) -> `orders` -> execution (09) |
| `AccountState` | account, equity, available, positions, open_orders, open_algo_orders, ts | execution -> `account_state` -> risk, scanner, console, public-publisher |
| `DecisionTimelineEntry` | at, stage {scanner, news, council, manager, risk, orders, outcome, scoring}, text, tone | council (06) -> `decision_cards.timeline` -> console and public-publisher (every known stage) |

Streams use consumer groups, `XACK` only after the durable write, and a dedupe table keyed on natural keys. Decisions go through a Postgres outbox written in the same transaction as the decision card.

## 4. Streams and Redis ACL
| Stream | Writers | Readers (consumer group) |
|:--|:--|:--|
| `candidates` | council scanner, news | council trigger; recorder reads the last 24 h (`XREVRANGE`, read-only) for the 100 ms depth hot set |
| `risk_flags` | veto-scan | risk, execution; console reads latest (`XREVRANGE`) |
| `decisions` | council relay | risk (`risk-paper`, `risk-testnet`, `risk-live`; the groups of the run mode's decision targets: paper mode -> paper, testnet mode -> testnet only, live mode -> paper and live) |
| `orders` | risk, testnet driver (synthetic testnet flow, own signing key, P09) | execution (`exec-paper`, `exec-testnet`, `exec-live`) |
| `account_state` | execution | risk (consumer group); council scanner, recorder (hot set, held coins for CMC WS), console, public-publisher, testnet driver read latest only (`XREVRANGE`, read-only key access) |
| `controls` | config-api, telegram-bot | risk, execution |
| `config_changed` | config-api | every service |
| `secret_test_request` | config-api | scope owners and scope services (recorder, council, scorer, execution, telegram-bot, news) |
| `secret_test_result` | scope owners | config-api |
| `dead_letter` | every stream consumer (unparseable or poison messages) | operator via console/admin |

Each service has its own ACL user limited to the streams above (`deploy/redis/users.acl.template`, checked by `tests/integration/test_redis_acl.py` with `ACL DRYRUN`). XADD rights sit in a selector limited to produced streams; latest-state readers get `XREVRANGE` only in a read-only selector, so they cannot join another service's consumer group. Consumers dead-letter a message after `max_deliveries` (default 10) failed deliveries. Each service has its own Postgres role (`deploy/postgres/init/01-roles.sh`, including `hdt_veto_scan`); insert-only tables (`quant_packets`, `candidate_sets`, `scanner_log`, `audit_log`, lesson events) grant no UPDATE/DELETE. Handlers deduplicate by natural key with `hdt.db.dedupe.insert_once` in the same transaction as their effect.

## 5. Producer/consumer matrix
Every ingested route has a named consumer. Routes without a consumer are disabled by default in `config/cmc_routes.yaml`.

### 5.1 CoinMarketCap (Startup plan, 29 REST + 1 WS)
| # | Route | Consumer | Cadence | Credits/month (est.) | Group | Keyless |
|--:|:--|:--|:--|--:|:--|:--|
| 1 | `/v5/exchange/derivatives/list` | Crowding (core/peripheral cohorts) | 1 h | 720 | core | no |
| 2 | `/v5/exchange/derivatives/market-pairs/list/latest` | Crowding (LTX, Migration) | 5 min core, 15 min peripheral, 8 exchanges | ~92,160 | core | no |
| 3 | `/v5/cryptocurrency/derivatives/market-pairs/list/latest` | Crowding, News | 15 min x 20 coins | ~57,600 | core | no |
| 4 | `/v5/derivatives/liquidations/quotes/latest` | Crowding, Macro | 5 min | 8,640 | core | no |
| 5 | `/v5/derivatives/liquidations/exchange/list/latest` | Crowding (VEX) | 5 min | 8,640 | core | no |
| 6 | `/v5/derivatives/liquidations/cryptocurrency/list/latest` | Crowding (LTX core), News | 1 min | 43,200 | core | no |
| 7 | `/v2/cryptocurrency/ohlcv/historical` | Technical (1h, derived 4h/1d), Macro regime | hourly (+ once per LTX coin: 720 h backfill, lake route `ohlcv_backfill`) | ~1,500 (+ ~8 per backfilled coin) | core | no |
| 8 | `/v3/cryptocurrency/quotes/latest` | Technical, Fundamental, reconciliation | 5 min | 8,640 | core | no |
| 9 | `/v3/cryptocurrency/quotes/historical` | Technical backfill | batches | ~2,000 | backfill | no |
| 10 | `/v2/cryptocurrency/price-performance-stats/latest` | Technical (ATH/ATL distance, period performance) | daily | ~100 | core | no |
| 11 | `/v3/cryptocurrency/listings/latest` | Universe (top 500) | 1 h | 2,160 | core | yes |
| 12 | `/v1/cryptocurrency/map` | Symbol map | daily | 30 | core | yes |
| 13 | `/v1/cryptocurrency/trending/latest` | News attention | 15 min | 2,880 | core | no |
| 14 | `/v1/cryptocurrency/trending/gainers-losers` | News, Technical | 15 min | 2,880 | core | no |
| 15 | `/v1/cryptocurrency/listings/new` | News, Fundamental | 1 h | 720 | core | no |
| 16 | `/v3/fear-and-greed/latest` | Macro | 15 min | 2,880 | core | yes |
| 17 | `/v3/fear-and-greed/historical` | Macro backfill | once | ~50 | backfill | yes |
| 18 | `/v1/altcoin-season-index/latest` | Macro | 15 min | 2,880 | core | yes |
| 19 | `/v1/altcoin-season-index/historical` | Macro backfill | once | ~50 | backfill | yes |
| 20 | `/v1/global-metrics/quotes/latest` | Macro | 5 min | 8,640 | core | yes |
| 21 | `/v3/index/cmc20-latest` | Macro | 15 min | ~2,880 | core | yes |
| 22 | `/v3/index/cmc100-latest` | Macro | 15 min | ~2,880 | core | yes |
| 23 | `/v1/cryptocurrency/categories` | Macro, Fundamental (narratives, Risk narrative ceiling) | 1 h | ~1,440 | core | no |
| 24 | `/v1/dex/security/detail` | Fundamental, News (veto) | on event | ~3,000 | core | yes |
| 25 | `/v1/dex/token/pools` | Fundamental | on event | ~2,000 | core | yes |
| 26 | `/v1/dex/liquidity-change/list` | Fundamental, News (hack check) | on event | ~1,500 | core | yes |
| 27 | `/v1/dex/holders/list` | Fundamental (every LTX coin with a contract address) | daily | ~1,000 | core | yes |
| 28 | `/v1/exchange/assets` | Macro (exchange reserve change) | daily | 240 | core | no |
| 29 | `/v1/key/info` | Ops (credit meter, governor) | 1 h | 0 (not charged) | core | no |
| 30 | WS `market@crypto_latest_price` | Risk (price cross-check of held coins, <= 5) | real time | ~64,800 | optional | no |

Routes the key's plan does not include: CMC answers `1006` (HTTP 403, not charged; observed on the Startup key 2026-09-29 for #7, #10, #13, #14 and #15). The recorder writes `route_health.status = disabled` with `last_error` starting "not on current CMC plan (1006)" (`NOT_ON_PLAN_ERROR`): not a failure (`consecutive_failures` reset, never late, never `route_dead`, no `hdt_recorder_route_up` series, the refusal is kept out of `cmc_credit_usage` and the projection), and the lake holds only the error captures, which every consumer reads as a missing input (`missing_route`; Technical and Macro abstain, news attention falls back to NewsZ). The route keeps its normal cadence, so after a plan upgrade its next call succeeds and it returns to `ok` with no restart. The console Data & credits page shows it as "not on current plan" and leaves it out of the late/failing count. Every other 1001-1007 code and any other 401/402/403 stays a `failing` route. A run refused for the key (1006, or no active key) is brought forward when a new CMC key version becomes active (the recorder reads the active version of `data/cmc_api_key` every 15 s), so a `once` backfill (#9) that failed before the key was entered runs again at once; its planner skips coins that already have a successful record. DEX routes #24-27 use only map `platform` objects with a contract `token_address` (a bare hex id of 40 or 64 digits, or a non-hex address): HYPE's 16-byte HyperCore token index on `hyperliquid` is left out (CMC returned `400 Parameter error` for it).

### 5.2 Binance USDS-M (9 public + 9 signed)
| # | Endpoint | Consumer | Cadence |
|:--|:--|:--|:--|
| B1 | `GET /fapi/v1/exchangeInfo`, `/fapi/v1/fundingInfo` | Levels (tick/lot), Risk, Execution, Crowding (funding interval) | daily |
| B2 | `GET /fapi/v1/klines`, `/fapi/v1/markPriceKlines` (1m label fallback) | Technical, Scoring resolver | startup, batches |
| B3 | WS `<symbol>@kline_5m/_15m` on `/market` | Technical | real time |
| B4 | WS `<symbol>@depth20@100ms` on `/public` (1 s snapshots; 100 ms diffs for candidate/held coins) | Microstructure, levels, paper fills | real time |
| B5 | WS `<symbol>@aggTrade`, `!forceOrder@arr` on `/market` | Microstructure (CVD), Crowding (liquidation lower bound), paper queue | real time |
| B6 | WS `<symbol>@markPrice@1s` on `/market` | Crowding, Risk, reconciliation, resolver | 1 s |
| B7 | `GET /fapi/v1/fundingRate` (optional) | Crowding funding reconciliation | batches |
| B8 | `GET /fapi/v1/openInterest`, `/futures/data/openInterestHist` (optional, 1 month only) | Crowding OI reconciliation | 5 min |
| B9 | `GET /fapi/v1/depth` | Microstructure book rebuild | startup |
| B10 | `POST /fapi/v1/order`, `POST /fapi/v1/algoOrder` | Execution | per order |
| B11 | `DELETE /fapi/v1/order`, `/allOpenOrders`, `/algoOrder`, `/algoOpenOrders`, `GET /openAlgoOrders` | Execution, kill switch | per order |
| B12 | `POST /fapi/v1/leverage` | Execution | per coin |
| B13 | `POST /fapi/v1/marginType` | Execution | per coin |
| B14 | `GET /fapi/v3/positionRisk` | Execution reconciler, re-sync | 30 s |
| B15 | `GET /fapi/v3/account`, `/fapi/v3/balance` | Execution -> `AccountState` | 30 s |
| B16 | `POST/PUT /fapi/v1/listenKey` | Execution user stream | 30-50 min |
| B17 | WS user data on `/private` | Execution (ORDER_TRADE_UPDATE, ALGO_UPDATE, ACCOUNT_UPDATE) | real time |
| B18 | `GET /fapi/v1/userTrades` (+ `allOrders`, `allAlgoOrders` for ledger rebuild) | Execution re-sync, Scoring via Postgres | daily, on reconnect |

### 5.3 Other services (8)
| # | Service | Consumer | Status |
|:--|:--|:--|:--|
| O1 | OpenRouter `/api/v1/chat/completions` (+ `/models`, `/key`) | 6 agents, news extractor, 2 judges, reflection (roles on gateway `openrouter`) | finalized |
| O1b | DeepSeek `https://api.deepseek.com/chat/completions` (+ `/user/balance`) | the same roles when their `gateway` is `deepseek` (temporary, owner decision 2026-09-28) | finalized |
| O2 | Full-text news feed | News | source not finalized |
| O3 | Binance announcement page | News (T0 listing/delist) | finalized |
| O4 | Sandboxed fetch of an ingested item URL | News | finalized |
| O5 | Token unlock schedule | Fundamental, News | source not finalized |
| O6 | Telegram Bot API | Ops | finalized |
| O7 | Langfuse (self-hosted v4) | Ops tracing | finalized |

## 6. Lake layout
- Staging: append-only files per `source/route/date=YYYY-MM-DD/hour=HH`, compacted to Parquet (zstd) when the hour closes; only closed files are synced.
- Record: `{source, route, params_hash, fetched_at, status_ts, http_status, body_sha256, body_zstd}`.
- Integrity: per-partition hash chain, daily Merkle root pushed to object storage with object-lock.
- Universe: `universe/date=YYYY-MM-DD` point-in-time dataset.

## 7. Council internals
- Consensus: quorum >= 4 opinionated agents; winning side >= 2/3 of total normalized weight; still wins when `{technical, micro}` counts as one vote.
- Debate: claims stripped of `agent` and `domain`, new random `claim_id`, shuffled, truncated to 280 characters, instruction-like strings filtered. `verified`, `hard`, `tier` assigned by code; `hard` decided by the versioned hard evidence policy table (`council/hard_evidence.py`).
- Edit bounds: +-0.5 logit per round, +-1.0 total from `z_model`; flips need a `hard` claim with matching `direction_hint`; all `url` claims together count as one piece of evidence.
- Manager and `select_candidate()`: winning-side weight vote, ties by higher R:R, then closer to mark.
- Each event pins `config_version_ids` and `universe_date`.
- Graph (LangGraph, checkpointed in Postgres with `thread_id = event_id`, `durability="sync"`): `load_context -> agents via Send -> commit_round1 -> check_consensus -> share_claims -> agents round N -> apply_revisions -> ... -> aggregate -> manager -> emit_decision`. A restarted council resumes unfinished events from `council_events`.
- Blind commit: the round-1 forecast hashes go to `decision_commits` and one `audit_log` row (`council_round1_commit`) before any sharing.
- Verifier: packet claims are checked against the committed packet by `packet_sha256` (value within 1 %), tool claims by recorded `result_id`, url claims by a verbatim quote (>= 12 characters) in the stored copy; code sets `verified`, `hard`, `tier`, `domain` and the direction. Hard evidence policy `hard-evidence-v3`: a fired strict LTX/migration packet trigger, an official T0 item of a directional event class, or an on-chain event confirmed by 2 independent vendors (two routes of one vendor count once) whose direction comes from a code rule (security flag, or a net liquidity drain >= 250k USD over the 24 h before the capture, gives DOWN; a url confirms an on-chain event only for an exploit class, on the event coin, at T0/T1 or official), never from the claim's `direction_hint`. Tool claims must carry a value that matches a content leaf of the cited result. The News agent is shown only news-tool and url claims that repeat no candidate level or probability.
- Admission: the scanner XADDs a candidate only after its `scanner_log` row commits; the council treats a candidate whose row it cannot read as shadow-only. A candidate is never skipped for its age (owner decision 2026-09-28: no `decision_ttl_s`, the council waits for the debate result); each event pins its `scoring_params` version.
- Delivery: `emit_decision` writes the card, forecasts, claims, shadow-lesson forecasts, episodic memory and a `decision_outbox` row in one transaction; the in-process relay XADDs each row to `decisions` and then marks it published, however long after the candidate (at-least-once: a crash between the XADD and the mark republishes the same stable `event_id`, and Risk dedupes on `processed_decisions`). A decision has no time limit and the outbox has no `expired` status; a meeting that runs out of attempts is marked failed (alert `council_event_failed`).
- Agents (phase 05): each agent is `LlmAgentRunner` (bounded ReAct over its tool session, <= 8 calls) on the role's pinned model on its gateway; `validate` clips the logit, computes `p_used = sigmoid(z_model + a_i * clip(z_llm - z_model))`, checks the candidate and claim references. Forced-abstain reasons: `model_not_configured`, `model_self_test_failed`, `packet_unavailable:<status>`, `data_quality_<flags>`, `news_assessment_unavailable`, `news_mode_abstain`, `tool_budget_exceeded`, `llm_unavailable`, `llm_output_invalid`, `internal_error`. A replay cache miss raises `LlmCacheMissError`.
- News evidence (phase 07): extractor (verbatim quote checked by code) -> judges A and B from different developers (not required when both judges run on the DeepSeek gateway); any class or direction disagreement abstains (soft veto for EXPLOIT/DELIST). Tier is computed by code; Binance announcements are the only listing T0 source. `zF` is the cross-sectional z-score of hourly Binance funding across the universe.
- LLM gateway (owner decision 2026-09-28): every LLM role config (`models` section, `RoleModelConfig`) names its `gateway`. `openrouter` (default, planned long-term) sends a pinned `developer/model` slug with provider preferences and json_schema strict structured output, cost from `usage.cost`. `deepseek` (temporary) sends `deepseek-flash` or `deepseek-v4-pro` to `api.deepseek.com` with the key `llm/deepseek_api_key`, JSON Output (`json_object` plus the Pydantic JSON Schema in the system prompt, parsed by the same schema with one retry), the `thinking` toggle (default disabled; enabled sends `reasoning_effort`, omits temperature and passes each tool step's `reasoning_content` back), provider `deepseek`, and a cost priced from `config/deepseek.yaml` (peak/off-peak, cache hit/miss). The cache key keeps the gateways apart (provider routing `deepseek`, the gateway inside `params_hash`); OpenRouter keys and agent versions are unchanged. The console Models page drafts all 10 roles on DeepSeek, runs their model tests and saves the version.

## 7a. Scoring and go-live
- Scorer (`hdt.scoring.main`): resolver (RAW_12H / RESID_12H, 1 m mark-kline fallback, triple-barrier label, `missing` after 6 h, never dropped), Hedge sleeping experts (eta 0.13, daily fixed share 0.01, chosen by the synthetic oracle test) with regime hierarchy and caps, `a`/`r` trust, isotonic calibration with intercept on a purged window, claims audit, Reflection with a lesson A/B, conditional LightGBM stacking. Outputs `scored_forecasts`, `weight_history`, `calibration_*`, `scoring_params` (read by the council through `PgParamsSource`).
- Lessons: the `lessons` read model is a view over the append-only `store` lesson events, so a console approval is visible without a scorer run.
- Go-live (`hdt.golive`, phase 11): G4 on the paper final evaluation set (one-way time split in `golive_eval_splits`), per `target_type`; writes `gate_progress` and, through the scorer role only, `gate_flags` (read by the console live gate). The weekly job writes interim `gate_progress` only; G4 is decided exactly once per split generation by `python -m hdt.golive.final run` (rows in `golive_g4_finals`, the `gate_flags` row and the evidence file in one step; a second run is refused). The live gate decides G4 from the newest final only, and a DB trigger refuses a G4 `gate_flags` row that does not repeat it. A split must start in the future (DB CHECK, `clock_timestamp()` `created_at`); a new split generation is accepted only after the current one's final. The final records the pinned config by content (models, council, agent identities, static YAML digests); while live, models and council saves are refused (409 `live_config_locked`), and a changed pin closes the gate until a new split and final (a rollback to the pinned content reopens it). G4 also checks the whole paper account (Sharpe, PSR, max drawdown from `equity_snapshots`) and that model and council config stayed pinned over the window. Weekly report: Markdown/JSON plus a Telegram digest through the alert outbox (kind `weekly_report`).

## 8. Risk and execution invariants
- A decision has no time limit (owner decision 2026-09-28): Risk never rejects for age, and the entry legs it signs carry no `expires_at` (the GTX entry lives `post_only_wait_s`, then the IOC fallback). The money-path guard is one pure rule, `hdt.core.entry_guard`, applied twice to every OPEN: by Risk against the Binance mark at evaluation (step 5b) and by execution against its adapter mark (Binance premiumIndex, or the paper venue) right before the entry and again before its IOC fallback. Refused when the mark is at or beyond the invalidation (`stop_crossed`), at or beyond TP1 (`tp_crossed`) or farther than `max_entry_distance_atr` ATR from the entry (`entry_distance`; Risk signs that bound in price units as `OrderIntent.max_entry_distance`). Risk records the reason in `risk_verdicts.reason`; execution voids the entry intents with it (`processed_intents.reason`), raises `entry_refused` and counts `hdt_entries_refused_total{reason}`, and refuses to send without a mark (`no_mark`), so an entry that waited in `orders` while execution was down is sent only while its levels still hold. Risk still reserves an approved, unprocessed entry's risk in the portfolio limits for up to `PENDING_LOOKBACK` (1 day) or until execution marks it done, void or rejected.
- Only Risk signs `OrderIntent` (Ed25519, key mounted only into `risk`); `account` is inside the signed payload.
- Execution applies its own hard limits even for valid signatures, and is the only caller of signed endpoints.
- Every non-zero position has a reduce-only STOP on the exchange; checked after each fill, each reconcile cycle and after every re-sync.
- Client ids: `base32(sha256(event_id))[:20]-{leg}-{seq}` (<= 36 chars); `newClientOrderId` for regular orders, `clientAlgoId` for algo orders.
- One-way position mode checked at startup; refuse to run otherwise.
- Kill switch order: stop intake, cancel entries, cancel TP algos by id, verify STOP per position, optional flatten.
- Execution service (`python -m hdt.execution.main`): one runtime per namespace enabled by the pinned run mode (`build_namespace`: stop guard, order manager, kill switch, reconciler, re-sync, intake, AccountState publisher). Paper uses the lake-driven paper venue; testnet/live use `BinanceAdapter` on demo-fapi/fapi with the vault scope `exec` key, a Hedge Mode account is refused with a critical alert. The first RESYNC runs before `orders` (`exec-{account}`) is read. Loops: user stream or paper venue, AccountState, reconciler every `reconcile_interval_s`, order timers and `KillSwitch.enforce` every second, metrics (`hdt_account_*`, `hdt_open_positions`, `hdt_reconcile_mismatches_total`, `hdt_kill_state`). Service readers: `risk_flags` (`exec-flags`), `controls` (`exec-controls`: pause/resume/kill/flatten), config watcher, vault owner key test (`GET /fapi/v3/account`); a rotated key re-attaches its namespace.
- `hdt-kill` (independent path, Postgres + exchange only, never Redis or Telegram): writes `kill_state = killed` (command id `hdt-kill:` / `hdt-kill-flatten:`); a running service engages it within a second through `KillSwitch.enforce` (also for a repeated kill with a new command id and after a restart), otherwise the CLI runs the sequence itself. It never cancels a STOP, prints every position with its STOP and exits 0 only when every namespace is SAFE. `--flatten` needs `--confirm-flatten`.
- Testnet driver (`python -m hdt.execution.testnet_driver`): no council input, no Binance key; prices from demo public endpoints, intents signed with its own key (keyring: `testnet` only), lifecycle observed on `account_state`: GTX entry at the ask rejected, signed IOC fills, STOP seen, signed exit, flat. Reports STOP latency p50/p99; exit 0 only for N consecutive clean lifecycles with p99 <= 2 s. It shares the testnet namespace with the council decisions of run mode `testnet` (owner decision 2026-09-29), so run it on a symbol with no council position.

## 9. Configuration and secrets
- Static defaults in `config/*.yaml` (versioned files). Runtime configuration is immutable versions in Postgres (`config_versions`, `active_config`), changed through config-api and announced on `config_changed`; services apply changes at event boundaries.
- Application secrets (CMC, OpenRouter, DeepSeek, Binance, Telegram, news feed) are sealed in the browser with the scope public key; only the owning service holds the scope private key. Bootstrap secrets (per-role DSNs, Redis ACL users, scope private keys, session secret, OrderIntent signing key, TLS) live in a sops file mounted as docker secrets.
