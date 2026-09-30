# Codebase Summary

Product name: Moirai (brand assets and guide in `brand/`). Repository: `Moirai` (GitHub `Suharaz/Moirai`). Reference material (`plans/`, `visuals/`, `cmc-docs/`) lives at the workspace root `nghien-cuu/`, not inside this repository.

## Tree
```text
Moirai/
  README.md  pyproject.toml  uv.lock  .python-version  .env.example  .gitignore  .sops.yaml
  .pre-commit-config.yaml  .secrets.baseline  alembic.ini
  docker-compose.dev.yml            dev Postgres + Redis
  docker-compose.yml                production stack (phase 10)
  config/                           versioned static defaults (no runtime state, no universe file)
  deploy/                           postgres init roles, redis ACL, prometheus, grafana, caddy, backup, runbook
  brand/                            Moirai logo SVGs, exports, brand book (`brand/README.md`)
  web/landing/                      static marketing landing page served by Caddy at `/` (dashboard at `/app`)
  docs/                             this documentation set
  scripts/                          check_env, check_language, redis ACL generator, later phase scripts
  services/fetcher/                 sandboxed article fetcher container (phase 07)
  src/hdt/                          application package
  tests/                            unit, contract, integration, replay, fixtures
```

## Packages and owning phase
| Package | Purpose | Phase |
|:--|:--|:--|
| `hdt.core` | clock, canonical ids/hashing, config loading (`BootstrapSettings` via pydantic-settings, `HDT_CONFIG_DIR`), JSON logging, Redis Streams helper (`StreamConsumer.run`, poison limit) | 01 |
| `hdt.contracts` | v1 data contracts + stream names | 01 |
| `hdt.db` | SQLAlchemy base, sessions, natural-key dedupe (`insert_once`), auto-discovered models, Alembic migrations | 01 (+ each phase adds models) |
| `hdt.settings` | hard ceilings (01), section schemas, immutable versions, `config_changed` events (12) | 01, 12 |
| `hdt.vault` | log redaction (01), SealedBox scopes, loader, owner self-test protocol (12) | 01, 12 |
| `hdt.configapi` | FastAPI config-api: auth, TOTP, CSRF, step-up, audit, section routes | 12 |
| `hdt.console` | Streamlit admin console (`app.py` gate + navigation, 16 page scripts in `views/` + sign-in) | 12 |
| `hdt.llm` | OpenRouter catalog and key check; DeepSeek price table and static catalog (`deepseek`, `config/deepseek.yaml`) and key check (`deepseek_key_check`, `/user/balance`); `key_checks` (`LLM_SECRET_CHECKS` of the `llm` scope) | 12, 05 |
| `hdt.ingest` | CMC client/collectors/WS, credit meter/governor, Binance public REST/WS, scheduler, symbol map | 02 |
| `hdt.lake` | raw store (staging -> Parquet, hash chain), PIT query, universe | 02 |
| `hdt.features` | per-agent features, liquidation/funding semantics, reconciliation, price levels | 03 |
| `hdt.quant` | per-agent `p_model`, QuantPacket, cache, scanner | 03 |
| `hdt.tools` | live/replay tool registries, budget enforcer, tool implementations | 04 |
| `hdt.memory` | bitemporal memory store, lessons, episodic records | 04 |
| `hdt.agents` | skills (04); `llm_types` (StructuredLlm protocol), `llm_router` (per-role gateway: OpenRouter or DeepSeek client, `llm_calls` ledger, `llm_cache`, replay cache-only), `runner` (`LlmAgentRunner`, bounded ReAct per agent), `validate` (clip, `p_used`, candidate and claim checks, forced abstain), `model_test` (structured-output self-test, `RoleHealth`), `versions` (agent_versions book), `prompting`, `prompts/*.md` | 04, 05 |
| `hdt.news` | sources and ingest (RSS/Atom, licensed API, Binance announcements, unlock calendar), dedupe, text, tiering, official, onchain, extractor, judge, verdict, modes, market, attention, assess (`PgNewsAssessor`), source_lookup, fetcher_client, store, unlocks, veto, `veto_scan`, `main`, `evaluate` | 07 |
| `hdt.council` | ports, consensus, aggregate, manager, claims, verifier, hard_evidence, revision, commit, graph (LangGraph), checkpoint, decision_card (card + outbox relay), trigger, adapters, `main` | 06 |
| `hdt.scoring` | resolver, loss, hedge, regime_weights, oracle, coverage, llm_trust, revision, calibration, engine, params (`PgParamsSource`), stacking, shadow, store, claims_audit, reflection, service, `main` | 08 |
| `hdt.golive` | G4 statistics and gate, ablation replay, event study, threshold calibration proposals, paper-vs-live comparison, weekly report, evaluation split | 11 |
| `hdt.risk` | risk service: gate, sizing, limits, veto, hedge book, signer, kill state | 09 |
| `hdt.execution` | execution service (`main`): verify, limits, Binance client/adapter, venues, paper engine, order manager, stop guard, reconciler, kill switch, `hdt-kill` CLI (`kill_cli`), vault key check, testnet driver | 09 |
| `hdt.ops` | metrics, alerts, Telegram bot, labeling app | 10, 07 |
| `hdt.public`, `hdt.public_dashboard` | snapshot publisher/allowlist/schema and the public dashboard | 10 |

## Status
Implemented phases: 01 (skeleton, dependencies, dev infrastructure, config defaults, contracts, core primitives, database base, language check, CI), 12 (admin console and secrets vault), 02 (ingestion and lake), 03 (quant core, scanner, gate G1 tooling), 04 (tool registry, memory, skills), 09 (risk and execution) and 10 (ops, production compose, backups, public dashboard). Phases 05 (LLM agents), 06 (council), 07 (news evidence), 08 (scoring and learning) and the phase 11 tooling (`hdt.golive`, `scripts/event_study.py`, `scripts/ablation_replay.py`, `scripts/weekly_report.py`) are implemented; they were built before gate G1 passed, by owner decision. Migrations run in one chain up to `0011_golive`. Phase 09 wires the `risk` and `execution` services, `hdt-kill` and the testnet driver; the testnet success criteria need demo keys and wall-clock runs.
