# Project Overview and Product Development Requirements

## 1. Product
Moirai (repository `Moirai`, package `hdt`; named after the three Fates who spin, measure and cut the thread) is an AI trading system for crypto perpetual futures on Binance USDS-M. A council of six LLM agents, each backed by a deterministic quantitative core, forecasts the 12 h direction of a coin when a scanner raises a candidate. Code verifies every claim the agents exchange, a fixed-rule Manager decides, an independent Risk service sizes and signs orders, and an Execution service is the only component that talks to signed Binance endpoints. Agent weights learn from forecast outcomes.

Real money is used only after the shadow gate G4 passes (phase 11). The system trades a limited-capital sub-account at 1/4 size first.

Design source of truth: `plans/260927-0641-ai-derivatives-trading-council/research/00-design-contract.md` (workspace root `nghien-cuu/`). If a doc here conflicts with the Design Contract, the Design Contract wins.

## 2. Goals
| # | Goal | Priority |
|---|------|----------|
| 1 | Immutable CMC + Binance recorder runs continuously, measures real CMC credit, has a credit governor | P1 |
| 2 | Gate G1: measure LTX edge on the lake before building the LLM layer | P1 |
| 3 | Six-agent council + debate + Manager run deterministically on replay; Risk is a separate service | P1 |
| 4 | Weights `w`, `a`, `r` self-learn; learnability proven by synthetic tests and shadow | P1 |
| 5 | Safe execution: a STOP always exists, orders are signed by Risk, paper trading runs on mainnet data | P1 |
| 6 | Admin console configures models, keys, parameters and run mode safely; reachable only over Tailscale/SSH | P1 |
| 7 | Pass the statistically valid gate G4 before going live at 1/4 size | P1 |
| 8 | Public read-only performance dashboard with no path to trading stores | P1 |

## 3. Users
- Owner/operator (1-3 people): configures and supervises through the private admin console and Telegram.
- Public visitors: read the public performance dashboard (positions, scored decisions, agent metrics, LLM cost). They have no controls.

## 4. Functional requirements
### 4.1 Data
- Record 30 CoinMarketCap routes (29 REST + 1 WebSocket) and Binance public data verbatim into an append-only lake with `fetched_at` and sha256; point-in-time queries only return records with `fetched_at <= as_of`.
- Credit meter and credit governor for the CMC Startup plan (daily budget = remaining credit / days until billing anchor; load shedding context/DEX first, derivatives last). Error codes 1009/1010/1011 are separate halt states.
- The universe is a point-in-time dataset in the lake (`universe/date=`), never a config file.

### 4.2 Signal and quant core
- LTX (Liquidation Term-Structure Exhaustion) is the core signal; Leverage Migration is a meta-model feature; Hollow Hype is the News agent module.
- Every number is computed by code: per-agent features, a separate logistic `p_model` per agent, one shared price-level candidate set with `side` per `(coin, as_of)`.
- The scanner emits `Candidate` (LTX / MIGRATION / HELD); phase 07 emits HOLLOW_HYPE. Only candidates trigger the council.

### 4.3 Council
- Six agents: Crowding, Technical, Microstructure, Fundamental, News, Macro. Each is an LLM called through OpenRouter plus the `quant_core` tool. The model per agent is chosen on the admin console.
- The LLM adjusts in logit space: `z_used = z_model + a_i * clip(z_llm - z_model, -0.5, 0.5)`, computed by code.
- Round 1 is blind and hash-committed. Debate of at most 3 rounds exchanges only anonymous, code-verified claims. Direction flips require a code-classified `hard` claim in the same direction.
- Consensus: quorum >= 4, winning side >= 2/3 of normalized weight, and it still wins when each correlated group counts as one vote.
- Manager: fixed rules (size 1.0 on consensus; 0.5 after 3 rounds when pooled `p` >= 0.60 / <= 0.40 and disagreement `D` below threshold; otherwise NO_TRADE).
- Aggregation: log opinion pool with learned intercept and out-of-sample calibration, clipped to [0.02, 0.98].

### 4.4 Learning
- `w` by Hedge with sleeping experts (abstain = pool loss), hierarchical per-regime offsets, floor 5 %, ceiling 40 %, learned from round 1 only.
- `a` (starts 1.0) and `r` (starts 0.5) by EMA on loss differences. All tables split by `target_type` (`RAW_12H`, `RESID_12H`) and `label_spec_version`.
- Reflection lessons are structured, shadow-tested and human-approved on the console before activation.

### 4.5 Risk and execution
- Risk (separate service): dedupe, packet integrity by sha256, same-side candidate, sign check `p_side > 0.5`, OPEN/HOLD/EXIT by position, OPEN levels re-checked against the current mark (no decision TTL, owner decision 2026-09-28), fail-closed news veto, deterministic sizing, hard ceilings, Ed25519 signature on `OrderIntent`.
- Execution: signature verification, its own hard limits, order lifecycle, emulated OCO, STOP invariant every reconcile cycle, REST re-sync on user stream reconnect, kill switch, `AccountState` publishing.
- Account namespaces `paper | testnet | live` are fully separated.

### 4.6 Console and dashboard
- Admin console (Streamlit + FastAPI config-api): model per role (10 roles), write-only API keys sealed per scope in the browser, CMC routes with credit projection, Binance/Risk/Council parameters (tighten-only within code ceilings), run mode with live gate, pause/resume/kill/flatten, lessons review, audit, immutable config versions with rollback.
- Public dashboard: separate deployment reading only allowlisted, delayed, schema-validated JSON snapshots.

## 5. Non-functional requirements
- Python 3.12, uv with hash-pinned lock, Pydantic v2 contracts, deterministic replay (injected clock, canonical JSON, LLM cache miss in replay is a hard error).
- Security: secrets never in git; per-scope sealed secrets; per-service Postgres roles and Redis ACL users; mandatory log redaction; only port 443 is public.
- English-only repository enforced by `scripts/check_language.py` in pre-commit and CI.

## 6. Hard ceilings (code constants, configuration may only tighten)
| Ceiling | Value |
|:--|:--|
| Leverage | <= 3x, isolated |
| Risk per trade | <= 0.5 % of equity |
| Open positions | <= 4 (hedge book excluded), <= 1 per coin |
| Same-direction risk | <= 1.5 % of equity, including the BTC hedge book |
| Daily loss | -2 % kills the namespace |
| Live size multiplier | <= 1, default 0.25 |

## 7. Gates
- **G1** (end of phase 03): pre-registered 12 h event study on the recorded lake. Blocks phases 05-08 until results have enough power. If the CI contains 0 once powered, the core strategy is replanned.
- **G4** (phase 11, final evaluation set, paper only): >= 150 independent entry orders; Spiegelhalter |Z| < 1.96 and calibration slope 95 % CI contains 1; annualized daily Sharpe > 1.5 and PSR(0) >= 0.95; max drawdown < 10 %; each retained component wins a paired bootstrap log-loss ablation with Holm correction.

## 8. Acceptance by phase
| Phase | Acceptance summary |
|:--|:--|
| 01 Bootstrap | `uv run pytest` green; `check_env.py` correct; 9 docs; `check_language.py` passes; contract round-trip tests |
| 12 Console | Sealed scopes cannot be decrypted by config-api or foreign scopes; ceilings enforced on direct API calls; version pinning; live gate refused without G4/checklist/TOTP |
| 02 Lake | 72 h run without core gaps; PIT look-ahead property test; 7-day credit pilot; hash chain detects tampering; simulated 1010 halts correctly |
| 03 Quant | Deterministic packet hashes; levels property test; liquidation and funding semantics tests; scanner tests; G1 report |
| 04 Tools/memory | Replay opens no socket; bitemporal memory property test; identical prompts on replay |
| 05 Agents | No output passes bounds violations; parse error rate < 1 %; provenance fields recorded |
| 06 Council | Anti-manipulation scenarios pass; deterministic replay; resume after kill |
| 07 News | SSRF tests; labeled-set precision/recall targets; no Fade panic without refuting evidence |
| 08 Scoring | Synthetic oracle test; sleeping experts invariance; idempotent rescoring |
| 09 Risk/Exec | Testnet lifecycles; STOP within 2 s; exactly-once entry; sizing properties; namespace isolation |
| 10 Ops | Healthy compose; network isolation tests; snapshot allowlist tests; only 443 public |
| 11 Shadow | G4 passes; live 1/4 size matches shadow within tolerance |

## 9. Non-goals
- No LLM ever places orders or writes prices, sizes or leverage.
- No Langfuse cloud; no embeddings (MinHash dedupe instead); no `similar_cases` tool.
- MerkleAI is an architecture reference only (no LICENSE); its LLM-places-orders flow is not copied.
- No direct Anthropic/OpenAI keys; every LLM goes through OpenRouter.

## 10. Open decisions
1. Full-text news feed source and unlock calendar source (phase 07 step 1).
2. OpenRouter model slug per role (chosen on the console, tested before applying).
3. Binance key-permission check endpoint (phase 12 step 1).
4. Actual CMC Startup quota, read from `/v1/key/info` once a key exists.
5. Actual LTX trigger frequency; if too low, G1 extends the recording time.
