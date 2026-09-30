# Code Standards

## 1. Language and style
- The whole repository is English: code, identifiers, comments, docstrings, log and error messages, UI copy, LLM prompts, commit messages and docs. Only the conversation with the owner is Vietnamese.
- `scripts/check_language.py` runs in pre-commit and CI and fails on Vietnamese diacritics (NFC-normalized) outside `tests/fixtures/raw/` and `data/`.
- Never use the em dash or en dash characters; use `-`. No emojis anywhere (UI uses one 2px-stroke SVG icon set).
- Conventional commits (`feat:`, `fix:`, `docs:`, `test:`, `chore:`, `refactor:`).

## 2. Toolchain
- Python 3.12, uv. `uv sync --frozen` in CI and images (the lock file carries hashes). Only the lock owner changes dependencies; every dependency is pinned.
- Lint: `ruff check` + `ruff format`. Types: `mypy --strict` for `src/hdt/core`, `src/hdt/council`, `src/hdt/scoring`, `src/hdt/risk`; the rest of `src/hdt` is type-checked in normal mode.
- Tests: `pytest` with markers `pg`, `redis`, `integration`, `network` (live external calls, deselected by default); property tests use `hypothesis`.
- Supply chain: `pip-audit` and `osv-scanner` in CI, `detect-secrets` in pre-commit, images pinned by digest.

## 3. Layout
- Package `src/hdt/`, one sub-package per concern (`core`, `contracts`, `db`, `settings`, `vault`, `ingest`, `lake`, `features`, `quant`, `tools`, `memory`, `agents`, `council`, `news`, `scoring`, `risk`, `execution`, `ops`, `public`, `console`, `configapi`, `llm`).
- Contracts are defined once in `src/hdt/contracts/` and imported everywhere; never redefine a contract model in another package.
- Database models live in `src/hdt/db/models/<area>.py`; modules are auto-discovered. Migrations are hand-written Alembic revisions with pre-assigned ids.
- Files stay small and focused (target < 200 lines); split by responsibility, not by size alone.

## 4. Determinism
- Time comes only from `hdt.core.clock`; replay injects a fixed or stepped clock. Never call `datetime.now()` directly in business code.
- Hashes are computed over canonical JSON from `hdt.core.ids` (sorted keys, no insignificant whitespace, UTC timestamps, exact context-independent decimals, integral floats rendered from their shortest digits so a Postgres JSONB round-trip re-hashes identically). There is exactly one canonical JSON implementation.
- Hashed contracts (`QuantPacket`) are deeply immutable; `model_copy` re-validates the hash. LLM output is parsed only with draft models (`ClaimDraft`, `AgentForecastDraft`); code-assigned fields (`p_model`, `p_used`, provenance, `verified`, `hard`, `tier`, `domain_url`) never come from LLM text.
- Point-in-time: every feature takes `as_of` and reads only records with `fetched_at <= as_of`.
- Replay never touches the network; an LLM cache miss in replay is a hard error.
- Thresholds are never literals in code: they come from `config/*.yaml` (versioned static files), `config/p_model/<agent>.<target_type>.yaml`, or pinned runtime config versions.

## 5. Security rules
- No secret in git, logs or error messages. `hdt.vault.redact` is installed on every logger: any field whose name contains a secret keyword (`api_key`, `secret`, `password`, `token`, `signature`, `listenKey`, `dsn`, `private_key`, `sealed_blob`, `authorization`, `credential`, including prefixed names such as `CMC_API_KEY`), URL userinfo passwords, bearer/Telegram/OpenRouter tokens, and every bootstrap secret read through `hdt.core.config` (registered for exact-match redaction). `OrderIntent.signature` is excluded from reprs.
- Application secrets are only read through `hdt.vault.loader` inside their scope owner; bootstrap secrets come from docker secret files.
- External content and other agents' output are schema-bound data, never instructions. Unknown fields on stream messages never become orders.
- Hard ceilings are code constants in `hdt.settings.ceilings`; configuration may only tighten them.
- LLMs never produce prices, sizes, leverage or order parameters; they choose a `candidate_id` and a bounded probability adjustment.

## 6. Error handling and logging
- Structured JSON logs through `hdt.core.logging`; include `event_id`, `coin_id`, `account` where relevant.
- Typed exceptions per integration (e.g. CMC error codes map to distinct exception classes); halt states are explicit, not retried blindly.
- Retries use exponential backoff with jitter and respect rate-limit headers.
- Stream consumers `XACK` only after the durable write; idempotency through dedupe tables keyed on natural keys (`hdt.db.dedupe.insert_once`, same transaction as the effect). A message failing more than `max_deliveries` times is dead-lettered.

## 7. Testing
- Pure functions (features, sizing, consensus, aggregation, scoring) have table tests with hand-computed values and property tests for invariants.
- Contract round-trip tests cover every producer -> consumer pair.
- Tests needing Postgres/Redis use the shared fixtures in `tests/conftest.py` (unique database per session, random Redis DB index) so parallel runs do not collide.
- Tests must catch plausible consumer-visible bugs; no tests that only check wiring or mock echoes.
