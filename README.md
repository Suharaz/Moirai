<picture>
  <source media="(prefers-color-scheme: dark)" srcset="brand/logo/moirai-lockup-horizontal.svg">
  <img src="brand/logo/moirai-lockup-horizontal-ink.svg" alt="Moirai" height="64">
</picture>

# Moirai

Repository `Moirai`, Python package `hdt`. Brand assets and guide: `brand/`.


AI council for crypto perpetual futures trading on Binance USDS-M. Six LLM agents (each model chosen on the admin console, called through OpenRouter) work on top of a deterministic quantitative core, debate for at most three rounds using code-verified anonymous claims, and a fixed-rule Manager decides. An independent Risk service sizes and signs orders; an Execution service is the only component holding exchange keys. Agent weights learn from forecast outcomes. CoinMarketCap (Startup plan) is the signal layer, Binance is the execution layer and source of truth.

> **Financial risk warning.** This software trades leveraged derivatives. Leveraged trading can lose more than the capital allocated to it, and past or simulated performance does not predict future results. The system runs paper trading first and may only trade real money after the statistical shadow gate G4 passes, at 1/4 size, on a limited-capital sub-account with a trade-only API key (no withdrawals, IP whitelist). Nothing in this repository is investment advice. Use it at your own risk.

## Architecture in one picture
```text
recorder (CMC 30 routes + Binance public) -> immutable lake (Parquet, hash chain) -> point-in-time queries
  -> quant core (features, per-agent p_model, shared price levels) -> scanner -> stream `candidates`
  -> council (6 agents, blind round 1, <= 3 debate rounds, aggregate, Manager) -> outbox -> stream `decisions`
  -> risk (fail-closed veto, sizing, hard ceilings, Ed25519 signature) -> stream `orders`
  -> execution (verify, own limits, STOP invariant, reconcile, kill switch) -> stream `account_state`
  -> scorer (12 h labels -> w / a / r / calibration) -> back to the council
admin console (private, Tailscale + password + TOTP) configures models, sealed keys, parameters, run mode
public dashboard (read-only snapshots) shows positions, scored decisions, agent metrics, LLM cost
```
Details: [`docs/system-architecture.md`](docs/system-architecture.md). Design source of truth: `plans/260927-0641-ai-derivatives-trading-council/research/00-design-contract.md` at the workspace root.

## Development
```bash
uv python install 3.12
uv sync --frozen
cp .env.example .env                              # local bootstrap values only, git-ignored
uv run python scripts/check_env.py
docker compose -f docker-compose.dev.yml up -d
uv run alembic upgrade head
uv run pytest
uv run ruff check . && uv run mypy
uv run python scripts/check_language.py
```
Application API keys (CMC, OpenRouter, Binance, Telegram, news feed) are never placed in `.env`; they are entered on the admin console and sealed per scope. See [`docs/deployment-guide.md`](docs/deployment-guide.md).

## Language policy
The whole repository is English: code, identifiers, comments, logs, UI copy, LLM prompts, commit messages and docs. `scripts/check_language.py` enforces it in pre-commit and CI.

## Documentation
| Doc | Content |
|:--|:--|
| [project-overview-pdr](docs/project-overview-pdr.md) | goals, requirements, gates, non-goals |
| [system-architecture](docs/system-architecture.md) | services, networks, contracts, streams, producer/consumer matrix |
| [api-endpoints-design](docs/api-endpoints-design.md) | CMC, Binance, OpenRouter, Telegram, config-api |
| [code-standards](docs/code-standards.md) | toolchain, determinism, security, testing rules |
| [codebase-summary](docs/codebase-summary.md) | repository tree and package ownership |
| [design-guidelines](docs/design-guidelines.md) | console and dashboard design system |
| [deployment-guide](docs/deployment-guide.md) | dev setup, secrets model, VPS target, run modes |
