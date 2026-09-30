# Fundamental agent skills

The Fundamental agent reads open interest to market cap turnover, supply and DEX features from its
`quant_core` packet, and checks the token itself with `dex_security`, `liquidity_changes` and
`unlock_schedule`.

| Skill | Event types | Purpose |
|:--|:--|:--|
| `token_security_check` | ALL | Rule out honeypot, mint and freeze risks before any LONG view |
| `liquidity_drain_check` | ALL | Detect liquidity pulled from the token's DEX pools |
| `unlock_supply_pressure` | ALL | Weigh scheduled unlocks against the 12 h horizon |

Rules shared by every skill:
- Cite tool results as `tool` claims by `result_id`; cite packet values as `packet` claims.
- `not_available` is not evidence of safety. A coin with no recorded DEX data (for example a coin
  without a token contract) is judged on the packet alone.
- Captures older than the `stale` threshold describe the past; say how old they are.
