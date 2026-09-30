# Crowding agent skills

The Crowding agent reads liquidation term structure and leverage positioning (LTX, Leverage Migration)
from its `quant_core` packet and checks the live state with `get_snapshot`.

| Skill | Event types | Purpose |
|:--|:--|:--|
| `ltx_exhaustion_check` | LTX, HELD | Confirm that a liquidation cascade is exhausted before leaning against it |
| `leverage_migration_read` | MIGRATION, HELD | Read where leverage moved and whether it is being forced out |
| `squeeze_risk_check` | ALL | Look for the opposite-side squeeze that breaks a crowding thesis |

Rules shared by every skill:
- Numbers come only from tool results; cite them as `packet` or `tool` claims with the exact value.
- A value flagged `stale` or listed under `data_quality` / `missing` is not evidence. If a main feature
  of the setup is stale or missing, abstain.
- The packet `p_model` is the anchor; move away from it only for reasons you can cite.
