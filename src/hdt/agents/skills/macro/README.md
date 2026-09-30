# Macro agent skills

The Macro agent reads the market regime from its `quant_core` packet: BTC trend and volatility regime,
Fear and Greed, BTC dominance, altcoin season index and total market metrics. It has no other tool.

| Skill | Event types | Purpose |
|:--|:--|:--|
| `regime_read` | ALL | Place the event in the trend/sideways and high/low volatility regime |
| `btc_contagion_check` | LTX, MIGRATION, HELD | Check whether a BTC flush can drag the coin |

Rules shared by every skill:
- Only packet values are evidence; cite each as a `packet` claim.
- Regime features change slowly: the same regime supports many events, so weigh it as context, not as
  a trigger.
