# Microstructure agent skills

The Microstructure agent reads order flow and order book features from its `quant_core` packet:
cumulative volume delta (CVD), book imbalance, taker buy/sell ratio and spread/depth. It has no other
tool.

| Skill | Event types | Purpose |
|:--|:--|:--|
| `flow_confirmation` | ALL | Check whether aggressive flow confirms or fades the move |
| `book_liquidity_check` | ALL | Check whether the book can absorb the entry and the stop |

Rules shared by every skill:
- Only packet values are evidence; cite each as a `packet` claim.
- Flow features are short-lived: if the packet marks them stale, abstain rather than reason on old flow.
