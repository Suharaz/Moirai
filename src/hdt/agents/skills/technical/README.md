# Technical agent skills

The Technical agent reads trend, momentum and divergence features from its `quant_core` packet
(computed on recorded Binance klines at the event time). It has no other tool.

| Skill | Event types | Purpose |
|:--|:--|:--|
| `confluence_checklist` | ALL | Count independent technical confirmations before moving away from `p_model` |
| `divergence_read` | LTX, MIGRATION, HOLLOW_HYPE | Judge whether price and momentum disagree at an extreme |

Rules shared by every skill:
- Only packet values are evidence; cite each as a `packet` claim with the exact number.
- Timeframes that are stale or missing in `data_quality` do not count.
- Technical and Microstructure count as one vote for the consensus rule, so repeat nothing the order
  flow already says; bring the chart view.
