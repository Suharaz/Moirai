---
name: divergence_read
title: Read price and momentum divergence at an extreme
event_types: [LTX, MIGRATION, HOLLOW_HYPE]
---
Goal: after a forced or crowded move, judge whether momentum already disagrees with price.

1. From the packet, compare the latest price extreme with the previous one and the momentum reading at
   each. A lower price low with a higher momentum low is bullish divergence; a higher price high with a
   lower momentum high is bearish divergence.
2. Divergence counts only at an extreme: after a liquidation spike (LTX), a leverage build-up (MIGRATION)
   or an attention spike (HOLLOW_HYPE). In the middle of a range it is noise.
3. Divergence on the 1h view outweighs the 15m view; divergence on both is stronger.
4. A divergence against the event direction (for example bearish divergence on an LTX long setup) is a
   reason to stay near `p_model` or to lean the other way by a small amount.
5. If the packet shows no divergence features or flags them as missing, say so and do not infer one.

Cite the two extremes and their momentum values as packet claims.
