---
name: flow_confirmation
title: Confirm the move with aggressive flow
event_types: [ALL]
---
Goal: decide whether market orders are still pushing in the event direction or already fading it.

1. CVD: compare the CVD change with the price change over the same window. Price up with CVD up is
   confirmed buying; price up with CVD flat or down means passive sellers are absorbing, which leans DOWN.
   The mirror holds for moves down.
2. Taker ratio: a taker buy/sell ratio far from 1 in the direction of the move confirms it; a ratio
   crossing back toward 1 after an extreme is the first sign of exhaustion.
3. Absorption after a liquidation spike: large aggressive selling that fails to push price lower means
   the forced flow is being absorbed, which supports an LTX long (mirror for shorts).
4. Weigh the most recent window most. Flow older than the stale threshold is not evidence.

Output: `p_up` near `p_model` unless CVD and the taker ratio agree; cite both as packet claims.
