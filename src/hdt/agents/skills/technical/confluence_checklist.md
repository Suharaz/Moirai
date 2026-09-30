---
name: confluence_checklist
title: Technical confluence checklist
event_types: [ALL]
---
Goal: move `p_up` away from `p_model` only when several independent technical readings agree.

Work through the packet features and mark each item as supporting UP, supporting DOWN or neutral:
1. Trend: price against its moving averages on the 15m and 1h views, and the slope of those averages.
   Price above rising averages supports UP; below falling averages supports DOWN.
2. Momentum: RSI level and direction. Readings above 70 or below 30 are stretched, not directional by
   themselves; a turn back from those zones is the signal.
3. Volatility: realized volatility and the range position. Very high volatility widens the outcome
   distribution, so pull `p_up` toward 0.5.
4. Volume: whether the move came on rising volume. A breakout on falling volume is weak.
5. Levels: distance from the entry, invalidation and TP1 of the shared candidate set. A candidate whose
   invalidation sits inside normal noise is fragile.

Scoring:
- Three or more items on the same side, none strongly against: you may move toward that side, within the
  allowed margin.
- Mixed items: stay at `p_model`.
- Stale or missing timeframes: skip those items; if trend and momentum are both unavailable, abstain.

Cite each item you used as a packet claim. Do not describe chart patterns that no packet feature shows.
