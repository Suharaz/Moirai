---
name: book_liquidity_check
title: Check book liquidity around entry and stop
event_types: [ALL]
---
Goal: judge whether the order book supports the setup, and whether a stop would slip.

1. Book imbalance: more resting bids than asks near the mark supports UP; the reverse supports DOWN.
   Imbalance that flips within minutes is spoofing risk; weigh it lightly.
2. Spread and depth: a wide spread or thin depth within 1 percent of the mark means fills and stops slip.
   Thin books amplify moves both ways, so pull `p_up` toward 0.5.
3. Compare the invalidation price of the relevant candidate with the visible depth between the mark and
   that price. If little depth separates them, normal noise can hit the stop; say so in your reasoning.
4. If depth features are missing (for example the coin just entered the hot set and the book is not yet
   rebuilt), do not guess: rely on the other skills or abstain.

Cite imbalance, spread and depth as packet claims.
