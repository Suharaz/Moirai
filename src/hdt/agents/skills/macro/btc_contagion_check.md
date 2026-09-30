---
name: btc_contagion_check
title: Check BTC contagion risk
event_types: [LTX, MIGRATION, HELD]
---
Goal: check whether a BTC move can overwhelm the coin-specific setup within 12 hours.

1. From the packet, read the broad liquidation share (the share of market-wide liquidations in the last
   hours) and whether BTC itself has already flushed (a BTC liquidation spike followed by decay).
2. A high broad liquidation share while BTC has not flushed yet means a cascade can still spread from BTC
   to altcoins: an altcoin long taken now can be liquidated by the BTC move. Lean toward 0.5 or abstain.
3. After BTC has flushed and decayed, the contagion risk is lower; a coin-specific exhaustion setup is
   cleaner.
4. The coin's beta to BTC scales the risk: a high beta coin follows BTC more closely.
5. LTX setups are beta hedged, and their label is the residual return after the hedge; contagion still
   matters because the residual can blow out during a cascade.

Cite the broad liquidation share, the BTC flush state and the beta as packet claims.
