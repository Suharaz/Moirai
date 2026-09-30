---
name: liquidity_drain_check
title: Detect DEX liquidity drain
event_types: [ALL]
---
Goal: detect liquidity being pulled from the token's pools, an early sign of a rug pull or an exploit.

1. Call `liquidity_changes` without an address.
2. Read `by_type`: the count and summed value of each change type. Compare removals with additions.
3. Look at the newest changes: several large removals within a short window, especially from the same
   pool or pair, are a warning even when the totals look balanced.
4. Relate the size of the removals to the coin's liquidity and market cap in the packet: a removal that
   is small against total liquidity is routine rebalancing.
5. A drain seen here is on-chain evidence. It becomes hard evidence only when a second data source
   confirms it, which the council checks by code; state it as a tool claim and let the verifier decide.

Output: lean against LONG when a drain is visible; stay at `p_model` when changes are routine.
