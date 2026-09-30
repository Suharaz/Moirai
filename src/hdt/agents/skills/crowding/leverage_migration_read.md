---
name: leverage_migration_read
title: Read a leverage migration
event_types: [MIGRATION, HELD]
---
Goal: tell whether leverage piling into this coin is fresh conviction or crowding that is about to be
forced out.

1. Call `quant_core` and read the migration features: the share of open interest moving into the coin
   (S_P), its z-score against the universe (zM), the gap between open interest growth and price (G), the
   forced deleveraging ratio (FD) and the basis gap (BG).
2. Open interest rising much faster than price (large positive G) with a rich basis means late, crowded
   longs: it leans against further upside. Open interest rising with falling price and a discounted basis
   means crowded shorts: it leans against further downside.
3. A migration that coincides with rising forced deleveraging (FD up) is being unwound; follow the unwind
   direction only if the liquidations are one-sided.
4. Call `get_snapshot` for `funding_rate`, `next_funding_time`, `funding_interval_h` and `basis_pct`.
   Normalize funding to an hourly rate before comparing coins with different intervals. A funding payment
   due within the hour can move price briefly; do not treat that move as a signal.
5. If the packet flags any migration feature as stale or missing, abstain.

Output: `p_up` anchored on `p_model`. State which side is crowded and why, citing G, zM and the basis.
