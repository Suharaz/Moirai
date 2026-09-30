---
name: squeeze_risk_check
title: Check for an opposite-side squeeze
event_types: [ALL]
---
Goal: before agreeing with any crowding thesis, look for the squeeze that would break it.

1. From `get_snapshot`, compare `liq_short_1h` with `liq_long_1h`. Short liquidations of at least twice
   the long liquidations in the last hour mean a short squeeze is running: do not lean SHORT. The mirror
   case (long liquidations at least twice the short ones) argues against leaning LONG.
2. Funding at an extreme on one side (crowded longs pay a high positive rate, crowded shorts pay a
   negative one) is fuel for a squeeze against that side.
3. Open interest that keeps rising into the move means new positions are still arriving; a squeeze needs
   open interest to start falling.
4. If the snapshot values are stale (the `stale` flag is set) you cannot rule a squeeze in or out: move
   `p_up` toward 0.5 rather than guessing.

Output: a note in your reasoning on squeeze risk, with the liquidation ratio and funding cited as tool
claims. A running squeeze against your direction is a reason to abstain or to stay close to 0.5.
