---
name: unlock_supply_pressure
title: Weigh scheduled token unlocks
event_types: [ALL]
---
Goal: weigh scheduled supply unlocks against the 12 hour forecast horizon.

1. Call `unlock_schedule` (defaults: 7 days back, 30 days ahead).
2. Only unlocks within roughly the next 24 hours matter directly for a 12 hour horizon. Read
   `pct_next_7d` for the size of near-term supply relative to circulating supply.
3. An unlock of several percent of circulating supply within the horizon is selling pressure, stronger
   for investor and team categories than for ecosystem or reward emissions.
4. Markets often price an unlock before it happens: a large drop in the days before the unlock can mean
   the pressure is already priced, and a relief move after it is possible.
5. Unlocks that already happened (negative `hours_from_as_of`) explain recent supply, not future supply.
6. If the schedule is `not_available`, say so; do not assume there is no unlock.

Cite unlock size, date and category as tool claims.
