---
name: verify_exchange_listing
title: Verify an exchange listing or delisting claim
event_types: [ALL]
---
Goal: accept a listing or delisting only when the exchange itself announced it.

1. Call `known_events` for the coin. If an event key for this listing already exists (for example
   `binance_spot_listing:<symbol>`), the news is not new: it cannot be a fresh catalyst.
2. Call `check_official` with the exchange (for example `binance`) and a keyword: the coin symbol, then
   the project name if the symbol gives no match.
3. `official: true` with a matching title is T0 evidence. Check that the announcement date fits the news
   timeline; an old announcement re-posted as new is recycled hype.
4. `official: false` means the claim is unverified, whatever the article says. An article quoting an
   official announcement that `check_official` cannot find is at most T1 and may be fabricated.
5. Pages that look official on another domain do not count: only the exchange's own domains are checked.
6. A delisting verified as T0 is a hard negative: say so clearly, because it feeds the veto.

Output: for a verified new listing, the Ride mode needs high hardness, T0, high novelty and news no older
than 2 hours; otherwise treat the story as attention only.
