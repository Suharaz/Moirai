---
name: token_security_check
title: Token contract security check
event_types: [ALL]
---
Goal: make sure the token contract does not carry risks that make any upside view unsafe.

1. Call `dex_security` without an address (the event coin's recorded contract is used).
2. Red flags, any one of which rules out leaning LONG: a honeypot status that is not clean, a mintable or
   freezable contract, a rug pull or fake token status, a very high sell tax, or the token flagged or
   reported by the security vendor.
3. Read the hit security items: each has a code, a risk level and a description. Several high-level hits
   together are a strong negative; a single low-level hit is a note, not a verdict.
4. If the result is `not_available` or the capture is stale, state that the contract could not be
   checked; do not claim it is safe.
5. Security findings argue against LONG exposure; they do not by themselves justify SHORT, because
   perpetual price can ignore contract risk until it is exploited.

Cite the security level, the statuses and the hit items as tool claims.
