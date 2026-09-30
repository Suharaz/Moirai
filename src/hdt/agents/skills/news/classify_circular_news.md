---
name: classify_circular_news
title: Classify circular news and hollow hype
event_types: [HOLLOW_HYPE, HELD]
---
Goal: separate attention without substance from a real catalyst.

1. Call `get_news` for the last 24 hours. Group items by what they report, not by headline wording.
2. Circular: several articles that all trace back to one source, often a single post or a price move
   itself (a coin is up, so articles say it is up). No new fact enters the chain.
3. KOL or meme: the story is an influencer mention, a community campaign or a meme, with no product,
   partnership or exchange fact behind it.
4. Rumor: an unconfirmed claim with no T0 source; `check_official` finds nothing.
5. Real catalyst: a new fact from a T0 source (exchange announcement, the project's own channel, on-chain
   data) that `known_events` does not already hold.
6. Fetch the originals with `fetch_source` for the one or two items that carry the claim, not for every
   repost. Quote the sentence that states the claim.

Fade hype leans SHORT only when all hold: class circular, KOL/meme or rumor; low hardness; strong
attention; open interest up sharply; and no short squeeze (1 h short liquidations below twice the long
ones). Fade panic leans LONG only with T0 refuting evidence (an independent on-chain check or a third
party), never on the project's own denial. Without that evidence, abstain.
