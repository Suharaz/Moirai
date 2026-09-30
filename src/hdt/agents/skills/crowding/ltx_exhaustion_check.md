---
name: ltx_exhaustion_check
title: Confirm liquidation exhaustion (LTX)
event_types: [LTX, HELD]
---
Goal: decide whether the forced-flow side of a liquidation cascade is spent, so price can mean-revert
against it over the next 12 hours.

1. Call `quant_core`. Read the LTX family: SPIKE (recent 1h-4h liquidation rate against the 24h
   baseline), DECAY (last hour against the 1h-4h rate), FDz (liquidations over open interest, as a
   cross-sectional z-score), Skew4 (share of the dominant liquidated side over 4h), dOI4 (4h open
   interest change) and the funding change.
2. Exhaustion needs all of: a real spike (SPIKE high), a fading last hour (DECAY low), a large forced
   deleveraging relative to open interest (FDz high), one-sided liquidations (Skew4 high) and open
   interest that actually fell (dOI4 clearly negative). A spike without decay is a cascade still running:
   do not lean against it.
3. Direction: when longs were liquidated (long-dominant Skew4) exhaustion leans LONG; when shorts were
   liquidated it leans SHORT. Funding moving back toward neutral supports the same direction.
4. Call `get_snapshot` with fields `liq_long_1h`, `liq_short_1h`, `liq_total_4h`, `funding_rate`,
   `open_interest_usd` and check that the latest recorded values still match the packet. If the last hour
   shows a new burst on the same side, exhaustion is not confirmed.
5. Check the BTC context in the packet: if BTC has not flushed yet while the broad liquidation share is
   high, the cascade can spread; stay near `p_model` or abstain.

Output: a `p_up` close to `p_model` unless steps 2-5 give citable reasons. Cite SPIKE, DECAY, FDz and
dOI4 as packet claims; cite snapshot values as tool claims with their `result_id`.
