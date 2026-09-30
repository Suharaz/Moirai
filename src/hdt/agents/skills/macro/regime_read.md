---
name: regime_read
title: Read the market regime
event_types: [ALL]
---
Goal: place the event in its market regime and adjust how much any directional view should be trusted.

1. Trend or sideways: BTC above rising averages with expanding total market cap is an uptrend regime;
   below falling averages a downtrend; neither is sideways.
2. Volatility: high realized volatility widens the 12 hour outcome distribution. In a high volatility
   regime, keep `p_up` closer to 0.5.
3. Sentiment: extreme greed makes crowded longs fragile; extreme fear makes crowded shorts fragile. Use
   the Fear and Greed index as a contrarian context only at the extremes.
4. Rotation: rising BTC dominance and a low altcoin season index mean capital is leaving altcoins; an
   altcoin long fights that flow.
5. Combine: in a clear trend regime, setups in the trend direction deserve more room; against the trend,
   stay near `p_model`.

Cite the regime features you used as packet claims.
