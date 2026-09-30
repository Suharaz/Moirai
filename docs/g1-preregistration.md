# G1 preregistration (phase 03)

Committed before any event result is read. Changing anything below after results are seen requires a new
preregistration file with a new date; the old one is kept.

## Question
Do the LTX and Leverage Migration triggers, as defined in `config/scanner.yaml` rule_version `s1`, carry a
positive 12h edge after costs on the self-recorded lake (phase 02 onward)?

## Decision horizon
- 12h only. 4h, 8h and 24h are reported as reference and are never used to pass or fail the gate.

## Unit of analysis
- One event per `(coin, non-overlapping 12h window)`: the first trigger in the window; later triggers of the
  same coin inside the window are ignored.
- BTC is never an event (not in the tradable set). Coins come from the PIT universe of the event date.
- Strict thresholds decide the gate; loose thresholds are counted for frequency only.

## Metric
- LTX (hedged setup): 12h residual return after beta hedge, `r_coin - beta_btc(t) x r_btc`, sign multiplied
  by the signal direction (+1 LONG, -1 SHORT); `beta_btc` is the packet value at `t` (30 days of 1h returns).
- Migration (unhedged): 12h mark return in the signal direction.
- Marks: Binance 1 s mark events, fallback 1m mark price klines; no mark within `max_mark_gap_s` of a label
  time drops the event (never interpolated). Label spec `lbl1`.
- Minus round-trip cost: taker fee twice plus estimated slippage twice, from the pinned risk config
  (`fees.taker`, `slippage_bp`); LTX adds the hedge leg cost.

## Test
- Mean event metric with a daily block bootstrap (blocks = UTC days, 10 000 resamples, fixed seed), 95% CI.
- Minimum effect `delta` = 2 x round-trip cost. Power 0.8, two-sided alpha 0.05.
- `sigma` is estimated from non-event data only: the same metric computed on every scanner slot without a
  trigger over the same period (no event result is looked at). `N_min = ((1.96 + 0.84) x sigma / delta)^2`.

## Decision
- `N < N_min`: keep recording, phases 05-08 stay blocked, rerun weekly.
- `N >= N_min` and the CI contains 0: stop and replan the core strategy.
- `N >= N_min` and the CI excludes 0 in the signal direction: gate passes; fit the first `p_model` files.

## Frequency report
- Events per day and per coin for strict and loose thresholds, used to set `max_candidates_per_day` and to
  estimate shadow duration.

## Output
- `reports/g1/<date>.md` plus a JSON with the same numbers, deterministically recomputable from the lake
  (same lake, same config, same seed produce identical files).

## Amendment 1 (2026-09-27 UTC, before any event result)
Written while implementing `scripts/g1_event_study.py`, before the first (no-event) smoke run of the study. It
was committed as `7fdf30d` (2026-09-28 06:09:59 +0700, 2026-09-27 23:09:59 UTC) after that smoke run had
written its report and after the real study on the dev lake had been launched. Both runs found zero mature
events (every label `immature`, N = 0 for both rules), so no event result existed when it was committed. It
makes the rules above exact; it does not change a threshold. (This paragraph was corrected on 2026-09-28: it
first said the amendment predated any study run. The rules below are unchanged.)
- Independence across rules: the unit is one event per (coin, non-overlapping 12h window) over both triggers
  together. The first strict trigger of either rule opens the window; every later trigger of the coin inside
  it is ignored. Two triggers of one coin at the same slot: LTX is kept, Migration is ignored.
- Strict trigger: the scanner's strict evaluation for the slot (LTX: strict thresholds and the contagion block
  not active; Migration: strict thresholds), with a signal side.
- `delta` per rule, fixed from non-event data only:
  - Migration: `delta = 2 x (2 x taker + 2 x slippage)`.
  - LTX: the hedge leg costs `|beta_btc|` times the coin leg, so `delta = 2 x (2 x taker + 2 x slippage) x
    (1 + median |beta_btc|)`, the median taken over every non-event LTX baseline sample (every slot and coin
    of the sigma baseline with a `beta_btc`). Each event's own cost uses its own `beta_btc` at `t`.
- `sigma` per rule: the sample standard deviation of the gross 12h metric (LTX residual, Migration mark
  return) over every scanner slot and every coin of the rule's cross-section without a strict trigger of that
  rule at that slot. No subsampling.
- Missing labels: an event or baseline sample whose horizon ends after the study end is `immature` and is not
  counted; a missing mark or `beta_btc` drops the sample (counted by reason in the report).
- Gate state over both rules: the preregistered question covers both triggers, so the gate passes only when
  both rules pass. Otherwise the least favourable state decides, in this order: `failed` (replan),
  `insufficient_data`, `insufficient_power`. Each rule's state is reported.
