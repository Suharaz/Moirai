# Go-live checklist (phase 11)

The path from shadow to live trading. The code enforces the gate: the console enables `live` only when the
newest final G4 evaluation (`golive_g4_finals`) passed for every target type that has a split, today's
`models` / `council` config, agent versions and static scanner / scoring / council files are the ones
pinned over that evaluation's windows, the live key is valid, every console go-live item is checked and
the owner has a fresh TOTP step-up (`hdt.configapi.routes.mode`). The only writer of that evaluation is the
one-shot final command (section 5). Its `G4` row in `gate_flags` is display only: the database refuses a
`G4` flag that does not repeat the newest final, and the live gate never reads the flag's outcome. This
document covers the operational steps around that gate.
Commands use the runbook conventions (`dc`, `/opt/hdt`; `docs/runbook.md` section 1).

## 1. Shadow run
- Run every service in shadow: the `paper` account measures performance on mainnet data, the testnet runs
  the synthetic lifecycle driver. Only the `paper` account counts for G4. Run mode `testnet` sends the
  council decisions to the demo account instead of paper, so the shadow window must run in `paper` mode.
- Keep the model slugs pinned for the whole shadow window (console `models` section). A slug change starts
  a new window: record it in `docs/project-changelog.md`. The G4 `pinned` check fails when the model slug
  or agent version of any agent, or the `models` or `council` config version, differs between the cards of
  the evaluation window.
- The shadow lasts at least 8 weeks, and until the final evaluation set holds at least 150 independent
  paper entry orders per target type.
- Weekly report: every Monday 01:00 UTC from cron on the VPS:

  ```cron
  0 1 * * 1 cd /opt/hdt && docker compose --env-file .env.prod --profile scoring run --rm -v /opt/hdt/reports:/app/reports scorer python scripts/weekly_report.py
  ```

  It writes `reports/golive/weekly/<ISO week>.md` and `.json`, upserts the interim G4 rows of
  `gate_progress` (`overall.met` stays empty: the interim never decides) and queues a `weekly_report`
  alert (info). The telegram-bot delivers the alert and closes it 24 h later. The weekly job never writes
  `gate_flags` and never computes ablations on evaluation events. `--dry-run` writes the report files
  only (no `gate_progress`, no alert).

## 2. Event study on shadow data (weeks 2-3)
- `dc --profile scoring run --rm -v /opt/hdt/reports:/app/reports scorer python scripts/event_study.py
  --start <first shadow day>`. It covers the LTX, MIGRATION and HOLLOW_HYPE signals, split by target type:
  - the preregistered horizon is 12 h; 4, 8 and 24 h are reference only;
  - costs are taker 0.05 % and slippage 2 bp per side, plus the BTC hedge leg for `RESID_12H`, and the
    realized Binance funding over the holding window;
  - CIs are UTC-day block bootstrap 95 %.
- Decision on the 12 h horizon only, per group: `n_min` is the number of signals needed to detect twice
  the median round-trip cost with power 0.8 (two-sided 0.05), from the standard deviation of the group's
  12 h nets. With at least `n_min` 12 h values, the decision is `edge` when the CI lies above 0 and
  `replan` otherwise; below `n_min` it is `underpowered`. Only events the council finished (status `done`)
  count; skipped and failed events do not.
- Exit code (LTX groups):
  - 1: an LTX group is `replan`. Stop and replan the core strategy (phase 03); do not continue towards live.
  - 2: no LTX signal has a 12 h value yet.
  - 3: an LTX group is `underpowered` (or too small to estimate `n_min`): keep collecting signals.
  - 0: every LTX group shows an edge.

## 3. Calibration / evaluation split (once per target type)
- Choose the split instant after enough calibration data (week 4 or later) and before any G4 reading. The
  instant must be in the future: the command refuses a past `eval_start`, and the database stamps
  `created_at` itself and checks `eval_start >= created_at`, so a split can never be placed after results
  were seen:

  ```sh
  dc --profile scoring run --rm scorer python -m hdt.golive.split set --target-type RESID_12H \
    --eval-start 2026-12-01T00:00:00+00:00 --by <owner> --reason "<why this instant>"
  ```

- Each target type has numbered split generations; the scorer role cannot update them or pick the number.
  The current split (highest generation) is never moved: the database refuses a new generation while the
  current one has no final evaluation. G4 reads only events at or after the current split; calibration
  reads only events before it.
- Re-evaluation: after a final is recorded (pass or fail), a new split of every required target type starts
  a new evaluation window (generation + 1), and a later final on those windows replaces the old outcome.
  This is the only way to re-open the gate after a configuration change (section 7).
- `python -m hdt.golive.split show` lists the current splits, their generation and whether their final is
  recorded. Exit code 1: the current split has no final yet; 2: the instant was refused.

## 4. Weekly ablation and threshold calibration (week 4 and later)
- `dc --profile scoring run --rm -v /opt/hdt/reports:/app/reports scorer python scripts/ablation_replay.py`
  writes `reports/golive/ablation/<UTC timestamp>-calibration.{json,md,diff}`, on the calibration set
  only (events before their split). Ablations on evaluation events exist only in the final G4 evidence:
  - replayed ablations: round 1 pool vs debate vs `r = 1`, `a = 0`, drop each agent, and 2/3 consensus vs
    simple majority;
  - scanner threshold cuts;
  - proposals for the manager `d_threshold`, Hedge `eta` and LTX `spike_min`, with the config diff.
- Debate-dependent ablations are forward shadow branches; the report lists them and never replays them.
- Every event is replayed with the council rules of its own pinned `council` config version (the YAML
  default when none was active), never with today's.
- Proposals are never applied by code. Apply an accepted change through the console, then mirror it in
  the YAML default. A scanner change also needs a new scanner `rule_version`. Record every keep or
  disable decision in `docs/project-changelog.md`, with the figures from the report.
- Before applying a new `eta`, run the synthetic oracle test (`python -m hdt.scoring.oracle`).

## 5. Gate G4 (console gates page, `gate_progress` keys `<TARGET_TYPE>.<check>` and `account.<check>`)
Checked per required target type on its final evaluation window, paper account, after fees and funding:

| Check | Target |
|:--|:--|
| `n_entries` | at least 150 independent paper entry orders |
| `spiegelhalter_z` | \|Z\| < 1.96 |
| `calibration_slope` | slope 95 % CI contains 1 (ECE reported only) |
| `sharpe` | annualized daily Sharpe > 1.5, no-trade days included |
| `psr` | PSR(0) >= 0.95 |
| `max_drawdown` | < 10 % of the paper equity curve |
| `pinned` | model slugs, agent versions and `models` / `council` config versions unchanged over the window |
| `pins_agree` | every required target type ran with the same pinned configuration (one final can only match one active config) |
| `ablation` | every retained component wins the paired log-loss bootstrap (Holm over the retained components, alpha 0.05), with no replay mismatch |

- The per-target PnL counts every position opened by a decision card of the window, scored or not,
  labeled or not, plus its share of the BTC hedge book (split by the notional x time of the alt positions
  it hedged).
- Account checks (`account.sharpe`, `account.psr`, `account.max_drawdown`, same targets): the whole paper
  account's end-of-day equity from `equity_snapshots` (unrealized PnL included, every position whatever
  its event) over the union of the windows.
- Required target types: every target type with a decision card since the earliest split, or with a
  split. A command-line list can never narrow them. `<TARGET_TYPE>.split` fails while one has no split.
- Window: from the split to the UTC midnight after the day its 150th independent paper entry was
  decided (the window is then sealed and never grows).
- The weekly report shows the interim figures. The gate is decided once by the final evaluation, after
  every required window is sealed, every scored card of each window has a label and the label retry
  period (`resolver.missing_retry_h` after the 12 h horizon) is over:

  ```sh
  dc --profile scoring run --rm scorer python -m hdt.golive.final status
  dc --profile scoring run --rm -v /opt/hdt/reports:/app/reports scorer python -m hdt.golive.final run
  ```

  `run` refuses (exit 2, nothing written) while anything blocks it. Otherwise it writes the evidence
  `reports/golive/g4/final-<UTC timestamp>.json` (never overwritten) and, in one transaction, one
  `golive_g4_finals` row per required target type on its current split generation, the `gate_flags` row
  (`decided_by = scorer:g4`) and the `gate_progress` rows. Each final row records its window's pins plus
  the digests of `config/scanner.yaml`, `config/scoring.yaml` and `config/council.yaml` (`static` pins).
  A second final evaluation of the same split generation fails and writes nothing. `--dry-run` prints the
  result and writes nothing.
- After a final, the weekly report keeps the recorded rows of the decided target types, the account
  checks, `pins_agree` and `overall` as the final wrote them; it updates only target types without a final.
- `overall` passes only when every required target type and the account checks pass. Do not
  re-calibrate on evaluation data after reading it.

## 6. Console go-live items (all must be checked)
| Item id | What to verify |
|:--|:--|
| `live_key_trade_only` | Binance live key: trading only; withdrawals and transfers disabled |
| `live_key_ip_whitelist` | Live key restricted to the VPS egress IP |
| `live_sub_account` | Live trading on a sub-account with capped capital |
| `initial_equity_limit` | Owner decided the initial live equity (sub-account balance) |
| `one_way_mode` | Live account in One-way position mode |
| `kill_switch_live_test` | Kill switch engaged on live with a minimum-size order; flat and stop report received |
| `independent_kill_path` | `hdt-kill` over SSH tested, and the Binance app runbook (runbook section 6) |
| `backup_restore_drill` | Point-in-time restore drill completed (runbook section 14) |
| `console_not_public` | Console and config-api unreachable from the internet (`ss -tlnp` and an external scan) |

Also before live: re-test the alert path (`dc exec telegram-bot python -m hdt.ops.alerts test`) and
confirm the `backup_stale` alert is clear.

## 7. Live at 1/4 size, then 1/2, then 1
- Switch the mode to `live` from the console with `size_multiplier = 0.25`, after the TOTP step-up.
- Stay at each size for at least 4 weeks. The weekly report compares paper and live since the activation
  of the current `mode` version:
  - fill rate, at least 20 events on both accounts, within 10 percentage points;
  - entry price vs the paper fill, at least 20 pairs, mean adverse slippage at most 1x `fees.slippage_bp`
    (the paper fill already includes `fees.slippage_bp`, so live stays within 2x of the book);
  - stop slippage vs the trigger price, at least 10 stops, mean at most 2x `fees.slippage_bp`.
- Verdict `hold_size`: do not increase size. `insufficient_data`: keep measuring. `ok_to_step`: the owner
  may step to 1/2, then to 1. Each step re-measures from its own activation.
- Record every size step, with its figures, in `docs/project-changelog.md`.

## 8. Configuration while live
- The live gate binds the final evaluation to the configuration it measured, compared by content: a
  `models` / `council` save, a new agent version (model, provider routing, prompt or skill) or a changed
  static file closes the gate with `g4_config_changed`. A rollback to the pinned content re-opens it.
- While `live` is active, the console refuses every `models` and `council` save and rollback (409
  `live_config_locked`). A `live` save that keeps or lowers `size_multiplier` skips the G4 checks, so the
  size can always come down; any increase, and every switch into `live`, needs the gate open.
- The static YAML files are baked into the image: changing them needs a redeploy, which is not refused at
  runtime; the next `live` save that raises the size or re-enters `live` sees the new digests and refuses.
- To change a pinned section: switch the mode to `paper` (refused while the live namespace has exposure:
  close or flatten first), make the change, set a new split per target type (section 3), shadow until the
  new windows seal, run the final evaluation again (section 5), then switch to `live` at 1/4 size.
