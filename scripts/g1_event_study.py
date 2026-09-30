"""Gate G1 event study (phase 03): lake -> frequency + 12 h event study -> reports/g1/.

Runs the preregistered study (`docs/g1-preregistration.md`, `hdt.quant.study.EventStudy`) over every scanner
slot of `[--start, --end)` on the recorded lake and writes:
- `reports/g1/<end date>.json`: every number of the study (inputs, per-rule N, N_min, sigma, delta, CIs at
  12 h and the 4 / 8 / 24 h references, drops, baseline, frequency, every event);
- `reports/g1/<end date>.md`: the same, readable;
- `reports/g1/latest.json`: the gate status read by `hdt.quant.gates.g1_status`, with the study it was
  computed on (window, seed, rule / label / feature versions, config pins) that `scripts/fit_p_model.py`
  binds to.
Every run writes all three files; there is no dry-run mode.
The same lake, config files, window and seed give identical files. `--end` defaults to the start of the
current UTC hour; labels ending after `--end` are missing (`immature`).

Exit code: 0 passed, 1 failed (replan), 2 insufficient power or data (keep recording, rerun weekly).

Usage: `uv run python scripts/g1_event_study.py --start 2026-10-01 [--end 2026-11-01T00:00:00+00:00]`
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

from hdt.core.clock import ensure_utc, utcnow
from hdt.core.config import scanner_config, static_config
from hdt.lake.pit_query import PitQuery
from hdt.quant.gates import DECISION_HORIZON_H, G1Status, status_from_rules, write_status
from hdt.quant.study import EventStudy, StudyResult

ROOT = Path(__file__).resolve().parents[1]
EXIT = {"passed": 0, "failed": 1, "insufficient_power": 2, "insufficient_data": 2}


def _instant(text: str) -> datetime:
    value = datetime.fromisoformat(text)
    return ensure_utc(value if value.tzinfo else value.replace(tzinfo=UTC))


def _num(value: float | int | None, digits: int = 6) -> str:
    if value is None:
        return "-"
    return str(value) if isinstance(value, int) else f"{value:.{digits}f}"


def render_markdown(result: StudyResult, status: G1Status) -> str:
    inp = result.inputs
    lines = [
        f"# G1 event study {inp.end.date().isoformat()}",
        "",
        f"- Window: {inp.start.isoformat()} to {inp.end.isoformat()}",
        f"- Every scanner slot: {result.slots} evaluated, {result.slots_without_universe} without a universe",
        f"- Versions: rule `{inp.rule_version}`, labels `{inp.label_spec_version}`, "
        f"features `{inp.feature_ver}`",
        f"- Costs: taker {inp.taker}, slippage {inp.slippage_bp} bp per side; "
        f"max mark gap {inp.max_mark_gap_s} s",
        "",
        f"## Gate state: {status.state}",
        "",
        "Every preregistered rule must pass; the least favourable rule decides "
        "(failed, then insufficient data, then insufficient power).",
        "",
        "| Rule | State | N | N_min | sigma | delta | 12h mean | 95% CI |",
        "|:--|:--|--:|--:|--:|--:|--:|:--|",
    ]
    for name, rule in sorted(result.rules.items()):
        ci = rule.horizons.get(str(DECISION_HORIZON_H))
        lines.append(
            f"| {name} | {rule.state} | {rule.n_events} | {_num(rule.n_min)} | {_num(rule.sigma)} | "
            f"{_num(rule.delta)} | {_num(ci.mean if ci else None)} | "
            f"{f'[{ci.ci_low:.6f}, {ci.ci_high:.6f}]' if ci else '-'} |"
        )
    lines += ["", "## Reference horizons (never decide the gate)", ""]
    lines += ["| Rule | Horizon | N | mean | 95% CI |", "|:--|--:|--:|--:|:--|"]
    for name, rule in sorted(result.rules.items()):
        for h, ci in sorted(rule.horizons.items(), key=lambda kv: int(kv[0])):
            if int(h) == DECISION_HORIZON_H:
                continue
            lines.append(
                f"| {name} | {h}h | {ci.n if ci else 0} | {_num(ci.mean if ci else None)} | "
                f"{f'[{ci.ci_low:.6f}, {ci.ci_high:.6f}]' if ci else '-'} |"
            )
    lines += ["", "## Dropped events (12h)", ""]
    for name, rule in sorted(result.rules.items()):
        drops = ", ".join(f"{k}: {v}" for k, v in rule.dropped.items()) or "none"
        lines.append(f"- {name}: {drops}")
    lines += ["", "## Non-event baseline (sigma source)", ""]
    for name, base in sorted(result.baseline.items()):
        lines.append(f"- {name}: " + ", ".join(f"{k}={_num(v)}" for k, v in base.items()))
    freq = result.frequency
    lines += [
        "",
        "## Trigger frequency (independent events)",
        "",
        f"- Days: {freq['days']:.2f}; strict events per day, all rules: "
        f"{freq['strict_events_per_day_all_rules']:.3f}; busiest day: {freq['strict_max_one_day_all_rules']}",
        "",
        "| Thresholds | Rule | Events | Per day | Busiest day | Coins |",
        "|:--|:--|--:|--:|--:|--:|",
    ]
    for level in ("strict", "loose"):
        for name, row in sorted(freq[level].items()):
            lines.append(
                f"| {level} | {name} | {row['events']} | {row['events_per_day']:.3f} | "
                f"{row['max_events_one_day']} | {row['coins']} |"
            )
    lines += ["", "## Decision", "", _decision_text(status.state), ""]
    return "\n".join(lines)


def _decision_text(state: str) -> str:
    return {
        "passed": "Every rule has N >= N_min and a 95% CI above 0: the gate passes; fit the first p_model "
        "files (`scripts/fit_p_model.py`) and review them.",
        "failed": "N >= N_min and the CI contains 0 (or is below 0) for a rule: stop and replan the core "
        "strategy.",
        "insufficient_power": "N < N_min: keep recording, phases 05-08 stay blocked, rerun weekly.",
        "insufficient_data": "Not enough recorded data to estimate the test: keep recording, phases 05-08 "
        "stay blocked, rerun weekly.",
    }[state]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument("--start", required=True, type=_instant)
    parser.add_argument("--end", type=_instant, default=None)
    parser.add_argument("--reports", type=Path, default=ROOT / "reports" / "g1")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args(argv)
    end = args.end or utcnow().replace(minute=0, second=0, microsecond=0)
    if end <= args.start:
        parser.error("--end must be after --start")
    static = static_config()
    data = static.settings.data
    pit = PitQuery(Path(data.staging_root), Path(data.lake_root))

    def progress(at: datetime) -> None:
        if not args.quiet and at.minute == 0 and at.hour % 6 == 0 and at.second < 60:
            print(f"scanning {at.isoformat()}", file=sys.stderr, flush=True)

    result = EventStudy(pit, static, scanner_config(), progress=progress).run(args.start, end)
    stem = end.date().isoformat()
    args.reports.mkdir(parents=True, exist_ok=True)
    json_path = args.reports / f"{stem}.json"
    status = status_from_rules(
        result.rules,
        end,
        (json_path.relative_to(ROOT) if json_path.is_relative_to(ROOT) else json_path).as_posix(),
        study=result.inputs.study(),
    )
    payload = {"status": status.to_json(), **result.to_json()}
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (args.reports / f"{stem}.md").write_text(render_markdown(result, status), encoding="utf-8")
    write_status(args.reports, status)
    print(json.dumps(status.to_json(), sort_keys=True))
    return EXIT[status.state]


if __name__ == "__main__":
    sys.exit(main())
