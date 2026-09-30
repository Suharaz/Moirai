"""Shadow event study (phase 11): returns 4 / 8 / 12 / 24 h after the LTX, Migration and Hollow Hype signals
the running system emitted, net of taker fee, slippage and realized funding, split by target type.

`hdt.golive.event_study` does the work (12 h preregistered and deciding, the others reference only; UTC-day
block bootstrap 95% CI, fixed seed). Writes `reports/golive/event_study/<end date>.json` and `.md` and
`latest.json`. Runs with the scorer DSN (`HDT_PG_DSN`, role `hdt_scorer`) and the read-only lake.

Exit code (LTX groups, on the preregistered 12 h horizon only; 4 / 8 / 24 h never decide):
- 1: replan (phase 11 step 2, stop and replan the core strategy): an LTX group has at least `n_min` 12 h
  values and a 12 h CI that does not lie above 0;
- 2: no LTX signal has a 12 h value yet;
- 3: underpowered: an LTX group has fewer 12 h values than `n_min` (or too few to estimate it) and none
  signals a replan;
- 0: every LTX group shows an edge with enough power.

Usage: `uv run python scripts/event_study.py --start 2026-10-01 [--end 2026-11-01T00:00:00+00:00]`
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from hdt.core.clock import ensure_utc, utcnow
from hdt.core.config import scanner_config, static_config
from hdt.db.session import make_engine
from hdt.golive.event_study import Costs, render_markdown, study_json
from hdt.lake.pit_query import PitQuery

ROOT = Path(__file__).resolve().parents[1]


def _instant(text: str) -> datetime:
    value = datetime.fromisoformat(text)
    return ensure_utc(value if value.tzinfo else value.replace(tzinfo=UTC))


EXIT_REPLAN = 1
EXIT_NO_DATA = 2
EXIT_UNDERPOWERED = 3


def exit_code(study: dict[str, Any]) -> int:
    ltx = [g for g in study["groups"] if g["source"] == "LTX"]
    if any(g["replan_signal"] for g in ltx):
        return EXIT_REPLAN
    if not any(g["horizons"].get("12") for g in ltx):
        return EXIT_NO_DATA
    if any(g["decision_12h"] != "edge" for g in ltx):
        return EXIT_UNDERPOWERED
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument("--start", required=True, type=_instant)
    parser.add_argument("--end", type=_instant, default=None)
    parser.add_argument("--reports", type=Path, default=ROOT / "reports" / "golive" / "event_study")
    args = parser.parse_args(argv)
    now = utcnow()
    end = args.end or now.replace(minute=0, second=0, microsecond=0)
    if end <= args.start:
        parser.error("--end must be after --start")
    static = static_config()
    scanner = scanner_config()
    data = static.settings.data
    pit = PitQuery(Path(data.staging_root), Path(data.lake_root))
    engine = make_engine()
    try:
        with engine.connect() as conn:
            study = study_json(
                conn,
                pit,
                start=args.start,
                end=end,
                costs=Costs(static.risk.fees.taker, static.risk.fees.slippage_bp),
                label_params=scanner.labels,
                btc_cmc_symbol=static.indicators.reserves.btc_symbol,
                fetched_by=now,
            )
    finally:
        engine.dispose()
    args.reports.mkdir(parents=True, exist_ok=True)
    stem = end.date().isoformat()
    text = json.dumps(study, indent=2, sort_keys=True) + "\n"
    (args.reports / f"{stem}.json").write_text(text, encoding="utf-8")
    (args.reports / "latest.json").write_text(text, encoding="utf-8")
    (args.reports / f"{stem}.md").write_text(render_markdown(study), encoding="utf-8")
    print(
        json.dumps(
            [
                {
                    k: g[k]
                    for k in ("source", "target_type", "n_signals", "state_12h", "decision_12h", "n_min")
                }
                for g in study["groups"]
            ]
        )
    )
    return exit_code(study)


if __name__ == "__main__":
    sys.exit(main())
