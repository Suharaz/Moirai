"""Ablation replay and threshold calibration (phase 11): numeric variants from decision cards and stored
forecasts, paired bootstrap on log-loss with Holm, and a proposed config diff (never applied).

`hdt.golive.ablation` replays round-1 vs final pool, r = 1, a = 0, drop-one-agent in the round-1 pool,
consensus 2/3 vs simple majority and the scanner threshold cuts, each event with the council rules of its
own pinned config version; debate-dependent ablations are listed as forward shadow branches only.
`hdt.golive.calibrate` proposes the manager D threshold, Hedge eta and the LTX SPIKE threshold.

Calibration set only (events before their target type's split): ablations on evaluation events are computed
once, by the final G4 evaluation (`python -m hdt.golive.final run`), and live in its evidence file, so that
nothing is tuned on the evaluation set. Writes `reports/golive/ablation/<UTC timestamp>-calibration.json`,
`.md` and `.diff` (the proposal). Runs with the scorer DSN (`HDT_PG_DSN`, role `hdt_scorer`).

Usage: `uv run python scripts/ablation_replay.py`
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from hdt.core.clock import utcnow
from hdt.core.config import scanner_config, scoring_config, static_config
from hdt.db.session import make_engine, make_session_factory
from hdt.golive.weekly import CALIBRATION_SET, GoLiveContext, replay_markdown, replay_report

ROOT = Path(__file__).resolve().parents[1]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument("--reports", type=Path, default=ROOT / "reports" / "golive" / "ablation")
    args = parser.parse_args(argv)
    now = utcnow()
    engine = make_engine()
    sessions = make_session_factory(engine)
    try:
        with sessions() as session:
            ctx = GoLiveContext.load(session, static_config(), scoring_config(), scanner_config())
            report = replay_report(session.connection(), ctx, now=now)
            session.rollback()
    finally:
        engine.dispose()
    payload = {"generated_at": now.isoformat(), **report.to_json()}
    markdown = [
        f"# Ablation replay ({CALIBRATION_SET} set) {payload['generated_at']}",
        "",
        *replay_markdown(payload),
    ]
    markdown += [f"- {note}" for note in payload["notes"]]
    args.reports.mkdir(parents=True, exist_ok=True)
    stem = f"{now.strftime('%Y%m%dT%H%M%SZ')}-{CALIBRATION_SET}"
    (args.reports / f"{stem}.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (args.reports / f"{stem}.md").write_text("\n".join(markdown).rstrip("\n") + "\n", encoding="utf-8")
    (args.reports / f"{stem}.diff").write_text(payload["config_diff"], encoding="utf-8")
    print(
        json.dumps({t: a["retained_components_win"] for t, a in payload["ablations"].items()}, sort_keys=True)
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
