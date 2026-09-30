"""Weekly go-live report (phase 11 step 1): gates, weights, calibration, costs, event study, ablations and
paper vs live, as Markdown on disk plus a Telegram digest through the ops alert outbox.

One run:
1. gate G4 interim on the final evaluation set so far (`hdt.golive.g4`): the check rows go to
   `gate_progress` with an undecided `overall`, except the rows of a target type whose current split already
   has a final evaluation (and the account, `pins_agree` and `overall` rows once a final exists): those stay
   as the final recorded them. The weekly job never writes `gate_flags` and never computes ablations on
   evaluation events: the gate is decided once per split by `python -m hdt.golive.final run`;
2. ablation replay on the calibration set and the calibration proposals (never applied);
3. the shadow event study since the first council event (`--skip-event-study` to omit; needs the lake);
4. paper vs live over the current size step;
5. `reports/golive/weekly/<ISO week>.md` and `.json`, and the `weekly_report` alert (info) that the
   telegram-bot delivers.
Runs with the scorer DSN (`HDT_PG_DSN`, role `hdt_scorer`). `--dry-run` writes the files only (no
gate_progress, no alert).

Schedule (the scorer service has no weekly hook): every Monday 01:00 UTC from cron on the VPS; the exact
crontab line is in `docs/go-live-checklist.md` section 1.

Usage: `uv run python scripts/weekly_report.py [--dry-run] [--no-telegram] [--skip-event-study]`
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import sqlalchemy as sa

from hdt.core.clock import ensure_utc, utcnow
from hdt.core.config import scanner_config, scoring_config, static_config
from hdt.db.models.decision import CouncilEventRow
from hdt.db.session import make_engine, make_session_factory, transaction
from hdt.golive.data import load_finals, load_replay_events
from hdt.golive.event_study import Costs, study_json
from hdt.golive.g4 import write_progress
from hdt.golive.weekly import (
    GoLiveContext,
    calibration_summary,
    compute_g4,
    costs_summary,
    day_floor,
    gate_rows,
    iso_week,
    live_comparison,
    render_markdown,
    replay_report,
    send_report,
    week_window,
    weights_summary,
)
from hdt.lake.pit_query import PitQuery

ROOT = Path(__file__).resolve().parents[1]


def _instant(text: str) -> datetime:
    value = datetime.fromisoformat(text)
    return ensure_utc(value if value.tzinfo else value.replace(tzinfo=UTC))


def _relative(path: Path) -> str:
    return (path.relative_to(ROOT) if path.is_relative_to(ROOT) else path).as_posix()


def _jsonable(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in r.items()} for r in rows]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument("--reports", type=Path, default=ROOT / "reports" / "golive")
    parser.add_argument("--event-study-start", type=_instant, default=None)
    parser.add_argument("--skip-event-study", action="store_true")
    parser.add_argument("--no-telegram", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    now = utcnow()
    week = iso_week(now)
    static, scoring, scanner = static_config(), scoring_config(), scanner_config()
    engine = make_engine()
    sessions = make_session_factory(engine)
    try:
        with sessions() as session:
            conn = session.connection()
            ctx = GoLiveContext.load(session, static, scoring, scanner)
            events = load_replay_events(conn, static=static, end=day_floor(now))
            g4, _ = compute_g4(conn, ctx, now=now, events=events)
            recorded = load_finals(conn)
            finals = [f.to_json() for _, f in sorted(recorded.items())]
            replay = replay_report(conn, ctx, now=now, events=events)
            since, until = week_window(now)
            costs = costs_summary(conn, since=since, until=until)
            live = live_comparison(conn, ctx)
            study = None
            if not args.skip_event_study:
                first = (
                    args.event_study_start
                    or conn.execute(sa.select(sa.func.min(CouncilEventRow.as_of))).scalar()
                )
                if first is not None:
                    data = static.settings.data
                    study = study_json(
                        conn,
                        PitQuery(Path(data.staging_root), Path(data.lake_root)),
                        start=ensure_utc(first),
                        end=now.replace(minute=0, second=0, microsecond=0),
                        costs=Costs(ctx.taker, ctx.slippage_bp),
                        label_params=scanner.labels,
                        btc_cmc_symbol=static.indicators.reserves.btc_symbol,
                        fetched_by=now,
                    )
            session.rollback()

        weekly_dir = args.reports / "weekly"
        weekly_dir.mkdir(parents=True, exist_ok=True)
        md_path = weekly_dir / f"{week}.md"
        report: dict[str, Any] = {
            "week": week,
            "generated_at": now.isoformat(),
            "report_path": _relative(md_path),
            "g4": g4.to_json(),
            "g4_finals": finals,
            "replay": replay.to_json(),
            "event_study": {k: v for k, v in study.items() if k != "rows"} if study else None,
            "live": live.to_json() if live else None,
            "costs": costs,
            "notes": [*replay.notes],
        }
        alert_id = None
        with transaction(sessions) as session:
            conn = session.connection()
            if not args.dry_run:
                # never over the rows a final evaluation of the current split wrote (review M-2)
                write_progress(conn, g4, decided=recorded.keys())
            report["gates"] = _jsonable(gate_rows(conn))
            report["weights"] = _jsonable(weights_summary(conn))
            report["calibration"] = _jsonable(calibration_summary(conn))
            if not args.dry_run and not args.no_telegram:
                alert_id = send_report(session, report)
    finally:
        engine.dispose()
    md_path.write_text(render_markdown(report), encoding="utf-8")
    (weekly_dir / f"{week}.json").write_text(
        json.dumps(report, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8"
    )
    print(
        json.dumps(
            {
                "week": week,
                "g4_final": finals,
                "alert_id": alert_id,
                "report": report["report_path"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
