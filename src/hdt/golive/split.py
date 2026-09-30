"""The calibration / final evaluation split per target type (`golive_eval_splits`, one row per generation).

Phase 11 risk handling: calibrate only on the calibration set and evaluate G4 once on the later, untouched
evaluation set. Setting a split is a one-way owner decision, so it is a separate command (never done by the
weekly job) and the table grants the scorer SELECT and a column-level INSERT only:

    python -m hdt.golive.split show
    python -m hdt.golive.split set --target-type RESID_12H --eval-start 2026-12-01T00:00:00+00:00 \\
        --by <owner> --reason "<why this instant>"

The evaluation set must start in the future: a split can never be placed after results were seen. The code
refuses `eval_start < now`; the database stamps `created_at` itself (`clock_timestamp()`) and checks
`eval_start >= created_at`.

A split is never moved: while the current split of a target type has no final evaluation, `set` refuses a
new one (exit 1), and so does the database trigger. Once the final evaluation of the current split is
recorded (passed or failed), `set` starts the next generation: the re-evaluation path after a `models` or
`council` change, which closes the live gate (`hdt.configapi.routes.mode`) until a new final evaluation of
the new splits passes. `--by` and `--reason` are stored on the row, which is never updated or deleted.

Runs with the scorer DSN in `HDT_PG_DSN`. Exit code 1 when the current split of that target type has no
final evaluation yet, 2 when the instant is refused.
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime

import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError

from hdt.core.clock import ensure_utc, utcnow
from hdt.db.models.golive import TARGET_TYPES, GoLiveEvalSplitRow
from hdt.db.session import make_engine
from hdt.golive.data import load_current_splits, load_finals


def set_split(
    conn: sa.Connection, target_type: str, eval_start: datetime, *, by: str, reason: str, now: datetime
) -> int | None:
    """Insert the next split of `target_type`; returns its generation, or None while the current split has
    no final evaluation (a split is never moved)."""
    if target_type not in TARGET_TYPES:
        raise ValueError(f"unknown target_type {target_type!r}")
    if not by.strip() or not reason.strip():
        raise ValueError("decided_by and reason are required")
    start = ensure_utc(eval_start)
    if start < ensure_utc(now):
        raise ValueError(
            f"eval_start {start.isoformat()} is in the past: the evaluation set must start later"
        )
    current = load_current_splits(conn).get(target_type)
    if current is not None and target_type not in load_finals(conn):
        return None
    stmt = (
        sa.insert(GoLiveEvalSplitRow)
        .values(target_type=target_type, eval_start=start, decided_by=by.strip(), reason=reason.strip())
        .returning(GoLiveEvalSplitRow.generation)
    )
    return int(conn.execute(stmt).scalar_one())


def _instant(text: str) -> datetime:
    value = datetime.fromisoformat(text)
    return ensure_utc(value if value.tzinfo else value.replace(tzinfo=UTC))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m hdt.golive.split")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("show")
    s = sub.add_parser("set")
    s.add_argument("--target-type", required=True, choices=TARGET_TYPES)
    s.add_argument("--eval-start", required=True, type=_instant)
    s.add_argument("--by", required=True)
    s.add_argument("--reason", required=True)
    args = parser.parse_args(argv)
    engine = make_engine()
    try:
        if args.command == "show":
            with engine.connect() as conn:
                splits = load_current_splits(conn)
                finals = load_finals(conn)
            for target in TARGET_TYPES:
                split = splits.get(target)
                if split is None:
                    print(f"{target}: not set")
                    continue
                state = "final recorded" if target in finals else "no final yet"
                start = split.eval_start.isoformat()
                print(f"{target}: split {split.generation} starts at {start} ({state})")
            return 0
        try:
            with engine.begin() as conn:
                generation = set_split(
                    conn, args.target_type, args.eval_start, by=args.by, reason=args.reason, now=utcnow()
                )
        except (ValueError, IntegrityError) as exc:
            print(f"{args.target_type}: {exc}", file=sys.stderr)
            return 2
        if generation is None:
            print(
                f"{args.target_type}: the current split has no final evaluation yet; it is never moved",
                file=sys.stderr,
            )
            return 1
        start = args.eval_start.isoformat()
        print(f"{args.target_type}: split {generation} starts the evaluation set at {start}")
        return 0
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
