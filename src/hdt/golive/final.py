"""The one-shot final G4 evaluation (phase 11 risk handling: "final evaluation run only once").

    python -m hdt.golive.final status
    python -m hdt.golive.final run [--dry-run] [--reports reports/golive]

`status` prints the evaluation windows, what still blocks the final evaluation and the finals recorded for
the current splits. `run` refuses (exit 2, nothing written) while anything blocks it
(`hdt.golive.weekly.final_blockers`: a split and a sealed window for every required target type, every scored
card of each window labeled, the label retry period over, no final recorded for the current split yet).
Otherwise it computes G4 on the sealed windows, ablations included, writes the evidence
`<reports>/g4/final-<UTC timestamp>.json` (never overwritten) and, in one transaction, inserts one
`golive_g4_finals` row per required target type on its current split generation (with the `models` /
`council` config version ids and agent pins of its window and the static digests of this process,
`hdt.golive.pins`), the G4 `gate_flags` row and the `gate_progress` rows. A second final evaluation of the
same splits fails on the primary key (exit 1) and leaves no evidence file. `--dry-run` computes and prints
the result, writing nothing.

The live gate (`hdt.configapi.routes.mode`) reads the newest final evaluation: a pass opens `live` only while
today's configuration matches its pins. After a change, the re-evaluation path is a new split per required
target type (`python -m hdt.golive.split set`, accepted once the current split has its final) and a new
final evaluation of those splits.

Runs with the scorer DSN (`HDT_PG_DSN`, role `hdt_scorer`: the only role that may insert gate flags).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

from sqlalchemy.exc import IntegrityError

from hdt.core.clock import utcnow
from hdt.core.config import scanner_config, scoring_config, static_config
from hdt.db.session import make_engine, make_session_factory, transaction
from hdt.golive.data import load_finals
from hdt.golive.g4 import write_final
from hdt.golive.pins import static_digests
from hdt.golive.weekly import GoLiveContext, compute_g4, final_blockers


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m hdt.golive.final")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("status")
    run = sub.add_parser("run")
    run.add_argument("--dry-run", action="store_true")
    run.add_argument("--reports", type=Path, default=Path("reports") / "golive")
    args = parser.parse_args(argv)
    now = utcnow()
    static, scoring, scanner = static_config(), scoring_config(), scanner_config()
    engine = make_engine()
    sessions = make_session_factory(engine)
    try:
        with sessions() as session:
            conn = session.connection()
            ctx = GoLiveContext.load(session, static, scoring, scanner)
            blockers = final_blockers(conn, ctx, now=now)
            if args.command == "status":
                interim, _ = compute_g4(conn, ctx, now=now)
                finals = load_finals(conn)
                session.rollback()
                windows = {
                    t: {"eval_start": r.eval_start, "eval_end": r.eval_end, "sealed": r.sealed}
                    for t, r in interim.targets.items()
                }
                print(
                    json.dumps(
                        {
                            "windows": windows,
                            "missing_split": list(interim.missing_split),
                            "blockers": blockers,
                            "finals": {t: f.to_json() for t, f in sorted(finals.items())},
                        },
                        indent=2,
                        sort_keys=True,
                        default=str,
                    )
                )
                return 0
            if blockers:
                session.rollback()
                print("final evaluation refused:\n" + "\n".join(f"- {b}" for b in blockers), file=sys.stderr)
                return 2
            result, _ = compute_g4(conn, ctx, now=now, final=True)
            session.rollback()
        static_pins = static_digests(static, scanner, scoring)
        evidence_json = {**result.to_json(), "static_pins": static_pins}
        text = json.dumps(evidence_json, indent=2, sort_keys=True) + "\n"
        if args.dry_run:
            print(text, end="")
            return 0
        g4_dir = args.reports / "g4"
        g4_dir.mkdir(parents=True, exist_ok=True)
        path = g4_dir / f"final-{now.strftime('%Y%m%dT%H%M%SZ')}.json"
        evidence = f"{path.as_posix()}#sha256={hashlib.sha256(text.encode('utf-8')).hexdigest()}"
        written = False
        try:
            with transaction(sessions) as session:
                write_final(session.connection(), result, evidence, static_pins=static_pins)
                with path.open("x", encoding="utf-8") as fh:
                    fh.write(text)
                written = True
        except IntegrityError:
            if written:
                path.unlink(missing_ok=True)
            print("final evaluation already recorded for these splits: it runs only once", file=sys.stderr)
            return 1
        except BaseException:
            if written:
                path.unlink(missing_ok=True)
            raise
    finally:
        engine.dispose()
    print(json.dumps({"g4_passed": result.passed, "evidence_ref": evidence}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
