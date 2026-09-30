"""Fit the per-agent `p_model` coefficients on the recorded lake (phase 03, after gate G1 passes).

Collects the independent candidate events of the passed G1 report's window
(`hdt.quant.fit.collect_samples`) and fits each model agent (Crowding, Technical, Microstructure,
Fundamental, Macro) per target type with an L2 logistic regression, walk-forward. One new version file per
(agent, target type) is written to `--out` (default `reports/p_model/`), for example
`reports/p_model/crowding-resid_12h-pm1.yaml`. The active `config/p_model/<agent>.<target>.yaml` is not
touched: a reviewer promotes a version by committing it there (a new file version, never an in-place edit).

Refuses to run unless `reports/g1/latest.json` reads `passed` with the study it was computed on, the JSON
report it names holds the same status, and that study's seed, rule / label / feature versions and config pins
are the current ones (`hdt.quant.fit.passed_g1`). The window is the study's `[start, end)`; no option can
change it. Fit parameters come from the versioned `config/p_model_fit.yaml`.

Usage: `uv run python scripts/fit_p_model.py [--reports reports/g1] [--out reports/p_model]`
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

from hdt.contracts.common import AgentName, TargetType
from hdt.core.config import (
    CONFIG_DIR,
    load_p_model,
    load_p_model_fit,
    scanner_config,
    static_config,
)
from hdt.lake.pit_query import PitQuery
from hdt.quant.fit import FitRefusedError, collect_samples, fit_agent, passed_g1
from hdt.quant.p_model import MODEL_AGENTS

ROOT = Path(__file__).resolve().parents[1]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument("--reports", type=Path, default=ROOT / "reports" / "g1")
    parser.add_argument("--out", type=Path, default=ROOT / "reports" / "p_model")
    args = parser.parse_args(argv)
    static, scanner = static_config(), scanner_config()
    try:
        study, source = passed_g1(args.reports, ROOT, static, scanner)
    except FitRefusedError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    params = load_p_model_fit(CONFIG_DIR)
    data = static.settings.data
    pit = PitQuery(Path(data.staging_root), Path(data.lake_root))
    specs = [load_p_model(a.value, t.value, CONFIG_DIR) for a in MODEL_AGENTS for t in TargetType]
    samples = collect_samples(pit, static, scanner, study.start, study.end, specs)
    args.out.mkdir(parents=True, exist_ok=True)
    for spec in specs:
        agent = AgentName(spec.agent)
        result = fit_agent(agent, spec, samples, params, source=source)
        path = args.out / f"{result.spec.p_model_ver}.yaml"
        body = yaml.safe_dump(result.spec.model_dump(mode="json"), sort_keys=False, allow_unicode=False)
        path.write_text(body, encoding="utf-8")
        print(
            f"{agent.value} {spec.target_type}: {result.rows} rows, oos log-loss {result.spec.oos_logloss}, "
            f"shrunk {list(result.spec.shrunk_families)} -> {path}"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
