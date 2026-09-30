"""`hdt-kill` from a source checkout (the installed console script is the same `hdt.execution.kill_cli`).

Usage: `python scripts/hdt_kill.py --reason "..." [--account paper|testnet|live|all] [--flatten
--confirm-flatten] [--wait-s 30]`. Exit 0 = every namespace SAFE, 1 = not SAFE, 2 = usage error.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hdt.execution.kill_cli import cli  # noqa: E402

if __name__ == "__main__":
    sys.exit(cli())
