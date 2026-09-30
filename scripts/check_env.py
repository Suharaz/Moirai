"""Print present/missing for every bootstrap variable listed in .env.example. Never prints values.

A variable counts as present when NAME is set (environment or .env) or NAME_FILE points to a readable,
non-empty file. Variables whose name ends in `_FILE` hold a path: they count as present only when that
path is a readable, non-empty file. Exit code 1 when any variable is missing.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hdt.core.config import read_env_value  # noqa: E402


def expected_names(example: Path) -> list[str]:
    names: list[str] = []
    for raw in example.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        names.append(line.split("=", 1)[0].strip())
    return names


def main() -> int:
    names = expected_names(ROOT / ".env.example")
    missing = 0
    width = max(len(n) for n in names)
    for name in names:
        value = read_env_value(name)
        if name.endswith("_FILE") and value is not None:
            path = Path(value)
            present = path.is_file() and path.stat().st_size > 0
        else:
            present = value is not None
        missing += 0 if present else 1
        print(f"{name.ljust(width)}  {'present' if present else 'missing'}")
    print(f"\n{len(names) - missing}/{len(names)} present")
    return 1 if missing else 0


if __name__ == "__main__":
    raise SystemExit(main())
