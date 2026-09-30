"""Render deploy/redis/users.acl from the template and HDT_REDIS_PASSWORD_<USER> variables.

Passwords are read from the environment (or NAME_FILE) and from `.env` when present; the rendered file
is git-ignored. Redis ACL files do not support comments, so comment lines are dropped. The script never
prints a password.
"""

from __future__ import annotations

import re
import sys
from collections.abc import Callable
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hdt.core.config import read_env_value  # noqa: E402

TEMPLATE = ROOT / "deploy" / "redis" / "users.acl.template"
OUTPUT = ROOT / "deploy" / "redis" / "users.acl"
PLACEHOLDER = re.compile(r"\{\{([A-Z_]+)\}\}")
SAFE_PASSWORD = re.compile(r"^[A-Za-z0-9_\-.~+=]{16,}$")


def strip_comments(template: str) -> str:
    lines = [line for line in template.splitlines() if line.strip() and not line.lstrip().startswith("#")]
    return "\n".join(lines) + "\n"


def render(template: str, lookup: Callable[[str], str | None] = read_env_value) -> str:
    """Fill every placeholder with `lookup("HDT_REDIS_PASSWORD_<USER>")` (default: environment / .env)."""
    missing: list[str] = []

    def substitute(match: re.Match[str]) -> str:
        name = f"HDT_REDIS_PASSWORD_{match.group(1)}"
        value = lookup(name)
        if value is None:
            missing.append(name)
            return ""
        if not SAFE_PASSWORD.fullmatch(value):
            raise SystemExit(f"{name}: must be >= 16 chars of [A-Za-z0-9_-.~+=] (no spaces)")
        return value

    rendered = PLACEHOLDER.sub(substitute, strip_comments(template))
    if missing:
        raise SystemExit("missing: " + ", ".join(sorted(set(missing))))
    return rendered


def main() -> int:
    OUTPUT.write_text(render(TEMPLATE.read_text(encoding="utf-8")), encoding="utf-8", newline="\n")
    print(f"wrote {OUTPUT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
