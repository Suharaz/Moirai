"""English-only repository check.

Scans tracked text files (`git ls-files`, or the paths given on the command line), normalizes each file to
NFC and reports every Vietnamese letter as `path:line:col`. Exit code 1 when anything is found.

Rejected unconditionally (after NFC): the Latin-1 / Latin Extended letters used by Vietnamese, the
U+1EA0-U+1EF9 block, and any combining mark U+0300-U+0323 still attached to a vowel. Math notation such
as `w` + U+0303 stays allowed. Path allowlist: `tests/fixtures/raw/` and `data/` (recorded third-party
text). Token allowlist: `config/language_allowlist.txt` (exact proper names, one per line).
"""

from __future__ import annotations

import subprocess
import sys
import unicodedata
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ALLOWLIST_FILE = ROOT / "config" / "language_allowlist.txt"
ALLOWED_PREFIXES = ("tests/fixtures/raw/", "data/")

_RANGES: tuple[tuple[int, int], ...] = (
    (0x00C0, 0x00C3),
    (0x00C8, 0x00CA),
    (0x00CC, 0x00CD),
    (0x00D2, 0x00D5),
    (0x00D9, 0x00DA),
    (0x00DD, 0x00DD),
    (0x00E0, 0x00E3),
    (0x00E8, 0x00EA),
    (0x00EC, 0x00ED),
    (0x00F2, 0x00F5),
    (0x00F9, 0x00FA),
    (0x00FD, 0x00FD),
    (0x0102, 0x0103),
    (0x0110, 0x0111),
    (0x0128, 0x0129),
    (0x0168, 0x0169),
    (0x01A0, 0x01A1),
    (0x01AF, 0x01B0),
    (0x1EA0, 0x1EF9),
)
_COMBINING = (0x0300, 0x0323)
_VOWELS = frozenset("aeiouyAEIOUY")


@dataclass(frozen=True)
class Hit:
    path: str
    line: int
    col: int
    char: str

    def render(self) -> str:
        return f"{self.path}:{self.line}:{self.col}: U+{ord(self.char):04X}"


def _in_ranges(cp: int) -> bool:
    return any(lo <= cp <= hi for lo, hi in _RANGES)


def _is_combining_mark(cp: int) -> bool:
    return _COMBINING[0] <= cp <= _COMBINING[1]


def _flagged_positions(line: str) -> list[int]:
    positions: list[int] = []
    for idx, ch in enumerate(line):
        cp = ord(ch)
        if _in_ranges(cp):
            positions.append(idx)
        elif _is_combining_mark(cp):
            base = idx - 1
            while base >= 0 and _is_combining_mark(ord(line[base])):
                base -= 1
            if base >= 0 and line[base] in _VOWELS:
                positions.append(idx)
    return positions


def _word_at(line: str, idx: int) -> str:
    def is_word(c: str) -> bool:
        return c.isalnum() or c == "_" or unicodedata.category(c).startswith("M")

    start = idx
    while start > 0 and is_word(line[start - 1]):
        start -= 1
    end = idx
    while end < len(line) and is_word(line[end]):
        end += 1
    return line[start:end]


def scan_text(path: str, text: str, allowlist: frozenset[str] = frozenset()) -> list[Hit]:
    hits: list[Hit] = []
    normalized = unicodedata.normalize("NFC", text)
    for line_no, line in enumerate(normalized.splitlines(), start=1):
        for idx in _flagged_positions(line):
            if allowlist and _word_at(line, idx) in allowlist:
                continue
            hits.append(Hit(path, line_no, idx + 1, line[idx]))
    return hits


def is_path_allowed(rel_path: str) -> bool:
    posix = rel_path.replace("\\", "/")
    return posix.startswith(ALLOWED_PREFIXES)


def load_allowlist(path: Path = ALLOWLIST_FILE) -> frozenset[str]:
    if not path.exists():
        return frozenset()
    entries = (unicodedata.normalize("NFC", raw.strip()) for raw in path.read_text("utf-8").splitlines())
    return frozenset(e for e in entries if e and not e.startswith("#"))


def read_text_file(path: Path) -> str | None:
    """Decode a text file for scanning; None only for binary files.

    UTF-16 (BOM) is decoded; bytes that are not valid UTF-8 are decoded as cp1258 (the legacy Windows
    Vietnamese code page) so text in a non-UTF-8 encoding is still scanned instead of silently skipped.
    """
    data = path.read_bytes()
    if data.startswith((b"\xff\xfe", b"\xfe\xff")):
        return data.decode("utf-16", errors="replace")
    if b"\x00" in data[:8192]:
        return None
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return data.decode("cp1258", errors="replace")


def tracked_files(root: Path) -> list[str]:
    out = subprocess.run(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],  # noqa: S607
        cwd=root,
        check=True,
        capture_output=True,
    ).stdout
    return [p for p in out.decode("utf-8").split("\x00") if p]


def _display_path(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def scan_paths(root: Path, paths: list[Path], allowlist: frozenset[str]) -> list[Hit]:
    hits: list[Hit] = []
    for full in paths:
        rel = _display_path(full, root)
        if is_path_allowed(rel) or not full.is_file():
            continue
        text = read_text_file(full)
        if text is None:
            continue
        hits.extend(scan_text(rel, text, allowlist))
    return hits


def main(argv: list[str] | None = None, root: Path = ROOT) -> int:
    args = sys.argv[1:] if argv is None else argv
    paths = [Path(a) for a in args] if args else [root / p for p in tracked_files(root)]
    hits = scan_paths(root, paths, load_allowlist(root / "config" / "language_allowlist.txt"))
    for hit in hits:
        print(hit.render())
    if hits:
        print(f"{len(hits)} non-English character(s) found", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
