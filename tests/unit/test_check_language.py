"""Every sample is built from escapes so this file itself stays English-only."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[2]


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("check_language", ROOT / "scripts" / "check_language.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_language"] = module
    spec.loader.exec_module(module)
    return module


cl = _load()


def _repo(tmp_path: Path, files: dict[str, str], allowlist: str = "") -> Path:
    (tmp_path / "config").mkdir(parents=True, exist_ok=True)
    (tmp_path / "config" / "language_allowlist.txt").write_text(allowlist, encoding="utf-8")
    for rel, text in files.items():
        path = tmp_path / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    return tmp_path


def _run(root: Path, rel: str) -> int:
    return int(cl.main([str(root / rel)], root=root))


@pytest.mark.parametrize(
    "sample",
    [
        "gi\u1ea3m r\u1ee7i ro",  # U+1EA0-series letters
        "c\u00e1c",  # Latin-1 vowel with accent
        "ca\u0301c",  # decomposed: base vowel + combining acute
        "\u0111\u01b0\u1ee3c",  # d with stroke, horn letters
    ],
)
def test_vietnamese_text_fails(tmp_path: Path, sample: str, capsys: pytest.CaptureFixture[str]) -> None:
    root = _repo(tmp_path, {"src/mod.py": f"# ok line\nx = '{sample}'\n"})
    assert _run(root, "src/mod.py") == 1
    assert "src/mod.py:2:" in capsys.readouterr().out


def test_math_tilde_on_w_passes(tmp_path: Path) -> None:
    root = _repo(tmp_path, {"docs/math.md": "normalized weight w\u0303_i\n"})
    assert _run(root, "docs/math.md") == 0


def test_clean_english_file_passes(tmp_path: Path) -> None:
    root = _repo(tmp_path, {"docs/a.md": "Plain English text with numbers 1,234.56 and symbols <= >=.\n"})
    assert _run(root, "docs/a.md") == 0


def test_raw_fixture_path_is_allowlisted(tmp_path: Path) -> None:
    root = _repo(tmp_path, {"tests/fixtures/raw/news.txt": "tin t\u1ee9c\n", "data/x.txt": "c\u00e1c\n"})
    assert _run(root, "tests/fixtures/raw/news.txt") == 0
    assert _run(root, "data/x.txt") == 0


def test_allowlisted_proper_name_passes_but_other_words_fail(tmp_path: Path) -> None:
    name = "Nguy\u1ec5n"
    root = _repo(
        tmp_path,
        {"docs/ok.md": f"Author: {name}\n", "docs/bad.md": f"Author: {name} vi\u1ebft\n"},
        allowlist=f"# reviewed\n{name}\n",
    )
    assert _run(root, "docs/ok.md") == 0
    assert _run(root, "docs/bad.md") == 1


def test_binary_file_is_skipped(tmp_path: Path) -> None:
    root = _repo(tmp_path, {})
    (root / "blob.bin").write_bytes(b"\x00\x01" + "c\u00e1c".encode())
    assert _run(root, "blob.bin") == 0


@pytest.mark.parametrize("encoding", ["utf-16", "cp1258"])
def test_vietnamese_in_non_utf8_encodings_is_still_caught(tmp_path: Path, encoding: str) -> None:
    root = _repo(tmp_path, {})
    (root / "docs").mkdir()
    (root / "docs" / "legacy.txt").write_bytes("ghi ch\u00fa c\u00e1c\n".encode(encoding))
    assert _run(root, "docs/legacy.txt") == 1
