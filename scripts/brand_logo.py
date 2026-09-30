"""Generate the Moirai logo SVGs in `brand/logo/` from their geometry.

The SVG files are committed; run this after changing a parameter below, then re-export the PNGs as
described in `brand/README.md`:

    .venv/Scripts/python.exe scripts/brand_logo.py

The mark: one thread in three parallel strands draws an M, which is also a price path (up, down, up,
down). A thin blade crosses the last leg: the stop cuts the loss. The solid mark (one heavy strand, for
small sizes) shows the cut as a fallen block instead, so it reads as a full stop.
"""

from __future__ import annotations

import itertools
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "brand" / "logo"

GILT = "#C9A55C"
BONE = "#ECE6D8"
INK = "#17141F"
OBSIDIAN = "#0B0A0F"
OXBLOOD_ON_DARK = "#B23A34"
OXBLOOD_ON_LIGHT = "#8E2424"

Pt = tuple[float, float]

M_PATH: list[Pt] = [(26, 106), (26, 30), (64, 70), (102, 30), (102, 106)]  # centre line, 128 grid
STRANDS = (-8.0, 0.0, 8.0)
STRAND_W = 3.2
# blade (three-strand mark)
BLADE_Y = 80.0  # where the blade crosses the centre strand
BLADE_SLOPE = 0.38  # rises to the right
BLADE_GAP = 9.0
BLADE_W = 2.4
BLADE_REACH = 7.0
# fallen block (solid mark)
BLOCK_Y = 84.0
BLOCK_SLOPE = 0.55
BLOCK_GAP = 7.0
BLOCK_DROP = 3.0

CAP = 40.0  # wordmark cap height
WORD_W = 4.2  # wordmark stroke
TRACK = 15.0


def _fmt(pts: list[Pt]) -> str:
    return " ".join(f"{x:.2f},{y:.2f}" for x, y in pts)


def offset_polyline(pts: list[Pt], d: float) -> list[Pt]:
    """Parallel polyline at signed distance `d`, with miter joins."""
    normals = []
    for (ax, ay), (bx, by) in itertools.pairwise(pts):
        n = math.hypot(bx - ax, by - ay)
        normals.append((-(by - ay) / n, (bx - ax) / n))
    out: list[Pt] = []
    for i, (px, py) in enumerate(pts):
        if i in (0, len(pts) - 1):
            nx, ny = normals[0 if i == 0 else -1]
            out.append((px + nx * d, py + ny * d))
            continue
        (n1x, n1y), (n2x, n2y) = normals[i - 1], normals[i]
        mx, my = n1x + n2x, n1y + n2y
        ml = math.hypot(mx, my)
        mx, my = mx / ml, my / ml
        k = d / (mx * n1x + my * n1y)
        out.append((px + mx * k, py + my * k))
    return out


def _on_line(y0: float, slope: float, x: float) -> float:
    return y0 - slope * (x - M_PATH[-1][0])


def _group(width: float, parts: list[str]) -> str:
    attrs = (
        f'fill="none" stroke-width="{width}" stroke-linecap="butt" stroke-linejoin="miter" '
        'stroke-miterlimit="12"'
    )
    return f"<g {attrs}>{''.join(parts)}</g>"


def bladed_mark(thread: str, blade: str) -> str:
    parts, legs = [], []
    for d in STRANDS:
        line = offset_polyline(M_PATH, d)
        x = line[-1][0]
        legs.append(x)
        y = _on_line(BLADE_Y, BLADE_SLOPE, x)
        parts.append(f'<polyline points="{_fmt([*line[:-1], (x, y - BLADE_GAP / 2)])}" stroke="{thread}"/>')
        parts.append(f'<polyline points="{_fmt([(x, y + BLADE_GAP / 2), line[-1]])}" stroke="{thread}"/>')
    x1, x2 = min(legs) - BLADE_REACH, max(legs) + BLADE_REACH
    parts.append(
        f'<line x1="{x1:.2f}" y1="{_on_line(BLADE_Y, BLADE_SLOPE, x1):.2f}" x2="{x2:.2f}" '
        f'y2="{_on_line(BLADE_Y, BLADE_SLOPE, x2):.2f}" stroke="{blade}" stroke-width="{BLADE_W}"/>'
    )
    return _group(STRAND_W, parts)


def solid_mark(thread: str, cut: str, width: float = 13.0) -> str:
    line = list(M_PATH)
    x, bottom = line[-1]
    y = _on_line(BLOCK_Y, BLOCK_SLOPE, x)
    body = f'<polyline points="{_fmt([*line[:-1], (x, y)])}" stroke="{thread}"/>'
    fallen = [(x, y + BLOCK_GAP + BLOCK_DROP), (x, bottom + BLOCK_DROP)]
    block = f'<polyline points="{_fmt(fallen)}" stroke="{cut}"/>'
    return _group(width, [body, block])


def wordmark(ink: str) -> tuple[str, float]:
    """MOIRAI in monoline capitals; the A is a Greek lambda (an M turned upside down)."""
    h = WORD_W / 2
    s: list[str] = []
    x = 0.0
    m_w = 36.0
    s.append(
        f'<polyline points="{_fmt([(x + h, CAP), (x + h, h * 2), (x + m_w / 2, CAP * 0.62), (x + m_w - h, h * 2), (x + m_w - h, CAP)])}"/>'  # noqa: E501
    )
    x += m_w + TRACK
    s.append(f'<circle cx="{x + CAP / 2:.2f}" cy="{CAP / 2:.2f}" r="{CAP / 2 - h:.2f}"/>')
    x += CAP + TRACK
    s.append(f'<line x1="{x + h:.2f}" y1="0" x2="{x + h:.2f}" y2="{CAP:.2f}"/>')
    x += WORD_W + TRACK
    stem, bowl_w, bowl_h = x + h, 16.0, 22.0
    r = (bowl_h - WORD_W) / 2
    s.append(
        f'<path d="M{stem:.2f},{CAP:.2f} V{h:.2f} H{stem + bowl_w - r:.2f} '
        f"A{r:.2f},{r:.2f} 0 0 1 {stem + bowl_w - r:.2f},{bowl_h - h:.2f} H{stem:.2f} "
        f'M{stem + bowl_w - r - 2:.2f},{bowl_h - h:.2f} L{x + 27:.2f},{CAP:.2f}"/>'
    )
    x += 28 + TRACK
    a_w = 36.0
    s.append(f'<polyline points="{_fmt([(x, CAP), (x + a_w / 2, h * 1.2), (x + a_w, CAP)])}"/>')
    x += a_w + TRACK
    s.append(f'<line x1="{x + h:.2f}" y1="0" x2="{x + h:.2f}" y2="{CAP:.2f}"/>')
    x += WORD_W
    return f'<g stroke="{ink}">{_group(WORD_W, s)}</g>', x


def svg(view: str, body: str) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{view}" role="img" aria-label="Moirai">'
        f"<title>Moirai</title>{body}</svg>\n"
    )


def lockup_horizontal(mark: str, word: str, word_w: float) -> str:
    scale, word_x = 0.62, 72.0
    width = word_x + word_w * scale + 4
    body = (
        f'<g transform="scale(0.5)">{mark}</g>'
        f'<g transform="translate({word_x},{32 - CAP * scale / 2:.2f}) scale({scale})">{word}</g>'
    )
    return svg(f"0 0 {width:.2f} 64", body)


def lockup_stacked(mark: str, word: str, word_w: float) -> str:
    scale = 0.9
    width = max(word_w * scale, 128) + 16
    body = (
        f'<g transform="translate({(width - 128) / 2:.2f},0)">{mark}</g>'
        f'<g transform="translate({(width - word_w * scale) / 2:.2f},146) scale({scale})">{word}</g>'
    )
    return svg(f"0 0 {width:.2f} {146 + CAP * scale + 8:.2f}", body)


def tile(mark: str, scale: float, radius: int) -> str:
    """A mark centred on an obsidian rounded square (safe inside a circle crop)."""
    return svg(
        "0 0 128 128",
        f'<rect width="128" height="128" rx="{radius}" fill="{OBSIDIAN}"/>'
        f'<g transform="translate(64,64) scale({scale}) translate(-64,-68)">{mark}</g>',
    )


def main() -> None:
    dark, ink = bladed_mark(GILT, OXBLOOD_ON_DARK), bladed_mark(INK, OXBLOOD_ON_LIGHT)
    word_dark, word_w = wordmark(BONE)
    word_ink, _ = wordmark(INK)
    word_view = f"-2 -2 {word_w + 4:.2f} {CAP + 6:.2f}"
    files = {
        "moirai-mark.svg": svg("0 0 128 128", dark),
        "moirai-mark-ink.svg": svg("0 0 128 128", ink),
        "moirai-mark-solid.svg": svg("0 0 128 128", solid_mark(GILT, OXBLOOD_ON_DARK)),
        "moirai-mark-solid-ink.svg": svg("0 0 128 128", solid_mark(INK, OXBLOOD_ON_LIGHT)),
        "moirai-wordmark.svg": svg(word_view, word_dark),
        "moirai-wordmark-ink.svg": svg(word_view, word_ink),
        "moirai-lockup-horizontal.svg": lockup_horizontal(dark, word_dark, word_w),
        "moirai-lockup-horizontal-ink.svg": lockup_horizontal(ink, word_ink, word_w),
        "moirai-lockup-stacked.svg": lockup_stacked(dark, word_dark, word_w),
        "moirai-lockup-stacked-ink.svg": lockup_stacked(ink, word_ink, word_w),
        "moirai-app-icon.svg": tile(solid_mark(GILT, OXBLOOD_ON_DARK), 0.78, 28),
        "favicon.svg": tile(solid_mark(GILT, OXBLOOD_ON_DARK, width=16.0), 0.8, 24),
    }
    OUT.mkdir(parents=True, exist_ok=True)
    for name, text in files.items():
        (OUT / name).write_text(text, encoding="utf-8", newline="\n")
    print(f"wrote {len(files)} files to {OUT}")


if __name__ == "__main__":
    main()
