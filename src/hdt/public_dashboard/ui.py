"""HTML building blocks of the public dashboard.

The markup, formatting and chart styling follow the admin console (`hdt.console.components`), but the
helpers are defined here because the console module imports the database, config-api client and
configuration code, which the public dashboard must never load. Only `hdt.console.theme` and `hdt.brand`
(standard library only) are shared, so both apps render the same design tokens.
"""

from __future__ import annotations

import html
import math
import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Final, Literal

import altair as alt
import streamlit as st
from streamlit.delta_generator import DeltaGenerator

from hdt.console.theme import DEFAULT_THEME, SANS, ThemeMode, chart_palette

Kind = Literal["pos", "neg", "warn", "info", "mute"]
THEME_KEY: Final[str] = "hdt_public_theme"


# --------------------------------------------------------------------------- formatting


def esc(value: object) -> str:
    return html.escape("" if value is None else str(value), quote=True)


def parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def fmt_num(value: float | int | None, dp: int = 2) -> str:
    """US number format `1,234.56`; `-` for missing values."""
    if value is None or (isinstance(value, float) and not math.isfinite(value)):
        return "-"
    return f"{value:,.{dp}f}"


def fmt_int(value: int | float | None) -> str:
    return "-" if value is None else f"{round(value):,}"


def fmt_signed(value: float | None, dp: int = 2) -> str:
    """Profit/loss always carries a sign; a value that rounds to zero shows as `+0.00`, never `-0.00`."""
    return "-" if value is None else f"{round(value, dp) + 0.0:+,.{dp}f}"


def fmt_pct(fraction: float | None, dp: int = 1, *, signed: bool = False) -> str:
    if fraction is None:
        return "-"
    pct = fraction * 100
    return f"{pct:+,.{dp}f}%" if signed else f"{pct:,.{dp}f}%"


def fmt_usd(value: float | None, dp: int = 2) -> str:
    if value is None:
        return "-"
    sign = "-" if value < 0 else ""
    return f"{sign}${abs(value):,.{dp}f}"


def fmt_ts(value: str | datetime | None) -> str:
    """UTC timestamp `YYYY-MM-DD HH:MM`."""
    ts = parse_ts(value) if isinstance(value, str) else value
    return "-" if ts is None else ts.astimezone(UTC).strftime("%Y-%m-%d %H:%M")


def fmt_ts_short(value: str | datetime | None) -> str:
    """Table timestamp `MM-DD HH:MM` (UTC)."""
    ts = parse_ts(value) if isinstance(value, str) else value
    return "-" if ts is None else ts.astimezone(UTC).strftime("%m-%d %H:%M")


def fmt_duration(seconds: float | None) -> str:
    """Compact duration `6h12m`, `1m12s`."""
    if seconds is None:
        return "-"
    total = max(0, round(seconds))
    days, rem = divmod(total, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    if days:
        return f"{days}d{hours:02d}h"
    if hours:
        return f"{hours}h{minutes:02d}m"
    if minutes:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


# --------------------------------------------------------------------------- HTML atoms


def icon(name: str, *, cls: str = "", small: bool = False) -> str:
    size = " sm" if small else ""
    extra = f" {cls}" if cls else ""
    return f'<span class="i i-{esc(name)}{size}{extra}" aria-hidden="true"></span>'


def badge(text: str, kind: Kind = "mute") -> str:
    return f'<span class="badge b-{kind}">{esc(text)}</span>'


def tag(text: str, kind: Literal["cmc", "bnb", "llm", ""] = "") -> str:
    cls = f" t-{kind}" if kind else ""
    return f'<span class="tag{cls}">{esc(text)}</span>'


def callout(body_html: str, kind: Kind | None = None, *, icon_name: str = "info") -> str:
    cls = {"pos": " c-pos", "neg": " c-neg", "warn": " c-warn", "info": " c-info"}.get(kind or "", "")
    return f'<div class="callout{cls}" role="note">{icon(icon_name)}<div>{body_html}</div></div>'


def page_head(title: str, description: str = "", *, badge_html: str = "") -> str:
    """Page heading; `badge_html` (pre-escaped) sits on the title line, e.g. a decision's outcome tag."""
    desc = f"<p>{esc(description)}</p>" if description else ""
    badge_html = f" {badge_html}" if badge_html else ""
    return f'<div class="page-head"><div><h1 tabindex="-1">{esc(title)}{badge_html}</h1>{desc}</div></div>'


@dataclass(frozen=True)
class Meter:
    fraction: float
    kind: Literal["pos", "neg", "warn", ""] = ""

    def html(self) -> str:
        width = max(0.0, min(self.fraction, 1.0)) * 100
        cls = "meter" + (f" m-{self.kind}" if self.kind else "")
        return f'<div class="{cls}" aria-hidden="true"><span style="width:{width:.2f}%"></span></div>'


@dataclass(frozen=True)
class Kpi:
    label: str
    value_html: str
    sub_html: str = ""
    value_cls: str = ""
    meter: Meter | None = None

    def html(self) -> str:
        meter = self.meter.html() if self.meter else ""
        cls = f" {self.value_cls}" if self.value_cls else ""
        return (
            f'<div class="kpi"><div class="lab">{esc(self.label)}</div>'
            f'<div class="val{cls}">{self.value_html}</div>'
            f'<div class="sub">{self.sub_html}</div>{meter}</div>'
        )


def kpis(items: Sequence[Kpi]) -> str:
    return '<div class="kpis">' + "".join(k.html() for k in items) + "</div>"


def pnl_html(value: float | None, dp: int = 2, *, arrow: bool = True) -> tuple[str, str]:
    """Signed PnL with an arrow (KPI) and its class; never color alone. Sign follows the displayed value."""
    if value is None:
        return "-", ""
    shown = round(value, dp)
    cls = "up" if shown > 0 else "down" if shown < 0 else ""
    arrow_html = icon("up-a" if shown > 0 else "down-a") if arrow and shown != 0 else ""
    return f"{arrow_html}{esc(fmt_signed(value, dp))}", cls


def signed_span(value: float | None, dp: int = 2) -> str:
    shown = 0.0 if value is None else round(value, dp)
    cls = "up" if shown > 0 else "down" if shown < 0 else ""
    return f'<span class="num {cls}">{esc(fmt_signed(value, dp))}</span>'


def side_html(side: str | None) -> str:
    if side == "LONG":
        return '<span class="side-long">LONG</span>'
    if side == "SHORT":
        return '<span class="side-short">SHORT</span>'
    return esc(side or "-")


def check_list(items: Iterable[tuple[bool | None, str]]) -> str:
    out = []
    for state, text_html in items:
        name, cls = ("wait", "wait-i") if state is None else ("ok", "ok-i") if state else ("no", "no-i")
        label = "Pending" if state is None else "Met" if state else "Not met"
        out.append(f'<li>{icon(name, cls=cls)}<span class="sr">{label}: </span><span>{text_html}</span></li>')
    return '<ul class="check-list">' + "".join(out) + "</ul>"


def kv(pairs: Iterable[tuple[str, str]]) -> str:
    body = "".join(f"<dt>{esc(k)}</dt><dd>{v}</dd>" for k, v in pairs)
    return f'<dl class="kv">{body}</dl>'


# A table with fewer body rows than this is too short to hold an open header tooltip inside its scroll box.
SHORT_TABLE_ROWS: Final[int] = 5


@dataclass(frozen=True)
class Column:
    label: str
    numeric: bool = False
    tip: str = ""

    def head_html(self, tip_id: str) -> str:
        """Header cell; a `tip` opens on hover, tap or keyboard focus (the label is focusable for that) and
        is the label's accessible description, not part of the column name."""
        cls = ' class="r"' if self.numeric else ""
        if not self.tip:
            return f"<th{cls}>{esc(self.label)}</th>"
        return (
            f'<th{cls}><span class="th-tip" tabindex="0" aria-describedby="{esc(tip_id)}">{esc(self.label)}'
            f'{icon("info", small=True)}<span class="tip" role="tooltip" id="{esc(tip_id)}">{esc(self.tip)}'
            "</span></span></th>"
        )


@dataclass(frozen=True)
class Cell:
    html: str
    cls: str = ""


CellLike = Cell | str


@dataclass
class Table:
    columns: Sequence[Column]
    rows: list[Sequence[CellLike]] = field(default_factory=list)
    chosen: set[int] = field(default_factory=set)
    caption: str = ""

    def html(self) -> str:
        slug = re.sub(r"[^a-z0-9]+", "-", self.caption.lower()).strip("-") or "table"
        head = "".join(c.head_html(f"tip-{slug}-{i}") for i, c in enumerate(self.columns))
        body_rows: list[str] = []
        for idx, row in enumerate(self.rows):
            cells: list[str] = []
            for col, cell in zip(self.columns, row, strict=True):
                c = cell if isinstance(cell, Cell) else Cell(esc(cell))
                classes = " ".join(x for x in ("r num" if col.numeric else "", c.cls) if x)
                cls_attr = f' class="{classes}"' if classes else ""
                cells.append(f"<td{cls_attr}>{c.html}</td>")
            tr_cls = ' class="is-chosen"' if idx in self.chosen else ""
            body_rows.append(f"<tr{tr_cls}>{''.join(cells)}</tr>")
        cap = f'<caption class="sr">{esc(self.caption)}</caption>' if self.caption else ""
        short = len(self.rows) < SHORT_TABLE_ROWS and any(c.tip for c in self.columns)
        wrap_cls = "tbl-wrap tbl-short" if short else "tbl-wrap"
        return (
            f'<div class="{wrap_cls}"><table>{cap}<thead><tr>{head}</tr></thead>'
            f"<tbody>{''.join(body_rows)}</tbody></table></div>"
        )


def card(inner_html: str, *, title: str = "", subtitle: str = "") -> str:
    head = ""
    if title:
        sub = f"<p>{esc(subtitle)}</p>" if subtitle else ""
        head = f'<div class="card-head"><div><h2>{esc(title)}</h2>{sub}</div></div>'
    return f'<div class="card">{head}{inner_html}</div>'


@dataclass(frozen=True)
class Hero:
    """Headline figure of a page: label, big value, change line and a small fact list on the right."""

    label_html: str
    value_html: str
    delta_html: str = ""
    facts: Sequence[tuple[str, str]] = ()

    def html(self) -> str:
        facts = "".join(f"<dt>{esc(k)}</dt><dd>{v}</dd>" for k, v in self.facts)
        side = f'<dl class="hero-meta">{facts}</dl>' if facts else ""
        delta = f'<div class="hero-delta">{self.delta_html}</div>' if self.delta_html else ""
        return (
            f'<section class="hero"><div><div class="hero-lab">{self.label_html}</div>'
            f'<div class="hero-val">{self.value_html}</div>{delta}</div>{side}</section>'
        )


def empty_html(title: str, detail: str = "") -> str:
    body = f"<p>{esc(detail)}</p>" if detail else ""
    return (
        f'<div class="empty" role="note"><span class="ei">{icon("wait")}</span>'
        f"<div><b>{esc(title)}</b>{body}</div></div>"
    )


def render(markup: str) -> None:
    """Emit dashboard HTML (wrapped in the `.hdt` scope); `st.html` sanitizes scripts."""
    st.html(f'<div class="hdt">{markup}</div>')


def empty(title: str, detail: str = "") -> None:
    """Empty state: what is missing and when it will appear."""
    render(empty_html(title, detail))


# --------------------------------------------------------------------------- theme and charts


def current_theme() -> ThemeMode:
    value = st.session_state.get(THEME_KEY, DEFAULT_THEME)
    return "light" if value == "light" else "dark"


def toggle_theme() -> None:
    st.session_state[THEME_KEY] = "light" if current_theme() == "dark" else "dark"


def panel(key: str) -> DeltaGenerator:
    """A glass card holding a whole column (heading, controls, content); side-by-side panels match height."""
    return st.container(key=f"hdt-card-panel-{key}", height="stretch")


def palette() -> dict[str, str]:
    return chart_palette(current_theme())


def show_chart(chart: alt.TopLevelMixin, *, height: int = 260, description: str = "") -> None:
    """Render an Altair chart with the design tokens and a text summary (never color alone)."""
    p = palette()
    styled = (
        chart.properties(height=height, background=p["background"])
        .configure_view(strokeWidth=0)
        .configure_axis(
            labelColor=p["text"],
            titleColor=p["text"],
            gridColor=p["grid"],
            domain=False,
            ticks=False,
            labelPadding=8,
            labelFont="Fira Code",
            titleFont=SANS,
            labelFontSize=11,
            titleFontSize=11,
            titleFontWeight=500,
        )
        .configure_axisX(grid=False, labelOverlap="greedy", labelFlush=True)
        .configure_legend(
            labelColor=p["text"], titleColor=p["text"], labelFont=SANS, labelFontSize=12, orient="bottom"
        )
    )
    st.altair_chart(styled, theme=None, use_container_width=True)
    if description:
        st.caption(description)
