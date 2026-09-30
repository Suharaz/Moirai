"""Shared UI building blocks for the admin console.

HTML helpers return escaped markup that follows the mockup classes (`theme.py` styles them under `.hdt`);
`render()` emits it through `st.html`. Native helpers wrap Streamlit widgets with the console conventions:
async buttons disable themselves while running and report through a polite toast, dangerous actions go
through `st.dialog` confirmations, and every number is formatted in US style with tabular digits.
"""

from __future__ import annotations

import html
import math
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Final, Literal

import altair as alt
import streamlit as st
from sqlalchemy import Engine

from hdt.console.api import ApiError, describe_error
from hdt.console.readmodels import ReadModels
from hdt.console.theme import DEFAULT_THEME, SANS, ThemeMode, chart_palette
from hdt.core.clock import ensure_utc, utcnow
from hdt.core.config import read_env_value
from hdt.db.session import make_engine

Kind = Literal["pos", "neg", "warn", "info", "mute"]
ToastKind = Literal["ok", "err", "info"]

_NOTIFY_KEY = "_hdt_notifications"
_TOAST_ICONS: dict[ToastKind, str] = {
    "ok": ":material/check_circle:",
    "err": ":material/error:",
    "info": ":material/info:",
}


# --------------------------------------------------------------------------- formatting


def esc(value: object) -> str:
    return html.escape("" if value is None else str(value), quote=True)


def fmt_num(value: float | int | None, dp: int = 2) -> str:
    """US number format `1,234.56`; `-` for missing values."""
    if value is None or (isinstance(value, float) and not math.isfinite(value)):
        return "-"
    return f"{value:,.{dp}f}"


def fmt_int(value: int | float | None) -> str:
    if value is None:
        return "-"
    return f"{round(value):,}"


def fmt_signed(value: float | None, dp: int = 2) -> str:
    """Profit/loss always carries a sign."""
    if value is None:
        return "-"
    return f"{value:+,.{dp}f}"


def fmt_pct(fraction: float | None, dp: int = 1, *, signed: bool = False) -> str:
    """Render a fraction (0.0123) as a percentage (`1.2%`)."""
    if fraction is None:
        return "-"
    pct = fraction * 100
    return f"{pct:+,.{dp}f}%" if signed else f"{pct:,.{dp}f}%"


def fmt_usd(value: float | None, dp: int = 2) -> str:
    """LLM cost in dollars: `$1.94`."""
    if value is None:
        return "-"
    sign = "-" if value < 0 else ""
    return f"{sign}${abs(value):,.{dp}f}"


def fmt_ts(value: datetime | None) -> str:
    """UTC timestamp `YYYY-MM-DD HH:MM`."""
    if value is None:
        return "-"
    return ensure_utc(value).strftime("%Y-%m-%d %H:%M")


def fmt_ts_short(value: datetime | None) -> str:
    """Table timestamp `MM-DD HH:MM` (UTC)."""
    if value is None:
        return "-"
    return ensure_utc(value).strftime("%m-%d %H:%M")


def fmt_duration(seconds: float | None) -> str:
    """Compact age `6h12m`, `1m12s`, `0.4s`."""
    if seconds is None:
        return "-"
    if seconds < 1:
        return f"{seconds:.1f}s"
    total = round(seconds)
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


def badge(text: str, kind: Kind = "mute", *, icon_name: str | None = None, dot: bool = False) -> str:
    lead = icon(icon_name, small=True) if icon_name else ('<span class="dot"></span>' if dot else "")
    return f'<span class="badge b-{kind}">{lead}{esc(text)}</span>'


def tag(text: str, kind: Literal["cmc", "bnb", "llm", ""] = "") -> str:
    cls = f" t-{kind}" if kind else ""
    return f'<span class="tag{cls}">{esc(text)}</span>'


def callout(body_html: str, kind: Kind | None = None, *, icon_name: str = "info") -> str:
    cls = {"pos": " c-pos", "neg": " c-neg", "warn": " c-warn", "info": " c-info"}.get(kind or "", "")
    return f'<div class="callout{cls}" role="note">{icon(icon_name)}<div>{body_html}</div></div>'


def page_head(title: str, description: str = "", *, right_html: str = "") -> str:
    desc = f"<p>{esc(description)}</p>" if description else ""
    right = f'<div class="row-actions">{right_html}</div>' if right_html else ""
    return f'<div class="page-head"><div><h1 tabindex="-1">{esc(title)}</h1>{desc}</div>{right}</div>'


@dataclass(frozen=True)
class Meter:
    fraction: float
    kind: Literal["pos", "neg", "warn", ""] = ""
    marker: float | None = None
    tall: bool = False

    def html(self) -> str:
        width = max(0.0, min(self.fraction, 1.0)) * 100
        cls = "meter" + (f" m-{self.kind}" if self.kind else "") + (" tall" if self.tall else "")
        mark = ""
        if self.marker is not None:
            mark = f'<i style="left:{max(0.0, min(self.marker, 1.0)) * 100:.2f}%"></i>'
        return f'<div class="{cls}" aria-hidden="true"><span style="width:{width:.2f}%"></span>{mark}</div>'


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
    """Signed PnL with an arrow (KPI) and its class; never color alone."""
    if value is None:
        return "-", ""
    cls = "up" if value > 0 else "down" if value < 0 else ""
    arrow_html = ""
    if arrow and value != 0:
        arrow_html = icon("up-a" if value > 0 else "down-a")
    return f"{arrow_html}{esc(fmt_signed(value, dp))}", cls


def check_item(state: bool | None, text_html: str) -> str:
    name, cls = ("wait", "wait-i") if state is None else ("ok", "ok-i") if state else ("no", "no-i")
    label = "Pending" if state is None else "Met" if state else "Not met"
    return f'<li>{icon(name, cls=cls)}<span class="sr">{label}: </span><span>{text_html}</span></li>'


def check_list(items: Iterable[tuple[bool | None, str]]) -> str:
    return '<ul class="check-list">' + "".join(check_item(s, t) for s, t in items) + "</ul>"


def kv(pairs: Iterable[tuple[str, str]], *, text: bool = False) -> str:
    """Definition list; values are pre-escaped HTML."""
    cls = "kv text" if text else "kv"
    body = "".join(f"<dt>{esc(k)}</dt><dd>{v}</dd>" for k, v in pairs)
    return f'<dl class="{cls}">{body}</dl>'


@dataclass(frozen=True)
class Column:
    label: str
    numeric: bool = False


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
        head = "".join(
            f'<th class="r">{esc(c.label)}</th>' if c.numeric else f"<th>{esc(c.label)}</th>"
            for c in self.columns
        )
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
        return (
            f'<div class="tbl-wrap"><table>{cap}<thead><tr>{head}</tr></thead>'
            f"<tbody>{''.join(body_rows)}</tbody></table></div>"
        )


def card(inner_html: str, *, title: str = "", subtitle: str = "", right_html: str = "") -> str:
    head = ""
    if title or right_html:
        sub = f"<p>{esc(subtitle)}</p>" if subtitle else ""
        head = f'<div class="card-head"><div><h2>{esc(title)}</h2>{sub}</div>{right_html}</div>'
    return f'<div class="card">{head}{inner_html}</div>'


def render(markup: str, *, target: object | None = None) -> None:
    """Emit console HTML (wrapped in the `.hdt` scope) into the page or a container."""
    dg = target if target is not None else st
    dg.html(f'<div class="hdt">{markup}</div>')  # type: ignore[attr-defined]


def source_unavailable(title: str, missing: Sequence[str]) -> str:
    """Explicit empty state naming the data source that does not exist yet."""
    items = "".join(f"<li>{esc(m)}</li>" for m in missing)
    return callout(
        f'<b>{esc(title)}</b><ul style="margin:6px 0 0 18px;padding:0">{items}</ul>',
        "warn",
        icon_name="db",
    )


# --------------------------------------------------------------------------- notifications and actions


def notify(message: str, kind: ToastKind = "ok") -> None:
    """Queue a toast; shown on the next render, survives `st.rerun()`."""
    queue: list[tuple[str, ToastKind]] = st.session_state.setdefault(_NOTIFY_KEY, [])
    queue.append((message, kind))


def drain_notifications() -> None:
    """Show queued toasts (polite, auto-dismiss after the short duration, about 4 s)."""
    queue: list[tuple[str, ToastKind]] = st.session_state.pop(_NOTIFY_KEY, [])
    for message, kind in queue:
        st.toast(message, icon=_TOAST_ICONS[kind], duration="short")


def _mark_busy(busy_key: str) -> None:
    st.session_state[busy_key] = True


def action_button(
    label: str,
    *,
    key: str,
    run: Callable[[], str],
    button_type: Literal["primary", "secondary", "tertiary"] = "secondary",
    icon_name: str | None = None,
    disabled: bool = False,
    help_text: str | None = None,
    on_error: Callable[[ApiError], None] | None = None,
) -> None:
    """Async button: disables itself while the call runs, then reports through a toast.

    `run` performs the request and returns the success message. API errors become an error toast (or are
    handed to `on_error`); the button is re-enabled on the following render.
    """
    busy_key = f"_hdt_busy_{key}"
    busy = bool(st.session_state.get(busy_key, False))
    st.button(
        label,
        key=key,
        type=button_type,
        icon=icon_name,
        disabled=disabled or busy,
        help=help_text,
        on_click=_mark_busy,
        args=(busy_key,),
    )
    if not busy:
        return
    try:
        with st.spinner(f"{label}..."):
            message = run()
    except ApiError as exc:
        st.session_state[busy_key] = False
        if on_error is not None:
            on_error(exc)
        else:
            notify(describe_error(exc), "err")
        st.rerun()
    st.session_state[busy_key] = False
    notify(message, "ok")
    st.rerun()


TOTP_PATTERN = r"^\d{6}$"


def totp_input(label: str, *, key: str, help_text: str | None = None) -> str:
    """Six-digit TOTP field that commits while typing (so confirmation buttons enable immediately)."""
    value = st.text_input(
        label,
        key=key,
        max_chars=6,
        autocomplete="one-time-code",
        placeholder="000000",
        help=help_text,
        live=True,
        validate=(TOTP_PATTERN, "The code must be exactly 6 digits."),
    )
    return (value or "").strip()


def is_totp(value: str) -> bool:
    return len(value) == 6 and value.isdigit()


def typed_confirmation(word: str, *, key: str) -> bool:
    """`Type WORD to confirm` field; returns True once it matches exactly."""
    value = st.text_input(f"Type {word} to confirm", key=key, autocomplete="off", live=True)
    return (value or "").strip() == word


# --------------------------------------------------------------------------- data access

_CACHE_KEY: Final[str] = "_hdt_session_cache"
TOP_MODE_CACHE: Final[str] = "top:mode"
TOP_SECTIONS_CACHE: Final[str] = "top:sections"


def session_cached[T](key: str, ttl: timedelta, load: Callable[[], T]) -> T:
    """Per-browser-session memo with a short TTL (keeps the top bar from calling config-api every run).

    Failures are not cached; the next run retries.
    """
    cache: dict[str, tuple[datetime, object]] = st.session_state.setdefault(_CACHE_KEY, {})
    now = utcnow()
    hit = cache.get(key)
    if hit is not None and now - hit[0] < ttl:
        return hit[1]  # type: ignore[return-value]
    value = load()
    cache[key] = (now, value)
    return value


def invalidate_top_bar() -> None:
    """Drop the cached run mode and config version so the top bar reflects a change on the next run."""
    cache: dict[str, tuple[datetime, object]] = st.session_state.setdefault(_CACHE_KEY, {})
    for key in (TOP_MODE_CACHE, TOP_SECTIONS_CACHE):
        cache.pop(key, None)


CONSOLE_DSN_ENV: Final[str] = "HDT_PG_DSN"


@st.cache_resource
def _console_engine() -> tuple[Engine | None, str]:
    """Engine of the read-only console role (`hdt_console_ro`, default_transaction_read_only)."""
    dsn = read_env_value(CONSOLE_DSN_ENV)
    if dsn is None:
        return None, f"{CONSOLE_DSN_ENV} (DSN of the read-only console role hdt_console_ro) is not set."
    return make_engine(dsn, pool_size=3), ""


def read_models() -> ReadModels:
    """Read models for this script run (one information_schema lookup per instance)."""
    engine, reason = _console_engine()
    return ReadModels(engine, unavailable_reason=reason)


# --------------------------------------------------------------------------- theme and charts

THEME_KEY: Final[str] = "hdt_theme"


def current_theme() -> ThemeMode:
    value = st.session_state.get(THEME_KEY, DEFAULT_THEME)
    return "light" if value == "light" else "dark"


def palette() -> dict[str, str]:
    """Concrete token colors of the active theme (charts cannot use CSS variables)."""
    return chart_palette(current_theme())


def show_chart(chart: alt.TopLevelMixin, *, height: int = 260, description: str = "") -> None:
    """Render an Altair chart with the console tokens (not the Streamlit theme) and a text summary."""
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
