"""Public performance dashboard: `streamlit run src/hdt/public_dashboard/app.py` (service `dashboard`).

Anyone can view it: no login, no login form, no link to the admin console. It reads only the immutable
snapshots that `public-publisher` writes to `public-store`, with read-only S3 credentials
(`HDT_PUBLIC_STORE_*`). It has no credentials, network route or code path to Postgres, Redis, the vault,
config-api or execution, and imports none of those packages (checked by `tests/unit/public`).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Final

import streamlit as st

from hdt import brand
from hdt.console.theme import stylesheet
from hdt.public_dashboard import ui, views
from hdt.public_dashboard.data import STALE_AFTER, LoadedSnapshot, SnapshotUnavailableError, load_snapshot

SIDE_NOTE: Final[str] = (
    '<p class="side-note"><b>Public performance</b>'
    "Read-only. Snapshots every 60 s. Decisions, positions and orders appear as they happen.</p>"
)
_CLOCK_JS: Final[str] = (
    '<span class="top-meta mono" id="hdt-clock" aria-label="UTC time"></span>'
    "<script>(function(){var el=document.getElementById('hdt-clock');if(!el)return;"
    "function t(){el.textContent=new Date().toISOString().slice(11,19)+' UTC';}"
    "t();if(window.__hdtClock)clearInterval(window.__hdtClock);window.__hdtClock=setInterval(t,1000);})();</script>"
)
PAGES: Final[tuple[tuple[str, str, str, Callable[[LoadedSnapshot], None]], ...]] = (
    ("Overview", "overview", ":material/dashboard:", views.overview),
    ("Positions", "positions", ":material/account_balance_wallet:", views.positions),
    ("Decisions", "decisions", ":material/balance:", views.decisions),
    ("Agents", "agents", ":material/smart_toy:", views.agents),
    ("AI costs", "ai-costs", ":material/attach_money:", views.costs),
)


def _page(render: Callable[[LoadedSnapshot], None], snap: LoadedSnapshot | None) -> Callable[[], None]:
    def run() -> None:
        if snap is None:
            ui.render(
                ui.page_head(brand.NAME)
                + ui.callout(
                    "The public snapshot is not available right now. Please try again in a minute.", "warn"
                )
            )
            return
        render(snap)

    return run


def _sidebar_footer() -> None:
    """UTC clock and theme toggle, pinned to the bottom of the sidebar."""
    with st.sidebar.container(key="hdt-side-foot", horizontal=True, vertical_alignment="center"):
        st.html(f'<div class="hdt">{_CLOCK_JS}</div>', unsafe_allow_javascript=True, width="content")
        dark = ui.current_theme() == "dark"
        st.button(
            "",
            key="hdt-theme-toggle",
            icon=":material/light_mode:" if dark else ":material/dark_mode:",
            help="Toggle light or dark theme",
            on_click=ui.toggle_theme,
        )


def _stale_notice(snap: LoadedSnapshot | None) -> None:
    if snap is None or not snap.is_stale():
        return
    minutes = int(snap.age().total_seconds() // 60)
    ui.render(
        ui.callout(
            f"This snapshot was generated {minutes} minutes ago "
            f"({ui.esc(ui.fmt_ts(snap.generated_at))} UTC). Publishing is delayed by more than "
            f"{int(STALE_AFTER.total_seconds() // 60)} minutes, so the figures may be out of date.",
            "warn",
            icon_name="alert",
        )
    )


def main() -> None:
    st.set_page_config(
        page_title=brand.NAME, page_icon=brand.FAVICON, layout="wide", initial_sidebar_state="auto"
    )
    st.html(stylesheet(ui.current_theme()))
    try:
        snap: LoadedSnapshot | None = load_snapshot()
    except SnapshotUnavailableError:
        snap = None
    pages = [
        st.Page(_page(render, snap), title=title, url_path=path, icon=icon, default=path == "overview")
        for title, path, icon, render in PAGES
    ]
    st.logo(brand.lockup_svg(ui.current_theme()), size="large")
    nav = st.navigation({"Performance": pages}, position="sidebar")
    with st.sidebar:
        ui.render(SIDE_NOTE)
    _sidebar_footer()
    st.set_page_config(page_title=f"{nav.title} - {brand.NAME}")
    _stale_notice(snap)
    nav.run()


if __name__ == "__main__":
    main()
