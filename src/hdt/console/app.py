"""Admin console entry point: `uv run streamlit run src/hdt/console/app.py`.

Runs on every script run, before any page: applies the theme, validates the config-api session
server-side (`GET /auth/session`), redirects to sign-in (remembering the requested page) when there is no
valid session, renders the top bar and the grouped navigation, then runs the requested page.
The console holds no DB write access and no private key.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Final

import streamlit as st

from hdt import brand
from hdt.console import auth
from hdt.console import components as ui
from hdt.console.api import ApiError, ModeState, SectionSummary
from hdt.console.readmodels import ReadModels, RunStatus
from hdt.console.theme import stylesheet
from hdt.core.logging import configure_logging

DEFAULT_PATH: Final[str] = "overview"


@dataclass(frozen=True)
class PageDef:
    path: str
    title: str
    url_path: str
    icon: str


NAV: Final[tuple[tuple[str, tuple[PageDef, ...]], ...]] = (
    (
        "Performance",
        (
            PageDef("views/01_Overview.py", "Overview", "overview", ":material/dashboard:"),
            PageDef("views/02_Positions.py", "Positions", "positions", ":material/account_balance_wallet:"),
            PageDef("views/03_Decisions.py", "Decisions", "decisions", ":material/balance:"),
            PageDef("views/04_Agents.py", "Agents", "agents", ":material/smart_toy:"),
            PageDef("views/05_AI_Costs.py", "AI costs", "ai-costs", ":material/attach_money:"),
        ),
    ),
    (
        "Internal",
        (PageDef("views/06_Data_Credits.py", "Data & credits", "data-credits", ":material/database:"),),
    ),
    (
        "Configuration",
        (
            PageDef("views/07_Models.py", "Models", "models", ":material/memory:"),
            PageDef("views/08_API_Keys.py", "API keys", "api-keys", ":material/key:"),
            PageDef("views/09_CMC.py", "CoinMarketCap", "cmc", ":material/show_chart:"),
            PageDef("views/10_Binance.py", "Binance & universe", "binance", ":material/toll:"),
            PageDef("views/11_Risk.py", "Risk", "risk", ":material/shield:"),
            PageDef("views/12_Council.py", "Council", "council", ":material/groups:"),
            PageDef("views/13_Run_Mode.py", "Run mode", "run-mode", ":material/toggle_on:"),
        ),
    ),
    (
        "Operations",
        (
            PageDef("views/14_Controls.py", "Controls", "controls", ":material/power_settings_new:"),
            PageDef("views/15_Lessons.py", "Lessons", "lessons", ":material/menu_book:"),
            PageDef("views/16_Audit.py", "Audit & versions", "audit", ":material/list:"),
        ),
    ),
)

TITLES: Final[dict[str, str]] = {p.url_path: p.title for _, group in NAV for p in group}


@st.cache_resource
def _logging_ready() -> bool:
    configure_logging("console")
    return True


def _toggle_theme() -> None:
    st.session_state[ui.THEME_KEY] = "light" if ui.current_theme() == "dark" else "dark"


def _build_pages(lessons_label: str) -> tuple[dict[str, list[st.Page]], dict[str, st.Page]]:
    sections: dict[str, list[st.Page]] = {}
    by_path: dict[str, st.Page] = {}
    for section, defs in NAV:
        pages: list[st.Page] = []
        for d in defs:
            title = lessons_label if d.url_path == "lessons" else d.title
            page = st.Page(
                str(Path(__file__).parent / d.path),
                title=title,
                url_path=d.url_path,
                icon=d.icon,
                default=d.url_path == DEFAULT_PATH,
            )
            pages.append(page)
            by_path[d.url_path] = page
        sections[section] = pages
    return sections, by_path


# --------------------------------------------------------------------------- top bar

_CLOCK_JS: Final[str] = (
    '<span class="top-meta mono" id="hdt-clock" aria-label="UTC time"></span>'
    "<script>(function(){var el=document.getElementById('hdt-clock');if(!el)return;"
    "function t(){el.textContent=new Date().toISOString().slice(11,19)+' UTC';}"
    "t();if(window.__hdtClock)clearInterval(window.__hdtClock);window.__hdtClock=setInterval(t,1000);})();</script>"
)


def _mode_pill(mode: ModeState | None, error: ApiError | None) -> str:
    if mode is None:
        detail = f" ({error.code})" if error is not None else ""
        return (
            f'<span class="status-pill pill-mute" title="Run mode unavailable{ui.esc(detail)}">'
            '<span class="dot"></span>MODE ?</span>'
        )
    kind = {"paper": "pill-info", "testnet": "pill-warn", "live": "pill-neg"}[mode.mode]
    mult = f" x{mode.size_multiplier:g}" if mode.mode == "live" else ""
    return (
        f'<span class="status-pill {kind}" title="Run mode"><span class="dot"></span>'
        f"{mode.mode.upper()}{mult}</span>"
    )


def _status_pill(status: RunStatus) -> str:
    kind = {"running": "pill-pos", "paused": "pill-warn", "killed": "pill-neg", "unknown": "pill-mute"}[
        status.state
    ]
    title = ui.esc(status.detail)
    return (
        f'<span class="status-pill {kind}" role="status" title="{title}">'
        f'<span class="dot"></span>{ui.esc(status.label)}</span>'
    )


def _config_version_label(sections: list[SectionSummary]) -> str:
    """Config version ids share one sequence across sections, so the newest active id is the latest change."""
    ids = [s.active_version_id for s in sections if s.active_version_id is not None]
    return f"config v{max(ids)}" if ids else "config not saved yet"


def _render_top_bar(user: str, models: ReadModels) -> None:
    client = auth.get_client()
    mode: ModeState | None = None
    mode_error: ApiError | None = None
    try:
        mode = ui.session_cached(ui.TOP_MODE_CACHE, timedelta(seconds=10), client.get_mode)
    except ApiError as exc:
        mode_error = exc
    try:
        sections = ui.session_cached(ui.TOP_SECTIONS_CACHE, timedelta(seconds=10), client.config_sections)
        version = _config_version_label(sections)
    except ApiError:
        version = "config version unavailable"
    account = mode.mode if mode is not None else "paper"
    status = models.run_status(account)
    initials = "".join(part[:1] for part in user.replace("_", " ").split()[:2]).upper() or "?"
    with st.container(key="hdt-topbar", horizontal=True, vertical_alignment="center", gap="small"):
        ui.render(
            '<div class="topbar">'
            f"{_mode_pill(mode, mode_error)}{_status_pill(status)}"
            f'<span class="top-meta mono hide-sm">{ui.esc(version)}</span></div>'
        )
        st.html(f'<div class="hdt">{_CLOCK_JS}</div>', unsafe_allow_javascript=True, width="content")
        ui.render(
            f'<span class="user"><span class="avatar" aria-hidden="true">{ui.esc(initials)}</span>'
            f"<span>{ui.esc(user)}</span></span>"
        )
        dark = ui.current_theme() == "dark"
        st.button(
            "",
            key="hdt-theme-toggle",
            icon=":material/light_mode:" if dark else ":material/dark_mode:",
            help="Toggle light or dark theme",
            on_click=_toggle_theme,
        )
        if st.button("", key="hdt-sign-out", icon=":material/logout:", help="Sign out"):
            auth.sign_out()
            st.rerun()


# --------------------------------------------------------------------------- main


def main() -> None:
    _logging_ready()
    st.set_page_config(
        page_title=f"{brand.NAME} console",
        page_icon=brand.FAVICON,
        layout="wide",
        initial_sidebar_state="auto",
    )
    st.html(stylesheet(ui.current_theme()))
    client = auth.get_client()
    gate = auth.check_session(client)
    authenticated = gate.state is auth.GateState.AUTHENTICATED
    # Read models only after the server-side session check: a signed-out request never touches Postgres.
    models = ui.read_models() if authenticated else None
    sections, by_path = _build_pages(models.lessons_nav_label() if models is not None else "Lessons")
    sign_in = st.Page(
        lambda: auth.render_sign_in(TITLES, DEFAULT_PATH),
        title="Sign in",
        url_path=auth.SIGN_IN_PATH,
        icon=":material/lock:",
        visibility="hidden",
    )
    all_pages = [p for group in sections.values() for p in group]
    if authenticated:
        st.logo(brand.lockup_svg(ui.current_theme()), size="large")
        nav = st.navigation({**sections, "": [sign_in]}, position="sidebar")
    else:
        nav = st.navigation([*all_pages, sign_in], position="hidden")

    requested = auth.ReturnTarget(nav.url_path or DEFAULT_PATH, auth.current_query())
    decision = auth.decide_route(
        gate.state, requested, remembered=auth.remembered_target(), default_path=DEFAULT_PATH
    )
    st.set_page_config(page_title=f"{nav.title} - {brand.NAME} console")

    if decision.action is auth.RouteAction.SHOW_UNAVAILABLE:
        auth.render_unavailable(gate.error)
        return
    if decision.action is auth.RouteAction.REDIRECT_TO_SIGN_IN:
        assert decision.remember is not None
        auth.remember_target(decision.remember)
        st.switch_page(sign_in)
    if decision.action is auth.RouteAction.REDIRECT_TO_TARGET:
        assert decision.target is not None
        auth.clear_target()
        page = by_path.get(decision.target.url_path, by_path[DEFAULT_PATH])
        st.switch_page(page, query_params=decision.target.query_dict() or None)

    ui.drain_notifications()
    if models is not None and gate.info is not None:
        _render_top_bar(gate.info.user, models)
    nav.run()


main()
