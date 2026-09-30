"""Console sign-in, per-run server-side session validation and sign-in redirects.

Every script run (every page, every interaction) calls `GET /auth/session` on config-api before anything
is rendered. Without a valid session the requested page (with its query parameters) is remembered and
the browser is redirected to the sign-in page; after password + TOTP the user returns to that page.
The session token and CSRF token live only in the server-side Streamlit session state.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Final

import streamlit as st

from hdt import brand
from hdt.console import components as ui
from hdt.console.api import (
    AccountLockedError,
    ApiError,
    ApiUnavailableError,
    ConfigApiClient,
    InvalidCredentialsError,
    NotAuthenticatedError,
    SessionInfo,
    ValidationFailedError,
    client_from_env,
    describe_error,
)

SIGN_IN_PATH: Final[str] = "sign-in"
_CLIENT_KEY: Final[str] = "_hdt_client"
_TARGET_KEY: Final[str] = "_hdt_return_to"

ClientFactory = Callable[[], ConfigApiClient]


def _browser_ip() -> str | None:
    try:
        return st.context.ip_address
    except Exception:
        return None


def _default_factory() -> ConfigApiClient:
    return client_from_env(forwarded_for=_browser_ip)


_factory: ClientFactory = _default_factory


def install_client_factory(factory: ClientFactory) -> None:
    """Replace how per-session clients are built (tests inject a fake config-api transport)."""
    global _factory
    _factory = factory


def get_client() -> ConfigApiClient:
    """The config-api client of this browser session (created on first use)."""
    client = st.session_state.get(_CLIENT_KEY)
    if not isinstance(client, ConfigApiClient):
        client = _factory()
        st.session_state[_CLIENT_KEY] = client
    return client


# --------------------------------------------------------------------------- gate


class GateState(Enum):
    AUTHENTICATED = "authenticated"
    SIGNED_OUT = "signed_out"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True)
class GateResult:
    state: GateState
    info: SessionInfo | None = None
    error: ApiError | None = None


def check_session(client: ConfigApiClient) -> GateResult:
    """Validate the session server-side (`GET /auth/session`); never trusts local state alone."""
    if client.session is None:
        return GateResult(GateState.SIGNED_OUT)
    try:
        info = client.validate_session()
    except NotAuthenticatedError as exc:
        return GateResult(GateState.SIGNED_OUT, error=exc)
    except ApiError as exc:
        return GateResult(GateState.UNAVAILABLE, error=exc)
    return GateResult(GateState.AUTHENTICATED, info=info)


@dataclass(frozen=True)
class ReturnTarget:
    url_path: str
    query: tuple[tuple[str, str], ...] = ()

    def query_dict(self) -> dict[str, str]:
        return dict(self.query)


class RouteAction(Enum):
    RENDER = "render"
    REDIRECT_TO_SIGN_IN = "redirect_to_sign_in"
    REDIRECT_TO_TARGET = "redirect_to_target"
    SHOW_UNAVAILABLE = "show_unavailable"


@dataclass(frozen=True)
class RouteDecision:
    action: RouteAction
    remember: ReturnTarget | None = None
    target: ReturnTarget | None = None


def decide_route(
    state: GateState,
    requested: ReturnTarget,
    *,
    remembered: ReturnTarget | None,
    default_path: str,
) -> RouteDecision:
    """Pure routing rule (unit-tested).

    - signed out, any page but sign-in: remember the page and redirect to sign-in;
    - signed out on sign-in: render it;
    - authenticated on sign-in: go back to the remembered page (or the default page);
    - authenticated elsewhere: render the page;
    - config-api unreachable: render nothing but an explicit unavailable state (never an unvalidated page).
    """
    on_sign_in = requested.url_path == SIGN_IN_PATH
    if state is GateState.UNAVAILABLE:
        return RouteDecision(RouteAction.SHOW_UNAVAILABLE)
    if state is GateState.SIGNED_OUT:
        if on_sign_in:
            return RouteDecision(RouteAction.RENDER)
        return RouteDecision(RouteAction.REDIRECT_TO_SIGN_IN, remember=requested)
    if on_sign_in:
        target = remembered if remembered is not None and remembered.url_path != SIGN_IN_PATH else None
        return RouteDecision(RouteAction.REDIRECT_TO_TARGET, target=target or ReturnTarget(default_path))
    return RouteDecision(RouteAction.RENDER)


def remember_target(target: ReturnTarget) -> None:
    st.session_state[_TARGET_KEY] = target


def remembered_target() -> ReturnTarget | None:
    value = st.session_state.get(_TARGET_KEY)
    return value if isinstance(value, ReturnTarget) else None


def clear_target() -> None:
    st.session_state.pop(_TARGET_KEY, None)


def current_query() -> tuple[tuple[str, str], ...]:
    return tuple((k, str(v)) for k, v in st.query_params.to_dict().items())


# --------------------------------------------------------------------------- sign-in page


def _target_title(target: ReturnTarget | None, titles: Mapping[str, str]) -> str | None:
    if target is None:
        return None
    event = target.query_dict().get("event")
    if target.url_path == "decisions" and event:
        return f"decision {event}"
    return titles.get(target.url_path)


def _toggle_theme() -> None:
    st.session_state[ui.THEME_KEY] = "light" if ui.current_theme() == "dark" else "dark"


def render_sign_in(titles: Mapping[str, str], default_path: str) -> None:
    """Sign-in page: username, password, 6-digit TOTP (mandatory on every sign-in)."""
    target = remembered_target()
    _, middle, _ = st.columns([1, 1.25, 1])
    with middle:
        ui.render(
            '<div class="brand brand-lg"><div class="brand-mark" aria-hidden="true"></div>'
            f'<div><span class="wordmark" role="img" aria-label="{brand.NAME}"></span>'
            '<span class="sub">Admin console</span></div></div>'
        )
        with st.form("hdt-sign-in", border=True, enter_to_submit=True):
            ui.render('<h1 tabindex="-1" class="form-title">Sign in to the admin console</h1>')
            wanted = _target_title(target, titles)
            if wanted and (target is None or target.url_path != default_path):
                ui.render(ui.callout(f"Sign in to open <b>{ui.esc(wanted)}</b>.", "info", icon_name="lock"))
            username = st.text_input("Username", autocomplete="username", key="hdt-login-user")
            password = st.text_input(
                "Password", type="password", autocomplete="current-password", key="hdt-login-pass"
            )
            totp = st.text_input(
                "6-digit TOTP code",
                max_chars=6,
                autocomplete="one-time-code",
                placeholder="000000",
                help="From your authenticator app. Required on every sign-in.",
                key="hdt-login-totp",
                validate=(ui.TOTP_PATTERN, "The code must be exactly 6 digits."),
            )
            submitted = st.form_submit_button(
                "Sign in", type="primary", icon=":material/lock:", use_container_width=True
            )
        ui.render(
            '<p class="login-note" style="margin-top:14px">'
            f"{ui.icon('info', small=True)}Reachable only over Tailscale or an SSH tunnel. "
            "5 failed attempts lock the account for 15 minutes. "
            "Sessions expire after 30 minutes of inactivity.</p>"
        )
        dark = ui.current_theme() == "dark"
        st.button(
            "Light theme" if dark else "Dark theme",
            key="hdt-theme-toggle",
            icon=":material/light_mode:" if dark else ":material/dark_mode:",
            type="tertiary",
            on_click=_toggle_theme,
        )
        if not submitted:
            return
        user = (username or "").strip()
        code = (totp or "").strip()
        if not user or not password:
            st.error("Enter your username and password.", icon=":material/error:")
            return
        if not ui.is_totp(code):
            st.error("The code must be exactly 6 digits.", icon=":material/error:")
            return
        client = get_client()
        try:
            with st.spinner("Signing in..."):
                client.login(user, password, code)
        except (
            InvalidCredentialsError,
            AccountLockedError,
            ValidationFailedError,
            ApiUnavailableError,
        ) as exc:
            st.error(describe_error(exc), icon=":material/error:")
            return
        except ApiError as exc:
            st.error(describe_error(exc), icon=":material/error:")
            return
        for key in ("hdt-login-pass", "hdt-login-totp"):
            st.session_state.pop(key, None)
        ui.notify("Signed in")
        st.rerun()


def render_unavailable(error: ApiError | None) -> None:
    detail = describe_error(error) if error is not None else "config-api did not answer."
    ui.render(
        ui.page_head(
            "Console unavailable", "The console validates your session with config-api on every page."
        )
        + ui.callout(
            f"<b>Cannot verify the session.</b> {ui.esc(detail)} "
            "Nothing is shown until the session is verified.",
            "neg",
            icon_name="alert",
        )
    )
    if st.button("Try again", icon=":material/refresh:", key="hdt-retry-session"):
        st.rerun()


def sign_out() -> None:
    client = get_client()
    try:
        client.logout()
    except ApiError:
        client.session = None
    st.session_state.pop(_CLIENT_KEY, None)
    clear_target()
    ui.notify("Signed out", "info")


# --------------------------------------------------------------------------- step-up


def with_step_up(client: ConfigApiClient, totp: str, action: Callable[[], str]) -> str:
    """Re-authenticate with a fresh TOTP (`POST /auth/step-up`), then run the protected action."""
    if not ui.is_totp(totp):
        raise ValidationFailedError(422, "invalid_totp", "The code must be exactly 6 digits.")
    client.step_up(totp)
    return action()
