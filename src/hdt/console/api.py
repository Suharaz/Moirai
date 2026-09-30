"""config-api client used by the console (the console's only write path).

The console holds no DB write access and no private key: every configuration page calls config-api over
the internal network. The config-api session (cookie `hdt_session` + CSRF token) lives only in the
console's server-side Streamlit session state; it is never sent to the browser. Each request forwards the
browser IP in `X-Forwarded-For` so config-api can audit it (trusted only from the console address).

Errors are mapped to typed exceptions from the documented body `{"code", "message"}` (422 adds
`errors[{loc, msg}]`, the live gate adds `missing[]`).
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from http.cookies import CookieError, SimpleCookie
from typing import Any, Final, Literal

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from hdt.core.config import require_env_value

SESSION_COOKIE: Final[str] = "hdt_session"
CSRF_HEADER: Final[str] = "X-CSRF-Token"
CONFIGAPI_URL_ENV: Final[str] = "HDT_CONFIGAPI_URL"
DEFAULT_TIMEOUT_S: Final[float] = 10.0

Scope = Literal["data", "llm", "exec", "ops"]
Section = Literal["models", "cmc", "binance", "risk", "council", "mode", "golive"]
RunMode = Literal["paper", "testnet", "live"]
Account = Literal["paper", "testnet", "live"]
ControlAction = Literal["pause", "resume", "kill", "flatten"]


# --------------------------------------------------------------------------- errors


@dataclass(frozen=True)
class FieldError:
    loc: tuple[str | int, ...]
    msg: str

    @property
    def path(self) -> str:
        parts = [str(p) for p in self.loc if p not in ("body", "payload")]
        return ".".join(parts)


class ApiError(Exception):
    """Any failure talking to config-api."""

    def __init__(
        self,
        status: int,
        code: str,
        message: str,
        *,
        errors: Sequence[FieldError] = (),
        missing: Sequence[str] = (),
    ) -> None:
        super().__init__(f"{status} {code}: {message}")
        self.status = status
        self.code = code
        self.message = message
        self.errors = tuple(errors)
        self.missing = tuple(missing)


class NotAuthenticatedError(ApiError):
    """401 not_authenticated: no or expired session; the console must sign in again."""


class InvalidCredentialsError(ApiError):
    """401 invalid_credentials on sign-in or step-up."""


class AccountLockedError(ApiError):
    """423: 5 failed attempts lock the account for 15 minutes."""


class CsrfError(ApiError):
    """403 csrf_failed."""


class StepUpRequiredError(ApiError):
    """403 step_up_required: a fresh TOTP (valid 5 minutes) is needed for this action."""


class ForbiddenError(ApiError):
    """Any other 403."""


class StaleParentError(ApiError):
    """409 stale_parent: someone saved a newer version since this form was loaded."""


class LiveGateClosedError(ApiError):
    """409 live_gate_closed: `missing` lists the unmet live conditions."""


class ConflictError(ApiError):
    """Any other 409."""


class ValidationFailedError(ApiError):
    """422: `errors` carries one entry per invalid field."""


class NotFoundError(ApiError):
    """404: unknown resource or a route that does not exist yet."""


class ApiUnavailableError(ApiError):
    """Transport failure, timeout, 5xx or an unreadable response."""


def _field_errors(raw: object) -> list[FieldError]:
    out: list[FieldError] = []
    if not isinstance(raw, list):
        return out
    for item in raw:
        if not isinstance(item, Mapping):
            continue
        loc_raw = item.get("loc", ())
        loc = (
            tuple(p for p in loc_raw if isinstance(p, str | int)) if isinstance(loc_raw, list | tuple) else ()
        )
        out.append(FieldError(loc=loc, msg=str(item.get("msg", ""))))
    return out


def error_from_response(status: int, body: object) -> ApiError:
    """Map an error response to its typed exception (pure; unit-tested)."""
    data: Mapping[str, Any] = body if isinstance(body, Mapping) else {}
    detail = data.get("detail")
    if isinstance(detail, Mapping) and "code" in detail:
        data = detail  # FastAPI HTTPException(detail={...}) nests the documented body
    code = str(data.get("code") or f"http_{status}")
    message = str(data.get("message") or (detail if isinstance(detail, str) else "") or f"HTTP {status}")
    errors = _field_errors(
        data.get("errors") if "errors" in data else detail if isinstance(detail, list) else None
    )
    missing_raw = data.get("missing")
    missing = [str(m) for m in missing_raw] if isinstance(missing_raw, list) else []
    kwargs: dict[str, Any] = {"errors": errors, "missing": missing}
    if status == 401:
        cls: type[ApiError] = (
            InvalidCredentialsError if code == "invalid_credentials" else NotAuthenticatedError
        )
    elif status == 403:
        cls = {"step_up_required": StepUpRequiredError, "csrf_failed": CsrfError}.get(code, ForbiddenError)
    elif status == 404:
        cls = NotFoundError
    elif status == 409:
        cls = {"stale_parent": StaleParentError, "live_gate_closed": LiveGateClosedError}.get(
            code, ConflictError
        )
    elif status == 422:
        cls = ValidationFailedError
    elif status == 423:
        cls = AccountLockedError
    elif status >= 500:
        cls = ApiUnavailableError
    else:
        cls = ApiError
    return cls(status, code, message, **kwargs)


def describe_error(exc: ApiError) -> str:
    """One user-facing sentence for a toast or an inline alert."""
    if isinstance(exc, NotAuthenticatedError):
        return "Your session has expired. Sign in again."
    if isinstance(exc, InvalidCredentialsError):
        return "Wrong username, password or TOTP code."
    if isinstance(exc, AccountLockedError):
        return "The account is locked for 15 minutes after 5 failed attempts."
    if isinstance(exc, StepUpRequiredError):
        return "This action needs a fresh TOTP code."
    if isinstance(exc, CsrfError):
        return "The request was rejected (CSRF check). Reload the page and sign in again."
    if isinstance(exc, StaleParentError):
        return "A newer version was saved meanwhile. Reload the page to see it, then apply your change again."
    if isinstance(exc, LiveGateClosedError):
        missing = ", ".join(exc.missing) if exc.missing else exc.message
        return f"Live cannot be enabled yet: {missing}."
    if isinstance(exc, ValidationFailedError):
        if exc.errors:
            first = exc.errors[0]
            more = f" (+{len(exc.errors) - 1} more)" if len(exc.errors) > 1 else ""
            where = f"{first.path}: " if first.path else ""
            return f"Rejected by config-api: {where}{first.msg}{more}"
        return f"Rejected by config-api: {exc.message}"
    if isinstance(exc, ApiUnavailableError):
        return f"config-api is unavailable ({exc.message})."
    return f"config-api error {exc.status}: {exc.message}"


# --------------------------------------------------------------------------- response models


class _Model(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)


class SessionInfo(_Model):
    user: str
    csrf_token: str
    expires_at: datetime
    step_up_until: datetime | None = None


class StepUpResult(_Model):
    step_up_until: datetime


class ConfigVersion(_Model):
    """One immutable version of a config section (`version_dict` of config-api)."""

    id: int
    section: str
    payload: dict[str, Any]
    schema_version: int | None = None
    payload_sha256: str | None = None
    author: str | None = None
    reason: str | None = None
    created_at: datetime | None = None
    parent_id: int | None = None


class SectionSummary(_Model):
    """`GET /config/sections` row: the active version of one section (None before the first save)."""

    section: str
    active_version_id: int | None = None
    updated_at: datetime | None = None
    author: str | None = None


class VersionList(_Model):
    section: str
    active_version_id: int | None = None
    versions: tuple[ConfigVersion, ...] = ()


class CatalogPricing(_Model):
    prompt: float
    completion: float


class CatalogModel(_Model):
    slug: str
    name: str
    developer: str
    context_length: int | None = None
    pricing: CatalogPricing
    supported_parameters: tuple[str, ...] = ()


class ModelCatalog(_Model):
    fetched_at: datetime | None = None
    models: tuple[CatalogModel, ...] = ()


class ModelTestResult(_Model):
    status: Literal["pending", "done", "error"]
    schema_valid: bool | None = None
    latency_ms: float | None = None
    cost_usd: float | None = None
    provider: str | None = None
    model_returned: str | None = None
    error: str | None = None


class SecretRow(_Model):
    scope: str
    name: str
    version: int | None = None
    last4: str | None = None
    fingerprint: str | None = None
    status: Literal["pending", "active", "invalid", "retired"] | None = None
    reason: str | None = None
    checked_at: datetime | None = None
    created_by: str | None = None
    created_at: datetime | None = None
    details: dict[str, Any] = Field(default_factory=dict)


class SecretWriteResult(_Model):
    version: int
    status: str


class RouteProjection(_Model):
    route: str | int
    credits: float


class CmcProjection(_Model):
    monthly_projection: float
    quota: float
    fraction: float
    per_route: tuple[dict[str, Any], ...] = ()
    blocked: bool


class LiveGate(_Model):
    g4_passed: bool
    live_key_valid: bool
    checklist_complete: bool
    missing: tuple[str, ...] = ()


class ModeState(_Model):
    mode: RunMode
    size_multiplier: float
    live_gate: LiveGate


class ChecklistItem(_Model):
    id: str
    label: str | None = None
    checked: bool = False
    checked_by: str | None = None
    checked_at: datetime | None = None


class GoLiveChecklist(_Model):
    """`GET|POST /golive/checklist`: attribution (`checked_by`, `checked_at`) is set by the server."""

    version_id: int | None = None
    items: tuple[ChecklistItem, ...] = ()
    done: int = 0
    total: int = 0
    complete: bool = False


class ControlLogRow(_Model):
    id: int | str | None = None
    at: datetime | None = None
    user: str | None = None
    account: str | None = None
    action: str
    reason: str | None = None


class AuditRow(_Model):
    id: int | str | None = None
    at: datetime
    user: str | None = None
    ip: str | None = None
    action: str
    section: str | None = None
    diff_redacted: Any = None


# --------------------------------------------------------------------------- client


@dataclass
class ApiSession:
    token: str
    csrf_token: str
    user: str
    expires_at: datetime
    step_up_until: datetime | None = None


def _session_cookie(response: httpx.Response) -> str | None:
    for header in response.headers.get_list("set-cookie"):
        jar: SimpleCookie = SimpleCookie()
        try:
            jar.load(header)
        except CookieError:
            continue
        morsel = jar.get(SESSION_COOKIE)
        if morsel is not None and morsel.value:
            return morsel.value
    return None


_SEGMENT = re.compile(r"^[a-z0-9_]{1,64}$")


def _segment(value: str) -> str:
    """Path segments are fixed identifiers; refuse anything that could alter the route."""
    if not _SEGMENT.match(value):
        raise ValueError(f"invalid path segment {value!r}")
    return value


@dataclass
class ConfigApiClient:
    """Synchronous client (Streamlit scripts are synchronous). One instance per browser session."""

    base_url: str
    transport: httpx.BaseTransport | None = None
    timeout_s: float = DEFAULT_TIMEOUT_S
    forwarded_for: Callable[[], str | None] | None = None
    session: ApiSession | None = field(default=None)

    def __post_init__(self) -> None:
        self._http = httpx.Client(
            base_url=self.base_url.rstrip("/") + "/api",
            transport=self.transport,
            timeout=self.timeout_s,
            follow_redirects=False,
        )

    def close(self) -> None:
        self._http.close()

    # ---------------------------------------------------------------- transport

    def _headers(self, *, mutating: bool) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.forwarded_for is not None:
            ip = self.forwarded_for()
            if ip:
                headers["X-Forwarded-For"] = ip
        if self.session is not None:
            headers["Cookie"] = f"{SESSION_COOKIE}={self.session.token}"
            if mutating:
                headers[CSRF_HEADER] = self.session.csrf_token
        return headers

    def _request(
        self,
        method: str,
        path: str,
        *,
        json: object | None = None,
        params: Mapping[str, Any] | None = None,
    ) -> httpx.Response:
        mutating = method != "GET"
        clean_params = {k: v for k, v in (params or {}).items() if v not in (None, "")}
        try:
            response = self._http.request(
                method, path, json=json, params=clean_params or None, headers=self._headers(mutating=mutating)
            )
        except httpx.TimeoutException as exc:
            raise ApiUnavailableError(0, "timeout", f"no answer within {self.timeout_s:.0f} s") from exc
        except httpx.HTTPError as exc:
            raise ApiUnavailableError(0, "transport", type(exc).__name__) from exc
        if response.status_code >= 400:
            try:
                body: object = response.json()
            except ValueError:
                body = {"message": response.text[:200]}
            error = error_from_response(response.status_code, body)
            if isinstance(error, NotAuthenticatedError):
                self.session = None
            raise error
        return response

    def _json(self, response: httpx.Response) -> Any:
        if response.status_code == 204 or not response.content:
            return None
        try:
            return response.json()
        except ValueError as exc:
            raise ApiUnavailableError(response.status_code, "bad_response", "response is not JSON") from exc

    def _parse[M: BaseModel](self, model: type[M], data: object) -> M:
        try:
            return model.model_validate(data)
        except ValidationError as exc:
            raise ApiUnavailableError(200, "bad_response", f"unexpected {model.__name__} shape") from exc

    def _get(self, path: str, params: Mapping[str, Any] | None = None) -> Any:
        return self._json(self._request("GET", path, params=params))

    def _post(self, path: str, body: object | None = None) -> Any:
        return self._json(self._request("POST", path, json=body if body is not None else {}))

    # ---------------------------------------------------------------- auth

    def login(self, username: str, password: str, totp: str) -> ApiSession:
        self.session = None
        response = self._request(
            "POST", "/auth/login", json={"username": username, "password": password, "totp": totp}
        )
        token = _session_cookie(response)
        if token is None:
            raise ApiUnavailableError(
                response.status_code, "bad_response", "sign-in returned no session cookie"
            )
        info = self._parse(SessionInfo, self._json(response))
        self.session = ApiSession(
            token=token,
            csrf_token=info.csrf_token,
            user=info.user,
            expires_at=info.expires_at,
            step_up_until=info.step_up_until,
        )
        return self.session

    def logout(self) -> None:
        if self.session is None:
            return
        try:
            self._request("POST", "/auth/logout", json={})
        finally:
            self.session = None

    def validate_session(self) -> SessionInfo:
        """`GET /auth/session`; raises `NotAuthenticatedError` (and forgets the session) when invalid."""
        if self.session is None:
            raise NotAuthenticatedError(401, "not_authenticated", "no session")
        info = self._parse(SessionInfo, self._get("/auth/session"))
        self.session.csrf_token = info.csrf_token
        self.session.user = info.user
        self.session.expires_at = info.expires_at
        self.session.step_up_until = info.step_up_until
        return info

    def step_up(self, totp: str) -> datetime:
        result = self._parse(StepUpResult, self._post("/auth/step-up", {"totp": totp}))
        if self.session is not None:
            self.session.step_up_until = result.step_up_until
        return result.step_up_until

    # ---------------------------------------------------------------- config sections

    def config_sections(self) -> list[SectionSummary]:
        data = self._get("/config/sections")
        items = data.get("sections") if isinstance(data, Mapping) else None
        if not isinstance(items, list):
            raise ApiUnavailableError(200, "bad_response", "unexpected sections shape")
        return [self._parse(SectionSummary, i) for i in items]

    def get_config(self, section: Section) -> ConfigVersion | None:
        """Active version of `section`, or None when the section has never been saved."""
        data = self._get(f"/config/{_segment(section)}")
        if not isinstance(data, Mapping) or "active" not in data:
            raise ApiUnavailableError(200, "bad_response", "unexpected section shape")
        active = data["active"]
        return None if active is None else self._parse(ConfigVersion, active)

    def config_versions(self, section: Section, limit: int = 50) -> VersionList:
        return self._parse(VersionList, self._get(f"/config/{_segment(section)}/versions", {"limit": limit}))

    def config_schema(self, section: Section) -> dict[str, Any]:
        data = self._get(f"/config/{_segment(section)}/schema")
        if not isinstance(data, Mapping):
            raise ApiUnavailableError(200, "bad_response", "schema is not an object")
        return dict(data)

    def save_config(
        self, section: Section, payload: Mapping[str, Any], reason: str, parent_id: int | None
    ) -> ConfigVersion:
        body = {"payload": dict(payload), "reason": reason, "parent_id": parent_id}
        return self._parse(ConfigVersion, self._post(f"/config/{_segment(section)}", body))

    def rollback(self, section: Section, version_id: int, reason: str) -> ConfigVersion:
        body = {"version_id": version_id, "reason": reason}
        return self._parse(ConfigVersion, self._post(f"/config/{_segment(section)}/rollback", body))

    # ---------------------------------------------------------------- models

    def model_catalog(self, gateway: str = "openrouter") -> ModelCatalog:
        return self._parse(ModelCatalog, self._get("/models/catalog", {"gateway": gateway}))

    def start_model_test(self, role: str, config: Mapping[str, Any]) -> str:
        data = self._post("/models/test", {"role": _segment(role), "config": dict(config)})
        request_id = data.get("request_id") if isinstance(data, Mapping) else None
        if not isinstance(request_id, str) or not request_id:
            raise ApiUnavailableError(200, "bad_response", "model test returned no request_id")
        return request_id

    def model_test_result(self, request_id: str) -> ModelTestResult:
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", request_id):
            raise ValueError("invalid request id")
        return self._parse(ModelTestResult, self._get(f"/models/test/{request_id}"))

    # ---------------------------------------------------------------- secrets

    def secrets(self) -> list[SecretRow]:
        data = self._get("/secrets")
        items = data.get("secrets", data) if isinstance(data, Mapping) else data
        if not isinstance(items, list):
            raise ApiUnavailableError(200, "bad_response", "unexpected secrets shape")
        return [self._parse(SecretRow, i) for i in items]

    def public_keys(self) -> dict[str, str]:
        data = self._get("/secrets/public-keys")
        if not isinstance(data, Mapping):
            raise ApiUnavailableError(200, "bad_response", "public keys is not an object")
        return {str(k): str(v) for k, v in data.items()}

    def write_secret(
        self, scope: Scope, name: str, *, sealed_blob: str, last4: str, fingerprint: str
    ) -> SecretWriteResult:
        body = {"sealed_blob": sealed_blob, "last4": last4, "fingerprint": fingerprint}
        return self._parse(
            SecretWriteResult, self._post(f"/secrets/{_segment(scope)}/{_segment(name)}", body)
        )

    def retest_secret(self, scope: Scope, name: str) -> dict[str, Any]:
        data = self._post(f"/secrets/{_segment(scope)}/{_segment(name)}/test", {})
        return dict(data) if isinstance(data, Mapping) else {}

    # ---------------------------------------------------------------- cmc

    def cmc_projection(self, payload: Mapping[str, Any]) -> CmcProjection:
        return self._parse(CmcProjection, self._post("/cmc/projection", {"payload": dict(payload)}))

    # ---------------------------------------------------------------- mode and go-live

    def get_mode(self) -> ModeState:
        return self._parse(ModeState, self._get("/mode"))

    def set_mode(self, mode: RunMode, size_multiplier: float, reason: str) -> ModeState:
        body = {"mode": mode, "size_multiplier": size_multiplier, "reason": reason}
        return self._parse(ModeState, self._post("/mode", body))

    def golive_checklist(self) -> GoLiveChecklist:
        return self._parse(GoLiveChecklist, self._get("/golive/checklist"))

    def save_golive_checklist(self, checked: Mapping[str, bool], reason: str) -> GoLiveChecklist:
        """Send the checked state of every item; the server attributes newly checked items."""
        items = [{"id": item_id, "checked": value} for item_id, value in checked.items()]
        return self._parse(
            GoLiveChecklist, self._post("/golive/checklist", {"items": items, "reason": reason})
        )

    # ---------------------------------------------------------------- controls

    def control(
        self, account: Account, action: ControlAction, reason: str, confirm_text: str = ""
    ) -> dict[str, Any]:
        body = {"account": account, "action": action, "reason": reason, "confirm_text": confirm_text}
        data = self._post("/controls", body)
        return dict(data) if isinstance(data, Mapping) else {}

    def controls_log(self, limit: int = 50) -> list[ControlLogRow]:
        data = self._get("/controls/log", {"limit": limit})
        items = data.get("items", data) if isinstance(data, Mapping) else data
        if not isinstance(items, list):
            raise ApiUnavailableError(200, "bad_response", "unexpected controls log shape")
        return [self._parse(ControlLogRow, i) for i in items]

    # ---------------------------------------------------------------- audit

    def audit(
        self,
        *,
        section: str | None = None,
        user: str | None = None,
        action: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int = 200,
    ) -> list[AuditRow]:
        params = {
            "section": section,
            "user": user,
            "action": action,
            "since": since.isoformat() if since else None,
            "until": until.isoformat() if until else None,
            "limit": limit,
        }
        data = self._get("/audit", params)
        items = data.get("items", data) if isinstance(data, Mapping) else data
        if not isinstance(items, list):
            raise ApiUnavailableError(200, "bad_response", "unexpected audit shape")
        return [self._parse(AuditRow, i) for i in items]

    # ---------------------------------------------------------------- lessons (routes arrive with phase 08)

    def lessons_route_available(self) -> bool:
        """True once config-api serves the lesson review routes (phase 08); 404 means not yet."""
        try:
            self._get("/lessons", {"limit": 1})
        except NotFoundError:
            return False
        return True

    def approve_lesson(self, lesson_id: str, note: str, *, expected_state: str) -> dict[str, Any]:
        """`expected_state` is the state the reviewer saw; a concurrent review makes the API answer 409."""
        body = {"note": note, "expected_state": expected_state}
        data = self._post(f"/lessons/{_lesson_id(lesson_id)}/approve", body)
        return dict(data) if isinstance(data, Mapping) else {}

    def retire_lesson(self, lesson_id: str, note: str, *, expected_state: str) -> dict[str, Any]:
        """`expected_state` is the state the reviewer saw; a concurrent review makes the API answer 409."""
        body = {"note": note, "expected_state": expected_state}
        data = self._post(f"/lessons/{_lesson_id(lesson_id)}/retire", body)
        return dict(data) if isinstance(data, Mapping) else {}


def _lesson_id(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", value):
        raise ValueError(f"invalid lesson id {value!r}")
    return value


def client_from_env(forwarded_for: Callable[[], str | None] | None = None) -> ConfigApiClient:
    """Production client: base URL from `HDT_CONFIGAPI_URL` (internal network address of config-api)."""
    return ConfigApiClient(base_url=require_env_value(CONFIGAPI_URL_ENV), forwarded_for=forwarded_for)
