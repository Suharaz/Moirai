"""Uniform error bodies: `{"code", "message"}` plus `errors` (422) or other structured fields."""

from __future__ import annotations

import re
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import ValidationError

_FIELD_PROBLEM = re.compile(r"^([a-z_][a-z0-9_]*) must ")


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str, **extra: Any) -> None:
        super().__init__(message)
        self.status, self.code, self.message, self.extra = status, code, message, extra

    def body(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, **self.extra}


def error_list(
    exc: ValidationError | ValueError, prefix: tuple[str | int, ...] = ("payload",)
) -> list[dict[str, Any]]:
    """`[{loc, msg}]` for a payload that failed validation; never echoes input values."""
    if isinstance(exc, ValidationError):
        errors: list[dict[str, Any]] = []
        for err in exc.errors(include_input=False, include_url=False):
            loc = [*prefix, *err["loc"]]
            msg = str(err["msg"]).removeprefix("Value error, ")
            errors.extend(_split_problems(loc, msg))
        return errors
    return _split_problems(list(prefix), str(exc))


def _split_problems(loc: list[str | int], msg: str) -> list[dict[str, Any]]:
    """Ceiling violations join several `field must ...` problems with '; ': give each its own loc."""
    parts = [part.strip() for part in msg.split("; ") if part.strip()]
    if len(parts) > 1 or (parts and _FIELD_PROBLEM.match(parts[0]) and len(loc) <= 1):
        out = []
        for part in parts:
            match = _FIELD_PROBLEM.match(part)
            out.append({"loc": [*loc, match.group(1)] if match else loc, "msg": part})
        return out
    return [{"loc": loc, "msg": msg}]


def validation_error(
    exc: ValidationError | ValueError, prefix: tuple[str | int, ...] = ("payload",)
) -> ApiError:
    return ApiError(422, "validation_error", "the payload is invalid", errors=error_list(exc, prefix))


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def _api_error(_request: Request, exc: ApiError) -> JSONResponse:
        return JSONResponse(status_code=exc.status, content=exc.body())

    @app.exception_handler(RequestValidationError)
    async def _request_invalid(_request: Request, exc: RequestValidationError) -> JSONResponse:
        errors = [
            {
                "loc": list(err.get("loc", ())),
                "msg": str(err.get("msg", "invalid")).removeprefix("Value error, "),
            }
            for err in exc.errors()
        ]
        return JSONResponse(
            status_code=422,
            content={"code": "validation_error", "message": "the request is invalid", "errors": errors},
        )
