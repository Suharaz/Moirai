"""Canonical JSON and hashing: the only canonical-JSON implementation in the repository.

Rules (stable across processes, versions and a Postgres JSONB round-trip):
- keys sorted, separators `,` and `:`, UTF-8, no NaN/Infinity;
- integral floats render as integers built from their shortest round-trip digits (`2.0` -> `2`,
  `4.2e17` -> `420000000000000000`, `1.2345678901234567e20` -> `123456789012345670000`), which is exactly
  what Postgres JSONB returns after storing the float's JSON text as `numeric`;
- other floats render with Python's shortest round-trip `repr`; `-0.0` becomes `0`;
- datetimes must be aware and render in UTC as `YYYY-MM-DDTHH:MM:SS.ffffffZ`;
- `Decimal` renders as a plain decimal string without exponent or trailing zeros, independent of the
  active decimal context;
- enums render as their value; sets are sorted; Pydantic models are dumped by field name.
"""

from __future__ import annotations

import base64
import hashlib
import json
import math
from collections.abc import Mapping
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from pydantic import BaseModel

type JSONValue = bool | int | float | str | list[JSONValue] | dict[str, JSONValue] | None


def decimal_str(value: Decimal) -> str:
    """Exact plain rendering: no exponent, no trailing fractional zeros, no rounding."""
    if not value.is_finite():
        raise ValueError("non-finite Decimal cannot be serialized")
    if value.is_zero():
        return "0"
    sign, digit_tuple, exponent = value.as_tuple()
    assert isinstance(exponent, int)
    digits = "".join(str(d) for d in digit_tuple).lstrip("0") or "0"
    while exponent < 0 and digits.endswith("0"):
        digits = digits[:-1]
        exponent += 1
    if exponent >= 0:
        text = digits + "0" * exponent
    else:
        places = -exponent
        text = (
            digits[:-places] + "." + digits[-places:]
            if len(digits) > places
            else "0." + "0" * (places - len(digits)) + digits
        )
    return ("-" if sign else "") + text


def utc_timestamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("naive datetime cannot be serialized canonically")
    return value.astimezone(UTC).replace(tzinfo=None).isoformat(timespec="microseconds") + "Z"


def to_canonical(value: Any) -> JSONValue:
    """Convert `value` into plain JSON types following the canonical rules."""
    if isinstance(value, BaseModel):
        return to_canonical(value.model_dump(mode="python"))
    if value is None or isinstance(value, bool | str):
        return value
    if isinstance(value, Enum):
        return to_canonical(value.value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("NaN/Infinity cannot be serialized canonically")
        return int(Decimal(repr(value))) if value.is_integer() else value
    if isinstance(value, Decimal):
        return decimal_str(value)
    if isinstance(value, datetime):
        return utc_timestamp(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        out: dict[str, JSONValue] = {}
        for key, item in value.items():
            if isinstance(key, Enum):
                key = key.value
            if not isinstance(key, str):
                raise TypeError(f"canonical JSON keys must be strings, got {type(key).__name__}")
            out[key] = to_canonical(item)
        return out
    if isinstance(value, list | tuple):
        return [to_canonical(item) for item in value]
    if isinstance(value, set | frozenset):
        items = [to_canonical(item) for item in value]
        return sorted(items, key=lambda item: json.dumps(item, sort_keys=True))
    raise TypeError(f"type {type(value).__name__} is not canonically serializable")


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        to_canonical(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def sha256_hex(data: bytes | str) -> str:
    raw = data.encode("utf-8") if isinstance(data, str) else data
    return hashlib.sha256(raw).hexdigest()


def canonical_sha256(value: Any) -> str:
    return sha256_hex(canonical_json(value))


def b32_digest(data: bytes | str, length: int) -> str:
    """Upper-case RFC 4648 base32 of sha256(data) without padding, truncated to `length` chars."""
    if not 1 <= length <= 52:
        raise ValueError("length must be between 1 and 52")
    raw = data.encode("utf-8") if isinstance(data, str) else data
    return base64.b32encode(hashlib.sha256(raw).digest()).decode("ascii").rstrip("=")[:length]
