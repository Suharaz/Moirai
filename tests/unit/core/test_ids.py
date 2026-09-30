from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal, localcontext

import pytest
from hypothesis import given
from hypothesis import strategies as st

from hdt.core.ids import b32_digest, canonical_json, canonical_sha256


def test_key_order_does_not_change_the_hash() -> None:
    assert canonical_sha256({"a": 1, "b": [1, 2]}) == canonical_sha256({"b": [1, 2], "a": 1})


def test_decimal_representations_of_the_same_value_hash_equal() -> None:
    assert canonical_json({"p": Decimal("1.10")}) == canonical_json({"p": Decimal("1.1")})
    assert canonical_json({"p": Decimal("1E+2")}) == b'{"p":"100"}'
    assert canonical_json({"p": Decimal("-0.000")}) == b'{"p":"0"}'


def test_same_instant_in_different_timezones_is_identical() -> None:
    utc = datetime(2026, 9, 27, 10, 0, tzinfo=UTC)
    tokyo = utc.astimezone(timezone(timedelta(hours=9)))
    assert canonical_json({"t": utc}) == canonical_json({"t": tokyo})


def test_naive_datetime_and_nan_are_rejected() -> None:
    with pytest.raises(ValueError, match="naive"):
        canonical_json({"t": datetime(2026, 9, 27)})  # noqa: DTZ001
    with pytest.raises(ValueError, match="NaN"):
        canonical_json({"x": float("nan")})


def test_negative_zero_float_is_normalized() -> None:
    assert canonical_json([-0.0]) == canonical_json([0.0])


@given(st.dictionaries(st.text(min_size=1), st.integers() | st.floats(allow_nan=False, allow_infinity=False)))
def test_canonical_json_is_deterministic(value: dict[str, float]) -> None:
    assert canonical_json(value) == canonical_json(dict(reversed(list(value.items()))))


def test_b32_digest_is_uppercase_base32_and_truncated() -> None:
    digest = b32_digest("event-1", 20)
    assert len(digest) == 20
    assert set(digest) <= set("ABCDEFGHIJKLMNOPQRSTUVWXYZ234567")
    assert b32_digest("event-1", 20) != b32_digest("event-2", 20)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (2.0, b"2"),
        (4.2e17, b"420000000000000000"),
        (1e22, b"10000000000000000000000"),
        (1.2345678901234567e20, b"123456789012345670000"),
        (1.5e-07, b"1.5e-07"),
    ],
)
def test_floats_render_like_their_postgres_jsonb_roundtrip(value: float, expected: bytes) -> None:
    assert canonical_json(value) == expected
    assert canonical_json(json.loads(expected)) == expected


def test_decimal_rendering_is_exact_and_context_independent() -> None:
    long = Decimal("1.00000000000000000000000000000001")
    assert canonical_json(long) == b'"1.00000000000000000000000000000001"'
    assert canonical_json(long) != canonical_json(Decimal("1"))
    with localcontext() as ctx:
        ctx.prec = 3
        assert canonical_json(Decimal("123456.7890")) == b'"123456.789"'


def test_timestamps_pad_years_below_1000() -> None:
    assert canonical_json(datetime(999, 1, 2, tzinfo=UTC)) == b'"0999-01-02T00:00:00.000000Z"'
