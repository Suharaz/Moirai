"""Deterministic exchange client ids: `base32(sha256(event_id))[:20]-{leg}-{seq}`, <= 36 characters."""

from __future__ import annotations

import re

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from hdt.contracts.common import Leg
from hdt.contracts.order import CLIENT_ID_PATTERN
from hdt.execution.client_ids import (
    MAX_SEQ,
    belongs_to,
    client_id,
    event_prefix,
    next_seq,
    parse_client_id,
)

BINANCE = re.compile(r"^[\.A-Z\:/a-z0-9_-]{1,36}$")


def test_client_id_shape() -> None:
    cid = client_id("evt-0001", Leg.ENTRY, 0)
    prefix, leg, seq = cid.split("-")
    assert len(prefix) == 20
    assert re.fullmatch(r"[A-Z2-7]{20}", prefix)
    assert (leg, seq) == ("entry", "0")


@settings(max_examples=200, deadline=None)
@given(
    event_id=st.text(min_size=1, max_size=200),
    leg=st.sampled_from(list(Leg)),
    seq=st.integers(min_value=0, max_value=MAX_SEQ),
)
def test_every_id_fits_binance_and_round_trips(event_id: str, leg: Leg, seq: int) -> None:
    cid = client_id(event_id, leg, seq)
    assert len(cid) <= 36
    assert BINANCE.fullmatch(cid)
    assert re.fullmatch(CLIENT_ID_PATTERN, cid)
    parsed = parse_client_id(cid)
    assert parsed is not None
    assert (parsed.prefix, parsed.leg, parsed.seq) == (event_prefix(event_id), leg, seq)
    assert belongs_to(cid, event_id)


def test_longest_leg_at_max_seq_is_exactly_36() -> None:
    assert len(client_id("evt", Leg.ENTRY_IOC, MAX_SEQ)) == 36
    with pytest.raises(ValueError, match="seq"):
        client_id("evt", Leg.ENTRY_IOC, MAX_SEQ + 1)
    with pytest.raises(ValueError, match="seq"):
        client_id("evt", Leg.SL, -1)


def test_deterministic_and_distinct_by_event_leg_and_seq() -> None:
    assert client_id("evt-1", Leg.SL, 0) == client_id("evt-1", Leg.SL, 0)
    ids = {client_id(e, leg, s) for e in ("evt-1", "evt-2") for leg in Leg for s in (0, 1)}
    assert len(ids) == 2 * len(Leg) * 2


def test_empty_event_id_is_refused() -> None:
    with pytest.raises(ValueError, match="event_id"):
        client_id("", Leg.SL, 0)


def test_next_seq_continues_from_the_ledger_per_event_and_leg() -> None:
    existing = [
        client_id("evt-1", Leg.SL, 0),
        client_id("evt-1", Leg.SL, 1),
        client_id("evt-1", Leg.TP1, 0),
        client_id("evt-2", Leg.SL, 5),
        "web_ABCDEF",  # exchange- or operator-generated ids are ignored
    ]
    assert next_seq(existing, "evt-1", Leg.SL) == 2
    assert next_seq(existing, "evt-1", Leg.TP1) == 1
    assert next_seq(existing, "evt-1", Leg.TRAIL) == 0
    assert next_seq([], "evt-3", Leg.ENTRY) == 0


def test_foreign_ids_do_not_parse() -> None:
    assert parse_client_id("web_ABCDEF") is None
    assert parse_client_id(f"{event_prefix('e')}-bogus-0") is None
    assert not belongs_to(client_id("evt-1", Leg.SL, 0), "evt-2")
