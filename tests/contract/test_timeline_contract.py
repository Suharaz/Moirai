"""Decision-card timeline producer contract: `stage` is mandatory; the public card keeps every known stage."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from hdt.contracts import DecisionTimelineEntry
from hdt.public.publisher import public_timeline

AT = datetime(2026, 9, 27, 10, 0, tzinfo=UTC)
SCHEMA = Path(__file__).resolve().parents[2] / "src" / "hdt" / "public" / "snapshot_schema.json"


def _entry(stage: str, text: str, tone: str = "") -> dict[str, object]:
    return DecisionTimelineEntry.model_validate(
        {"at": AT, "stage": stage, "text": text, "tone": tone}
    ).model_dump(mode="json")


def test_conforming_producer_entries_reach_the_public_timeline_whatever_their_stage() -> None:
    entries = [
        _entry("scanner", "Scanner emitted a LTX candidate"),
        _entry("council", "Round 2 closed", "warn"),
        _entry("risk", "Risk: approved, sizing 0.8% of equity"),
        _entry("orders", "Orders sent: entry and STOP at 1.701", "pos"),
        _entry("outcome", "Closed at time stop", "pos"),
    ]
    published = public_timeline(entries)
    assert [e["text"] for e in published] == [
        "Scanner emitted a LTX candidate",
        "Round 2 closed",
        "Risk: approved, sizing 0.8% of equity",
        "Orders sent: entry and STOP at 1.701",
        "Closed at time stop",
    ]
    assert published[0] == {
        "at": "2026-09-27T10:00:00.000000Z",
        "text": "Scanner emitted a LTX candidate",
        "tone": "",
    }


def test_a_long_timeline_publishes_only_its_last_entries_within_the_schema_cap() -> None:
    # 120 entries on one card used to exceed `maxItems` and reject every snapshot until the card aged
    # out of the last 1000 decisions (review m4).
    entries = []
    for n in range(60):
        entries.append(
            DecisionTimelineEntry.model_validate(
                {"at": AT + timedelta(minutes=n), "stage": "council", "text": f"round {n}"}
            ).model_dump(mode="json")
        )
        entries.append(_entry("risk", f"risk check {n}"))
    published = public_timeline(entries)
    cap = json.loads(SCHEMA.read_text(encoding="utf-8"))["$defs"]["decision_card"]["properties"]["timeline"]
    assert len(published) == cap["maxItems"]
    assert [e["text"] for e in published] == [
        text for n in range(35, 60) for text in (f"round {n}", f"risk check {n}")
    ]


@pytest.mark.parametrize(
    "raw",
    [
        {"at": AT, "text": "Orders sent: entry and STOP at 1.701"},  # no stage
        {"at": AT, "stage": "sizing", "text": "Sizing 0.8%"},  # unknown stage
        {"at": "2026-09-27T10:00:00", "stage": "scanner", "text": "naive timestamp"},
        {"at": AT, "stage": "scanner", "text": ""},
    ],
)
def test_entries_outside_the_contract_are_refused(raw: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        DecisionTimelineEntry.model_validate(raw)
