"""An in-flight event and replay retain the exact config version selected at event start."""

from __future__ import annotations

import pytest
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from hdt.settings import store
from hdt.settings.schemas import ModeSection, Section
from hdt.settings.versions import load_pin, pin_current

pytestmark = [pytest.mark.pg, pytest.mark.integration]


def test_config_change_does_not_change_event_pin(pg_engine: Engine) -> None:
    with Session(pg_engine) as session, session.begin():
        initial = store.create_version(
            session,
            Section.MODE,
            ModeSection(mode="paper", size_multiplier=1.0),
            author="operator",
            reason="initial paper",
            parent_id=None,
        )
    with Session(pg_engine) as session:
        event_pin = pin_current(session)
        recorded_ids = event_pin.version_ids()
        assert recorded_ids == {"mode": initial.id}
    with Session(pg_engine) as session, session.begin():
        changed = store.create_version(
            session,
            Section.MODE,
            ModeSection(mode="testnet", size_multiplier=0.5),
            author="operator",
            reason="switch to testnet",
            parent_id=initial.id,
        )
    with Session(pg_engine) as session:
        assert pin_current(session).mode().mode.value == "testnet"
        replay = load_pin(session, recorded_ids)
        assert replay.mode().mode.value == "paper"
        assert replay.version_ids() == recorded_ids
        assert changed.id != event_pin.get(Section.MODE).id
        with pytest.raises(store.StaleParentError):
            store.create_version(
                session,
                Section.MODE,
                ModeSection(mode="paper", size_multiplier=1.0),
                author="stale editor",
                reason="stale form",
                parent_id=initial.id,
            )
