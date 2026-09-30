"""A change to a role's `fallback_models` alone creates a new agent version (config-api and council alike)."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import Any

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import Session, sessionmaker

from hdt.agents.versions import AgentVersionBook, VersionKey
from hdt.contracts.common import AgentName
from hdt.core.ids import canonical_sha256
from hdt.db.base import Base
from hdt.db.models import import_all_models
from hdt.settings import store
from hdt.settings.schemas import ModelsSection, RoleModelConfig, Section

pytestmark = [pytest.mark.pg, pytest.mark.integration]


def role(**extra: Any) -> RoleModelConfig:
    return RoleModelConfig.model_validate(
        {
            "model": "anthropic/claude-sonnet-4.5",
            "temperature": 0.2,
            "max_tokens": 800,
            "timeout_s": 30,
            **extra,
        }
    )


def save(session: Session, config: RoleModelConfig, parent_id: int | None) -> tuple[int, list[int]]:
    models = ModelsSection(roles={"technical": config})
    version = store.create_version(
        session, Section.MODELS, models, author="owner", reason="models", parent_id=parent_id
    )
    rows = store.record_agent_versions(session, models, config_version_id=version.id, author="owner")
    return version.id, [r.version for r in rows]


def test_a_fallback_change_alone_is_a_new_agent_version(
    fresh_database: Callable[[], AbstractContextManager[str]],
) -> None:
    plain = role()
    one = role(fallback_models=["openai/gpt-4.1"])
    other = role(fallback_models=["google/gemini-2.5-pro"])
    reordered = role(fallback_models=["google/gemini-2.5-pro", "openai/gpt-4.1"])
    assert len({store.provider_prefs_hash(c) for c in (plain, one, other, reordered)}) == 4
    # Without fallbacks the hash is unchanged: rows recorded before fallbacks were hashed keep matching.
    assert store.provider_prefs_hash(plain) == canonical_sha256(plain.provider_preferences())

    import_all_models()
    with fresh_database() as url:
        engine = sa.create_engine(url, future=True)
        try:
            Base.metadata.create_all(engine)
            sessions = sessionmaker(engine, expire_on_commit=False)
            with sessions() as session, session.begin():
                first, created = save(session, plain, None)
                assert created == [1]
                second, created = save(session, role(temperature=0.5), first)
                assert created == []
                third, created = save(session, one, second)
                assert created == [2]
                _, created = save(session, other, third)
                assert created == [3]

            # The council's book hashes the same way: the fallback config resolves to the recorded row.
            key = VersionKey.of(AgentName.TECHNICAL, other, prompt_hash="p" * 64, skill_commit="c" * 40)
            assert key.provider_prefs_hash == store.provider_prefs_hash(other)
            plain_key = VersionKey.of(AgentName.TECHNICAL, plain, prompt_hash="p" * 64, skill_commit="c" * 40)
            book = AgentVersionBook(sessions, write=True)
            fallback_version = book.version(key, config_version_id=None)
            assert book.version(plain_key, config_version_id=None) > fallback_version
        finally:
            engine.dispose()


def test_interleaved_pins_resolve_to_one_version_each_and_a_reactivation_is_new(
    fresh_database: Callable[[], AbstractContextManager[str]],
) -> None:
    """N4: resolving against the agent's global latest row made A,B,A,B,A,B give versions 1..6."""
    a, b = role(), role(model="openai/gpt-4.1")
    import_all_models()
    with fresh_database() as url:
        engine = sa.create_engine(url, future=True)
        try:
            Base.metadata.create_all(engine)
            sessions = sessionmaker(engine, expire_on_commit=False)
            with sessions() as session, session.begin():
                cv_a, _ = save(session, a, None)
                cv_b, _ = save(session, b, cv_a)
            key_a = VersionKey.of(AgentName.TECHNICAL, a, prompt_hash="p" * 64, skill_commit="c" * 40)
            key_b = VersionKey.of(AgentName.TECHNICAL, b, prompt_hash="p" * 64, skill_commit="c" * 40)
            book = AgentVersionBook(sessions, write=True)
            seen = [
                book.version(key, config_version_id=cv)
                for _ in range(3)
                for key, cv in ((key_a, cv_a), (key_b, cv_b))
            ]
            assert len(set(seen)) == 2
            assert seen[:2] * 3 == seen
            # A fresh book (another process) resolves the same way; replay reads the same versions.
            assert AgentVersionBook(sessions, write=True).version(key_a, config_version_id=cv_a) == seen[0]
            reader = AgentVersionBook(sessions, write=False)
            assert reader.version(key_b, config_version_id=cv_b) == seen[1]

            # A -> B -> A through config-api: the re-activated A is a new version, stable afterwards.
            with sessions() as session, session.begin():
                cv_a2, _ = save(session, a, cv_b)
            again = book.version(key_a, config_version_id=cv_a2)
            assert again not in seen
            assert book.version(key_b, config_version_id=cv_b) == seen[1]
            assert book.version(key_a, config_version_id=cv_a2) == again

            # A prompt change and its revert under one pin: each is a new version.
            key_p2 = VersionKey.of(AgentName.TECHNICAL, a, prompt_hash="q" * 64, skill_commit="c" * 40)
            p2 = book.version(key_p2, config_version_id=cv_a2)
            reverted = book.version(key_a, config_version_id=cv_a2)
            assert len({again, p2, reverted}) == 3
        finally:
            engine.dispose()
