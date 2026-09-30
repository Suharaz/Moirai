"""Agent versions of the council agents (Design Contract section 4, phase 05 Red Team Delta).

An agent's version changes when its model slug, provider preferences, prompt template or skills change.
config-api records model and provider changes (`hdt.settings.store.record_agent_versions`, which carries the
last prompt hash and skill commit over); the council records prompt and skill changes here, in the same
`agent_versions` table and row shape, the first time an agent runs with them. Phase 08 applies the
new-version weight cap from these rows.

Replay never writes: it reuses the highest recorded version matching all four fields, or `0` (never a real
version, they start at 1) when the replayed configuration was never run live.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Final, Protocol

from sqlalchemy import Select, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from hdt.contracts.common import AgentName
from hdt.core.clock import utcnow
from hdt.db.models.settings import AgentVersionRow
from hdt.db.session import transaction
from hdt.settings.schemas import RoleModelConfig
from hdt.settings.store import provider_prefs_hash

UNREGISTERED_VERSION: Final[int] = 0
_INSERT_ATTEMPTS: Final[int] = 3


@dataclass(frozen=True)
class VersionKey:
    agent: AgentName
    model_slug: str
    provider_prefs_hash: str
    prompt_hash: str
    skill_commit: str

    @classmethod
    def of(
        cls, agent: AgentName, config: RoleModelConfig, *, prompt_hash: str, skill_commit: str
    ) -> VersionKey:
        return cls(AgentName(agent), config.model, provider_prefs_hash(config), prompt_hash, skill_commit)

    def matches(self, row: AgentVersionRow) -> bool:
        return (row.model_slug, row.provider_prefs_hash, row.prompt_hash, row.skill_commit) == (
            self.model_slug,
            self.provider_prefs_hash,
            self.prompt_hash,
            self.skill_commit,
        )


class VersionSource(Protocol):
    def version(self, key: VersionKey, *, config_version_id: int | None) -> int:
        """The `agent_versions.version` of `key` (blocking)."""
        ...


class AgentVersionBook:
    """Resolves (and, when `write`, records) the `agent_versions.version` of an agent configuration.

    Resolution is scoped to the event's pinned models `config_version_id` (review cycle 2, N4), so version
    identity depends on configuration, not on call order:

    - the agent's "head at the pin" is its row with the highest `(config_version_id, version)` among rows
      whose `config_version_id` is at most the pin (a row without one counts as 0);
    - the key resolves to the head when the head matches it; otherwise (write mode) a new row is recorded
      with the pin as its `config_version_id`.

    This keeps both rules of phase 05/08 ("changing model/prompt = new agent_version"):
    - interleaved events pinned to A (cv 1) and B (cv 2) resolve to the same two versions for ever, because
      each pin sees only its own head;
    - A -> B -> A through config-api is a new version, because config-api records the re-activation at save
      with the new config version id, and that row is the head at the new pin;
    - a prompt or skill change and its revert under one pin is a new version each time, because the reverted
      key no longer matches the head.
    Without a pin (`config_version_id=None`) every row is in scope and the head is the newest row.
    """

    def __init__(
        self, session_factory: sessionmaker[Session], *, write: bool, author: str = "council"
    ) -> None:
        self._factory = session_factory
        self._write = write
        self._author = author
        self._lock = threading.Lock()

    def version(self, key: VersionKey, *, config_version_id: int | None) -> int:
        """Blocking (run it in a worker thread from async code)."""
        with self._lock:
            if self._write:
                return self._record(key, config_version_id)
            return self._lookup(key, config_version_id)

    @staticmethod
    def _in_scope(
        query: Select[AgentVersionRow], key: VersionKey, pin: int | None
    ) -> Select[AgentVersionRow]:
        query = query.where(AgentVersionRow.agent == key.agent.value)
        if pin is not None:
            query = query.where(func.coalesce(AgentVersionRow.config_version_id, 0) <= pin)
        return query

    def _head(self, session: Session, key: VersionKey, pin: int | None) -> AgentVersionRow | None:
        order = (
            (AgentVersionRow.version.desc(),)
            if pin is None
            else (func.coalesce(AgentVersionRow.config_version_id, 0).desc(), AgentVersionRow.version.desc())
        )
        return session.scalar(self._in_scope(select(AgentVersionRow), key, pin).order_by(*order).limit(1))

    def _lookup(self, key: VersionKey, pin: int | None) -> int:
        """Replay: the head at the pin when it matches, else the highest matching row in scope, else 0."""
        with self._factory() as session:
            head = self._head(session, key, pin)
            if head is not None and key.matches(head):
                return head.version
            found = session.scalar(
                self._in_scope(select(AgentVersionRow), key, pin)
                .where(
                    AgentVersionRow.model_slug == key.model_slug,
                    AgentVersionRow.provider_prefs_hash == key.provider_prefs_hash,
                    AgentVersionRow.prompt_hash == key.prompt_hash,
                    AgentVersionRow.skill_commit == key.skill_commit,
                )
                .order_by(AgentVersionRow.version.desc())
                .limit(1)
            )
        return found.version if found is not None else UNREGISTERED_VERSION

    def _record(self, key: VersionKey, pin: int | None) -> int:
        for attempt in range(1, _INSERT_ATTEMPTS + 1):
            try:
                with transaction(self._factory) as session:
                    head = self._head(session, key, pin)
                    if head is not None and key.matches(head):
                        return head.version
                    latest = session.scalar(
                        select(func.max(AgentVersionRow.version)).where(
                            AgentVersionRow.agent == key.agent.value
                        )
                    )
                    version = int(latest) + 1 if latest is not None else 1
                    session.add(
                        AgentVersionRow(
                            agent=key.agent.value,
                            version=version,
                            model_slug=key.model_slug,
                            provider_prefs_hash=key.provider_prefs_hash,
                            prompt_hash=key.prompt_hash,
                            skill_commit=key.skill_commit,
                            config_version_id=pin,
                            created_by=self._author,
                            created_at=utcnow(),
                        )
                    )
                return version
            except IntegrityError:
                if attempt == _INSERT_ATTEMPTS:
                    raise
        raise RuntimeError("unreachable")
