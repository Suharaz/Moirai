"""Operator events published by config-api and the config watcher used by every service.

- `ConfigChanged` on stream `config_changed` after a new version becomes active.
- `ControlCommand` on stream `controls` (pause / resume / kill / flatten); execution enforces the resume
  rules (no daily-loss resume within the same UTC day, no reconcile resume while not clean).

`ConfigWatcher` gives a service its view of the active versions: it consumes `config_changed` with a
consumer group (`config-<service>`, XACK after the new version is loaded), reconciles with
`active_config` at startup and periodically (so a lost message or a restart converges to the database),
and only swaps versions in when the service calls `apply_pending()` at an event boundary.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Iterable
from typing import Literal

from pydantic import Field, PositiveInt
from redis.asyncio import Redis
from sqlalchemy.orm import Session, sessionmaker

from hdt.contracts.common import Account, InboundContractModel, UtcDatetime
from hdt.contracts.streams import Stream
from hdt.core.streams import StreamConsumer, ensure_group, publish
from hdt.settings.schemas import Section
from hdt.settings.versions import ConfigPin, ConfigVersion, active_versions

log = logging.getLogger(__name__)

ControlAction = Literal["pause", "resume", "kill", "flatten"]


class ConfigChanged(InboundContractModel):
    schema_version: Literal[1] = 1
    section: Section
    version_id: PositiveInt
    previous_version_id: PositiveInt | None
    author: str
    changed_at: UtcDatetime


class ControlCommand(InboundContractModel):
    schema_version: Literal[1] = 1
    command_id: str = Field(min_length=1, max_length=64)
    account: Account
    action: ControlAction
    reason: str = Field(min_length=1, max_length=500)
    requested_by: str
    source: Literal["console", "telegram", "cli"]
    requested_at: UtcDatetime


async def publish_config_changed(
    redis: Redis, version: ConfigVersion, previous_version_id: int | None, maxlen: int | None = None
) -> str:
    message = ConfigChanged(
        section=version.section,
        version_id=version.id,
        previous_version_id=previous_version_id,
        author=version.author,
        changed_at=version.created_at,
    )
    return await publish(redis, Stream.CONFIG_CHANGED, message, maxlen=maxlen)


class ConfigWatcher:
    """A service's applied config versions, updated only at event boundaries."""

    def __init__(
        self,
        *,
        redis: Redis,
        session_factory: sessionmaker[Session],
        service: str,
        consumer: str,
        sections: Iterable[Section] | None = None,
        reconcile_interval_s: float = 60.0,
        block_ms: int = 5000,
        reclaim_idle_ms: int = 60000,
    ) -> None:
        self._redis = redis
        self._factory = session_factory
        self._sections = frozenset(sections) if sections is not None else frozenset(Section)
        self._interval = reconcile_interval_s
        self._last_reconcile = float("-inf")
        self.group = f"config-{service}"
        self.current: dict[Section, ConfigVersion] = {}
        self._pending: dict[Section, ConfigVersion] = {}
        self._consumer: StreamConsumer[ConfigChanged] = StreamConsumer(
            redis=redis,
            stream=Stream.CONFIG_CHANGED,
            group=self.group,
            consumer=consumer,
            model=ConfigChanged,
            handler=self._handle,
            block_ms=block_ms,
            reclaim_idle_ms=reclaim_idle_ms,
        )

    async def start(self) -> dict[Section, ConfigVersion]:
        """Create the group (new groups start at the stream tail) and load the active versions."""
        await ensure_group(self._redis, Stream.CONFIG_CHANGED, self.group, start_id="$")
        await self.reconcile()
        return self.apply_pending()

    @property
    def pending(self) -> dict[Section, ConfigVersion]:
        return dict(self._pending)

    def pin(self) -> ConfigPin:
        return ConfigPin(dict(self.current))

    def apply_pending(self) -> dict[Section, ConfigVersion]:
        """Swap in versions received since the last boundary; returns what changed."""
        applied = {s: v for s, v in self._pending.items() if self.current.get(s, None) != v}
        self.current.update(applied)
        self._pending.clear()
        for section, version in applied.items():
            log.info("config version applied", extra={"section": section.value, "version_id": version.id})
        return applied

    async def reconcile(self) -> set[Section]:
        """Queue every section whose active version differs from what this service holds."""
        self._last_reconcile = time.monotonic()
        active = await asyncio.to_thread(self._read_active)
        changed: set[Section] = set()
        for section, version in active.items():
            if section in self._sections and self._stage(version):
                changed.add(section)
        return changed

    async def poll_once(self) -> int:
        """Consume `config_changed` once and reconcile when the interval has elapsed."""
        acked = await self._consumer.run_once()
        if time.monotonic() - self._last_reconcile >= self._interval:
            await self.reconcile()
        return acked

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await self.poll_once()

    def _read_active(self) -> dict[Section, ConfigVersion]:
        with self._factory() as session:
            return active_versions(session)

    def _stage(self, version: ConfigVersion) -> bool:
        held = self._pending.get(version.section) or self.current.get(version.section)
        if held is not None and held.id == version.id:
            return False
        self._pending[version.section] = version
        return True

    async def _handle(self, _msg_id: str, message: ConfigChanged) -> bool:
        if message.section not in self._sections:
            return True
        # The database is the source of truth: stage what is active now, whatever order messages arrive in.
        active = await asyncio.to_thread(self._read_active)
        version = active.get(message.section)
        if version is not None:
            self._stage(version)
        return True
