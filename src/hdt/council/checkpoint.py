"""LangGraph checkpoints of in-flight meetings in Postgres (`thread_id = event_id`).

The tables are created by migration `0009_council` (the council role has no DDL rights), so `setup()` is
never called. The sync `PostgresSaver` runs over a psycopg connection pool and its async methods delegate
to worker threads: psycopg's async connections need a selector event loop, which the Windows default
(proactor) loop is not, and the council's blocking work already runs in threads. Every checkpoint write is
synchronous with the graph (`durability="sync"`), so a killed process resumes from the last finished task.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator, Sequence
from contextlib import contextmanager
from typing import Any

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import ChannelVersions, Checkpoint, CheckpointMetadata, CheckpointTuple
from langgraph.checkpoint.postgres import PostgresSaver
from psycopg import Connection
from psycopg.rows import DictRow, dict_row
from psycopg_pool import ConnectionPool

from hdt.memory.store import libpq_dsn


class ThreadedPostgresSaver(PostgresSaver):
    """`PostgresSaver` whose async API runs the sync implementation in a worker thread."""

    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        return await asyncio.to_thread(self.get_tuple, config)

    async def alist(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[CheckpointTuple]:
        items = await asyncio.to_thread(
            lambda: list(self.list(config, filter=filter, before=before, limit=limit))
        )
        for item in items:
            yield item

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        return await asyncio.to_thread(self.put, config, checkpoint, metadata, new_versions)

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        await asyncio.to_thread(self.put_writes, config, writes, task_id, task_path)

    async def adelete_thread(self, thread_id: str) -> None:
        await asyncio.to_thread(self.delete_thread, thread_id)


@contextmanager
def open_checkpointer(dsn: str, *, max_size: int = 4) -> Iterator[ThreadedPostgresSaver]:
    """A checkpointer over a small pool for `dsn` (SQLAlchemy or libpq form) of the calling service's role."""
    pool: ConnectionPool[Connection[DictRow]] = ConnectionPool(
        libpq_dsn(dsn),
        min_size=1,
        max_size=max_size,
        kwargs={"autocommit": True, "prepare_threshold": 0, "row_factory": dict_row},
        open=True,
    )
    try:
        yield ThreadedPostgresSaver(pool)
    finally:
        pool.close()
