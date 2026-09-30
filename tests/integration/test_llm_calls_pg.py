"""LLM call ledger on Postgres (migration 0007_llm): a live call is recorded and cached under the council
role, a replay router answers from the cache without any network, the read-only roles see the ledger but
not the cached replies, and agent versions are recorded once per configuration."""

from __future__ import annotations

import json
from collections.abc import Callable
from contextlib import AbstractContextManager
from pathlib import Path
from typing import Any

import httpx2
import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy.orm import sessionmaker

from hdt.agents.llm_router import LlmRouter
from hdt.agents.llm_types import LlmCacheMissError
from hdt.agents.versions import UNREGISTERED_VERSION, AgentVersionBook, VersionKey
from hdt.contracts.common import AgentName
from hdt.contracts.forecast import AgentForecastDraft
from hdt.settings.schemas import RoleModelConfig

pytestmark = [pytest.mark.pg, pytest.mark.integration]

ROOT = Path(__file__).resolve().parents[2]
MODEL = "anthropic/claude-sonnet-4.5"
REPLY = json.dumps({"p_llm": 0.61, "abstain": False, "claims": [], "cited_claim_ids": [], "reason": "r"})


def role_config() -> RoleModelConfig:
    return RoleModelConfig.model_validate(
        {
            "model": MODEL,
            "temperature": 0.2,
            "max_tokens": 800,
            "timeout_s": 30,
            "provider": {"only": ["Anthropic"]},
        }
    )


def openrouter(sent: list[dict[str, Any]]) -> httpx2.AsyncClient:
    def handler(request: httpx2.Request) -> httpx2.Response:
        sent.append(json.loads(request.content))
        return httpx2.Response(
            200,
            json={
                "id": f"gen-pg-{len(sent)}",
                "object": "chat.completion",
                "created": 1,
                "model": MODEL,
                "provider": "Anthropic",
                "choices": [
                    {"index": 0, "finish_reason": "stop", "message": {"role": "assistant", "content": REPLY}}
                ],
                "usage": {"prompt_tokens": 90, "completion_tokens": 15, "total_tokens": 105, "cost": 0.002},
            },
        )

    return httpx2.AsyncClient(transport=httpx2.MockTransport(handler))


async def ask(router: LlmRouter, user: str = "packet") -> Any:
    return await router.structured(
        role="technical",
        pipeline="council",
        event_id="evt-pg",
        config=role_config(),
        system="rules",
        user=user,
        schema=AgentForecastDraft,
    )


async def test_llm_ledger_cache_replay_and_grants(
    fresh_database: Callable[[], AbstractContextManager[str]], pg_role_url: Callable[[str, str], str]
) -> None:
    with fresh_database() as url:
        cfg = Config(str(ROOT / "alembic.ini"))
        cfg.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
        command.upgrade(cfg, "head")

        council = sa.create_engine(pg_role_url(url, "hdt_council"))
        console = sa.create_engine(pg_role_url(url, "hdt_console_ro"))
        try:
            council_sessions = sessionmaker(council)
            sent: list[dict[str, Any]] = []
            live = LlmRouter(
                mode="live",
                api_key=lambda: "sk-or-test",
                http_client=openrouter(sent),
                session_factory=council_sessions,
            )
            first = await ask(live)
            again = await ask(live)
            assert len(sent) == 1  # the second identical call is answered from llm_cache
            assert again.cached
            assert again.output == first.output

            replay = LlmRouter(mode="replay", api_key=None, session_factory=council_sessions)
            replayed = await replay.structured(
                role="technical",
                pipeline="council",
                event_id="evt-pg",
                config=role_config(),
                system="rules",
                user="packet",
                schema=AgentForecastDraft,
            )
            assert replayed.output.p_llm == 0.61
            assert replayed.generation_id == first.generation_id
            with pytest.raises(LlmCacheMissError):
                await ask(replay, user="a prompt never sent live")

            with console.connect() as conn:
                columns = "pipeline, role, event_id, model_slug, provider, cost_usd, status"
                rows = conn.execute(sa.text(f"SELECT {columns} FROM llm_calls")).all()
                assert [tuple(r) for r in rows] == [
                    ("council", "technical", "evt-pg", MODEL, "Anthropic", pytest.approx(0.002), "ok")
                ]
                with pytest.raises(sa.exc.ProgrammingError, match="permission denied"):
                    conn.execute(sa.text("SELECT response FROM llm_cache")).all()

            key = VersionKey.of(
                AgentName.TECHNICAL, role_config(), prompt_hash="p" * 64, skill_commit="c" * 40
            )
            reader = AgentVersionBook(council_sessions, write=False)
            assert reader.version(key, config_version_id=None) == UNREGISTERED_VERSION
            writer = AgentVersionBook(council_sessions, write=True)
            version = writer.version(key, config_version_id=None)
            assert version >= 1
            assert (
                AgentVersionBook(council_sessions, write=True).version(key, config_version_id=None) == version
            )
            changed = VersionKey.of(
                AgentName.TECHNICAL, role_config(), prompt_hash="q" * 64, skill_commit="c" * 40
            )
            assert (
                AgentVersionBook(council_sessions, write=True).version(changed, config_version_id=None)
                > version
            )
            assert (
                AgentVersionBook(council_sessions, write=False).version(key, config_version_id=None)
                == version
            )

            # A -> B -> A through one long-lived book: A is a new version again, never its stale v1.
            book = AgentVersionBook(council_sessions, write=True)
            reader = AgentVersionBook(council_sessions, write=False)
            a = VersionKey.of(AgentName.MACRO, role_config(), prompt_hash="a" * 64, skill_commit="c" * 40)
            b = VersionKey.of(AgentName.MACRO, role_config(), prompt_hash="b" * 64, skill_commit="c" * 40)
            first_a = book.version(a, config_version_id=None)
            assert reader.version(a, config_version_id=None) == first_a
            assert book.version(b, config_version_id=None) == first_a + 1
            assert book.version(a, config_version_id=None) == first_a + 2
            assert reader.version(a, config_version_id=None) == first_a + 2
        finally:
            council.dispose()
            console.dispose()


def test_the_public_publisher_reads_only_the_cost_columns_of_llm_calls(
    fresh_database: Callable[[], AbstractContextManager[str]], pg_role_url: Callable[[str, str], str]
) -> None:
    """M17: 0007 granted `hdt_publisher_ro` SELECT on the whole table (generation ids, provider, hashes)."""
    with fresh_database() as url:
        cfg = Config(str(ROOT / "alembic.ini"))
        cfg.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
        command.upgrade(cfg, "head")
        publisher = sa.create_engine(pg_role_url(url, "hdt_publisher_ro"))
        try:
            with publisher.connect() as conn:
                conn.execute(
                    sa.text(
                        "SELECT called_at, pipeline, role, event_id, model_slug, prompt_tokens, "
                        "completion_tokens, cost_usd, count(*) OVER () FROM llm_calls"
                    )
                ).all()
            for column in ("generation_id", "provider", "model_returned", "prompt_hash", "*"):
                with publisher.connect() as conn, pytest.raises(sa.exc.ProgrammingError, match="permission"):
                    conn.execute(sa.text(f"SELECT {column} FROM llm_calls"))
        finally:
            publisher.dispose()
