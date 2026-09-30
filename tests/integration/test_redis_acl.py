"""The rendered Redis ACL enforces the stream producer/consumer matrix (docs/system-architecture.md).

Each template user is loaded under a throwaway name (`t<hex>_<user>`, no password) and checked with
`ACL DRYRUN`, so nothing is executed against real keys; the users are deleted afterwards. The risk service
also runs for real over a connection authenticated as the template's `risk` user (review cycle 3, C1).
"""

from __future__ import annotations

import asyncio
import re
import secrets
import time
from collections.abc import AsyncIterator
from datetime import timedelta
from pathlib import Path

import pytest
import sqlalchemy as sa
from redis.asyncio import Redis
from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

from fake_exchange import ledger_sessions
from hdt.contracts.common import Account
from hdt.contracts.streams import Stream
from hdt.core.clock import utcnow
from hdt.core.config import read_env_value, static_config
from hdt.core.streams import REDIS_SOCKET_TIMEOUT_S, encode
from hdt.db.models.risk import ProcessedDecisionRow
from hdt.lake.pit_query import PitQuery
from hdt.risk.main import RiskService
from intent_builders import RISK_SIGNER
from risk_builders import decision

pytestmark = pytest.mark.redis

TEMPLATE = Path(__file__).resolve().parents[2] / "deploy" / "redis" / "users.acl.template"
USER_LINE = re.compile(r"^user (\S+) on >\{\{[A-Z_]+\}\} (.*)$")
SELECTOR = re.compile(r"\([^)]*\)|\S+")

# user -> streams it may XADD (besides dead_letter for every consumer)
PRODUCES: dict[str, set[str]] = {
    "recorder": {"secret_test_result"},
    "council": {"candidates", "decisions", "secret_test_result"},
    "news": {"candidates", "secret_test_result"},
    "veto_scan": {"risk_flags"},
    "scorer": {"secret_test_result"},
    "risk": {"orders"},
    "execution": {"account_state", "secret_test_result"},
    "testnet_driver": {"orders"},
    "configapi": {"config_changed", "secret_test_request", "controls"},
    "console": set(),
    "publisher": set(),
    "telegram": {"secret_test_result", "controls"},
    "monitor": set(),
}
CONSUMES: dict[str, set[str]] = {
    "recorder": {"config_changed", "secret_test_request"},
    "council": {"candidates", "config_changed", "secret_test_request"},
    "news": {"config_changed", "secret_test_request"},
    "veto_scan": {"config_changed"},
    "scorer": {"config_changed", "secret_test_request"},
    "risk": {"decisions", "risk_flags", "account_state", "controls", "config_changed"},
    "execution": {"orders", "risk_flags", "controls", "config_changed", "secret_test_request"},
    "testnet_driver": set(),
    "configapi": {"secret_test_result"},
    "console": set(),
    "publisher": set(),
    "telegram": {"config_changed", "secret_test_request"},
    "monitor": set(),
}
STREAMS = sorted({"dead_letter"} | set().union(*PRODUCES.values(), *CONSUMES.values()))


def _template_users() -> dict[str, list[str]]:
    users: dict[str, list[str]] = {}
    for line in TEMPLATE.read_text(encoding="utf-8").splitlines():
        match = USER_LINE.match(line)
        if match and match.group(1) != "admin":
            users[match.group(1)] = SELECTOR.findall(match.group(2))
    return users


@pytest.fixture
async def acl_users() -> AsyncIterator[tuple[Redis, dict[str, str]]]:
    url = read_env_value("HDT_TEST_REDIS_URL")
    if url is None:
        raise RuntimeError("HDT_TEST_REDIS_URL is not set: start the dev stack and fill .env")
    client: Redis = Redis.from_url(url.rstrip("/") + "/0", decode_responses=True)
    prefix = f"t{secrets.token_hex(4)}_"
    names: dict[str, str] = {}
    try:
        for user, rules in _template_users().items():
            names[user] = prefix + user
            await client.execute_command("ACL", "SETUSER", names[user], "reset", "on", "nopass", *rules)
        yield client, names
    finally:
        for name in names.values():
            await client.execute_command("ACL", "DELUSER", name)
        await client.aclose()


async def _allowed(client: Redis, user: str, *command: str) -> bool:
    reply = await client.execute_command("ACL", "DRYRUN", user, *command)
    return reply == "OK"


def test_matrix_covers_every_template_user() -> None:
    assert set(_template_users()) == set(PRODUCES)


async def test_xadd_follows_the_producer_matrix(acl_users: tuple[Redis, dict[str, str]]) -> None:
    client, names = acl_users
    wrong: list[str] = []
    for user, name in names.items():
        expected = set(PRODUCES[user]) | ({"dead_letter"} if CONSUMES[user] else set())
        for stream in STREAMS:
            if await _allowed(client, name, "XADD", stream, "*", "f", "v") != (stream in expected):
                wrong.append(f"{user} XADD {stream}: expected {'allow' if stream in expected else 'deny'}")
    assert wrong == []


async def test_consumers_can_read_and_ack_only_their_streams(acl_users: tuple[Redis, dict[str, str]]) -> None:
    client, names = acl_users
    wrong: list[str] = []
    for user, name in names.items():
        for stream in STREAMS:
            read = await _allowed(client, name, "XREADGROUP", "GROUP", "g", "c", "STREAMS", stream, ">")
            ack = await _allowed(client, name, "XACK", stream, "g", "0-1")
            claim = await _allowed(client, name, "XAUTOCLAIM", stream, "g", "c", "60000", "0-0")
            pending = await _allowed(client, name, "XPENDING", stream, "g", "0-1", "0-1", "1")
            expected = stream in CONSUMES[user]
            if {read, ack, claim, pending} != {expected}:
                wrong.append(
                    f"{user} consume {stream}: read={read} ack={ack} autoclaim={claim} "
                    f"xpending={pending} expected={expected}"
                )
    assert wrong == []


@pytest.mark.parametrize(
    "command",
    [
        ("XTRIM", "orders", "MAXLEN", "0"),
        ("XDEL", "decisions", "0-1"),
        ("DEL", "orders"),
        ("XGROUP", "DESTROY", "orders", "g"),
        ("XGROUP", "SETID", "decisions", "g", "0"),
        ("FLUSHDB",),
        ("CONFIG", "GET", "*"),
        ("KEYS", "*"),
    ],
)
async def test_destructive_commands_are_denied_for_every_service(
    acl_users: tuple[Redis, dict[str, str]], command: tuple[str, ...]
) -> None:
    client, names = acl_users
    allowed = [user for user, name in names.items() if await _allowed(client, name, *command)]
    assert allowed == []


READ_LATEST = {
    ("council", "account_state"),
    ("console", "account_state"),
    ("console", "risk_flags"),
    ("publisher", "account_state"),
    ("testnet_driver", "account_state"),
    ("risk", "account_state"),
    ("risk", "risk_flags"),
    # recorder hot set (ingest/hot_set.py): candidates and positions, read without a consumer group
    ("recorder", "candidates"),
    ("recorder", "account_state"),
}


async def test_latest_state_readers_can_xrevrange(acl_users: tuple[Redis, dict[str, str]]) -> None:
    client, names = acl_users
    denied = [
        f"{user} {stream}"
        for user, stream in sorted(READ_LATEST)
        if not await _allowed(client, names[user], "XREVRANGE", stream, "+", "-", "COUNT", "1")
    ]
    assert denied == []


# ------------------------------------------------------------------ the risk service under its ACL user


@pytest.fixture
async def risk_redis(redis_client: Redis) -> AsyncIterator[Redis]:
    """A connection authenticated as the template's `risk` user, on the test's leased DB index.

    `+select` is the only rule added to the template line: it lets the client switch to the leased index
    (production connects to index 0 and never sends SELECT). Key and command rules are the template's."""
    name = f"t{secrets.token_hex(4)}_risk"
    rules = _template_users()["risk"]
    await redis_client.execute_command("ACL", "SETUSER", name, "reset", "on", "nopass", *rules, "+select")
    kwargs = redis_client.connection_pool.connection_kwargs
    client = Redis(
        host=kwargs["host"],
        port=kwargs["port"],
        db=kwargs["db"],
        username=name,
        password="unused",  # nopass: any password authenticates
        socket_timeout=REDIS_SOCKET_TIMEOUT_S,
    )
    try:
        yield client
    finally:
        await client.aclose()
        await redis_client.execute_command("ACL", "DELUSER", name)


def _processed(sessions: sessionmaker[Session], event_id: str) -> int:
    with sessions() as s:
        q = (
            sa.select(sa.func.count())
            .select_from(ProcessedDecisionRow)
            .where(ProcessedDecisionRow.event_id == event_id)
        )
        return int(s.scalar(q) or 0)


@pytest.mark.pg
async def test_risk_attaches_and_handles_its_backlog_under_its_acl_user(
    pg_engine: Engine, redis_client: Redis, risk_redis: Redis, tmp_path: Path
) -> None:
    sessions = ledger_sessions(pg_engine)
    svc = RiskService(
        static=static_config(),
        sessions=sessions,
        redis=risk_redis,
        signer=RISK_SIGNER,
        pit=PitQuery(tmp_path, tmp_path),
    )
    svc.block_ms = 10  # detach waits for the blocking read to return
    svc.mode = lambda: Account.PAPER  # type: ignore[method-assign]
    try:
        await svc.sync_namespaces()  # first attach: creates the group
        assert set(svc.namespaces) == {Account.PAPER}
        await svc.detach(Account.PAPER)  # e.g. weeks on another mode: the group stays where it stopped
        # A decision has no time limit (owner decision 2026-09-28): what was published while detached is
        # handled on re-attach, however old, and judged against the current market by the gate.
        now_ms = int(time.time() * 1000)
        late = utcnow() - timedelta(hours=1)
        stream = str(Stream.DECISIONS)
        await redis_client.xadd(
            stream, encode(decision(event_id="evt-acl-old", as_of=late)), id=f"{now_ms - 3_600_000}-0"
        )
        await redis_client.xadd(
            stream, encode(decision(event_id="evt-acl-young", as_of=late)), id=f"{now_ms - 1_000}-0"
        )
        await svc.sync_namespaces()  # re-attach after the long detach
        ns = svc.namespaces[Account.PAPER]
        for _ in range(100):
            acked = (await redis_client.xpending(stream, "risk-paper"))["pending"] == 0
            if acked and _processed(sessions, "evt-acl-young") and _processed(sessions, "evt-acl-old"):
                break
            await asyncio.sleep(0.05)
        assert _processed(sessions, "evt-acl-old") == 1
        assert _processed(sessions, "evt-acl-young") == 1
        assert (await redis_client.xpending(stream, "risk-paper"))["pending"] == 0
        assert [t.get_name() for t in ns.tasks if t.done()] == []  # no consumer died on NOPERM
    finally:
        for account in list(svc.namespaces):
            await svc.detach(account)
