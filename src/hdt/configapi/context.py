"""config-api settings (bootstrap values only) and per-process application state."""

from __future__ import annotations

import ipaddress
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import TypeVar

from argon2 import PasswordHasher
from fastapi import Request
from redis.asyncio import Redis
from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker
from starlette.concurrency import run_in_threadpool

from hdt.core.config import StaticConfig, read_env_value, require_env_value
from hdt.db.session import transaction
from hdt.llm.openrouter_catalog import OpenRouterCatalog
from hdt.vault.redact import register_secret
from hdt.vault.scopes import PUBLIC_KEYS_PATH, Scope

T = TypeVar("T")
type IpNetwork = ipaddress.IPv4Network | ipaddress.IPv6Network

MIN_SESSION_SECRET_BYTES = 32


def parse_trusted_proxies(text: str | None) -> tuple[IpNetwork, ...]:
    """Comma-separated IPs / CIDRs whose `X-Forwarded-For` header is trusted (the console container)."""
    if not text:
        return ()
    return tuple(ipaddress.ip_network(part.strip(), strict=False) for part in text.split(",") if part.strip())


@dataclass(frozen=True)
class ConfigApiSettings:
    pg_dsn: str = field(repr=False)
    redis_url: str = field(repr=False)
    session_secret: bytes = field(repr=False)
    trusted_proxies: tuple[IpNetwork, ...] = ()
    public_keys_path: Path = PUBLIC_KEYS_PATH

    def __post_init__(self) -> None:
        if len(self.session_secret) < MIN_SESSION_SECRET_BYTES:
            raise ValueError(f"the session secret must be at least {MIN_SESSION_SECRET_BYTES} bytes")

    @classmethod
    def from_env(cls) -> ConfigApiSettings:
        """HDT_PG_DSN (role hdt_configapi), HDT_REDIS_URL (ACL user configapi), HDT_SESSION_SECRET_FILE,
        optional HDT_CONFIGAPI_TRUSTED_PROXIES."""
        secret = require_env_value("HDT_SESSION_SECRET")
        register_secret(secret)
        return cls(
            pg_dsn=require_env_value("HDT_PG_DSN"),
            redis_url=require_env_value("HDT_REDIS_URL"),
            session_secret=secret.encode("utf-8"),
            trusted_proxies=parse_trusted_proxies(read_env_value("HDT_CONFIGAPI_TRUSTED_PROXIES")),
        )


@dataclass
class AppState:
    settings: ConfigApiSettings
    engine: Engine
    session_factory: sessionmaker[Session]
    redis: Redis
    static: StaticConfig
    public_keys: dict[Scope, str]
    catalog: OpenRouterCatalog
    hasher: PasswordHasher
    dummy_hash: str = field(repr=False)

    def stream_maxlen(self, stream: str) -> int | None:
        return self.static.settings.streams.maxlen.get(stream)

    async def db(self, fn: Callable[[Session], T]) -> T:
        """Run `fn` in one transaction on a worker thread (committed on success, rolled back on error)."""
        return await run_in_threadpool(self._run, fn)

    def _run(self, fn: Callable[[Session], T]) -> T:
        with transaction(self.session_factory) as session:
            return fn(session)


def get_state(request: Request) -> AppState:
    state: AppState = request.app.state.hdt
    return state
