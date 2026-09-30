"""config-api application factory and entry point (`python -m hdt.configapi.main`).

Bootstrap inputs (docker secrets / env): HDT_PG_DSN (role hdt_configapi), HDT_REDIS_URL (ACL user
configapi), HDT_SESSION_SECRET_FILE, optional HDT_CONFIGAPI_TRUSTED_PROXIES, HDT_CONFIGAPI_HOST
(default 127.0.0.1) and HDT_CONFIGAPI_PORT (default 8081). Never publish the port: the console reaches
it on the internal network, operators through Tailscale / SSH tunnel.
"""

from __future__ import annotations

import logging
import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import uvicorn
from argon2 import PasswordHasher
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from hdt.configapi.context import AppState, ConfigApiSettings, get_state
from hdt.configapi.errors import install_error_handlers
from hdt.configapi.routes import api_router
from hdt.core.config import StaticConfig, read_env_value, static_config
from hdt.core.logging import configure_logging
from hdt.core.streams import connect_redis
from hdt.db.session import make_engine, make_session_factory
from hdt.llm.openrouter_catalog import OpenRouterCatalog
from hdt.settings.store import seed_if_missing
from hdt.vault.scopes import load_public_keys

log = logging.getLogger(__name__)


def create_app(
    settings: ConfigApiSettings | None = None,
    *,
    catalog: OpenRouterCatalog | None = None,
    static: StaticConfig | None = None,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        cfg = settings or ConfigApiSettings.from_env()
        engine = make_engine(cfg.pg_dsn)
        redis: Redis = connect_redis(cfg.redis_url)
        hasher = PasswordHasher()
        state = AppState(
            settings=cfg,
            engine=engine,
            session_factory=make_session_factory(engine),
            redis=redis,
            static=static or static_config(),
            public_keys=load_public_keys(cfg.public_keys_path),
            catalog=catalog or OpenRouterCatalog(),
            hasher=hasher,
            dummy_hash=hasher.hash(secrets.token_hex(16)),
        )
        seeded = await state.db(lambda s: seed_if_missing(s, state.static))
        for version in seeded:
            log.info(
                "config section seeded", extra={"section": version.section.value, "version_id": version.id}
            )
        app.state.hdt = state
        try:
            yield
        finally:
            await redis.aclose()
            engine.dispose()

    app = FastAPI(title="hdt config-api", lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    install_error_handlers(app)
    app.include_router(api_router())

    @app.get("/api/health", include_in_schema=False)
    async def health(request: Request) -> JSONResponse:
        state = get_state(request)
        checks: dict[str, Any] = {}
        try:
            await state.db(lambda s: s.execute(text("SELECT 1")))
            checks["postgres"] = "ok"
        except SQLAlchemyError:
            checks["postgres"] = "down"
        try:
            await state.redis.ping()
            checks["redis"] = "ok"
        except RedisError:
            checks["redis"] = "down"
        healthy = all(value == "ok" for value in checks.values())
        return JSONResponse(
            status_code=200 if healthy else 503, content={"status": "ok" if healthy else "degraded", **checks}
        )

    return app


def main() -> None:
    configure_logging("configapi")
    host = read_env_value("HDT_CONFIGAPI_HOST") or "127.0.0.1"
    port = int(read_env_value("HDT_CONFIGAPI_PORT") or "8081")
    uvicorn.run(create_app(), host=host, port=port, proxy_headers=False, server_header=False, log_config=None)


if __name__ == "__main__":
    main()
