"""Scope owner protocol: test and activate new secret versions inside the service holding the scope key.

Flow (Design Contract section 10, phase 12):
1. config-api stores a `pending` sealed blob and publishes `SecretTestRequest` on `secret_test_request`.
2. The owner worker of the scope (consumer group `owner-<scope>`, one owner service per scope) opens the
   blob with the scope private key, checks the fingerprint, the payload schema and last4, then runs the
   check registered for `(scope, name)`.
3. In one transaction it activates the version (the previous active one becomes `retired`) or marks it
   `invalid` with a reason, then publishes `SecretTestResult` on `secret_test_result`. The secret itself
   never leaves the owner. Processing is idempotent per `request_id` (redelivery re-publishes only).

The `llm` owner also executes console model tests (`ModelTestRequest`) through a runner registered by
the LLM layer, writing the outcome to `model_tests`.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Annotated, Any, Final, Literal

from nacl.public import PrivateKey
from pydantic import BaseModel, Field, PositiveInt, RootModel, ValidationError
from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from hdt.contracts.common import InboundContractModel, UtcDatetime
from hdt.contracts.streams import Stream
from hdt.core.clock import utcnow
from hdt.core.config import LlmRole
from hdt.core.streams import StreamConsumer, publish
from hdt.db.models.settings import ModelTestRow, SecretRow
from hdt.db.session import transaction
from hdt.settings.schemas import RoleModelConfig
from hdt.vault.redact import redact, register_secret
from hdt.vault.scopes import SECRET_SPECS, Scope
from hdt.vault.sealed import SealedBlobError, fingerprint, last4, unseal

log = logging.getLogger(__name__)

OWNER_RESTART_MIN_S: Final[float] = 0.5
OWNER_RESTART_MAX_S: Final[float] = 30.0

type CheckValue = str | int | float | bool | None


def owner_group(scope: str) -> str:
    return f"owner-{scope}"


# --------------------------------------------------------------------------- messages


class SecretTestRequest(InboundContractModel):
    schema_version: Literal[1] = 1
    kind: Literal["secret_test"] = "secret_test"
    request_id: str = Field(min_length=1, max_length=64)
    scope: Scope
    name: str = Field(min_length=1, max_length=64)
    version: PositiveInt
    requested_by: str
    requested_at: UtcDatetime


class ModelTestRequest(InboundContractModel):
    schema_version: Literal[1] = 1
    kind: Literal["model_test"] = "model_test"
    request_id: str = Field(min_length=1, max_length=64)
    scope: Literal["llm"] = "llm"
    role: LlmRole
    config: RoleModelConfig
    requested_by: str
    requested_at: UtcDatetime


class OwnerRequest(RootModel[Annotated[SecretTestRequest | ModelTestRequest, Field(discriminator="kind")]]):
    """Union parsed by every owner worker from `secret_test_request`."""


class SecretTestResult(InboundContractModel):
    schema_version: Literal[1] = 1
    kind: Literal["secret_test"] = "secret_test"
    request_id: str
    scope: Scope
    name: str
    version: PositiveInt
    status: Literal["pending", "active", "invalid", "retired"]
    reason: str | None
    checked_at: UtcDatetime | None
    details: dict[str, CheckValue] = Field(default_factory=dict)


class ModelTestResult(InboundContractModel):
    schema_version: Literal[1] = 1
    kind: Literal["model_test"] = "model_test"
    request_id: str
    role: LlmRole
    status: Literal["done", "error"]
    schema_valid: bool | None
    latency_ms: int | None
    cost_usd: float | None
    provider: str | None
    model_returned: str | None
    error: str | None
    completed_at: UtcDatetime


class OwnerResult(RootModel[Annotated[SecretTestResult | ModelTestResult, Field(discriminator="kind")]]):
    """Union published on `secret_test_result`."""


# --------------------------------------------------------------------------- check registry types


@dataclass(frozen=True)
class CheckOutcome:
    """`valid`/`invalid` are definitive answers; `error` means the check could not complete."""

    status: Literal["valid", "invalid", "error"]
    reason: str | None = None
    details: Mapping[str, CheckValue] = field(default_factory=dict)


@dataclass(frozen=True)
class ModelTestOutcome:
    schema_valid: bool | None
    latency_ms: int | None
    cost_usd: float | None
    provider: str | None
    model_returned: str | None
    error: str | None


type SecretCheck = Callable[[Any], Awaitable[CheckOutcome]]
type ModelTestRunner = Callable[[ModelTestRequest], Awaitable[ModelTestOutcome]]


# --------------------------------------------------------------------------- worker


class OwnerWorker:
    """Consumes `secret_test_request` for one scope; see the module docstring for the protocol."""

    def __init__(
        self,
        *,
        scope: Scope,
        redis: Redis,
        session_factory: sessionmaker[Session],
        private_key: PrivateKey,
        checks: Mapping[str, SecretCheck],
        consumer: str,
        model_test_runner: ModelTestRunner | None = None,
        block_ms: int = 5000,
        reclaim_idle_ms: int = 60000,
        result_maxlen: int | None = None,
    ) -> None:
        self.scope = scope
        self._redis = redis
        self._factory = session_factory
        self._key = private_key
        self._checks = dict(checks)
        self._runner = model_test_runner
        self._maxlen = result_maxlen
        self._consumer: StreamConsumer[OwnerRequest] = StreamConsumer(
            redis=redis,
            stream=Stream.SECRET_TEST_REQUEST,
            group=owner_group(scope),
            consumer=consumer,
            model=OwnerRequest,
            handler=self._handle,
            batch_size=10,
            block_ms=block_ms,
            reclaim_idle_ms=reclaim_idle_ms,
        )

    async def start(self) -> None:
        await self._consumer.start()

    async def run_once(self) -> int:
        return await self._consumer.run_once()

    async def run(self, stop: asyncio.Event) -> None:
        """Consume until `stop` is set; Redis being down or restarted never ends the worker.

        Any Redis error (the consumer group cannot be created while Redis is down, a lost connection,
        NOGROUP after Redis restarted without its data) restarts the consumer after a capped exponential
        backoff, which re-creates the group. The backoff starts again from `OWNER_RESTART_MIN_S` once the
        group exists, so one long outage never slows the recovery from the next one. Only a real bug ends
        this coroutine, so a service that exits on a dead owner task never restarts for a Redis blip.
        """
        delay = OWNER_RESTART_MIN_S
        while not stop.is_set():
            try:
                await self._consumer.start()
                delay = OWNER_RESTART_MIN_S  # Redis answered: a later failure is a new outage
                while not stop.is_set():
                    await self._consumer.run_once()
            except (RedisError, OSError):
                log.warning(
                    "owner worker lost Redis, restarting",
                    extra={"scope": self.scope, "retry_in_s": delay},
                    exc_info=True,
                )
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=delay)
                delay = min(delay * 2, OWNER_RESTART_MAX_S)

    async def _handle(self, _msg_id: str, message: OwnerRequest) -> bool:
        request = message.root
        if request.scope != self.scope:
            return True  # another scope's owner handles it
        result: BaseModel | None
        if isinstance(request, SecretTestRequest):
            result = await self.process_secret_request(request)
        else:
            result = await self.process_model_test(request)
        if result is not None:
            await publish(self._redis, Stream.SECRET_TEST_RESULT, result, maxlen=self._maxlen)
        return True

    # ------------------------------------------------------------------ secrets

    async def process_secret_request(self, request: SecretTestRequest) -> SecretTestResult | None:
        row = await asyncio.to_thread(self._load_secret, request)
        if row is None:
            log.warning(
                "secret test request for an unknown version",
                extra={"vault_item": f"{request.scope}/{request.name}", "version": request.version},
            )
            return None
        if row.last_request_id == request.request_id or row.status not in ("pending", "active"):
            return _secret_result(request, row)
        outcome = await self._evaluate(request, row)
        return await asyncio.to_thread(self._apply, request, outcome)

    def _load_secret(self, request: SecretTestRequest) -> SecretRow | None:
        with self._factory() as session:
            row = session.get(SecretRow, (request.scope, request.name, request.version))
            if row is not None:
                session.expunge(row)
            return row

    async def _evaluate(self, request: SecretTestRequest, row: SecretRow) -> CheckOutcome:
        spec = SECRET_SPECS.get((request.scope, request.name))
        if spec is None:
            return CheckOutcome("invalid", f"unknown secret {request.scope}/{request.name}")
        try:
            plaintext = unseal(self._key, row.sealed_blob)
        except SealedBlobError:
            return CheckOutcome("invalid", "the blob cannot be opened with this scope's private key")
        if fingerprint(plaintext) != row.fingerprint:
            return CheckOutcome(
                "invalid", "fingerprint mismatch: the blob is not the secret recorded by the browser"
            )
        try:
            payload = spec.payload_model.model_validate_json(plaintext)
        except ValidationError as exc:
            problems = "; ".join(
                f"{'.'.join(str(p) for p in err['loc']) or 'payload'}: {err['msg']}"
                for err in exc.errors(include_input=False, include_url=False)
            )
            return CheckOutcome("invalid", f"payload does not match the {spec.name} schema ({problems})")
        for value in payload.model_dump().values():
            if isinstance(value, str):
                register_secret(value)
        if last4(str(getattr(payload, spec.primary_field))) != row.last4:
            return CheckOutcome("invalid", "last4 does not match the decrypted key")
        check = self._checks.get(request.name)
        if check is None:
            return CheckOutcome("invalid", f"no owner check is registered for {request.scope}/{request.name}")
        try:
            return await check(payload)
        except Exception as exc:
            return CheckOutcome("error", redact(f"{type(exc).__name__}: {exc}")[:500])

    def _apply(self, request: SecretTestRequest, outcome: CheckOutcome) -> SecretTestResult | None:
        with transaction(self._factory) as session:
            rows = session.scalars(
                select(SecretRow)
                .where(SecretRow.scope == request.scope, SecretRow.name == request.name)
                .order_by(SecretRow.version)
                .with_for_update()
            ).all()
            row = next((r for r in rows if r.version == request.version), None)
            if row is None:
                return None
            if row.last_request_id == request.request_id or row.status not in ("pending", "active"):
                return _secret_result(request, row)
            if row.status == "pending":
                _decide_pending(session, rows, row, outcome)
            elif outcome.status == "invalid":
                row.status, row.reason = "invalid", outcome.reason
            else:
                row.reason = None if outcome.status == "valid" else f"last check failed: {outcome.reason}"
            if outcome.status != "error":
                row.check_details = dict(outcome.details)
            row.checked_at = utcnow()
            row.last_request_id = request.request_id
            log.info(
                "secret test processed",
                extra={
                    "vault_item": f"{row.scope}/{row.name}",
                    "version": row.version,
                    "status": row.status,
                },
            )
            return _secret_result(request, row)

    # ------------------------------------------------------------------ model tests

    async def process_model_test(self, request: ModelTestRequest) -> ModelTestResult | None:
        status = await asyncio.to_thread(self._model_test_status, request.request_id)
        if status != "pending":
            return None
        if self._runner is None:
            outcome = ModelTestOutcome(
                None, None, None, None, None, "model tests are not available in this service"
            )
        else:
            try:
                outcome = await self._runner(request)
            except Exception as exc:
                outcome = ModelTestOutcome(
                    None, None, None, None, None, redact(f"{type(exc).__name__}: {exc}")[:500]
                )
        return await asyncio.to_thread(self._complete_model_test, request, outcome)

    def _model_test_status(self, request_id: str) -> str | None:
        with self._factory() as session:
            return session.scalar(select(ModelTestRow.status).where(ModelTestRow.request_id == request_id))

    def _complete_model_test(
        self, request: ModelTestRequest, outcome: ModelTestOutcome
    ) -> ModelTestResult | None:
        with transaction(self._factory) as session:
            row = session.get(ModelTestRow, request.request_id, with_for_update=True)
            if row is None or row.status != "pending":
                return None
            row.status = "error" if outcome.error else "done"
            row.schema_valid, row.latency_ms, row.cost_usd = (
                outcome.schema_valid,
                outcome.latency_ms,
                outcome.cost_usd,
            )
            row.provider, row.model_returned, row.error = (
                outcome.provider,
                outcome.model_returned,
                outcome.error,
            )
            row.completed_at = utcnow()
            return ModelTestResult(
                request_id=row.request_id,
                role=request.role,
                status="error" if outcome.error else "done",
                schema_valid=row.schema_valid,
                latency_ms=row.latency_ms,
                cost_usd=row.cost_usd,
                provider=row.provider,
                model_returned=row.model_returned,
                error=row.error,
                completed_at=row.completed_at,
            )


def _decide_pending(
    session: Session, rows: Sequence[SecretRow], row: SecretRow, outcome: CheckOutcome
) -> None:
    if outcome.status == "invalid":
        row.status, row.reason = "invalid", outcome.reason
        return
    if outcome.status == "error":
        row.status, row.reason = "invalid", f"check could not complete: {outcome.reason}"
        return
    if any(r.status == "active" and r.version > row.version for r in rows):
        row.status, row.reason = "retired", "superseded by a newer active version"
        return
    for other in rows:
        if other.status == "active" and other is not row:
            other.status, other.reason = "retired", f"replaced by version {row.version}"
    session.flush()  # retire first: at most one active version per secret (partial unique index)
    row.status, row.reason = "active", None


def _secret_result(request: SecretTestRequest, row: SecretRow) -> SecretTestResult:
    return SecretTestResult(
        request_id=request.request_id,
        scope=request.scope,
        name=request.name,
        version=request.version,
        status=row.status,
        reason=row.reason,
        checked_at=row.checked_at,
        details=dict(row.check_details or {}),
    )
