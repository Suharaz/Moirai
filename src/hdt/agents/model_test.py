"""Structured-output self-test of a role's model (phase 05 Red Team Delta, test-strategy 3.4).

`run_model_test` sends one uncached structured request (json_schema strict on OpenRouter, JSON Output on
DeepSeek) with the role's exact configuration and checks that the reply parses and carries the fixed
expected answer. It serves two callers:

- `model_test_runner(llm, health)`: the `ModelTestRunner` of the `llm` scope `OwnerWorker` (console model
  tests, `model_tests` table).
- `RoleHealth`: the startup and on-change self-test of every configured role, in every service that owns
  an `LlmRouter` (council agents, news extractor and judges, veto scan, reflection). Constructing it on a
  router attaches it as the router's role gate, so a role whose test fails is closed at the send point:
  every call of that role raises `LlmRoleClosedError` (`reason` `model_self_test_failed`) whatever the
  pipeline, `LlmAgentRunner` abstains with that reason, and a critical `config_invalid` alert is raised
  (resolved when a later test of the same role passes). There is no silent schema downgrade. A failed or
  unverifiable role is re-tested after `retry_after_s`. Replay never tests and never closes a role (it
  sends no request).
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Final

from pydantic import Field
from sqlalchemy.orm import Session, sessionmaker

from hdt.agents.llm_router import ROLE_PIPELINES, LlmRouter
from hdt.agents.llm_types import LlmOutputError, LlmUnavailableError
from hdt.contracts.common import ContractModel, DirectionHint
from hdt.core import alerts
from hdt.core.config import LlmRole
from hdt.core.ids import canonical_sha256
from hdt.db.session import transaction
from hdt.settings.schemas import ModelsSection, RoleModelConfig
from hdt.vault.owner import ModelTestOutcome, ModelTestRequest, ModelTestRunner
from hdt.vault.redact import redact

log = logging.getLogger(__name__)

CLOSED_REASON: Final[str] = "model_self_test_failed"
SELF_TEST_SYSTEM: Final[str] = (
    "You are a structured-output self-test. Reply only with the JSON object the schema describes."
)
SELF_TEST_USER: Final[str] = (
    'Fill the schema exactly as follows. total: the integer sum of 17 and 25. direction: "down". '
    'probability: 0.25. items: two items in this order, {"key": "a", "count": 1} then '
    '{"key": "b", "count": 2}. note: null.'
)
_ERROR_CHARS: Final[int] = 500


class SelfTestItem(ContractModel):
    key: str = Field(min_length=1, max_length=8)
    count: int


class SelfTestReply(ContractModel):
    """Exercises what the agent schemas need: integers, enums, bounded floats, nested arrays, nulls."""

    total: int
    direction: DirectionHint
    probability: float = Field(gt=0, lt=1)
    items: tuple[SelfTestItem, ...] = Field(max_length=4)
    note: str | None = None


EXPECTED: Final[SelfTestReply] = SelfTestReply(
    total=42,
    direction=DirectionHint.DOWN,
    probability=0.25,
    items=(SelfTestItem(key="a", count=1), SelfTestItem(key="b", count=2)),
    note=None,
)


async def run_model_test(llm: LlmRouter, role: LlmRole, config: RoleModelConfig) -> ModelTestOutcome:
    """`schema_valid` True: parsed and correct; False: the model cannot keep the schema (or answers
    wrongly); None: the test could not run (transport, key, provider or model refusal)."""
    if llm.mode != "live":
        return ModelTestOutcome(None, None, None, None, None, "model tests need the live router")
    started = time.monotonic()
    try:
        generation = await llm.structured(
            role=role,
            pipeline=ROLE_PIPELINES[role],
            event_id=None,
            config=config,
            system=SELF_TEST_SYSTEM,
            user=SELF_TEST_USER,
            schema=SelfTestReply,
            self_test=True,
        )
    except LlmOutputError as exc:
        latency = round((time.monotonic() - started) * 1000)
        return ModelTestOutcome(False, latency, None, None, None, _error(exc))
    except LlmUnavailableError as exc:
        return ModelTestOutcome(None, None, None, None, None, _error(exc))
    latency = round((time.monotonic() - started) * 1000)
    reply = generation.output
    correct = (
        reply.total == EXPECTED.total
        and reply.direction is EXPECTED.direction
        and abs(reply.probability - EXPECTED.probability) < 1e-9
        and reply.items == EXPECTED.items
        and reply.note is None
    )
    return ModelTestOutcome(
        schema_valid=correct,
        latency_ms=latency,
        cost_usd=generation.usage.cost_usd,
        provider=generation.provider or None,
        model_returned=generation.model_returned or None,
        error=None if correct else "the reply parsed but did not carry the requested values",
    )


def _error(exc: Exception) -> str:
    return redact(f"{type(exc).__name__}: {exc}")[:_ERROR_CHARS]


def config_fingerprint(role: LlmRole, config: RoleModelConfig) -> str:
    return canonical_sha256({"role": role, "config": config.model_dump(mode="json")})


@dataclass(frozen=True)
class RoleVerdict:
    fingerprint: str
    model: str
    passed: bool
    detail: str | None
    checked_at: float
    """Monotonic seconds."""


class RoleHealth:
    """Self-test verdicts of the configured roles in this process, and the role gate of `llm`.

    Service use: `health = RoleHealth(llm, sessions, service=SERVICE)`, then `await
    health.check_all(models)` at startup and after each config change. Every later `llm.structured` /
    `llm.tool_step` of a closed role raises `LlmRoleClosedError`; `closed_reason(role, config)` answers
    the same question without a call. Build a service's roles through one `RoleHealth` per router.
    """

    def __init__(
        self,
        llm: LlmRouter,
        session_factory: sessionmaker[Session] | None,
        *,
        service: str = "council",
        retry_after_s: float = 600.0,
        clock: Callable[[], float] = time.monotonic,
        tester: Callable[[LlmRouter, LlmRole, RoleModelConfig], Awaitable[ModelTestOutcome]] = run_model_test,
    ) -> None:
        self._llm = llm
        self._factory = session_factory
        self._service = service
        self._retry_after_s = retry_after_s
        self._clock = clock
        self._tester = tester
        self._verdicts: dict[LlmRole, RoleVerdict] = {}
        self._locks: dict[LlmRole, asyncio.Lock] = {}
        self._alerted: set[LlmRole] = set()
        if isinstance(llm, LlmRouter):
            llm.gate_with(self)

    def verdict(self, role: LlmRole) -> RoleVerdict | None:
        return self._verdicts.get(role)

    async def closed_reason(self, role: LlmRole, config: RoleModelConfig) -> str | None:
        """None when the role may run with `config`; otherwise why it is closed (tests it first when the
        configuration was never tested in this process or its failed verdict is due for a re-test)."""
        if self._llm.mode != "live":
            return None
        fingerprint = config_fingerprint(role, config)
        verdict = self._current(role, fingerprint)
        if verdict is None:
            lock = self._locks.setdefault(role, asyncio.Lock())
            async with lock:
                verdict = self._current(role, fingerprint)
                if verdict is None:
                    outcome = await self._tester(self._llm, role, config)
                    verdict = await self.observe(role, config, outcome)
        return None if verdict.passed else CLOSED_REASON

    async def check_all(self, models: ModelsSection) -> dict[LlmRole, str | None]:
        """Test every configured role not yet tested with its current configuration (startup, change)."""
        roles = sorted(models.roles)
        reasons = await asyncio.gather(*(self.closed_reason(role, models.roles[role]) for role in roles))
        return dict(zip(roles, reasons, strict=True))

    async def observe(self, role: LlmRole, config: RoleModelConfig, outcome: ModelTestOutcome) -> RoleVerdict:
        """Record a test outcome for `role` with `config`, raising or resolving the closed-role alert."""
        verdict = RoleVerdict(
            fingerprint=config_fingerprint(role, config),
            model=config.model,
            passed=outcome.schema_valid is True,
            detail=outcome.error,
            checked_at=self._clock(),
        )
        self._verdicts[role] = verdict
        if verdict.passed:
            if role in self._alerted:
                await asyncio.to_thread(self._resolve, role)
                self._alerted.discard(role)
        else:
            log.error(
                "llm role closed: structured-output self-test failed",
                extra={"role": role, "model": config.model, "detail": outcome.error},
            )
            await asyncio.to_thread(self._raise, role, config, outcome)
            self._alerted.add(role)
        return verdict

    def _current(self, role: LlmRole, fingerprint: str) -> RoleVerdict | None:
        verdict = self._verdicts.get(role)
        if verdict is None or verdict.fingerprint != fingerprint:
            return None
        if not verdict.passed and self._clock() - verdict.checked_at >= self._retry_after_s:
            return None
        return verdict

    def _raise(self, role: LlmRole, config: RoleModelConfig, outcome: ModelTestOutcome) -> None:
        title = f"LLM role {role} closed: structured-output self-test of {config.model} failed"
        detail = outcome.error or "no detail"
        try:
            if self._factory is None:
                alerts.alert(
                    None,
                    kind="config_invalid",
                    severity="critical",
                    title=title,
                    detail=detail,
                    service=self._service,
                    episode=_episode(role),
                )
                return
            with transaction(self._factory) as session:
                alerts.alert(
                    session,
                    kind="config_invalid",
                    severity="critical",
                    title=title,
                    detail=detail,
                    service=self._service,
                    episode=_episode(role),
                )
        except Exception:
            log.exception("could not raise the closed-role alert", extra={"role": role})

    def _resolve(self, role: LlmRole) -> None:
        if self._factory is None:
            return
        try:
            with transaction(self._factory) as session:
                alerts.resolve(
                    session,
                    kind="config_invalid",
                    account=None,
                    service=self._service,
                    episode=_episode(role),
                )
        except Exception:
            log.exception("could not resolve the closed-role alert", extra={"role": role})


def _episode(role: LlmRole) -> str:
    return f"llm_self_test:{role}"


def model_test_runner(llm: LlmRouter, health: RoleHealth | None = None) -> ModelTestRunner:
    """The `ModelTestRunner` for the `llm` scope `OwnerWorker` (console model tests).

    When `health` is given and the tested configuration is the one the role currently runs with, the
    outcome also updates the role's verdict (a failing console test closes the role, a passing one reopens
    it)."""

    async def run(request: ModelTestRequest) -> ModelTestOutcome:
        outcome = await run_model_test(llm, request.role, request.config)
        if health is not None:
            current = health.verdict(request.role)
            if current is not None and current.fingerprint == config_fingerprint(
                request.role, request.config
            ):
                await health.observe(request.role, request.config, outcome)
        return outcome

    return run
