"""The config-api live gate on the migrated schema (finals recorded as `hdt_scorer`, the gate read and the
saves run as `hdt_configapi`):

- G4 comes from the newest final evaluation (`golive_g4_finals`), never from a gate flag: a flag that does
  not repeat that final is refused by the database, and a forged one changes nothing (review I-4);
- a pass is bound to the configuration pinned over its windows, compared by content (`hdt.golive.pins`): a
  `models` / `council` change, a new agent version or another static file closes it with
  `g4_config_changed`, a rollback to the pinned content reopens it, and a new split plus a new final
  evaluation is the re-evaluation path (review I-5);
- while `live` is active, `models` and `council` saves and rollbacks are refused, checked again inside the
  save transaction, and a `live` save that only lowers the size skips the G4 checks (review N6, I-5)."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import timedelta
from itertools import count
from pathlib import Path
from typing import Any, TypeVar, cast

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from redis.exceptions import RedisError
from sqlalchemy.dialects.postgresql import distinct_on
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from hdt.configapi.auth import AuthContext
from hdt.configapi.context import AppState
from hdt.configapi.errors import ApiError
from hdt.configapi.routes import register_section_policies
from hdt.configapi.routes.mode import LivePinnedPolicy, ModePolicy, gate_body, read_live_gate
from hdt.configapi.sections import SaveContext, save_section
from hdt.contracts.common import Account
from hdt.core.clock import utcnow
from hdt.core.config import static_config
from hdt.db.models.settings import AgentVersionRow, ConfigVersionRow
from hdt.golive.pins import current_static_digests
from hdt.settings import store
from hdt.settings.schemas import (
    GOLIVE_ITEMS,
    GoLiveItem,
    GoLiveSection,
    ModelsSection,
    ModeSection,
    RoleModelConfig,
    Section,
)

pytestmark = [pytest.mark.pg, pytest.mark.integration]

ROOT = Path(__file__).resolve().parents[2]
T = TypeVar("T")
_EVIDENCE = count(1)


@dataclass
class Db:
    admin: sa.Engine
    role: Callable[[str], sa.Engine]


@pytest.fixture
def db(
    fresh_database: Callable[[], AbstractContextManager[str]], pg_role_url: Callable[[str, str], str]
) -> Iterator[Db]:
    register_section_policies()
    with fresh_database() as url:
        cfg = Config(str(ROOT / "alembic.ini"))
        cfg.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
        command.upgrade(cfg, "head")
        engines: dict[str, sa.Engine] = {}

        def role(name: str) -> sa.Engine:
            if name not in engines:
                engines[name] = sa.create_engine(pg_role_url(url, name))
            return engines[name]

        admin = sa.create_engine(url)
        try:
            with Session(admin) as session, session.begin():
                _ready_for_live(session)
            yield Db(admin, role)
        finally:
            for engine in (*engines.values(), admin):
                engine.dispose()


def _ready_for_live(session: Session) -> None:
    """Every live prerequisite but G4: seeded config, a `models` version with its agent version, an active
    live key, a complete checklist."""
    store.seed_if_missing(session, static_config())
    role = RoleModelConfig.model_validate(
        {"model": "anthropic/claude-sonnet-4.5", "temperature": 0.2, "max_tokens": 800, "timeout_s": 30}
    )
    models = ModelsSection(roles={"technical": role})
    version = store.create_version(
        session, Section.MODELS, models, author="owner", reason="models", parent_id=None
    )
    store.record_agent_versions(session, models, config_version_id=version.id, author="owner")
    golive = store.get_active(session, Section.GOLIVE)
    assert golive is not None
    now = utcnow()
    store.create_version(
        session,
        Section.GOLIVE,
        GoLiveSection(
            items=tuple(
                GoLiveItem(id=item_id, checked=True, checked_by="owner", checked_at=now)
                for item_id, _ in GOLIVE_ITEMS
            )
        ),
        author="owner",
        reason="checklist done",
        parent_id=golive.id,
    )
    session.execute(
        sa.text(
            "INSERT INTO secrets (scope, name, version, sealed_blob, last4, fingerprint, status, "
            "check_details, created_by, created_at) VALUES ('exec', 'binance_live', 1, '\\x00', 'abcd', "
            "'0123456789abcdef', 'active', '{}', 'owner', now())"
        )
    )


def _active_ids(db: Db) -> dict[str, int | None]:
    with Session(db.admin) as session:
        out: dict[str, int | None] = {}
        for section in (Section.MODELS, Section.COUNCIL):
            version = store.get_active(session, section)
            out[section.value] = version.id if version else None
        return out


def _pins(db: Db) -> dict[str, Any]:
    """The pins a final evaluation of today's configuration records."""
    with Session(db.admin) as session:
        a = AgentVersionRow
        newest = session.execute(
            sa.select(a.agent, a.version, a.model_slug)
            .ext(distinct_on(a.agent))
            .order_by(a.agent, a.version.desc())
        ).all()
    agents = {r.agent: {"model_slug": r.model_slug, "agent_version": str(r.version)} for r in newest}
    return {**_active_ids(db), "agents": agents, "static": current_static_digests()}


def _record_final(
    db: Db,
    *,
    passed: bool = True,
    pins: dict[str, Any] | None = None,
    targets: tuple[str, ...] = ("RAW_12H", "RESID_12H"),
    pinned: bool = True,
) -> str:
    """A new split of both target types and the final G4 evaluation of `targets` on those splits, pinned to
    today's configuration, plus its flag, as `hdt_scorer`. Returns the evidence ref."""
    evidence = f"reports/golive/g4/final-{next(_EVIDENCE)}.json#sha256=ab"
    with db.admin.begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO golive_eval_splits (target_type, eval_start, decided_by, reason) "
                "VALUES ('RAW_12H', now() + interval '1 day', 'owner', 'split'), "
                "('RESID_12H', now() + interval '1 day', 'owner', 'split')"
            )
        )
    stored = (pins if pins is not None else _pins(db)) if pinned else None
    with db.role("hdt_scorer").begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO golive_g4_finals (target_type, generation, eval_start, eval_end, passed, "
                "gate_passed, evidence_ref, result, pins) SELECT DISTINCT ON (target_type) target_type, "
                "generation, eval_start, eval_start + interval '30 days', :passed, :passed, :evidence, '{}', "
                "CAST(:pins AS jsonb) FROM golive_eval_splits WHERE target_type = ANY(:targets) "
                "ORDER BY target_type, generation DESC"
            ),
            {
                "passed": passed,
                "evidence": evidence,
                "pins": json.dumps(stored) if stored is not None else None,
                "targets": list(targets),
            },
        )
        conn.execute(
            sa.text(
                "INSERT INTO gate_flags (gate, passed, evidence_ref, decided_by, decided_at) "
                "VALUES ('G4', :passed, :evidence, 'scorer:g4', now())"
            ),
            {"passed": passed, "evidence": evidence},
        )
    return evidence


def _changed_model(section: Section, current: Any) -> Any:
    payload = dict(current.payload)
    if section is Section.MODELS:
        role = dict(payload["roles"]["technical"])
        role["temperature"] = round(float(role["temperature"]) + 0.1, 4)
        payload["roles"] = {**payload["roles"], "technical": role}
    else:
        payload["stance_long"] = round(float(payload["stance_long"]) + 0.01, 4)
    return type(current.model()).model_validate(payload)


def _change(db: Db, section: Section) -> int:
    """A content change of `section` activated directly (as a pinned save that won the race would be)."""
    with Session(db.admin) as session, session.begin():
        current = store.get_active(session, section)
        assert current is not None
        model = _changed_model(section, current)
        version = store.create_version(
            session, section, model, author="owner", reason="edit", parent_id=current.id
        )
        return version.id


def _restore(db: Db, section: Section, version_id: int) -> None:
    """Activate a new version carrying the payload of `version_id` (a rollback is a new id)."""
    with Session(db.admin) as session, session.begin():
        row = session.get(ConfigVersionRow, version_id)
        assert row is not None
        copy = ConfigVersionRow(
            section=row.section,
            payload_json=row.payload_json,
            payload_sha256=row.payload_sha256,
            schema_version=row.schema_version,
            author="owner",
            reason="rollback",
            created_at=utcnow(),
            parent_id=row.id,
        )
        session.add(copy)
        session.flush()
        assert store.activate(session, section, copy.id, by="owner")


def _missing(db: Db) -> list[str]:
    with sessionmaker(db.role("hdt_configapi"))() as session:
        return read_live_gate(session).missing(step_up=True)


# ------------------------------------------------------------------------------------------ the gate


def test_the_gate_opens_while_the_pinned_config_is_active(db: Db) -> None:
    assert _missing(db) == ["g4_gate_not_passed"]
    _record_final(db)
    with sessionmaker(db.role("hdt_configapi"))() as session:
        gate = read_live_gate(session)
    assert gate.missing(step_up=True) == []
    body = gate_body(gate, step_up=True)
    assert body["g4_config_current"] is True
    assert body["g4_config"]["active"] == _active_ids(db)
    assert body["g4_config"]["changes"] == []
    # a section outside the pin (risk) may get a new version without closing the gate
    with Session(db.admin) as session:
        risk = store.get_active(session, Section.RISK)
    assert risk is not None
    _restore(db, Section.RISK, risk.id)
    assert _missing(db) == []


@pytest.mark.parametrize("section", [Section.MODELS, Section.COUNCIL])
def test_a_config_change_closes_the_gate_and_a_rollback_to_the_pinned_content_reopens_it(
    db: Db, section: Section
) -> None:
    _record_final(db)
    pinned = _active_ids(db)[section.value]
    assert pinned is not None
    _change(db, section)
    assert _missing(db) == ["g4_config_changed"]
    _restore(db, section, pinned)
    assert _active_ids(db)[section.value] != pinned
    assert _missing(db) == []


def test_a_new_agent_version_or_another_static_file_closes_the_gate(db: Db) -> None:
    """Review M-12: agent pins and the static YAML are part of the binding."""
    _record_final(db)
    with sessionmaker(db.role("hdt_configapi"))() as session:
        other = {**current_static_digests(), "scanner": "0" * 64}
        gate = read_live_gate(session, static=other)
    assert gate.g4_changes == ("static:scanner",)
    assert gate.missing(step_up=True) == ["g4_config_changed"]
    # the council records a new prompt for `technical`: same model, new agent version
    with Session(db.admin) as session, session.begin():
        row = session.get(AgentVersionRow, ("technical", 1))
        assert row is not None
        session.add(
            AgentVersionRow(
                agent="technical",
                version=2,
                model_slug=row.model_slug,
                provider_prefs_hash=row.provider_prefs_hash,
                prompt_hash="new-prompt",
                skill_commit=row.skill_commit,
                config_version_id=row.config_version_id,
                created_by="council",
                created_at=utcnow(),
            )
        )
    with sessionmaker(db.role("hdt_configapi"))() as session:
        assert read_live_gate(session).g4_changes == ("agent:technical",)


def test_a_passed_final_without_pins_fails_closed(db: Db) -> None:
    _record_final(db, pinned=False)
    assert _missing(db) == ["g4_config_changed"]


def test_a_failed_final_keeps_its_reason(db: Db) -> None:
    _record_final(db, passed=False)
    _change(db, Section.MODELS)
    assert _missing(db) == ["g4_gate_not_passed"]


def test_a_forged_flag_cannot_override_a_failed_final(db: Db) -> None:
    """Review I-4: one INSERT into `gate_flags` must not open live after a failed final."""
    evidence = _record_final(db, passed=False)
    forge = sa.text(
        "INSERT INTO gate_flags (gate, passed, evidence_ref, decided_by, decided_at) "
        "VALUES ('G4', true, :evidence, 'scorer:g4', now() + interval '1 second')"
    )
    scorer = db.role("hdt_scorer")
    with pytest.raises(IntegrityError, match="must repeat the outcome"), scorer.begin() as conn:
        conn.execute(forge, {"evidence": evidence})
    assert _missing(db) == ["g4_gate_not_passed"]
    # even a flag that bypassed the trigger is never read by the live gate
    with db.admin.begin() as conn:
        conn.execute(sa.text("ALTER TABLE gate_flags DISABLE TRIGGER gate_flags_g4_final"))
        conn.execute(forge, {"evidence": evidence})
        conn.execute(sa.text("ALTER TABLE gate_flags ENABLE TRIGGER gate_flags_g4_final"))
    assert _missing(db) == ["g4_gate_not_passed"]


def test_a_flag_cannot_repeat_an_older_final(db: Db) -> None:
    passed = _record_final(db)
    _record_final(db, passed=False)
    assert _missing(db) == ["g4_gate_not_passed"]
    with pytest.raises(IntegrityError, match="not the newest final"), db.role("hdt_scorer").begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO gate_flags (gate, passed, evidence_ref, decided_by, decided_at) "
                "VALUES ('G4', true, :evidence, 'scorer:g4', now())"
            ),
            {"evidence": passed},
        )


def test_a_final_must_cover_every_split_target_type(db: Db) -> None:
    _record_final(db, targets=("RAW_12H",))
    with sessionmaker(db.role("hdt_configapi"))() as session:
        gate = read_live_gate(session)
    assert gate.g4 is not None
    assert gate.g4["uncovered"] == ["RESID_12H"]
    assert gate.missing(step_up=True) == ["g4_gate_not_passed"]


def test_a_new_split_and_final_re_evaluate_a_changed_config(db: Db) -> None:
    """Review I-5: the documented remediation (a new split and a new final evaluation) works."""
    _record_final(db)
    _change(db, Section.MODELS)
    assert _missing(db) == ["g4_config_changed"]
    _record_final(db)  # generation 2 of both splits, evaluated on the new config
    with Session(db.admin) as session:
        generations = session.execute(
            sa.text("SELECT target_type, max(generation) FROM golive_eval_splits GROUP BY target_type")
        ).all()
    assert dict(generations) == {"RAW_12H": 2, "RESID_12H": 2}
    assert _missing(db) == []


# ------------------------------------------------------------------------------------------- the saves


class _Models:
    """The catalog view `ModelsPolicy` reads: every slug is a structured-output model."""

    def get(self, slug: str) -> object:
        return object()


class _CatalogCache:
    async def get(self, *, refresh: bool = False) -> _Models:
        return _Models()


class _Redis:
    """Publishing `config_changed` fails; the save is already committed (services reconcile)."""

    def __getattr__(self, name: str) -> Callable[..., Any]:
        async def fail(*args: object, **kwargs: object) -> None:
            raise RedisError("offline")

        return fail


@dataclass
class _State:
    """The part of `AppState` a section save uses, on the `hdt_configapi` role."""

    sessions: sessionmaker[Session]
    static: Any
    redis: Any
    catalog: Any

    async def db(self, fn: Callable[[Session], T]) -> T:
        with self.sessions.begin() as s:
            return fn(s)

    def stream_maxlen(self, stream: str) -> int | None:
        return None


def _state(db: Db) -> AppState:
    state = _State(sessionmaker(db.role("hdt_configapi")), static_config(), _Redis(), _CatalogCache())
    return cast(AppState, state)


def _auth() -> AuthContext:
    now = utcnow()
    return AuthContext(
        user_id=1,
        username="operator",
        token_hash="t" * 64,
        csrf_token="c" * 32,
        expires_at=now + timedelta(hours=1),
        step_up_until=now + timedelta(minutes=5),
        ip=None,
    )


async def _save(db: Db, section: Section, payload: dict[str, Any], *, rollback_of: int | None = None) -> int:
    state = _state(db)
    active = await state.db(lambda s: store.get_active(s, section))
    result = await save_section(
        state,
        _auth(),
        section,
        payload,
        reason="test",
        parent_id=active.id if active else None,
        rollback_of=rollback_of,
    )
    return result.version.id


async def _save_change(db: Db, section: Section) -> int:
    state = _state(db)
    active = await state.db(lambda s: store.get_active(s, section))
    assert active is not None
    return await _save(db, section, _changed_model(section, active).model_dump(mode="json"))


async def _set_mode(db: Db, mode: Account, size: float) -> int:
    return await _save(db, Section.MODE, {"mode": mode.value, "size_multiplier": size})


async def test_models_and_council_saves_pass_in_paper_and_are_refused_while_live(db: Db) -> None:
    """Review N6: a `models` or `council` change while live is refused (save and rollback alike)."""
    first_models = _active_ids(db)["models"]
    assert first_models is not None
    await _save_change(db, Section.MODELS)
    await _save_change(db, Section.COUNCIL)
    _record_final(db)
    await _set_mode(db, Account.LIVE, 0.25)
    before = _active_ids(db)
    for section in (Section.MODELS, Section.COUNCIL):
        with pytest.raises(ApiError) as err:
            await _save_change(db, section)
        assert (err.value.status, err.value.code) == (409, "live_config_locked")
    with Session(db.admin) as session:
        payload = store.get_version(session, first_models, Section.MODELS).payload
    with pytest.raises(ApiError) as err:
        await _save(db, Section.MODELS, payload, rollback_of=first_models)
    assert err.value.code == "live_config_locked"
    assert _active_ids(db) == before
    assert _missing(db) == []


def _write(db: Db, ctx: SaveContext, policy: Any, model: Any) -> None:
    """The write half of a section save (`hdt.configapi.sections.save_section`) after its check passed."""
    with sessionmaker(db.role("hdt_configapi")).begin() as session:
        parent = ctx.active.id if ctx.active else None
        version = store.create_version(
            session, ctx.section, model, author="operator", reason="x", parent_id=parent
        )
        policy.after_create(session, ctx, version, model)


async def test_the_save_transaction_rechecks_the_live_binding(db: Db) -> None:
    """The check-then-write gap: a mode save and a pinned save that both passed their checks cannot both
    commit, because each re-checks under the pinned-config lock inside its transaction."""
    _record_final(db)
    state = _state(db)
    paper = await state.db(lambda s: store.get_active(s, Section.MODE))
    live = ModeSection(mode=Account.LIVE, size_multiplier=0.25)
    ctx = SaveContext(state, _auth(), Section.MODE, paper)
    await ModePolicy().check(ctx, live)  # the gate is open when checked
    _change(db, Section.COUNCIL)  # a council save commits in between
    with pytest.raises(ApiError) as err:
        _write(db, ctx, ModePolicy(), live)
    assert err.value.code == "live_gate_closed"
    assert err.value.extra["missing"] == ["g4_config_changed"]

    # the other order: a council save passed its check, then live committed (after a new evaluation)
    _record_final(db)
    council = await state.db(lambda s: store.get_active(s, Section.COUNCIL))
    assert council is not None
    council_ctx = SaveContext(state, _auth(), Section.COUNCIL, council)
    model = _changed_model(Section.COUNCIL, council)
    await LivePinnedPolicy().check(council_ctx, model)  # paper when checked
    await _set_mode(db, Account.LIVE, 0.25)
    with pytest.raises(ApiError) as err:
        _write(db, council_ctx, LivePinnedPolicy(), model)
    assert err.value.code == "live_config_locked"
    assert _active_ids(db)["council"] == council.id


async def test_lowering_the_size_while_live_skips_the_g4_checks(db: Db) -> None:
    """Review I-5: once the binding is broken while live, the size can still come down, never go up."""
    _record_final(db)
    await _set_mode(db, Account.LIVE, 0.25)
    _change(db, Section.COUNCIL)  # e.g. a change that landed despite the lock (DB admin)
    assert _missing(db) == ["g4_config_changed"]
    await _set_mode(db, Account.LIVE, 0.1)
    with pytest.raises(ApiError) as err:
        await _set_mode(db, Account.LIVE, 0.25)
    assert err.value.code == "live_gate_closed"
    assert err.value.extra["missing"] == ["g4_config_changed"]
