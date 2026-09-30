"""`/mode`: run mode `paper | testnet | live` and `size_multiplier`, with the live gate.

Every mode change needs a fresh step-up TOTP. Enabling `live` (or saving any version whose mode is `live`)
additionally needs:
- G4 passed in the newest final evaluation (`golive_g4_finals`, written once per split by phase 11): every
  row of that evaluation passed and it covers every target type that has a split. `gate_flags` is read only
  for the `decided_by` shown with it and never decides, so no flag can override a final;
- today's configuration still the one pinned over that evaluation's windows (`hdt.golive.pins`: the
  `models` and `council` payloads, the agent versions and the static digests, compared by content, so a
  rollback to the pinned content restores the gate);
- the Binance live key active (validated by its owner) and the go-live checklist complete.
A `live` save that keeps `live` at the same or a lower `size_multiplier` only reduces exposure: it skips the
two G4 conditions (it still needs the key, the checklist and the step-up). The G4 conditions are re-checked
inside the save transaction under the pinned-config lock.

While the active mode is `live`, a `models` or `council` save or rollback is refused (409
`live_config_locked`, `LivePinnedPolicy`): live would otherwise trade a forecaster G4 never evaluated. The
conservative path is the phase 12 one: switch to `paper` (refused while the `live` ledger holds exposure, so
close or flatten first), change the config, then set new splits and run a new final evaluation
(`python -m hdt.golive.split`, `python -m hdt.golive.final`) before `live` opens again. A mode save and a
pinned-section save take the same advisory lock inside their transactions, so the two can never interleave.

A change that would stop a namespace (`testnet` or `live`) while its ledger still holds an open position or
a working order is refused: close them first. The same rules apply to `POST /config/<section>` and to
rollbacks, because they run through the same section policies.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, ConfigDict, Field, PositiveInt
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from hdt.configapi.auth import AuthContext, require_session, require_step_up
from hdt.configapi.context import get_state
from hdt.configapi.errors import ApiError
from hdt.configapi.routes.secrets import secret_metadata
from hdt.configapi.sections import SaveContext, SectionPolicy, save_section, version_dict
from hdt.contracts.common import Account
from hdt.db.models.golive import GoLiveEvalSplitRow, GoLiveG4FinalRow
from hdt.db.models.settings import GateFlagRow
from hdt.execution.namespaces import enabled_accounts, has_exposure
from hdt.golive.pins import current_static_digests, pin_changes
from hdt.settings import store
from hdt.settings.schemas import GoLiveSection, ModeSection, Section, SectionModel, default_size_multiplier
from hdt.settings.versions import ConfigVersion

router = APIRouter(prefix="/mode", tags=["mode"])
LIVE_GATE = "G4"
LIVE_KEY = ("exec", "binance_live")
PINNED_SECTIONS = (Section.MODELS, Section.COUNCIL)
"""Console config sections a G4 pass is bound to (`hdt.golive.pins`)."""
G4_REASONS = frozenset({"g4_gate_not_passed", "g4_config_changed"})
_PIN_LOCK = text("SELECT pg_advisory_xact_lock(hashtext('hdt.config.live_pins'))")


@dataclass(frozen=True)
class LiveGate:
    g4: dict[str, Any] | None
    """The newest final G4 evaluation (None: none recorded): `passed`, `evidence_ref`, `decided_by`,
    `decided_at`, `splits` (target type -> generation) and `uncovered` (split target types it lacks)."""
    live_key: dict[str, Any] | None
    checklist_done: int
    checklist_total: int
    missing_checklist: list[str]
    g4_pins: tuple[dict[str, Any] | None, ...] = ()
    """`golive_g4_finals.pins` per target type of that evaluation (None: not pinned)."""
    active_pins: dict[str, int | None] = field(default_factory=dict)
    """The active version id of each of `PINNED_SECTIONS` (None: no console version is active)."""
    g4_changes: tuple[str, ...] = ()
    """What differs from the pins today (`hdt.golive.pins.pin_changes`)."""

    @property
    def g4_passed(self) -> bool:
        return bool(self.g4 and self.g4["passed"])

    @property
    def g4_config_current(self) -> bool:
        """The evaluation recorded its pins and today's configuration matches them for every target type
        (fails closed without a recorded final)."""
        return bool(self.g4_pins) and not self.g4_changes

    @property
    def live_key_valid(self) -> bool:
        return self.live_key is not None

    @property
    def checklist_complete(self) -> bool:
        return self.checklist_total > 0 and self.checklist_done == self.checklist_total

    def missing(self, *, step_up: bool, g4_required: bool = True) -> list[str]:
        missing = []
        if g4_required:
            if not self.g4_passed:
                missing.append("g4_gate_not_passed")
            elif not self.g4_config_current:
                missing.append("g4_config_changed")
        if not self.live_key_valid:
            missing.append("live_key_not_valid")
        if not self.checklist_complete:
            missing.append("checklist_incomplete")
        if not step_up:
            missing.append("totp_step_up_required")
        return missing


def _newest_final(session: Session) -> tuple[dict[str, Any] | None, tuple[dict[str, Any] | None, ...]]:
    f = GoLiveG4FinalRow
    evidence = session.scalar(
        select(f.evidence_ref).order_by(f.evaluated_at.desc(), f.generation.desc()).limit(1)
    )
    if evidence is None:
        return None, ()
    rows = session.execute(
        select(f.target_type, f.generation, f.passed, f.gate_passed, f.pins, f.evaluated_at)
        .where(f.evidence_ref == evidence)
        .order_by(f.target_type)
    ).all()
    split_types = set(session.scalars(select(GoLiveEvalSplitRow.target_type).distinct()))
    uncovered = sorted(split_types - {r.target_type for r in rows})
    flag = session.scalar(
        select(GateFlagRow.decided_by)
        .where(GateFlagRow.gate == LIVE_GATE, GateFlagRow.evidence_ref == evidence)
        .order_by(GateFlagRow.decided_at.desc(), GateFlagRow.id.desc())
        .limit(1)
    )
    g4 = {
        "passed": bool(rows) and not uncovered and all(r.passed and r.gate_passed for r in rows),
        "evidence_ref": evidence,
        "decided_by": flag,
        "decided_at": max(r.evaluated_at for r in rows),
        "splits": {r.target_type: r.generation for r in rows},
        "uncovered": uncovered,
    }
    return g4, tuple(r.pins for r in rows)


def read_live_gate(session: Session, *, static: Mapping[str, str] | None = None) -> LiveGate:
    """The live gate as of now. `static`: this process's static digests (default: its cached config)."""
    g4, pins = _newest_final(session)
    digests = current_static_digests() if static is None else static
    changes: dict[str, None] = {}
    for p in pins:
        changes.update(dict.fromkeys(pin_changes(session, p, static=digests)))
    live_key = next((r for r in secret_metadata(session, *LIVE_KEY) if r["status"] == "active"), None)
    golive = store.get_active(session, Section.GOLIVE)
    model = golive.model() if golive else None
    if isinstance(model, GoLiveSection):
        done, total, missing = sum(i.checked for i in model.items), len(model.items), model.missing()
    else:
        done, total, missing = 0, 0, []
    key = (
        {k: live_key[k] for k in ("version", "last4", "fingerprint", "checked_at", "details")}
        if live_key
        else None
    )
    active = {s.value: _active_id(session, s) for s in PINNED_SECTIONS}
    return LiveGate(g4, key, done, total, missing, pins, active, tuple(changes))


def _active_id(session: Session, section: Section) -> int | None:
    version = store.get_active(session, section)
    return version.id if version is not None else None


def gate_body(gate: LiveGate, *, step_up: bool) -> dict[str, Any]:
    return {
        "g4_passed": gate.g4_passed,
        "g4_config_current": gate.g4_config_current,
        "live_key_valid": gate.live_key_valid,
        "checklist_complete": gate.checklist_complete,
        "step_up_valid": step_up,
        "missing": gate.missing(step_up=step_up),
        "g4": gate.g4,
        "g4_config": {
            "pinned": list(gate.g4_pins),
            "active": gate.active_pins,
            "changes": list(gate.g4_changes),
        },
        "live_key": gate.live_key,
        "checklist": {
            "done": gate.checklist_done,
            "total": gate.checklist_total,
            "missing": gate.missing_checklist,
        },
    }


def exposed_accounts(session: Session, accounts: Sequence[Account]) -> list[Account]:
    """The namespaces among `accounts` whose ledger holds an open position or a working order."""
    return [a for a in accounts if has_exposure(session, a)]


def _active_model(active: ConfigVersion | None) -> ModeSection | None:
    model = active.model() if active is not None else None
    return model if isinstance(model, ModeSection) else None


def _active_mode(active: ConfigVersion | None) -> Account:
    model = _active_model(active)
    return model.mode if model is not None else Account.PAPER


def g4_required(active: ConfigVersion | None, model: ModeSection) -> bool:
    """False only for a `live` save that keeps `live` at the same or a lower size (exposure only shrinks)."""
    current = _active_model(active)
    return not (
        model.mode is Account.LIVE
        and current is not None
        and current.mode is Account.LIVE
        and model.size_multiplier <= current.size_multiplier
    )


def lock_pinned_config(session: Session) -> None:
    """Serialize mode saves with `models` / `council` saves (transaction-scoped advisory lock). Taken after
    the save's own write, so the check that follows sees every save committed before it."""
    session.execute(_PIN_LOCK)


def live_mode_active(session: Session) -> bool:
    return _active_mode(store.get_active(session, Section.MODE)) is Account.LIVE


def _live_gate_closed(gate: LiveGate, missing: list[str], *, step_up: bool) -> ApiError:
    return ApiError(
        409,
        "live_gate_closed",
        "live mode needs the G4 gate, a valid live key, a complete checklist and a fresh TOTP",
        missing=missing,
        live_gate=gate_body(gate, step_up=step_up),
    )


class ModePolicy(SectionPolicy):
    async def check(self, ctx: SaveContext, model: SectionModel) -> None:
        if not isinstance(model, ModeSection):
            raise TypeError("mode policy received another section")
        if model.mode is not Account.LIVE:
            require_step_up(ctx.auth)
        else:
            step_up = ctx.auth.has_step_up()
            gate = await ctx.state.db(read_live_gate)
            missing = gate.missing(step_up=step_up, g4_required=g4_required(ctx.active, model))
            if missing:
                raise _live_gate_closed(gate, missing, step_up=step_up)
        kept = enabled_accounts(model.mode)
        stopped = [a for a in enabled_accounts(_active_mode(ctx.active)) if a not in kept]
        if not stopped:
            return
        exposed = await ctx.state.db(lambda s: exposed_accounts(s, stopped))
        if exposed:
            names = ", ".join(a.value for a in exposed)
            raise ApiError(
                409,
                "namespace_has_exposure",
                f"switching to {model.mode.value} would stop namespace {names} while it still holds open "
                "positions or working orders; close them (or kill with flatten) and retry",
                accounts=[a.value for a in exposed],
            )

    def after_create(
        self, session: Session, ctx: SaveContext, version: ConfigVersion, model: SectionModel
    ) -> dict[str, Any]:
        """Re-check the G4 conditions of a `live` save inside its transaction, under the pinned-config lock:
        a `models` / `council` save committed after `check` closes the gate here."""
        if not isinstance(model, ModeSection):
            raise TypeError("mode policy received another section")
        if model.mode is Account.LIVE and g4_required(ctx.active, model):
            lock_pinned_config(session)
            gate = read_live_gate(session)
            missing = [m for m in gate.missing(step_up=True) if m in G4_REASONS]
            if missing:
                raise _live_gate_closed(gate, missing, step_up=ctx.auth.has_step_up())
        return {}


class LivePinnedPolicy(SectionPolicy):
    """The policy of a section G4 is bound to (`PINNED_SECTIONS`), wrapping its own rules (`inner`): a save
    or rollback is refused while the active run mode is `live`, checked before the write and again inside
    the save transaction under the pinned-config lock."""

    def __init__(self, inner: SectionPolicy | None = None) -> None:
        self._inner = inner if inner is not None else SectionPolicy()

    def normalize(self, ctx: SaveContext, raw: dict[str, Any]) -> dict[str, Any]:
        return self._inner.normalize(ctx, raw)

    async def check(self, ctx: SaveContext, model: SectionModel) -> None:
        if await ctx.state.db(live_mode_active):
            raise _live_config_locked(ctx.section)
        await self._inner.check(ctx, model)

    def after_create(
        self, session: Session, ctx: SaveContext, version: ConfigVersion, model: SectionModel
    ) -> dict[str, Any]:
        lock_pinned_config(session)
        if live_mode_active(session):
            raise _live_config_locked(ctx.section)
        return self._inner.after_create(session, ctx, version, model)


def _live_config_locked(section: Section) -> ApiError:
    return ApiError(
        409,
        "live_config_locked",
        f"'{section.value}' cannot change while the run mode is live: G4 evaluated the active config. "
        "Switch to paper first (close or flatten live positions), then change it; live needs a new split "
        "and a new final G4 evaluation afterwards",
        section=section.value,
    )


class ModeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Account
    size_multiplier: float | None = None
    reason: str = Field(min_length=1, max_length=500)
    parent_id: PositiveInt | None = None


def mode_body(active: ConfigVersion | None, gate: LiveGate, *, step_up: bool) -> dict[str, Any]:
    model = active.model() if active else None
    return {
        "mode": model.mode.value if isinstance(model, ModeSection) else None,
        "size_multiplier": model.size_multiplier if isinstance(model, ModeSection) else None,
        "version_id": active.id if active else None,
        "live_gate": gate_body(gate, step_up=step_up),
    }


def _read_mode(session: Session) -> tuple[ConfigVersion | None, LiveGate]:
    return store.get_active(session, Section.MODE), read_live_gate(session)


@router.get("")
async def get_mode(request: Request, ctx: AuthContext = Depends(require_session)) -> dict[str, Any]:
    active, gate = await get_state(request).db(_read_mode)
    return mode_body(active, gate, step_up=ctx.has_step_up())


@router.post("", status_code=201)
async def set_mode(
    body: ModeRequest, request: Request, ctx: AuthContext = Depends(require_session)
) -> dict[str, Any]:
    """Save a new `mode` version (same rules as `POST /config/mode`); returns the new mode state."""
    state = get_state(request)
    active = await state.db(lambda s: store.get_active(s, Section.MODE))
    parent_id = body.parent_id if body.parent_id is not None else (active.id if active else None)
    multiplier = (
        body.size_multiplier
        if body.size_multiplier is not None
        else default_size_multiplier(body.mode, state.static)
    )
    result = await save_section(
        state,
        ctx,
        Section.MODE,
        {"mode": body.mode.value, "size_multiplier": multiplier},
        reason=body.reason,
        parent_id=parent_id,
    )
    _, gate = await state.db(_read_mode)
    return {
        **mode_body(result.version, gate, step_up=ctx.has_step_up()),
        "version": version_dict(result.version),
    }
