"""Risk section rules: hard ceilings are enforced by the schema (`RiskSection`); while the run mode is
`live`, any risk change also needs a fresh step-up TOTP."""

from __future__ import annotations

from hdt.configapi.auth import require_step_up
from hdt.configapi.sections import SaveContext, SectionPolicy
from hdt.contracts.common import Account
from hdt.settings import store
from hdt.settings.schemas import ModeSection, Section, SectionModel


async def live_mode_active(ctx: SaveContext) -> bool:
    mode = await ctx.state.db(lambda s: store.get_active(s, Section.MODE))
    if mode is None:
        return False
    model = mode.model()
    return isinstance(model, ModeSection) and model.mode is Account.LIVE


class RiskPolicy(SectionPolicy):
    async def check(self, ctx: SaveContext, model: SectionModel) -> None:
        if await live_mode_active(ctx):
            require_step_up(ctx.auth)
