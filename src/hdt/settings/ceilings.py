"""Hard ceilings in code (Design Contract section 5, phase 09 sections 1-3). Configuration may only tighten.

Loosening any value here requires a code change and review. Every risk section (static YAML defaults,
console versions, direct config-api calls) is validated with `check_risk_limits`; the Risk service also
clips the pinned values at run time (`clip_risk_limits`, sizing, `PortfolioLimits.clipped`), so a config
that slipped past validation still cannot loosen a ceiling.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Final

LEVERAGE_MAX: Final[int] = 3
MARGIN_TYPE: Final[str] = "ISOLATED"
RISK_PCT_MAX: Final[float] = 0.005
MAX_POSITIONS: Final[int] = 4
MAX_POSITIONS_PER_COIN: Final[int] = 1
SAME_DIRECTION_RISK_MAX: Final[float] = 0.015
DAILY_LOSS_KILL_FLOOR: Final[float] = -0.02
SIZE_MULTIPLIER_MAX: Final[float] = 1.0
PAPER_SIZE_MULTIPLIER: Final[float] = 1.0  # paper measures the strategy at full size for gate G4
LIVE_SIZE_MULTIPLIER_DEFAULT: Final[float] = 0.25
MIN_RR: Final[float] = 1.5  # every price-level candidate needs reward:risk >= 1.5
MAX_MARGIN_PCT: Final[float] = 0.03  # margin per position <= 3 % of equity (bounds gap losses)
MAX_OI_FRAC: Final[float] = 0.005  # notional <= 0.5 % of the coin's open interest
MAX_VOLUME_1H_FRAC: Final[float] = 0.02  # notional <= 2 % of the coin's 1 h quote volume
LIQ_DISTANCE_MULT_MIN: Final[float] = 1.5  # liquidation farther than the stop by >= 1.5x the stop distance
MAX_POSITIONS_PER_NARRATIVE: Final[int] = 2
MAX_ENTRY_DISTANCE_ATR: Final[float] = 1.5  # candidate entry within 1.5 ATR of the Binance mark
ACCOUNT_STATE_MAX_AGE_S: Final[int] = 30  # an older AccountState vetoes every OPEN


class CeilingViolationError(ValueError):
    """A configuration value is looser than a hard ceiling."""


@dataclass(frozen=True)
class RiskLimits:
    """Every risk parameter bounded by a hard ceiling (field names are the `RiskFile` names)."""

    leverage_max: int
    risk_pct: float
    max_positions: int
    max_positions_per_coin: int
    same_direction_risk_max: float
    daily_loss_kill: float
    max_margin_pct: float
    max_oi_frac: float
    max_volume_1h_frac: float
    liq_distance_mult: float
    max_positions_per_narrative: int
    min_rr: float
    max_entry_distance_atr: float
    account_state_max_age_s: int

    @classmethod
    def of(cls, source: object) -> RiskLimits:
        """The limits read from any object carrying the `RiskFile` field names."""
        return cls(**{f.name: getattr(source, f.name) for f in fields(cls)})


def check_risk_limits(limits: RiskLimits) -> None:
    """Raise `CeilingViolationError` listing every field that exceeds its ceiling."""
    problems: list[str] = []
    if not 1 <= limits.leverage_max <= LEVERAGE_MAX:
        problems.append(f"leverage_max must be in [1, {LEVERAGE_MAX}]")
    if not 0 < limits.risk_pct <= RISK_PCT_MAX:
        problems.append(f"risk_pct must be in (0, {RISK_PCT_MAX}]")
    if not 1 <= limits.max_positions <= MAX_POSITIONS:
        problems.append(f"max_positions must be in [1, {MAX_POSITIONS}]")
    if not 1 <= limits.max_positions_per_coin <= MAX_POSITIONS_PER_COIN:
        problems.append(f"max_positions_per_coin must be in [1, {MAX_POSITIONS_PER_COIN}]")
    if not 0 < limits.same_direction_risk_max <= SAME_DIRECTION_RISK_MAX:
        problems.append(f"same_direction_risk_max must be in (0, {SAME_DIRECTION_RISK_MAX}]")
    if not DAILY_LOSS_KILL_FLOOR <= limits.daily_loss_kill < 0:
        problems.append(f"daily_loss_kill must be in [{DAILY_LOSS_KILL_FLOOR}, 0)")
    if not 0 < limits.max_margin_pct <= MAX_MARGIN_PCT:
        problems.append(f"max_margin_pct must be in (0, {MAX_MARGIN_PCT}]")
    if not 0 < limits.max_oi_frac <= MAX_OI_FRAC:
        problems.append(f"max_oi_frac must be in (0, {MAX_OI_FRAC}]")
    if not 0 < limits.max_volume_1h_frac <= MAX_VOLUME_1H_FRAC:
        problems.append(f"max_volume_1h_frac must be in (0, {MAX_VOLUME_1H_FRAC}]")
    if not limits.liq_distance_mult >= LIQ_DISTANCE_MULT_MIN:
        problems.append(f"liq_distance_mult must be >= {LIQ_DISTANCE_MULT_MIN}")
    if not 1 <= limits.max_positions_per_narrative <= MAX_POSITIONS_PER_NARRATIVE:
        problems.append(f"max_positions_per_narrative must be in [1, {MAX_POSITIONS_PER_NARRATIVE}]")
    if not limits.min_rr >= MIN_RR:
        problems.append(f"min_rr must be >= {MIN_RR}")
    if not 0 < limits.max_entry_distance_atr <= MAX_ENTRY_DISTANCE_ATR:
        problems.append(f"max_entry_distance_atr must be in (0, {MAX_ENTRY_DISTANCE_ATR}]")
    if not 1 <= limits.account_state_max_age_s <= ACCOUNT_STATE_MAX_AGE_S:
        problems.append(f"account_state_max_age_s must be in [1, {ACCOUNT_STATE_MAX_AGE_S}]")
    if problems:
        raise CeilingViolationError("; ".join(problems))


def clip_risk_limits(limits: RiskLimits) -> RiskLimits:
    """The same limits with every value moved back inside its hard ceiling (tighter values are kept)."""
    return RiskLimits(
        leverage_max=min(limits.leverage_max, LEVERAGE_MAX),
        risk_pct=min(limits.risk_pct, RISK_PCT_MAX),
        max_positions=min(limits.max_positions, MAX_POSITIONS),
        max_positions_per_coin=min(limits.max_positions_per_coin, MAX_POSITIONS_PER_COIN),
        same_direction_risk_max=min(limits.same_direction_risk_max, SAME_DIRECTION_RISK_MAX),
        daily_loss_kill=max(limits.daily_loss_kill, DAILY_LOSS_KILL_FLOOR),
        max_margin_pct=min(limits.max_margin_pct, MAX_MARGIN_PCT),
        max_oi_frac=min(limits.max_oi_frac, MAX_OI_FRAC),
        max_volume_1h_frac=min(limits.max_volume_1h_frac, MAX_VOLUME_1H_FRAC),
        liq_distance_mult=max(limits.liq_distance_mult, LIQ_DISTANCE_MULT_MIN),
        max_positions_per_narrative=min(limits.max_positions_per_narrative, MAX_POSITIONS_PER_NARRATIVE),
        min_rr=max(limits.min_rr, MIN_RR),
        max_entry_distance_atr=min(limits.max_entry_distance_atr, MAX_ENTRY_DISTANCE_ATR),
        account_state_max_age_s=min(limits.account_state_max_age_s, ACCOUNT_STATE_MAX_AGE_S),
    )


def check_size_multiplier(account: str, value: float) -> None:
    """`mode.size_multiplier` of namespace `account`: at most 1, and exactly 1 for `paper`."""
    if not 0 < value <= SIZE_MULTIPLIER_MAX:
        raise CeilingViolationError(f"size_multiplier must be in (0, {SIZE_MULTIPLIER_MAX}]")
    if account == "paper" and value != PAPER_SIZE_MULTIPLIER:
        raise CeilingViolationError(
            f"size_multiplier must be {PAPER_SIZE_MULTIPLIER} for paper (gate G4 measures the strategy at "
            "full size)"
        )
