"""Portfolio ceilings (phase 09 section 2): positions, per coin, same-direction risk incl. the hedge book,
narrative, daily loss; configuration may only tighten the constants in `hdt.settings.ceilings`."""

from __future__ import annotations

from decimal import Decimal

import pytest

from hdt.contracts.common import Side
from hdt.risk.limits import (
    Exposure,
    PortfolioLimits,
    check_portfolio,
    daily_loss_breached,
    first_failure,
    parse_categories,
    side_risk,
)

EQUITY = Decimal("10000")
DEFAULT = PortfolioLimits(
    max_positions=4,
    max_positions_per_coin=1,
    same_direction_risk_max=0.015,
    max_positions_per_narrative=2,
    daily_loss_kill=-0.02,
)


def _exp(
    symbol: str, side: Side = Side.LONG, risk: str = "10", cats: str = "", hedge: bool = False
) -> Exposure:
    return Exposure(symbol, side, Decimal(risk), parse_categories(cats), is_hedge_book=hedge)


def _reason(
    new: Exposure, book: list[Exposure], *, limits: PortfolioLimits = DEFAULT, day: str = "10000"
) -> str | None:
    checks = check_portfolio(new, book, equity=EQUITY, day_start_equity=Decimal(day), limits=limits)
    failed = first_failure(checks)
    return failed.reason if failed is not None else None


def test_fifth_position_is_refused_and_the_hedge_book_does_not_count() -> None:
    four = [_exp(s) for s in ("AAAUSDT", "BBBUSDT", "CCCUSDT", "DDDUSDT")]
    assert _reason(_exp("EEEUSDT"), four) == "max_positions"
    three_and_book = [*four[:3], _exp("BTCUSDT", Side.SHORT, "10", hedge=True)]
    assert _reason(_exp("EEEUSDT"), three_and_book) is None


def test_second_position_on_the_same_coin_is_refused() -> None:
    assert _reason(_exp("AAAUSDT", Side.SHORT), [_exp("AAAUSDT")]) == "coin_limit"


def test_same_direction_risk_ceiling_counts_the_hedge_book() -> None:
    # A short BTC book hedging long alts carries 100 USD (1.0%) of SHORT risk.
    book = [_exp("AAAUSDT", Side.LONG, "50"), _exp("BTCUSDT", Side.SHORT, "100", hedge=True)]
    new_short = _exp("BBBUSDT", Side.SHORT, "60")  # 1.0% + 0.6% = 1.6% > 1.5%
    assert _reason(new_short, book) == "same_direction_risk"
    without_book = [_exp("AAAUSDT", Side.LONG, "50")]
    assert _reason(new_short, without_book) is None
    assert side_risk(book, Side.SHORT) == Decimal("100")


def test_same_direction_risk_boundary_is_inclusive() -> None:
    book = [_exp("AAAUSDT", Side.LONG, "100")]
    assert _reason(_exp("BBBUSDT", Side.LONG, "50"), book) is None  # exactly 1.5%
    assert _reason(_exp("BBBUSDT", Side.LONG, "50.01"), book) == "same_direction_risk"


def test_third_position_in_the_same_narrative_is_refused() -> None:
    book = [_exp("AAAUSDT", cats="ai,layer-1"), _exp("BBBUSDT", cats="ai")]
    assert _reason(_exp("CCCUSDT", cats="ai"), book) == "narrative_limit"
    assert _reason(_exp("CCCUSDT", cats="defi"), book) is None
    assert _reason(_exp("CCCUSDT"), book) is None  # no narrative, no limit


def test_daily_loss_at_the_kill_level_blocks_every_open() -> None:
    assert _reason(_exp("AAAUSDT"), [], day="10204.09") == "daily_loss"  # -2.0%
    assert _reason(_exp("AAAUSDT"), [], day="10150") is None  # -1.48%
    assert daily_loss_breached(Decimal("9800"), Decimal("10000"), -0.02)
    assert not daily_loss_breached(Decimal("9801"), Decimal("10000"), -0.02)


def test_config_can_only_tighten_the_hard_ceilings() -> None:
    loose = PortfolioLimits(
        max_positions=10,
        max_positions_per_coin=3,
        same_direction_risk_max=0.10,
        max_positions_per_narrative=5,
        daily_loss_kill=-0.50,
    )
    four = [_exp(s) for s in ("AAAUSDT", "BBBUSDT", "CCCUSDT", "DDDUSDT")]
    assert _reason(_exp("EEEUSDT"), four, limits=loose) == "max_positions"
    assert _reason(_exp("AAAUSDT"), [_exp("AAAUSDT")], limits=loose) == "coin_limit"
    assert _reason(_exp("AAAUSDT", risk="200"), [], limits=loose) == "same_direction_risk"
    two_ai = [_exp("AAAUSDT", cats="ai"), _exp("BBBUSDT", cats="ai")]
    assert _reason(_exp("CCCUSDT", cats="ai"), two_ai, limits=loose) == "narrative_limit"
    assert _reason(_exp("AAAUSDT"), [], limits=loose, day="10300") == "daily_loss"  # -2.9% < -2%
    # a daily loss kill looser than -2% in config is ignored by the pure helper as well
    assert daily_loss_breached(Decimal("9750"), Decimal("10000"), -0.5)


def test_tighter_config_applies() -> None:
    tight = PortfolioLimits(2, 1, 0.005, 1, -0.01)
    assert _reason(_exp("CCCUSDT"), [_exp("AAAUSDT"), _exp("BBBUSDT")], limits=tight) == "max_positions"
    assert _reason(_exp("AAAUSDT", risk="60"), [], limits=tight) == "same_direction_risk"
    assert _reason(_exp("AAAUSDT"), [], limits=tight, day="10110") == "daily_loss"


def _hedge(side: Side, risk: str) -> Exposure:
    return Exposure("BTCUSDT", side, Decimal(risk), is_hedge_book=True, pending=True)


def test_projected_hedge_counts_on_the_side_it_adds_risk_to() -> None:
    def reason(new: Exposure, book: list[Exposure], projected: Exposure) -> str | None:
        checks = check_portfolio(
            new, book, equity=EQUITY, day_start_equity=EQUITY, limits=DEFAULT, projected_hedge=projected
        )
        failed = first_failure(checks)
        return failed.reason if failed is not None else None

    # A LONG alt grows the SHORT hedge book: 100 USD of SHORT alts + a 60 USD book = 1.6% > 1.5%.
    shorts = [_exp("AAAUSDT", Side.SHORT, "100")]
    assert reason(_exp("BBBUSDT", Side.LONG, "20"), shorts, _hedge(Side.SHORT, "60")) == "same_direction_risk"
    assert reason(_exp("BBBUSDT", Side.LONG, "20"), shorts, _hedge(Side.SHORT, "50")) is None  # exactly 1.5%
    # A SHORT book already over the budget that the new LONG shrinks never blocks it.
    over = [*shorts, _hedge(Side.SHORT, "80")]
    assert reason(_exp("BBBUSDT", Side.LONG, "20"), over, _hedge(Side.SHORT, "60")) is None
    # A book flipping to the new position's side counts on that side, the larger of before and after.
    longs = [_exp("AAAUSDT", Side.LONG, "100")]
    assert reason(_exp("BBBUSDT", Side.SHORT, "20"), longs, _hedge(Side.LONG, "40")) is None
    assert reason(_exp("BBBUSDT", Side.LONG, "20"), longs, _hedge(Side.LONG, "40")) == "same_direction_risk"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("ai, layer-1,,", frozenset({"ai", "layer-1"})),
        ("", frozenset()),
        (None, frozenset()),
        (3, frozenset()),
    ],
)
def test_categories_feature_parsing(value: object, expected: frozenset[str]) -> None:
    assert parse_categories(value) == expected


def test_every_check_is_reported_with_its_text() -> None:
    checks = check_portfolio(_exp("AAAUSDT"), [], equity=EQUITY, day_start_equity=EQUITY, limits=DEFAULT)
    assert [c.passed for c in checks] == [True] * 5
    assert all(c.text for c in checks)
