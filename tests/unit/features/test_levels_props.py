"""`levels` property test (phase 03 success criterion): side, stop on the correct side, R:R >= 1.5,
tick-aligned, unique `candidate_id`; the same inputs always give the same `candidate_set_sha256`.

Random inputs (price scale from 1e-6 to 1e5, ticks of 1 / 2 / 5 x 10^k, random 1h walks, books with walls,
pinned `min_rr` / `max_entry_distance_atr`) plus every candidate set of the synthetic lake snapshot. That
every agent of one (coin, as_of) points to the same set is proven through `quant_core` in
`tests/integration/test_quant_core_lake.py`.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import numpy as np
from hypothesis import given, settings
from hypothesis import strategies as st

from fixtures.synthetic_lake import MAIN_COINS, A, SyntheticLake, members_by_id
from hdt.contracts.candidate import CANDIDATE_ID_PATTERN, CandidateSet
from hdt.contracts.common import Side
from hdt.core.config import static_config
from hdt.features.bars import INTERVAL_MS, Bars, resample
from hdt.features.engine import FeatureEngine
from hdt.features.levels import LevelInputs, LevelRules, candidate_set
from hdt.features.micro import Book
from hdt.lake.universe import load_universe
from hdt.settings.ceilings import MIN_RR

PARAMS = static_config().indicators.levels
AS_OF = datetime(2026, 3, 10, 12, 0, 30, tzinfo=UTC)
HOUR_MS = INTERVAL_MS["1h"]


def assert_valid_set(cs: CandidateSet, inputs: LevelInputs, rules: LevelRules) -> None:
    assert inputs.mark is not None
    assert inputs.atr is not None
    assert inputs.tick is not None
    ids = [c.candidate_id for c in cs.candidates]
    assert len(ids) == len(set(ids))
    assert ids == sorted(ids)
    assert cs.levels_ver == PARAMS.levels_ver
    per_side = {Side.LONG: 0, Side.SHORT: 0}
    bound = Decimal(repr(rules.max_entry_atr * inputs.atr)) + inputs.tick
    for c in cs.candidates:
        per_side[c.side] += 1
        if c.side is Side.LONG:
            assert c.invalidation < c.entry < c.tp1
        else:
            assert c.tp1 < c.entry < c.invalidation
        assert c.tick == inputs.tick
        for price in (c.entry, c.invalidation, c.tp1):
            assert price > 0
            assert price % inputs.tick == 0
        rr = float(abs(c.tp1 - c.entry) / abs(c.entry - c.invalidation))
        assert rr >= rules.min_rr >= MIN_RR
        assert abs(c.rr - rr) <= 1e-9 * rr
        # Entries sit within the pinned ATR bound of the mark (one tick of rounding away from it).
        assert abs(c.entry - Decimal(repr(inputs.mark))) <= bound
        assert re.fullmatch(CANDIDATE_ID_PATTERN, c.candidate_id)
    assert max(per_side.values(), default=0) <= PARAMS.max_per_side


@st.composite
def level_cases(draw: st.DrawFn) -> tuple[LevelInputs, LevelRules]:
    rng = np.random.default_rng(draw(st.integers(min_value=0, max_value=2**32 - 1)))
    exponent = draw(st.integers(min_value=-6, max_value=5))
    mark = draw(st.floats(min_value=1.0, max_value=9.99)) * 10.0**exponent
    atr = mark * draw(st.floats(min_value=0.0005, max_value=0.08))
    tick = Decimal(draw(st.sampled_from([1, 2, 5]))).scaleb(
        exponent - draw(st.integers(min_value=1, max_value=6))
    )
    n = draw(st.integers(min_value=0, max_value=200))
    vol = atr / mark / 2
    walk = np.cumsum(rng.normal(0.0, vol, n))
    closes = mark * np.exp(walk - walk[-1]) if n else walk  # the newest close is the mark
    rows = []
    start = (int(AS_OF.timestamp() * 1000) // HOUR_MS - n) * HOUR_MS
    for i, close in enumerate(closes):
        open_ = closes[i - 1] if i else close
        high = max(open_, close) + abs(rng.normal(0.0, 0.3)) * atr
        low = max(min(open_, close) - abs(rng.normal(0.0, 0.3)) * atr, mark * 1e-3)
        volume = float(rng.lognormal(10.0, 1.0))
        rows.append(
            (start + i * HOUR_MS, open_, high, low, close, volume, volume * close, volume * close / 2)
        )
    bars_1h = Bars.from_rows(rows, HOUR_MS)
    book: Book | None = None
    if draw(st.booleans()):
        step = draw(st.floats(min_value=0.0001, max_value=0.01))
        qty = rng.lognormal(0.0, 1.0, 40) * (1.0 + 9.0 * (rng.random(40) < 0.1))
        bids = tuple((mark * (1 - (i + 1) * step), float(qty[i])) for i in range(20))
        asks = tuple((mark * (1 + (i + 1) * step), float(qty[20 + i])) for i in range(20))
        book = Book(bids, asks, AS_OF - timedelta(seconds=1))
    rules = LevelRules(
        min_rr=draw(st.floats(min_value=MIN_RR, max_value=3.0)),
        max_entry_atr=draw(st.floats(min_value=0.25, max_value=3.0)),
    )
    return LevelInputs(mark, atr, tick, bars_1h, resample(bars_1h, "4h"), book), rules


@settings(max_examples=300, deadline=None, derandomize=True)
@given(case=level_cases())
def test_every_candidate_is_well_formed(case: tuple[LevelInputs, LevelRules]) -> None:
    inputs, rules = case
    cs = candidate_set(7, AS_OF, inputs, PARAMS, rules)
    assert_valid_set(cs, inputs, rules)
    again = candidate_set(7, AS_OF, inputs, PARAMS, rules)
    assert again.candidate_set_sha256 == cs.candidate_set_sha256
    if cs.candidates:
        # The id binds the coin and the moment: another coin or as_of never reuses one.
        other = candidate_set(8, AS_OF + timedelta(minutes=5), inputs, PARAMS, rules)
        assert not {c.candidate_id for c in cs.candidates} & {c.candidate_id for c in other.candidates}


def test_missing_mark_atr_or_tick_gives_an_empty_set() -> None:
    bars = Bars.empty(HOUR_MS)
    rules = LevelRules(MIN_RR, 1.5)
    for mark, atr, tick in ((None, 1.0, Decimal("0.01")), (100.0, None, Decimal("0.01")), (100.0, 1.0, None)):
        inputs = LevelInputs(mark, atr, tick, bars, bars, None)
        assert candidate_set(7, AS_OF, inputs, PARAMS, rules).candidates == ()


def test_lake_snapshot_sets_are_well_formed_on_both_sides(
    main_lake: SyntheticLake, main_features: FeatureEngine
) -> None:
    static = static_config()
    rules = LevelRules(static.risk.min_rr, static.risk.max_entry_distance_atr)
    universe = load_universe(main_lake.pit, A)
    snap = main_features.snapshot(A, universe, static.cmc_routes)
    coins = members_by_id(MAIN_COINS)
    sides: set[Side] = set()
    for member in universe.members:
        if member.cmc_symbol == "BTC":
            continue
        inputs = snap.level_inputs(member)
        cs = snap.candidate_set(member, rules)
        assert cs.coin_id == member.cmc_id
        assert cs.as_of == A
        assert cs.candidates, member.cmc_symbol
        assert_valid_set(cs, inputs, rules)
        assert inputs.tick == Decimal(coins[member.cmc_id].tick)
        sides |= {c.side for c in cs.candidates}
    assert sides == {Side.LONG, Side.SHORT}
