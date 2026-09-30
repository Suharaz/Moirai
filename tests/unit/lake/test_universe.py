"""Universe: top-N CMC coins with a Binance perp, ranked by OI, read back point-in-time."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from hdt.ingest.symbol_map import CmcCoin, Perp, SymbolMap
from hdt.lake.pit_query import PitQuery
from hdt.lake.raw_store import RawStore
from hdt.lake.universe import UniverseInputs, build_universe, load_universe, universe_capture

T0 = datetime(2026, 9, 27, 0, 5, tzinfo=UTC)
SYMBOLS = SymbolMap.build(
    [CmcCoin(1, "BTC", 1), CmcCoin(1027, "ETH", 2), CmcCoin(5, "DOGE", 9), CmcCoin(7, "XRP", 300)],
    [Perp(s + "USDT", s, "USDT") for s in ("BTC", "ETH", "DOGE", "XRP")],
)
LISTINGS = [
    {"id": 1, "cmc_rank": 1},
    {"id": 1027, "cmc_rank": 2},
    {"id": 5, "cmc_rank": 9},
    {"id": 7, "cmc_rank": 300},  # outside the CMC top N
    {"id": 42, "cmc_rank": 3},  # no Binance perp
]


def _build(day: date, built_at: datetime, oi: dict[str, float]) -> object:
    return build_universe(
        UniverseInputs(LISTINGS, oi),
        SYMBOLS,
        day=day,
        built_at=built_at,
        cmc_top=250,
        ltx_size=2,
        watchlist_size=1,
        banned=("DOGEUSDT",),
        sources={"listings_latest": "x"},
    )


def test_ranked_by_open_interest_with_banned_and_out_of_top_removed() -> None:
    u = _build(T0.date(), T0, {"BTCUSDT": 5e9, "ETHUSDT": 7e9, "DOGEUSDT": 9e9, "XRPUSDT": 1e9})
    assert [m.binance_symbol for m in u.members] == ["ETHUSDT", "BTCUSDT"]  # type: ignore[attr-defined]
    assert [m.cmc_id for m in u.watchlist] == [1027]  # type: ignore[attr-defined]


def test_members_keep_every_eligible_coin_and_ltx_is_the_top_by_open_interest() -> None:
    bases = [f"C{n:02d}" for n in range(70)]
    symbols = SymbolMap.build(
        [CmcCoin(100 + n, base, n + 1) for n, base in enumerate(bases)],
        [Perp(base + "USDT", base, "USDT") for base in bases],
    )
    listings = [{"id": 100 + n, "cmc_rank": n + 1} for n in range(70)]
    oi = {base + "USDT": 1e6 * ((n * 37) % 70 + 1) for n, base in enumerate(bases)}  # OI order != rank order
    u = build_universe(
        UniverseInputs(listings, oi),
        symbols,
        day=T0.date(),
        built_at=T0,
        cmc_top=250,
        ltx_size=60,
        watchlist_size=20,
        banned=(),
        sources={},
    )
    assert len(u.members) == 70
    assert len(u.ltx) == 60
    by_oi = sorted(oi, key=lambda s: -oi[s])
    assert [m.binance_symbol for m in u.members] == by_oi
    assert [m.binance_symbol for m in u.ltx] == by_oi[:60]


def test_load_universe_is_point_in_time(tmp_path: Path) -> None:
    store = RawStore(tmp_path / "staging", tmp_path / "lake")
    pit = PitQuery(tmp_path / "staging", tmp_path / "lake")
    day1 = _build(T0.date(), T0, {"BTCUSDT": 5e9, "ETHUSDT": 1e9})
    t1 = T0 + timedelta(days=1)
    day2 = _build(t1.date(), t1, {"BTCUSDT": 1e9, "ETHUSDT": 5e9})
    store.append(universe_capture(day1))  # type: ignore[arg-type]
    store.append(universe_capture(day2))  # type: ignore[arg-type]
    before = load_universe(pit, t1 - timedelta(seconds=1))
    after = load_universe(pit, t1)
    assert before is not None
    assert before.members[0].binance_symbol == "BTCUSDT"
    assert after is not None
    assert after.members[0].binance_symbol == "ETHUSDT"
    assert load_universe(pit, T0 - timedelta(seconds=1)) is None
