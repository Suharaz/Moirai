"""Phase 03 performance criterion through the real `QuantCore` and its Postgres store.

The criterion is "60 coins x 5 agents < 2 s on a dev machine (excluding Postgres writes)".
`tests/unit/quant/test_quant_core_perf.py` times the compute path alone; this benchmark runs the production
path: `QuantCore.quant_core` for every (coin, agent), which reads the pinned config versions from Postgres and
inserts every candidate set and packet as `hdt_council` (one transaction per packet) on a migrated database.
The time spent inside those write transactions is measured separately and is the only part left out of the
budget; everything else (pin read, snapshots, candidate sets, p_model, packet hashing, cache) counts.

Warm as in the unit benchmark: the lake hours are parsed once by a shared `LakeView`; each timed pass uses a
new `QuantCore` (new feature engine, packet cache and pin memo) on emptied tables, so every row is a real
insert. The best of three passes is compared. Deselected by default (`perf`): `uv run pytest -m perf`.
"""

from __future__ import annotations

import contextlib
import time
from collections.abc import Iterator

import pytest
import sqlalchemy as sa

from fixtures.quant_db import QuantDb
from fixtures.synthetic_lake import FAR_FUTURE, MAIN_DAY, A, build_perf
from hdt.contracts.common import TargetType
from hdt.contracts.packet import QuantPacket
from hdt.core.config import scanner_config, static_config
from hdt.features.lake_io import LakeView
from hdt.lake.pit_query import PitQuery
from hdt.lake.universe import Universe, load_universe
from hdt.quant.p_model import MODEL_AGENTS
from hdt.quant.quant_core import QuantCore, QuantPins

pytestmark = [pytest.mark.pg, pytest.mark.integration, pytest.mark.perf]

TRADABLE_COINS = 60
BUDGET_S = 2.0


class _WriteClock:
    """Seconds spent inside `engine.begin()` blocks: the packet and candidate-set write transactions."""

    def __init__(self, engine: sa.Engine) -> None:
        self._begin = engine.begin
        self.seconds = 0.0

    @contextlib.contextmanager
    def begin(self) -> Iterator[sa.Connection]:
        start = time.perf_counter()
        try:
            with self._begin() as conn:
                yield conn
        finally:
            self.seconds += time.perf_counter() - start


def _stored(db: QuantDb) -> tuple[int, int]:
    with db.admin.connect() as conn:
        packets = conn.execute(sa.text("SELECT count(*) FROM quant_packets")).scalar_one()
        sets = conn.execute(sa.text("SELECT count(*) FROM candidate_sets")).scalar_one()
    return int(packets), int(sets)


def _pass(
    pit: PitQuery, view: LakeView, db: QuantDb, universe: Universe, pins: QuantPins
) -> list[QuantPacket]:
    core = QuantCore(pit, db.council, view=view, clock=lambda: FAR_FUTURE)
    btc = static_config().indicators.reserves.btc_symbol
    return [
        core(agent, member.cmc_id, A, pins)
        for member in universe.members
        if member.cmc_symbol != btc
        for agent in MODEL_AGENTS
    ]


def test_60_coins_x_5_agents_through_quant_core_and_postgres(
    tmp_path_factory: pytest.TempPathFactory, quant_db: QuantDb, monkeypatch: pytest.MonkeyPatch
) -> None:
    lake = build_perf(tmp_path_factory.mktemp("perf_lake_pg"), TRADABLE_COINS + 1)
    view = LakeView(lake.pit, clock=lambda: FAR_FUTURE)
    universe = load_universe(lake.pit, A)
    assert universe is not None
    pins = QuantPins(
        config_version_ids=quant_db.version_ids,
        universe_date=MAIN_DAY,
        target_type=TargetType.RESID_12H,
        label_spec_version=scanner_config().labels.label_spec_version,
    )
    cold = _pass(lake.pit, view, quant_db, universe, pins)
    assert len(cold) == TRADABLE_COINS * len(MODEL_AGENTS)
    assert _stored(quant_db) == (len(cold), TRADABLE_COINS)
    writes = _WriteClock(quant_db.council)
    monkeypatch.setattr(quant_db.council, "begin", writes.begin)
    passes: list[tuple[float, float]] = []
    for _ in range(3):
        with quant_db.admin.begin() as conn:
            conn.execute(sa.text("TRUNCATE quant_packets, candidate_sets"))
        writes.seconds = 0.0
        start = time.perf_counter()
        warm = _pass(lake.pit, view, quant_db, universe, pins)
        passes.append((time.perf_counter() - start, writes.seconds))
        assert [p.packet_sha256 for p in warm] == [p.packet_sha256 for p in cold]
        assert _stored(quant_db) == (len(cold), TRADABLE_COINS)  # every row really written
    without_writes = [total - written for total, written in passes]
    assert min(without_writes) < BUDGET_S, (
        "warm passes (total s, of which Postgres writes s): "
        f"{[(round(total, 3), round(written, 3)) for total, written in passes]}"
    )
