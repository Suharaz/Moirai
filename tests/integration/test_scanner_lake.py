"""Scanner on the synthetic lake with Postgres and Redis (phase 03 `test_scanner`).

At A one coin (XTRG) passes LTX strict and is emitted; at A_BLOCK five of the eleven non-BTC LTX coins flush
LONG while BTC has not, so breadth B >= 0.40 blocks every LTX candidate. BTC is never evaluated nor emitted,
every evaluation is logged with its strict / loose outcome, and a rerun of an as_of emits nothing new.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest
import sqlalchemy as sa
from redis.asyncio import Redis

import hdt.quant.scanner as scanner_module
from fixtures.quant_db import QuantDb
from fixtures.synthetic_lake import A_BLOCK, BRST, BTC, XTRG, A, SyntheticLake
from hdt.contracts.candidate import Candidate
from hdt.contracts.common import CandidateSource, TargetType
from hdt.contracts.streams import Stream
from hdt.core.config import scanner_config
from hdt.db.models.quant import ScannerLogRow, ScannerPublishedRow
from hdt.features.engine import FeatureEngine
from hdt.quant.scanner import Scanner

pytestmark = [pytest.mark.pg, pytest.mark.redis, pytest.mark.integration]

BREADTH_BLOCK = 0.40


def _log(db: QuantDb, as_of: Any) -> dict[tuple[int, str], ScannerLogRow]:
    with db.risk.connect() as conn:
        rows = conn.execute(sa.select(ScannerLogRow).where(ScannerLogRow.as_of == as_of)).all()
    return {(row.coin_id, row.rule): row for row in rows}


def _log_count(db: QuantDb) -> int:
    with db.risk.connect() as conn:
        return int(conn.execute(sa.select(sa.func.count()).select_from(ScannerLogRow)).scalar_one())


def _clear_log(db: QuantDb) -> None:
    """The module database may already hold these as_of slots."""
    with db.admin.begin() as conn:
        conn.execute(sa.delete(ScannerPublishedRow))
        conn.execute(sa.delete(ScannerLogRow))


def _published(db: QuantDb) -> set[tuple[int, str]]:
    with db.risk.connect() as conn:
        rows = conn.execute(sa.select(ScannerPublishedRow.coin_id, ScannerPublishedRow.rule)).all()
    return {(r.coin_id, r.rule) for r in rows}


async def _stream(redis: Redis) -> list[Candidate]:
    entries = await redis.xrange(str(Stream.CANDIDATES))
    return [Candidate.model_validate_json(fields[b"data"]) for _, fields in entries]


async def test_scanner_emits_strict_blocks_contagion_skips_btc_and_is_idempotent(
    main_lake: SyntheticLake, main_features: FeatureEngine, quant_db: QuantDb, redis_client: Redis
) -> None:
    scanner = Scanner(main_lake.pit, quant_db.council, redis_client, features=main_features)
    cadence = timedelta(seconds=scanner_config().cadence_s)

    first = await scanner.run(now=A)
    assert first.as_of == A
    assert not first.skipped
    log_a = _log(quant_db, A)
    xtrg = log_a[(XTRG.cmc_id, CandidateSource.LTX.value)]
    assert (xtrg.strict_pass, xtrg.loose_pass, xtrg.contagion_blocked) == (True, True, False)
    assert xtrg.emitted
    brst = log_a[(BRST.cmc_id, CandidateSource.LTX.value)]
    assert brst.strict_pass is False  # burst below the minimum notional
    assert brst.emitted is False
    emitted_a = await _stream(redis_client)
    assert [(c.coin_id, c.source, c.as_of) for c in emitted_a if c.source is CandidateSource.LTX] == [
        (XTRG.cmc_id, CandidateSource.LTX, A)
    ]
    assert {(c.coin_id, c.source.value) for c in emitted_a} == {k for k, row in log_a.items() if row.emitted}

    blocked = await scanner.run(now=A_BLOCK + cadence - timedelta(seconds=1))  # any time in the slot
    assert blocked.as_of == A_BLOCK
    assert not blocked.skipped
    log_b = _log(quant_db, A_BLOCK)
    ltx_b = {coin: row for (coin, rule), row in log_b.items() if rule == CandidateSource.LTX.value}
    xtrg_b = ltx_b[XTRG.cmc_id]
    assert (xtrg_b.strict_pass, xtrg_b.contagion_blocked, xtrg_b.emitted) == (True, True, False)
    assert xtrg_b.side == "LONG"
    assert xtrg_b.conditions["breadth"] >= BREADTH_BLOCK
    assert xtrg_b.conditions["btc_flushed"] is False
    assert not any(row.emitted for row in ltx_b.values())
    stream_b = await _stream(redis_client)
    assert not any(c.as_of == A_BLOCK and c.source is CandidateSource.LTX for c in stream_b)
    assert {(c.coin_id, c.source.value) for c in stream_b if c.as_of == A_BLOCK} == {
        k for k, row in log_b.items() if row.emitted
    }

    # BTC is never evaluated nor emitted; every evaluation carries its strict and loose outcome.
    for log in (log_a, log_b):
        assert BTC.cmc_id not in {coin for coin, _ in log}
        assert all(isinstance(row.strict_pass, bool) for row in log.values())
        assert all(isinstance(row.loose_pass, bool) for row in log.values())
        assert all(not row.strict_pass or row.loose_pass for row in log.values())
    assert BTC.cmc_id not in {c.coin_id for c in stream_b}

    # Rerun of the same slots (a restart, a double fire): nothing emitted, nothing logged twice.
    length, rows = await redis_client.xlen(str(Stream.CANDIDATES)), _log_count(quant_db)
    for now in (A, A_BLOCK + timedelta(seconds=5)):
        rerun = await scanner.run(now=now)
        assert rerun.skipped
        assert rerun.emitted == []
    assert await redis_client.xlen(str(Stream.CANDIDATES)) == length
    assert _log_count(quant_db) == rows
    assert rows == len(log_a) + len(log_b)


async def test_a_published_candidate_already_has_its_committed_scanner_log_row(
    main_lake: SyntheticLake,
    main_features: FeatureEngine,
    quant_db: QuantDb,
    redis_client: Redis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The council reads `scanner_log.strict_pass` when a candidate arrives: the row must be visible first."""
    real_publish = scanner_module.publish
    seen: list[tuple[int, bool | None]] = []

    async def observing_publish(redis: Redis, stream: Stream, candidate: Candidate, **kw: Any) -> Any:
        # A separate connection sees only committed rows.
        row = _log(quant_db, candidate.as_of).get((candidate.coin_id, candidate.source.value))
        seen.append((candidate.coin_id, None if row is None else row.strict_pass))
        return await real_publish(redis, stream, candidate, **kw)

    _clear_log(quant_db)
    monkeypatch.setattr(scanner_module, "publish", observing_publish)
    result = await Scanner(main_lake.pit, quant_db.council, redis_client, features=main_features).run(now=A)
    assert result.emitted
    assert (XTRG.cmc_id, True) in seen
    assert all(strict is not None for _, strict in seen)


async def test_a_failed_xadd_after_commit_is_relayed_by_the_next_run(
    main_lake: SyntheticLake,
    main_features: FeatureEngine,
    quant_db: QuantDb,
    redis_client: Redis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Redis down after the log commits: the rerun of the logged slot (and any later run) delivers once."""
    _clear_log(quant_db)
    await redis_client.delete(str(Stream.CANDIDATES))
    real_publish = scanner_module.publish

    async def redis_down(*_a: Any, **_kw: Any) -> Any:
        raise ConnectionError("redis unavailable")

    scanner = Scanner(main_lake.pit, quant_db.council, redis_client, features=main_features)
    monkeypatch.setattr(scanner_module, "publish", redis_down)
    first = await scanner.run(now=A)  # a failing relay is counted and retried, never raised
    assert first.emitted
    assert first.republished == []
    logged = {k for k, row in _log(quant_db, A).items() if row.emitted}
    assert logged
    assert _published(quant_db) == set()
    assert await redis_client.xlen(str(Stream.CANDIDATES)) == 0

    monkeypatch.setattr(scanner_module, "publish", real_publish)
    rerun = await scanner.run(now=A)
    assert rerun.skipped
    assert rerun.emitted == []
    assert {(c.coin_id, c.source.value) for c in rerun.republished} == logged
    assert {(c.coin_id, c.source.value) for c in await _stream(redis_client)} == logged
    assert _published(quant_db) == logged
    # Delivered once: a further run relays nothing more.
    again = await scanner.run(now=A)
    assert again.republished == []
    assert await redis_client.xlen(str(Stream.CANDIDATES)) == len(logged)


def _emitted_row(db: QuantDb, coin_id: int, as_of: Any, rule: CandidateSource) -> None:
    cfg = scanner_config()
    with db.admin.begin() as conn:
        conn.execute(
            sa.insert(ScannerLogRow).values(
                coin_id=coin_id,
                as_of=as_of,
                rule=rule.value,
                rule_version=cfg.rule_version,
                side="LONG",
                conditions={},
                strict_pass=True,
                loose_pass=True,
                contagion_blocked=None,
                emitted=True,
                dropped_budget=False,
                score=1.0 if rule is not CandidateSource.HELD else None,
                target_type=TargetType.RAW_12H.value,
                label_spec_version=cfg.labels.label_spec_version,
                universe_date=A.date(),
                feature_ver="test",
                created_at=A,
            )
        )


async def test_a_failing_row_does_not_stop_the_relay_and_the_newest_slot_goes_first(
    main_lake: SyntheticLake,
    main_features: FeatureEngine,
    quant_db: QuantDb,
    redis_client: Redis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """m5: one failing XADD aborted the loop, so newer rows of the same run were never sent, and the rows of
    an older slot were sent first."""
    _clear_log(quant_db)
    await redis_client.delete(str(Stream.CANDIDATES))
    older = A - timedelta(seconds=scanner_config().cadence_s)
    _emitted_row(quant_db, BRST.cmc_id, older, CandidateSource.LTX)
    _emitted_row(quant_db, BRST.cmc_id, A, CandidateSource.LTX)
    _emitted_row(quant_db, XTRG.cmc_id, A, CandidateSource.LTX)
    real_publish = scanner_module.publish
    attempts: list[tuple[int, Any]] = []

    async def first_fails(redis: Redis, stream: Stream, candidate: Candidate, **kw: Any) -> Any:
        attempts.append((candidate.coin_id, candidate.as_of))
        if len(attempts) == 1:
            raise ConnectionError("redis blip")
        return await real_publish(redis, stream, candidate, **kw)

    monkeypatch.setattr(scanner_module, "publish", first_fails)
    scanner = Scanner(main_lake.pit, quant_db.council, redis_client, features=main_features)
    relayed = await scanner._relay_unsent(A, None)

    assert [a for _c, a in attempts[:2]] == [A, A]  # the current slot first
    assert {(c.coin_id, c.as_of) for c in relayed} == {(coin, when) for coin, when in attempts[1:]}
    assert len(relayed) == 2
    assert A in {c.as_of for c in relayed}


async def test_a_scored_rule_is_relayed_before_held_for_the_same_coin_and_slot(
    main_lake: SyntheticLake, main_features: FeatureEngine, quant_db: QuantDb, redis_client: Redis
) -> None:
    """m6: `order_by(rule)` sent HELD before LTX (alphabetical), so the council admitted the unscored HELD
    and skipped the scored LTX as `spacing`."""
    _clear_log(quant_db)
    await redis_client.delete(str(Stream.CANDIDATES))
    _emitted_row(quant_db, XTRG.cmc_id, A, CandidateSource.HELD)
    _emitted_row(quant_db, XTRG.cmc_id, A, CandidateSource.LTX)
    scanner = Scanner(main_lake.pit, quant_db.council, redis_client, features=main_features)

    relayed = await scanner._relay_unsent(A, None)

    assert [c.source for c in relayed] == [CandidateSource.LTX, CandidateSource.HELD]
    assert [c.source for c in await _stream(redis_client)] == [CandidateSource.LTX, CandidateSource.HELD]


async def test_an_undelivered_emission_does_not_use_the_daily_budget(
    main_lake: SyntheticLake, main_features: FeatureEngine, quant_db: QuantDb, redis_client: Redis
) -> None:
    """m5: emitted rows that never reached the stream counted toward `max_candidates_per_day`."""
    _clear_log(quant_db)
    earlier = A - timedelta(seconds=scanner_config().cadence_s)
    for coin in range(900_000, 900_000 + scanner_config().max_candidates_per_day):
        _emitted_row(quant_db, coin, earlier, CandidateSource.LTX)
    result = await Scanner(main_lake.pit, quant_db.council, redis_client, features=main_features).run(now=A)
    assert result.emitted
