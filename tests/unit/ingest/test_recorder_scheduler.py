"""Recorder scheduling: late first runs still run once; the universe build waits for its CMC inputs and a
failed build is retried a bounded number of times the same UTC day; a new CMC key version brings forward the
jobs the key could not serve."""

from __future__ import annotations

import asyncio
import json
from collections import Counter
from collections.abc import AsyncIterator, Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
from apscheduler.job import Job
from nacl.public import PrivateKey
from redis.asyncio import Redis
from sqlalchemy.orm import Session, sessionmaker

from hdt.core.clock import ManualClock, use_clock, utcnow
from hdt.core.config import static_config
from hdt.ingest.cmc_collectors import JobResult, RouteJob, build_jobs
from hdt.ingest.scheduler import UNIVERSE_BUILD_JOB, Recorder
from hdt.lake.pit_query import PitQuery
from hdt.lake.raw_store import RawStore
from hdt.lake.schemas import Capture
from hdt.vault.loader import SecretLoader

RETRY = static_config().binance.universe_retry
T0 = datetime(2026, 9, 28, 0, 5, tzinfo=UTC)
LISTINGS = [{"id": 1, "cmc_rank": 1}, {"id": 1027, "cmc_rank": 2}]
CMC_MAP = [
    {"id": 1, "symbol": "BTC", "rank": 1, "is_active": 1},
    {"id": 1027, "symbol": "ETH", "rank": 2, "is_active": 1},
]
PERPS = {
    "symbols": [
        {
            "symbol": f"{base}USDT",
            "baseAsset": base,
            "quoteAsset": "USDT",
            "contractType": "PERPETUAL",
            "status": "TRADING",
        }
        for base in ("BTC", "ETH")
    ]
}


@dataclass
class FakeBinance:
    """Binance public REST: `exchangeInfo` fails while `failures` remain; every request is recorded."""

    failures: int = 0
    paths: list[str] = field(default_factory=list)

    def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.paths.append(path)
        if path == "/fapi/v1/exchangeInfo":
            if self.failures > 0:
                self.failures -= 1
                return httpx.Response(503, json={"code": -1001, "msg": "Internal error"})
            return httpx.Response(200, json=PERPS)
        if path == "/fapi/v1/premiumIndex":
            return httpx.Response(
                200,
                json=[
                    {"symbol": "BTCUSDT", "markPrice": "60000"},
                    {"symbol": "ETHUSDT", "markPrice": "3000"},
                ],
            )
        if path == "/fapi/v1/openInterest":
            return httpx.Response(200, json={"symbol": request.url.params["symbol"], "openInterest": "1000"})
        return httpx.Response(200, json=[])

    def builds(self) -> int:
        return self.paths.count("/fapi/v1/exchangeInfo")


def _cmc(route: str, data: Any) -> Capture:
    body = json.dumps({"status": {"error_code": 0}, "data": data}).encode()
    return Capture(source="cmc", route=route, fetched_at=utcnow(), http_status=200, body=body, params={})


@dataclass
class FakeRunner:
    """CMC route runner: a successful run records the route's response in the lake; routes in `key_blocked`
    report a run the key could not serve (no active key / not on the plan)."""

    store: RawStore
    outcome: str = "ok"
    key_blocked: set[str] = field(default_factory=set)

    async def run(self, job: RouteJob) -> JobResult:
        if self.outcome == "ok":
            data = LISTINGS if job.route.name == "listings_latest" else CMC_MAP
            self.store.append(_cmc(job.route.name, data))
        blocked = job.route.name in self.key_blocked
        return JobResult(job.route_key, self.outcome, 1, key_blocked=blocked)  # type: ignore[arg-type]


@dataclass
class Harness:
    rec: Recorder
    binance: FakeBinance
    runner: FakeRunner
    health: list[tuple[str, bool]]

    def job(self, route: str) -> RouteJob:
        return next(j for j in build_jobs(self.rec.cmc_routes()) if j.route.name == route)

    def builds(self) -> list[bool]:
        return [ok for key, ok in self.health if key == "U"]

    def pending_build(self) -> Job | None:
        return self.rec.scheduler.get_job(UNIVERSE_BUILD_JOB)


@pytest.fixture
def clock() -> Iterator[ManualClock]:
    manual = ManualClock(T0)
    with use_clock(manual):
        yield manual


@pytest.fixture
async def harness(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[Harness]:
    staging, lake = tmp_path / "staging", tmp_path / "lake"
    staging.mkdir()
    lake.mkdir()
    store = RawStore(staging, lake)
    binance = FakeBinance()
    sessions: sessionmaker[Session] = sessionmaker()
    async with httpx.AsyncClient(transport=httpx.MockTransport(binance)) as http:
        rec = Recorder(
            static=static_config(),
            sessions=sessions,
            redis=Redis(),
            secrets=SecretLoader("data", sessions, PrivateKey.generate()),
            http=http,
            store=store,
            pit=PitQuery(staging, lake),
        )
        health: list[tuple[str, bool]] = []

        async def binance_health(key: str, _at: datetime, ok: bool, _error: str | None = None) -> None:
            health.append((key, ok))

        async def no_streams() -> None:  # WebSocket connections are covered by the integration test
            return None

        runner = FakeRunner(store)
        # the CMC catalog as `schedule_cmc` registers it, without timers: tests run the route jobs directly
        rec.jobs = {f"cmc:{job.route_key}": job for job in build_jobs(rec.cmc_routes())}
        monkeypatch.setattr(rec, "runner", runner)
        monkeypatch.setattr(rec, "_binance_health", binance_health)
        monkeypatch.setattr(rec, "refresh_streams", no_streams)
        yield Harness(rec, binance, runner, health)
        if rec.scheduler.running:
            rec.scheduler.shutdown(wait=False)


async def _until(check: Callable[[], bool], timeout_s: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_s
    while not check():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not reached in time")
        await asyncio.sleep(0.02)


async def _run_pending(h: Harness, clock: ManualClock) -> float:
    """Run the pending universe build job at its due time; returns its delay in seconds."""
    job = h.pending_build()
    assert job is not None
    run_date: datetime = job.trigger.run_date
    delay = (run_date - clock.now()).total_seconds()
    clock.set(max(run_date, clock.now()))
    h.rec.scheduler.remove_job(UNIVERSE_BUILD_JOB)
    await job.func(*job.args)
    return delay


# ------------------------------------------------------------------ misfire


async def test_startup_cmc_runs_delayed_past_half_their_cadence_still_run_once(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    rec = harness.rec
    ran: list[str] = []

    async def run_cmc(job: RouteJob) -> None:
        ran.append(job.route_key)

    monkeypatch.setattr(rec, "_run_cmc", run_cmc)
    # The loop stalls 45 min between scheduling and the first wake-up: past half the hourly cadence of #11.
    with use_clock(ManualClock(datetime.now(UTC) - timedelta(minutes=45))):
        rec.schedule_cmc()
    hourly = [j.route_key for j in rec.jobs.values() if j.cadence_s == 3600]
    assert hourly
    rec.scheduler.start()
    await _until(lambda: len(ran) >= len(rec.jobs))
    await asyncio.sleep(0.3)  # no second run arrives
    assert Counter(ran) == Counter(j.route_key for j in rec.jobs.values())


# ------------------------------------------------------------------ universe build


@pytest.mark.parametrize("restart", [False, True], ids=["fresh-lake", "restart-with-old-inputs"])
async def test_first_universe_waits_for_both_cmc_input_routes(harness: Harness, restart: bool) -> None:
    h = harness
    if restart:  # records of an earlier process: only this process's own runs may start the build
        h.runner.store.append(_cmc("listings_latest", LISTINGS))
        h.runner.store.append(_cmc("crypto_map", CMC_MAP))
    listings, cmc_map = h.job("listings_latest"), h.job("crypto_map")
    h.rec.scheduler.start()
    await h.rec._run_cmc(listings)
    await asyncio.sleep(0.3)
    assert h.builds() == []  # #12 has not run yet: no build, no failing health row
    assert h.rec.bn.universe() is None
    await h.rec._run_cmc(cmc_map)
    await _until(lambda: h.rec.bn.universe() is not None)
    universe = h.rec.bn.universe()
    assert universe is not None
    assert universe.date == utcnow().date()
    assert [m.binance_symbol for m in universe.members] == ["BTCUSDT", "ETHUSDT"]
    assert h.builds() == [True]


async def test_input_runs_without_lake_inputs_do_not_spend_build_attempts(
    harness: Harness, clock: ManualClock
) -> None:
    h = harness
    h.runner.outcome = "failing"
    await h.rec._run_cmc(h.job("listings_latest"))
    await h.rec._run_cmc(h.job("crypto_map"))
    assert h.pending_build() is None
    h.runner.outcome = "ok"
    clock.advance(timedelta(hours=1))
    await h.rec._run_cmc(h.job("listings_latest"))
    assert h.pending_build() is None  # the map is still missing from the lake
    await h.rec._run_cmc(h.job("crypto_map"))
    await _run_pending(h, clock)
    assert h.builds() == [True]


async def test_failed_build_is_retried_with_capped_backoff_then_stops(
    harness: Harness, clock: ManualClock
) -> None:
    h = harness
    h.binance.failures = 1_000
    await h.rec._run_cmc(h.job("listings_latest"))
    await h.rec._run_cmc(h.job("crypto_map"))
    delays = [await _run_pending(h, clock)]
    while h.pending_build() is not None:
        delays.append(await _run_pending(h, clock))
    assert h.builds() == [False] * RETRY.max_attempts
    assert h.binance.builds() == RETRY.max_attempts
    assert delays[0] == pytest.approx(0.0, abs=1e-6)
    for failures, delay in enumerate(delays[1:], start=1):
        base = min(RETRY.backoff_s * 2 ** (failures - 1), RETRY.backoff_max_s)
        assert base <= delay <= base * (1 + RETRY.jitter_frac)
    # More input runs or the daily job the same day do not reopen the exhausted budget.
    await h.rec._run_cmc(h.job("listings_latest"))
    await h.rec.universe_daily()
    assert h.binance.builds() == RETRY.max_attempts
    assert h.pending_build() is None
    assert h.rec.bn.universe() is None


async def test_retry_stops_once_today_is_built_and_a_new_day_starts_a_new_budget(
    harness: Harness, clock: ManualClock
) -> None:
    h = harness
    h.binance.failures = 1
    await h.rec._run_cmc(h.job("listings_latest"))
    await h.rec._run_cmc(h.job("crypto_map"))
    await _run_pending(h, clock)
    assert h.pending_build() is not None
    await _run_pending(h, clock)
    assert h.builds() == [False, True]
    assert h.pending_build() is None
    await h.rec.universe_daily()  # today's universe exists: nothing is rebuilt
    assert h.binance.builds() == 2

    # Next UTC day: the 00:05 build fails once and is retried; a leftover retry for yesterday is a no-op.
    clock.set(datetime.combine(T0.date() + timedelta(days=1), T0.timetz()))
    h.binance.failures = 1
    await h.rec.universe_daily()
    assert h.builds() == [False, True, False]
    retry = h.pending_build()
    assert retry is not None
    assert retry.args == (clock.now().date(),)
    await h.rec.universe_attempt(T0.date())
    assert h.binance.builds() == 3
    await _run_pending(h, clock)
    assert h.builds() == [False, True, False, True]
    universe = h.rec.bn.universe()
    assert universe is not None
    assert universe.date == clock.now().date()


# ------------------------------------------------------------------ CMC key activation


async def test_new_key_version_brings_forward_only_the_jobs_the_key_could_not_serve(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """#9 (`once` backfill) failed for lack of a key: it runs again as soon as a key version is active,
    without a restart; a job that ran fine keeps its schedule, and the same version never re-triggers."""
    rec = harness.rec
    version: dict[str, int | None] = {"active": None}
    monkeypatch.setattr(rec.secrets, "active_version", lambda _name: version["active"])
    key_info_reads: list[int | None] = []

    async def read_key_info() -> bool:  # the new key may be on another plan: its rate limit is re-read
        key_info_reads.append(version["active"])
        return True

    monkeypatch.setattr(rec, "key_info", read_key_info)
    rec.scheduler.start(paused=True)
    rec.schedule_cmc()
    later = utcnow() + timedelta(days=1)

    def next_run(job_id: str) -> datetime:
        job = rec.scheduler.get_job(job_id)
        assert job is not None
        run_at: datetime = job.next_run_time
        return run_at

    def push_back() -> None:
        for job_id in ("cmc:9", "cmc:13"):
            rec.scheduler.modify_job(job_id, next_run_time=later)

    backfill, trending = rec.jobs["cmc:9"], rec.jobs["cmc:13"]
    assert backfill.once
    harness.runner.key_blocked = {backfill.route.name}
    await rec._run_cmc(backfill)
    await rec._run_cmc(trending)
    push_back()

    await rec.cmc_key_watch()  # still no key: nothing moves
    assert next_run("cmc:9") == later
    version["active"] = 1  # the key is entered on the console
    await rec.cmc_key_watch()
    assert next_run("cmc:9") <= utcnow()
    assert next_run("cmc:13") == later
    assert key_info_reads == [1]

    push_back()
    await rec.cmc_key_watch()  # same version: no second trigger
    assert next_run("cmc:9") == later

    harness.runner.key_blocked = set()
    await rec._run_cmc(backfill)  # the backfill ran with the key: nothing left to bring forward
    version["active"] = 2
    await rec.cmc_key_watch()
    assert next_run("cmc:9") == later
