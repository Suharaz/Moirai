"""CMC collectors: job catalog, paging keys, lake-derived targets, the 1010 stop order, 1006 plan refusals."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
import respx

from hdt.core.clock import ManualClock, use_clock
from hdt.core.config import static_config
from hdt.db.models.ingest import NOT_ON_PLAN_ERROR
from hdt.ingest.cmc_client import CmcClient, CmcResult, MinuteRateLimiter
from hdt.ingest.cmc_collectors import (
    CollectorRunner,
    JobResult,
    LakeView,
    Outcome,
    RouteJob,
    build_jobs,
    page_key,
    rank_exchanges,
)
from hdt.ingest.credit_governor import CreditGovernor
from hdt.lake.pit_query import PitQuery
from hdt.lake.raw_store import RawStore
from hdt.lake.schemas import Capture

BASE = "https://pro-api.coinmarketcap.com"
NOW = datetime(2026, 9, 27, 10, 0, tzinfo=UTC)


def _envelope(data: Any, code: int = 0, credits: int = 1, message: str | None = None) -> bytes:
    status = {
        "timestamp": "2026-09-27T10:00:00Z",
        "error_code": code,
        "error_message": message,
        "credit_count": credits,
    }
    return json.dumps({"status": status, "data": data}).encode()


class Harness:
    def __init__(self, tmp: Path, *, api_key: str | None = "k" * 32) -> None:
        self.store = RawStore(tmp / "staging", tmp / "lake")
        self.pit = PitQuery(tmp / "staging", tmp / "lake")
        self.governor = CreditGovernor(fallback_monthly_quota=450_000)
        self.health: list[tuple[str, Outcome]] = []

        async def sink(result: CmcResult) -> None:
            self.store.append(result.capture)
            self.governor.record(result.credit_count, result.capture.fetched_at)

        self.client = CmcClient(
            httpx.AsyncClient(),
            base_url=BASE,
            keyless_base_url=f"{BASE}/public-api",
            api_key=lambda: api_key,
            limiter=MinuteRateLimiter(60_000),
            on_response=sink,
            keyless_limiter=MinuteRateLimiter(60_000),
            backoff_base_s=0.001,
        )

        async def health(job: RouteJob, _at: datetime, outcome: Outcome, _err: str | None) -> None:
            self.health.append((job.route_key, outcome))

        self.runner = CollectorRunner(
            client=self.client, governor=self.governor, view=LakeView(self.pit), health=health
        )
        self.jobs = {job.route_key: job for job in build_jobs(static_config().cmc_routes)}


def test_catalog_splits_route_2_and_leaves_out_meter_event_and_stream_routes() -> None:
    keys = {job.route_key for job in build_jobs(static_config().cmc_routes)}
    assert {"2:core", "2:peripheral", "1", "3", "6", "11", "12", "27", "28"} <= keys
    assert not keys & {"2", "24", "25", "26", "29", "30"}


def test_page_keys_are_valid_lake_keys() -> None:
    assert page_key("", 1) == ""
    assert page_key("", 3) == "p3"
    assert page_key("binance", 2) == "binance:p2"


def test_exchanges_rank_by_liquidity_score_with_missing_scores_last() -> None:
    data = {
        "exchanges": [
            {"exchange_id": 1, "exchange_slug": "a", "liquidity_score": 50},
            {"exchange_id": 2, "exchange_slug": "b", "liquidity_score": None},
            {"exchange_id": 3, "exchange_slug": "c", "liquidity_score": 90},
        ]
    }
    assert [e.slug for e in rank_exchanges(data)] == ["c", "a", "b"]


@respx.mock
async def test_has_more_lists_are_paged_and_stored_under_page_keys(tmp_path: Path) -> None:
    h = Harness(tmp_path)
    pages = [
        _envelope({"cryptocurrencies": [{"crypto_id": 1}], "has_more": True}),
        _envelope({"cryptocurrencies": [{"crypto_id": 2}], "has_more": False}),
    ]
    route = respx.get(f"{BASE}/v5/derivatives/liquidations/cryptocurrency/list/latest").mock(
        side_effect=[httpx.Response(200, content=p) for p in pages]
    )
    with use_clock(ManualClock(NOW)):
        result = await h.runner.run(h.jobs["6"])
    assert result == JobResult("6", "ok", 2)
    assert [dict(c.request.url.params)["start"] for c in route.calls] == ["1", "251"]
    assert h.pit.latest("cmc", "liquidations_by_crypto", NOW, key="p2") is not None


@respx.mock
async def test_route_2_targets_come_from_the_months_first_route_1_record(tmp_path: Path) -> None:
    h = Harness(tmp_path)
    exchanges = [{"exchange_id": i, "exchange_slug": f"ex{i}", "liquidity_score": 100 - i} for i in range(10)]
    respx.get(f"{BASE}/v5/exchange/derivatives/list").mock(
        return_value=httpx.Response(200, content=_envelope({"exchanges": exchanges}))
    )
    pairs = respx.get(f"{BASE}/v5/exchange/derivatives/market-pairs/list/latest").mock(
        return_value=httpx.Response(200, content=_envelope({"market_pairs": []}))
    )
    clock = ManualClock(NOW)
    with use_clock(clock):
        pending = await h.runner.run(h.jobs["2:core"])
        assert pending.outcome == "pending"  # no route #1 record yet: nothing called
        assert not pairs.called
        await h.runner.run(h.jobs["1"])
        clock.advance(timedelta(seconds=1))
        await h.runner.run(h.jobs["2:core"])
        await h.runner.run(h.jobs["2:peripheral"])
    slugs = [dict(c.request.url.params)["exchange_slug"] for c in pairs.calls]
    assert slugs == ["ex0", "ex1", "ex2", "ex3", "ex4", "ex5", "ex6", "ex7"]


@respx.mock
async def test_monthly_cap_halts_every_route_without_retry(tmp_path: Path) -> None:
    """Simulated 1010: the failing call is not retried and every later job is refused before any HTTP."""
    h = Harness(tmp_path)
    capped = respx.get(f"{BASE}/v5/derivatives/liquidations/quotes/latest").mock(
        return_value=httpx.Response(429, content=_envelope(None, code=1010, credits=0))
    )
    others = respx.get(url__regex=rf"{BASE}/v(1|3|5)/(?!derivatives/liquidations/quotes).*").mock(
        return_value=httpx.Response(200, content=_envelope([]))
    )
    with use_clock(ManualClock(NOW)):
        first = await h.runner.run(h.jobs["4"])
        later = [await h.runner.run(h.jobs[k]) for k in ("20", "13", "16", "6", "1")]
    assert first.outcome == "halted"
    assert capped.call_count == 1
    assert [r.outcome for r in later] == ["halted"] * 5
    assert not others.called
    assert h.governor.halt_state(NOW) == "monthly_cap"


@respx.mock
async def test_shedding_stops_context_before_derivatives(tmp_path: Path) -> None:
    h = Harness(tmp_path)
    respx.get(url__regex=rf"{BASE}.*").mock(return_value=httpx.Response(200, content=_envelope({"value": 1})))
    with use_clock(ManualClock(NOW)):
        h.governor.record(0, NOW)  # opens today's budget (quota / 30 before any key info)
        h.governor.record(h.governor.daily_budget * 72 // 100, NOW)  # 72 % of today's budget used
        context = await h.runner.run(h.jobs["20"])  # global metrics, shed class context
        derivatives = await h.runner.run(h.jobs["4"])
    assert context.outcome == "shed"
    assert derivatives.outcome == "ok"
    assert ("20", "shed") in h.health


PLAN_REFUSAL = _envelope(
    None, code=1006, credits=0, message="Your API Key subscription plan doesn't support this endpoint."
)


@respx.mock
async def test_plan_refusal_disables_the_route_until_the_plan_allows_it(tmp_path: Path) -> None:
    """1006: `disabled` (not failing), no retry; after a plan upgrade the next call is `ok` on the job."""
    h = Harness(tmp_path)
    trending = respx.get(f"{BASE}/v1/cryptocurrency/trending/latest").mock(
        side_effect=[
            httpx.Response(403, content=PLAN_REFUSAL),
            httpx.Response(200, content=_envelope([{"id": 1}])),
        ]
    )
    job = h.jobs["13"]
    with use_clock(ManualClock(NOW)):
        refused = await h.runner.run(job)
        assert trending.call_count == 1
        upgraded = await h.runner.run(job)
    assert refused.outcome == "disabled"
    assert refused.key_blocked
    assert refused.error is not None
    assert refused.error.startswith(f"{NOT_ON_PLAN_ERROR}: trending_latest: Your API Key subscription plan")
    assert upgraded == JobResult("13", "ok", 1)
    assert h.health == [("13", "disabled"), ("13", "ok")]
    assert job.cadence_s == 900  # the refused route keeps its normal cadence


@respx.mock
@pytest.mark.parametrize(
    ("response", "calls"),
    [
        (httpx.Response(401, content=_envelope(None, code=1001, credits=0, message="invalid key")), 1),
        (httpx.Response(403, content=_envelope(None, code=1007, credits=0, message="disabled key")), 1),
        (httpx.Response(400, content=_envelope(None, code=400, credits=0, message="bad request")), 1),
        (httpx.Response(503, content=b"unavailable"), 3),
    ],
    ids=["1001", "1007", "400", "5xx"],
)
async def test_genuine_errors_stay_failing(tmp_path: Path, response: httpx.Response, calls: int) -> None:
    h = Harness(tmp_path)
    route = respx.get(f"{BASE}/v1/cryptocurrency/trending/latest").mock(return_value=response)
    with use_clock(ManualClock(NOW)):
        result = await h.runner.run(h.jobs["13"])
    assert (result.outcome, result.key_blocked) == ("failing", False)
    assert result.error is not None
    assert not result.error.startswith(NOT_ON_PLAN_ERROR)
    assert route.call_count == calls


async def test_a_run_without_an_active_key_is_failing_and_waits_for_the_key(tmp_path: Path) -> None:
    h = Harness(tmp_path, api_key=None)
    with use_clock(ManualClock(NOW)):
        result = await h.runner.run(h.jobs["13"])
    assert (result.outcome, result.calls, result.key_blocked) == ("failing", 0, True)


UNI = "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984"
BONK = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"
APTOS = "0x" + "a1" * 32
MAP = [
    {"id": 7083, "platform": {"slug": "ethereum", "token_address": UNI}},
    # HYPE: the map's address is its 16-byte HyperCore token index, not a contract (CMC: 400 Parameter error)
    {"id": 32196, "platform": {"slug": "hyperliquid", "token_address": "0x0d01dc56dcaaca66ad901c959b4011ec"}},
    {"id": 1, "platform": None},
    {"id": 5426, "platform": {"slug": "solana", "token_address": BONK}},
    {"id": 21794, "platform": {"slug": "aptos", "token_address": APTOS}},
]


class _Targets:
    """The recorded map's token refs through a real `LakeView`, with a fixed universe."""

    def __init__(self, view: LakeView, ltx: list[int], watchlist: list[int]) -> None:
        self._view = view
        self._ltx = ltx
        self._watchlist = watchlist

    def token_refs(self) -> Any:
        return self._view.token_refs()

    def has_record(self, route: str, key: str, params: Any = None) -> bool:
        return self._view.has_record(route, key, params)

    def universe(self) -> Any:
        return SimpleNamespace(
            ltx=[SimpleNamespace(cmc_id=i) for i in self._ltx],
            watchlist=[SimpleNamespace(cmc_id=i) for i in self._watchlist],
        )


@respx.mock
async def test_dex_holders_posts_contract_addresses_only(tmp_path: Path) -> None:
    h = Harness(tmp_path)
    h.store.append(
        Capture(
            source="cmc",
            route="crypto_map",
            fetched_at=NOW - timedelta(minutes=5),
            http_status=200,
            body=_envelope(MAP),
            params={},
        )
    )
    holders = respx.post(f"{BASE}/public-api/v1/dex/holders/list").mock(
        return_value=httpx.Response(200, content=_envelope({"holders": []}))
    )
    # Only BTC is on the watchlist: the other coins are LTX candidates and still need their holders.
    targets = _Targets(LakeView(h.pit), ltx=[1, 7083, 32196, 5426, 21794], watchlist=[1])
    runner = CollectorRunner(
        client=h.client,
        governor=h.governor,
        view=targets,  # type: ignore[arg-type]
        health=_no_health,
    )
    with use_clock(ManualClock(NOW)):
        result = await runner.run(h.jobs["27"])
    assert result == JobResult("27", "ok", 3)
    assert [json.loads(call.request.content) for call in holders.calls] == [
        {"tokenAddress": UNI, "platform": "ethereum", "tag": "tag_all"},
        {"tokenAddress": BONK, "platform": "solana", "tag": "tag_all"},
        {"tokenAddress": APTOS, "platform": "aptos", "tag": "tag_all"},
    ]
    assert all(not call.request.url.params for call in holders.calls)
    stored = {k: h.pit.latest("cmc", "dex_holders", NOW, key=k) for k in ("7083", "5426", "21794", "32196")}
    assert {k for k, record in stored.items() if record is not None} == {"7083", "5426", "21794"}


@respx.mock
async def test_ohlcv_backfills_each_ltx_coin_once_next_to_the_hourly_call(tmp_path: Path) -> None:
    h = Harness(tmp_path)
    ohlcv = respx.get(f"{BASE}/v2/cryptocurrency/ohlcv/historical").mock(
        side_effect=lambda request: httpx.Response(
            # The second backfilled coin fails: it is asked again on the next run.
            500 if request.url.params.get("id") == "1027" else 200,
            content=_envelope({"id": 1, "quotes": []}),
        )
    )
    # BTC already has a backfill from before route #7 sent `interval` (daily-sampled): it is fetched again.
    h.store.append(
        Capture(
            source="cmc",
            route="ohlcv_backfill",
            fetched_at=NOW - timedelta(hours=1),
            http_status=200,
            body=_envelope({"id": 1, "quotes": []}),
            params={"time_period": "hourly", "id": "1", "count": "720"},
            key="1",
        )
    )
    targets = _Targets(LakeView(h.pit), ltx=[1, 1027], watchlist=[1])
    runner = CollectorRunner(
        client=h.client,
        governor=h.governor,
        view=targets,  # type: ignore[arg-type]
        health=_no_health,
    )

    async def run_calls() -> list[dict[str, str]]:
        seen = len(ohlcv.calls)
        with use_clock(ManualClock(NOW)):
            await runner.run(h.jobs["7"])
        return [dict(call.request.url.params) for call in ohlcv.calls][seen:]

    first = await run_calls()
    hourly = {"time_period": "hourly", "interval": "hourly", "id": "1,1027", "count": "2"}
    assert first[0] == hourly
    backfill_btc = {"time_period": "hourly", "interval": "hourly", "id": "1", "count": "720"}
    assert [c for c in first[1:] if c["id"] == "1"] == [backfill_btc]
    assert {c["id"] for c in first[1:]} == {"1", "1027"}
    assert h.pit.latest("cmc", "ohlcv_historical", NOW, key="") is not None
    assert h.pit.latest("cmc", "ohlcv_historical", NOW, key="1") is None
    assert h.pit.latest("cmc", "ohlcv_backfill", NOW, key="1") is not None
    again = await run_calls()
    assert again[0] == hourly
    assert {c["id"] for c in again[1:]} == {"1027"}


async def _no_health(*_args: Any) -> None:
    return None
