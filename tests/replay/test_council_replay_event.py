"""Council replay entry point (`hdt.council.replay`) over events recorded by the deterministic fixtures:
the recorded card hash is reproduced on the recorded clock, any changed output or cache miss is reported, a
recorded transient abstain comes back verbatim, and the range report carries the phase 05 agent metrics."""

from __future__ import annotations

import contextlib
import dataclasses
from datetime import UTC, datetime, timedelta

import pytest
from test_council_deterministic import FEATURES, P_MODEL, TURNS, C, M, N, T, X

import hdt.council.replay as replay_module
from council_builders import (
    AS_OF,
    Params,
    ScriptedRunner,
    Turn,
    UniformParams,
    event_input,
    run_meeting,
    services,
)
from hdt.agents.llm_types import LlmCacheMissError
from hdt.agents.runner import REASON_INTERNAL, REASON_LLM_OUTPUT, REASON_LLM_UNAVAILABLE
from hdt.agents.validate import logit
from hdt.contracts.common import AgentName
from hdt.contracts.forecast import LakeRef, ToolCallRecord
from hdt.core.clock import ManualClock, use_clock
from hdt.core.ids import to_canonical
from hdt.council.ports import AgentRequest, AgentResult
from hdt.council.replay import (
    EventReplay,
    NothingToReplayError,
    PgReplay,
    RecordedEvent,
    ReplayReport,
    ReplayServices,
    replay_event,
)

START = AS_OF + timedelta(seconds=5)


async def record(runner: ScriptedRunner, *, event_id: str = "ev_test_1") -> RecordedEvent:
    """A meeting run live-like by the fixtures (on its own clock), as the replay reads it back."""
    with use_clock(ManualClock(START)):
        decision = await run_meeting(services(runner), event_id=event_id)
    return RecordedEvent.from_record(event_input(event_id=event_id)["event"], decision)


def replay_services(runner: ScriptedRunner) -> ReplayServices:
    s = services(runner)
    return ReplayServices(
        runner=runner,
        packets=s.packets,
        candidate_sets=s.candidate_sets,
        sources=s.sources,
        params=s.params,
        settings=s.settings,
    )


def scripted(turns: dict[tuple[AgentName, int], Turn] | None = None) -> ScriptedRunner:
    return ScriptedRunner(p_model=P_MODEL, turns=turns if turns is not None else TURNS, features=FEATURES)


class Failing(ScriptedRunner):
    """Records `reason` as the abstain of `failing` (the live model failed there)."""

    def __init__(self, failing: tuple[AgentName, int], reason: str) -> None:
        super().__init__(p_model=P_MODEL, turns={**TURNS, failing: Turn(None)}, features=FEATURES)
        self.failing = failing
        self.reason = reason

    async def forecast(self, request: AgentRequest) -> AgentResult:
        result = await super().forecast(request)
        if (request.agent, request.round) != self.failing:
            return result
        return AgentResult(forecast=result.forecast.model_copy(update={"abstain_reason": self.reason}))


class Missing(ScriptedRunner):
    """A replay runner whose cache has no reply for `missing` (every other call is served)."""

    def __init__(self, missing: tuple[AgentName, int]) -> None:
        super().__init__(p_model=P_MODEL, turns=TURNS, features=FEATURES)
        self.missing = missing

    async def forecast(self, request: AgentRequest) -> AgentResult:
        if (request.agent, request.round) == self.missing:
            raise LlmCacheMissError(f"no cached reply for {request.agent.value} round {request.round}")
        return await super().forecast(request)


class WithTool(ScriptedRunner):
    """Every Technical forecast made one quant_core call taking `latency_ms` with output `output`."""

    def __init__(self, latency_ms: int, output: str = "b") -> None:
        super().__init__(p_model=P_MODEL, turns=TURNS, features=FEATURES)
        self.call = ToolCallRecord(
            tool="quant_core",
            input_sha256="a" * 64,
            output_sha256=output * 64,
            status="ok",
            latency_ms=latency_ms,
        )

    async def forecast(self, request: AgentRequest) -> AgentResult:
        result = await super().forecast(request)
        if request.agent is not T:
            return result
        return AgentResult(forecast=result.forecast.model_copy(update={"tool_calls": (self.call,)}))


async def test_a_recorded_event_replays_to_its_card_hash_on_the_recorded_clock() -> None:
    recorded = await record(scripted())
    assert recorded.started_at == START

    replay = await replay_event(recorded, replay_services(scripted()))

    assert replay.status == "identical", replay.differences
    assert replay.replayed_card_sha256 == recorded.card_sha256
    assert replay.record is not None
    assert replay.record.card["rounds"] >= 2  # the debate is part of the replay
    # The injected clock is the recorded meeting start, whatever the wall clock says.
    assert {e["at"] for e in replay.record.card["timeline"]} == {to_canonical(START)}
    assert all(not run.verbatim for run in replay.runs)


async def test_a_changed_agent_output_is_reported_field_by_field() -> None:
    recorded = await record(scripted())

    replay = await replay_event(recorded, replay_services(scripted({**TURNS, (X, 1): Turn(0.49)})))

    assert replay.status == "different"
    assert replay.replayed_card_sha256 != recorded.card_sha256
    by_key = {d.key: d for d in replay.differences}
    assert (by_key["forecast.macro.1.submitted.p_llm"].recorded, by_key["card.card_sha256"].recorded) == (
        0.5,
        recorded.card_sha256,
    )
    assert by_key["forecast.macro.1.submitted.p_llm"].replayed == 0.49
    assert "recorded.card_sha256" not in by_key  # the stored rows themselves are intact


async def test_a_doctored_record_is_reported_even_when_the_replay_matches_it() -> None:
    recorded = await record(scripted())
    rows = [dict(r) for r in recorded.forecasts]
    rows[0] = {**rows[0], "stance": "SHORT" if rows[0]["stance"] != "SHORT" else "LONG"}
    doctored = RecordedEvent(
        event=recorded.event,
        card=recorded.card,
        forecasts=tuple(rows),
        claims=recorded.claims,
        shadow=recorded.shadow,
        started_at=recorded.started_at,
    )

    replay = await replay_event(doctored, replay_services(scripted()))

    assert replay.status == "different"
    keys = {d.key for d in replay.differences}
    assert "recorded.card_sha256" in keys
    assert f"forecast.{rows[0]['agent']}.{rows[0]['round']}.stance" in keys


async def test_a_cache_miss_fails_the_replay_as_a_cache_miss() -> None:
    recorded = await record(scripted())

    replay = await replay_event(recorded, replay_services(Missing((M, 2))))

    assert replay.status == "cache_miss"
    assert replay.error == "no cached reply for micro round 2"
    assert replay.record is None
    report = ReplayReport((replay,))
    assert report.to_json()["cache_misses"] == [{"event_id": "ev_test_1", "error": replay.error}]
    assert report.exit_code() == 1


async def test_a_recorded_transient_abstain_is_returned_verbatim() -> None:
    recorded = await record(Failing((X, 1), REASON_LLM_UNAVAILABLE))

    # The live call failed, so nothing was cached for it: the replay misses there and keeps the recording.
    replay = await replay_event(recorded, replay_services(Missing((X, 1))))

    assert replay.status == "identical", replay.differences
    macro = next(r for r in replay.runs if r.agent == X.value and r.round == 1)
    assert (macro.verbatim, macro.replayed, macro.forecast.abstain_reason) == (
        True,
        "cache_miss",
        REASON_LLM_UNAVAILABLE,
    )
    news = next(r for r in replay.runs if r.agent == N.value)
    assert not news.verbatim  # a scripted (reproducible) abstain is replayed, not copied


async def test_a_transient_abstain_whose_replay_now_forecasts_is_a_difference() -> None:
    """N2 repro: the recorded abstain was copied whatever the replay produced, so a replay that forecast
    where the live meeting abstained still came out `identical`."""
    recorded = await record(Failing((X, 1), REASON_LLM_UNAVAILABLE))

    replay = await replay_event(recorded, replay_services(scripted({**TURNS, (X, 1): Turn(0.49)})))

    assert replay.status == "different"
    diff = next(d for d in replay.differences if d.key == "forecast.macro.1.replayed")
    assert (diff.recorded, diff.replayed) == (REASON_LLM_UNAVAILABLE, "forecast")


async def test_a_transient_abstain_whose_replay_abstains_for_another_reason_is_a_difference() -> None:
    recorded = await record(Failing((X, 1), REASON_LLM_UNAVAILABLE))

    replay = await replay_event(recorded, replay_services(Failing((X, 1), REASON_LLM_OUTPUT)))

    assert replay.status == "different"
    diff = next(d for d in replay.differences if d.key == "forecast.macro.1.replayed")
    assert (diff.recorded, diff.replayed) == (REASON_LLM_UNAVAILABLE, REASON_LLM_OUTPUT)


async def test_an_internal_error_abstain_is_replayed_not_copied() -> None:
    """N2: `internal_error` is deterministic under the recorded inputs, so a change there must show."""
    recorded = await record(Failing((X, 1), REASON_INTERNAL))

    replay = await replay_event(recorded, replay_services(scripted({**TURNS, (X, 1): Turn(0.49)})))

    assert replay.status != "identical"
    assert not next(r for r in replay.runs if r.agent == X.value and r.round == 1).verbatim


async def test_a_shadow_runs_captures_are_pinned_for_its_replay() -> None:
    """m4: pins were read from the main forecasts only, so a capture made only by the round-1 A/B run with
    a shadow lesson was not available when that run was replayed."""
    recorded = await record(scripted())
    main = recorded.submitted(X, 1)
    assert main is not None
    capture = LakeRef(
        source="news",
        route="fetch_source",
        key="n_1",
        fetched_at=AS_OF + timedelta(seconds=3),
        http_status=200,
        body_sha256="1" * 64,
    )
    shadow = main.model_copy(update={"capture_manifest": (capture,)})
    with_shadow = dataclasses.replace(
        recorded, shadow={(X.value, "lesson_1"): shadow.model_dump(mode="json")}
    )

    assert capture in with_shadow.pinned(recorded.event_id, X, 1)
    assert capture not in with_shadow.pinned(recorded.event_id, T, 1)
    assert capture not in recorded.pinned(recorded.event_id, X, 1)


async def test_the_replay_loads_the_parameter_pins_recorded_on_the_card() -> None:
    """m11: the replay's `load_context` resolved the stacker and overlays by time, not from the record."""
    pins = {"stacker_trained_through": None, "live_versions": {"macro": 2}, "flagged": ["news"]}
    live = services(scripted(), params=Params(UniformParams(params_version=4, pins=pins)))
    with use_clock(ManualClock(START)):
        decision = await run_meeting(live)
    recorded = RecordedEvent.from_record(event_input()["event"], decision)

    replay_params = Params()
    services_ = dataclasses.replace(replay_services(scripted()), params=replay_params)
    await replay_event(recorded, services_)

    assert replay_params.calls[0] == (AS_OF, 4)
    assert replay_params.pins_seen[0] == pins


async def test_a_reproducible_abstain_is_replayed_so_a_change_shows() -> None:
    recorded = await record(scripted())

    replay = await replay_event(recorded, replay_services(scripted({**TURNS, (N, 1): Turn(0.55)})))

    assert replay.status == "different"
    assert "forecast.news.1.submitted.abstain" in {d.key for d in replay.differences}


async def test_recorded_tool_latency_is_kept_for_identical_calls_only() -> None:
    recorded = await record(WithTool(latency_ms=120))

    same = await replay_event(recorded, replay_services(WithTool(latency_ms=7)))
    other = await replay_event(recorded, replay_services(WithTool(latency_ms=120, output="c")))

    assert same.status == "identical", same.differences
    assert other.status == "different"
    assert "forecast.technical.1.submitted.tool_calls" in {d.key for d in other.differences}


async def test_the_range_report_carries_the_agent_metrics_and_passes_when_all_is_identical() -> None:
    first = await record(scripted(), event_id="ev_a")
    second = await record(scripted(), event_id="ev_b")
    replays = (
        await replay_event(first, replay_services(scripted())),
        await replay_event(second, replay_services(scripted())),
    )
    start = datetime(2026, 3, 2, tzinfo=UTC)
    report = ReplayReport(replays, start=start, end=start + timedelta(days=1))

    out = report.to_json()
    assert (out["events"], out["identical"], out["different"], out["cache_misses"], out["errors"]) == (
        2,
        2,
        [],
        [],
        [],
    )
    assert (out["start"], out["end"]) == ("2026-03-02T00:00:00.000000Z", "2026-03-03T00:00:00.000000Z")
    assert set(out["agents"]) == {a.value for a in AgentName}
    technical = out["agents"][T.value]
    # Two events of two rounds each; Technical: p_model 0.45, p_llm 0.42 in round 1 and 0.60 in round 2.
    assert (technical["runs"], technical["llm_forecasts"]) == (4, 4)
    assert technical["parse_errors"] == 0
    assert technical["parse_error_rate"] == 0.0
    assert abs(technical["mean_abs_p_llm_minus_p_model"] - 0.09) < 1e-9
    expected_dz = (abs(logit(0.42) - logit(0.45)) + abs(logit(0.60) - logit(0.45))) / 2
    assert abs(technical["mean_abs_z_llm_minus_z_model"] - expected_dz) < 1e-9
    assert out["agents"][N.value]["abstain_reasons"] == {"scripted abstain": 2}
    assert out["agents"][C.value]["llm_forecasts"] == 4
    assert out["passed"] is True
    assert report.exit_code() == 0


async def test_a_parse_error_rate_of_one_percent_or_more_fails_the_report() -> None:
    recorded = await record(Failing((M, 1), REASON_LLM_OUTPUT))
    replay = await replay_event(recorded, replay_services(Missing((M, 1))))
    assert replay.status == "identical", replay.differences

    report = ReplayReport((replay,))

    micro = report.to_json()["agents"][M.value]
    assert (micro["runs"], micro["parse_errors"], micro["parse_error_rate"]) == (1, 1, 1.0)
    assert report.parse_error_rate_exceeded() == [M.value]
    assert report.exit_code() == 1


def test_nothing_to_replay_is_its_own_exit_code() -> None:
    assert ReplayReport(()).exit_code() == 2


class OneBrokenEvent(PgReplay):
    async def event(self, event_id: str) -> EventReplay:
        if event_id == "ev_bad":
            raise KeyError("card_sha256")
        return EventReplay(event_id, "identical", "a" * 64, "a" * 64, (), ())


async def test_one_broken_event_does_not_abort_the_range(monkeypatch: pytest.MonkeyPatch) -> None:
    """m12: one event that failed to load aborted the whole range."""
    monkeypatch.setattr(
        replay_module, "recorded_event_ids", lambda conn, start, end: ["ev_a", "ev_bad", "ev_b"]
    )

    class Engine:
        def connect(self) -> contextlib.nullcontext[None]:
            return contextlib.nullcontext()

    start = datetime(2026, 3, 2, tzinfo=UTC)
    replay = OneBrokenEvent(engine=Engine(), services=replay_services(scripted()))  # type: ignore[arg-type]
    report = await replay.range(start, start + timedelta(days=1))

    assert [(e.event_id, e.status) for e in report.events] == [
        ("ev_a", "identical"),
        ("ev_bad", "error"),
        ("ev_b", "identical"),
    ]
    assert report.exit_code() == 1


def test_only_a_missing_event_is_nothing_to_replay(monkeypatch: pytest.MonkeyPatch) -> None:
    """m12: any `KeyError` (a `LookupError`) inside the replay was printed as "nothing to replay", exit 2."""

    async def missing(args: object, window: object) -> ReplayReport:
        raise NothingToReplayError("no council event ev_x")

    async def broken(args: object, window: object) -> ReplayReport:
        raise KeyError("card_sha256")

    monkeypatch.setattr(replay_module, "configure_logging", lambda *a, **k: None)
    monkeypatch.setattr(replay_module, "_run", missing)
    assert replay_module.main(["event", "ev_x"]) == 2
    monkeypatch.setattr(replay_module, "_run", broken)
    with pytest.raises(KeyError):
        replay_module.main(["event", "ev_x"])
