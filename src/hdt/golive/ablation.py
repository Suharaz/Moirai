"""Numeric ablation replay (phase 11 Red Team Delta #24, #29), pure.

Every variant is recomputed from what the decision card and its stored forecasts already hold: the round-1
and final-round `p_used` / `p_model` of each agent, the `w`, `r`, `b` and parameter snapshot (the
calibration maps of `scoring_params`) in force at decision time, and the council rules of the card's pinned
`council` config version (`ReplayEvent.rules`; the caller's rules only when the event carries none). Nothing
is re-asked from an LLM, so a replay is a pure function of the stored rows and gives identical numbers on
every run. A log-pool event whose replayed `full` differs from its stored `p_pooled` is a replay mismatch:
the replay does not reproduce what the council ran, so the ablation fails (`retained_components_win`).

Probability variants (scored with the clipped log-loss of `hdt.scoring.loss` against the 12 h label):
- `full`: the log pool of round 1 mixed with the final round by the learned `r` (what the council ran);
- `round1_pool`: round 1 only (no debate mixing); `r1`: `r = 1` for every agent (final round only);
- `a0`: `p_model` only (`a = 0`, no LLM adjustment);
- `drop:<agent>`: the round-1 pool without that agent (compared with `round1_pool`).
Decision variants (the probability the system acted on: the pooled `p` when the manager trades, 0.5 when it
does not, so skipping a good trade costs log-loss and skipping a bad one saves it):
- consensus threshold: the configured supermajority (2/3) vs a simple majority (1/2);
- threshold cuts from the scanner superset log: only strict triggers, then score cuts at multiples of the
  strict threshold (LTX `spike_min`, MIGRATION `zm_min`); a cut replaces every event below it by 0.5.

Components (`debate`, `llm_adjustment`, `agent:<name>`) are tested with a one-sided UTC-day block paired
bootstrap of `loss_variant - loss_full`; the Holm family is the retained components that could be tested
(a component that is not retained never decides, so it never dilutes the correction). A component wins when
the mean difference is positive and its Holm-adjusted p-value is below `alpha`. Decision variants form their
own Holm family (report only).
Debate-dependent ablations (anything that would change what agents said in rounds 2-3) cannot be replayed;
they are listed as forward shadow branches and never computed here.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from hdt.council.aggregate import aggregate
from hdt.council.consensus import consensus
from hdt.council.manager import Outcome, manage
from hdt.golive.stats import BOOTSTRAP_RESAMPLES, BOOTSTRAP_SEED, PairedBootstrap, holm, paired_bootstrap
from hdt.scoring.calibration import IsotonicMap
from hdt.scoring.loss import DEFAULT_CLIP, log_loss

ALPHA = 0.05
DEBATE_OVER = frozenset({"max_rounds", "no_new_claims", "debate_disabled"})
"""Same set as `hdt.council.graph.DEBATE_OVER` (the manager fallback applies only once the debate is over)."""
SIMPLE_MAJORITY = 0.5
CUT_MULTIPLES = (1.0, 1.25, 1.5, 2.0)
FORWARD_SHADOW_BRANCHES: tuple[tuple[str, str], ...] = (
    ("debate_without_agent", "Debate with one agent removed: the others would have read different claims"),
    ("max_rounds", "A different round cap changes which revisions exist"),
    ("a0_in_debate", "a = 0 during the debate changes the revisions agents made"),
    ("consensus_stop", "A different consensus threshold changes when the debate stops"),
    (
        "no_debate_branch",
        "Debate vs no-debate decided on forward shadow branches (Design Contract section 3)",
    ),
)


@dataclass(frozen=True)
class AgentPath:
    agent: str
    p_round1: float | None
    """Round-1 `p_used` (None: abstained)."""
    p_final: float | None
    """Effective final-round `p_used` (None: abstained in the final round)."""
    p_model_round1: float | None
    p_model_final: float | None


@dataclass(frozen=True)
class ReplayEvent:
    event_id: str
    coin_id: int
    as_of: datetime
    source: str
    target_type: str
    label_spec_version: str
    y: int
    rounds: int
    stop_reason: str
    p_pooled: float | None
    disagreement: float | None
    method: str | None
    params_version: int
    weights: Mapping[str, float]
    r: Mapping[str, float]
    a: Mapping[str, float]
    b: float
    maps: Mapping[str, IsotonicMap]
    cal_clip: tuple[float, float]
    agents: tuple[AgentPath, ...]
    scan_score: float | None = None
    scan_strict: bool | None = None
    scan_side: str | None = None
    """Signal direction of the scanner row (LONG / SHORT)."""
    label_value: float | None = None
    beta_btc: float | None = None
    rules: CouncilRules | None = None
    """The council rules of the card's pinned `council` config version (None: use the caller's rules)."""
    config_version_ids: Mapping[str, int] = field(default_factory=dict)
    """The card's pinned config versions (`decision_cards.config_version_ids`)."""
    agent_pins: Mapping[str, tuple[str | None, str | None]] = field(default_factory=dict)
    """agent -> (model_slug, agent_version) recorded on the card."""


@dataclass(frozen=True)
class CouncilRules:
    """The pool and decision parameters of `config/council.yaml` the replay needs."""

    agents: tuple[str, ...]
    p_clip: tuple[float, float]
    stance_long: float
    stance_short: float
    quorum: int
    supermajority: float
    groups: Mapping[str, tuple[str, ...]]
    p_long: float
    p_short: float
    d_threshold: float
    size_consensus: float
    size_fallback: float

    @classmethod
    def from_config(cls, council: Any) -> CouncilRules:
        m = council.manager
        return cls(
            agents=tuple(a.value if hasattr(a, "value") else str(a) for a in council.agents),
            p_clip=(float(council.p_clip[0]), float(council.p_clip[1])),
            stance_long=council.stance_long,
            stance_short=council.stance_short,
            quorum=council.quorum,
            supermajority=council.supermajority,
            groups={
                name: tuple(a.value if hasattr(a, "value") else str(a) for a in members)
                for name, members in council.correlation_groups.items()
            },
            p_long=m.p_long,
            p_short=m.p_short,
            d_threshold=m.d_threshold,
            size_consensus=m.size_consensus,
            size_fallback=m.size_fallback,
        )


# ------------------------------------------------------------------------------------------ pooling


def _pool(
    ev: ReplayEvent,
    round1: Mapping[str, float],
    final: Mapping[str, float],
    r: Callable[[str], float],
    rules: CouncilRules,
) -> float | None:
    result = aggregate(
        round1,
        final,
        weights=ev.weights,
        r=r,
        calibrate=lambda agent, p: ev.maps.get(agent, IsotonicMap())(p, ev.cal_clip),
        intercept_b=ev.b,
        p_clip=rules.p_clip,
    )
    return None if result is None else result.p


def _round1(ev: ReplayEvent) -> dict[str, float]:
    return {a.agent: a.p_round1 for a in ev.agents if a.p_round1 is not None}


def _final(ev: ReplayEvent) -> dict[str, float]:
    return {a.agent: a.p_final for a in ev.agents if a.p_final is not None}


def _learned_r(ev: ReplayEvent) -> Callable[[str], float]:
    return lambda agent: float(ev.r.get(agent, 0.0))


def variant_probabilities(ev: ReplayEvent, rules: CouncilRules) -> dict[str, float | None]:
    """Every probability variant of one event (None when nobody has an opinion in that variant)."""
    r1, fin = _round1(ev), _final(ev)
    model_r1 = {
        a.agent: a.p_model_round1
        for a in ev.agents
        if a.p_round1 is not None and a.p_model_round1 is not None
    }
    model_fin = {
        a.agent: (a.p_model_final if a.p_model_final is not None else a.p_model_round1)
        for a in ev.agents
        if a.p_final is not None and (a.p_model_final is not None or a.p_model_round1 is not None)
    }
    out: dict[str, float | None] = {
        "full": _pool(ev, r1, fin, _learned_r(ev), rules),
        "round1_pool": _pool(ev, r1, r1, _learned_r(ev), rules),
        "r1": _pool(ev, r1, fin, lambda _agent: 1.0, rules),
        "a0": _pool(
            ev, model_r1, {k: v for k, v in model_fin.items() if v is not None}, _learned_r(ev), rules
        ),
    }
    for agent in dict.fromkeys([*rules.agents, *sorted(r1)]):
        dropped = {k: v for k, v in r1.items() if k != agent}
        out[f"drop:{agent}"] = _pool(ev, dropped, dropped, _learned_r(ev), rules)
    return out


def acted_probability(ev: ReplayEvent, rules: CouncilRules, *, supermajority: float) -> float | None:
    """The pooled `p` when the manager would trade under `supermajority`, 0.5 when it would not."""
    fin = _final(ev)
    p = ev.p_pooled
    if p is None:
        return None
    present: dict[str, float | None] = {a.agent: a.p_final for a in ev.agents}
    result = consensus(
        {k: v for k, v in present.items() if k in fin},
        ev.weights,
        stance_long=rules.stance_long,
        stance_short=rules.stance_short,
        quorum=rules.quorum,
        supermajority=supermajority,
        groups=rules.groups,
    )
    decision = manage(
        result.side if result.reached else None,
        p,
        ev.disagreement,
        debate_over=ev.stop_reason in DEBATE_OVER,
        p_long=rules.p_long,
        p_short=rules.p_short,
        d_threshold=rules.d_threshold,
        size_consensus=rules.size_consensus,
        size_fallback=rules.size_fallback,
    )
    return 0.5 if decision.outcome is Outcome.NO_TRADE else p


# ---------------------------------------------------------------------------------------- comparisons


@dataclass(frozen=True)
class Comparison:
    name: str
    component: str | None
    """Set for component tests (the G4 family); None for decision variants (report only)."""
    full: str
    variant: str
    n: int
    loss_full: float | None
    loss_variant: float | None
    test: PairedBootstrap | None
    p_holm: float | None = None
    retained: bool = False

    @property
    def wins(self) -> bool | None:
        """The full system beats the variant (mean difference > 0, Holm-adjusted p < alpha)."""
        if self.test is None or self.p_holm is None:
            return None
        return self.test.mean > 0 and self.p_holm < ALPHA

    def to_json(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "component": self.component,
            "full": self.full,
            "variant": self.variant,
            "n": self.n,
            "loss_full": self.loss_full,
            "loss_variant": self.loss_variant,
            "mean_diff": self.test.mean if self.test else None,
            "ci_low": self.test.ci_low if self.test else None,
            "ci_high": self.test.ci_high if self.test else None,
            "days": self.test.days if self.test else None,
            "p_value": self.test.p_value if self.test else None,
            "p_holm": self.p_holm,
            "retained": self.retained,
            "wins": self.wins,
        }


def _compare(
    name: str,
    component: str | None,
    full_key: str,
    variant_key: str,
    rows: Sequence[tuple[ReplayEvent, Mapping[str, float | None]]],
    *,
    resamples: int,
    seed: int,
    retained: bool,
) -> Comparison:
    diffs: list[float] = []
    days = []
    lf = lv = 0.0
    for ev, probs in rows:
        pf, pv = probs.get(full_key), probs.get(variant_key)
        if pf is None or pv is None:
            continue
        a = log_loss(pf, ev.y, DEFAULT_CLIP)
        b = log_loss(pv, ev.y, DEFAULT_CLIP)
        lf += a
        lv += b
        diffs.append(b - a)
        days.append(ev.as_of.date())
    n = len(diffs)
    test = paired_bootstrap(diffs, days, resamples=resamples, seed=seed) if n else None
    return Comparison(
        name,
        component,
        full_key,
        variant_key,
        n,
        lf / n if n else None,
        lv / n if n else None,
        test,
        retained=retained,
    )


def _with_holm(comparisons: list[Comparison], *, retained_only: bool) -> list[Comparison]:
    """Holm over the tested comparisons (only the retained ones when `retained_only`); the others keep
    `p_holm = None`."""
    tested = [
        i for i, c in enumerate(comparisons) if c.test is not None and (c.retained or not retained_only)
    ]
    adjusted = holm([comparisons[i].test.p_value for i in tested])  # type: ignore[union-attr]
    out = list(comparisons)
    for i, value in zip(tested, adjusted, strict=True):
        c = out[i]
        out[i] = Comparison(
            c.name,
            c.component,
            c.full,
            c.variant,
            c.n,
            c.loss_full,
            c.loss_variant,
            c.test,
            value,
            c.retained,
        )
    return out


@dataclass(frozen=True)
class AblationResult:
    target_type: str
    n_events: int
    replay_mismatches: int
    """Log-pool events whose replayed `full` differs from the stored `p_pooled` by more than 1e-6."""
    components: list[Comparison]
    decisions: list[Comparison]
    forward_branches: tuple[tuple[str, str], ...] = FORWARD_SHADOW_BRANCHES
    notes: list[str] = field(default_factory=list)

    @property
    def retained_components_win(self) -> bool:
        """Every retained component wins and the replay reproduces every stored pool; False when a retained
        component could not be tested or any log-pool event is a replay mismatch."""
        retained = [c for c in self.components if c.retained]
        return self.replay_mismatches == 0 and bool(retained) and all(c.wins is True for c in retained)

    def to_json(self) -> dict[str, Any]:
        return {
            "target_type": self.target_type,
            "n_events": self.n_events,
            "replay_mismatches": self.replay_mismatches,
            "components": [c.to_json() for c in self.components],
            "decisions": [c.to_json() for c in self.decisions],
            "forward_shadow_branches": [
                {"name": name, "status": "forward_shadow_branch", "why": why}
                for name, why in self.forward_branches
            ],
            "retained_components_win": self.retained_components_win,
            "notes": self.notes,
        }


def cut_thresholds(strict_min: Mapping[str, float]) -> dict[str, list[float]]:
    """Score cuts per rule: multiples of the strict threshold."""
    return {rule: [round(value * k, 10) for k in CUT_MULTIPLES] for rule, value in strict_min.items()}


def run_ablation(
    events: Sequence[ReplayEvent],
    rules: CouncilRules,
    *,
    target_type: str,
    strict_min: Mapping[str, float] | None = None,
    resamples: int = BOOTSTRAP_RESAMPLES,
    seed: int = BOOTSTRAP_SEED,
) -> AblationResult:
    """Every numeric ablation of `events` of one target type (others are ignored). Each event is replayed with
    its own pinned council rules (`ReplayEvent.rules`), `rules` only for an event that carries none."""
    mine = sorted(
        (e for e in events if e.target_type == target_type and e.source != "HELD"),
        key=lambda e: (e.as_of, e.event_id),
    )
    rows = [(ev, variant_probabilities(ev, ev.rules or rules)) for ev in mine]
    mismatches = sum(
        1
        for ev, probs in rows
        if ev.method == "log_pool"
        and ev.p_pooled is not None
        and probs["full"] is not None
        and abs(float(probs["full"]) - ev.p_pooled) > 1e-6
    )
    debated = any(ev.rounds > 1 and any(v > 0 for v in ev.r.values()) for ev in mine)
    adjusted = any(any(v > 0 for v in ev.a.values()) for ev in mine)
    opinions = {a.agent for ev in mine for a in ev.agents if a.p_round1 is not None}
    # the roster the window ran with (each event's pinned council plus every agent that gave an opinion),
    # never today's: an agent removed since would otherwise escape its drop test (review M-12)
    roster = (
        tuple(dict.fromkeys([*(a for ev in mine for a in (ev.rules or rules).agents), *sorted(opinions)]))
        if mine
        else tuple(rules.agents)
    )
    kw: dict[str, Any] = {"resamples": resamples, "seed": seed}
    components = [
        _compare("debate", "debate", "full", "round1_pool", rows, retained=debated, **kw),
        _compare("llm_adjustment", "llm_adjustment", "full", "a0", rows, retained=adjusted, **kw),
        *(
            _compare(
                f"agent:{agent}",
                f"agent:{agent}",
                "round1_pool",
                f"drop:{agent}",
                rows,
                retained=agent in opinions,
                **kw,
            )
            for agent in roster
        ),
    ]
    decision_rows: list[tuple[ReplayEvent, Mapping[str, float | None]]] = []
    for ev, probs in rows:
        pinned = ev.rules or rules
        acted = {
            "configured": acted_probability(ev, pinned, supermajority=pinned.supermajority),
            "majority": acted_probability(ev, pinned, supermajority=SIMPLE_MAJORITY),
            "full": probs["full"],
            "r1": probs["r1"],
        }
        base = acted["configured"]
        if base is not None and ev.scan_strict is not None:
            acted["strict_only"] = base if ev.scan_strict else 0.5
        cuts = cut_thresholds(strict_min or {})
        for rule, values in cuts.items():
            for value in values:
                key = f"cut:{rule}>={value:g}"
                if base is None or ev.source != rule or ev.scan_score is None:
                    continue
                acted[key] = base if ev.scan_score >= value else 0.5
        decision_rows.append((ev, acted))
    decision_keys = sorted({k for _, acted in decision_rows for k in acted if k.startswith("cut:")})
    decisions = [
        _compare("consensus_majority", None, "configured", "majority", decision_rows, retained=False, **kw),
        _compare("r_equals_1", None, "full", "r1", decision_rows, retained=False, **kw),
        _compare("strict_only", None, "configured", "strict_only", decision_rows, retained=False, **kw),
        *(
            _compare(key, None, "configured", key, decision_rows, retained=False, **kw)
            for key in decision_keys
        ),
    ]
    notes = []
    if mismatches:
        notes.append(f"{mismatches} log-pool events replay to a different p than stored (check params)")
    return AblationResult(
        target_type=target_type,
        n_events=len(mine),
        replay_mismatches=mismatches,
        components=_with_holm(components, retained_only=True),
        decisions=_with_holm(decisions, retained_only=False),
        notes=notes,
    )
