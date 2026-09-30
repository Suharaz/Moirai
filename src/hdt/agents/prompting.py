"""Prompt assembly for the council agents (phase 05 step 3, Security Considerations).

- The system prompt is fixed text only: `prompts/common.md` plus `prompts/<agent>.md`. Nothing from an
  event, a tool, a news item or another agent is ever placed in it.
- The user message is built by code from labeled sections; every data section is canonical JSON (sorted
  keys, no wall-clock values) under a `(data)` heading, so external text stays inside data fields and the
  same event yields the same bytes on replay (the LLM cache key). Tool text is already sanitized and
  length-bounded by the tool layer.
- `template_hash(agent)` identifies the prompt template (system text, section layout version and the
  output schema): a change creates a new agent version (`hdt.agents.versions`).
- The agent never sees its own weight or `a_i`, never its previous `p_used`; the News agent never sees
  candidates or another agent's `p_model`.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import Any, Final

from hdt.agents.skills.loader import SkillSet
from hdt.contracts.candidate import CandidateSet
from hdt.contracts.common import AgentName
from hdt.contracts.forecast import AgentForecast, AgentForecastDraft, Claim
from hdt.core.ids import canonical_json, canonical_sha256
from hdt.memory.recall import MemoryRecall
from hdt.tools.base import ToolResult
from hdt.tools.ports import NewsAssessment

PROMPTS_ROOT: Final[Path] = Path(__file__).resolve().parent / "prompts"
COMMON_PROMPT: Final[str] = "common.md"
PROMPT_LAYOUT_VERSION: Final[str] = "1"
"""Bump when the code-built user message layout changes (it is part of `template_hash`)."""


@cache
def system_prompt(agent: AgentName) -> str:
    agent = AgentName(agent)
    common = _read(PROMPTS_ROOT / COMMON_PROMPT)
    role = _read(PROMPTS_ROOT / f"{agent.value}.md")
    return f"{common.strip()}\n\n{role.strip()}\n"


@cache
def template_hash(agent: AgentName) -> str:
    return canonical_sha256(
        {
            "layout": PROMPT_LAYOUT_VERSION,
            "system": system_prompt(AgentName(agent)),
            "schema": AgentForecastDraft.model_json_schema(),
        }
    )


def _read(path: Path) -> str:
    return path.read_bytes().decode("utf-8").replace("\r\n", "\n")


def _json(value: Any) -> str:
    return canonical_json(value).decode("utf-8")


def _section(title: str, body: str) -> str:
    return f"## {title}\n{body.strip()}\n"


@dataclass(frozen=True)
class PromptInputs:
    """Everything a prompt may show, gathered by the runner (never the LLM)."""

    agent: AgentName
    event: dict[str, Any]
    skills: SkillSet
    memory: MemoryRecall
    packet_result: ToolResult | None
    news: NewsAssessment | None
    candidate_set: CandidateSet | None
    previous: AgentForecast | None
    shared_claims: tuple[Claim, ...]
    tool_results: Sequence[ToolResult]


def _candidates(candidate_set: CandidateSet | None) -> list[dict[str, Any]]:
    if candidate_set is None:
        return []
    return [
        {
            "candidate_id": c.candidate_id,
            "side": c.side.value,
            "entry": str(c.entry),
            "invalidation": str(c.invalidation),
            "tp1": str(c.tp1),
            "rr": c.rr,
        }
        for c in candidate_set.candidates
    ]


def _previous(previous: AgentForecast) -> dict[str, Any]:
    """Own previous forecast as the agent wrote it (no `p_used`: it would reveal `a_i`)."""
    return {
        "round": previous.round,
        "abstain": previous.abstain,
        "abstain_reason": previous.abstain_reason,
        "p_llm": previous.p_llm,
        "candidate_id": previous.candidate_id,
        "claims": [_claim(c) for c in previous.claims],
        "cited_claim_ids": list(previous.cited_claim_ids),
        "reason": previous.reason,
    }


def _claim(claim: Claim) -> dict[str, Any]:
    return claim.model_dump(mode="json", exclude_none=True)


def context_sections(inputs: PromptInputs) -> list[str]:
    """The data sections shared by the tool steps and the final structured call."""
    sections = [_section("Event (data)", _json(inputs.event))]
    if inputs.agent is AgentName.NEWS:
        if inputs.news is not None:
            sections.append(
                _section("Your news assessment (data)", _json(inputs.news.model_dump(mode="json")))
            )
    else:
        if inputs.packet_result is not None:
            sections.append(
                _section(
                    "Your quant packet, tool result quant_core (data)", inputs.packet_result.prompt_text()
                )
            )
        if inputs.candidate_set is None:
            sections.append(
                _section(
                    "Candidate levels (data)",
                    "None: this event re-evaluates a held position (HOLD or EXIT only); "
                    "candidate_id must be null.",
                )
            )
        else:
            sections.append(_section("Candidate levels (data)", _json(_candidates(inputs.candidate_set))))
    if inputs.skills.skills:
        sections.append(_section("Skills (council procedures)", inputs.skills.prompt_text()))
    sections.append(_section("Your memory (data)", inputs.memory.prompt_text()))
    if inputs.previous is not None:
        sections.append(_section("Your previous round (data)", _json(_previous(inputs.previous))))
    if inputs.shared_claims:
        sections.append(
            _section(
                "Shared claims from the other agents, anonymized (data)",
                _json([_claim(c) for c in inputs.shared_claims]),
            )
        )
    return sections


def _tool_results(results: Iterable[ToolResult]) -> str:
    lines = [result.prompt_text() for result in results]
    return "\n".join(lines) if lines else "none"


def tool_step_user(inputs: PromptInputs, calls_left: int) -> str:
    task = (
        f"You may call your tools to gather evidence before forecasting (at most {calls_left} more calls "
        "for this event; tools read only data recorded at or before as_of). Call a tool only when its "
        "answer can change your forecast. When you have what you need, reply without any tool call."
    )
    return "\n".join(
        [
            *context_sections(inputs),
            _section("Tool results so far (data)", _tool_results(inputs.tool_results)),
            _section("Task", task),
        ]
    )


def final_user(inputs: PromptInputs) -> str:
    round_text = (
        "This is round 1: forecast independently."
        if int(inputs.event["round"]) == 1
        else "This is a revision round: revise your forecast only where the shared claims or new evidence "
        "justify it, and cite the claim ids (yours or shared) your revision relies on."
    )
    task = f"{round_text} Write your forecast now as the JSON object of the schema."
    return "\n".join(
        [
            *context_sections(inputs),
            _section("Tool results (data)", _tool_results(inputs.tool_results)),
            _section("Task", task),
        ]
    )
