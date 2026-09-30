"""Skill playbooks: versioned Markdown procedures an agent follows for a kind of event.

Layout: `src/hdt/agents/skills/<agent>/README.md` (index, never loaded into a prompt) and
`<agent>/<name>.md`, each opening with YAML front matter:

    ---
    name: verify_exchange_listing        # must equal the file stem
    title: Verify an exchange listing claim
    event_types: [HOLLOW_HYPE, HELD]     # candidate sources (`CandidateSource`) or [ALL]
    ---
    <Markdown body>

`SkillLoader.load(agent, event_type)` returns the agent's skills for that event type in name order and the
`skill_commit`: the git tree object id of the agent's skills directory, computed in-process (git's
`tree`/`blob` object format, text normalized to LF as git stores it), so it equals
`git rev-parse HEAD:src/hdt/agents/skills/<agent>` for the committed version and needs no git binary or
repository at run time. Any edit to any file of the agent's directory changes the commit, which is
written into the decision card with the tool calls. Skill text is data under version control, never
fetched or written at run time.
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Final

import yaml
from pydantic import Field, ValidationError

from hdt.contracts.common import AgentName, CandidateSource, ContractModel

SKILLS_ROOT: Final[Path] = Path(__file__).resolve().parent
ALL_EVENTS: Final[str] = "ALL"
INDEX_FILE: Final[str] = "README.md"
MAX_SKILL_CHARS: Final[int] = 6000
_FRONT_MATTER: Final = re.compile(r"\A---\n(.*?)\n---\n(.*)\Z", re.DOTALL)
_NAME: Final[str] = r"^[a-z0-9_]{1,64}$"
_IGNORED: Final = frozenset({"__pycache__", "__init__.py"})


class SkillError(ValueError):
    """A skill file is malformed (front matter, name, event types or size)."""


class SkillHeader(ContractModel):
    name: str = Field(pattern=_NAME)
    title: str = Field(min_length=1, max_length=120)
    event_types: tuple[str, ...] = Field(min_length=1)


class Skill(ContractModel):
    agent: AgentName
    name: str = Field(pattern=_NAME)
    title: str
    event_types: tuple[str, ...]
    body: str
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    def applies_to(self, event_type: CandidateSource) -> bool:
        return ALL_EVENTS in self.event_types or event_type.value in self.event_types

    def prompt_text(self) -> str:
        return f"## Skill: {self.title} ({self.name})\n\n{self.body.strip()}\n"


class SkillSet(ContractModel):
    agent: AgentName
    event_type: CandidateSource
    skills: tuple[Skill, ...]
    skill_commit: str = Field(pattern=r"^[0-9a-f]{40}$")

    def names(self) -> tuple[str, ...]:
        return tuple(skill.name for skill in self.skills)

    def prompt_text(self) -> str:
        return "\n".join(skill.prompt_text() for skill in self.skills)


def _text(path: Path) -> str:
    return path.read_bytes().decode("utf-8").replace("\r\n", "\n")


def parse_skill(agent: AgentName, path: Path) -> Skill:
    text = _text(path)
    match = _FRONT_MATTER.match(text)
    if match is None:
        raise SkillError(f"{path.name}: missing YAML front matter")
    try:
        raw = yaml.safe_load(match.group(1))
        header = SkillHeader.model_validate(raw)
    except (yaml.YAMLError, ValidationError) as exc:
        raise SkillError(f"{path.name}: invalid front matter: {exc}") from exc
    if header.name != path.stem:
        raise SkillError(f"{path.name}: name {header.name} does not match the file name")
    allowed = {source.value for source in CandidateSource} | {ALL_EVENTS}
    unknown = set(header.event_types) - allowed
    if unknown:
        raise SkillError(f"{path.name}: unknown event types {sorted(unknown)}")
    body = match.group(2).strip()
    if not body:
        raise SkillError(f"{path.name}: empty body")
    if len(body) > MAX_SKILL_CHARS:
        raise SkillError(f"{path.name}: body longer than {MAX_SKILL_CHARS} characters")
    return Skill(
        agent=agent,
        name=header.name,
        title=header.title,
        event_types=tuple(sorted(set(header.event_types))),
        body=body,
        sha256=hashlib.sha256(text.encode("utf-8")).hexdigest(),
    )


def _git_object(kind: str, payload: bytes) -> bytes:
    return hashlib.sha1(f"{kind} {len(payload)}\0".encode("ascii") + payload, usedforsecurity=False).digest()


def git_tree_id(directory: Path) -> str:
    """Git object id of the tree of `directory` (regular files as mode 100644, subdirectories recursed)."""
    return _tree(directory).hex()


def _tree(directory: Path) -> bytes:
    entries: list[tuple[str, bytes]] = []
    for child in directory.iterdir():
        if child.name in _IGNORED or child.name.startswith("."):
            continue
        if child.is_dir():
            # git sorts a tree entry as if its name ended with "/"
            entries.append((child.name + "/", b"40000 " + child.name.encode() + b"\0" + _tree(child)))
        elif child.is_file():
            blob = _git_object("blob", _text(child).encode("utf-8"))
            entries.append((child.name, b"100644 " + child.name.encode() + b"\0" + blob))
    entries.sort(key=lambda entry: entry[0].encode())
    return _git_object("tree", b"".join(payload for _, payload in entries))


class SkillLoader:
    def __init__(self, root: Path = SKILLS_ROOT) -> None:
        self._root = root

    def agent_dir(self, agent: AgentName) -> Path:
        path = self._root / AgentName(agent).value
        if not path.is_dir():
            raise SkillError(f"no skills directory for {AgentName(agent).value}")
        return path

    def all_skills(self, agent: AgentName) -> tuple[Skill, ...]:
        agent = AgentName(agent)
        paths = sorted(p for p in self.agent_dir(agent).glob("*.md") if p.name != INDEX_FILE)
        return tuple(parse_skill(agent, path) for path in paths)

    def commit(self, agent: AgentName) -> str:
        return git_tree_id(self.agent_dir(agent))

    def load(self, agent: AgentName, event_type: CandidateSource) -> SkillSet:
        agent, event_type = AgentName(agent), CandidateSource(event_type)
        return SkillSet(
            agent=agent,
            event_type=event_type,
            skills=tuple(skill for skill in self.all_skills(agent) if skill.applies_to(event_type)),
            skill_commit=self.commit(agent),
        )
