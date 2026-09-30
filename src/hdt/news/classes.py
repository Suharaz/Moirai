"""News event classes shared by the judges, the mode rules, the veto scan, the tables and the labels.

- Registered hard classes (`listing`, `delist`, `exploit`, `unlock`): the only classes a `url` claim can be
  `hard` for (council hard-evidence policy branch (a)) and the only ones `check_official` verifies.
- Hollow classes (`RUMOR`, `KOL_MEME`, `CIRCULAR`): attention without a verifiable primary fact (Fade hype
  / Fade panic candidates).
- `DENIAL`: a statement refuting an earlier claim (refuting evidence only when independent of the
  project, see `hdt.news.modes`).
"""

from __future__ import annotations

from typing import Final, Literal

EventClass = Literal[
    "LISTING",
    "DELIST",
    "EXPLOIT",
    "UNLOCK",
    "PARTNERSHIP",
    "PRODUCT",
    "REGULATORY",
    "OTHER_FACT",
    "RUMOR",
    "KOL_MEME",
    "CIRCULAR",
    "DENIAL",
    "NO_EVENT",
]
EVENT_CLASSES: Final[tuple[str, ...]] = (
    "LISTING",
    "DELIST",
    "EXPLOIT",
    "UNLOCK",
    "PARTNERSHIP",
    "PRODUCT",
    "REGULATORY",
    "OTHER_FACT",
    "RUMOR",
    "KOL_MEME",
    "CIRCULAR",
    "DENIAL",
    "NO_EVENT",
)
REGISTERED_CLASSES: Final[dict[str, str]] = {
    "LISTING": "listing",
    "DELIST": "delist",
    "EXPLOIT": "exploit",
    "UNLOCK": "unlock",
}
"""Judge class -> registered event class of the council hard-evidence policy."""
VETO_CLASSES: Final[frozenset[str]] = frozenset({"EXPLOIT", "DELIST", "UNLOCK"})
"""Bad catalysts the veto scan acts on (with direction down)."""
DISAGREEMENT_VETO_CLASSES: Final[frozenset[str]] = frozenset({"EXPLOIT", "DELIST"})
"""A judge disagreement where either judge says one of these is a soft veto, never a silent abstain."""
