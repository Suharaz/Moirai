"""Handle on the migrated, seeded database of `quant_db` (tests/integration/conftest.py)."""

from __future__ import annotations

from dataclasses import dataclass

import sqlalchemy as sa


@dataclass(frozen=True)
class QuantDb:
    admin: sa.Engine
    council: sa.Engine
    """`hdt_council`: the quant core and scanner writer."""
    risk: sa.Engine
    """`hdt_risk`: reads packets, cannot write them."""
    version_ids: dict[str, int]
    """Pinned config version ids after seeding (version 1 of every section)."""
