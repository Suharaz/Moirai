"""Public risk results: `hdt_publisher_ro` may read what Risk decided for each account of a council event.

A decision card shows, per account, the council intent, the intent Risk applied, its verdict, the reason and
the time, so a LONG that never traded is explained on the card. Column-level SELECT only: never `sizing`,
`checks`, `config_version_ids`, `intent_id`, `entry_client_id`, `stop_client_algo_id` or the signature flag.

Revision ID: 0015_public_risk_results
Revises: 0014_public_fill_prices
"""

from __future__ import annotations

from alembic import op

revision: str = "0015_public_risk_results"
down_revision: str | None = "0014_public_fill_prices"
branch_labels = None
depends_on = None

COLUMNS = "account, event_id, decision_intent, effective_intent, verdict, reason, decided_at"


def upgrade() -> None:
    op.execute(f"GRANT SELECT ({COLUMNS}) ON risk_verdicts TO hdt_publisher_ro")


def downgrade() -> None:
    op.execute(f"REVOKE SELECT ({COLUMNS}) ON risk_verdicts FROM hdt_publisher_ro")  # noqa: S608 - constant names
