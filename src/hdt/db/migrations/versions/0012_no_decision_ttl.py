"""No decision TTL (owner decision 2026-09-28): the council outbox forgets `expires_at` and `expired`.

A decision has no time limit any more: the relay publishes every outbox row however late and Risk judges an
OPEN by the current mark, so `decision_outbox.expires_at` and the `expired` status are dropped.

Existing `expired` rows are kept (never deleted) and become `published` with `published_at` and `stream_id`
left NULL. `published` is the only terminal status left and the one the relay never picks up again (it
reads `pending` only), so such a decision is never sent late; `published` without a `stream_id` marks it as
retired before publication (every really published row carries its stream id). The downgrade restores
`expires_at` (as_of + 120 s, the former default TTL) and turns exactly those rows back into `expired`.

Revision ID: 0012_no_decision_ttl
Revises: 0011_golive
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "0012_no_decision_ttl"
down_revision: str | None = "0011_golive"
branch_labels = None
depends_on = None

TS = sa.DateTime(timezone=True)
STATUS_CHECK = "ck_decision_outbox_status"


def upgrade() -> None:
    op.execute(
        "UPDATE decision_outbox SET status = 'published', published_at = NULL, stream_id = NULL "
        "WHERE status = 'expired'"
    )
    op.drop_constraint(op.f(STATUS_CHECK), "decision_outbox", type_="check")
    op.create_check_constraint(op.f(STATUS_CHECK), "decision_outbox", "status IN ('pending', 'published')")
    op.drop_column("decision_outbox", "expires_at")


def downgrade() -> None:
    op.add_column("decision_outbox", sa.Column("expires_at", TS, nullable=True))
    op.execute("UPDATE decision_outbox SET expires_at = as_of + interval '120 seconds'")  # former TTL
    op.alter_column("decision_outbox", "expires_at", nullable=False)
    op.drop_constraint(op.f(STATUS_CHECK), "decision_outbox", type_="check")
    op.create_check_constraint(
        op.f(STATUS_CHECK), "decision_outbox", "status IN ('pending', 'published', 'expired')"
    )
    op.execute(
        "UPDATE decision_outbox SET status = 'expired' "
        "WHERE status = 'published' AND stream_id IS NULL AND published_at IS NULL"
    )
