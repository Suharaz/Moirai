"""Public orders (owner decision 2026-09-30): `hdt_publisher_ro` may read the order tables.

The public dashboard now publishes every order the exchange accepted, so the publisher gets column-level
SELECT on `orders` and `algo_orders`: the trading fields only, never `client_id`, `client_algo_id`,
`exchange_order_id`, `algo_id`, `triggered_order_id`, `intent_id`, `link_id` or `last_error`.

Revision ID: 0013_public_orders
Revises: 0012_no_decision_ttl
"""

from __future__ import annotations

from alembic import op

revision: str = "0013_public_orders"
down_revision: str | None = "0012_no_decision_ttl"
branch_labels = None
depends_on = None

ORDER_COLUMNS = (
    "account",
    "event_id",
    "symbol",
    "leg",
    "side",
    "order_type",
    "price",
    "qty",
    "executed_qty",
    "avg_price",
    "reduce_only",
    "status",
    "created_at",
    "updated_at",
)
ALGO_ORDER_COLUMNS = (
    "account",
    "event_id",
    "symbol",
    "leg",
    "side",
    "order_type",
    "trigger_price",
    "qty",
    "reduce_only",
    "status",
    "created_at",
    "updated_at",
)


def upgrade() -> None:
    op.execute(f"GRANT SELECT ({', '.join(ORDER_COLUMNS)}) ON orders TO hdt_publisher_ro")
    op.execute(f"GRANT SELECT ({', '.join(ALGO_ORDER_COLUMNS)}) ON algo_orders TO hdt_publisher_ro")


def downgrade() -> None:
    algo = ", ".join(ALGO_ORDER_COLUMNS)
    orders = ", ".join(ORDER_COLUMNS)
    op.execute(f"REVOKE SELECT ({algo}) ON algo_orders FROM hdt_publisher_ro")  # noqa: S608 - constant names
    op.execute(f"REVOKE SELECT ({orders}) ON orders FROM hdt_publisher_ro")  # noqa: S608 - constant names
