"""Public order fill prices: `hdt_publisher_ro` may join an order to its fills.

`orders.avg_price` is set only when execution reads the order back over REST, so most filled orders carry
none; the public order list shows the volume-weighted price of the order's fills instead. The publisher
gets `orders.client_id` and `fills (account, client_id, price, qty)` for that join only: the client id is
never projected into the snapshot (allowlist, schema and forbidden-name gates).

Revision ID: 0014_public_fill_prices
Revises: 0013_public_orders
"""

from __future__ import annotations

from alembic import op

revision: str = "0014_public_fill_prices"
down_revision: str | None = "0013_public_orders"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("GRANT SELECT (client_id) ON orders TO hdt_publisher_ro")
    op.execute("GRANT SELECT (account, client_id, price, qty) ON fills TO hdt_publisher_ro")


def downgrade() -> None:
    op.execute("REVOKE SELECT (account, client_id, price, qty) ON fills FROM hdt_publisher_ro")
    op.execute("REVOKE SELECT (client_id) ON orders FROM hdt_publisher_ro")
