"""The account the public dashboard shows: a stale choice never outlives the snapshot that dropped it, and
paper is never shown."""

from __future__ import annotations

import pytest

from hdt.public_dashboard.views import pick_account


@pytest.mark.parametrize(
    ("available", "requested", "shown"),
    [
        (["testnet"], "live", "testnet"),
        (["testnet", "live"], "live", "live"),
        (["live", "testnet"], None, "live"),
        (["live"], "testnet", "live"),
        ([], "live", None),
        (["testnet"], "bogus", "testnet"),
        (["testnet", "live"], "testnet", "testnet"),
        (["paper"], None, None),
        (["paper", "testnet"], "paper", "testnet"),
    ],
)
def test_requested_account_is_kept_only_while_the_snapshot_has_it(
    available: list[str], requested: str | None, shown: str | None
) -> None:
    assert pick_account(available, requested) == shown
