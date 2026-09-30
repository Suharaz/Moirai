"""Redis stream names (the ACL in deploy/redis/users.acl.template grants access per service)."""

from __future__ import annotations

from enum import StrEnum


class Stream(StrEnum):
    CANDIDATES = "candidates"
    RISK_FLAGS = "risk_flags"
    DECISIONS = "decisions"
    ORDERS = "orders"
    ACCOUNT_STATE = "account_state"
    CONFIG_CHANGED = "config_changed"
    SECRET_TEST_REQUEST = "secret_test_request"  # noqa: S105
    SECRET_TEST_RESULT = "secret_test_result"  # noqa: S105
    CONTROLS = "controls"
    DEAD_LETTER = "dead_letter"
