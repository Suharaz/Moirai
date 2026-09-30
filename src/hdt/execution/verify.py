"""Signature verification of `OrderIntent` (execution side; only public keys live here).

An intent is accepted only when: it carries a signature, its `key_id` is in the keyring, that key is allowed
for the intent's `account` (the Risk key signs paper, testnet and live; the testnet driver key is granted
`testnet` only), and the Ed25519 signature is valid over `OrderIntent.signing_bytes()`. Replay protection
(`processed_intents`) is the intake's job.
"""

from __future__ import annotations

import base64
import binascii
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Final

from nacl.exceptions import BadSignatureError
from nacl.signing import VerifyKey

from hdt.contracts.common import Account
from hdt.contracts.order import OrderIntent
from hdt.core.config import read_env_value

VERIFY_KEYS_ENV: Final[str] = "HDT_ORDER_VERIFY_KEYS"


class KeyringError(ValueError):
    """The verify keyring is missing or malformed."""


@dataclass(frozen=True)
class TrustedKey:
    key_id: str
    accounts: frozenset[Account]
    verify_key: VerifyKey = field(repr=False)


@dataclass(frozen=True)
class Keyring:
    keys: Mapping[str, TrustedKey]

    @classmethod
    def from_json(cls, text: str) -> Keyring:
        try:
            data: Any = json.loads(text)
        except json.JSONDecodeError as exc:
            raise KeyringError(f"verify keyring is not JSON: {exc.msg}") from None
        items = data.get("keys") if isinstance(data, dict) else None
        if not isinstance(items, list) or not items:
            raise KeyringError("verify keyring needs a non-empty 'keys' list")
        keys: dict[str, TrustedKey] = {}
        for item in items:
            if not isinstance(item, dict):
                raise KeyringError("keyring entries must be objects")
            key_id, public, accounts = item.get("key_id"), item.get("public_key"), item.get("accounts")
            if not isinstance(key_id, str) or not key_id or key_id in keys:
                raise KeyringError("keyring entries need a unique non-empty key_id")
            try:
                raw = base64.b64decode(str(public), validate=True)
                allowed = (
                    frozenset(Account(a) for a in accounts) if isinstance(accounts, list) else frozenset()
                )
            except (binascii.Error, ValueError):
                raise KeyringError(f"keyring entry {key_id}: bad public_key or accounts") from None
            if len(raw) != 32 or not allowed:
                raise KeyringError(
                    f"keyring entry {key_id}: public_key must be 32 bytes and accounts non-empty"
                )
            keys[key_id] = TrustedKey(key_id, allowed, VerifyKey(raw))
        return cls(keys)

    @classmethod
    def from_env(cls, name: str = VERIFY_KEYS_ENV) -> Keyring:
        value = read_env_value(name)
        if value is None:
            raise KeyringError(f"{name} (or {name}_FILE) is not set")
        return cls.from_json(value)


@dataclass(frozen=True)
class Verification:
    ok: bool
    reason: str | None = None


def verify_intent(intent: OrderIntent, keyring: Keyring, account: Account) -> Verification:
    """Checks for the namespace `account`; a refusal's reason code goes to the intake's audit entry."""
    if intent.account is not account:
        return Verification(False, "wrong_account")
    if not intent.signature:
        return Verification(False, "unsigned")
    trusted = keyring.keys.get(intent.key_id)
    if trusted is None:
        return Verification(False, "unknown_key")
    if account not in trusted.accounts:
        return Verification(False, "key_not_allowed_for_account")
    try:
        signature = base64.b64decode(intent.signature, validate=True)
        trusted.verify_key.verify(intent.signing_bytes(), signature)
    except (binascii.Error, ValueError, BadSignatureError):
        return Verification(False, "bad_signature")
    return Verification(True)
