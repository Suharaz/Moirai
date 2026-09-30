"""Ed25519 signing of `OrderIntent` (PyNaCl). The private key is a startup secret mounted only into `risk`
(and a separate one into the testnet driver).

Key material formats:
- signing key (`HDT_ORDER_SIGNING_KEY[_FILE]`, `HDT_TESTNET_DRIVER_SIGNING_KEY[_FILE]`): one line
  `<key_id>:<base64 32-byte seed>`;
- verify keyring (`HDT_ORDER_VERIFY_KEYS[_FILE]`, execution only): JSON
  `{"keys": [{"key_id": ..., "public_key": <base64>, "accounts": ["paper", "testnet", "live"]}, ...]}`.
The signature is base64 over `OrderIntent.signing_bytes()` (canonical JSON of every field except
`signature`), so `account`, `key_id` and every price are covered.

`python -m hdt.risk.signer generate --key-id risk-2026a --accounts paper,testnet,live` prints a new signing
key line and the matching keyring entry.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import json
import re
import sys
from dataclasses import dataclass, field
from typing import Final

from nacl.signing import SigningKey

from hdt.contracts.common import Account
from hdt.contracts.order import OrderIntent
from hdt.core.config import read_env_value

RISK_KEY_ENV: Final[str] = "HDT_ORDER_SIGNING_KEY"
DRIVER_KEY_ENV: Final[str] = "HDT_TESTNET_DRIVER_SIGNING_KEY"
_KEY_ID: Final = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")


class SigningKeyError(ValueError):
    """The signing key material is missing or malformed."""


@dataclass(frozen=True)
class IntentSigner:
    key_id: str
    _key: SigningKey = field(repr=False)

    @classmethod
    def from_line(cls, line: str) -> IntentSigner:
        key_id, sep, seed_b64 = line.strip().partition(":")
        if not sep or not _KEY_ID.fullmatch(key_id):
            raise SigningKeyError("signing key must be '<key_id>:<base64 seed>'")
        try:
            seed = base64.b64decode(seed_b64, validate=True)
        except (binascii.Error, ValueError):
            raise SigningKeyError("signing key seed is not valid base64") from None
        if len(seed) != 32:
            raise SigningKeyError("signing key seed must be 32 bytes")
        return cls(key_id, SigningKey(seed))

    @classmethod
    def from_env(cls, name: str = RISK_KEY_ENV) -> IntentSigner:
        value = read_env_value(name)
        if value is None:
            raise SigningKeyError(f"{name} (or {name}_FILE) is not set")
        return cls.from_line(value)

    @property
    def public_key_b64(self) -> str:
        return base64.b64encode(bytes(self._key.verify_key)).decode("ascii")

    def sign(self, intent: OrderIntent) -> OrderIntent:
        """A copy of `intent` with this signer's `key_id` and the Ed25519 signature filled in."""
        unsigned = intent.model_copy(update={"key_id": self.key_id, "signature": ""})
        unsigned = OrderIntent.model_validate(unsigned.model_dump())
        signature = self._key.sign(unsigned.signing_bytes()).signature
        return unsigned.model_copy(update={"signature": base64.b64encode(signature).decode("ascii")})


def generate(key_id: str, accounts: list[Account]) -> tuple[str, dict[str, object]]:
    if not _KEY_ID.fullmatch(key_id):
        raise SigningKeyError("key_id must match [A-Za-z0-9_.-]{1,64}")
    key = SigningKey.generate()
    line = f"{key_id}:{base64.b64encode(bytes(key)).decode('ascii')}"
    entry: dict[str, object] = {
        "key_id": key_id,
        "public_key": base64.b64encode(bytes(key.verify_key)).decode("ascii"),
        "accounts": [a.value for a in accounts],
    }
    return line, entry


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m hdt.risk.signer")
    sub = parser.add_subparsers(dest="command", required=True)
    gen = sub.add_parser("generate", help="create a new Ed25519 OrderIntent signing key")
    gen.add_argument("--key-id", required=True)
    gen.add_argument("--accounts", required=True, help="comma-separated: paper,live or testnet")
    args = parser.parse_args(argv)
    accounts = [Account(a.strip()) for a in args.accounts.split(",") if a.strip()]
    if not accounts:
        parser.error("--accounts must name at least one account")
    line, entry = generate(args.key_id, accounts)
    sys.stdout.write("# signing key file (mount only into the signing service):\n")
    sys.stdout.write(line + "\n")
    sys.stdout.write("# keyring entry for HDT_ORDER_VERIFY_KEYS (execution):\n")
    sys.stdout.write(json.dumps(entry) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
