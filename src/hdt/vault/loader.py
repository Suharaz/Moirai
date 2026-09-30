"""Read the active version of a scope's secrets inside the owning service.

Only services holding the scope private key can use this. Every decrypted value is registered with
`hdt.vault.redact` before it is returned, and the plaintext fingerprint is checked against the one the
browser recorded, so a swapped or corrupted blob is never used.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

from nacl.public import PrivateKey
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from hdt.db.models.settings import SecretRow
from hdt.vault.redact import register_secret
from hdt.vault.scopes import Scope, load_private_key, secret_spec
from hdt.vault.sealed import SealedBlobError, fingerprint, unseal

log = logging.getLogger(__name__)


class SecretNotAvailableError(LookupError):
    """No active version exists for the requested secret (the owner has not validated one yet)."""


class SecretIntegrityError(RuntimeError):
    """The active blob does not decrypt or does not match its recorded fingerprint."""


@dataclass(frozen=True)
class ActiveSecret:
    scope: str
    name: str
    version: int
    fingerprint: str
    values: dict[str, Any] = field(repr=False)


def decrypt_secret(private_key: PrivateKey, scope: str, name: str, row: SecretRow) -> dict[str, Any]:
    """Open, fingerprint-check and schema-check one secret row; registers values for redaction."""
    spec = secret_spec(scope, name)
    try:
        plaintext = unseal(private_key, row.sealed_blob)
    except SealedBlobError as exc:
        raise SecretIntegrityError(f"{scope}/{name} v{row.version}: {exc}") from None
    if fingerprint(plaintext) != row.fingerprint:
        raise SecretIntegrityError(f"{scope}/{name} v{row.version}: fingerprint mismatch")
    try:
        payload = spec.payload_model.model_validate(json.loads(plaintext))
    except (ValueError, ValidationError):
        raise SecretIntegrityError(
            f"{scope}/{name} v{row.version}: payload does not match its schema"
        ) from None
    values = payload.model_dump()
    for value in values.values():
        if isinstance(value, str):
            register_secret(value)
    return values


class SecretLoader:
    """Caches decrypted secrets per version; `get` re-reads only the active version number."""

    def __init__(self, scope: Scope, session_factory: sessionmaker[Session], private_key: PrivateKey) -> None:
        self.scope = scope
        self._factory = session_factory
        self._key = private_key
        self._cache: dict[str, ActiveSecret] = {}

    @classmethod
    def from_env(cls, scope: Scope, session_factory: sessionmaker[Session]) -> SecretLoader:
        return cls(scope, session_factory, load_private_key(scope))

    def active_version(self, name: str) -> int | None:
        with self._factory() as session:
            return session.scalar(
                select(SecretRow.version).where(
                    SecretRow.scope == self.scope, SecretRow.name == name, SecretRow.status == "active"
                )
            )

    def get(self, name: str) -> ActiveSecret:
        """The active secret `name`; raises `SecretNotAvailableError` or `SecretIntegrityError`."""
        secret_spec(self.scope, name)
        with self._factory() as session:
            row = session.scalar(
                select(SecretRow).where(
                    SecretRow.scope == self.scope, SecretRow.name == name, SecretRow.status == "active"
                )
            )
            if row is None:
                self._cache.pop(name, None)
                raise SecretNotAvailableError(f"no active version of {self.scope}/{name}")
            cached = self._cache.get(name)
            if cached is not None and cached.version == row.version:
                return cached
            values = decrypt_secret(self._key, self.scope, name, row)
            secret = ActiveSecret(self.scope, name, row.version, row.fingerprint, values)
        self._cache[name] = secret
        log.info(
            "secret version loaded",
            extra={"vault_item": f"{self.scope}/{name}", "version": secret.version},
        )
        return secret

    def refresh(self) -> set[str]:
        """Names whose active version changed since they were loaded (call at a safe boundary)."""
        changed = {
            name for name, cached in self._cache.items() if self.active_version(name) != cached.version
        }
        for name in changed:
            self._cache.pop(name, None)
        return changed
