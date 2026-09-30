"""Only the intended scope owner can open a browser-sealed API credential."""

from __future__ import annotations

import pytest

from hdt.vault.sealed import (
    SealedBlobError,
    generate_keypair,
    private_key_from_b64,
    public_key_from_b64,
    seal,
    unseal,
)


def test_scope_separation_and_blob_roundtrip() -> None:
    exec_public, exec_private = generate_keypair()
    _, llm_private = generate_keypair()
    plaintext = b'{"api_key":"a-secret-that-must-not-leak"}'
    sealed = seal(public_key_from_b64(exec_public), plaintext)
    assert plaintext not in sealed
    assert unseal(private_key_from_b64(exec_private), sealed) == plaintext
    with pytest.raises(SealedBlobError):
        unseal(private_key_from_b64(llm_private), sealed)
