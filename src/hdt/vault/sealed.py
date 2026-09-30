"""Per-scope sealing with X25519 sealed boxes (PyNaCl `SealedBox` == libsodium `crypto_box_seal`).

The browser seals the secret JSON with the scope PUBLIC key (libsodium.js `crypto_box_seal`), so
config-api only ever stores an opaque blob. The scope owner opens it with the PRIVATE key.

Encodings: keys are standard base64 (32 raw bytes); sealed blobs are accepted in standard or URL-safe
base64, with or without padding (libsodium.js defaults to URL-safe without padding).
`fingerprint` = first 16 hex chars of sha256 over the exact plaintext JSON bytes.
"""

from __future__ import annotations

import base64
import binascii
import re
from typing import Final

from nacl.exceptions import CryptoError
from nacl.public import PrivateKey, PublicKey, SealedBox

from hdt.core.ids import sha256_hex

KEY_BYTES: Final[int] = 32
SEAL_OVERHEAD: Final[int] = 48  # crypto_box_SEALBYTES: ephemeral public key (32) + MAC (16)
MAX_SEALED_BYTES: Final[int] = 16 * 1024
FINGERPRINT_LEN: Final[int] = 16
FINGERPRINT_RE: Final = re.compile(r"^[0-9a-f]{16}$")


class SealedBlobError(ValueError):
    """The sealed blob is malformed or cannot be opened with the given key."""


def b64encode(raw: bytes) -> str:
    return base64.b64encode(raw).decode("ascii")


def b64decode_any(text: str) -> bytes:
    """Decode standard or URL-safe base64, padded or not; raises `SealedBlobError`."""
    cleaned = text.strip().replace("-", "+").replace("_", "/")
    cleaned += "=" * (-len(cleaned) % 4)
    try:
        return base64.b64decode(cleaned, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise SealedBlobError("not valid base64") from exc


def generate_keypair() -> tuple[str, str]:
    """Return (public_b64, private_b64) for a new scope key pair."""
    private = PrivateKey.generate()
    return b64encode(bytes(private.public_key)), b64encode(bytes(private))


def public_key_from_b64(text: str) -> PublicKey:
    raw = b64decode_any(text)
    if len(raw) != KEY_BYTES:
        raise SealedBlobError(f"a public key must be {KEY_BYTES} bytes")
    return PublicKey(raw)


def private_key_from_b64(text: str) -> PrivateKey:
    raw = b64decode_any(text)
    if len(raw) != KEY_BYTES:
        raise SealedBlobError(f"a private key must be {KEY_BYTES} bytes")
    return PrivateKey(raw)


def parse_sealed_blob(text: str) -> bytes:
    """Validate the shape of a submitted blob (never its content, which config-api cannot read)."""
    raw = b64decode_any(text)
    if len(raw) <= SEAL_OVERHEAD:
        raise SealedBlobError("sealed blob is too short to hold a sealed message")
    if len(raw) > MAX_SEALED_BYTES:
        raise SealedBlobError(f"sealed blob exceeds {MAX_SEALED_BYTES} bytes")
    return raw


def seal(public_key: PublicKey, plaintext: bytes) -> bytes:
    """Seal `plaintext` for the holder of the matching private key (same wire format as the browser)."""
    return SealedBox(public_key).encrypt(plaintext)


def unseal(private_key: PrivateKey, sealed: bytes) -> bytes:
    try:
        return SealedBox(private_key).decrypt(sealed)
    except CryptoError as exc:
        raise SealedBlobError("cannot open the sealed blob with this scope key") from exc


def fingerprint(plaintext: bytes) -> str:
    return sha256_hex(plaintext)[:FINGERPRINT_LEN]


def last4(value: str) -> str:
    return value[-4:]
