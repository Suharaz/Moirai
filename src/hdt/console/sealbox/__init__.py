"""Sealbox: custom Streamlit component that seals API keys in the browser.

The frontend (`frontend/`, plain JS, vendored libsodium.js, no CDN) renders write-only inputs, seals the
plaintext with the scope PUBLIC key (libsodium `crypto_box_seal`, PyNaCl `SealedBox` compatible) and hands
back exactly `{sealed_blob, last4, fingerprint}`. `parse_sealed` refuses any other shape, so a plaintext
field can never reach Python by accident. The console forwards the three values to config-api, which
stores a `pending` row; only the owning service can decrypt and activate it.
"""

from __future__ import annotations

import base64
import binascii
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal

import streamlit.components.v1 as components
from pydantic import BaseModel, ConfigDict, ValidationError, field_validator

FRONTEND_DIR: Final[Path] = Path(__file__).resolve().parent / "frontend"
SEALED_KEYS: Final[frozenset[str]] = frozenset({"sealed_blob", "last4", "fingerprint"})
# crypto_box_seal output = 32-byte ephemeral public key + 16-byte MAC + ciphertext.
SEAL_OVERHEAD: Final[int] = 48
PUBLIC_KEY_BYTES: Final[int] = 32
MIN_SECRET_LENGTH: Final[int] = 12

_component = components.declare_component("hdt_sealbox", path=str(FRONTEND_DIR))


class SealboxPayloadError(ValueError):
    """The component returned something other than the three sealed values."""


class SealedSecret(BaseModel):
    """The only data the browser returns for a secret. No plaintext field exists on this model."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    sealed_blob: str
    last4: str
    fingerprint: str

    @field_validator("sealed_blob")
    @classmethod
    def _blob(cls, value: str) -> str:
        try:
            raw = base64.b64decode(value, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ValueError("sealed_blob must be standard base64") from exc
        if len(raw) <= SEAL_OVERHEAD:
            raise ValueError("sealed_blob is too short to be a sealed box")
        return value

    @field_validator("last4")
    @classmethod
    def _last4(cls, value: str) -> str:
        if len(value) != 4 or not value.isprintable():
            raise ValueError("last4 must be exactly 4 printable characters")
        return value

    @field_validator("fingerprint")
    @classmethod
    def _fingerprint(cls, value: str) -> str:
        if not re.fullmatch(r"[0-9a-f]{16}", value):
            raise ValueError("fingerprint must be 16 lowercase hex characters")
        return value


def parse_sealed(raw: object) -> SealedSecret:
    """Validate the component value: a mapping with exactly the three sealed keys."""
    if not isinstance(raw, Mapping):
        raise SealboxPayloadError("sealbox returned a non-object value")
    keys = set(raw.keys())
    if keys != SEALED_KEYS:
        raise SealboxPayloadError(f"sealbox returned unexpected keys: {sorted(keys ^ SEALED_KEYS)}")
    try:
        return SealedSecret.model_validate(dict(raw))
    except ValidationError as exc:
        raise SealboxPayloadError(f"sealbox returned an invalid value: {exc.error_count()} error(s)") from exc


def validate_public_key(public_key_b64: str) -> str:
    """The scope public key must be standard base64 of 32 bytes (X25519)."""
    try:
        raw = base64.b64decode(public_key_b64, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("scope public key is not base64") from exc
    if len(raw) != PUBLIC_KEY_BYTES:
        raise ValueError("scope public key must be 32 bytes")
    return public_key_b64


FieldKind = Literal["secret", "user_ids"]


@dataclass(frozen=True)
class SecretField:
    name: str
    label: str
    kind: FieldKind = "secret"
    help: str = ""


@dataclass(frozen=True)
class SecretSpec:
    """One application secret: scope, name, the sealed JSON fields and how the API keys page shows it."""

    scope: Literal["data", "llm", "exec", "ops"]
    name: str
    title: str
    logo: str
    color: Literal["cmc", "bnb", "llm", "text-2"]
    fields: tuple[SecretField, ...]
    primary: str
    owner_check: str


SECRET_SPECS: Final[tuple[SecretSpec, ...]] = (
    SecretSpec(
        scope="data",
        name="cmc_api_key",
        title="CoinMarketCap",
        logo="CMC",
        color="cmc",
        fields=(SecretField("api_key", "API key"),),
        primary="api_key",
        owner_check="The recorder calls /v1/key/info (0 credits): plan, monthly credits and usage.",
    ),
    SecretSpec(
        scope="llm",
        name="openrouter_api_key",
        title="OpenRouter",
        logo="OR",
        color="llm",
        fields=(SecretField("api_key", "API key"),),
        primary="api_key",
        owner_check="The council calls GET /api/v1/key and reads limit_remaining.",
    ),
    SecretSpec(
        scope="llm",
        name="deepseek_api_key",
        title="DeepSeek",
        logo="DS",
        color="llm",
        fields=(SecretField("api_key", "API key"),),
        primary="api_key",
        owner_check="The council calls GET /user/balance and reads is_available and the balance.",
    ),
    SecretSpec(
        scope="exec",
        name="binance_testnet",
        title="Binance testnet",
        logo="BN",
        color="bnb",
        fields=(SecretField("api_key", "API key"), SecretField("api_secret", "Secret")),
        primary="api_key",
        owner_check="Execution calls signed GET /fapi/v3/account on demo-fapi.",
    ),
    SecretSpec(
        scope="exec",
        name="binance_live",
        title="Binance live",
        logo="BN",
        color="bnb",
        fields=(SecretField("api_key", "API key"), SecretField("api_secret", "Secret")),
        primary="api_key",
        owner_check=(
            "Execution checks the key permissions (withdrawals off, IP whitelist on, futures on, "
            "no transfers) and calls signed GET /fapi/v3/account; activation is refused on any violation."
        ),
    ),
    SecretSpec(
        scope="ops",
        name="telegram",
        title="Telegram bot",
        logo="TG",
        color="text-2",
        fields=(
            SecretField("bot_token", "Bot token"),
            SecretField(
                "allowed_user_ids",
                "Allowed Telegram user ids",
                kind="user_ids",
                help="from.id values allowed to use the bot, separated by commas.",
            ),
        ),
        primary="bot_token",
        owner_check="The telegram bot calls getMe and getWebhookInfo.",
    ),
    SecretSpec(
        scope="data",
        name="news_feed",
        title="Full-text news feed",
        logo="NW",
        color="text-2",
        fields=(SecretField("api_key", "API key"),),
        primary="api_key",
        owner_check="The news worker checks the key once the feed is chosen (phase 07).",
    ),
)


def spec_for(scope: str, name: str) -> SecretSpec:
    for spec in SECRET_SPECS:
        if spec.scope == scope and spec.name == name:
            return spec
    raise KeyError(f"{scope}/{name}")


def sealbox(
    spec: SecretSpec,
    public_key_b64: str,
    *,
    key: str,
    tokens: Mapping[str, str],
    nonce: int = 0,
    locked: bool = False,
) -> SealedSecret | None:
    """Render the sealing form; returns the sealed values after the user clicks "Seal in browser".

    `nonce` rebuilds the form (fresh empty inputs) after a save; `locked` disables sealing.
    """
    raw = _component(
        scope=spec.scope,
        name=spec.name,
        public_key=validate_public_key(public_key_b64),
        fields=[{"name": f.name, "label": f.label, "kind": f.kind, "help": f.help} for f in spec.fields],
        primary=spec.primary,
        tokens=dict(tokens),
        nonce=nonce,
        locked=locked,
        key=key,
        default=None,
    )
    if raw is None:
        return None
    return parse_sealed(raw)
