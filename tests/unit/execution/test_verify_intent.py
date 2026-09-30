"""Ed25519 signing (Risk) and verification (execution) of `OrderIntent`.

Success criterion (verification part): unsigned, wrong signature, or a `paper` intent sent into `live` is
rejected. The intake path (record + Critical alert, replay, own ceiling) is covered by
`tests/integration/test_intent_intake.py`.
"""

from __future__ import annotations

import base64
import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from hdt.contracts.common import Account, Leg, Side
from hdt.contracts.order import OrderIntent
from hdt.execution.verify import Keyring, KeyringError, verify_intent
from hdt.risk.signer import IntentSigner, SigningKeyError, _main, generate
from intent_builders import DRIVER_SIGNER, KEYRING, RISK_ENTRY, RISK_SIGNER, open_plan

NOW = datetime(2026, 9, 28, 12, 0, tzinfo=UTC)


def _entry(account: Account = Account.PAPER, *, sign: bool = True) -> OrderIntent:
    plan = open_plan(
        account=account,
        event_id="evt-sig",
        symbol="SOLUSDT",
        side=Side.LONG,
        qty=Decimal("9"),
        entry=Decimal("100.00"),
        stop=Decimal("98.00"),
        tp1=Decimal("104.00"),
        ioc_price=Decimal("100.10"),
        now=NOW,
        sign=sign,
    )
    return next(i for i in plan if i.leg is Leg.ENTRY)


def test_signed_intent_verifies_for_its_namespace() -> None:
    intent = _entry()
    assert intent.signature
    assert intent.key_id == RISK_SIGNER.key_id
    assert verify_intent(intent, KEYRING, Account.PAPER).ok
    assert verify_intent(_entry(Account.LIVE), KEYRING, Account.LIVE).ok
    assert verify_intent(_entry(Account.TESTNET), KEYRING, Account.TESTNET).ok


def test_unsigned_intent_is_rejected() -> None:
    assert verify_intent(_entry(sign=False), KEYRING, Account.PAPER).reason == "unsigned"


@pytest.mark.parametrize(
    "update",
    [{"qty": Decimal("90")}, {"price": Decimal("99.00")}, {"leverage": 1}, {"symbol": "ETHUSDT"}],
)
def test_any_changed_field_breaks_the_signature(update: dict[str, object]) -> None:
    tampered = _entry().model_copy(update=update)
    assert verify_intent(tampered, KEYRING, Account.PAPER).reason == "bad_signature"


def test_signature_from_another_key_under_a_trusted_key_id_is_rejected() -> None:
    impostor_line, _ = generate(RISK_SIGNER.key_id, [Account.PAPER])
    forged = IntentSigner.from_line(impostor_line).sign(_entry(sign=False))
    assert verify_intent(forged, KEYRING, Account.PAPER).reason == "bad_signature"


def test_garbage_signature_is_rejected() -> None:
    garbage = _entry().model_copy(update={"signature": "not base64 !"})
    assert verify_intent(garbage, KEYRING, Account.PAPER).reason == "bad_signature"


def test_unknown_key_id_is_rejected() -> None:
    other_line, _ = generate("rogue", [Account.PAPER])
    rogue = IntentSigner.from_line(other_line).sign(_entry(sign=False))
    assert verify_intent(rogue, KEYRING, Account.PAPER).reason == "unknown_key"


def test_paper_intent_sent_into_live_is_rejected() -> None:
    paper = _entry(Account.PAPER)
    assert verify_intent(paper, KEYRING, Account.LIVE).reason == "wrong_account"
    relabelled = OrderIntent.model_validate({**paper.model_dump(), "account": Account.LIVE})
    assert verify_intent(relabelled, KEYRING, Account.LIVE).reason == "bad_signature"


def test_driver_key_is_trusted_for_testnet_only() -> None:
    driver_live = DRIVER_SIGNER.sign(_entry(Account.LIVE, sign=False))
    assert verify_intent(driver_live, KEYRING, Account.LIVE).reason == "key_not_allowed_for_account"
    risk_testnet = RISK_SIGNER.sign(_entry(Account.TESTNET, sign=False))
    assert verify_intent(risk_testnet, KEYRING, Account.TESTNET).reason == "key_not_allowed_for_account"


def test_signing_sets_the_key_id_and_covers_it() -> None:
    signed = RISK_SIGNER.sign(_entry(sign=False).model_copy(update={"key_id": "placeholder"}))
    assert signed.key_id == RISK_SIGNER.key_id
    alias = {**RISK_ENTRY, "key_id": "risk-alias"}  # same public key under a second id
    keyring = Keyring.from_json(json.dumps({"keys": [RISK_ENTRY, alias]}))
    assert verify_intent(signed, keyring, Account.PAPER).ok
    swapped = signed.model_copy(update={"key_id": "risk-alias"})
    assert verify_intent(swapped, keyring, Account.PAPER).reason == "bad_signature"


@pytest.mark.parametrize(
    "line",
    ["no-separator", "bad id!:AAAA", "k1:not-base64!!", f"k1:{base64.b64encode(b'short').decode()}"],
)
def test_malformed_signing_key_lines_are_refused(line: str) -> None:
    with pytest.raises(SigningKeyError):
        IntentSigner.from_line(line)


@pytest.mark.parametrize(
    "text",
    [
        "not json",
        json.dumps({"keys": []}),
        json.dumps({"keys": [{"key_id": "k", "public_key": "AAAA", "accounts": ["paper"]}]}),
        json.dumps(
            {"keys": [{"key_id": "k", "public_key": base64.b64encode(bytes(32)).decode(), "accounts": []}]}
        ),
        json.dumps(
            {"keys": [{"key_id": "k", "public_key": base64.b64encode(bytes(32)).decode(), "accounts": ["x"]}]}
        ),
    ],
)
def test_malformed_keyrings_are_refused(text: str) -> None:
    with pytest.raises(KeyringError):
        Keyring.from_json(text)


def test_duplicate_key_ids_are_refused() -> None:
    _, entry = generate("dup", [Account.PAPER])
    with pytest.raises(KeyringError, match="unique"):
        Keyring.from_json(json.dumps({"keys": [entry, entry]}))


def test_generate_cli_prints_a_matching_key_line_and_keyring_entry(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert _main(["generate", "--key-id", "risk-cli", "--accounts", "paper,live"]) == 0
    lines = [ln for ln in capsys.readouterr().out.splitlines() if not ln.startswith("#")]
    signer = IntentSigner.from_line(lines[0])
    keyring = Keyring.from_json(json.dumps({"keys": [json.loads(lines[1])]}))
    assert keyring.keys["risk-cli"].accounts == frozenset({Account.PAPER, Account.LIVE})
    assert verify_intent(signer.sign(_entry(sign=False)), keyring, Account.PAPER).ok
