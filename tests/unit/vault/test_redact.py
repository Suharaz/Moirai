from __future__ import annotations

import io
import json
import logging

import pytest

from hdt.contracts import Account, Leg, OrderIntent, OrderSide, OrderType, TimeInForce
from hdt.core.logging import configure_logging
from hdt.vault.redact import MASK, redact, redact_value, register_secret

SHA = "a" * 64


@pytest.mark.parametrize(
    ("text", "secret"),
    [
        (
            "GET /fapi/v1/order?symbol=X&timestamp=1&signature=9f86d081884c7d659a2feaa0c55ad015",
            "9f86d081884c",
        ),
        (
            "headers={'X-MBX-APIKEY': 'vmPUZE6mv9SD5VNHk4HlWFsOr6aKE2zvsw0MuIgwCIPy6utIco14y7Ju91duEh8A'}",
            "vmPUZE6mv9",
        ),
        ("X-CMC_PRO_API_KEY: b54bcf4d-1bca-4e8e-9a24-22ff2c3d462c", "b54bcf4d-1bca"),
        ('{"listenKey": "pqia91ma19a5s61cv6a81va65sdf19v8a65a1a5s61cv6a81va65sdf19v8a65a1"}', "pqia91ma19"),
        ("wss://fstream.binance.com/private/ws/pqia91ma19a5s61cv6a81va65sdf19v8a65a1", "pqia91ma19"),
        ("https://api.telegram.org/bot123456789:AAHdqTcvCH1vGWJxfSeofSAs0K5PALDsawQ/getMe", "AAHdqTcvCH1"),
        ("Authorization: Bearer sk-or-v1-0123456789abcdef0123456789abcdef", "sk-or-v1-0123"),
        (
            '{"signature": "3f1c2b0a9d8e7f6a5b4c3d2e1f0a9b8c7d6e5f4a3b2c1d0e9f8a7b6c5d4e3f2a1"}',
            "3f1c2b0a9d8e",
        ),
        ("api_key=abcdef123456&x=1", "abcdef123456"),
        ('{"sealed_blob": "c2VhbGVkYmxvYmNvbnRlbnQ="}', "c2VhbGVkYmxvYmNvbnRlbnQ"),
        ("CMC_API_KEY=3f8e2a91-77aa-4c1b-9e0f-12ab34cd56ef", "3f8e2a91-77aa"),
        ("BINANCE_SECRET=Vq93kLm2Pz81Rt6Yw0Xe", "Vq93kLm2Pz81"),
        ("HDT_PG_DSN=postgresql://hdt_risk:SuperSecretPw123@127.0.0.1:5432/hdt", "SuperSecretPw123"),
        ("connect redis://council:Rp0-9aZ_x~Kq@127.0.0.1:6379/0 failed", "Rp0-9aZ_x~Kq"),
        ('{"cmc_api_key": "3f8e2a91-77aa-4c1b-9e0f-12ab34cd56ef"}', "3f8e2a91-77aa"),
        ("signature: 3q2+7w3q2+7w3q2+7w==", "3q2+7w3q2+7w"),
        ("session_secret: Hh7Jk9Lm1Nn3Pp5Qq7", "Hh7Jk9Lm1Nn3"),
        ("HDT_PG_PASSWORD_RISK='Wd8Ks2Lp0Qm4'", "Wd8Ks2Lp0Qm4"),
        ('{"password": "ab\\"cdEFgh123"}', "cdEFgh123"),
    ],
)
def test_known_secret_shapes_are_redacted(text: str, secret: str) -> None:
    out = redact(text)
    assert secret not in out
    assert MASK in out


def test_hashes_and_ids_stay_readable() -> None:
    line = f"packet_sha256={SHA} event_id=evt_1 prompt_tokens=120 completion_tokens=40"
    assert redact(line) == line


def test_registered_runtime_secret_is_redacted_everywhere() -> None:
    register_secret("Zq8LmN3pRt7Vw2Xy")
    assert "Zq8LmN3pRt7Vw2Xy" not in redact("value Zq8LmN3pRt7Vw2Xy leaked in a message")


@pytest.mark.parametrize("key", ["cmc_api_key", "binance_secret", "HDT_PG_PASSWORD_RISK", "signature", "dsn"])
def test_structured_values_under_prefixed_secret_names_are_masked(key: str) -> None:
    assert redact_value(key, "Kq93mZ81Lp") == MASK


def test_order_intent_signature_never_reaches_a_log_line() -> None:
    intent = OrderIntent(
        intent_id="i1",
        account=Account.PAPER,
        event_id="evt1",
        leg=Leg.ENTRY,
        seq=0,
        symbol="SOLUSDT",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        qty="1.5",
        price="100",
        reduce_only=False,
        tif=TimeInForce.GTX,
        client_id="ABCDEFGHIJKLMNOPQRST-entry-0",
        created_at="2026-09-27T10:00:00Z",
        key_id="risk-2026-09",
        signature="q83vEjRWeJq83vEjRWeJq83vEjRWeJq83vEjRWeJq83vEjRWeJq83vEjRWeJq83vEjRWeJq83vEjRWeJq8==",
    )
    assert "q83vEjRWeJ" not in str(redact_value("intent", intent))
    assert "q83vEjRWeJ" not in redact(intent.model_dump_json())


def test_logging_pipeline_redacts_args_extras_and_exceptions() -> None:
    stream = io.StringIO()
    configure_logging("test", stream=stream)
    logger = logging.getLogger("hdt.test.redact")
    try:
        raise RuntimeError("failed with signature=deadbeefdeadbeefdeadbeef")
    except RuntimeError:
        logger.exception("request %s", "listenKey=abcdefghijklmnopqrstuv", extra={"api_key": "abc123secret"})
    record = json.loads(stream.getvalue().strip().splitlines()[-1])
    blob = json.dumps(record)
    assert "abcdefghijklmnopqrstuv" not in blob
    assert "deadbeefdeadbeef" not in blob
    assert record["service"] == "test"
