"""SigV4 signing (AWS published test vector) and the S3 client's error mapping and paging."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest

from hdt.public.store import (
    EMPTY_SHA256,
    AccessDeniedError,
    ObjectNotFoundError,
    PublicStore,
    StoreConfig,
    StoreError,
    sign_request,
)


def test_sigv4_matches_aws_get_object_example() -> None:
    # docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html, "GET Object" example.
    headers = sign_request(
        method="GET",
        url="https://examplebucket.s3.amazonaws.com/test.txt",
        headers={"Range": "bytes=0-9"},
        payload_sha256=EMPTY_SHA256,
        access_key="AKIAIOSFODNN7EXAMPLE",
        secret_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        region="us-east-1",
        now=datetime(2013, 5, 24, tzinfo=UTC),
    )
    assert headers["authorization"] == (
        "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, "
        "SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, "
        "Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
    )
    assert "host" not in headers


def _store(handler: httpx.MockTransport) -> PublicStore:
    config = StoreConfig(
        endpoint="http://public-store:9000", bucket="hdt-public", access_key="a", secret_key="s"
    )
    return PublicStore(config, http=httpx.Client(transport=handler))


def test_put_signs_the_body_hash() -> None:
    seen: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200)

    with _store(httpx.MockTransport(handle)) as store:
        store.put("blobs/abc.json", b'{"a":1}', cache_control="public, max-age=31536000, immutable")
    (request,) = seen
    assert request.url.path == "/hdt-public/blobs/abc.json"
    assert request.headers["x-amz-content-sha256"] == hashlib.sha256(b'{"a":1}').hexdigest()
    assert "cache-control" in request.headers["authorization"]


@pytest.mark.parametrize(
    ("status", "error"),
    [(404, ObjectNotFoundError), (403, AccessDeniedError), (500, StoreError)],
)
def test_http_errors_map_to_store_errors(status: int, error: type[StoreError]) -> None:
    with _store(httpx.MockTransport(lambda _r: httpx.Response(status))) as store, pytest.raises(error):
        store.get("latest.json")


def test_unreachable_store_raises_store_error() -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    with _store(httpx.MockTransport(handle)) as store, pytest.raises(StoreError, match="unreachable"):
        store.get("latest.json")


def test_list_follows_continuation_tokens() -> None:
    ns = "http://s3.amazonaws.com/doc/2006-03-01/"
    pages = {
        None: f'<ListBucketResult xmlns="{ns}"><IsTruncated>true</IsTruncated><Key>blobs/a.json</Key>'
        "<NextContinuationToken>t1</NextContinuationToken></ListBucketResult>",
        "t1": f'<ListBucketResult xmlns="{ns}"><IsTruncated>false</IsTruncated><Key>blobs/b.json</Key>'
        "</ListBucketResult>",
    }

    def handle(request: httpx.Request) -> httpx.Response:
        query = parse_qs(urlsplit(str(request.url)).query)
        assert query["prefix"] == ["blobs/"]
        token = query.get("continuation-token", [None])[0]
        return httpx.Response(200, content=pages[token].encode())

    with _store(httpx.MockTransport(handle)) as store:
        assert store.list("blobs/") == ["blobs/a.json", "blobs/b.json"]


def test_config_reads_file_secrets_and_reports_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    secret = tmp_path / "secret"
    secret.write_text("s3cr3t\n", encoding="utf-8")
    monkeypatch.setenv("HDT_PUBLIC_STORE_URL", "http://public-store:9000")
    monkeypatch.setenv("HDT_PUBLIC_STORE_BUCKET", "hdt-public")
    monkeypatch.setenv("HDT_PUBLIC_STORE_ACCESS_KEY", "reader")
    monkeypatch.setenv("HDT_PUBLIC_STORE_SECRET_KEY_FILE", str(secret))
    config = StoreConfig.from_env()
    assert config.secret_key == "s3cr3t"
    assert "s3cr3t" not in repr(config)
    monkeypatch.delenv("HDT_PUBLIC_STORE_BUCKET")
    with pytest.raises(StoreError, match="HDT_PUBLIC_STORE_BUCKET"):
        StoreConfig.from_env()
