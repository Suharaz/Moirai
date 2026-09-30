"""Minimal S3 client (AWS Signature Version 4, path-style) for the `public-store` MinIO bucket.

Only the calls the public data path needs: put, get, delete and list. Both sides use it: the publisher
with the write-only-to-this-bucket user, the dashboard with the read-only user. The module depends on
httpx and the standard library only (the dashboard must not import database, Redis or vault code).

Layout of the bucket (see `publisher.py`):
- `blobs/<sha256>.json`: one snapshot section, content-addressed, never rewritten;
- `manifests/<YYYYMMDDTHHMMSSZ>-<sha256>.json`: one snapshot version (generated_at + section hashes);
- `latest.json`: pointer to the newest manifest (the only object that is ever overwritten).
"""

from __future__ import annotations

import hashlib
import hmac
import os
import xml.etree.ElementTree as ET
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Final
from urllib.parse import quote, urlsplit

import httpx

ALGORITHM: Final[str] = "AWS4-HMAC-SHA256"
EMPTY_SHA256: Final[str] = hashlib.sha256(b"").hexdigest()
S3_NS: Final[str] = "{http://s3.amazonaws.com/doc/2006-03-01/}"
DEFAULT_REGION: Final[str] = "us-east-1"


class StoreError(RuntimeError):
    """An S3 call failed (transport, HTTP status or malformed response)."""

    def __init__(self, message: str, *, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


class ObjectNotFoundError(StoreError):
    """The requested key does not exist."""


class AccessDeniedError(StoreError):
    """The credentials are not allowed to perform the call (HTTP 403)."""


def _env(name: str) -> str | None:
    """`NAME` or the content of the file named by `NAME_FILE` (docker secrets)."""
    path = os.environ.get(f"{name}_FILE")
    if path:
        return Path(path).read_text(encoding="utf-8").strip()
    value = os.environ.get(name)
    return value.strip() if value else None


@dataclass(frozen=True)
class StoreConfig:
    endpoint: str
    bucket: str
    access_key: str = field(repr=False)
    secret_key: str = field(repr=False)
    region: str = DEFAULT_REGION

    @classmethod
    def from_env(cls, prefix: str = "HDT_PUBLIC_STORE") -> StoreConfig:
        """`<prefix>_URL`, `_BUCKET`, `_ACCESS_KEY`, `_SECRET_KEY` (each may be given as `..._FILE`)."""
        values = {key: _env(f"{prefix}_{key}") for key in ("URL", "BUCKET", "ACCESS_KEY", "SECRET_KEY")}
        missing = sorted(f"{prefix}_{k}" for k, v in values.items() if not v)
        if missing:
            raise StoreError(f"public store is not configured: {', '.join(missing)} missing")
        endpoint = str(values["URL"]).rstrip("/")
        if urlsplit(endpoint).scheme not in ("http", "https"):
            raise StoreError(f"{prefix}_URL must be an http(s) URL")
        return cls(
            endpoint=endpoint,
            bucket=str(values["BUCKET"]),
            access_key=str(values["ACCESS_KEY"]),
            secret_key=str(values["SECRET_KEY"]),
            region=_env(f"{prefix}_REGION") or DEFAULT_REGION,
        )


def _hmac(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def signing_key(secret_key: str, date: str, region: str, service: str = "s3") -> bytes:
    k_date = _hmac(f"AWS4{secret_key}".encode(), date)
    return _hmac(_hmac(_hmac(k_date, region), service), "aws4_request")


def _uri_encode(value: str, *, keep_slash: bool) -> str:
    return quote(value, safe="/-_.~" if keep_slash else "-_.~")


def sign_request(
    *,
    method: str,
    url: str,
    headers: Mapping[str, str],
    payload_sha256: str,
    access_key: str,
    secret_key: str,
    region: str,
    now: datetime,
) -> dict[str, str]:
    """Headers to send (input headers + x-amz-date + x-amz-content-sha256 + Authorization)."""
    parts = urlsplit(url)
    amz_date = now.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
    date = amz_date[:8]
    all_headers = {k.lower(): " ".join(v.strip().split()) for k, v in headers.items()}
    all_headers["host"] = parts.netloc
    all_headers["x-amz-date"] = amz_date
    all_headers["x-amz-content-sha256"] = payload_sha256
    signed = sorted(all_headers)
    query_pairs = []
    if parts.query:
        for item in parts.query.split("&"):
            key, _, value = item.partition("=")
            query_pairs.append((key, value))
    canonical_query = "&".join(f"{k}={v}" for k, v in sorted(query_pairs))
    canonical_request = "\n".join(
        [
            method,
            parts.path or "/",
            canonical_query,
            "".join(f"{name}:{all_headers[name]}\n" for name in signed),
            ";".join(signed),
            payload_sha256,
        ]
    )
    scope = f"{date}/{region}/s3/aws4_request"
    string_to_sign = "\n".join(
        [ALGORITHM, amz_date, scope, hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()]
    )
    signature = hmac.new(
        signing_key(secret_key, date, region), string_to_sign.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    out = {k: v for k, v in all_headers.items() if k != "host"}
    out["authorization"] = (
        f"{ALGORITHM} Credential={access_key}/{scope}, SignedHeaders={';'.join(signed)}, "
        f"Signature={signature}"
    )
    return out


class PublicStore:
    """Synchronous S3 client bound to one bucket (the publisher loop and Streamlit are both sync)."""

    def __init__(
        self, config: StoreConfig, *, http: httpx.Client | None = None, timeout_s: float = 10.0
    ) -> None:
        self._cfg = config
        self._http = http or httpx.Client(timeout=timeout_s, trust_env=False)
        self._owns_http = http is None

    def close(self) -> None:
        if self._owns_http:
            self._http.close()

    def __enter__(self) -> PublicStore:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def _url(self, key: str = "", query: Mapping[str, str] | None = None) -> str:
        path = f"/{_uri_encode(self._cfg.bucket, keep_slash=False)}"
        if key:
            path += "/" + _uri_encode(key, keep_slash=True)
        url = f"{self._cfg.endpoint}{path}"
        if query:
            url += "?" + "&".join(
                f"{_uri_encode(k, keep_slash=False)}={_uri_encode(v, keep_slash=False)}"
                for k, v in sorted(query.items())
            )
        return url

    def _request(
        self,
        method: str,
        key: str = "",
        *,
        body: bytes = b"",
        query: Mapping[str, str] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> httpx.Response:
        url = self._url(key, query)
        payload_sha = hashlib.sha256(body).hexdigest() if body else EMPTY_SHA256
        signed = sign_request(
            method=method,
            url=url,
            headers=headers or {},
            payload_sha256=payload_sha,
            access_key=self._cfg.access_key,
            secret_key=self._cfg.secret_key,
            region=self._cfg.region,
            now=datetime.now(UTC),
        )
        try:
            response = self._http.request(method, url, content=body or None, headers=signed)
        except httpx.HTTPError as exc:
            raise StoreError(f"public store unreachable ({type(exc).__name__})") from None
        if response.status_code == 404:
            raise ObjectNotFoundError(f"{key or self._cfg.bucket} not found", status=404)
        if response.status_code == 403:
            raise AccessDeniedError(f"access denied for {method} {key or self._cfg.bucket}", status=403)
        if response.status_code >= 300:
            raise StoreError(
                f"{method} {key or self._cfg.bucket}: HTTP {response.status_code}",
                status=response.status_code,
            )
        return response

    def put(
        self, key: str, body: bytes, *, content_type: str = "application/json", cache_control: str = ""
    ) -> None:
        headers = {"content-type": content_type}
        if cache_control:
            headers["cache-control"] = cache_control
        self._request("PUT", key, body=body, headers=headers)

    def get(self, key: str) -> bytes:
        return self._request("GET", key).content

    def delete(self, key: str) -> None:
        self._request("DELETE", key)

    def list(self, prefix: str) -> list[str]:
        """Every key under `prefix` (follows continuation tokens)."""
        keys: list[str] = []
        token: str | None = None
        while True:
            query = {"list-type": "2", "prefix": prefix, "max-keys": "1000"}
            if token:
                query["continuation-token"] = token
            response = self._request("GET", query=query)
            try:
                root = ET.fromstring(response.content)  # noqa: S314 - trusted MinIO response
            except ET.ParseError as exc:
                raise StoreError(f"malformed ListObjectsV2 response: {exc}") from None
            keys.extend(el.text or "" for el in root.iter(f"{S3_NS}Key"))
            truncated = (root.findtext(f"{S3_NS}IsTruncated") or "false").lower() == "true"
            token = root.findtext(f"{S3_NS}NextContinuationToken")
            if not truncated or not token:
                return keys
