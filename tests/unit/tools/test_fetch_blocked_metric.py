"""Every fetcher answer refused by the fetch contract is counted in `hdt_fetch_blocked_total{reason}`."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from prometheus_client import REGISTRY

from hdt.core.ids import sha256_hex
from hdt.tools.base import ToolBackendError
from hdt.tools.impl.fetch_source import check_document
from hdt.tools.ports import FetchedDocument, NewsItem

URL = "https://www.coindesk.com/a?id=1"
ITEM = NewsItem(
    item_id="rss:1",
    coin_ids=(1,),
    title="AAA listed",
    url=URL,
    source_name="CoinDesk",
    ingested_at=datetime(2026, 3, 2, 12, tzinfo=UTC),
)
BODY = b"<html>AAA listed</html>"


def _document(**changes: object) -> FetchedDocument:
    values: dict[str, object] = {
        "item_id": ITEM.item_id,
        "requested_url": URL,
        "final_url": URL,
        "redirect_chain": (),
        "http_status": 200,
        "body": BODY,
        "body_sha256": sha256_hex(BODY),
    }
    values.update(changes)
    return FetchedDocument.model_validate(values)


def _blocked(reason: str) -> float:
    return REGISTRY.get_sample_value("hdt_fetch_blocked_total", {"reason": reason}) or 0.0


@pytest.mark.parametrize(
    ("reason", "document"),
    [
        ("wrong_item", _document(item_id="rss:2")),
        ("sha256_mismatch", _document(body_sha256="0" * 64)),
        ("final_url_mismatch", _document(final_url="https://www.coindesk.com/b")),
        (
            "redirect_added_query",
            _document(
                redirect_chain=("https://www.coindesk.com/b?id=1&utm_source=x",),
                final_url="https://www.coindesk.com/b?id=1&utm_source=x",
            ),
        ),
    ],
)
def test_refused_answer_is_counted_by_reason(reason: str, document: FetchedDocument) -> None:
    before = _blocked(reason)
    with pytest.raises(ToolBackendError):
        check_document(ITEM, document)
    assert _blocked(reason) == before + 1


def test_accepted_answer_is_not_counted() -> None:
    before = _blocked("wrong_item")
    check_document(ITEM, _document())
    assert _blocked("wrong_item") == before
