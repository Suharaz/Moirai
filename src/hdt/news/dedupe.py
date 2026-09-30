"""Duplicate detection of news items at ingestion (no embeddings).

Three layers (design contract section 6):
1. exact: the normalized URL (`url_key`, unique in `news_items`) and the normalized title fingerprint;
2. near: MinHash (datasketch, `hdt.memory.dedupe`) of title + lead text, Jaccard >= 0.8, among recent
   items that share a coin (syndicated and lightly edited copies);
3. event: the extractor's `event_key` (e.g. `binance_spot_listing:XYZ`) through
   `EpisodicWriter.record_known_event` (exact key or MinHash of the title among known events), which
   catches rewritten articles about one event; `collapse_events` keeps one piece of evidence per event.

A duplicate is still stored (with `duplicate_of` pointing to the first copy) so the label set can measure
leaked duplicates, but it is never judged and never listed by `NewsIndex.items`.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from typing import Final, Literal

from hdt.memory.dedupe import NEAR_DUPLICATE_JACCARD, DuplicateIndex, fingerprint, shingles

LEAD_CHARS: Final[int] = 600


@dataclass(frozen=True)
class DuplicateMark:
    duplicate_of: str | None = None
    kind: Literal["exact", "near"] | None = None
    similarity: float | None = None


def dedupe_text(title: str, summary: str | None) -> str:
    return f"{title}\n{(summary or '')[:LEAD_CHARS]}".strip()


@dataclass(frozen=True)
class RecentItem:
    item_id: str
    coin_ids: tuple[int, ...]
    title: str
    summary: str | None
    duplicate_of: str | None


class IngestDeduper:
    """Recent items of the dedupe window; `check` then `add` for every new item, in ingestion order."""

    def __init__(self, recent: Iterable[RecentItem], threshold: float = NEAR_DUPLICATE_JACCARD) -> None:
        self._index = DuplicateIndex(threshold)
        self._titles: dict[str, str] = {}
        self._coins: dict[str, frozenset[int]] = {}
        self._root: dict[str, str] = {}
        for item in recent:
            self.add(item.item_id, item.coin_ids, item.title, item.summary, item.duplicate_of)

    def check(self, coin_ids: Sequence[int], title: str, summary: str | None) -> DuplicateMark:
        coins = frozenset(coin_ids)
        title_fp = fingerprint(title)
        for key, fp in self._titles.items():
            if fp == title_fp and coins & self._coins[key]:
                return DuplicateMark(self._root[key], "exact", 1.0)
        text = dedupe_text(title, summary)
        for match in self._index.matches(text):
            if coins & self._coins[match.key]:
                return DuplicateMark(self._root[match.key], match.kind, round(match.similarity, 4))
        return DuplicateMark()

    def add(
        self,
        item_id: str,
        coin_ids: Sequence[int],
        title: str,
        summary: str | None,
        duplicate_of: str | None = None,
    ) -> None:
        if item_id in self._coins:
            return
        self._coins[item_id] = frozenset(coin_ids)
        self._titles[item_id] = fingerprint(title)
        self._root[item_id] = self._root.get(duplicate_of, duplicate_of) if duplicate_of else item_id
        text = dedupe_text(title, summary)
        if shingles(text):
            self._index.add(item_id, text)


def collapse_events[T](
    entries: Iterable[T], event_key: Callable[[T], str | None], order: Callable[[T], tuple[object, ...]]
) -> list[T]:
    """One entry per event key (the first by `order`); entries without a key are kept as they are."""
    best: dict[str, T] = {}
    loose: list[T] = []
    for entry in entries:
        key = event_key(entry)
        if key is None:
            loose.append(entry)
        elif key not in best or order(entry) < order(best[key]):
            best[key] = entry
    return sorted([*best.values(), *loose], key=order)
