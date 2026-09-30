"""Duplicate detection without embeddings: exact fingerprint + MinHash (datasketch).

Text is normalized (NFKC, case-folded, punctuation to spaces, blanks collapsed) and turned into a set of
word unigrams and bigrams. Two texts are exact duplicates when their normalized forms are equal and near
duplicates when the MinHash estimate of their shingle-set Jaccard similarity is >= the threshold
(0.8 by default, the value phase 07 uses for news). MinHash uses a fixed seed and scheme, so signatures
are identical across processes and replays. Memory indexes are small (known events of a window, one
agent's lessons), so every signature is compared: no LSH false negatives, deterministic ordering.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from itertools import pairwise
from typing import Final, Literal

from datasketch import MinHash

from hdt.core.ids import sha256_hex

NUM_PERM: Final[int] = 128
NEAR_DUPLICATE_JACCARD: Final[float] = 0.8
_SEED: Final[int] = 1
_SCHEME: Final = "affine32"
_NON_WORD: Final = re.compile(r"[^\w]+")


def normalize_text(text: str) -> str:
    folded = unicodedata.normalize("NFKC", text).casefold()
    return " ".join(_NON_WORD.sub(" ", folded).replace("_", " ").split())


def fingerprint(text: str) -> str:
    """sha256 of the normalized text: equal fingerprints are exact duplicates."""
    return sha256_hex(normalize_text(text))


def shingles(text: str) -> frozenset[str]:
    words = normalize_text(text).split()
    return frozenset(words) | frozenset(f"{a} {b}" for a, b in pairwise(words))


def minhash(text: str, num_perm: int = NUM_PERM) -> MinHash:
    signature = MinHash(num_perm=num_perm, seed=_SEED, scheme=_SCHEME)
    for shingle in sorted(shingles(text)):
        signature.update(shingle.encode("utf-8"))
    return signature


@dataclass(frozen=True)
class Match:
    key: str
    kind: Literal["exact", "near"]
    similarity: float


class DuplicateIndex:
    """In-memory index of keyed texts answering "is this text already known?"."""

    def __init__(self, threshold: float = NEAR_DUPLICATE_JACCARD, num_perm: int = NUM_PERM) -> None:
        if not 0.0 < threshold <= 1.0:
            raise ValueError("threshold must be in (0, 1]")
        self.threshold = threshold
        self._num_perm = num_perm
        self._exact: dict[str, str] = {}
        self._signatures: dict[str, MinHash] = {}

    def __len__(self) -> int:
        return len(self._signatures)

    def add(self, key: str, text: str) -> None:
        if key in self._signatures:
            raise ValueError(f"duplicate index key {key!r}")
        if not shingles(text):
            raise ValueError("text has no words to index")
        signature = minhash(text, self._num_perm)
        self._signatures[key] = signature
        self._exact.setdefault(fingerprint(text), key)

    def matches(self, text: str) -> list[Match]:
        """Known texts equal to or near `text`: exact first, then by similarity (desc), then key."""
        if not shingles(text):
            return []
        out: list[Match] = []
        exact_key = self._exact.get(fingerprint(text))
        if exact_key is not None:
            out.append(Match(exact_key, "exact", 1.0))
        signature = minhash(text, self._num_perm)
        near: list[Match] = []
        for key, known in self._signatures.items():
            if key == exact_key:
                continue
            similarity = float(signature.jaccard(known))
            if similarity >= self.threshold:
                near.append(Match(key, "near", similarity))
        near.sort(key=lambda m: (-m.similarity, m.key))
        return out + near

    def first_match(self, text: str) -> Match | None:
        found = self.matches(text)
        return found[0] if found else None
