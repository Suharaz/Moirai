"""Text helpers of the news pipeline: URL keys, item text, and the code quote check."""

from __future__ import annotations

import re
import unicodedata
from typing import Final
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from hdt.tools.base import sanitize_text
from hdt.tools.impl.fetch_source import document_text

MIN_QUOTE_CHARS: Final[int] = 12
_TRACKING_PREFIXES: Final[tuple[str, ...]] = ("utm_", "mc_", "pk_")
_TRACKING_KEYS: Final[frozenset[str]] = frozenset({"fbclid", "gclid", "igshid", "ref", "ref_src", "cmpid"})
_QUOTE_MAP: Final[dict[int, str]] = {
    ord("\u2018"): "'",
    ord("\u2019"): "'",
    ord("\u201a"): "'",
    ord("\u201b"): "'",
    ord("\u201c"): '"',
    ord("\u201d"): '"',
    ord("\u201e"): '"',
    ord("\u2032"): "'",
    ord("\u2033"): '"',
    ord("\u2010"): "-",
    ord("\u2011"): "-",
    ord("\u2012"): "-",
    ord("\u2013"): "-",
    ord("\u2014"): "-",
    ord("\u2212"): "-",
    ord("\u00a0"): " ",
    ord("\u2026"): "...",
}
_BLANKS: Final = re.compile(r"\s+")
_HTML_HINT: Final = re.compile(r"<[a-zA-Z/!][^>]*>")


def url_key(url: str) -> str:
    """Normalized URL for exact duplicates: lower-case scheme and host, no fragment, no tracking query
    parameters, sorted query, no trailing slash."""
    parts = urlsplit(url.strip())
    host = (parts.hostname or "").rstrip(".").lower()
    if parts.port and parts.port not in (80, 443):
        host = f"{host}:{parts.port}"
    query = sorted(
        (key, value)
        for key, value in parse_qsl(parts.query, keep_blank_values=True)
        if not key.lower().startswith(_TRACKING_PREFIXES) and key.lower() not in _TRACKING_KEYS
    )
    path = parts.path.rstrip("/") or "/"
    return urlunsplit(
        (
            "https" if parts.scheme.lower() in ("http", "https") else parts.scheme,
            host,
            path,
            urlencode(query),
            "",
        )
    )


def plain_text(value: str | None, max_chars: int) -> str | None:
    """Feed text as sanitized plain text (HTML reduced to its visible text); None when empty."""
    if not value:
        return None
    text = document_text(value.encode("utf-8"), "text/html") if _HTML_HINT.search(value) else value
    cleaned = sanitize_text(text, max_chars)
    return cleaned or None


def item_text(title: str, summary: str | None, content: str | None) -> str:
    """The stored copy of an ingested item as the extractor sees it and the quote check reads it."""
    return "\n\n".join(part for part in (title, summary, content) if part)


def normalize_for_quote(text: str) -> str:
    """NFKC, typographic quotes and dashes folded to ASCII, blanks collapsed (case is kept)."""
    folded = unicodedata.normalize("NFKC", text).translate(_QUOTE_MAP)
    return _BLANKS.sub(" ", folded).strip()


def quote_found(quote: str, text: str) -> bool:
    """True when `quote` appears verbatim in `text` (modulo blanks and typographic punctuation).

    Too-short quotes never pass: they would match almost any article.
    """
    needle = normalize_for_quote(quote).strip(" \"'")
    if len(needle) < MIN_QUOTE_CHARS:
        return False
    return needle in normalize_for_quote(text)
