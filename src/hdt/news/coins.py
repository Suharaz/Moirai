"""Mapping news text to coins of the point-in-time universe (self-mapping by symbol + name).

The directory holds the tradable coins of the universe known at `as_of` (`universe/`) with their CMC names
from the latest recorded `listings_latest`. A text maps to a coin when it contains:
- the cashtag `$SYM` or the symbol in parentheses `(SYM)` (any symbol length), or
- the bare symbol as a whole word in its own case (symbols of 3+ characters that are not common English
  words), or
- the coin name as whole words, case-insensitive (names of 4+ characters).
Contract tickers `SYMUSDT`, `SYMUSDC`, `SYMFDUSD` (optionally `1000SYM...` / `1MSYM...`) name SYM.
In pair notation `BASE/QUOTE` (`FOO/ETH`, `FOO/USDT`) only the base leg maps the item to a coin, and even
that is only a market mention: removing a FOO/TUSD spot pair is not news that FOO itself is delisted.
Two rules follow:
- `CoinDirectory.match` (ingestion mapping, soft vetoes) counts base legs;
- `mentions` (shared by `check_official`: an announcement names the coin) ignores pair notation on both
  legs, so a pair-removal notice is never official for a coin, and a DELIST of it is at most soft.
Explicit symbols / CMC ids given by a licensed API map directly.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Final

from hdt.lake.pit_query import PitQuery
from hdt.lake.universe import load_universe
from hdt.memory.dedupe import normalize_text
from hdt.tools.pit import cmc_data

AMBIGUOUS_SYMBOLS: Final[frozenset[str]] = frozenset(
    {
        "ONE",
        "ALL",
        "ANY",
        "ARE",
        "BIG",
        "CAN",
        "FOR",
        "GAS",
        "GET",
        "HOT",
        "KEY",
        "NEW",
        "NOT",
        "NOW",
        "OUT",
        "RAY",
        "SUN",
        "THE",
        "TOP",
        "WIN",
        "YOU",
        "USD",
        "CEO",
        "ETF",
        "SEC",
        "NFT",
        "DEX",
        "CEX",
        "API",
        "AND",
    }
)
LISTINGS_LOOKBACK: Final[timedelta] = timedelta(days=3)
_PAIR_QUOTE: Final = re.compile(r"(?<=[A-Za-z0-9])/[A-Za-z0-9]{2,15}(?![A-Za-z0-9/])")
"""The `/QUOTE` leg of a `BASE/QUOTE` pair (removed before symbols are matched)."""
_PAIR: Final = re.compile(r"(?<![A-Za-z0-9$/])[A-Za-z0-9]{1,15}/[A-Za-z0-9]{2,15}(?![A-Za-z0-9/])")
"""A whole `BASE/QUOTE` pair (both legs: a market, not a coin)."""
CONTRACT_QUOTES: Final[tuple[str, ...]] = ("USDT", "USDC", "FDUSD")
CONTRACT_MULTIPLIERS: Final[tuple[str, ...]] = ("1000", "1M")


def without_quote_legs(text: str) -> str:
    return _PAIR_QUOTE.sub("", text)


def without_pairs(text: str) -> str:
    return _PAIR.sub(" ", text)


def symbol_mentioned(symbol: str, text: str) -> bool:
    """`$SYM`, `(SYM)`, the contract ticker (`SYMUSDT`, `1000SYMUSDT`, ...) or the bare symbol (3+
    characters, same case, not ambiguous) in `text`, where `text` already had its pairs prepared."""
    escaped = re.escape(symbol)
    if re.search(rf"(?:\${escaped}|\({escaped}\))(?![A-Za-z0-9])", text, re.IGNORECASE):
        return True
    multipliers = "|".join(CONTRACT_MULTIPLIERS)
    quotes = "|".join(CONTRACT_QUOTES)
    contract = rf"(?<![A-Za-z0-9$])(?:{multipliers})?{escaped}(?:{quotes})(?![A-Za-z0-9])"
    if re.search(contract, text) is not None:
        return True
    return (
        len(symbol) >= 3
        and symbol.upper() not in AMBIGUOUS_SYMBOLS
        and re.search(rf"(?<![A-Za-z0-9$]){escaped}(?![A-Za-z0-9])", text) is not None
    )


def normalized_name(name: str | None) -> str:
    """The coin name as matched (names shorter than 4 characters never match: empty)."""
    return normalize_text(name) if name and len(name) >= 4 else ""


def mentions(coin: CoinRef, text: str) -> bool:
    """The text names the coin itself, outside any pair notation (see the module rules)."""
    if symbol_mentioned(coin.symbol, without_pairs(text)):
        return True
    name = normalized_name(coin.name)
    return bool(name) and f" {name} " in f" {normalize_text(text)} "


@dataclass(frozen=True)
class CoinRef:
    coin_id: int
    symbol: str
    name: str | None


class CoinDirectory:
    def __init__(self, coins: Iterable[CoinRef]) -> None:
        self.coins: dict[int, CoinRef] = {c.coin_id: c for c in coins}
        self._by_symbol: dict[str, list[int]] = {}
        for coin in self.coins.values():
            self._by_symbol.setdefault(coin.symbol.upper(), []).append(coin.coin_id)
        self._names = {
            coin.coin_id: name for coin in self.coins.values() if (name := normalized_name(coin.name))
        }

    def __len__(self) -> int:
        return len(self.coins)

    def get(self, coin_id: int) -> CoinRef | None:
        return self.coins.get(coin_id)

    def by_symbol(self, symbol: str) -> tuple[int, ...]:
        return tuple(self._by_symbol.get(symbol.strip().lstrip("$").upper(), ()))

    def match(self, text: str) -> tuple[int, ...]:
        """Coins the text maps to: `mentions` plus the base leg of a pair (text prepared once)."""
        bases = without_quote_legs(text)
        found = {coin.coin_id for coin in self.coins.values() if symbol_mentioned(coin.symbol, bases)}
        normalized = f" {normalize_text(text)} "
        found.update(coin_id for coin_id, name in self._names.items() if f" {name} " in normalized)
        return tuple(sorted(found))

    @classmethod
    def from_lake(cls, pit: PitQuery, as_of: datetime) -> CoinDirectory:
        """Universe members known at `as_of` with names from the latest recorded `listings_latest`."""
        universe = load_universe(pit, as_of)
        if universe is None:
            return cls(())
        names = _listing_names(pit, as_of)
        return cls(
            CoinRef(m.cmc_id, m.cmc_symbol, names.get(m.cmc_id))
            for m in universe.members
            if m.cmc_symbol.upper() != "BTC"
        )


def _listing_names(pit: PitQuery, as_of: datetime) -> Mapping[int, str]:
    record = pit.latest("cmc", "listings_latest", as_of, lookback=LISTINGS_LOOKBACK)
    data: Any = cmc_data(record) if record is not None else None
    out: dict[int, str] = {}
    if isinstance(data, list):
        for item in data:
            if (
                isinstance(item, dict)
                and isinstance(item.get("id"), int)
                and isinstance(item.get("name"), str)
            ):
                out[item["id"]] = item["name"]
    return out
