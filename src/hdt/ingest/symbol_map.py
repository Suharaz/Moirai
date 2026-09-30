"""CMC `crypto_id` <-> Binance USDS-M perpetual symbol.

Built from `/v1/cryptocurrency/map` (active coins) and `exchangeInfo` (TRADING perpetuals), plus the manual
override table in `binance.yaml` for contracts quoted per 1000 (or 1M) units (`1000PEPEUSDT` -> PEPE x1000)
and ticker renames. Base-asset symbols are ambiguous on CMC (many coins share a ticker); the lowest
`rank` active coin wins unless an override pins `cmc_id`.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from hdt.contracts.order import SYMBOL_PATTERN
from hdt.core.config import SymbolOverride

_SYMBOL = re.compile(SYMBOL_PATTERN)
_MULTIPLIER_PREFIX = re.compile(r"^(1000000|1000|1M)(?=[A-Z0-9])")


@dataclass(frozen=True)
class Perp:
    symbol: str
    base_asset: str
    quote_asset: str
    onboard_ms: int | None = None


@dataclass(frozen=True)
class SymbolLink:
    cmc_id: int
    cmc_symbol: str
    binance_symbol: str
    multiplier: int
    """Binance contract units per coin unit: price_binance = price_cmc * multiplier."""


@dataclass(frozen=True)
class CmcCoin:
    cmc_id: int
    symbol: str
    rank: int | None
    is_active: bool = True


def parse_exchange_info(data: Any) -> list[Perp]:
    """TRADING USDT-margined perpetuals from `GET /fapi/v1/exchangeInfo`.

    Symbols outside `SYMBOL_PATTERN` (e.g. non-ASCII tickers) are left out: an order cannot carry them,
    so they can never be traded and would only break lake keys and stream names.
    """
    perps: list[Perp] = []
    for item in data.get("symbols", []) if isinstance(data, Mapping) else []:
        if (
            item.get("contractType") == "PERPETUAL"
            and item.get("status") == "TRADING"
            and item.get("quoteAsset") == "USDT"
            and _SYMBOL.fullmatch(str(item.get("symbol", "")))
        ):
            onboard = item.get("onboardDate")
            perps.append(
                Perp(
                    symbol=str(item["symbol"]),
                    base_asset=str(item["baseAsset"]),
                    quote_asset=str(item["quoteAsset"]),
                    onboard_ms=onboard if isinstance(onboard, int) else None,
                )
            )
    return perps


def parse_cmc_map(data: Any) -> list[CmcCoin]:
    coins: list[CmcCoin] = []
    for item in data if isinstance(data, list) else []:
        rank = item.get("rank")
        coins.append(
            CmcCoin(
                cmc_id=int(item["id"]),
                symbol=str(item["symbol"]).upper(),
                rank=rank if isinstance(rank, int) else None,
                is_active=item.get("is_active", 1) == 1,
            )
        )
    return coins


def _split_multiplier(base_asset: str) -> tuple[str, int]:
    match = _MULTIPLIER_PREFIX.match(base_asset)
    if match is None:
        return base_asset, 1
    prefix = match.group(1)
    multiplier = 1_000_000 if prefix in ("1000000", "1M") else 1000
    return base_asset[len(prefix) :], multiplier


class SymbolMap:
    def __init__(self, links: Iterable[SymbolLink]) -> None:
        self._by_id: dict[int, SymbolLink] = {}
        self._by_symbol: dict[str, SymbolLink] = {}
        for link in links:
            self._by_id.setdefault(link.cmc_id, link)
            self._by_symbol[link.binance_symbol] = link

    @classmethod
    def build(
        cls,
        cmc_coins: Iterable[CmcCoin],
        perps: Iterable[Perp],
        overrides: Iterable[SymbolOverride] = (),
    ) -> SymbolMap:
        by_ticker: dict[str, CmcCoin] = {}
        by_id: dict[int, CmcCoin] = {}
        for coin in cmc_coins:
            if not coin.is_active:
                continue
            by_id[coin.cmc_id] = coin
            best = by_ticker.get(coin.symbol)
            if best is None or _rank_key(coin) < _rank_key(best):
                by_ticker[coin.symbol] = coin
        pinned = {o.binance_symbol: o for o in overrides}
        links: list[SymbolLink] = []
        for perp in perps:
            override = pinned.get(perp.symbol)
            if override is not None:
                pinned_coin = (
                    by_id.get(override.cmc_id)
                    if override.cmc_id
                    else by_ticker.get(override.cmc_symbol.upper())
                )
                if pinned_coin is not None:
                    links.append(
                        SymbolLink(pinned_coin.cmc_id, pinned_coin.symbol, perp.symbol, override.multiplier)
                    )
                continue
            ticker, multiplier = _split_multiplier(perp.base_asset)
            matched = by_ticker.get(ticker)
            if matched is not None:
                links.append(SymbolLink(matched.cmc_id, matched.symbol, perp.symbol, multiplier))
        # A coin with two contracts (e.g. a ticker rename) resolves by_cmc_id to the override-pinned one.
        links.sort(key=lambda link: (link.cmc_id, link.binance_symbol not in pinned))
        return cls(links)

    def by_cmc_id(self, cmc_id: int) -> SymbolLink | None:
        return self._by_id.get(cmc_id)

    def by_binance_symbol(self, symbol: str) -> SymbolLink | None:
        return self._by_symbol.get(symbol)

    def links(self) -> list[SymbolLink]:
        return sorted(self._by_symbol.values(), key=lambda link: link.binance_symbol)

    def __len__(self) -> int:
        return len(self._by_symbol)


def _rank_key(coin: CmcCoin) -> tuple[int, int]:
    return (coin.rank if coin.rank is not None else 1 << 30, coin.cmc_id)
