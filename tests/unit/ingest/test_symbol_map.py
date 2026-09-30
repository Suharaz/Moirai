"""CMC <-> Binance symbol mapping: 1000x contracts, overrides, ticker collisions, renames."""

from __future__ import annotations

from hdt.core.config import SymbolOverride
from hdt.ingest.symbol_map import CmcCoin, Perp, SymbolMap, parse_exchange_info

COINS = [
    CmcCoin(1, "BTC", 1),
    CmcCoin(24478, "PEPE", 30),
    CmcCoin(99999, "PEPE", 4000),  # a copycat sharing the ticker
    CmcCoin(28321, "POL", 25),
    CmcCoin(3890, "MATIC", None, is_active=False),
    CmcCoin(10804, "FLOKI", 60),
    CmcCoin(21685, "BABYDOGE", 200),
]


def _perp(symbol: str, base: str) -> Perp:
    return Perp(symbol=symbol, base_asset=base, quote_asset="USDT")


def test_plain_and_1000x_contracts_resolve_to_the_best_ranked_coin() -> None:
    m = SymbolMap.build(COINS, [_perp("BTCUSDT", "BTC"), _perp("1000PEPEUSDT", "1000PEPE")])
    assert m.by_binance_symbol("BTCUSDT").cmc_id == 1  # type: ignore[union-attr]
    pepe = m.by_cmc_id(24478)
    assert pepe is not None
    assert pepe.binance_symbol == "1000PEPEUSDT"
    assert pepe.multiplier == 1000
    assert m.by_cmc_id(99999) is None


def test_override_pins_multiplier_and_rename() -> None:
    overrides = [
        SymbolOverride(binance_symbol="1MBABYDOGEUSDT", cmc_symbol="BABYDOGE", multiplier=1_000_000),
        SymbolOverride(binance_symbol="MATICUSDT", cmc_symbol="POL", multiplier=1),
    ]
    m = SymbolMap.build(
        COINS, [_perp("1MBABYDOGEUSDT", "1MBABYDOGE"), _perp("MATICUSDT", "MATIC")], overrides
    )
    baby = m.by_binance_symbol("1MBABYDOGEUSDT")
    assert baby is not None
    assert baby.cmc_id == 21685
    assert baby.multiplier == 1_000_000
    renamed = m.by_binance_symbol("MATICUSDT")
    assert renamed is not None
    assert renamed.cmc_id == 28321  # inactive MATIC never matches


def test_unmatched_perps_are_dropped_and_exchange_info_filters_non_trading() -> None:
    info = {
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "baseAsset": "BTC",
                "quoteAsset": "USDT",
                "contractType": "PERPETUAL",
                "status": "TRADING",
            },
            {
                "symbol": "BTCUSDT_261225",
                "baseAsset": "BTC",
                "quoteAsset": "USDT",
                "contractType": "CURRENT_QUARTER",
                "status": "TRADING",
            },
            {
                "symbol": "OLDUSDT",
                "baseAsset": "OLD",
                "quoteAsset": "USDT",
                "contractType": "PERPETUAL",
                "status": "SETTLING",
            },
            {
                "symbol": "ZZZUSDT",
                "baseAsset": "ZZZ",
                "quoteAsset": "USDT",
                "contractType": "PERPETUAL",
                "status": "TRADING",
            },
        ]
    }
    perps = parse_exchange_info(info)
    assert [p.symbol for p in perps] == ["BTCUSDT", "ZZZUSDT"]
    assert len(SymbolMap.build(COINS, perps)) == 1


def test_untradable_non_ascii_perps_stay_out_of_the_universe_inputs() -> None:
    info = {
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "baseAsset": "BTC",
                "quoteAsset": "USDT",
                "contractType": "PERPETUAL",
                "status": "TRADING",
            },
            {
                "symbol": "币安人生USDT",
                "baseAsset": "币安人生",
                "quoteAsset": "USDT",
                "contractType": "PERPETUAL",
                "status": "TRADING",
            },
        ]
    }
    assert [p.symbol for p in parse_exchange_info(info)] == ["BTCUSDT"]
