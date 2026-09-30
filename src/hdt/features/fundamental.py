"""Fundamental features (Fundamental agent) and the shared CMC quote parser.

- From route #8 `quotes_latest` (all universe members, one call): price, volume, market cap, supply,
  `date_added`, tags (the common `categories` key: sorted, comma-joined tag slugs);
- OI / market cap and OI / 24h volume, with OI = the coin's USD open interest over the route #2 exchanges;
- supply: circulating / total and circulating / max;
- route #15 `listings_new`: the coin is among the newest listings;
- DEX routes #24-27 (keyed by CMC id, fetched on events): pool liquidity (USD), top-10 holder share
  (balance / total supply) and contract security flags. The DEX envelope is unverified, so both a bare
  array and a `{data, status}` envelope are accepted.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from hdt.contracts.common import DataQualityFlag
from hdt.features.common import FeatureBlock, ratio
from hdt.features.lake_io import CMC, LakeView, as_float, parse_ts, usd_quote
from hdt.lake.schemas import RawRecord

QUOTES_ROUTE = "quotes_latest"
NEW_LISTINGS_ROUTE = "listings_new"
DEX_POOLS_ROUTE = "dex_token_pools"
DEX_HOLDERS_ROUTE = "dex_holders"
DEX_SECURITY_ROUTE = "dex_security_detail"
TOP_HOLDERS = 10


@dataclass(frozen=True)
class CmcQuote:
    coin_id: int
    price: float | None
    volume_24h: float | None
    market_cap: float | None
    circulating_supply: float | None
    total_supply: float | None
    max_supply: float | None
    date_added: datetime | None
    categories: str | None
    last_updated: datetime | None


def parse_quotes(data: Any) -> dict[int, CmcQuote]:
    """`data` is an array of items (v3) or a map keyed by id (v2)."""
    items: list[Any] = (
        list(data.values()) if isinstance(data, Mapping) else data if isinstance(data, list) else []
    )
    out: dict[int, CmcQuote] = {}
    for item in items:
        if isinstance(item, list):  # v2 symbol maps hold arrays
            item = item[0] if item else None
        if not isinstance(item, Mapping) or not isinstance(item.get("id"), int):
            continue
        usd = usd_quote(item)
        out[item["id"]] = CmcQuote(
            coin_id=item["id"],
            price=as_float(usd.get("price")) if usd else None,
            volume_24h=as_float(usd.get("volume_24h")) if usd else None,
            market_cap=as_float(usd.get("market_cap")) if usd else None,
            circulating_supply=as_float(item.get("circulating_supply")),
            total_supply=as_float(item.get("total_supply")),
            max_supply=as_float(item.get("max_supply")),
            date_added=parse_ts(item.get("date_added")),
            categories=_categories(item.get("tags")),
            last_updated=parse_ts((usd or {}).get("last_updated") or item.get("last_updated")),
        )
    return out


def _categories(tags: Any) -> str | None:
    if not isinstance(tags, list):
        return None
    slugs = set()
    for tag in tags:
        if isinstance(tag, str) and tag:
            slugs.add(tag)
        elif isinstance(tag, Mapping) and isinstance(tag.get("slug"), str) and tag["slug"]:
            slugs.add(tag["slug"])
    return ",".join(sorted(slugs)) if slugs else None


def read_quotes(
    view: LakeView, as_of: datetime, lookback: timedelta
) -> tuple[RawRecord | None, dict[int, CmcQuote]]:
    record, data = view.latest_cmc_ok(QUOTES_ROUTE, as_of, lookback=lookback)
    return record, parse_quotes(data) if data is not None else {}


def new_listing_ids(view: LakeView, as_of: datetime) -> set[int] | None:
    _, data = view.latest_cmc_ok(NEW_LISTINGS_ROUTE, as_of, lookback=timedelta(hours=3))
    if not isinstance(data, list):
        return None
    return {item["id"] for item in data if isinstance(item, Mapping) and isinstance(item.get("id"), int)}


# --------------------------------------------------------------------------- DEX


def dex_payload(view: LakeView, route: str, coin_id: int, as_of: datetime, lookback: timedelta) -> Any:
    """Newest successful DEX body for the coin (bare array or the `data` of an envelope)."""
    record = view.latest(CMC, route, as_of, key=str(coin_id), lookback=lookback)
    if record is None or record.http_status != 200:
        return None
    body = view.body_json(record)
    if isinstance(body, Mapping) and "data" in body:
        status = body.get("status")
        code = status.get("error_code") if isinstance(status, Mapping) else 0
        return body["data"] if code in (0, "0", None) else None
    return body


def dex_features(view: LakeView, coin_id: int, as_of: datetime) -> FeatureBlock:
    block = FeatureBlock()
    lookback = timedelta(days=7)
    pools = dex_payload(view, DEX_POOLS_ROUTE, coin_id, as_of, lookback)
    liq = (
        [as_float(p.get("liqUsd")) for p in pools if isinstance(p, Mapping)]
        if isinstance(pools, list)
        else None
    )
    block.set("dex_liquidity_usd", sum(x for x in liq if x is not None) if liq else None)
    holders = dex_payload(view, DEX_HOLDERS_ROUTE, coin_id, as_of, lookback)
    rows = holders.get("holders") if isinstance(holders, Mapping) else holders
    block.set("holder_top10_share", _top_share(rows) if isinstance(rows, list) else None)
    security = dex_payload(view, DEX_SECURITY_ROUTE, coin_id, as_of, lookback)
    entry = security[0] if isinstance(security, list) and security else security
    block.update(_security(entry if isinstance(entry, Mapping) else None))
    missing = [name for name, value in block.values.items() if value is None]
    if missing:
        block.flag(DataQualityFlag.MISSING_ROUTE, *missing)
    return block


def _top_share(rows: list[Any]) -> float | None:
    parsed = []
    total = None
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        balance = as_float(row.get("balance"))
        total = total or as_float(row.get("totalSupply"))
        if balance is not None:
            parsed.append(balance)
    if not parsed or not total:
        return None
    return ratio(sum(sorted(parsed, reverse=True)[:TOP_HOLDERS]), total)


_SECURITY_NAMES = ("sec_vendor_flagged", "sec_risk_hits", "sec_buy_tax", "sec_sell_tax", "sec_honeypot")


def _security(entry: Mapping[str, Any] | None) -> dict[str, float | int | bool | None]:
    if entry is None:
        return dict.fromkeys(_SECURITY_NAMES)
    extra = entry.get("extra") if isinstance(entry.get("extra"), Mapping) else {}
    raw_items = entry.get("securityItems")
    items: list[Any] = raw_items if isinstance(raw_items, list) else []
    hits = len([item for item in items if isinstance(item, Mapping) and item.get("isHit") is True])
    display = (
        entry.get("evmDisplay")
        if isinstance(entry.get("evmDisplay"), Mapping)
        else entry.get("solanaDisplay")
    )
    honeypot = display.get("honeypotStatus") if isinstance(display, Mapping) else None
    flagged = extra.get("isFlaggedByVendor") if isinstance(extra, Mapping) else None
    return {
        "sec_vendor_flagged": flagged if isinstance(flagged, bool) else None,
        "sec_risk_hits": hits,
        "sec_buy_tax": as_float(extra.get("buyTax")) if isinstance(extra, Mapping) else None,
        "sec_sell_tax": as_float(extra.get("sellTax")) if isinstance(extra, Mapping) else None,
        "sec_honeypot": as_float(honeypot),
    }


def fundamental_features(
    quote: CmcQuote | None, oi_agg_usd: float | None, new_ids: set[int] | None, coin_id: int, as_of: datetime
) -> FeatureBlock:
    block = FeatureBlock()
    names = (
        "oi_mcap",
        "oi_volume_turnover",
        "circ_total_supply",
        "circ_max_supply",
        "listing_age_days",
        "is_new_listing",
    )
    if quote is None:
        block.update(dict.fromkeys(names))
        block.flag(DataQualityFlag.MISSING_ROUTE, *names)
        return block
    block.set("oi_mcap", ratio(oi_agg_usd, quote.market_cap))
    block.set("oi_volume_turnover", ratio(oi_agg_usd, quote.volume_24h))
    block.set("circ_total_supply", ratio(quote.circulating_supply, quote.total_supply))
    block.set("circ_max_supply", ratio(quote.circulating_supply, quote.max_supply))
    age = (as_of - quote.date_added).total_seconds() / 86400.0 if quote.date_added else None
    block.set("listing_age_days", age)
    block.set("is_new_listing", coin_id in new_ids if new_ids is not None else None)
    missing = [name for name in names if block.values.get(name) is None and name != "circ_max_supply"]
    if missing:
        block.flag(DataQualityFlag.MISSING_ROUTE, *missing)
    return block
