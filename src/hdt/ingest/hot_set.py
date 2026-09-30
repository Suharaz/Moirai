"""Hot set: symbols that additionally get 100 ms depth diffs (Design Contract section 6, order book).

A symbol is hot while it has a candidate from the last `candidate_window` (stream `candidates`) or an open
position in any account namespace (latest `account_state` per account). Both streams are read without a
consumer group (`XREVRANGE`), so the recorder never takes messages away from their real consumers.
Candidates carry CMC coin ids; the current universe maps them to Binance symbols.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any, Final

from redis.asyncio import Redis

from hdt.contracts.streams import Stream
from hdt.core.clock import utcnow
from hdt.lake.universe import Universe

log = logging.getLogger(__name__)

ACCOUNT_STATE_SCAN: Final[int] = 50
CANDIDATE_SCAN: Final[int] = 5000


def _text(value: bytes | str) -> str:
    return value.decode() if isinstance(value, bytes) else value


def _payload(fields: dict[Any, Any]) -> dict[str, Any] | None:
    raw = {(_text(k)): _text(v) for k, v in fields.items()}.get("data")
    if raw is None:
        return None
    try:
        parsed = json.loads(raw)
    except ValueError:
        return None
    return parsed if isinstance(parsed, dict) else None


def held_symbols(entries: Any) -> set[str]:
    """Symbols with a non-zero position in the newest `AccountState` of each account (newest first)."""
    seen: set[str] = set()
    held: set[str] = set()
    for _msg_id, fields in entries or []:
        state = _payload(fields)
        if state is None or state.get("account") in seen:
            continue
        seen.add(str(state.get("account")))
        for position in state.get("positions") or []:
            try:
                if float(position.get("qty", 0)) != 0:
                    held.add(str(position["symbol"]))
            except (TypeError, ValueError, KeyError):
                continue
    return held


def candidate_coins(entries: Any) -> set[int]:
    coins: set[int] = set()
    for _msg_id, fields in entries or []:
        candidate = _payload(fields)
        if candidate is not None and isinstance(candidate.get("coin_id"), int):
            coins.add(candidate["coin_id"])
    return coins


class HotSet:
    def __init__(
        self,
        redis: Redis,
        universe: Callable[[], Universe | None],
        *,
        candidate_window: timedelta = timedelta(hours=24),
        clock: Callable[[], datetime] = utcnow,
    ) -> None:
        self._redis = redis
        self._universe = universe
        self._window = candidate_window
        self._clock = clock

    async def symbols(self) -> set[str]:
        since_ms = int((self._clock() - self._window).timestamp() * 1000)
        candidates = await self._redis.xrevrange(
            str(Stream.CANDIDATES), max="+", min=f"{since_ms}-0", count=CANDIDATE_SCAN
        )
        states = await self._redis.xrevrange(
            str(Stream.ACCOUNT_STATE), max="+", min="-", count=ACCOUNT_STATE_SCAN
        )
        hot = held_symbols(states)
        universe = self._universe()
        if universe is not None:
            by_coin = {m.cmc_id: m.binance_symbol for m in universe.members}
            hot |= {by_coin[c] for c in candidate_coins(candidates) if c in by_coin}
        return hot

    async def held_cmc_ids(self) -> list[int]:
        """CMC ids of held coins (for the optional CMC WebSocket price cross-check)."""
        states = await self._redis.xrevrange(
            str(Stream.ACCOUNT_STATE), max="+", min="-", count=ACCOUNT_STATE_SCAN
        )
        held = held_symbols(states)
        universe = self._universe()
        if universe is None:
            return []
        return sorted(m.cmc_id for m in universe.members if m.binance_symbol in held)
