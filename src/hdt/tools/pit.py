"""A lake view that cannot see past a horizon: the capability through which tools read the raw store.

Tools never hold a `PitQuery`; they get a `PitView` built for the event's `as_of`. Every read is bounded
by `fetched_at <= horizon`, so neither an LLM argument nor a tool bug can reach later data. Live and
replay registries read the lake through the same view, so both see the same records for one event.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any, Final

from hdt.core.clock import ensure_utc
from hdt.lake.pit_query import PitQuery
from hdt.lake.schemas import RawRecord
from hdt.lake.universe import Universe, UniverseMember, load_universe
from hdt.tools.base import LookAheadError, NotAvailableError, ToolBackendError

DEFAULT_LOOKBACK: Final[timedelta] = timedelta(hours=6)


class PitView:
    def __init__(self, pit: PitQuery, horizon: datetime) -> None:
        self._pit = pit
        self.horizon = ensure_utc(horizon)

    def at(self, as_of: datetime) -> PitView:
        """A narrower view (an earlier `as_of`); widening is refused."""
        as_of = ensure_utc(as_of)
        if as_of > self.horizon:
            raise LookAheadError(f"{as_of.isoformat()} is after the view horizon {self.horizon.isoformat()}")
        return self if as_of == self.horizon else PitView(self._pit, as_of)

    def latest(
        self, source: str, route: str, *, key: str = "", lookback: timedelta = DEFAULT_LOOKBACK
    ) -> RawRecord | None:
        return self._pit.latest(source, route, self.horizon, key=key, lookback=lookback)

    def series(self, source: str, route: str, start: datetime, *, key: str = "") -> list[RawRecord]:
        """Records with `start <= fetched_at <= horizon`, oldest first."""
        return self._pit.series(source, route, start, self.horizon, as_of=self.horizon, key=key)

    def universe(self) -> Universe | None:
        return load_universe(self._pit, self.horizon)

    def member(self, coin_id: int) -> UniverseMember:
        universe = self.universe()
        if universe is None:
            raise NotAvailableError("no point-in-time universe was built at or before as_of")
        for member in universe.members:
            if member.cmc_id == coin_id:
                return member
        raise NotAvailableError(f"coin {coin_id} is not in the point-in-time universe of {universe.date}")


def record_json(record: RawRecord) -> Any:
    try:
        return json.loads(record.body())
    except ValueError as exc:
        raise ToolBackendError(f"{record.source}/{record.route} body is not JSON") from exc


def record_params(record: RawRecord) -> dict[str, Any]:
    try:
        params = json.loads(record.params_json) if record.params_json else {}
    except ValueError as exc:
        raise ToolBackendError(f"{record.source}/{record.route} params are not JSON") from exc
    return params if isinstance(params, dict) else {}


def cmc_data(record: RawRecord) -> Any | None:
    """The `data` of a successful CMC response (HTTP 200 and `status.error_code` 0), else None."""
    if record.http_status != 200:
        return None
    body = record_json(record)
    if not isinstance(body, dict):
        return None
    status = body.get("status")
    if isinstance(status, dict) and str(status.get("error_code", "0")) != "0":
        return None
    return body.get("data")


def age_seconds(record: RawRecord, as_of: datetime) -> float:
    return round((ensure_utc(as_of) - record.fetched_at).total_seconds(), 3)
