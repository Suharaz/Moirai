"""Shared feature types and numeric helpers.

A `FeatureBlock` holds scalar feature values and the data-quality flags that touch them. Values are
finite floats, ints, strings, bools or None; a non-finite number becomes None so canonical hashing never
fails. Flags map to the feature names they affect; an empty set means the whole packet.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field

import numpy as np

from hdt.contracts.common import DataQualityFlag
from hdt.contracts.packet import FeatureValue

Flags = dict[DataQualityFlag, set[str]]


@dataclass
class FeatureBlock:
    values: dict[str, FeatureValue] = field(default_factory=dict)
    flags: Flags = field(default_factory=dict)

    def set(self, name: str, value: FeatureValue) -> None:
        self.values[name] = clean(value)

    def update(self, values: Mapping[str, FeatureValue]) -> None:
        for name, value in values.items():
            self.set(name, value)

    def flag(self, flag: DataQualityFlag, *names: str) -> None:
        """Flag `names` (no names: the whole packet)."""
        target = self.flags.setdefault(flag, set())
        target.update(names)
        if not names:
            target.add("")

    def merge(self, other: FeatureBlock) -> FeatureBlock:
        self.update(other.values)
        for flag, names in other.flags.items():
            self.flags.setdefault(flag, set()).update(names)
        return self

    def data_quality(self) -> dict[DataQualityFlag, tuple[str, ...]]:
        """Contract shape: flag -> sorted affected names; a whole-packet flag has an empty tuple."""
        out: dict[DataQualityFlag, tuple[str, ...]] = {}
        for flag in sorted(self.flags, key=lambda f: f.value):
            names = self.flags[flag]
            out[flag] = () if "" in names else tuple(sorted(names))
        return out

    def select(self, names: Iterable[str]) -> FeatureBlock:
        """Sub-block with only `names`; flags keep only the names that remain (packet-wide flags stay)."""
        wanted = set(names)
        block = FeatureBlock({k: v for k, v in self.values.items() if k in wanted})
        for flag, affected in self.flags.items():
            kept = {n for n in affected if n in wanted or n == ""}
            if kept:
                block.flags[flag] = kept
        return block


def clean(value: FeatureValue | np.generic) -> FeatureValue:
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value  # type: ignore[return-value]


def finite(value: float | None) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return float(value)


def ratio(num: float | None, den: float | None) -> float | None:
    if num is None or den is None or den == 0:
        return None
    return finite(num / den)


def change(now: float | None, before: float | None) -> float | None:
    """Relative change `now / before - 1` (None when `before` is missing or not positive)."""
    if now is None or before is None or before <= 0:
        return None
    return finite(now / before - 1.0)


def zscore_last(series: Sequence[float] | np.ndarray, window: int) -> float | None:
    """z of the last value against the trailing `window` values (including it)."""
    arr = np.asarray(series, dtype=float)
    arr = arr[~np.isnan(arr)]
    if arr.size < 3:
        return None
    tail = arr[-window:]
    std = float(np.std(tail, ddof=1)) if tail.size > 1 else 0.0
    if std == 0.0 or not math.isfinite(std):
        return None
    return finite((float(tail[-1]) - float(np.mean(tail))) / std)


def cross_section_z(values: Mapping[int, float | None], min_count: int) -> dict[int, float | None]:
    """Cross-sectional z-score over the non-null members (None everywhere below `min_count` members)."""
    present = {k: v for k, v in values.items() if v is not None and math.isfinite(v)}
    out: dict[int, float | None] = dict.fromkeys(values)
    if len(present) < min_count:
        return out
    keys = sorted(present)
    arr = np.array([present[k] for k in keys], dtype=float)
    std = float(np.std(arr, ddof=1))
    if std == 0.0 or not math.isfinite(std):
        return out
    mean = float(np.mean(arr))
    for key in keys:
        out[key] = finite((present[key] - mean) / std)
    return out


def quantile(values: Sequence[float], q: float) -> float | None:
    arr = np.asarray(values, dtype=float)
    arr = arr[~np.isnan(arr)]
    if arr.size == 0:
        return None
    return float(np.quantile(arr, q, method="linear"))


def last_valid(arr: np.ndarray) -> float | None:
    valid = arr[~np.isnan(arr)]
    return finite(float(valid[-1])) if valid.size else None


def compress(
    name: str,
    series: np.ndarray,
    *,
    atr: float | None,
    close: float | None,
    slope_bars: int,
    z_window: int,
    price_unit: bool,
) -> dict[str, FeatureValue]:
    """`<name>_last`, `<name>_slope_atr`, `<name>_z` for one indicator series.

    The slope is the per-bar change over `slope_bars` divided by ATR. For price-unit indicators the ATR is
    in price units; for unitless indicators (oscillators, ratios) the ATR is expressed as a fraction of
    close (ATR / close), which keeps the slope scale-free across coins.
    """
    valid = series[~np.isnan(series)]
    last = finite(float(valid[-1])) if valid.size else None
    slope: float | None = None
    if valid.size > slope_bars and atr is not None and atr > 0:
        per_bar = (float(valid[-1]) - float(valid[-1 - slope_bars])) / slope_bars
        unit = atr if price_unit else (atr / close if close else None)
        slope = finite(per_bar / unit) if unit else None
    return {
        f"{name}_last": last,
        f"{name}_slope_atr": slope,
        f"{name}_z": zscore_last(valid, z_window) if valid.size else None,
    }
