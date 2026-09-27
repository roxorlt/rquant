"""Pure calculation for a pool member's comparable adjusted close return."""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class PoolAdjustedReturn:
    gain_pct: float
    entry_line_price: float | None


def calculate_adjusted_pool_return(
    *,
    entry_close: float | None,
    current_close: float | None,
    entry_factor: float | None,
    current_factor: float | None,
    current_volume: float | None,
) -> PoolAdjustedReturn | None:
    """Return the forward-adjusted change only for complete, tradable day evidence."""

    values = (entry_close, current_close, entry_factor, current_factor, current_volume)
    if any(
        value is None
        or isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value <= 0
        for value in values
    ):
        return None
    assert entry_close is not None
    assert current_close is not None
    assert entry_factor is not None
    assert current_factor is not None
    same_factor = entry_factor == current_factor
    if same_factor:
        gain_pct = (current_close / entry_close - 1.0) * 100.0
    else:
        comparable_entry = entry_close * entry_factor / current_factor
        if not math.isfinite(comparable_entry) or comparable_entry <= 0:
            return None
        gain_pct = (current_close / comparable_entry - 1.0) * 100.0
    if not math.isfinite(gain_pct):
        return None
    return PoolAdjustedReturn(
        gain_pct=gain_pct,
        entry_line_price=entry_close if same_factor else None,
    )
