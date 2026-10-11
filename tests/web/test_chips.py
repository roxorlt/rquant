from datetime import date, timedelta

import numpy as np
import pytest

from rquant.web.chips import _tri, chip_series, volume_profile


def test_triangle_mass_sums_to_one() -> None:
    edges = np.linspace(0, 10, 51)
    assert _tri(2, 8, 5, edges).sum() == pytest.approx(1.0)
    assert _tri(3, 3, 3, edges).sum() == pytest.approx(1.0)


def test_full_turnover_replaces_old_chips_and_profit_ratio() -> None:
    edges = np.linspace(0, 20, 101)
    d0 = date(2026, 1, 1)
    bars = [{"date": d0, "high": 6, "low": 4, "close": 5, "turnover": 1},
            {"date": d0 + timedelta(1), "high": 16, "low": 14, "close": 15, "turnover": 100}]
    out = chip_series(bars, 2, edges)
    assert out[0].avg_cost == pytest.approx(5, abs=0.2)
    assert out[1].avg_cost == pytest.approx(15, abs=0.2)  # fully turned over
    assert out[1].profit_ratio == pytest.approx(0.5, abs=0.1)


def test_volume_profile_poc_and_value_area() -> None:
    edges = np.linspace(0, 10, 101)
    vp = volume_profile([(6, 4, 5, 100), (9, 8, 8.5, 10)], edges, "daily", 2)
    assert vp is not None and vp.poc == pytest.approx(5, abs=0.2)
    assert vp.val < 5 < vp.vah < 8
