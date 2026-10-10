from __future__ import annotations

import pandas as pd
import pytest

from rquant.backtest.band import bootstrap_band


def test_constant_returns_give_a_degenerate_band() -> None:
    band = bootstrap_band(pd.Series([0.01] * 20), days=3)
    assert [round(b.p50, 6) for b in band] == [0.01, 0.0201, 0.030301]
    assert band[-1].p5 == pytest.approx(band[-1].p95)


def test_band_widens_with_horizon_and_is_reproducible() -> None:
    returns = pd.Series([0.02, -0.01, 0.005, -0.02, 0.01, 0.0] * 5)
    band = bootstrap_band(returns, days=20, seed=1)
    assert band[0].p95 - band[0].p5 < band[-1].p95 - band[-1].p5
    assert band == bootstrap_band(returns, days=20, seed=1)
    assert bootstrap_band(pd.Series([0.01]), days=5) == []
