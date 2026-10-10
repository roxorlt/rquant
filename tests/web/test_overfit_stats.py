"""PSR/DSR wiring on synthetic returns."""

from __future__ import annotations

import numpy as np
import pandas as pd

from rquant.web.overfit_stats import overfit_stats


def _returns(mean: float, seed: int, n: int = 250) -> pd.Series:
    rng = np.random.default_rng(seed)
    return pd.Series(rng.normal(mean, 0.01, n),
                     index=pd.bdate_range("2025-01-01", periods=n))


def test_strong_edge_has_high_psr_and_dsr_falls_with_more_trials() -> None:
    good = _returns(0.003, 1)
    stats = overfit_stats(good, [0.3, 0.0])
    assert stats is not None and stats.psr is not None and stats.psr > 0.99
    many = overfit_stats(good, [0.3, *np.linspace(-0.1, 0.25, 199)])
    assert many is not None and stats.dsr is not None and many.dsr is not None
    assert many.dsr < stats.dsr


def test_noise_is_not_significant_and_short_series_has_no_psr() -> None:
    noise = overfit_stats(_returns(0.0, 2), [])
    assert noise is not None and noise.psr is not None and noise.psr < 0.95
    short = overfit_stats(_returns(0.001, 3, n=10), [])
    assert short is not None and short.psr is None
