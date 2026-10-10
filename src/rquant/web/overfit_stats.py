"""PSR / MinTRL / DSR for one daily return series (roadmap module 5).

DSR treats every stored portfolio run on the same preset as one trial of the
family. That is an honest lower bound on the number of trials actually tried,
which the page states; there is no experiment registry gate here.
"""

from __future__ import annotations

from collections.abc import Sequence

import pandas as pd
from pydantic import BaseModel

from rquant.overfit import (
    DeflatedSharpeInput,
    SinglePeriodSharpeInput,
    deflated_sharpe_ratio_per_period,
    minimum_track_record_length_per_period,
    probabilistic_sharpe_ratio_per_period,
)


class OverfitStats(BaseModel):
    observations: int
    sharpe_per_day: float
    psr: float | None
    min_track_record_days: int | None
    dsr: float | None
    trials: int


def sharpe_per_period(returns: pd.Series) -> float | None:
    std = float(returns.std(ddof=1)) if len(returns) > 1 else 0.0
    return None if std <= 0 else float(returns.mean()) / std


def overfit_stats(returns: pd.Series, family_sharpes: Sequence[float] = ()) -> OverfitStats | None:
    sharpe = sharpe_per_period(returns)
    if sharpe is None:
        return None
    try:
        single = SinglePeriodSharpeInput(
            observed_sharpe_per_period=sharpe,
            benchmark_sharpe_per_period=0.0,
            skewness=float(returns.skew()),
            pearson_kurtosis=float(returns.kurt()) + 3.0,
            independent_observations=len(returns),
        )
    except ValueError:
        return OverfitStats(observations=len(returns), sharpe_per_day=sharpe, psr=None,
                            min_track_record_days=None, dsr=None, trials=len(family_sharpes))
    psr = probabilistic_sharpe_ratio_per_period(single).probability
    trl = minimum_track_record_length_per_period(single, confidence=0.95).minimum_observations
    dsr = None
    if len(family_sharpes) >= 2:
        dsr = deflated_sharpe_ratio_per_period(DeflatedSharpeInput(
            selected_strategy=single,
            independent_trial_count=len(family_sharpes),
            family_sharpe_std_per_period=float(pd.Series(family_sharpes).std(ddof=1)),
        )).probability
    return OverfitStats(observations=len(returns), sharpe_per_day=sharpe, psr=psr,
                        min_track_record_days=trl, dsr=dsr, trials=len(family_sharpes))
