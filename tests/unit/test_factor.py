"""Factor expression safety and test statistics on synthetic panels."""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest

from rquant.factor.evaluation import daily_ic, factor_test
from rquant.factor.expr import FactorExpressionError, evaluate, validate
from rquant.factor.store import list_factors, read_factor, run_factor, save


def _panel(days: int = 80, names: int = 30, seed: int = 0) -> dict[str, pd.DataFrame]:
    rng = np.random.default_rng(seed)
    idx = pd.bdate_range("2026-01-01", periods=days)
    cols = [f"{600000 + i}.SH" for i in range(names)]
    close = pd.DataFrame(10 * np.exp(np.cumsum(rng.normal(0, 0.02, (days, names)), axis=0)),
                         index=idx, columns=cols)
    return {f: close for f in ("open", "high", "low", "close", "pre_close")} | {
        "pct_chg": close.pct_change() * 100, "vol": close * 0 + 1, "amount": close * 0 + 1}


@pytest.mark.parametrize("bad", [
    "__import__('os')", "close.__class__", "foo(close)", "ts_mean(close, 0)",
    "ts_mean(close, n)", "'a'", "close if close else open", "[close]",
])
def test_unsafe_or_invalid_expressions_are_rejected(bad: str) -> None:
    with pytest.raises(FactorExpressionError):
        evaluate(bad, _panel(10, 3))


def test_operators_evaluate() -> None:
    panel = _panel(30, 5)
    out = evaluate("cs_rank(ts_delta(close, 5) / ts_std(close, 10)) - 0.5", panel)
    assert out.iloc[:9].isna().all().all()
    assert out.iloc[-1].between(-0.5, 0.5).all()
    validate("-log(abs(ts_mean(close, 3)))")


def test_perfect_foresight_factor_has_ic_one_and_noise_near_zero() -> None:
    panel = _panel()
    close = panel["close"]
    future = close.shift(-5) / close - 1
    assert daily_ic(future, future).mean() == pytest.approx(1.0)
    perfect = factor_test(future, close, horizon=5)
    assert perfect.mean_ic == pytest.approx(1.0)
    assert [q.quantile for q in perfect.quantiles] == [1, 2, 3, 4, 5]
    assert perfect.long_short is not None and perfect.long_short > 0
    noise = factor_test(pd.DataFrame(np.random.default_rng(9).normal(size=close.shape),
                                     index=close.index, columns=close.columns), close)
    assert noise.mean_ic is not None and abs(noise.mean_ic) < 0.1


def test_store_round_trip(tmp_path) -> None:
    panel = _panel()
    result = run_factor(panel, "ts_delta(close, 5)", date(2026, 2, 2), date(2026, 4, 1))
    run = save("动量5", "ts_delta(close, 5)", date(2026, 2, 2), date(2026, 4, 1), result,
               root=tmp_path)
    assert result.ic_series[0].date >= date(2026, 2, 2)
    assert read_factor(run.factor_id, tmp_path) == run
    assert [r.factor_id for r in list_factors(tmp_path)] == [run.factor_id]
