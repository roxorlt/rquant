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


def test_tracking_recomputes_recent_ic(tmp_path) -> None:
    from rquant.factor.store import read_tracking, save_tracking, track

    panel = _panel(days=120)
    end = panel["close"].index[-1].date()
    run = save("动量5", "ts_delta(close, 5)", date(2026, 2, 2), date(2026, 3, 2),
               run_factor(panel, "ts_delta(close, 5)", date(2026, 2, 2), date(2026, 3, 2)),
               root=tmp_path)
    tracking = track(run, panel, end, lookback_days=60)
    assert tracking.latest_date is not None and tracking.latest_date <= end
    assert tracking.points and tracking.research_ic == run.result.mean_ic
    save_tracking(tracking, tmp_path)
    assert read_tracking(run.factor_id, tmp_path) == tracking


def test_comparisons_and_boolean_ops() -> None:
    panel = _panel(30, 5)
    hit = evaluate("close > ts_mean(close, 5) and not vol < 0", panel)
    assert set(hit.iloc[-1].unique()) <= {0.0, 1.0}
    assert hit.iloc[:4].isna().all().all()  # warm-up stays NaN, never a "hit"


@pytest.mark.parametrize(("tdx", "expr"), [
    ("C>MA(C,5)", "close>ts_mean(close,5)"),
    ("MA5:=MA(C,5);MA10:=MA(C,10);CROSS1:=MA5>MA10;CROSS1 AND V>REF(V,1)*2",
     "((ts_mean(close,5))>(ts_mean(close,10))) and vol>delay(vol,1)*2"),
    ("C=HHV(H,20) OR C<>REF(C,1)", "close==ts_max(high,20) or close!=delay(close,1)"),
])
def test_tdx_translation(tdx: str, expr: str) -> None:
    from rquant.factor.tdx import translate

    assert translate(tdx) == expr
    evaluate(expr, _panel(30, 5))


@pytest.mark.parametrize("bad", ["CROSS(C,MA(C,5))", "C>MA(C,N)", "DRAWTEXT(1,2,'x')", ""])
def test_tdx_unsupported_is_explicit(bad: str) -> None:
    from rquant.factor.tdx import TdxFormulaError, translate

    with pytest.raises(TdxFormulaError):
        translate(bad)


def test_condition_screen_uses_last_day_only(tmp_path) -> None:
    from rquant.factor.condition import list_conditions, save, screen
    from rquant.factor.tdx import translate

    panel = _panel(40, 20)
    expr = translate("C>MA(C,5)")
    trade_date, universe, hits = screen(panel, expr)
    last = panel["close"].iloc[-1]
    ma = panel["close"].rolling(5).mean().iloc[-1]
    assert trade_date == panel["close"].index[-1].date() and universe == 20
    assert [h.code for h in hits] == sorted(last[last > ma].index)
    run = save("站上5日线", expr, "C>MA(C,5)", (trade_date, universe, hits), root=tmp_path)
    assert list_conditions(tmp_path)[0].run_id == run.run_id
