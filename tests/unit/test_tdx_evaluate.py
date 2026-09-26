"""Fixed examples for the restricted TDX formula evaluator."""

from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pandas as pd
import pytest
from pydantic import ValidationError

from rquant.screen.rules import above_ma
from rquant.screen.tdx.evaluate import (
    EvaluationRejectedError,
    FormulaEvaluationInput,
    HistoricalBar,
    StockHistory,
    evaluate_formula,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")
DATES = [date(2026, 9, day) for day in range(1, 6)]


def _stock(
    code: str = "000001.SZ",
    closes: tuple[float | None, ...] = (1, 2, 3, 4, 5),
    *,
    complete: bool = True,
    opens: tuple[float | None, ...] | None = None,
) -> StockHistory:
    bars = [
        HistoricalBar(
            trade_date=DATES[index],
            open=(opens[index] if opens is not None else 1),
            high=(close + 1 if close is not None else None),
            low=(close - 1 if close is not None else None),
            close=close,
            vol=(index + 1) * 10,
            amount=(index + 1) * 100,
        )
        for index, close in enumerate(closes)
    ]
    return StockHistory(stock_code=code, complete_from_listing=complete, bars=bars)


def _run(
    formula: str,
    *stocks: StockHistory,
    decision_date: date = DATES[-1],
):
    request = FormulaEvaluationInput(
        formula=formula,
        decision_date=decision_date,
        decision_at=datetime(2026, 9, 5, 17, tzinfo=SHANGHAI),
        stocks=list(stocks) or [_stock()],
    )
    return evaluate_formula(request)


@pytest.mark.parametrize(
    ("formula", "expected"),
    [
        ("CLOSE=5", True),
        ("OPEN=1 AND HIGH=6 AND LOW=4 AND VOL=50 AND AMOUNT=500", True),
        ("MA(CLOSE,3)=4", True),
        ("EMA(CLOSE,3)>4.0624 AND EMA(CLOSE,3)<4.0626", True),
        ("SMA(CLOSE,3,1)>3.395 AND SMA(CLOSE,3,1)<3.396", True),
        ("SMA(CLOSE,3,2)>4.5061 AND SMA(CLOSE,3,2)<4.5062", True),
        ("HHV(HIGH,3)=6 AND LLV(LOW,3)=2", True),
        ("SUM(VOL,3)=120 AND COUNT(CLOSE>3,3)=2", True),
        ("EVERY(CLOSE>2,3) AND EXIST(CLOSE>4,3)", True),
        ("REF(CLOSE,2)=3 AND REF(CLOSE>3,1)", True),
        ("CROSS(CLOSE,4)", True),
        ("BARSLAST(CLOSE=3)=2", True),
        ("IF(CLOSE>4,10,20)=10", True),
        ("AND(CLOSE>4,OR(OPEN>2,NOT(CLOSE<4)))", True),
        ("+CLOSE-2*OPEN/2=4", True),
        ("CLOSE>=5 AND CLOSE<=5 AND CLOSE<>4 AND CLOSE<6", True),
        ("CLOSE=4", False),
    ],
)
def test_all_whitelisted_functions_and_operators_have_hand_checked_results(
    formula: str, expected: bool
) -> None:
    result = _run(formula)

    assert result.syntax_version == "tdx-v1"
    assert result.required_lookback_bars >= 0
    assert result.decisions[0].status == ("match" if expected else "no_match")
    assert result.decisions[0].reason is None


def test_ema_sma_and_ma_match_fixed_mytt_reference_values() -> None:
    # MyTT EMA/SMA use pandas ewm(adjust=False); MA uses rolling(N).
    assert _run("EMA(CLOSE,3)>4.062499 AND EMA(CLOSE,3)<4.062501").decisions[0].status == "match"
    result = _run("SMA(CLOSE,3,1)>3.395060 AND SMA(CLOSE,3,1)<3.395063")
    assert result.decisions[0].status == "match"
    assert _run("MA(CLOSE,3)=4").decisions[0].status == "match"


def test_variables_are_scoped_per_stock_and_never_share_history() -> None:
    first = _stock("000001.SZ")
    second = _stock("000002.SZ", (10, 10, 10, 10, 10))
    result = _run("A:=BARSLAST(CLOSE=3); A>=0", first, second)

    assert [(item.stock_code, item.status, item.reason) for item in result.decisions] == [
        ("000001.SZ", "match", None),
        ("000002.SZ", "unknown", "never_true"),
    ]


def test_equivalent_ma_formula_matches_builtin_rule_on_same_prices() -> None:
    stocks = [_stock(), _stock("000002.SZ", (10, 10, 10, 10, 10))]
    formula = _run("CLOSE>MA(CLOSE,3)", *stocks)
    wide = pd.DataFrame({"CLOSE[0]": [5.0, 10.0], "MA3[0]": [4.0, 10.0]})

    assert [item.status == "match" for item in formula.decisions] == list(above_ma(3)(wide))


def test_short_window_and_missing_decision_date_remain_unknown() -> None:
    short = _stock(closes=(1, 2))
    result = _run("MA(CLOSE,3)>0", short)
    assert result.decisions[0].status == "unknown"
    assert result.decisions[0].reason == "missing_date"

    result = _run("MA(CLOSE,5)>0", _stock(closes=(1, 2, 3)))
    assert result.decisions[0].reason == "missing_date"

    result = _run("MA(CLOSE,5)>0", _stock(closes=(1, 2, 3)), decision_date=DATES[2])
    assert result.decisions[0].status == "unknown"
    assert result.decisions[0].reason == "insufficient_history"

    result = _run("REF(CLOSE,1)>0", _stock(closes=(1,)), decision_date=DATES[0])
    assert result.decisions[0].reason == "insufficient_history"


@pytest.mark.parametrize("formula", ["EMA(CLOSE,2)>0", "SMA(CLOSE,2,1)>0", "BARSLAST(CLOSE>2)>=0"])
def test_recursive_functions_require_explicit_complete_history(formula: str) -> None:
    result = _run(formula, _stock(complete=False))
    assert result.decisions[0].status == "unknown"
    assert result.decisions[0].reason == "incomplete_history"


def test_known_boolean_branch_can_resolve_incomplete_recursive_history() -> None:
    incomplete = _stock(complete=False)
    assert _run("OPEN=0 AND EMA(CLOSE,3)>0", incomplete).decisions[0].status == "no_match"
    assert _run("OPEN=1 OR BARSLAST(CLOSE>2)>0", incomplete).decisions[0].status == "match"
    assert _run("NOT(EMA(CLOSE,3)>0)", incomplete).decisions[0].status == "unknown"


def test_unknown_stays_unknown_under_not_and_comparison_but_kleene_logic_is_sound() -> None:
    missing = _stock(closes=(1, 2, 3, 4, None))
    assert _run("NOT(CLOSE>0)", missing).decisions[0].reason == "missing_value"
    assert _run("CLOSE>0", missing).decisions[0].status == "unknown"
    assert _run("CLOSE>0 AND OPEN=0", missing).decisions[0].status == "no_match"
    assert _run("CLOSE>0 OR OPEN=1", missing).decisions[0].status == "match"
    assert _run("IF(CLOSE>0,1,0)>0", missing).decisions[0].status == "unknown"


def test_missing_window_values_and_unknown_cross_do_not_become_matches() -> None:
    missing = _stock(closes=(1, 2, 3, None, 5))
    assert _run("MA(CLOSE,3)>0", missing).decisions[0].reason == "missing_value"
    assert _run("COUNT(CLOSE>0,3)>0", missing).decisions[0].reason == "missing_value"
    assert _run("CROSS(CLOSE,4)", missing).decisions[0].reason == "missing_value"


def test_zero_division_and_overflow_are_unknown() -> None:
    zero = _stock(opens=(1, 1, 1, 1, 0))
    assert _run("CLOSE/OPEN>0", zero).decisions[0].reason == "division_by_zero"
    large = "1" + "0" * 308
    assert _run(f"{large}*{large}>0", zero).decisions[0].reason == "non_finite"


def test_nonzero_arithmetic_underflow_does_not_fake_a_zero() -> None:
    tiny = "0." + "0" * 299 + "1"
    large = "1" + "0" * 300
    result = _run(f"{tiny}/{large}=0")
    assert result.decisions[0].status == "unknown"
    assert result.decisions[0].reason == "numeric_underflow"


def test_rejected_text_cannot_smuggle_ast_or_unsafe_expressions() -> None:
    for formula in ["REF(CLOSE,-1)>0", "CLOSE.__class__>0", "CLOSE/0>0"]:
        with pytest.raises(EvaluationRejectedError) as error:
            _run(formula)
        assert error.value.code == "formula"


def test_shape_date_and_non_finite_inputs_are_rejected() -> None:
    with pytest.raises(ValidationError):
        HistoricalBar(trade_date=DATES[0], close=float("nan"))
    unsorted = _stock().model_copy(update={"bars": tuple(reversed(_stock().bars))})
    with pytest.raises(EvaluationRejectedError, match="日期"):
        _run("CLOSE>0", unsorted)
    future = _stock().model_copy(
        update={"bars": (*_stock().bars, HistoricalBar(trade_date=date(2026, 9, 6), close=6))}
    )
    with pytest.raises(EvaluationRejectedError, match="未来"):
        _run("CLOSE>0", future)
    with pytest.raises(EvaluationRejectedError, match="重复"):
        _run("CLOSE>0", _stock(), _stock())


def test_validated_input_cannot_mutate_rows_between_checks_and_evaluation() -> None:
    stock = _stock()
    request = FormulaEvaluationInput(
        formula="CLOSE>0", decision_date=DATES[-1],
        decision_at=datetime(2026, 9, 5, 17, tzinfo=SHANGHAI), stocks=[stock],
    )
    assert isinstance(stock.bars, tuple)
    assert isinstance(request.stocks, tuple)
    with pytest.raises(AttributeError):
        stock.bars.append(HistoricalBar(trade_date=date(2026, 9, 6), close=6))


def test_decision_time_must_be_aware_and_after_daily_bar_is_visible() -> None:
    with pytest.raises(ValidationError):
        FormulaEvaluationInput(
            formula="CLOSE>0", decision_date=DATES[-1],
            decision_at=datetime(2026, 9, 5, 17), stocks=[_stock()],
        )
    with pytest.raises(EvaluationRejectedError, match="时点"):
        evaluate_formula(
            FormulaEvaluationInput(
                formula="CLOSE>0", decision_date=DATES[-1],
                decision_at=datetime(2026, 9, 5, 9, tzinfo=SHANGHAI),
                stocks=[_stock()],
            )
        )


def test_formula_budget_rejects_before_building_intermediate_vectors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.screen.tdx.evaluate as module

    monkeypatch.setattr(module, "MAX_STOCKS", 1)
    with pytest.raises(EvaluationRejectedError) as error:
        _run("CLOSE>0", _stock(), _stock("000002.SZ"))
    assert error.value.code == "limit"

    monkeypatch.setattr(module, "MAX_STOCKS", 100)
    monkeypatch.setattr(module, "MAX_BARS_PER_STOCK", 4)
    with pytest.raises(EvaluationRejectedError) as error:
        _run("CLOSE>0")
    assert error.value.code == "limit"

    monkeypatch.setattr(module, "MAX_BARS_PER_STOCK", 100)
    monkeypatch.setattr(module, "MAX_VECTOR_CELLS", 4)
    with pytest.raises(EvaluationRejectedError) as error:
        _run("CLOSE>0")
    assert error.value.code == "limit"


def test_history_byte_limit_measures_serialized_values_not_only_row_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.screen.tdx.evaluate as module

    rich = HistoricalBar(
        trade_date=DATES[0],
        open=1.2345678901234567, high=1.2345678901234567,
        low=1.2345678901234567, close=1.2345678901234567,
        vol=1.2345678901234567, amount=1.2345678901234567,
    )
    monkeypatch.setattr(module, "MAX_HISTORY_INPUT_BYTES", 140)
    request = FormulaEvaluationInput(
        formula="CLOSE>0", decision_date=DATES[0],
        decision_at=datetime(2026, 9, 5, 17, tzinfo=SHANGHAI),
        stocks=[StockHistory(
            stock_code="000001.SZ", complete_from_listing=True, bars=[rich],
        )],
    )
    with pytest.raises(EvaluationRejectedError) as error:
        evaluate_formula(request)
    assert error.value.code == "limit"


def test_full_history_bar_last_can_reset_after_unknown_condition() -> None:
    stock = _stock(closes=(1, None, 3, 4, 5))
    assert _run("BARSLAST(CLOSE=3)=2", stock).decisions[0].status == "match"
