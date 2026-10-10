"""Price-basis assembly preserves numeric and frame behavior."""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    import pandas as pd

    from rquant.price_adjustment import PriceFactorBasis

_FIRST = date(2026, 6, 22)
_SECOND = date(2026, 6, 23)
_REFERENCE = date(2026, 6, 24)


def _basis(factors: dict[date, float | None]) -> PriceFactorBasis:
    from rquant.price_adjustment import resolve_price_factor_basis

    return resolve_price_factor_basis(
        required_dates=(_FIRST, _SECOND, _REFERENCE),
        factor_by_date=factors,
        reference_date=_REFERENCE,
    )


def _frame() -> pd.DataFrame:
    import pandas as pd

    frame = pd.DataFrame(
        {
            "amount": [100, 200, 300],
            "close": ["inf", "-inf", "3.25"],
            "trade_date": [_FIRST, _SECOND, _REFERENCE],
            "open": [" 2.5 ", "invalid", None],
            "high": [10.0, None, 4.0],
            "low": pd.array([4.0, 6.0, pd.NA], dtype="Float64"),
            "pre_close": ["1e1", "8.0", "invalid"],
            "label": ["a", "b", "c"],
        },
        index=pd.Index([7, 3, 7], name="observation"),
    )
    for column in ("open", "close", "pre_close"):
        frame[column] = frame[column].astype(object)
    return frame


def test_fractional_basis_preserves_values_dtypes_order_and_input() -> None:
    import pandas as pd

    from rquant.stock_features import _apply_price_basis

    frame = _frame()
    original = frame.copy(deep=True)
    expected = frame.copy(deep=True)
    expected["open"] = [1.25, float("nan"), float("nan")]
    expected["high"] = [5.0, float("nan"), 4.0]
    expected["low"] = pd.array([2.0, 9.0, pd.NA], dtype="Float64")
    expected["close"] = [float("inf"), -float("inf"), 3.25]
    expected["pre_close"] = [5.0, 12.0, float("nan")]

    result = _apply_price_basis(frame, _basis({_FIRST: 1.0, _SECOND: 3.0, _REFERENCE: 2.0}))

    pd.testing.assert_frame_equal(result, expected, check_exact=True)
    pd.testing.assert_frame_equal(frame, original, check_exact=True)


def test_available_basis_missing_row_ratio_preserves_nan_behavior() -> None:
    import pandas as pd

    from rquant.price_adjustment import resolve_price_factor_basis
    from rquant.stock_features import _apply_price_basis

    frame = _frame()
    original = frame.copy(deep=True)
    expected = frame.copy(deep=True)
    expected["open"] = [1.25, float("nan"), float("nan")]
    expected["high"] = [5.0, float("nan"), 4.0]
    expected["low"] = pd.array([2.0, pd.NA, pd.NA], dtype="Float64")
    expected["close"] = [float("inf"), float("nan"), 3.25]
    expected["pre_close"] = [5.0, float("nan"), float("nan")]
    basis = resolve_price_factor_basis(
        required_dates=(_FIRST, _REFERENCE),
        factor_by_date={_FIRST: 1.0, _REFERENCE: 2.0},
        reference_date=_REFERENCE,
    )

    result = _apply_price_basis(frame, basis)

    pd.testing.assert_frame_equal(result, expected, check_exact=True)
    pd.testing.assert_frame_equal(frame, original, check_exact=True)


def test_empty_price_frame_preserves_schema_and_input() -> None:
    import pandas as pd

    from rquant.stock_features import _apply_price_basis

    frame = _frame().iloc[:0].copy()
    original = frame.copy(deep=True)
    expected = frame.copy(deep=True)
    for column in ("open", "close", "pre_close"):
        expected[column] = pd.Series(index=expected.index, dtype="float64")

    result = _apply_price_basis(frame, _basis({_FIRST: 1.0, _SECOND: 3.0, _REFERENCE: 2.0}))

    pd.testing.assert_frame_equal(result, expected, check_exact=True)
    pd.testing.assert_frame_equal(frame, original, check_exact=True)


@pytest.mark.parametrize(
    ("factor_date", "factor"),
    [
        (_REFERENCE, None),
        (_REFERENCE, float("nan")),
        (_REFERENCE, 0.0),
        (_FIRST, None),
        (_FIRST, float("inf")),
        (_FIRST, -1.0),
    ],
    ids=[
        "missing_reference",
        "nonfinite_reference",
        "zero_reference",
        "missing_required",
        "nonfinite_required",
        "negative_required",
    ],
)
def test_unavailable_basis_rejects_without_mutating_input(
    factor_date: date, factor: float | None
) -> None:
    import pandas as pd

    from rquant.stock_features import _apply_price_basis

    frame = _frame()
    original = frame.copy(deep=True)
    factors = {_FIRST: 1.0, _SECOND: 3.0, _REFERENCE: 2.0}
    factors[factor_date] = factor
    basis = _basis(factors)
    assert not basis.available

    with pytest.raises(ValueError, match="cannot adjust prices with an unavailable basis"):
        _apply_price_basis(frame, basis)

    pd.testing.assert_frame_equal(frame, original, check_exact=True)
