"""Joint residuals are checked against an independent dummy-matrix OLS."""

from __future__ import annotations

import math

import numpy as np
import pytest

from rquant.factor import time_series as series
from tests.unit.test_factor_neutralization import _DAY, _at, _industry, _size


def _joint(
    values: tuple[float | None, ...],
    groups: tuple[str | None, ...],
    caps: tuple[float | None, ...],
    *,
    reverse: bool = False,
) -> dict[str, series._Cell]:
    stocks = tuple(str(i) for i in range(len(values)))
    if reverse:
        stocks = stocks[::-1]
    return series.neutralize_factor_cells(
        "industry_size",
        {
            code: series._Cell(
                values[int(code)], "missing_value" if values[int(code)] is None else None, _at(_DAY)
            )
            for code in stocks
        },
        industries={code: _industry(_DAY, code, groups[int(code)]) for code in stocks},
        market_caps={code: _size(_DAY, code, caps[int(code)]) for code in stocks},
    )


def test_joint_residual_matches_dummy_ols_and_both_exposures() -> None:
    groups = ("X", "X", "X", "Y", "Y", "Y", "Z", "Z", "Z")
    caps = tuple(math.exp(x) for x in (1, 2, 3, 8, 9, 11, 14, 15, 18))
    y = np.array((2.0, 9.0, 4.0, 35.0, 39.0, 45.0, 80.0, 73.0, 99.0))
    matrix = np.column_stack(
        [*[np.array(groups) == group for group in ("X", "Y", "Z")], np.log(caps)]
    )
    expected = y - matrix @ np.linalg.lstsq(matrix, y, rcond=None)[0]
    for reverse in (False, True):
        result = _joint(tuple(y), groups, caps, reverse=reverse)
        actual = np.array([result[str(i)].value for i in range(len(y))])
        assert actual == pytest.approx(expected, abs=2e-12)
        assert matrix.T @ actual == pytest.approx(np.zeros(4), abs=2e-11)
    demeaned = y - np.array([y[np.array(groups) == group].mean() for group in groups])
    chained = (
        demeaned
        - np.column_stack([np.ones(len(y)), np.log(caps)])
        @ np.linalg.lstsq(np.column_stack([np.ones(len(y)), np.log(caps)]), demeaned, rcond=None)[0]
    )
    assert not np.allclose(expected, chained)


def test_joint_common_sample_keeps_missing_and_drops_small_groups() -> None:
    result = _joint(
        (1, 5, 4, 9, 3, None, 7), ("X", "X", "Y", "Y", "Z", "X", None), (1, 3, 5, 7, 8, 2, 4)
    )
    assert result["4"].reason == "insufficient_samples"
    assert result["5"].reason == "missing_value"
    assert result["6"].reason == "missing_context"
    assert all(result[str(i)].value is not None for i in range(4))


@pytest.mark.parametrize(
    "values,groups,caps,reason",
    [
        ((1, 2), ("X", "X"), (1, 2), "insufficient_samples"),
        ((1, 2, 3, 4), ("X", "X", "Y", "Y"), (1, 1, 100, 100), "zero_variance"),
    ],
)
def test_joint_rejects_insufficient_or_only_between_group_variance(
    values: tuple[float, ...], groups: tuple[str, ...], caps: tuple[float, ...], reason: str
) -> None:
    assert {cell.reason for cell in _joint(values, groups, caps).values()} == {reason}


def test_joint_extreme_positive_caps_stays_finite() -> None:
    result = _joint(
        (1e308, -1e308, 3e307, 4e307), ("X", "X", "Y", "Y"), (5e-324, 1e-300, 1e280, 1e308)
    )
    assert all(cell.value is not None and math.isfinite(cell.value) for cell in result.values())


def test_joint_reports_precision_when_scaling_erases_nonzero_group_values() -> None:
    result = _joint((1e308, 1e308, 1e-308, -1e-308), ("X", "X", "Y", "Y"), (1, 2, 3, 4))
    assert {cell.reason for cell in result.values()} == {"precision_limit"}
