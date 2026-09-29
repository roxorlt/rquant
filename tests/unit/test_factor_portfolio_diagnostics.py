"""Pure quantile portfolio curves and target-weight changes."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from rquant.factor.evaluate import FactorEvaluationInput, FactorSample

MARKET_TZ = timezone(timedelta(hours=8))
FIRST = datetime(2026, 7, 14, 15, tzinfo=MARKET_TZ)
SECOND = FIRST + timedelta(days=1)
AS_OF = SECOND + timedelta(days=1, hours=1)


def _sample(
    code: str,
    factor: float,
    returned: float,
    *,
    decision_at: datetime = FIRST,
    return_end_at: datetime | None = None,
) -> FactorSample:
    from rquant.factor.evaluate import FactorSample

    return FactorSample(
        stock_code=code,
        decision_at=decision_at,
        factor_visible_at=decision_at - timedelta(minutes=1),
        factor_value=factor,
        return_end_at=return_end_at or decision_at + timedelta(days=1),
        forward_return=returned,
    )


def _input(
    samples: tuple[FactorSample, ...],
    *,
    direction: str = "higher_is_better",
    universe: tuple[str, ...] | None = None,
) -> FactorEvaluationInput:
    from rquant.factor.evaluate import FactorEvaluationInput

    return FactorEvaluationInput(
        universe=universe or tuple(sorted({sample.stock_code for sample in samples})),
        as_of=AS_OF,
        direction=direction,
        samples=samples,
    )


def test_three_stock_two_period_curves_are_compounded_in_time_order() -> None:
    from rquant.factor.portfolio import evaluate_factor_portfolios

    samples = (
        _sample("A", 3, 0.1, decision_at=SECOND),
        _sample("C", 3, 0.2),
        _sample("B", 1, -0.1, decision_at=SECOND),
        _sample("B", 2, 0),
        _sample("C", 2, 0.05, decision_at=SECOND),
        _sample("A", 1, 0.1),
    )
    first, second = evaluate_factor_portfolios(_input(samples)).days

    assert [day.decision_date for day in (first, second)] == [date(2026, 7, 14), date(2026, 7, 15)]
    assert (first.decision_at, first.return_end_at) == (FIRST, SECOND)
    assert (second.decision_at, second.return_end_at) == (
        SECOND,
        SECOND + timedelta(days=1),
    )
    early = first.groupings[0]
    later = second.groupings[0]
    assert (early.group_count, early.status, early.source_sample_count) == (3, "ok", 3)
    assert [point.period_return for point in early.groups] == pytest.approx([0.1, 0, 0.2])
    assert [point.cumulative_return for point in early.groups] == pytest.approx([0.1, 0, 0.2])
    assert [point.period_return for point in later.groups] == pytest.approx([-0.1, 0.05, 0.1])
    assert [point.cumulative_return for point in later.groups] == pytest.approx([-0.01, 0.05, 0.32])
    assert [point.target_weight_turnover for point in early.groups] == [None, None, None]
    assert [point.target_weight_turnover for point in later.groups] == pytest.approx([1, 1, 1])
    assert (early.long_short_return, early.long_short_cumulative_spread) == pytest.approx(
        (0.1, 0.1)
    )
    assert (later.long_short_return, later.long_short_cumulative_spread) == pytest.approx(
        (0.2, 0.33)
    )
    assert all(group.status == "insufficient_samples" for group in first.groupings[1:])
    assert all(
        group.groups == () and group.long_short_return is None for group in first.groupings[1:]
    )


def test_fixed_universe_valid_subsets_compound_groups_and_target_weight_changes() -> None:
    from rquant.factor.portfolio import evaluate_factor_portfolios

    samples = (
        _sample("A", 1, 0.1),
        _sample("B", 2, 0.2),
        _sample("C", 3, 0.3),
        _sample("A", 1, 0.05, decision_at=SECOND),
        _sample("C", 2, -0.1, decision_at=SECOND),
        _sample("D", 3, 0.2, decision_at=SECOND),
    )
    first, second = evaluate_factor_portfolios(
        _input(tuple(reversed(samples)), universe=("A", "B", "C", "D"))
    ).days

    assert [day.source_sample_count for day in (first, second)] == [3, 3]
    assert [group.period_return for group in first.groupings[0].groups] == pytest.approx(
        [0.1, 0.2, 0.3]
    )
    assert [group.period_return for group in second.groupings[0].groups] == pytest.approx(
        [0.05, -0.1, 0.2]
    )
    assert [group.cumulative_return for group in second.groupings[0].groups] == pytest.approx(
        [0.155, 0.08, 0.56]
    )
    assert [group.target_weight_turnover for group in second.groupings[0].groups] == [0, 1, 1]
    assert all(group.status == "insufficient_samples" for group in second.groupings[1:])


def test_partial_day_below_three_keeps_explicit_insufficient_grouping() -> None:
    from rquant.factor.portfolio import evaluate_factor_portfolios

    samples = tuple(
        _sample(code, factor, 0, decision_at=decision_at)
        for decision_at, stocks in (
            (FIRST, (("A", 1), ("B", 2), ("C", 3))),
            (SECOND, (("A", 1), ("B", 2))),
        )
        for code, factor in stocks
    )
    first, second = evaluate_factor_portfolios(_input(samples, universe=("A", "B", "C"))).days

    assert first.groupings[0].status == "ok"
    assert second.source_sample_count == 2
    assert all(group.status == "insufficient_samples" for group in second.groupings)


def test_direction_tie_order_and_group_sizes_match_existing_evaluator() -> None:
    from rquant.factor.evaluate import evaluate_factor
    from rquant.factor.portfolio import evaluate_factor_portfolios

    samples = tuple(
        _sample(code, factor, returned)
        for code, factor, returned in (
            ("C", 1, 0.3),
            ("B", 1, 0.2),
            ("E", 3, 0.5),
            ("A", 1, 0.1),
            ("D", 2, 0.4),
        )
    )
    for direction in ("higher_is_better", "lower_is_better"):
        data = _input(samples, direction=direction)
        current = evaluate_factor(data).days[0]
        portfolio = evaluate_factor_portfolios(data).days[0]
        for grouping, curve in zip(current.groupings, portfolio.groupings, strict=True):
            assert curve.group_count == grouping.group_count
            assert curve.status == grouping.status
            assert [point.member_count for point in curve.groups] == [
                point.member_count for point in grouping.groups
            ]
            assert [point.period_return for point in curve.groups] == pytest.approx(
                [point.mean_forward_return for point in grouping.groups]
            )
    higher = evaluate_factor_portfolios(_input(samples)).days[0].groupings[0]
    assert [point.period_return for point in higher.groups] == pytest.approx([0.15, 0.35, 0.5])
    assert [point.member_count for point in higher.groups] == [2, 2, 1]


def test_equal_weight_target_change_is_half_for_one_of_two_replacements() -> None:
    from rquant.factor.portfolio import evaluate_factor_portfolios

    samples = tuple(
        _sample(code, factor, 0, decision_at=decision_at)
        for decision_at, rankings in (
            (FIRST, (("A", 1), ("B", 2), ("C", 3), ("D", 4))),
            (SECOND, (("A", 1), ("C", 2), ("B", 3), ("D", 4))),
        )
        for code, factor in rankings
    )
    first, second = evaluate_factor_portfolios(_input(samples)).days
    assert [point.target_weight_turnover for point in first.groupings[0].groups] == [
        None,
        None,
        None,
    ]
    assert [point.target_weight_turnover for point in second.groupings[0].groups] == pytest.approx(
        [0.5, 1.0, 0.0]
    )


def test_ten_group_partition_has_no_empty_group_with_eleven_stocks() -> None:
    from rquant.factor.portfolio import evaluate_factor_portfolios

    samples = tuple(_sample(f"{index:06d}.SZ", float(index), index / 100) for index in range(11))
    day = evaluate_factor_portfolios(_input(tuple(reversed(samples)))).days[0]
    grouping = day.groupings[2]

    assert grouping.status == "ok"
    assert [point.member_count for point in grouping.groups] == [2] + [1] * 9
    assert [point.period_return for point in grouping.groups] == pytest.approx(
        [0.005] + [index / 100 for index in range(2, 11)]
    )
    assert grouping.long_short_return == pytest.approx(0.095)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda rows: (
                rows[:-1] + (_sample("C", 3, 0.3, decision_at=SECOND + timedelta(minutes=1)),)
            ),
            "same decision_at",
        ),
        (
            lambda rows: (
                rows[:-1]
                + (
                    _sample(
                        "C",
                        3,
                        0.3,
                        decision_at=SECOND,
                        return_end_at=SECOND + timedelta(days=1, minutes=1),
                    ),
                )
            ),
            "same return_end_at",
        ),
        (
            lambda rows: tuple(
                _sample(
                    sample.stock_code,
                    sample.factor_value,
                    sample.forward_return,
                    decision_at=sample.decision_at,
                    return_end_at=SECOND + timedelta(minutes=1),
                )
                if sample.decision_at == FIRST
                else sample
                for sample in rows
            ),
            "overlap",
        ),
    ],
)
def test_cumulative_curve_rejects_incomplete_or_inconsistent_windows(
    mutate: Callable[[tuple[FactorSample, ...]], tuple[FactorSample, ...]], message: str
) -> None:
    from rquant.factor.portfolio import evaluate_factor_portfolios

    samples = tuple(
        _sample(code, factor, 0, decision_at=decision_at)
        for decision_at in (FIRST, SECOND)
        for code, factor in (("A", 1), ("B", 2), ("C", 3))
    )
    changed = mutate(samples)
    with pytest.raises(ValueError, match=message):
        evaluate_factor_portfolios(_input(changed))


@pytest.mark.parametrize(
    ("returns", "message"),
    [
        ((0, 0, -1.01), "below -100%"),
    ],
)
def test_return_below_minus_one_cannot_be_compounded(
    returns: tuple[float, float, float], message: str
) -> None:
    from rquant.factor.portfolio import evaluate_factor_portfolios

    samples = tuple(
        _sample(code, factor, returned)
        for factor, (code, returned) in enumerate(zip(("A", "B", "C"), returns, strict=True), 1)
    )
    with pytest.raises(ValueError, match=message):
        evaluate_factor_portfolios(_input(samples))


def test_long_short_spread_below_minus_one_is_a_diagnostic_not_compounded_nav() -> None:
    from rquant.factor.portfolio import evaluate_factor_portfolios

    samples = tuple(
        _sample(code, factor, returned)
        for factor, (code, returned) in enumerate(
            zip(("A", "B", "C"), (1.0, 0.0, -1.0), strict=True), 1
        )
    )
    group = evaluate_factor_portfolios(_input(samples)).days[0].groupings[0]

    assert group.long_short_return == pytest.approx(-2.0)
    assert group.long_short_cumulative_spread == pytest.approx(-2.0)


def test_compounding_overflow_is_rejected_instead_of_emitting_infinity() -> None:
    from rquant.factor.portfolio import evaluate_factor_portfolios

    samples = tuple(
        _sample(code, factor, 1e308 if code == "C" else 0, decision_at=decision_at)
        for decision_at in (FIRST, SECOND)
        for factor, code in enumerate(("A", "B", "C"), 1)
    )
    with pytest.raises(ValueError, match="non-finite"):
        evaluate_factor_portfolios(_input(samples))
