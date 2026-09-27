"""Long-window IC statistics from already evaluated market dates."""

from __future__ import annotations

from datetime import date, timedelta
from math import atan, fsum, nextafter, pi, sqrt
from statistics import stdev

import pytest

from rquant.factor.evaluate import CorrelationResult, DailyFactorResult, FactorEvaluation


def _correlation(status: str, value: float | None = None) -> CorrelationResult:
    return CorrelationResult(
        status=status,
        value=value,
        source_sample_count=10,
        effective_sample_count=10,
    )


def _day(
    offset: int,
    normal: CorrelationResult,
    rank: CorrelationResult | None = None,
) -> DailyFactorResult:
    return DailyFactorResult(
        decision_date=date(2026, 7, 1) + timedelta(days=offset),
        source_sample_count=10,
        effective_sample_count=10,
        normal_ic=normal,
        rank_ic=rank if rank is not None else normal,
        groupings=(),
    )


def test_normal_and_rank_ic_are_summarized_independently_with_fixed_reference() -> None:
    from rquant.factor.summary import summarize_factor_ic

    evaluation = FactorEvaluation(
        days=(
            _day(0, _correlation("ok", 0.1), _correlation("ok", -0.3)),
            _day(1, _correlation("ok", 0.2), _correlation("ok", 0.0)),
            _day(2, _correlation("ok", 0.3), _correlation("ok", 0.3)),
        )
    )

    result = summarize_factor_ic(evaluation)
    normal = result.normal_ic
    rank = result.rank_ic

    assert normal.status == rank.status == "ok"
    assert normal.source_day_count == normal.valid_day_count == 3
    assert normal.mean == pytest.approx(0.2)
    assert normal.sample_std == pytest.approx(0.1)
    assert normal.ir == pytest.approx(2.0)
    assert normal.positive_rate == pytest.approx(1.0)
    assert normal.strong_signal_rate == pytest.approx(1.0)
    assert normal.t_value == pytest.approx(2 * sqrt(3))
    assert normal.p_value == pytest.approx(1 - sqrt(6 / 7))
    assert normal.skewness == pytest.approx(0.0, abs=1e-12)
    assert normal.excess_kurtosis == pytest.approx(-1.5)

    assert rank.mean == pytest.approx(0.0, abs=1e-12)
    assert rank.sample_std == pytest.approx(0.3)
    assert rank.ir == pytest.approx(0.0, abs=1e-12)
    assert rank.positive_rate == pytest.approx(1 / 3)
    assert rank.strong_signal_rate == pytest.approx(2 / 3)
    assert rank.t_value == pytest.approx(0.0, abs=1e-12)
    assert rank.p_value == pytest.approx(1.0)
    assert rank.skewness == pytest.approx(0.0, abs=1e-12)
    assert rank.excess_kurtosis == pytest.approx(-1.5)


def test_unavailable_daily_ic_keeps_reason_counts_and_does_not_enter_denominator() -> None:
    from rquant.factor.summary import summarize_factor_ic

    evaluation = FactorEvaluation(
        days=(
            _day(0, _correlation("ok", 0.02), _correlation("zero_variance")),
            _day(1, _correlation("insufficient_samples"), _correlation("ok", -0.05)),
            _day(2, _correlation("zero_variance"), _correlation("ok", 0.05)),
            _day(3, _correlation("ok", -0.03), _correlation("insufficient_samples")),
        )
    )

    result = summarize_factor_ic(evaluation)
    normal = result.normal_ic
    rank = result.rank_ic

    assert (normal.source_day_count, normal.valid_day_count) == (4, 2)
    assert (normal.insufficient_day_count, normal.zero_variance_day_count) == (1, 1)
    assert normal.mean == pytest.approx(-0.005)
    assert normal.positive_rate == pytest.approx(0.5)
    assert normal.strong_signal_rate == pytest.approx(0.5)  # |0.02| is not > 0.02
    assert normal.t_value == pytest.approx(-0.2)
    assert normal.p_value == pytest.approx(1 - 2 * atan(0.2) / pi)
    assert (rank.insufficient_day_count, rank.zero_variance_day_count) == (1, 1)
    assert rank.mean == pytest.approx(0.0)
    assert rank.strong_signal_rate == pytest.approx(1.0)
    assert rank.p_value == pytest.approx(1.0)


def test_zero_valid_days_leave_every_statistic_unavailable() -> None:
    from rquant.factor.summary import summarize_factor_ic

    result = summarize_factor_ic(FactorEvaluation(days=()))
    for series in (result.normal_ic, result.rank_ic):
        assert series.status == "no_valid_days"
        assert series.source_day_count == series.valid_day_count == 0
        assert series.insufficient_day_count == series.zero_variance_day_count == 0
        assert all(
            getattr(series, name) is None
            for name in (
                "mean",
                "sample_std",
                "ir",
                "positive_rate",
                "strong_signal_rate",
                "t_value",
                "p_value",
                "skewness",
                "excess_kurtosis",
            )
        )


def test_single_valid_day_has_mean_and_rates_without_inferential_statistics() -> None:
    from rquant.factor.summary import summarize_factor_ic

    result = summarize_factor_ic(
        FactorEvaluation(
            days=(
                _day(0, _correlation("insufficient_samples")),
                _day(1, _correlation("ok", -0.03)),
            )
        )
    ).normal_ic

    assert result.status == "insufficient_samples"
    assert (result.source_day_count, result.valid_day_count) == (2, 1)
    assert result.mean == pytest.approx(-0.03)
    assert result.positive_rate == pytest.approx(0.0)
    assert result.strong_signal_rate == pytest.approx(1.0)
    assert all(
        getattr(result, name) is None
        for name in ("sample_std", "ir", "t_value", "p_value", "skewness", "excess_kurtosis")
    )


def test_two_valid_days_use_sample_std_and_exact_student_t_probability() -> None:
    from rquant.factor.summary import summarize_factor_ic

    result = summarize_factor_ic(
        FactorEvaluation(days=(_day(0, _correlation("ok", 0.1)), _day(1, _correlation("ok", 0.2))))
    ).normal_ic

    assert result.status == "ok"
    assert result.mean == pytest.approx(0.15)
    assert result.sample_std == pytest.approx(sqrt(0.005))
    assert result.ir == pytest.approx(0.15 / sqrt(0.005))
    assert result.t_value == pytest.approx(3.0)
    assert result.p_value == pytest.approx(2 * atan(1 / 3) / pi)
    assert result.skewness == pytest.approx(0.0, abs=1e-12)
    assert result.excess_kurtosis == pytest.approx(-2.0)


def test_four_dates_match_independent_student_t_three_df_reference() -> None:
    from rquant.factor.summary import summarize_factor_ic

    result = summarize_factor_ic(
        FactorEvaluation(
            days=tuple(
                _day(index, _correlation("ok", value))
                for index, value in enumerate((0.0, 1.0, 0.0, 1.0))
            )
        )
    ).normal_ic

    assert result.t_value == pytest.approx(sqrt(3))
    assert result.p_value == pytest.approx(0.5 - 1 / pi)


def test_tiny_but_distinct_finite_ic_preserves_scale_free_statistics() -> None:
    from rquant.factor.summary import summarize_factor_ic

    result = summarize_factor_ic(
        FactorEvaluation(
            days=tuple(
                _day(index, _correlation("ok", value))
                for index, value in enumerate((1e-150, 2e-150, 3e-150))
            )
        )
    ).normal_ic

    assert result.status == "ok"
    assert result.mean == pytest.approx(2e-150, rel=1e-12, abs=0)
    assert result.sample_std == pytest.approx(1e-150, rel=1e-12, abs=0)
    assert result.ir == pytest.approx(2.0)
    assert result.t_value == pytest.approx(2 * sqrt(3))
    assert result.p_value == pytest.approx(1 - sqrt(6 / 7))
    assert result.skewness == pytest.approx(0.0, abs=1e-12)
    assert result.excess_kurtosis == pytest.approx(-1.5)


def test_subnormal_five_day_ic_keeps_ratios_when_raw_mean_and_std_underflow() -> None:
    from rquant.factor.summary import summarize_factor_ic

    smallest = float.fromhex("0x0.0000000000001p-1022")
    result = summarize_factor_ic(
        FactorEvaluation(
            days=tuple(
                _day(index, _correlation("ok", value))
                for index, value in enumerate((smallest, 0.0, 0.0, 0.0, 0.0))
            )
        )
    ).normal_ic

    assert result.status == "precision_limit"
    assert result.mean is None
    assert result.sample_std is None
    assert result.ir == pytest.approx(1 / sqrt(5))
    assert result.t_value == pytest.approx(1.0)
    assert result.p_value == pytest.approx(1 - 7 / (5 * sqrt(5)))


def test_subnormal_two_day_ic_does_not_report_false_zero_mean_or_t_statistic() -> None:
    from rquant.factor.summary import summarize_factor_ic

    smallest = float.fromhex("0x0.0000000000001p-1022")
    result = summarize_factor_ic(
        FactorEvaluation(
            days=(_day(0, _correlation("ok", smallest)), _day(1, _correlation("ok", 0.0)))
        )
    ).normal_ic

    assert result.status == "precision_limit"
    assert result.mean is None
    assert result.sample_std == smallest
    assert result.ir == pytest.approx(1 / sqrt(2))
    assert result.t_value == pytest.approx(1.0)
    assert result.p_value == pytest.approx(0.5)


def test_mixed_sign_cancellation_preserves_nonzero_finite_mean_and_ir() -> None:
    from rquant.factor.summary import summarize_factor_ic

    values = (0.05639256240464006, 0.17102367116895592, -0.22741623357359597)
    expected_mean = fsum(values) / len(values)
    assert expected_mean == 2.3129646346357427e-18

    result = summarize_factor_ic(
        FactorEvaluation(
            days=tuple(_day(index, _correlation("ok", value)) for index, value in enumerate(values))
        )
    ).normal_ic

    expected_ir = expected_mean / stdev(values)
    assert result.status == "precision_limit"
    assert result.mean == pytest.approx(expected_mean, rel=1e-12, abs=0)
    assert result.ir == pytest.approx(expected_ir, rel=1e-12, abs=0)
    assert result.t_value == pytest.approx(expected_ir * sqrt(3), rel=1e-12, abs=0)
    assert result.p_value is None  # The nonzero t tail rounds to 1.0 in binary64.


def test_mixed_sign_extremely_small_exact_zero_sum_does_not_gain_phantom_mean() -> None:
    from rquant.factor.summary import summarize_factor_ic

    values = (5.639256240464006e-202, 1.710236711689559e-201, -2.2741623357359598e-201)
    assert fsum(values) == 0.0

    result = summarize_factor_ic(
        FactorEvaluation(
            days=tuple(_day(index, _correlation("ok", value)) for index, value in enumerate(values))
        )
    ).normal_ic

    assert result.status == "ok"
    assert result.mean == 0.0
    assert result.ir == 0.0
    assert result.t_value == 0.0
    assert result.p_value == 1.0


def test_mixed_scale_subnormal_residue_reports_unrepresentable_ratios() -> None:
    from rquant.factor.summary import summarize_factor_ic

    smallest = float.fromhex("0x0.0000000000001p-1022")
    values = (1.0, -1.0, smallest)
    assert fsum(values) == smallest

    result = summarize_factor_ic(
        FactorEvaluation(
            days=tuple(_day(index, _correlation("ok", value)) for index, value in enumerate(values))
        )
    ).normal_ic

    assert result.status == "precision_limit"
    assert result.mean is None
    assert result.sample_std == pytest.approx(1.0)
    assert result.ir is None
    assert result.t_value == smallest
    assert result.p_value is None


def test_constant_valid_days_report_zero_variance_without_inventing_ratios() -> None:
    from rquant.factor.summary import summarize_factor_ic

    result = summarize_factor_ic(
        FactorEvaluation(days=tuple(_day(index, _correlation("ok", 0.04)) for index in range(3)))
    ).normal_ic

    assert result.status == "zero_variance"
    assert result.mean == pytest.approx(0.04)
    assert result.sample_std == pytest.approx(0.0)
    assert result.positive_rate == pytest.approx(1.0)
    assert result.strong_signal_rate == pytest.approx(1.0)
    assert all(
        getattr(result, name) is None
        for name in ("ir", "t_value", "p_value", "skewness", "excess_kurtosis")
    )


def test_three_identical_decimal_ic_values_have_exactly_zero_variance() -> None:
    from rquant.factor.summary import summarize_factor_ic

    result = summarize_factor_ic(
        FactorEvaluation(days=tuple(_day(index, _correlation("ok", 0.1)) for index in range(3)))
    ).normal_ic

    assert result.status == "zero_variance"
    assert result.mean == 0.1
    assert result.sample_std == 0.0
    assert result.ir is result.t_value is result.p_value is None
    assert result.skewness is result.excess_kurtosis is None


def test_adjacent_floats_near_one_keep_sample_variance_and_population_moments() -> None:
    from rquant.factor.summary import summarize_factor_ic

    lower = nextafter(1.0, 0.0)
    result = summarize_factor_ic(
        FactorEvaluation(
            days=(_day(0, _correlation("ok", 1.0)), _day(1, _correlation("ok", lower)))
        )
    ).normal_ic

    assert result.sample_std == pytest.approx((1.0 - lower) / sqrt(2), rel=1e-12, abs=0)
    assert result.skewness == pytest.approx(0.0, abs=1e-12)
    assert result.excess_kurtosis == pytest.approx(-2.0)


@pytest.mark.parametrize(
    "days",
    [
        (_day(0, _correlation("ok", 0.1)), _day(0, _correlation("ok", 0.2))),
        (_day(1, _correlation("ok", 0.1)), _day(0, _correlation("ok", 0.2))),
    ],
)
def test_duplicate_or_unsorted_decision_dates_are_rejected(
    days: tuple[DailyFactorResult, ...],
) -> None:
    from rquant.factor.summary import summarize_factor_ic

    with pytest.raises(ValueError, match="decision_date"):
        summarize_factor_ic(FactorEvaluation(days=days))


@pytest.mark.parametrize("invalid_value", [float("nan"), float("inf"), float("-inf")])
def test_nonfinite_daily_ic_is_rejected_even_in_unchecked_model_instance(
    invalid_value: float,
) -> None:
    from rquant.factor.summary import summarize_factor_ic

    invalid = CorrelationResult.model_construct(
        status="ok", value=invalid_value, source_sample_count=10, effective_sample_count=10
    )
    with pytest.raises(ValueError, match="finite"):
        summarize_factor_ic(FactorEvaluation(days=(_day(0, invalid),)))


@pytest.mark.parametrize("value", [-1.1, 1.1])
def test_impossible_daily_correlation_is_rejected(value: float) -> None:
    from rquant.factor.summary import summarize_factor_ic

    with pytest.raises(ValueError, match="range"):
        summarize_factor_ic(FactorEvaluation(days=(_day(0, _correlation("ok", value)),)))


def test_ok_daily_correlation_requires_a_value() -> None:
    from rquant.factor.summary import summarize_factor_ic

    with pytest.raises(ValueError, match="finite"):
        summarize_factor_ic(FactorEvaluation(days=(_day(0, _correlation("ok")),)))


def test_unavailable_days_keep_reason_counts_when_no_ic_is_valid() -> None:
    from rquant.factor.summary import summarize_factor_ic

    result = summarize_factor_ic(
        FactorEvaluation(
            days=(
                _day(0, _correlation("insufficient_samples")),
                _day(1, _correlation("zero_variance")),
            )
        )
    ).normal_ic

    assert result.status == "no_valid_days"
    assert result.source_day_count == 2
    assert result.valid_day_count == 0
    assert result.insufficient_day_count == result.zero_variance_day_count == 1
    assert result.mean is None
