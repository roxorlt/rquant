"""Pure daily cross-sectional factor evaluation contracts."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from rquant.factor.evaluate import FactorSample


MARKET_TZ = timezone(timedelta(hours=8))
DECISION_AT = datetime(2026, 7, 14, 15, tzinfo=MARKET_TZ)
AS_OF = datetime(2026, 7, 16, 16, tzinfo=MARKET_TZ)


def _sample(
    stock_code: str,
    factor_value: float,
    forward_return: float,
    *,
    decision_at: datetime = DECISION_AT,
) -> FactorSample:
    from rquant.factor.evaluate import FactorSample

    return FactorSample(
        stock_code=stock_code,
        decision_at=decision_at,
        factor_visible_at=decision_at - timedelta(minutes=1),
        factor_value=factor_value,
        return_end_at=decision_at + timedelta(days=1),
        forward_return=forward_return,
    )


def test_perfect_ic_and_direction_control_group_order() -> None:
    from rquant.factor.evaluate import FactorEvaluationInput, evaluate_factor

    samples = tuple(
        _sample(f"00000{factor}.SZ", float(factor), (factor - 2) / 10) for factor in (1, 2, 3)
    )
    common = {"universe": tuple(sample.stock_code for sample in samples), "as_of": AS_OF}

    positive = evaluate_factor(
        FactorEvaluationInput(**common, direction="higher_is_better", samples=samples)
    ).days[0]
    negative = evaluate_factor(
        FactorEvaluationInput(**common, direction="lower_is_better", samples=samples)
    ).days[0]

    assert positive.decision_date == date(2026, 7, 14)
    assert positive.source_sample_count == positive.effective_sample_count == 3
    assert positive.normal_ic.status == positive.rank_ic.status == "ok"
    assert positive.normal_ic.value == pytest.approx(1)
    assert positive.rank_ic.value == pytest.approx(1)
    assert negative.normal_ic.value == pytest.approx(-1)
    assert negative.rank_ic.value == pytest.approx(-1)
    assert [group.mean_forward_return for group in positive.groupings[0].groups] == pytest.approx(
        [-0.1, 0, 0.1]
    )
    assert [group.mean_forward_return for group in negative.groupings[0].groups] == pytest.approx(
        [0.1, 0, -0.1]
    )


def test_rank_ic_uses_average_ranks_for_tied_values() -> None:
    from rquant.factor.evaluate import FactorEvaluationInput, evaluate_factor

    samples = tuple(
        _sample(f"{index:06d}.SZ", factor, ret)
        for index, (factor, ret) in enumerate(((1, 1), (1, 2), (2, 2), (3, 3)), 1)
    )
    day = evaluate_factor(
        FactorEvaluationInput(
            universe=tuple(sample.stock_code for sample in samples),
            as_of=AS_OF,
            direction="higher_is_better",
            samples=samples,
        )
    ).days[0]

    assert day.rank_ic.value == pytest.approx(5 / 6)
    assert day.normal_ic.value == pytest.approx(2 / (5.5**0.5))


@pytest.mark.parametrize("constant", ["factor", "return"])
def test_constant_cross_section_is_uncalculable(constant: str) -> None:
    from rquant.factor.evaluate import FactorEvaluationInput, evaluate_factor

    samples = tuple(
        _sample(
            f"{index:06d}.SZ",
            1 if constant == "factor" else index,
            1 if constant == "return" else index,
        )
        for index in range(1, 4)
    )
    day = evaluate_factor(
        FactorEvaluationInput(
            universe=tuple(sample.stock_code for sample in samples),
            as_of=AS_OF,
            direction="higher_is_better",
            samples=samples,
        )
    ).days[0]

    for result in (day.normal_ic, day.rank_ic):
        assert result.status == "zero_variance"
        assert result.value is None
        assert result.source_sample_count == result.effective_sample_count == 3


def test_days_and_stable_quantile_groups_are_ordered_and_counted() -> None:
    from rquant.factor.evaluate import FactorEvaluationInput, evaluate_factor

    later = tuple(
        _sample(f"{index:06d}.SZ", 1, index / 100) for index in (11, 1, 10, 2, 9, 3, 8, 4, 7, 5, 6)
    )
    earlier_at = DECISION_AT - timedelta(days=1)
    earlier = (_sample("000001.SZ", 1, 0.1, decision_at=earlier_at),)
    evaluation = evaluate_factor(
        FactorEvaluationInput(
            universe=tuple(sample.stock_code for sample in later),
            as_of=AS_OF,
            direction="higher_is_better",
            samples=later + earlier,
        )
    )

    assert [day.decision_date for day in evaluation.days] == [date(2026, 7, 13), date(2026, 7, 14)]
    first, second = evaluation.days
    assert first.normal_ic.status == first.rank_ic.status == "insufficient_samples"
    assert first.normal_ic.value is None
    assert first.source_sample_count == first.effective_sample_count == 1
    assert [(item.group_count, item.status, item.groups) for item in first.groupings] == [
        (3, "insufficient_samples", ()),
        (5, "insufficient_samples", ()),
        (10, "insufficient_samples", ()),
    ]
    assert [(item.group_count, item.status) for item in second.groupings] == [
        (3, "ok"),
        (5, "ok"),
        (10, "ok"),
    ]
    group_3 = second.groupings[0]
    assert [(group.group_number, group.member_count) for group in group_3.groups] == [
        (1, 4),
        (2, 4),
        (3, 3),
    ]
    assert [group.mean_forward_return for group in group_3.groups] == pytest.approx(
        [0.025, 0.065, 0.1]
    )
    assert [group.member_count for group in second.groupings[1].groups] == [3, 2, 2, 2, 2]
    assert [group.member_count for group in second.groupings[2].groups] == [2] + [1] * 9
    assert "stock_code" not in str(evaluation.model_dump())
    assert "000001.SZ" not in str(evaluation.model_dump())


def test_duplicate_stock_within_market_date_is_rejected_even_across_timezones() -> None:
    from rquant.factor.evaluate import FactorEvaluationInput

    same_market_day = DECISION_AT.astimezone(UTC) + timedelta(hours=1)
    with pytest.raises(ValueError, match="duplicate"):
        FactorEvaluationInput(
            universe=("000001.SZ",),
            as_of=AS_OF,
            direction="higher_is_better",
            samples=(
                _sample("000001.SZ", 1, 0.1),
                _sample("000001.SZ", 2, 0.2, decision_at=same_market_day),
            ),
        )


@pytest.mark.parametrize(
    ("override", "message"),
    [
        ({"factor_visible_at": DECISION_AT + timedelta(seconds=1)}, "factor_visible_at"),
        ({"return_end_at": DECISION_AT}, "return_end_at"),
        ({"decision_at": DECISION_AT.replace(tzinfo=None)}, "decision_at"),
        ({"factor_visible_at": DECISION_AT.replace(tzinfo=None)}, "factor_visible_at"),
        ({"return_end_at": AS_OF.replace(tzinfo=None)}, "return_end_at"),
        ({"factor_value": float("nan")}, "factor_value"),
        ({"factor_value": float("inf")}, "factor_value"),
        ({"forward_return": float("-inf")}, "forward_return"),
    ],
)
def test_invalid_pit_or_nonfinite_sample_is_rejected(
    override: dict[str, object], message: str
) -> None:
    from rquant.factor.evaluate import FactorSample

    data: dict[str, object] = {
        "stock_code": "000001.SZ",
        "decision_at": DECISION_AT,
        "factor_visible_at": DECISION_AT - timedelta(minutes=1),
        "factor_value": 1.0,
        "return_end_at": DECISION_AT + timedelta(days=1),
        "forward_return": 0.1,
    }
    data.update(override)
    with pytest.raises(ValueError, match=message):
        FactorSample(**data)


def test_unfinished_return_window_or_naive_as_of_is_rejected() -> None:
    from rquant.factor.evaluate import FactorEvaluationInput

    sample = _sample("000001.SZ", 1, 0.1)
    for invalid_as_of in (DECISION_AT, AS_OF.replace(tzinfo=None)):
        with pytest.raises(ValueError, match="as_of|return_end_at"):
            FactorEvaluationInput(
                universe=(sample.stock_code,),
                as_of=invalid_as_of,
                direction="higher_is_better",
                samples=(sample,),
            )


def test_universe_membership_and_duplicate_universe_are_checked() -> None:
    from rquant.factor.evaluate import FactorEvaluationInput

    sample = _sample("000001.SZ", 1, 0.1)
    for universe in (("000002.SZ",), ("000001.SZ", "000001.SZ")):
        with pytest.raises(ValueError):
            FactorEvaluationInput(
                universe=universe,
                as_of=AS_OF,
                direction="higher_is_better",
                samples=(sample,),
            )
