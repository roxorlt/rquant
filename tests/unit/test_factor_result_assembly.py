"""A frozen factor batch and forward returns yield one reproducible research result."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone
from typing import TYPE_CHECKING

import pytest
from pydantic import ValidationError

from rquant.factor import (
    DecisionTime,
    FactorTimeSeriesInput,
    FeatureCatalog,
    FeatureObservation,
    build_factor_definition,
)

if TYPE_CHECKING:
    from rquant.factor.result import FactorForwardReturn, FactorResearchRequest

_TZ = timezone(timedelta(hours=8))
_DAYS = (date(2026, 7, 14), date(2026, 7, 15))
_STOCKS = ("A", "B", "C")
_DECISIONS = tuple(datetime(day.year, day.month, day.day, 15, tzinfo=_TZ) for day in _DAYS)
_ENDS = (_DECISIONS[1], _DECISIONS[1] + timedelta(days=1))
_AS_OF = _ENDS[1] + timedelta(hours=1)
_FACTORS = ((1.0, 2.0, 3.0), (3.0, 1.0, 2.0))
_RETURNS = ((0.1, 0.0, 0.2), (0.1, -0.1, 0.05))


def _observations() -> tuple[FeatureObservation, ...]:
    return tuple(
        FeatureObservation(
            stock_code=stock,
            trade_date=day,
            column="close",
            value=_FACTORS[day_index][stock_index],
            first_visible_at=_DECISIONS[day_index] - timedelta(minutes=1),
        )
        for day_index, day in enumerate(_DAYS)
        for stock_index, stock in enumerate(_STOCKS)
    )


def _forward_returns() -> tuple[FactorForwardReturn, ...]:
    from rquant.factor.result import FactorForwardReturn

    return tuple(
        FactorForwardReturn(
            stock_code=stock,
            decision_date=day,
            decision_at=_DECISIONS[day_index],
            return_end_at=_ENDS[day_index],
            value=_RETURNS[day_index][stock_index],
            missing_reason=None,
            first_available_at=_ENDS[day_index],
        )
        for day_index, day in enumerate(_DAYS)
        for stock_index, stock in enumerate(_STOCKS)
    )


def _request(
    *,
    forward_returns: tuple[FactorForwardReturn, ...] | None = None,
    observations: tuple[FeatureObservation, ...] | None = None,
    version: int = 1,
    factor_source_id: str = "feature-snapshot-a",
    return_source_id: str = "adjusted-price-snapshot-a",
    return_price_basis: str = "forward_adjusted",
    as_of: datetime = _AS_OF,
) -> FactorResearchRequest:
    from rquant.factor.result import FactorResearchRequest

    factor_input = FactorTimeSeriesInput(
        definition=build_factor_definition(
            factor_id="price_factor",
            name_zh="价格因子",
            category="technical",
            direction="higher_is_better",
            version=version,
            earliest_available_date=_DAYS[0],
            expression="close",
            feature_catalog=FeatureCatalog(columns=("close",)),
        ),
        universe=_STOCKS,
        trading_days=_DAYS,
        decision_times=tuple(
            DecisionTime(trade_date=day, decision_at=decision)
            for day, decision in zip(_DAYS, _DECISIONS, strict=True)
        ),
        observations=observations if observations is not None else _observations(),
    )
    return FactorResearchRequest(
        factor_input=factor_input,
        forward_returns=forward_returns if forward_returns is not None else _forward_returns(),
        as_of=as_of,
        factor_source_id=factor_source_id,
        return_source_id=return_source_id,
        return_price_basis=return_price_basis,
        rebalance_frequency="daily",
    )


def _missing_return(row: FactorForwardReturn, reason: str = "missing_price") -> FactorForwardReturn:
    return type(row).model_validate(
        {**row.model_dump(), "value": None, "missing_reason": reason, "first_available_at": None}
    )


def test_complete_two_period_result_reuses_ic_and_compounds_research_groups() -> None:
    from rquant.factor import assemble_factor_research_result

    result = assemble_factor_research_result(_request())

    assert (result.factor_id, result.factor_version) == ("price_factor", 1)
    assert result.trading_days == _DAYS
    assert result.universe == _STOCKS
    assert result.factor_source_id == "feature-snapshot-a"
    assert result.return_source_id == "adjusted-price-snapshot-a"
    assert result.return_price_basis == "forward_adjusted"
    assert result.rebalance_frequency == "daily"
    assert [day.coverage.valid_count for day in result.days] == [3, 3]
    assert all(day.coverage.expected_count == 3 for day in result.days)
    assert all(day.status == "evaluated" for day in result.days)
    assert result.days[0].evaluation is not None
    assert result.days[0].evaluation.normal_ic.value == pytest.approx(0.5)
    assert result.days[0].evaluation.rank_ic.value == pytest.approx(0.5)
    assert result.days[1].evaluation is not None
    assert result.days[1].evaluation.rank_ic.value == pytest.approx(1.0)
    assert result.summary_status == "evaluated"
    assert result.ic_summary is not None
    assert result.ic_summary.normal_ic.source_day_count == 2
    assert result.portfolio_status == "available"
    assert result.portfolio_diagnostics is not None
    first, second = result.portfolio_diagnostics.days
    assert [group.period_return for group in first.groupings[0].groups] == pytest.approx(
        [0.1, 0.0, 0.2]
    )
    assert [group.cumulative_return for group in second.groupings[0].groups] == pytest.approx(
        [-0.01, 0.05, 0.32]
    )
    assert [group.target_weight_turnover for group in second.groupings[0].groups] == [1.0] * 3
    assert result == assemble_factor_research_result(_request())
    assert len(result.sha256) == 64
    assert len(result.input_sha256) == 64
    content = json.dumps(
        result.model_dump(mode="json", exclude={"sha256"}),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    assert result.sha256 == hashlib.sha256(content.encode("utf-8")).hexdigest()


def test_partial_pairs_keep_missing_reasons_and_disable_portfolio_curve() -> None:
    from rquant.factor import assemble_factor_research_result

    rows = _forward_returns()
    observations = tuple(
        row.model_copy(update={"value": None})
        if (row.trade_date, row.stock_code) == (_DAYS[0], "C")
        else row
        for row in _observations()
    )
    result = assemble_factor_research_result(
        _request(
            observations=observations,
            forward_returns=rows[:1] + (_missing_return(rows[1]),) + rows[2:],
        )
    )

    first = result.days[0]
    assert (first.coverage.expected_count, first.coverage.valid_count) == (3, 1)
    assert (first.coverage.factor_missing_count, first.coverage.return_missing_count) == (1, 1)
    assert [(item.reason, item.count) for item in first.coverage.factor_missing_by_reason] == [
        ("missing_value", 1)
    ]
    assert [(item.reason, item.count) for item in first.coverage.return_missing_by_reason] == [
        ("missing_price", 1)
    ]
    assert first.evaluation is not None
    assert first.evaluation.normal_ic.status == "insufficient_samples"
    assert first.evaluation.normal_ic.value is None
    assert first.evaluation.effective_sample_count == 1
    assert result.days[1].coverage.valid_count == 3
    assert result.portfolio_status == "insufficient_data"
    assert result.portfolio_diagnostics is None


def test_zero_pair_day_remains_in_coverage_without_invented_ic() -> None:
    from rquant.factor import assemble_factor_research_result

    rows = _forward_returns()
    result = assemble_factor_research_result(
        _request(forward_returns=tuple(_missing_return(row) for row in rows[:3]) + rows[3:])
    )

    assert len(result.days) == 2
    assert result.days[0].status == "no_samples"
    assert result.days[0].coverage.valid_count == 0
    assert result.days[0].evaluation is None
    assert result.days[1].evaluation is not None
    assert result.ic_summary is not None
    assert result.ic_summary.normal_ic.source_day_count == 1
    assert result.portfolio_diagnostics is None


def test_no_valid_pairs_in_entire_range_has_explicit_no_samples_result() -> None:
    from rquant.factor import assemble_factor_research_result

    rows = tuple(_missing_return(row) for row in _forward_returns())
    result = assemble_factor_research_result(_request(forward_returns=rows))

    assert result.summary_status == "no_samples"
    assert result.ic_summary is None
    assert result.portfolio_status == "insufficient_data"
    assert result.portfolio_diagnostics is None
    assert all(day.status == "no_samples" and day.evaluation is None for day in result.days)


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (lambda rows: rows[:-1], "complete"),
        (lambda rows: rows + (rows[0],), "duplicate"),
        (
            lambda rows: (
                rows[:1] + (rows[1].model_copy(update={"stock_code": "OUTSIDE"}),) + rows[2:]
            ),
            "outside",
        ),
        (
            lambda rows: (
                rows[:1]
                + (
                    rows[1].model_copy(
                        update={
                            "decision_date": date(2026, 7, 13),
                            "decision_at": _DECISIONS[0] - timedelta(days=1),
                        }
                    ),
                )
                + rows[2:]
            ),
            "outside",
        ),
        (
            lambda rows: (
                rows[:1]
                + (
                    rows[1].model_copy(
                        update={"decision_at": _DECISIONS[0] + timedelta(minutes=1)}
                    ),
                )
                + rows[2:]
            ),
            "decision_at",
        ),
        (
            lambda rows: (
                rows[:1]
                + (
                    rows[1].model_copy(
                        update={
                            "return_end_at": _ENDS[0] + timedelta(hours=1),
                            "first_available_at": _ENDS[0] + timedelta(hours=1),
                        }
                    ),
                )
                + rows[2:]
            ),
            "same return_end_at",
        ),
        (
            lambda rows: tuple(
                row.model_copy(
                    update={
                        "return_end_at": _DECISIONS[1] + timedelta(minutes=1),
                        "first_available_at": _DECISIONS[1] + timedelta(minutes=1),
                    }
                )
                if row.decision_date == _DAYS[0]
                else row
                for row in rows
            ),
            "overlap",
        ),
    ],
)
def test_request_rejects_incomplete_or_misaligned_return_grid(
    change: Callable[[tuple[FactorForwardReturn, ...]], tuple[FactorForwardReturn, ...]],
    message: str,
) -> None:
    with pytest.raises(ValidationError, match=message):
        _request(forward_returns=change(_forward_returns()))


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"return_end_at": _DECISIONS[0]}, "follow decision_at"),
        ({"value": -1.01}, "-100%"),
        ({"value": float("nan")}, "finite"),
        ({"value": float("inf")}, "finite"),
        ({"value": None, "missing_reason": None}, "exactly one"),
        ({"value": 0.1, "missing_reason": "missing_price"}, "exactly one"),
        ({"value": None, "missing_reason": "bogus", "first_available_at": None}, "Input should be"),
        ({"first_available_at": _ENDS[0] - timedelta(seconds=1)}, "not precede"),
    ],
)
def test_forward_return_rejects_invalid_value_or_state(
    overrides: dict[str, object], message: str
) -> None:
    from rquant.factor.result import FactorForwardReturn

    base = _forward_returns()[0]
    with pytest.raises(ValidationError, match=message):
        FactorForwardReturn.model_validate({**base.model_dump(), **overrides})


def test_request_rejects_future_availability_and_wrong_unfinished_state() -> None:
    rows = _forward_returns()
    future = rows[0].model_copy(update={"first_available_at": _AS_OF + timedelta(seconds=1)})
    with pytest.raises(ValidationError, match="as_of"):
        _request(forward_returns=(future,) + rows[1:])

    unfinished = tuple(
        row.model_copy(
            update={
                "return_end_at": _AS_OF + timedelta(days=1),
                "value": None,
                "missing_reason": "missing_price",
                "first_available_at": None,
            }
        )
        for row in rows[3:]
    )
    with pytest.raises(ValidationError, match="window_unfinished"):
        _request(forward_returns=rows[:3] + unfinished)


@pytest.mark.parametrize("factor_source_id,return_source_id", [("", "returns"), ("feature", " ")])
def test_request_requires_both_trimmed_source_identities(
    factor_source_id: str, return_source_id: str
) -> None:
    with pytest.raises(ValidationError, match="source"):
        _request(factor_source_id=factor_source_id, return_source_id=return_source_id)


def test_content_digest_binds_definition_source_basis_and_return_values() -> None:
    from rquant.factor import assemble_factor_research_result

    base = assemble_factor_research_result(_request())
    changed_rows = _forward_returns()
    alternatives = (
        _request(version=2),
        _request(factor_source_id="feature-snapshot-b"),
        _request(return_source_id="adjusted-price-snapshot-b"),
        _request(return_price_basis="raw"),
        _request(
            forward_returns=(changed_rows[0].model_copy(update={"value": 0.11}),) + changed_rows[1:]
        ),
    )
    assert all(assemble_factor_research_result(item).sha256 != base.sha256 for item in alternatives)
