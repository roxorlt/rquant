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
_WINDOW_DAYS = (
    date(2026, 7, 14),
    date(2026, 7, 15),
    date(2026, 7, 16),
    date(2026, 7, 17),
)
_WINDOW_DECISIONS = tuple(
    datetime(day.year, day.month, day.day, 9, 25, tzinfo=_TZ) for day in _WINDOW_DAYS
)
_WINDOW_ENDS = (
    _WINDOW_DECISIONS[1],
    _WINDOW_DECISIONS[2],
    _WINDOW_DECISIONS[3],
    datetime(2026, 7, 20, 9, 25, tzinfo=_TZ),
)
_WINDOW_CLOSES = ((1.0, 3.0, 5.0), (2.0, 4.0, 6.0), (3.0, 5.0, 7.0), (4.0, 6.0, 8.0))
_WINDOW_RETURNS = ((0.1, 0.2, 0.3), (0.1, 0.2, 0.3), (0.1, 0.2, 0.3), (0.3, 0.2, 0.1))
_VISIBILITY_DAY = date(2026, 7, 17)
_VISIBILITY_DECISION = datetime(2026, 7, 17, 9, 25, tzinfo=_TZ)
_VISIBILITY_END = datetime(2026, 7, 17, 15, tzinfo=_TZ)
_VISIBILITY_READY = datetime(2026, 7, 20, 9, 25, tzinfo=_TZ)


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
    expression: str = "close",
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
            expression=expression,
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


def _partial_group_request(*, second_missing: tuple[str, ...] = ("B",)) -> FactorResearchRequest:
    from rquant.factor.result import FactorForwardReturn, FactorResearchRequest

    base = _request()
    stocks = ("A", "B", "C", "D")
    factors = ((1.0, 2.0, 3.0, None), (1.0, 2.5, 2.0, 3.0))
    returns = ((0.1, 0.2, 0.3, 0.4), (0.05, 0.1, -0.1, 0.2))
    observations = tuple(
        FeatureObservation(
            stock_code=stock,
            trade_date=day,
            column="close",
            value=factors[day_index][stock_index],
            first_visible_at=_DECISIONS[day_index] - timedelta(minutes=1),
        )
        for day_index, day in enumerate(_DAYS)
        for stock_index, stock in enumerate(stocks)
    )
    forward_returns = tuple(
        FactorForwardReturn(
            stock_code=stock,
            decision_date=day,
            decision_at=_DECISIONS[day_index],
            return_end_at=_ENDS[day_index],
            value=None
            if day_index == 1 and stock in second_missing
            else returns[day_index][stock_index],
            missing_reason="missing_price" if day_index == 1 and stock in second_missing else None,
            first_available_at=None
            if day_index == 1 and stock in second_missing
            else _ENDS[day_index],
        )
        for day_index, day in enumerate(_DAYS)
        for stock_index, stock in enumerate(stocks)
    )
    return FactorResearchRequest(
        factor_input=FactorTimeSeriesInput.model_validate(
            base.factor_input.model_copy(update={"universe": stocks, "observations": observations})
        ),
        forward_returns=forward_returns,
        as_of=_AS_OF,
        factor_source_id=base.factor_source_id,
        return_source_id=base.return_source_id,
        return_price_basis=base.return_price_basis,
        holding_sessions=base.holding_sessions,
    )


def _missing_return(row: FactorForwardReturn, reason: str = "missing_price") -> FactorForwardReturn:
    return type(row).model_validate(
        {**row.model_dump(), "value": None, "missing_reason": reason, "first_available_at": None}
    )


def _window_request(
    *,
    evaluation_days: tuple[date, ...] = _WINDOW_DAYS[2:],
    first_a_close: float = 1.0,
    expression: str = "ts_mean(close, 3)",
    holding_sessions: int = 1,
) -> FactorResearchRequest:
    from rquant.factor.result import FactorForwardReturn, FactorResearchRequest

    observations = tuple(
        FeatureObservation(
            stock_code=stock,
            trade_date=day,
            column="close",
            value=first_a_close if (day_index, stock_index) == (0, 0) else value,
            first_visible_at=_WINDOW_DECISIONS[day_index] - timedelta(minutes=1),
        )
        for day_index, (day, closes) in enumerate(zip(_WINDOW_DAYS, _WINDOW_CLOSES, strict=True))
        for stock_index, (stock, value) in enumerate(zip(_STOCKS, closes, strict=True))
    )
    factor_input = FactorTimeSeriesInput(
        definition=build_factor_definition(
            factor_id="rolling_price_factor",
            name_zh="滚动价格因子",
            category="technical",
            direction="higher_is_better",
            version=1,
            earliest_available_date=_WINDOW_DAYS[0],
            expression=expression,
            feature_catalog=FeatureCatalog(columns=("close",)),
        ),
        universe=_STOCKS,
        trading_days=_WINDOW_DAYS,
        decision_times=tuple(
            DecisionTime(trade_date=day, decision_at=decision)
            for day, decision in zip(_WINDOW_DAYS, _WINDOW_DECISIONS, strict=True)
        ),
        observations=observations,
    )
    returns = tuple(
        FactorForwardReturn(
            stock_code=stock,
            decision_date=day,
            decision_at=_WINDOW_DECISIONS[day_index],
            return_end_at=_WINDOW_ENDS[day_index],
            value=_WINDOW_RETURNS[day_index][stock_index],
            missing_reason=None,
            first_available_at=_WINDOW_ENDS[day_index],
        )
        for day_index, day in enumerate(_WINDOW_DAYS)
        if day in evaluation_days
        for stock_index, stock in enumerate(_STOCKS)
    )
    return FactorResearchRequest(
        factor_input=factor_input,
        evaluation_days=evaluation_days,
        forward_returns=returns,
        as_of=_WINDOW_ENDS[-1] + timedelta(hours=1),
        factor_source_id="close-history-a",
        return_source_id="forward-returns-a",
        return_price_basis="forward_adjusted",
        holding_sessions=holding_sessions,
    )


def _visibility_returns(
    *,
    missing_reason: str | None = None,
    first_available_at: datetime | None = _VISIBILITY_READY,
    expected_available_at: datetime | None = None,
) -> tuple[FactorForwardReturn, ...]:
    from rquant.factor.result import FactorForwardReturn

    return tuple(
        FactorForwardReturn(
            stock_code=stock,
            decision_date=_VISIBILITY_DAY,
            decision_at=_VISIBILITY_DECISION,
            return_end_at=_VISIBILITY_END,
            value=None if missing_reason is not None else (index + 1) / 10,
            missing_reason=missing_reason,
            first_available_at=first_available_at,
            expected_available_at=expected_available_at,
        )
        for index, stock in enumerate(_STOCKS)
    )


def _visibility_request(
    *, as_of: datetime, forward_returns: tuple[FactorForwardReturn, ...]
) -> FactorResearchRequest:
    from rquant.factor.result import FactorResearchRequest

    factor_input = FactorTimeSeriesInput(
        definition=build_factor_definition(
            factor_id="visibility_factor",
            name_zh="可见性因子",
            category="technical",
            direction="higher_is_better",
            version=1,
            earliest_available_date=_VISIBILITY_DAY,
            expression="close",
            feature_catalog=FeatureCatalog(columns=("close",)),
        ),
        universe=_STOCKS,
        trading_days=(_VISIBILITY_DAY,),
        decision_times=(
            DecisionTime(trade_date=_VISIBILITY_DAY, decision_at=_VISIBILITY_DECISION),
        ),
        observations=tuple(
            FeatureObservation(
                stock_code=stock,
                trade_date=_VISIBILITY_DAY,
                column="close",
                value=float(index + 1),
                first_visible_at=_VISIBILITY_DECISION - timedelta(minutes=1),
            )
            for index, stock in enumerate(_STOCKS)
        ),
    )
    return FactorResearchRequest(
        factor_input=factor_input,
        forward_returns=forward_returns,
        as_of=as_of,
        factor_source_id="friday-feature-snapshot",
        return_source_id="friday-price-snapshot",
        return_price_basis="forward_adjusted",
        holding_sessions=1,
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
    assert result.holding_sessions == 1
    assert "rebalance_frequency" not in result.model_dump()
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


def test_result_carries_required_decay_from_the_same_research_request() -> None:
    from rquant.factor import assemble_factor_research_result
    from rquant.factor.decay import FactorICDecayResult
    from rquant.factor.result import FactorResearchResult

    request = _request()
    result = assemble_factor_research_result(request)
    decay = result.ic_decay

    assert isinstance(decay, FactorICDecayResult)
    assert FactorResearchResult.model_fields["ic_decay"].is_required()
    assert (decay.factor_id, decay.factor_version) == (result.factor_id, result.factor_version)
    assert (decay.factor_source_id, decay.return_source_id) == (
        result.factor_source_id,
        result.return_source_id,
    )
    assert (decay.return_price_basis, decay.holding_sessions) == (
        result.return_price_basis,
        result.holding_sessions,
    )
    assert (decay.universe, decay.evaluation_days, decay.as_of) == (
        result.universe,
        result.trading_days,
        result.as_of,
    )
    assert len(decay.input_sha256) == 64
    assert [(day.normal_ic, day.rank_ic) for day in decay.periods[0].days] == [
        (day.evaluation.normal_ic, day.evaluation.rank_ic) for day in result.days
    ]
    assert decay.periods[1].days[0].base_date == _DAYS[0]
    assert decay.periods[1].days[0].target_date == _DAYS[1]
    assert decay.periods[1].days[0].rank_ic.value == pytest.approx(-0.5)
    assert decay.periods[2].status == "no_target_period"
    assert decay.periods[2].ic_summary is None
    assert result == assemble_factor_research_result(request)
    assert "ic_decay" in result.model_dump()
    with pytest.raises(ValidationError, match="ic_decay"):
        FactorResearchResult.model_validate(result.model_dump(exclude={"ic_decay"}))


def test_reordered_research_facts_keep_one_input_identity_and_content_digest() -> None:
    from rquant.factor import assemble_factor_research_result
    from rquant.factor.result import FactorResearchRequest

    request = _request()
    reordered = FactorResearchRequest.model_validate(
        request.model_copy(
            update={
                "factor_input": request.factor_input.model_copy(
                    update={"observations": tuple(reversed(request.factor_input.observations))}
                ),
                "forward_returns": tuple(reversed(request.forward_returns)),
            }
        )
    )
    original_result = assemble_factor_research_result(request)
    reordered_result = assemble_factor_research_result(reordered)

    assert original_result.input_sha256 == original_result.ic_decay.input_sha256
    assert reordered_result.input_sha256 == reordered_result.ic_decay.input_sha256
    assert reordered_result.input_sha256 == original_result.input_sha256
    assert reordered_result.sha256 == original_result.sha256
    assert reordered_result == original_result


def test_public_request_identity_canonicalizes_all_fact_row_types() -> None:
    from rquant.factor import (
        IndustryObservation,
        MarketCapObservation,
        factor_research_request_sha256,
    )
    from rquant.factor.result import FactorResearchRequest

    request = _request()
    industries = (
        IndustryObservation(
            stock_code="A",
            trade_date=_DAYS[0],
            industry="科技",
            first_visible_at=_DECISIONS[0] - timedelta(minutes=1),
        ),
        IndustryObservation(
            stock_code="B",
            trade_date=_DAYS[1],
            industry="工业",
            first_visible_at=_DECISIONS[1] - timedelta(minutes=1),
        ),
    )
    market_caps = (
        MarketCapObservation(
            stock_code="A",
            trade_date=_DAYS[0],
            market_cap=100.0,
            first_visible_at=_DECISIONS[0] - timedelta(minutes=1),
        ),
        MarketCapObservation(
            stock_code="B",
            trade_date=_DAYS[1],
            market_cap=200.0,
            first_visible_at=_DECISIONS[1] - timedelta(minutes=1),
        ),
    )
    with_context = FactorResearchRequest.model_validate(
        request.model_copy(
            update={
                "factor_input": request.factor_input.model_copy(
                    update={
                        "industry_observations": industries,
                        "market_cap_observations": market_caps,
                    }
                )
            }
        )
    )
    reordered = FactorResearchRequest.model_validate(
        with_context.model_copy(
            update={
                "factor_input": with_context.factor_input.model_copy(
                    update={
                        "observations": tuple(reversed(with_context.factor_input.observations)),
                        "industry_observations": tuple(reversed(industries)),
                        "market_cap_observations": tuple(reversed(market_caps)),
                    }
                ),
                "forward_returns": tuple(reversed(with_context.forward_returns)),
            }
        )
    )

    assert factor_research_request_sha256(with_context) == factor_research_request_sha256(reordered)


def test_later_decay_and_content_digest_follow_target_return_facts() -> None:
    from rquant.factor import assemble_factor_research_result

    original_rows = _forward_returns()
    changed_rows = (
        original_rows[:3]
        + (original_rows[3].model_copy(update={"value": -0.2}),)
        + original_rows[4:]
    )
    original = assemble_factor_research_result(_request())
    changed = assemble_factor_research_result(_request(forward_returns=changed_rows))

    assert original.days[0].evaluation == changed.days[0].evaluation
    assert original.ic_decay.periods[1].days[0].rank_ic.value == pytest.approx(-0.5)
    assert changed.ic_decay.periods[1].days[0].rank_ic.value == pytest.approx(1.0)
    assert original.sha256 != changed.sha256


def test_decay_preserves_missing_and_unmatured_return_states() -> None:
    from rquant.factor import assemble_factor_research_result

    all_missing = assemble_factor_research_result(
        _request(forward_returns=tuple(_missing_return(row) for row in _forward_returns()))
    )
    assert all_missing.summary_status == "no_samples"
    assert all_missing.ic_decay.periods[0].status == "no_valid_days"
    assert all_missing.ic_decay.periods[0].valid_pair_count == 0
    assert all_missing.ic_decay.periods[0].ic_summary.normal_ic.mean is None
    assert all_missing.ic_decay.periods[2].status == "no_target_period"

    unfinished = assemble_factor_research_result(
        _visibility_request(
            as_of=_VISIBILITY_END - timedelta(minutes=1),
            forward_returns=_visibility_returns(
                missing_reason="window_unfinished", first_available_at=None
            ),
        )
    )
    assert unfinished.ic_decay.periods[0].status == "no_valid_days"
    assert unfinished.ic_decay.periods[0].valid_pair_count == 0
    assert unfinished.ic_decay.periods[0].days[0].normal_ic.value is None
    assert unfinished.ic_decay.periods[1].status == "no_target_period"


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


def test_three_valid_pairs_each_day_keep_partial_group_diagnostics() -> None:
    from rquant.factor import assemble_factor_research_result
    from rquant.factor.result import FactorResearchRequest

    request = _partial_group_request()
    result = assemble_factor_research_result(request)

    assert result.summary_status == "evaluated"
    assert result.ic_summary is not None
    assert result.portfolio_status == "available_partial"
    assert [(day.coverage.expected_count, day.coverage.valid_count) for day in result.days] == [
        (4, 3),
        (4, 3),
    ]
    assert [
        (row.reason, row.count) for row in result.days[0].coverage.factor_missing_by_reason
    ] == [("missing_value", 1)]
    assert [
        (row.reason, row.count) for row in result.days[1].coverage.return_missing_by_reason
    ] == [("missing_price", 1)]
    assert result.portfolio_diagnostics is not None
    first, second = result.portfolio_diagnostics.days
    assert [day.source_sample_count for day in (first, second)] == [3, 3]
    assert [point.period_return for point in first.groupings[0].groups] == pytest.approx(
        [0.1, 0.2, 0.3]
    )
    assert [point.period_return for point in second.groupings[0].groups] == pytest.approx(
        [0.05, -0.1, 0.2]
    )
    assert [point.cumulative_return for point in second.groupings[0].groups] == pytest.approx(
        [0.155, 0.08, 0.56]
    )
    assert [point.target_weight_turnover for point in second.groupings[0].groups] == [0, 1, 1]
    assert all(group.status == "insufficient_samples" for group in first.groupings[1:])
    assert all(group.status == "insufficient_samples" for group in second.groupings[1:])
    content = json.dumps(
        result.model_dump(mode="json", exclude={"sha256"}),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    assert result.sha256 == hashlib.sha256(content.encode("utf-8")).hexdigest()

    reordered = FactorResearchRequest.model_validate(
        request.model_copy(
            update={
                "factor_input": request.factor_input.model_copy(
                    update={"observations": tuple(reversed(request.factor_input.observations))}
                ),
                "forward_returns": tuple(reversed(request.forward_returns)),
            }
        )
    )
    repeated = assemble_factor_research_result(reordered)
    assert repeated.input_sha256 == result.input_sha256
    assert repeated.sha256 == result.sha256
    assert repeated == result


def test_one_day_with_only_two_valid_pairs_keeps_entire_curve_unavailable() -> None:
    from rquant.factor import assemble_factor_research_result

    result = assemble_factor_research_result(_partial_group_request(second_missing=("B", "D")))

    assert [day.coverage.valid_count for day in result.days] == [3, 2]
    assert result.days[1].evaluation is not None
    assert result.days[1].evaluation.groupings[0].status == "insufficient_samples"
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


def test_legal_return_spread_below_minus_100_percent_keeps_complete_result() -> None:
    from rquant.factor import assemble_factor_research_result

    rows = _forward_returns()
    altered = (
        rows[0].model_copy(update={"value": 1.0}),
        rows[1],
        rows[2].model_copy(update={"value": -0.2}),
    ) + rows[3:]

    result = assemble_factor_research_result(_request(forward_returns=altered))

    assert [day.coverage.valid_count for day in result.days] == [3, 3]
    assert result.ic_summary is not None
    assert result.portfolio_status == "available"
    assert result.portfolio_diagnostics is not None
    first, second = result.portfolio_diagnostics.days
    first_three = first.groupings[0]
    second_three = second.groupings[0]
    assert [group.cumulative_return for group in first_three.groups] == pytest.approx(
        [1.0, 0.0, -0.2]
    )
    assert first_three.long_short_return == pytest.approx(-1.2)
    assert first_three.long_short_cumulative_spread == pytest.approx(-1.2)
    assert second_three.long_short_return == pytest.approx(0.2)
    assert second_three.long_short_cumulative_spread == pytest.approx(-0.92)


def test_result_carries_full_definition_when_expression_changes_under_same_identity() -> None:
    from rquant.factor import assemble_factor_research_result

    original = assemble_factor_research_result(_request())
    changed = assemble_factor_research_result(_request(expression="close * 2"))

    assert original.factor_id == changed.factor_id == "price_factor"
    assert original.factor_version == changed.factor_version == 1
    assert original.factor_source_id == changed.factor_source_id
    assert original.definition.expression == "close"
    assert changed.definition.expression == "close * 2"
    assert changed.definition.dependency_columns == ("close",)
    assert changed.definition.feature_catalog.columns == ("close",)
    assert changed.sha256 != original.sha256


def test_warmup_history_feeds_first_evaluation_day_without_return_rows() -> None:
    from rquant.factor import assemble_factor_research_result, evaluate_factor_time_series

    request = _window_request()
    factors = evaluate_factor_time_series(request.factor_input)
    first_values = [point.value for point in factors.values if point.trade_date == _WINDOW_DAYS[2]]
    result = assemble_factor_research_result(request)

    assert first_values == pytest.approx([2.0, 4.0, 6.0])
    assert len(request.forward_returns) == 2 * len(_STOCKS)
    assert result.trading_days == _WINDOW_DAYS[2:]
    assert [day.decision_date for day in result.days] == list(_WINDOW_DAYS[2:])
    assert [day.coverage.valid_count for day in result.days] == [3, 3]
    assert all(day.coverage.expected_count == 3 for day in result.days)
    assert result.days[0].evaluation is not None
    assert result.days[0].evaluation.normal_ic.value == pytest.approx(1.0)
    assert result.days[1].evaluation is not None
    assert result.days[1].evaluation.normal_ic.value == pytest.approx(-1.0)
    assert result.ic_summary is not None
    assert result.ic_summary.normal_ic.source_day_count == 2
    assert result.portfolio_status == "available"
    assert result.portfolio_diagnostics is not None
    first, second = result.portfolio_diagnostics.days
    assert [day.decision_date for day in result.portfolio_diagnostics.days] == list(
        _WINDOW_DAYS[2:]
    )
    assert [group.period_return for group in first.groupings[0].groups] == pytest.approx(
        [0.1, 0.2, 0.3]
    )
    assert [group.cumulative_return for group in second.groupings[0].groups] == pytest.approx(
        [0.43, 0.44, 0.43]
    )
    assert result == assemble_factor_research_result(request)


def test_changing_warmup_history_changes_first_evaluation_and_digest() -> None:
    from rquant.factor import assemble_factor_research_result, evaluate_factor_time_series

    original_request = _window_request()
    altered_request = _window_request(first_a_close=10.0)
    original = assemble_factor_research_result(original_request)
    altered = assemble_factor_research_result(altered_request)
    altered_values = evaluate_factor_time_series(altered_request.factor_input).values

    assert next(
        point.value
        for point in altered_values
        if (point.trade_date, point.stock_code) == (_WINDOW_DAYS[2], "A")
    ) == pytest.approx(5.0)
    assert original.days[0].evaluation is not None
    assert altered.days[0].evaluation is not None
    assert original.days[0].evaluation.rank_ic.value != altered.days[0].evaluation.rank_ic.value
    assert original.input_sha256 != altered.input_sha256
    assert original.sha256 != altered.sha256


def test_calculation_calendar_may_contain_gap_between_evaluation_days() -> None:
    from rquant.factor import assemble_factor_research_result

    evaluation_days = (_WINDOW_DAYS[0], _WINDOW_DAYS[2])
    request = _window_request(evaluation_days=evaluation_days, expression="close")
    result = assemble_factor_research_result(request)

    assert len(request.forward_returns) == 2 * len(_STOCKS)
    assert result.trading_days == evaluation_days
    assert [day.decision_date for day in result.days] == list(evaluation_days)
    assert [day.coverage.valid_count for day in result.days] == [3, 3]
    assert result.portfolio_status == "available"
    assert (
        result.input_sha256
        != assemble_factor_research_result(_window_request(expression="close")).input_sha256
    )


@pytest.mark.parametrize(
    ("evaluation_days", "message"),
    [
        ((), "nonempty"),
        ((_WINDOW_DAYS[2], _WINDOW_DAYS[2]), "ascend"),
        ((_WINDOW_DAYS[3], _WINDOW_DAYS[2]), "ascend"),
        ((date(2026, 7, 13),), "subset"),
    ],
)
def test_request_rejects_invalid_evaluation_days(
    evaluation_days: tuple[date, ...], message: str
) -> None:
    from rquant.factor.result import FactorResearchRequest

    with pytest.raises(ValidationError, match=message):
        FactorResearchRequest.model_validate(
            {**_window_request().model_dump(), "evaluation_days": evaluation_days}
        )


def test_evaluation_return_grid_rejects_missing_and_warmup_rows() -> None:
    from rquant.factor.result import FactorForwardReturn, FactorResearchRequest

    request = _window_request()
    rows = request.forward_returns
    with pytest.raises(ValidationError, match="complete"):
        FactorResearchRequest.model_validate({**request.model_dump(), "forward_returns": rows[:-1]})

    warmup_row = FactorForwardReturn(
        stock_code="A",
        decision_date=_WINDOW_DAYS[0],
        decision_at=_WINDOW_DECISIONS[0],
        return_end_at=_WINDOW_ENDS[0],
        value=0.1,
        missing_reason=None,
        first_available_at=_WINDOW_ENDS[0],
    )
    with pytest.raises(ValidationError, match="outside requested grid"):
        FactorResearchRequest.model_validate(
            {**request.model_dump(), "forward_returns": rows + (warmup_row,)}
        )


def test_evaluation_return_windows_remain_nonoverlapping() -> None:
    from rquant.factor.result import FactorResearchRequest

    request = _window_request()
    rows = tuple(
        row.model_copy(
            update={
                "return_end_at": _WINDOW_DECISIONS[3] + timedelta(minutes=1),
                "first_available_at": _WINDOW_DECISIONS[3] + timedelta(minutes=1),
            }
        )
        if row.decision_date == _WINDOW_DAYS[2]
        else row
        for row in request.forward_returns
    )
    with pytest.raises(ValidationError, match="overlap"):
        FactorResearchRequest.model_validate({**request.model_dump(), "forward_returns": rows})


def test_as_of_still_covers_the_full_calculation_calendar() -> None:
    from rquant.factor.result import FactorResearchRequest

    request = _window_request(evaluation_days=(_WINDOW_DAYS[0],), expression="close")
    with pytest.raises(ValidationError, match="as_of"):
        FactorResearchRequest.model_validate(
            {**request.model_dump(), "as_of": _WINDOW_DECISIONS[2] + timedelta(hours=1)}
        )


@pytest.mark.parametrize("holding_sessions", [1, 5, 10, 20])
def test_holding_sessions_are_exact_and_saved_in_request_and_result(holding_sessions: int) -> None:
    from rquant.factor import assemble_factor_research_result

    request = _window_request(holding_sessions=holding_sessions)
    result = assemble_factor_research_result(request)

    assert request.holding_sessions == holding_sessions
    assert result.holding_sessions == holding_sessions
    assert result.model_dump()["holding_sessions"] == holding_sessions
    assert "rebalance_frequency" not in result.model_dump()
    if holding_sessions != 1:
        assert (
            result.input_sha256 != assemble_factor_research_result(_window_request()).input_sha256
        )


@pytest.mark.parametrize("holding_sessions", [0, 2, 6, 21, "weekly"])
def test_holding_sessions_reject_unsupported_values(holding_sessions: int | str) -> None:
    from rquant.factor.result import FactorResearchRequest

    with pytest.raises(ValidationError, match="holding_sessions"):
        FactorResearchRequest.model_validate(
            {**_window_request().model_dump(), "holding_sessions": holding_sessions}
        )


def test_legacy_daily_input_maps_to_one_session_without_a_frequency_label() -> None:
    from rquant.factor import assemble_factor_research_result

    request = _request()
    result = assemble_factor_research_result(request)

    assert request.evaluation_days is None
    assert request.holding_sessions == result.holding_sessions == 1
    assert "rebalance_frequency" not in request.model_dump()
    assert "rebalance_frequency" not in result.model_dump()


@pytest.mark.parametrize("frequency", ["weekly", "monthly"])
def test_legacy_calendar_labels_cannot_claim_exact_sessions(frequency: str) -> None:
    from rquant.factor.result import FactorResearchRequest

    legacy_data = _request().model_dump()
    legacy_data.pop("holding_sessions")
    legacy_data["rebalance_frequency"] = frequency
    with pytest.raises(ValidationError, match="exact holding_sessions"):
        FactorResearchRequest.model_validate(legacy_data)


@pytest.mark.parametrize(
    "as_of",
    [
        _VISIBILITY_END,
        datetime(2026, 7, 18, 12, tzinfo=_TZ),
        _VISIBILITY_READY - timedelta(minutes=1),
    ],
)
def test_completed_friday_window_remains_pending_until_monday_0925(as_of: datetime) -> None:
    from rquant.factor import assemble_factor_research_result

    rows = _visibility_returns(
        missing_reason="visibility_pending",
        first_available_at=None,
        expected_available_at=_VISIBILITY_READY,
    )
    result = assemble_factor_research_result(_visibility_request(as_of=as_of, forward_returns=rows))

    assert result.days[0].status == "no_samples"
    assert result.days[0].coverage.expected_count == len(_STOCKS)
    assert result.days[0].coverage.valid_count == 0
    assert result.days[0].coverage.factor_missing_count == 0
    assert result.days[0].coverage.return_missing_count == len(_STOCKS)
    assert [
        (item.reason, item.count) for item in result.days[0].coverage.return_missing_by_reason
    ] == [("visibility_pending", len(_STOCKS))]
    assert result.summary_status == "no_samples"
    assert result.ic_summary is None
    assert result.portfolio_status == "insufficient_data"
    assert result.portfolio_diagnostics is None
    assert (
        result.input_sha256
        != assemble_factor_research_result(
            _visibility_request(
                as_of=as_of,
                forward_returns=_visibility_returns(
                    missing_reason="visibility_pending",
                    first_available_at=None,
                    expected_available_at=_VISIBILITY_READY + timedelta(minutes=1),
                ),
            )
        ).input_sha256
    )


def test_pending_is_rejected_at_expected_visibility_time() -> None:
    from rquant.factor.result import FactorResearchRequest

    rows = _visibility_returns(
        missing_reason="visibility_pending",
        first_available_at=None,
        expected_available_at=_VISIBILITY_READY,
    )
    request = _visibility_request(
        as_of=_VISIBILITY_READY - timedelta(minutes=1), forward_returns=rows
    )
    with pytest.raises(ValidationError, match="visibility_pending"):
        FactorResearchRequest.model_validate({**request.model_dump(), "as_of": _VISIBILITY_READY})


def test_visible_return_enters_ic_and_groups_at_monday_0925() -> None:
    from rquant.factor import assemble_factor_research_result

    rows = _visibility_returns()
    result = assemble_factor_research_result(
        _visibility_request(as_of=_VISIBILITY_READY, forward_returns=rows)
    )

    assert result.days[0].coverage.valid_count == len(_STOCKS)
    assert result.days[0].evaluation is not None
    assert result.days[0].evaluation.normal_ic.value == pytest.approx(1.0)
    assert result.portfolio_status == "available"
    assert result.portfolio_diagnostics is not None
    assert [
        group.period_return for group in result.portfolio_diagnostics.days[0].groupings[0].groups
    ] == pytest.approx([0.1, 0.2, 0.3])


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        (
            {"value": None, "missing_reason": "visibility_pending", "first_available_at": None},
            "expected_available_at",
        ),
        (
            {
                "value": None,
                "missing_reason": "visibility_pending",
                "first_available_at": None,
                "expected_available_at": _VISIBILITY_END,
            },
            "return_end_at",
        ),
        ({"expected_available_at": _VISIBILITY_READY}, "only.*visibility_pending"),
        (
            {
                "value": None,
                "missing_reason": "source_unavailable",
                "first_available_at": None,
                "expected_available_at": _VISIBILITY_READY,
            },
            "only.*visibility_pending",
        ),
    ],
)
def test_expected_availability_is_exclusive_to_valid_pending_returns(
    overrides: dict[str, object], message: str
) -> None:
    from rquant.factor.result import FactorForwardReturn

    base = _visibility_returns()[0]
    with pytest.raises(ValidationError, match=message):
        FactorForwardReturn.model_validate({**base.model_dump(), **overrides})


def test_pending_requires_the_price_window_to_have_ended() -> None:
    from rquant.factor.result import FactorResearchRequest

    rows = _visibility_returns(
        missing_reason="visibility_pending",
        first_available_at=None,
        expected_available_at=_VISIBILITY_READY,
    )
    request = _visibility_request(as_of=_VISIBILITY_END, forward_returns=rows)
    with pytest.raises(ValidationError, match="window_unfinished"):
        FactorResearchRequest.model_validate(
            {**request.model_dump(), "as_of": _VISIBILITY_END - timedelta(minutes=1)}
        )


def test_present_return_cannot_arrive_before_its_first_available_at() -> None:
    from rquant.factor.result import FactorResearchRequest

    request = _visibility_request(as_of=_VISIBILITY_READY, forward_returns=_visibility_returns())
    with pytest.raises(ValidationError, match="as_of"):
        FactorResearchRequest.model_validate(
            {**request.model_dump(), "as_of": _VISIBILITY_READY - timedelta(minutes=1)}
        )


def test_window_unfinished_before_friday_close_remains_valid() -> None:
    from rquant.factor import assemble_factor_research_result
    from rquant.factor.result import FactorResearchRequest

    request = _visibility_request(
        as_of=_VISIBILITY_END - timedelta(minutes=1),
        forward_returns=_visibility_returns(
            missing_reason="window_unfinished", first_available_at=None
        ),
    )
    result = assemble_factor_research_result(request)
    assert result.days[0].coverage.return_missing_by_reason[0].reason == "window_unfinished"
    with pytest.raises(ValidationError, match="window_unfinished"):
        FactorResearchRequest.model_validate({**request.model_dump(), "as_of": _VISIBILITY_END})


@pytest.mark.parametrize("missing_reason", ["missing_price", "suspended", "source_unavailable"])
def test_real_missing_return_remains_valid_after_visibility_time(missing_reason: str) -> None:
    from rquant.factor import assemble_factor_research_result

    result = assemble_factor_research_result(
        _visibility_request(
            as_of=_VISIBILITY_READY,
            forward_returns=_visibility_returns(
                missing_reason=missing_reason, first_available_at=None
            ),
        )
    )
    assert result.days[0].coverage.return_missing_by_reason[0].reason == missing_reason
