"""Whole-day stream statistics preserve batch mathematics and bounded ownership."""

from __future__ import annotations

import tracemalloc
import weakref
from collections.abc import Iterable, Iterator
from datetime import date, datetime, time, timedelta, timezone
from typing import TYPE_CHECKING

import pytest
from pydantic import ValidationError

if TYPE_CHECKING:
    from rquant.factor.daily_stream import FactorDailyStreamBatch, FactorDailyStreamRequest
    from rquant.factor.evaluate import FactorDirection, FactorSample
    from rquant.factor.result import HoldingSessions

_TZ = timezone(timedelta(hours=8))
_FIRST = date(2026, 7, 1)
_AS_OF = datetime(2026, 9, 30, 16, tzinfo=_TZ)


def _at(day: date) -> datetime:
    return datetime.combine(day, time(9, 30), _TZ)


def _request(
    days: tuple[date, ...],
    *,
    direction: FactorDirection = "higher_is_better",
    expression: str = "close",
    holding_sessions: HoldingSessions = 1,
    as_of: datetime = _AS_OF,
) -> FactorDailyStreamRequest:
    from rquant.factor.daily_stream import FactorDailyStreamRequest, FactorDailyStreamSources
    from rquant.factor.definition import build_factor_definition
    from rquant.factor.expression import FeatureCatalog

    return FactorDailyStreamRequest(
        definition=build_factor_definition(
            factor_id="synthetic_daily_stream",
            name_zh="逐日合成因子",
            category="technical",
            direction=direction,
            version=1,
            earliest_available_date=_FIRST,
            expression=expression,
            feature_catalog=FeatureCatalog(columns=("close",)),
        ),
        selection="all",
        evaluation_days=days,
        as_of=as_of,
        sources=FactorDailyStreamSources(
            source_mode="historical_retrospective",
            universe_source_id="synthetic-daily-a-share-pools",
            universe_source_sha256="a" * 64,
            factor_source_id="synthetic-factor-values",
            factor_source_sha256="b" * 64,
            return_source_id="synthetic-completed-returns",
            return_source_sha256="c" * 64,
        ),
        return_price_basis="raw",
        holding_sessions=holding_sessions,
    )


def _batch(
    request: FactorDailyStreamRequest,
    day: date,
    codes: tuple[str, ...],
    factors: tuple[float | None, ...] | None = None,
    returns: tuple[float | None, ...] | None = None,
) -> FactorDailyStreamBatch:
    from rquant.factor.daily_stream import (
        FactorDailyStreamBatch,
        factor_daily_stream_request_sha256,
    )
    from rquant.factor.result import FactorForwardReturn
    from rquant.factor.time_series import FactorTimeSeriesValue
    from rquant.factor.universe import FactorUniverseExclusions, FactorUniverseResult
    from rquant.runtime_contracts import canonical_sha256

    factors = factors if factors is not None else tuple(float(index) for index in range(len(codes)))
    returns = returns if returns is not None else tuple(index / 100 for index in range(len(codes)))
    decision_at = _at(day)
    end_at = decision_at + timedelta(days=request.holding_sessions)
    observed_at = request.as_of - timedelta(hours=1)
    pool = FactorUniverseResult(
        selection=request.selection,
        trade_date=day,
        source_mode="historical_retrospective",
        stock_codes=tuple(sorted(codes)),
        security_count=len(codes),
        input_count=len(codes),
        selected_count=len(codes),
        excluded=FactorUniverseExclusions(),
        security_observed_at=observed_at,
        index_observed_at=observed_at if request.selection in ("hs300", "zz1000") else None,
        input_sha256=canonical_sha256((request.selection, day, tuple(sorted(codes)))),
    )
    return FactorDailyStreamBatch(
        request_sha256=factor_daily_stream_request_sha256(request),
        sources=request.sources,
        universe=pool,
        decision_at=decision_at,
        return_end_at=end_at,
        factor_values=tuple(
            FactorTimeSeriesValue(
                stock_code=code,
                trade_date=day,
                value=value,
                missing_reason="missing_observation" if value is None else None,
                latest_visible_at=None if value is None else decision_at - timedelta(minutes=1),
            )
            for code, value in zip(codes, factors, strict=True)
        ),
        forward_returns=tuple(
            FactorForwardReturn(
                stock_code=code,
                decision_date=day,
                decision_at=decision_at,
                return_end_at=end_at,
                value=value,
                missing_reason="missing_price" if value is None else None,
                first_available_at=None if value is None else end_at,
            )
            for code, value in zip(codes, returns, strict=True)
        ),
    )


def _known_batches(
    request: FactorDailyStreamRequest, *, ties: bool = False
) -> tuple[FactorDailyStreamBatch, ...]:
    codes = tuple(f"{index:06d}.SZ" for index in range(11))
    return tuple(
        _batch(
            request,
            day,
            tuple(reversed(codes)),
            tuple(float((index + offset) // 3 if ties else index + offset) for index in range(11)),
            tuple((index * (offset + 1) % 7 - 3) / 100 for index in range(11)),
        )
        for offset, day in enumerate(request.evaluation_days)
    )


def _samples(batches: Iterable[FactorDailyStreamBatch]) -> tuple[FactorSample, ...]:
    from rquant.factor.evaluate import FactorSample

    samples: list[FactorSample] = []
    for batch in batches:
        returned = {row.stock_code: row for row in batch.forward_returns}
        for factor in batch.factor_values:
            row = returned[factor.stock_code]
            assert factor.value is not None and row.value is not None
            samples.append(
                FactorSample(
                    stock_code=factor.stock_code,
                    decision_at=batch.decision_at,
                    factor_visible_at=factor.latest_visible_at or batch.decision_at,
                    factor_value=factor.value,
                    return_end_at=batch.return_end_at,
                    forward_return=row.value,
                )
            )
    return tuple(samples)


@pytest.mark.parametrize("direction", ["higher_is_better", "lower_is_better"])
@pytest.mark.parametrize("ties", [False, True])
def test_complete_known_stream_matches_batch_ic_groups_portfolios_and_summary(
    direction: FactorDirection, ties: bool
) -> None:
    from rquant.factor.daily_stream import evaluate_factor_daily_stream
    from rquant.factor.evaluate import FactorEvaluationInput, evaluate_factor
    from rquant.factor.portfolio import evaluate_factor_portfolios
    from rquant.factor.summary import summarize_factor_ic

    request = _request(
        tuple(_FIRST + timedelta(days=index) for index in range(3)), direction=direction
    )
    batches = _known_batches(request, ties=ties)
    reference = FactorEvaluationInput(
        universe=batches[0].universe.stock_codes,
        as_of=request.as_of,
        direction=direction,
        samples=_samples(batches),
    )
    evaluation = evaluate_factor(reference)
    portfolios = evaluate_factor_portfolios(reference)
    result = evaluate_factor_daily_stream(request, iter(batches))

    assert result.request == request
    assert result.ic_summary == summarize_factor_ic(evaluation)
    for actual, expected_ic, expected_portfolio in zip(
        result.days, evaluation.days, portfolios.days, strict=True
    ):
        assert actual.status == "complete"
        assert actual.coverage.expected_count == actual.coverage.valid_count == 11
        assert actual.evaluation == expected_ic
        assert actual.decision_at == expected_portfolio.decision_at
        assert actual.return_end_at == expected_portfolio.return_end_at
        for grouping, expected in zip(
            actual.portfolio_groupings, expected_portfolio.groupings, strict=True
        ):
            assert grouping.model_dump(exclude={"cumulative_status"}) == expected.model_dump()
            assert grouping.cumulative_status == "available"
    assert len(result.batch_sha256s) == 3
    assert len(result.input_sha256) == len(result.sha256) == 64


def test_stream_source_is_iterated_once_and_never_materialized() -> None:
    from rquant.factor.daily_stream import evaluate_factor_daily_stream

    request = _request((_FIRST, _FIRST + timedelta(days=1)))

    class OneShot:
        def __init__(self) -> None:
            self.calls = 0

        def __iter__(self) -> Iterator[FactorDailyStreamBatch]:
            self.calls += 1
            assert self.calls == 1, "source was iterated twice"
            for day in request.evaluation_days:
                yield _batch(request, day, ("000001.SZ", "000002.SZ", "000003.SZ"))

        def __len__(self) -> int:
            raise AssertionError("source was materialized or length inspected")

    source = OneShot()
    result = evaluate_factor_daily_stream(request, source)
    assert source.calls == 1
    assert len(result.days) == 2


def test_daily_membership_change_has_exact_turnover_and_compounding() -> None:
    from rquant.factor.daily_stream import evaluate_factor_daily_stream

    request = _request((_FIRST, _FIRST + timedelta(days=1)))
    first = _batch(
        request,
        _FIRST,
        ("000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ"),
        (1.0, 2.0, 3.0, 4.0),
        (0.1, 0.2, 0.3, 0.4),
    )
    second = _batch(
        request,
        request.evaluation_days[1],
        ("000001.SZ", "000003.SZ", "000005.SZ", "000004.SZ"),
        (1.0, 2.0, 3.0, 4.0),
        (0.2, 0.0, -0.1, 0.1),
    )
    early, later = evaluate_factor_daily_stream(request, (first, second)).days
    groups = later.portfolio_groupings[0].groups
    assert [point.period_return for point in groups] == pytest.approx([0.1, -0.1, 0.1])
    assert [point.cumulative_return for point in groups] == pytest.approx([0.265, 0.17, 0.54])
    assert [point.target_weight_turnover for point in groups] == pytest.approx([0.5, 1.0, 0.0])
    assert all(
        point.target_weight_turnover is None for point in early.portfolio_groupings[0].groups
    )
    assert later.portfolio_groupings[0].long_short_return == pytest.approx(0.0)
    assert later.portfolio_groupings[0].long_short_cumulative_spread == pytest.approx(0.275)


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ("missing", "missing_batch"),
        ("extra", "unexpected_batch"),
        ("reversed", "date_order_mismatch"),
        ("duplicate", "date_order_mismatch"),
    ],
)
def test_stream_day_schedule_must_match_exactly(change: str, reason: str) -> None:
    from rquant.factor.daily_stream import evaluate_factor_daily_stream

    request = _request((_FIRST, _FIRST + timedelta(days=1)))
    batches = _known_batches(request)
    altered = {
        "missing": batches[:1],
        "extra": batches + (_batch(request, _FIRST + timedelta(days=2), ("000001.SZ",)),),
        "reversed": tuple(reversed(batches)),
        "duplicate": batches[:1] * 2,
    }[change]
    with pytest.raises(ValueError, match=reason):
        evaluate_factor_daily_stream(request, iter(altered))


def test_iterator_failure_is_not_returned_as_a_completed_result() -> None:
    from rquant.factor.daily_stream import evaluate_factor_daily_stream

    request = _request((_FIRST, _FIRST + timedelta(days=1)))

    def source() -> Iterator[FactorDailyStreamBatch]:
        yield _batch(request, _FIRST, ("000001.SZ",))
        raise RuntimeError("synthetic-source-failed")

    with pytest.raises(RuntimeError, match="synthetic-source-failed"):
        evaluate_factor_daily_stream(request, source())


@pytest.mark.parametrize("days", [(_FIRST, _FIRST), (_FIRST + timedelta(days=1), _FIRST)])
def test_request_schedule_must_ascend_without_duplicates(days: tuple[date, ...]) -> None:
    with pytest.raises(ValidationError) as caught:
        _request(days)
    assert caught.value.errors()[0]["type"] == "factor_daily_stream_invalid_schedule"


@pytest.mark.parametrize("day_count", [0, 1_025])
def test_request_schedule_is_bounded(day_count: int) -> None:
    with pytest.raises(ValidationError):
        _request(tuple(_FIRST + timedelta(days=index) for index in range(day_count)))


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ("request", "request_binding_mismatch"),
        ("source", "source_binding_mismatch"),
        ("selection", "selection_mismatch"),
    ],
)
def test_each_batch_is_bound_to_the_exact_request_sources_and_selector(
    change: str, reason: str
) -> None:
    from rquant.factor.daily_stream import evaluate_factor_daily_stream

    request = _request((_FIRST,))
    batch = _batch(request, _FIRST, ("000001.SZ",))
    update = {
        "request": {"request_sha256": "f" * 64},
        "source": {"sources": batch.sources.model_copy(update={"factor_source_id": "other"})},
        "selection": {"universe": batch.universe.model_copy(update={"selection": "gem"})},
    }[change]
    with pytest.raises(ValueError, match=reason):
        evaluate_factor_daily_stream(request, (batch.model_copy(update=update),))


@pytest.mark.parametrize(
    "pool_update",
    [{"selected_count": 2}, {"input_count": 2, "security_count": 2}, {"security_count": 0}],
)
def test_pool_result_counts_are_rechecked(pool_update: dict[str, object]) -> None:
    from rquant.factor.daily_stream import FactorDailyStreamBatch

    batch = _batch(_request((_FIRST,)), _FIRST, ("000001.SZ",))
    with pytest.raises(ValidationError) as caught:
        FactorDailyStreamBatch.model_validate(
            batch.model_copy(update={"universe": batch.universe.model_copy(update=pool_update)})
        )
    assert caught.value.errors()[0]["type"] == "factor_daily_stream_invalid_pool_counts"


def test_pool_result_duplicates_are_refused() -> None:
    from rquant.factor.daily_stream import FactorDailyStreamBatch

    batch = _batch(_request((_FIRST,)), _FIRST, ("000001.SZ", "000002.SZ"))
    copied = batch.universe.model_copy(update={"stock_codes": ("000001.SZ", "000001.SZ")})
    with pytest.raises(ValidationError) as caught:
        FactorDailyStreamBatch.model_validate(batch.model_copy(update={"universe": copied}))
    assert caught.value.errors()[0]["type"] == "factor_daily_stream_duplicate_pool_stock"


@pytest.mark.parametrize("field", ["stock_codes", "source_mode", "input_sha256", "trade_date"])
def test_pool_result_code_mode_digest_and_date_are_revalidated(field: str) -> None:
    from rquant.factor.daily_stream import FactorDailyStreamBatch

    batch = _batch(_request((_FIRST,)), _FIRST, ("000001.SZ",))
    malformed = {
        "stock_codes": ("A",),
        "source_mode": "live",
        "input_sha256": "bad",
        "trade_date": "2026-07-01",
    }[field]
    with pytest.raises(ValidationError):
        FactorDailyStreamBatch.model_validate(
            batch.model_copy(
                update={"universe": batch.universe.model_copy(update={field: malformed})}
            )
        )


@pytest.mark.parametrize("index_source", [False, True])
def test_pool_observation_must_be_available_by_request_as_of(index_source: bool) -> None:
    from rquant.factor.daily_stream import evaluate_factor_daily_stream

    request = _request((_FIRST,))
    if index_source:
        request = request.model_copy(update={"selection": "hs300"})
    batch = _batch(request, _FIRST, ("000001.SZ",))
    field = "index_observed_at" if index_source else "security_observed_at"
    pool = batch.universe.model_copy(update={field: request.as_of + timedelta(seconds=1)})
    with pytest.raises(ValueError, match="pool_observation_after_as_of"):
        evaluate_factor_daily_stream(request, (batch.model_copy(update={"universe": pool}),))


@pytest.mark.parametrize("index_source", [False, True])
def test_pool_day_must_not_follow_its_real_observation_day(index_source: bool) -> None:
    from rquant.factor.daily_stream import FactorDailyStreamBatch

    request = _request((_FIRST,))
    if index_source:
        request = request.model_copy(update={"selection": "hs300"})
    batch = _batch(request, _FIRST, ("000001.SZ",))
    field = "index_observed_at" if index_source else "security_observed_at"
    pool = batch.universe.model_copy(update={field: _at(_FIRST) - timedelta(days=1)})
    with pytest.raises(ValidationError) as caught:
        FactorDailyStreamBatch.model_validate(batch.model_copy(update={"universe": pool}))
    assert caught.value.errors()[0]["type"] == "factor_daily_stream_pool_date_after_observation"


@pytest.mark.parametrize("index_source", [False, True])
def test_index_observation_presence_must_match_selection(index_source: bool) -> None:
    from rquant.factor.daily_stream import FactorDailyStreamBatch

    request = _request((_FIRST,))
    if index_source:
        request = request.model_copy(update={"selection": "hs300"})
    batch = _batch(request, _FIRST, ("000001.SZ",))
    pool = batch.universe.model_copy(
        update={"index_observed_at": None if index_source else request.as_of}
    )
    with pytest.raises(ValidationError) as caught:
        FactorDailyStreamBatch.model_validate(batch.model_copy(update={"universe": pool}))
    assert caught.value.errors()[0]["type"] == "factor_daily_stream_invalid_pool_index_observation"


@pytest.mark.parametrize("component", ["factor_values", "forward_returns"])
@pytest.mark.parametrize("change", ["missing", "extra", "duplicate"])
def test_every_pool_stock_needs_exactly_one_factor_and_return_row(
    component: str, change: str
) -> None:
    from rquant.factor.daily_stream import FactorDailyStreamBatch

    batch = _batch(_request((_FIRST,)), _FIRST, ("000001.SZ", "000002.SZ"))
    rows = getattr(batch, component)
    altered = {
        "missing": rows[:1],
        "extra": rows + (rows[0].model_copy(update={"stock_code": "999999.SZ"}),),
        "duplicate": rows[:1] * 2,
    }[change]
    with pytest.raises(ValidationError) as caught:
        FactorDailyStreamBatch.model_validate(batch.model_copy(update={component: altered}))
    label = "factor" if component == "factor_values" else "return"
    reason = f"duplicate_{label}_row" if change == "duplicate" else f"{label}_grid_mismatch"
    assert caught.value.errors()[0]["type"] == f"factor_daily_stream_{reason}"


@pytest.mark.parametrize("change", ["date", "future_visible", "missing_with_visible", "bad_state"])
def test_factor_facts_are_checked_even_when_their_returns_are_missing(change: str) -> None:
    from rquant.factor.daily_stream import FactorDailyStreamBatch

    batch = _batch(_request((_FIRST,)), _FIRST, ("000001.SZ",), returns=(None,))
    factor = batch.factor_values[0]
    update = {
        "date": {"trade_date": _FIRST + timedelta(days=1)},
        "future_visible": {"latest_visible_at": batch.decision_at + timedelta(seconds=1)},
        "missing_with_visible": {"value": None, "missing_reason": "missing_observation"},
        "bad_state": {"value": None},
    }[change]
    with pytest.raises(ValidationError) as caught:
        FactorDailyStreamBatch.model_validate(
            batch.model_copy(update={"factor_values": (factor.model_copy(update=update),)})
        )
    assert (
        caught.value.errors()[0]["type"]
        == {
            "date": "factor_daily_stream_factor_date_mismatch",
            "future_visible": "factor_daily_stream_factor_visibility_invalid",
            "missing_with_visible": "factor_daily_stream_factor_visibility_invalid",
            "bad_state": "factor_value_state_invalid",
        }[change]
    )


@pytest.mark.parametrize("change", ["date", "decision", "end"])
def test_return_facts_are_bound_to_the_common_decision_and_window(change: str) -> None:
    from rquant.factor.daily_stream import FactorDailyStreamBatch

    batch = _batch(_request((_FIRST,)), _FIRST, ("000001.SZ",))
    row = batch.forward_returns[0]
    update = {
        "date": {
            "decision_date": _FIRST + timedelta(days=1),
            "decision_at": row.decision_at + timedelta(days=1),
            "return_end_at": row.return_end_at + timedelta(days=1),
            "first_available_at": row.first_available_at + timedelta(days=1),
        },
        "decision": {"decision_at": row.decision_at + timedelta(seconds=1)},
        "end": {
            "return_end_at": row.return_end_at + timedelta(seconds=1),
            "first_available_at": row.first_available_at + timedelta(seconds=1),
        },
    }[change]
    with pytest.raises(ValidationError) as caught:
        FactorDailyStreamBatch.model_validate(
            batch.model_copy(update={"forward_returns": (row.model_copy(update=update),)})
        )
    assert caught.value.errors()[0]["type"] == (
        "factor_daily_stream_return_date_mismatch"
        if change == "date"
        else "factor_daily_stream_return_window_mismatch"
    )


@pytest.mark.parametrize("change", ["wrong_civil_day", "invalid_end"])
def test_batch_common_window_requires_its_shanghai_day_and_positive_duration(change: str) -> None:
    from rquant.factor.daily_stream import FactorDailyStreamBatch

    batch = _batch(_request((_FIRST,)), _FIRST, ("000001.SZ",))
    update = (
        {"decision_at": batch.decision_at + timedelta(days=1)}
        if change == "wrong_civil_day"
        else {"return_end_at": batch.decision_at}
    )
    with pytest.raises(ValidationError) as caught:
        FactorDailyStreamBatch.model_validate(batch.model_copy(update=update))
    assert caught.value.errors()[0]["type"] == (
        "factor_daily_stream_decision_date_mismatch"
        if change == "wrong_civil_day"
        else "factor_daily_stream_invalid_return_window"
    )


def test_present_returns_must_be_mature_and_first_available_by_as_of() -> None:
    from rquant.factor.daily_stream import evaluate_factor_daily_stream

    request = _request((_FIRST,), as_of=_at(_FIRST) + timedelta(hours=1))
    batch = _batch(request, _FIRST, ("000001.SZ",))
    with pytest.raises(ValueError, match="return_not_available"):
        evaluate_factor_daily_stream(request, (batch,))


@pytest.mark.parametrize("window_is_future", [False, True])
def test_window_unfinished_reason_must_match_the_request_clock(window_is_future: bool) -> None:
    from rquant.factor.daily_stream import evaluate_factor_daily_stream

    request = _request(
        (_FIRST,), as_of=_at(_FIRST) + timedelta(hours=1) if window_is_future else _AS_OF
    )
    batch = _batch(request, _FIRST, ("000001.SZ",), returns=(None,))
    row = batch.forward_returns[0].model_copy(
        update={"missing_reason": "missing_price" if window_is_future else "window_unfinished"}
    )
    with pytest.raises(ValueError, match="return_missing_reason_mismatch"):
        evaluate_factor_daily_stream(
            request, (batch.model_copy(update={"forward_returns": (row,)}),)
        )


def test_valid_unfinished_and_visibility_pending_returns_are_explicit_missing() -> None:
    from rquant.factor.daily_stream import evaluate_factor_daily_stream

    for kind, as_of in (
        ("window_unfinished", _at(_FIRST) + timedelta(hours=1)),
        ("visibility_pending", _at(_FIRST) + timedelta(days=1, hours=1)),
    ):
        request = _request((_FIRST,), as_of=as_of)
        batch = _batch(request, _FIRST, ("000001.SZ",), returns=(None,))
        row = batch.forward_returns[0].model_copy(
            update={
                "missing_reason": kind,
                "expected_available_at": batch.return_end_at + timedelta(hours=2)
                if kind == "visibility_pending"
                else None,
            }
        )
        day = evaluate_factor_daily_stream(
            request, (batch.model_copy(update={"forward_returns": (row,)}),)
        ).days[0]
        assert day.status == "no_samples"
        assert [(count.reason, count.count) for count in day.coverage.return_missing_by_reason] == [
            (kind, 1)
        ]


def test_expired_visibility_pending_reason_is_refused() -> None:
    from rquant.factor.daily_stream import evaluate_factor_daily_stream

    request = _request((_FIRST,))
    batch = _batch(request, _FIRST, ("000001.SZ",), returns=(None,))
    row = batch.forward_returns[0].model_copy(
        update={
            "missing_reason": "visibility_pending",
            "expected_available_at": batch.return_end_at + timedelta(hours=2),
        }
    )
    with pytest.raises(ValueError, match="return_missing_reason_mismatch"):
        evaluate_factor_daily_stream(
            request, (batch.model_copy(update={"forward_returns": (row,)}),)
        )


@pytest.mark.parametrize("change", ["overlap", "future_decision", "late_return_visibility"])
def test_cross_batch_window_and_request_clock_constraints(change: str) -> None:
    from rquant.factor.daily_stream import evaluate_factor_daily_stream

    days = (_FIRST, _FIRST + timedelta(days=1))
    request = _request(days, holding_sessions=5) if change == "overlap" else _request(days)
    if change == "future_decision":
        request = _request(days, as_of=_at(days[1]) - timedelta(minutes=1))
    batches = tuple(_batch(request, day, (), returns=()) for day in days)
    if change == "late_return_visibility":
        request = _request((_FIRST,))
        batch = _batch(request, _FIRST, ("000001.SZ",))
        late = batch.forward_returns[0].model_copy(
            update={"first_available_at": request.as_of + timedelta(seconds=1)}
        )
        batches = (batch.model_copy(update={"forward_returns": (late,)}),)
    with pytest.raises(
        ValueError,
        match={
            "overlap": "window_overlap",
            "future_decision": "decision_after_as_of",
            "late_return_visibility": "return_not_available",
        }[change],
    ):
        evaluate_factor_daily_stream(request, batches)


def test_constant_factors_without_a_source_visible_time_keep_existing_semantics() -> None:
    from rquant.factor.daily_stream import evaluate_factor_daily_stream

    request = _request((_FIRST,), expression="5")
    batch = _batch(
        request, _FIRST, ("000001.SZ", "000002.SZ", "000003.SZ"), factors=(5.0, 5.0, 5.0)
    )
    batch = batch.model_copy(
        update={
            "factor_values": tuple(
                row.model_copy(update={"latest_visible_at": None}) for row in batch.factor_values
            )
        }
    )
    result = evaluate_factor_daily_stream(request, (batch,))
    assert result.days[0].coverage.valid_count == 3
    assert (
        result.days[0].evaluation.normal_ic.status
        == result.days[0].evaluation.rank_ic.status
        == "zero_variance"
    )


def test_order_of_pool_codes_factor_rows_and_return_rows_does_not_change_any_digest() -> None:
    from rquant.factor.daily_stream import evaluate_factor_daily_stream

    request = _request((_FIRST,))
    batch = _known_batches(request)[0]
    reversed_batch = batch.model_copy(
        update={
            "universe": batch.universe.model_copy(
                update={"stock_codes": tuple(reversed(batch.universe.stock_codes))}
            ),
            "factor_values": tuple(reversed(batch.factor_values)),
            "forward_returns": tuple(reversed(batch.forward_returns)),
        }
    )
    assert evaluate_factor_daily_stream(request, (batch,)) == evaluate_factor_daily_stream(
        request, (reversed_batch,)
    )


def test_completed_batches_and_source_rows_are_released_before_next_batch() -> None:
    from rquant.factor.daily_stream import evaluate_factor_daily_stream

    request = _request(tuple(_FIRST + timedelta(days=index) for index in range(3)))
    released: list[bool] = []

    def source() -> Iterator[FactorDailyStreamBatch]:
        for day in request.evaluation_days:
            batch = _batch(request, day, ("000001.SZ", "000002.SZ", "000003.SZ"))
            refs = (
                weakref.ref(batch),
                weakref.ref(batch.factor_values[0]),
                weakref.ref(batch.forward_returns[0]),
            )
            yield batch
            del batch
            assert all(ref() is None for ref in refs), "stream retained completed source objects"
            released.append(True)

    result = evaluate_factor_daily_stream(request, source())
    assert len(released) == len(result.days) == 3
    assert "stock_code" not in str(result.model_dump())


@pytest.mark.parametrize("direction", ["higher_is_better", "lower_is_better"])
def test_known_perfect_positive_and_negative_ic(direction: FactorDirection) -> None:
    from rquant.factor.daily_stream import evaluate_factor_daily_stream

    request = _request((_FIRST,), direction=direction)
    batch = _batch(
        request,
        _FIRST,
        ("000001.SZ", "000002.SZ", "000003.SZ"),
        factors=(1.0, 2.0, 3.0),
        returns=(0.1, 0.2, 0.3),
    )
    day = evaluate_factor_daily_stream(request, (batch,)).days[0]
    expected = 1.0 if direction == "higher_is_better" else -1.0
    assert day.evaluation.normal_ic.value == pytest.approx(expected)
    assert day.evaluation.rank_ic.value == pytest.approx(expected)


@pytest.mark.parametrize("holding_sessions", [1, 5, 10, 20])
def test_declared_holding_period_uses_explicit_non_overlapping_windows(
    holding_sessions: HoldingSessions,
) -> None:
    from rquant.factor.daily_stream import evaluate_factor_daily_stream

    request = _request(
        (_FIRST, _FIRST + timedelta(days=holding_sessions)), holding_sessions=holding_sessions
    )
    result = evaluate_factor_daily_stream(
        request,
        (
            _batch(request, day, ("000001.SZ", "000002.SZ", "000003.SZ"))
            for day in request.evaluation_days
        ),
    )
    assert result.request.holding_sessions == holding_sessions
    assert result.days[0].return_end_at == result.days[1].decision_at


@pytest.mark.parametrize("sample_count", [0, 1])
def test_empty_or_one_pair_day_preserves_insufficient_ic(sample_count: int) -> None:
    from rquant.factor.daily_stream import evaluate_factor_daily_stream

    request = _request((_FIRST,))
    batch = _batch(request, _FIRST, tuple(f"{index:06d}.SZ" for index in range(sample_count)))
    result = evaluate_factor_daily_stream(request, (batch,))
    day = result.days[0]
    assert day.status == ("no_samples" if not sample_count else "complete")
    assert day.coverage.expected_count == day.coverage.valid_count == sample_count
    assert (
        day.evaluation.normal_ic.status == day.evaluation.rank_ic.status == "insufficient_samples"
    )
    assert day.evaluation.normal_ic.value is day.evaluation.rank_ic.value is None
    assert result.ic_summary.normal_ic.status == "no_valid_days"
    assert result.ic_summary.normal_ic.insufficient_day_count == 1


def test_constant_returns_preserve_zero_variance_reason() -> None:
    from rquant.factor.daily_stream import evaluate_factor_daily_stream

    request = _request((_FIRST,))
    batch = _batch(
        request, _FIRST, ("000001.SZ", "000002.SZ", "000003.SZ"), returns=(0.0, 0.0, 0.0)
    )
    day = evaluate_factor_daily_stream(request, (batch,)).days[0]
    assert day.evaluation.normal_ic.status == day.evaluation.rank_ic.status == "zero_variance"


def test_explicit_missing_counts_overlap_without_silent_pool_shrinkage_and_break_curves() -> None:
    from rquant.factor.daily_stream import evaluate_factor_daily_stream

    request = _request(tuple(_FIRST + timedelta(days=index) for index in range(4)))
    codes = tuple(f"{index:06d}.SZ" for index in range(11))
    first = _batch(request, request.evaluation_days[0], codes)
    missing = _batch(
        request,
        request.evaluation_days[1],
        codes,
        factors=(None, None) + tuple(float(index) for index in range(2, 11)),
        returns=(None, 0.01, None) + tuple(index / 100 for index in range(3, 11)),
    )
    recovered = _batch(request, request.evaluation_days[2], codes)
    continued = _batch(request, request.evaluation_days[3], codes)
    first_day, partial, next_day, last_day = evaluate_factor_daily_stream(
        request, (first, missing, recovered, continued)
    ).days
    assert partial.status == "partial"
    assert partial.coverage.expected_count == 11
    assert partial.coverage.valid_count == 8
    assert partial.coverage.factor_missing_count == partial.coverage.return_missing_count == 2
    assert [(count.reason, count.count) for count in partial.coverage.factor_missing_by_reason] == [
        ("missing_observation", 2)
    ]
    assert [(count.reason, count.count) for count in partial.coverage.return_missing_by_reason] == [
        ("missing_price", 2)
    ]
    assert partial.evaluation.normal_ic.status == "ok"
    assert [group.status for group in partial.portfolio_groupings] == [
        "ok",
        "ok",
        "insufficient_samples",
    ]
    assert all(group.cumulative_status == "available" for group in first_day.portfolio_groupings)
    for day in (partial, next_day, last_day):
        for grouping in day.portfolio_groupings:
            assert grouping.cumulative_status == "gap"
            assert grouping.long_short_cumulative_spread is None
            assert all(point.cumulative_return is None for point in grouping.groups)
    for grouping in next_day.portfolio_groupings:
        assert all(point.target_weight_turnover is None for point in grouping.groups)
    for grouping in last_day.portfolio_groupings:
        assert all(point.target_weight_turnover == 0 for point in grouping.groups)


def test_group_count_insufficiency_breaks_only_its_own_curve_and_previous_members() -> None:
    from rquant.factor.daily_stream import evaluate_factor_daily_stream

    request = _request(tuple(_FIRST + timedelta(days=index) for index in range(3)))
    result = evaluate_factor_daily_stream(
        request,
        (
            _batch(request, day, tuple(f"{index:06d}.SZ" for index in range(size)))
            for day, size in zip(request.evaluation_days, (3, 5, 10), strict=True)
        ),
    )
    assert all(
        day.status == "complete" and day.evaluation.normal_ic.status == "ok" for day in result.days
    )
    for day in result.days:
        assert day.portfolio_groupings[0].cumulative_status == "available"
        assert all(group.cumulative_status == "gap" for group in day.portfolio_groupings[1:])
    assert all(
        point.target_weight_turnover is None
        for point in result.days[1].portfolio_groupings[1].groups
    )
    assert all(
        point.target_weight_turnover is None
        for point in result.days[2].portfolio_groupings[2].groups
    )


@pytest.mark.parametrize("component", ["factor_values", "forward_returns"])
def test_single_day_fact_capacity_cannot_be_exceeded(component: str) -> None:
    from rquant.factor.daily_stream import FactorDailyStreamBatch

    batch = _batch(_request((_FIRST,)), _FIRST, ("000001.SZ",))
    with pytest.raises(ValidationError) as caught:
        FactorDailyStreamBatch.model_validate(
            batch.model_copy(update={component: (getattr(batch, component)[0],) * 7_001})
        )
    assert caught.value.errors()[0]["type"] == "too_long"


@pytest.mark.parametrize(
    "component", ["factor", "return", "window", "pool", "definition", "source", "cutoff"]
)
def test_fact_window_pool_definition_or_source_change_changes_input_digest(component: str) -> None:
    from rquant.factor.daily_stream import evaluate_factor_daily_stream

    request = _request((_FIRST,))
    codes = ("000001.SZ", "000002.SZ", "000003.SZ")
    batch = _batch(request, _FIRST, codes)
    original = evaluate_factor_daily_stream(request, (batch,))
    changed_request = request
    changed = batch
    if component == "factor":
        changed = batch.model_copy(
            update={
                "factor_values": (batch.factor_values[0].model_copy(update={"value": 2.5}),)
                + batch.factor_values[1:]
            }
        )
    elif component == "return":
        changed = batch.model_copy(
            update={
                "forward_returns": (batch.forward_returns[0].model_copy(update={"value": 0.25}),)
                + batch.forward_returns[1:]
            }
        )
    elif component == "window":
        end = batch.return_end_at + timedelta(minutes=1)
        changed = batch.model_copy(
            update={
                "return_end_at": end,
                "forward_returns": tuple(
                    row.model_copy(update={"return_end_at": end, "first_available_at": end})
                    for row in batch.forward_returns
                ),
            }
        )
    elif component == "pool":
        changed = _batch(request, _FIRST, ("000001.SZ", "000002.SZ", "000004.SZ"))
    else:
        if component == "definition":
            changed_request = _request((_FIRST,), direction="lower_is_better")
        elif component == "source":
            changed_request = request.model_copy(
                update={
                    "sources": request.sources.model_copy(
                        update={"factor_source_id": "changed-factor-archive"}
                    )
                }
            )
        else:
            changed_request = _request((_FIRST,), as_of=request.as_of + timedelta(minutes=1))
        changed = _batch(changed_request, _FIRST, codes)
    result = evaluate_factor_daily_stream(changed_request, (changed,))
    assert result.input_sha256 != original.input_sha256
    assert result.sha256 != original.sha256


def test_compounding_overflow_refuses_the_final_result() -> None:
    from rquant.factor.daily_stream import evaluate_factor_daily_stream

    request = _request((_FIRST, _FIRST + timedelta(days=1)))
    with pytest.raises(ValueError, match="non-finite cumulative return"):
        evaluate_factor_daily_stream(
            request,
            (
                _batch(
                    request, day, ("000001.SZ", "000002.SZ", "000003.SZ"), returns=(0.0, 0.0, 1e308)
                )
                for day in request.evaluation_days
            ),
        )


def test_all_new_cross_layer_models_are_frozen_strict_and_reject_extra_fields() -> None:
    from rquant.factor.daily_stream import evaluate_factor_daily_stream

    request = _request((_FIRST,))
    batch = _batch(
        request, _FIRST, ("000001.SZ", "000002.SZ", "000003.SZ"), factors=(None, 2.0, 3.0)
    )
    result = evaluate_factor_daily_stream(request, (batch,))
    complete = evaluate_factor_daily_stream(
        request, (_batch(request, _FIRST, ("000001.SZ", "000002.SZ", "000003.SZ")),)
    )
    day = result.days[0]
    for model in (
        request.sources,
        request,
        batch,
        day.coverage,
        day,
        day.portfolio_groupings[0],
        complete.days[0].portfolio_groupings[0].groups[0],
        result,
    ):
        assert model.model_config["strict"] is True
        with pytest.raises(ValidationError) as extra:
            type(model).model_validate({**model.model_dump(), "undeclared": True})
        assert extra.value.errors()[0]["type"] == "extra_forbidden"
        field, current = next(iter(model.model_dump().items()))
        with pytest.raises(ValidationError) as frozen:
            setattr(model, field, current)
        assert frozen.value.errors()[0]["type"] == "frozen_instance"


def test_large_lazy_stream_releases_sources_and_records_one_bounded_memory_observation() -> None:
    from rquant.factor.daily_stream import evaluate_factor_daily_stream

    request = _request(tuple(_FIRST + timedelta(days=index) for index in range(16)))
    codes = tuple(f"{index:06d}.SZ" for index in range(7_000))
    released: list[bool] = []

    def source() -> Iterator[FactorDailyStreamBatch]:
        for day in request.evaluation_days:
            batch = _batch(request, day, codes)
            refs = (
                weakref.ref(batch),
                weakref.ref(batch.factor_values[0]),
                weakref.ref(batch.forward_returns[0]),
            )
            yield batch
            del batch
            assert all(ref() is None for ref in refs)
            released.append(True)

    tracemalloc.start()
    try:
        result = evaluate_factor_daily_stream(request, source())
        retained, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    print(
        f"synthetic_stream_memory days=16 stocks_per_day=7000 paired_points=112000 "
        f"traced_retained_bytes={retained} traced_peak_bytes={peak} "
        f"released_batches={len(released)}"
    )
    assert len(result.days) == len(released) == 16
    assert all(
        day.coverage.expected_count == day.coverage.valid_count == 7_000 for day in result.days
    )
    assert all(day.evaluation.normal_ic.value == pytest.approx(1.0) for day in result.days)
    assert peak < 256 * 1024 * 1024
    assert "stock_code" not in str(result.model_dump())
