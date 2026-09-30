"""Evaluation-sequence decay uses bounded historical factor cross-sections."""

from __future__ import annotations

import weakref
from collections.abc import Iterator
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest
from pydantic import ValidationError

from rquant.runtime_contracts import canonical_sha256
from tests.unit.test_factor_daily_stream import _FIRST, _batch, _known_batches, _request

if TYPE_CHECKING:
    from rquant.factor.daily_stream import FactorDailyStreamBatch, FactorDailyStreamRequest
    from rquant.factor.result import FactorResearchRequest


def _legacy_request(
    request: FactorDailyStreamRequest, batches: tuple[FactorDailyStreamBatch, ...]
) -> FactorResearchRequest:
    from rquant.factor.result import FactorResearchRequest
    from rquant.factor.time_series import DecisionTime, FactorTimeSeriesInput, FeatureObservation

    return FactorResearchRequest(
        factor_input=FactorTimeSeriesInput(
            definition=request.definition,
            universe=batches[0].universe.stock_codes,
            trading_days=request.evaluation_days,
            decision_times=tuple(
                DecisionTime(trade_date=batch.universe.trade_date, decision_at=batch.decision_at)
                for batch in batches
            ),
            observations=tuple(
                FeatureObservation(
                    stock_code=row.stock_code,
                    trade_date=row.trade_date,
                    column="close",
                    value=row.value,
                    first_visible_at=row.latest_visible_at or batch.decision_at,
                )
                for batch in batches
                for row in batch.factor_values
            ),
        ),
        evaluation_days=request.evaluation_days,
        forward_returns=tuple(row for batch in batches for row in batch.forward_returns),
        as_of=request.as_of,
        factor_source_id=request.sources.factor_source_id,
        return_source_id=request.sources.return_source_id,
        return_price_basis=request.return_price_basis,
        holding_sessions=request.holding_sessions,
    )


@pytest.mark.parametrize("direction", ["higher_is_better", "lower_is_better"])
def test_all_ten_lags_match_actual_batch_decay_with_ties(direction: str) -> None:
    from rquant.factor.daily_stream import evaluate_factor_daily_stream
    from rquant.factor.decay import evaluate_factor_ic_decay
    from rquant.factor.decay_stream import FactorICDecayStream, FactorICDecayStreamRequest

    request = _request(
        tuple(_FIRST + timedelta(days=index * 3) for index in range(12)), direction=direction
    )
    batches = _known_batches(request, ties=True)
    stream = FactorICDecayStream(
        FactorICDecayStreamRequest(
            statistics_request=request, computation_stock_codes=batches[0].universe.stock_codes
        )
    )
    for batch in batches:
        stream.consume(batch)
    statistics = evaluate_factor_daily_stream(request, iter(batches))
    result = stream.finish(statistics)
    expected = evaluate_factor_ic_decay(_legacy_request(request, batches))
    assert result.periods == expected.periods
    assert result.periods[0].ic_summary == statistics.ic_summary
    assert result.batch_sha256s == statistics.batch_sha256s
    assert stream.cached_period_count == stream.cached_factor_count == 0


def test_dynamic_membership_uses_base_factor_even_when_its_own_return_is_missing() -> None:
    from rquant.factor.daily_stream import evaluate_factor_daily_stream
    from rquant.factor.decay_stream import FactorICDecayStream, FactorICDecayStreamRequest

    days = tuple(_FIRST + timedelta(days=index * 3) for index in range(3))
    request = _request(days)
    a, b, c, d = (f"{index:06d}.SZ" for index in range(1, 5))
    batches = (
        _batch(request, days[0], (a, b, c), (1.0, 2.0, 3.0), (None, None, None)),
        _batch(request, days[1], (b, c, d), (90.0, 10.0, 20.0), (0.2, 0.3, 0.1)),
        _batch(request, days[2], (), (), ()),
    )
    stream = FactorICDecayStream(
        FactorICDecayStreamRequest(statistics_request=request, computation_stock_codes=(a, b, c, d))
    )
    for batch in batches:
        stream.consume(batch)
    result = stream.finish(evaluate_factor_daily_stream(request, iter(batches)))
    first, second, third, fourth = result.periods[:4]
    assert first.days[0].valid_pair_count == 0
    assert second.days[0].valid_pair_count == 2
    assert second.days[0].normal_ic.value == pytest.approx(1.0)
    assert second.days[0].rank_ic.value == pytest.approx(1.0)
    assert second.days[1].valid_pair_count == third.days[0].valid_pair_count == 0
    assert third.status == "no_valid_days"
    assert fourth.status == "no_target_period" and fourth.ic_summary is None


def test_rehashed_wrong_statistics_input_is_refused_and_cache_is_cleared() -> None:
    from rquant.factor.daily_stream import FactorDailyStreamResult, evaluate_factor_daily_stream
    from rquant.factor.decay_stream import FactorICDecayStream, FactorICDecayStreamRequest

    request = _request(tuple(_FIRST + timedelta(days=index * 3) for index in range(3)))
    batches = _known_batches(request)
    stream = FactorICDecayStream(
        FactorICDecayStreamRequest(
            statistics_request=request, computation_stock_codes=batches[0].universe.stock_codes
        )
    )
    for batch in batches:
        stream.consume(batch)
    statistics = evaluate_factor_daily_stream(request, iter(batches))
    fields = statistics.model_dump(exclude={"sha256"})
    fields["batch_sha256s"] = ("0" * 64,) + statistics.batch_sha256s[1:]
    fields["input_sha256"] = canonical_sha256((statistics.request_sha256, fields["batch_sha256s"]))
    forged = FactorDailyStreamResult(**fields, sha256=canonical_sha256(fields))
    with pytest.raises(ValueError, match="statistics_completion_mismatch"):
        stream.finish(forged)
    assert stream.completion is None
    assert stream.cached_period_count == stream.cached_factor_count == 0


def test_wrong_request_batch_and_incomplete_finish_cannot_complete() -> None:
    from rquant.factor.daily_stream import evaluate_factor_daily_stream
    from rquant.factor.decay_stream import FactorICDecayStream, FactorICDecayStreamRequest

    request = _request(tuple(_FIRST + timedelta(days=index * 3) for index in range(2)))
    batches = _known_batches(request)
    decay_request = FactorICDecayStreamRequest(
        statistics_request=request, computation_stock_codes=batches[0].universe.stock_codes
    )
    stream = FactorICDecayStream(decay_request)
    stream.consume(batches[0])
    with pytest.raises(ValueError, match="batch_binding_mismatch"):
        stream.consume(batches[1].model_copy(update={"request_sha256": "0" * 64}))
    assert stream.cached_period_count == stream.cached_factor_count == 0
    assert stream.completion is None
    incomplete = FactorICDecayStream(decay_request)
    incomplete.consume(batches[0])
    with pytest.raises(ValueError, match="statistics_completion_mismatch"):
        incomplete.finish(evaluate_factor_daily_stream(request, iter(batches)))
    assert incomplete.completion is None and incomplete.cached_factor_count == 0


def test_normalized_row_order_and_strict_revalidation_bind_the_result() -> None:
    from rquant.factor.daily_stream import evaluate_factor_daily_stream
    from rquant.factor.decay_stream import (
        FactorICDecayStream,
        FactorICDecayStreamRequest,
        FactorICDecayStreamResult,
    )

    request = _request(tuple(_FIRST + timedelta(days=index * 3) for index in range(3)))
    batches = _known_batches(request)
    decay_request = FactorICDecayStreamRequest(
        statistics_request=request,
        computation_stock_codes=tuple(reversed(batches[0].universe.stock_codes)),
    )
    first, second = FactorICDecayStream(decay_request), FactorICDecayStream(decay_request)
    for batch in batches:
        first.consume(batch)
        second.consume(
            batch.model_copy(
                update={
                    "universe": batch.universe.model_copy(
                        update={"stock_codes": tuple(reversed(batch.universe.stock_codes))}
                    ),
                    "factor_values": tuple(reversed(batch.factor_values)),
                    "forward_returns": tuple(reversed(batch.forward_returns)),
                }
            )
        )
    statistics = evaluate_factor_daily_stream(request, iter(batches))
    result = first.finish(statistics)
    assert result == second.finish(statistics)
    with pytest.raises(ValidationError, match="input binding"):
        FactorICDecayStreamResult.model_validate(
            result.model_copy(update={"input_sha256": "0" * 64})
        )
    with pytest.raises(ValidationError, match="frozen"):
        result.sha256 = "0" * 64
    with pytest.raises(ValidationError):
        FactorICDecayStreamRequest.model_validate(
            decay_request.model_copy(
                update={"computation_stock_codes": list(decay_request.computation_stock_codes)}
            )
        )


def test_more_than_ten_periods_cache_only_scalars_and_release_source_batches() -> None:
    from rquant.factor.daily_stream import FactorDailyStreamBatch, evaluate_factor_daily_stream
    from rquant.factor.decay_stream import FactorICDecayStream, FactorICDecayStreamRequest

    request = _request(tuple(_FIRST + timedelta(days=index * 3) for index in range(16)))
    codes = tuple(f"{index:06d}.SZ" for index in range(1, 25))
    stream = FactorICDecayStream(
        FactorICDecayStreamRequest(statistics_request=request, computation_stock_codes=codes)
    )
    refs = []
    maximum = 0

    def source() -> Iterator[FactorDailyStreamBatch]:
        nonlocal maximum
        for index, day in enumerate(request.evaluation_days):
            batch = _batch(request, day, codes)
            refs.extend(
                (
                    weakref.ref(batch),
                    weakref.ref(batch.universe),
                    weakref.ref(batch.factor_values[0]),
                    weakref.ref(batch.forward_returns[0]),
                )
            )
            yield batch
            stream.consume(batch)
            assert stream.cached_period_count == min(index + 1, 10)
            assert stream.cached_factor_count == min(index + 1, 10) * len(codes)
            maximum = max(maximum, stream.cached_factor_count)
            del batch

    class OneShot(Iterator[FactorDailyStreamBatch]):
        def __init__(self) -> None:
            self.rows = source()
            self.iterations = 0
            self.closed = False

        def __iter__(self) -> OneShot:
            self.iterations += 1
            assert self.iterations == 1
            return self

        def __next__(self) -> FactorDailyStreamBatch:
            return next(self.rows)

        def close(self) -> None:
            self.closed = True
            self.rows.close()

    owned = OneShot()
    try:
        statistics = evaluate_factor_daily_stream(request, owned)
    finally:
        owned.close()
    assert owned.iterations == 1 and owned.closed
    assert all(ref() is None for ref in refs)
    assert stream.completion is None and maximum == 240
    result = stream.finish(statistics)
    assert len(result.periods[9].days) == 7
    assert stream.cached_period_count == stream.cached_factor_count == 0
    print(
        f"DECAY_CACHE: periods=16 stocks=24 maximum_frames=10 maximum_factors={maximum} "
        f"source_refs_released={len(refs)} input_iterations=1 input_closed=true cache_cleared=true"
    )
