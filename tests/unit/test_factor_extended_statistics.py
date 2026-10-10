"""Synthetic MAD and diagnostic goldens over the original one-shot statistics."""

from __future__ import annotations

import json
import statistics
from datetime import timedelta

import numpy as np
import pytest
from pydantic import ValidationError


@pytest.mark.parametrize("value", [0.0, -1.0, float("nan"), float("inf"), True, "3"])
def test_public_mad_rejects_nonpositive_nonfinite_and_coerced_values(value: object) -> None:
    from rquant.factor.run_request import FactorRunParameters
    from tests.unit.test_factor_run_neutralization import _PARAMETERS

    with pytest.raises(ValidationError):
        FactorRunParameters.model_validate({**_PARAMETERS, "mad_multiple": value})


def test_public_processing_parameters_default_to_original_canonical() -> None:
    from rquant.factor.member_archive import _bytes
    from rquant.factor.run_request import FactorRunParameters
    from rquant.strict_json import canonical_json_bytes
    from tests.unit.test_factor_run_neutralization import _PARAMETERS

    payload = canonical_json_bytes(_PARAMETERS)
    request = FactorRunParameters.model_validate_json(payload)
    assert request.mad_multiple is None and request.extended_statistics is False
    assert _bytes(request) == payload
    changed = FactorRunParameters.model_validate_json(
        json.dumps({**_PARAMETERS, "mad_multiple": 3.0, "extended_statistics": True})
    )
    assert changed.mad_multiple == 3.0 and changed.extended_statistics is True
    assert changed != request


@pytest.mark.parametrize(
    "values",
    [(1.0, 2.0, 3.0, 100.0), (2.0, 2.0, 2.0, 100.0), (1.0, 1.0, 3.0, 3.0), (None, 1.0, 2.0, 100.0)],
)
def test_mad_uses_only_selected_valid_values_and_original_operator(values: tuple) -> None:
    from rquant.factor.formula_stream import (
        FactorFormulaStreamRequest,
        evaluate_factor_formula_stream,
    )
    from tests.unit.test_factor_formula_stream import _DAYS, _batch, _request

    codes = tuple(f"{n:06d}.SZ" for n in range(1, 6))
    old = _request("close", codes=codes, days=_DAYS[:1])
    request = FactorFormulaStreamRequest.model_validate({**old.model_dump(), "mad_multiple": 1.0})
    batch = _batch(
        request,
        _DAYS[0],
        members=codes[:4],
        values={(code, "close"): value for code, value in zip(codes, (*values, 1e30), strict=True)},
    )
    finite = [value for value in values if value is not None]
    center = statistics.median(finite)
    spread = 1.4826 * statistics.median(abs(value - center) for value in finite)
    expected = [
        None if value is None else min(center + spread, max(center - spread, value))
        for value in values
    ]
    stream = evaluate_factor_formula_stream(request, (batch,))
    day = next(stream)
    assert [value.value for value in day.values] == pytest.approx(expected)
    tuple(stream)
    assert stream.completion is not None


def test_mad_precedes_post_formula_industry_neutralization() -> None:
    from rquant.factor.formula_stream import (
        FactorFormulaStreamRequest,
        evaluate_factor_formula_stream,
    )
    from tests.unit.test_factor_formula_neutralization import _context_batch, _request_context

    base = _request_context(mode="industry")
    request = FactorFormulaStreamRequest.model_validate({**base.model_dump(), "mad_multiple": 1.0})
    batches = []
    raw = [1.0, 2.0, 3.0, 4.0, 5.0, 100.0]
    for day in request.trading_days:
        batch = _context_batch(request, day)
        points = tuple(
            point.model_copy(update={"value": value})
            for point, value in zip(batch.feature_points, raw, strict=True)
        )
        batches.append(type(batch).model_validate({**batch.model_dump(), "feature_points": points}))
    center = statistics.median(raw)
    spread = 1.4826 * statistics.median(abs(v - center) for v in raw)
    clipped = [min(center + spread, max(center - spread, v)) for v in raw]
    expected = [
        v - statistics.mean(clipped[:3] if i < 3 else clipped[3:]) for i, v in enumerate(clipped)
    ]
    assert [
        v.value for v in tuple(evaluate_factor_formula_stream(request, batches))[0].values
    ] == pytest.approx(expected)


def _extended_request(
    days: tuple,
    *,
    method: str = "rank",
    sources: object = None,
    direction: str = "higher_is_better",
) -> object:
    from rquant.factor.daily_stream import FactorDailyStreamRequest
    from tests.unit.test_factor_daily_stream import _request

    request = _request(days, direction=direction)
    return FactorDailyStreamRequest.model_validate(
        {**request.model_dump(), "extended_statistics": {"ic_method": method, "sources": sources}}
    )


def test_rank_autocorrelation_keeps_adjacent_dates_and_ignores_missing_returns() -> None:
    from rquant.factor.daily_stream import evaluate_factor_daily_stream
    from tests.unit.test_factor_daily_stream import _FIRST, _batch

    days = tuple(_FIRST + timedelta(days=i) for i in range(5))
    request = _extended_request(days)
    codes = ("000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ")
    batches = (
        _batch(request, days[0], codes[:3], (1.0, 3.0, 3.0)),
        _batch(request, days[1], codes, (3.0, 2.0, 2.0, 1.0), (None,) * 4),
        _batch(request, days[2], codes, (None,) * 4),
        _batch(request, days[3], codes[1:], (2.0, 3.0, 1.0)),
        _batch(request, days[4], codes[1:], (7.0, 7.0, 7.0)),
    )
    points = evaluate_factor_daily_stream(
        request, iter(batches)
    ).extended_statistics.autocorrelation_points
    assert [p.trade_date for p in points] == list(days)
    assert points[0].status == "first_period" and points[0].previous_trade_date is None
    assert points[1].status == "ok" and points[1].common_count == 3
    assert points[1].value == pytest.approx(-1.0)
    assert points[2].status == points[3].status == "no_common_members"
    assert points[3].previous_trade_date == days[2]
    assert points[4].status == "zero_variance" and points[4].value is None


@pytest.mark.parametrize("method", ["rank", "normal"])
def test_industry_ic_uses_current_method_direction_and_truthful_coverage(method: str) -> None:
    from rquant.factor.daily_stream import evaluate_factor_daily_stream
    from rquant.factor.industry_source import FactorIndustryFact
    from rquant.factor.neutralization_context import FactorNeutralizationDayBatch
    from tests.unit.test_factor_daily_stream import _FIRST, _at, _batch
    from tests.unit.test_factor_formula_neutralization import _request_context

    days = (_FIRST, _FIRST + timedelta(days=1))
    source = _request_context().sources.context
    request = _extended_request(days, method=method, sources=source, direction="lower_is_better")
    codes = tuple(f"{i:06d}.SZ" for i in range(1, 7))
    batches = []
    y, r = (1.0, 2.0, 8.0, 2.0, 4.0, 7.0), (0.1, 0.4, 0.2, 0.9, 0.2, None)
    for day in days:
        panel = day - timedelta(days=1)
        facts = tuple(
            FactorIndustryFact(
                stock_code=c,
                trade_date=panel,
                status="ambiguous" if i == 5 else "valid",
                l1_code=None if i == 5 else ("801010.SI" if i < 3 else "801011.SI"),
                l1_name=None if i == 5 else "合成行业",
            )
            for i, c in enumerate(codes)
        )
        context = FactorNeutralizationDayBatch(
            sources=source,
            trade_date=day,
            panel_date=panel,
            stock_codes=codes,
            assumed_visible_at=_at(day),
            industry_facts=facts,
            market_cap_facts=tuple(
                __import__(
                    "rquant.factor.market_cap_source", fromlist=["FactorMarketCapFact"]
                ).FactorMarketCapFact(stock_code=c, trade_date=panel, status="valid", total_mv=1.0)
                for c in codes
            ),
        )
        batch = _batch(request, day, codes, y, r)
        batches.append(type(batch).model_validate({**batch.model_dump(), "context": context}))
    result = evaluate_factor_daily_stream(request, batches)
    extended = result.extended_statistics
    assert extended.industry_status == "available" and extended.ic_method == method
    assert [day.valid_label_count for day in extended.industry_coverage_days] == [5, 5]
    assert [day.paired_count for day in extended.industry_coverage_days] == [5, 5]
    assert extended.industry_coverage_days[0].missing_by_reason[0].reason == "ambiguous"
    left, right = (-1.0, -2.0, -8.0), (0.1, 0.4, 0.2)
    if method == "rank":
        left, right = (3.0, 2.0, 1.0), (1.0, 3.0, 2.0)
    expected = float(np.corrcoef(left, right)[0, 1])
    summary = extended.industry_summaries[0]
    assert summary.ic_summary.mean == pytest.approx(expected)
    assert summary.ic_summary.valid_day_count == 2 and summary.sample_count == 6
    assert result.days[0].coverage.valid_count == 5


def test_missing_industry_only_marks_extended_diagnostic_unavailable() -> None:
    from rquant.factor.daily_stream import evaluate_factor_daily_stream
    from tests.unit.test_factor_daily_stream import _FIRST, _batch

    request = _extended_request((_FIRST,))
    result = evaluate_factor_daily_stream(
        request, (_batch(request, _FIRST, ("000001.SZ", "000002.SZ")),)
    )
    assert result.days[0].evaluation.rank_ic.status == "ok"
    assert result.extended_statistics.industry_status == "unavailable"
    assert result.extended_statistics.industry_reason
    assert not result.extended_statistics.industry_summaries


def test_rank_autocorrelation_single_overlap_remains_insufficient() -> None:
    from rquant.factor.daily_stream import evaluate_factor_daily_stream
    from tests.unit.test_factor_daily_stream import _FIRST, _batch

    days = (_FIRST, _FIRST + timedelta(days=3))
    request = _extended_request(days)
    batches = (
        _batch(request, days[0], ("000001.SZ", "000002.SZ")),
        _batch(request, days[1], ("000002.SZ", "000003.SZ")),
    )
    point = evaluate_factor_daily_stream(
        request, batches
    ).extended_statistics.autocorrelation_points[1]
    assert point.previous_trade_date == days[0] and point.common_count == 1
    assert point.status == "insufficient_samples" and point.value is None


def test_extended_one_shot_cache_release_and_failed_tail_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import weakref

    from rquant.factor.daily_stream import evaluate_factor_daily_stream
    from rquant.factor.extended_statistics import FactorExtendedStatisticsAccumulator
    from tests.unit.test_factor_daily_stream import _FIRST, _batch

    days = tuple(_FIRST + timedelta(days=i) for i in range(3))
    request = _extended_request(days)
    accumulators, refs = [], []
    close = FactorExtendedStatisticsAccumulator.close

    def closed(accumulator: FactorExtendedStatisticsAccumulator) -> None:
        assert len(accumulator.previous) <= 7000
        close(accumulator)
        accumulators.append(accumulator)

    monkeypatch.setattr(FactorExtendedStatisticsAccumulator, "close", closed)

    def batches(fail: bool) -> object:
        for day in days:
            assert all(ref() is None for ref in refs)
            batch = _batch(request, day, ("000001.SZ", "000002.SZ", "000003.SZ"))
            refs.append(weakref.ref(batch))
            yield batch
            del batch
        if fail:
            raise RuntimeError("synthetic natural tail failure")

    result = evaluate_factor_daily_stream(request, batches(False))
    assert len(result.extended_statistics.autocorrelation_points) == 3
    assert all(ref() is None for ref in refs)
    with pytest.raises(RuntimeError, match="natural tail"):
        evaluate_factor_daily_stream(request, batches(True))
    assert len(accumulators) == 2
    assert all(not a.previous and not a.points and not a.series for a in accumulators)


def test_extended_unknown_labels_and_wrong_binding_remain_explicit() -> None:
    from rquant.factor.daily_stream import (
        FactorDailyStreamBatch,
        FactorDailyStreamError,
        evaluate_factor_daily_stream,
    )
    from rquant.factor.industry_source import FactorIndustryFact
    from rquant.factor.neutralization_context import FactorNeutralizationDayBatch
    from tests.unit.test_factor_daily_stream import _FIRST, _at, _batch
    from tests.unit.test_factor_formula_neutralization import _request_context

    source = _request_context().sources.context
    source = type(source).model_validate({**source.model_dump(), "market_cap": None})
    request = _extended_request((_FIRST,), sources=source)
    codes = ("000001.SZ", "000002.SZ", "000003.SZ")
    panel = _FIRST - timedelta(days=1)
    context = FactorNeutralizationDayBatch(
        sources=source,
        trade_date=_FIRST,
        panel_date=panel,
        stock_codes=codes,
        assumed_visible_at=_at(_FIRST),
        industry_facts=tuple(
            FactorIndustryFact(stock_code=code, trade_date=panel, status=status)
            for code, status in zip(
                codes, ("missing", "ambiguous", "boundary_unverified"), strict=True
            )
        ),
    )
    batch = _batch(request, _FIRST, codes)
    batch = FactorDailyStreamBatch.model_validate({**batch.model_dump(), "context": context})
    result = evaluate_factor_daily_stream(request, (batch,))
    assert result.days[0].evaluation.rank_ic.status == "ok"
    coverage = result.extended_statistics.industry_coverage_days[0]
    assert coverage.valid_label_count == coverage.paired_count == 0
    assert {r.reason for r in coverage.missing_by_reason} == {
        "missing",
        "ambiguous",
        "boundary_unverified",
    }
    assert not result.extended_statistics.industry_summaries
    for wrong in (
        None,
        context.model_copy(
            update={"sources": source.model_copy(update={"binding_hash": "0" * 64})}
        ),
    ):
        with pytest.raises(FactorDailyStreamError, match="source_binding_mismatch"):
            evaluate_factor_daily_stream(request, (batch.model_copy(update={"context": wrong}),))


def test_mad_nonfinite_threshold_keeps_original_missing_reason() -> None:
    from rquant.factor.formula_stream import (
        FactorFormulaStreamRequest,
        evaluate_factor_formula_stream,
    )
    from tests.unit.test_factor_formula_stream import _DAYS, _batch, _request

    codes = ("000001.SZ", "000002.SZ")
    base = _request("close", codes=codes, days=_DAYS[:1])
    request = FactorFormulaStreamRequest.model_validate(
        {**base.model_dump(), "mad_multiple": 1e308}
    )
    batch = _batch(
        request, _DAYS[0], values={(codes[0], "close"): -1e308, (codes[1], "close"): 1e308}
    )
    day = tuple(evaluate_factor_formula_stream(request, (batch,)))[0]
    assert all(v.value is None and v.missing_reason == "non_finite_result" for v in day.values)
