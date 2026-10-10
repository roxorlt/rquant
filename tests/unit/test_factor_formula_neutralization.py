"""One-day context, dynamic member history and post-formula joint residuals."""

from __future__ import annotations

import math
from datetime import timedelta

import numpy as np
import pytest

from rquant.factor.formula_stream import FactorFormulaStreamRequest, evaluate_factor_formula_stream
from rquant.factor.industry_source import FactorIndustryFact
from rquant.factor.market_cap_source import FactorMarketCapFact
from tests.unit.test_factor_formula_stream import _DAYS, _at, _batch, _request


def _request_context(
    expression: str = "close", mode: str = "none", *, count: int = 6
) -> FactorFormulaStreamRequest:
    from rquant.factor.neutralization_context import (
        FactorIndustryContextBinding,
        FactorMarketCapContextBinding,
        FactorNeutralizationSources,
    )

    request = _request(
        "close", codes=tuple(f"{n:06d}.SZ" for n in range(1, count + 1)), days=_DAYS[:2]
    )
    context = FactorNeutralizationSources(
        context_sha256="d" * 64,
        prepared_source_sha256="e" * 64,
        snapshot_id="a" * 64,
        binding_hash="a" * 64,
        scope_content_hash="f" * 64,
        code_commit="b" * 40,
        industry=FactorIndustryContextBinding(
            source_sha256="1" * 64, collection_sha256="2" * 64, captured_at=request.as_of
        ),
        market_cap=FactorMarketCapContextBinding(
            source_sha256="3" * 64, generation_sha256="4" * 64
        ),
    )
    definition = request.definition.model_dump()
    from rquant.factor.definition import build_factor_definition

    definition = build_factor_definition(
        **{
            key: definition[key]
            for key in (
                "factor_id",
                "name_zh",
                "category",
                "direction",
                "version",
                "earliest_available_date",
            )
        },
        feature_catalog=request.definition.feature_catalog,
        expression=expression,
    )
    return FactorFormulaStreamRequest.model_validate(
        {
            **request.model_dump(),
            "definition": definition,
            "neutralization": mode,
            "sources": {
                **request.sources.model_dump(),
                "feature_source_id": context.snapshot_id,
                "context": context,
            },
        }
    )


def _context_batch(
    request: FactorFormulaStreamRequest, day: object, *, members: tuple[str, ...] | None = None
) -> object:
    from rquant.factor.neutralization_context import FactorNeutralizationDayBatch

    codes = request.computation_stock_codes
    panel = day - timedelta(days=1)
    industries = tuple(
        FactorIndustryFact(
            stock_code=code,
            trade_date=panel,
            status="valid",
            l1_code="801010.SI" if n < len(codes) // 2 else "801011.SI",
            l1_name="合成行业",
        )
        for n, code in enumerate(codes)
    )
    caps = tuple(
        FactorMarketCapFact(
            stock_code=code,
            trade_date=panel,
            status="valid",
            total_mv=math.exp(n + (8 if n >= len(codes) // 2 else 0)),
        )
        for n, code in enumerate(codes)
    )
    ctx = FactorNeutralizationDayBatch(
        sources=request.sources.context,
        trade_date=day,
        panel_date=panel,
        stock_codes=codes,
        assumed_visible_at=_at(day),
        industry_facts=industries,
        market_cap_facts=caps,
    )
    batch = _batch(
        request,
        day,
        members=members,
        values={(code, "close"): float(n * n + 3 * n + 1) for n, code in enumerate(codes)},
    )
    return type(batch).model_validate({**batch.model_dump(), "context": ctx})


def test_stream_post_joint_matches_independent_ols_and_input_chain() -> None:
    request = _request_context(mode="industry_size")
    batches = tuple(_context_batch(request, day) for day in request.trading_days)
    stream = evaluate_factor_formula_stream(request, iter(batches))
    days = tuple(stream)
    assert stream.completion is not None
    first = batches[0].context
    groups = np.array([fact.l1_code for fact in first.industry_facts])
    x = np.column_stack(
        [
            groups == "801010.SI",
            groups == "801011.SI",
            np.log([fact.total_mv for fact in first.market_cap_facts]),
        ]
    )
    y = np.array([n * n + 3 * n + 1 for n in range(6)], dtype=float)
    expected = y - x @ np.linalg.lstsq(x, y, rcond=None)[0]
    assert [value.value for value in days[0].values] == pytest.approx(expected, abs=1e-12)
    assert days[0].sources.context == request.sources.context
    assert days[0].input_sha256 != days[1].input_sha256


@pytest.mark.parametrize("expression", ["industry_neutralize(close)", "size_neutralize(close)"])
def test_context_dsl_stream_matches_existing_dense_semantics(expression: str) -> None:
    from rquant.factor.time_series import (
        FactorTimeSeriesInput,
        FeatureObservation,
        evaluate_factor_time_series,
    )

    request = _request_context(expression=expression)
    batch = _context_batch(request, request.trading_days[0])
    industries, caps = batch.context.observations()
    dense = evaluate_factor_time_series(
        FactorTimeSeriesInput(
            definition=request.definition,
            universe=request.computation_stock_codes,
            trading_days=(batch.universe.trade_date,),
            decision_times=(request.decision_times[0],),
            observations=tuple(
                FeatureObservation(
                    stock_code=p.stock_code,
                    trade_date=p.trade_date,
                    column=p.column,
                    value=p.value,
                    first_visible_at=p.first_visible_at,
                )
                for p in batch.feature_points
            ),
            industry_observations=tuple(industries.values()),
            market_cap_observations=tuple(caps.values()),
        )
    )
    stream = evaluate_factor_formula_stream(
        request, (batch, _context_batch(request, request.trading_days[1]))
    )
    first = next(stream)
    assert first.values == dense.values
    stream.close()
    assert stream.completion is None and not stream._engine.history


def test_nested_neutralization_retains_original_day_members() -> None:
    request = _request_context(expression="ref(industry_neutralize(close), 1)", count=6)
    codes = request.computation_stock_codes
    batches = (
        _context_batch(request, request.trading_days[0], members=codes[:2] + codes[3:5]),
        _context_batch(
            request, request.trading_days[1], members=(codes[0], codes[2], codes[3], codes[5])
        ),
    )
    days = tuple(evaluate_factor_formula_stream(request, batches))
    points = {v.stock_code: v for v in days[1].values}
    assert points[codes[0]].value == -2.0
    assert points[codes[3]].value == -5.0
    assert points[codes[2]].missing_reason == "missing_observation"
    assert points[codes[5]].missing_reason == "missing_observation"


def test_context_binding_missing_future_and_budget_are_checked_before_success() -> None:
    from rquant.factor.formula_stream import FactorFormulaStreamError, _compile

    request = _request_context(mode="industry")
    batch = _context_batch(request, request.trading_days[0])
    for changed in (
        batch.model_copy(update={"context": None}),
        batch.model_copy(
            update={
                "context": batch.context.model_copy(
                    update={"assumed_visible_at": request.as_of + timedelta(days=1)}
                )
            }
        ),
    ):
        stream = evaluate_factor_formula_stream(request, (changed,))
        with pytest.raises((ValueError, FactorFormulaStreamError)):
            next(stream)
        assert stream.completion is None
    plain = _request("close", codes=request.computation_stock_codes, days=request.trading_days)
    assert _compile(request).slots > _compile(plain).slots


def test_raw_nonpositive_and_nonfinite_caps_only_remove_common_samples() -> None:
    request = _request_context(mode="industry_size")
    batch = _context_batch(request, request.trading_days[0])
    facts = list(batch.context.market_cap_facts)
    facts[0] = FactorMarketCapFact(
        stock_code=facts[0].stock_code,
        trade_date=facts[0].trade_date,
        total_mv=0.0,
        status="non_positive",
    )
    facts[3] = FactorMarketCapFact(
        stock_code=facts[3].stock_code,
        trade_date=facts[3].trade_date,
        total_mv=float("inf"),
        status="non_finite",
    )
    context = type(batch.context).model_validate(
        {**batch.context.model_dump(), "market_cap_facts": tuple(facts)}
    )
    batch = type(batch).model_validate({**batch.model_dump(), "context": context})
    stream = evaluate_factor_formula_stream(
        request, (batch, _context_batch(request, request.trading_days[1]))
    )
    first = next(stream)
    assert [point.missing_reason for point in first.values] == [
        "missing_context",
        None,
        None,
        "missing_context",
        None,
        None,
    ]
    tuple(stream)
    assert stream.completion is not None


def test_context_working_vectors_count_towards_existing_cache_budget() -> None:
    from rquant.factor.formula_stream import FactorFormulaStreamError

    request = _request_context(
        expression="ts_mean(close, 252) + ts_mean(vol, 30)", mode="industry_size", count=7000
    )

    def forbidden() -> object:
        pytest.fail("over-budget stream consumed its source")
        yield

    with pytest.raises(FactorFormulaStreamError, match="cache_budget_exceeded"):
        evaluate_factor_formula_stream(request, forbidden())


def test_context_request_rejects_wrong_price_binding_and_late_capture() -> None:
    request = _request_context(mode="industry_size")
    source = request.sources.context
    for changed in (
        source.model_copy(update={"snapshot_id": "0" * 64}),
        source.model_copy(update={"binding_hash": "0" * 64}),
        source.model_copy(
            update={
                "industry": source.industry.model_copy(
                    update={"captured_at": request.as_of + timedelta(seconds=1)}
                )
            }
        ),
    ):
        with pytest.raises(ValueError):
            type(request).model_validate(
                {
                    **request.model_dump(),
                    "sources": {**request.sources.model_dump(), "context": changed},
                }
            )
