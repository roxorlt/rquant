"""Daily formulas preserve the existing mathematics and historical pool membership."""

from __future__ import annotations

import tracemalloc
import weakref
from collections.abc import Iterator
from datetime import date, datetime, time, timedelta, timezone
from typing import TYPE_CHECKING

import pytest
from pydantic import ValidationError

from rquant.factor.capability import HISTORICAL_DAILY_V1
from rquant.factor.definition import build_factor_definition
from rquant.factor.time_series import (
    DecisionTime,
    FactorTimeSeriesInput,
    FeatureObservation,
    evaluate_factor_time_series,
)
from rquant.factor.universe import (
    DailyIndexConstituentBatch,
    DailySecurityBatch,
    DailySecurityFact,
    FactorUniverseRequest,
)

if TYPE_CHECKING:
    from rquant.factor.formula_stream import (
        FactorFormulaStreamBatch,
        FactorFormulaStreamRequest,
    )
    from rquant.factor.universe import UniverseSelection

_TZ = timezone(timedelta(hours=8))
_FIRST = date(2026, 7, 1)
_DAYS = tuple(_FIRST + timedelta(days=index) for index in range(5))
_AS_OF = datetime(2026, 9, 30, 16, tzinfo=_TZ)
_CODES = ("000001.SZ", "000002.SZ", "000003.SZ")


def _at(day: date, hour: int = 15) -> datetime:
    return datetime.combine(day, time(hour), _TZ)


def _request(
    expression: str,
    *,
    days: tuple[date, ...] = _DAYS,
    codes: tuple[str, ...] = _CODES,
    selection: UniverseSelection = "all",
    earliest: date | None = None,
) -> FactorFormulaStreamRequest:
    from rquant.factor.formula_stream import FactorFormulaStreamRequest, FactorFormulaStreamSources

    return FactorFormulaStreamRequest(
        definition=build_factor_definition(
            factor_id="synthetic_formula_stream",
            name_zh="逐日公式样本",
            category="technical",
            direction="higher_is_better",
            version=1,
            earliest_available_date=earliest,
            expression=expression,
            feature_catalog=HISTORICAL_DAILY_V1.feature_catalog(),
        ),
        computation_stock_codes=codes,
        trading_days=days,
        decision_times=tuple(DecisionTime(trade_date=day, decision_at=_at(day)) for day in days),
        as_of=_AS_OF,
        selection=selection,
        sources=FactorFormulaStreamSources(
            source_mode="historical_retrospective",
            feature_source_id="synthetic-features",
            feature_source_sha256="a" * 64,
            security_source_id="synthetic-securities",
            security_source_sha256="b" * 64,
            index_source_id="synthetic-members" if selection in ("hs300", "zz1000") else None,
            index_source_sha256="c" * 64 if selection in ("hs300", "zz1000") else None,
        ),
    )


def _batch(
    request: FactorFormulaStreamRequest,
    day: date,
    *,
    members: tuple[str, ...] | None = None,
    values: dict[tuple[str, str], float | None] | None = None,
    missing: frozenset[tuple[str, str]] = frozenset(),
) -> FactorFormulaStreamBatch:
    from rquant.factor.formula_stream import (
        FactorFormulaFeaturePoint,
        FactorFormulaStreamBatch,
        factor_formula_stream_request_sha256,
    )

    members = request.computation_stock_codes if members is None else members
    index = request.trading_days.index(day)
    securities = DailySecurityBatch(
        trade_date=day,
        source_id=request.sources.security_source_id,
        source_sha256=request.sources.security_source_sha256,
        source_mode="historical_retrospective",
        security_scope="china_a_share",
        observed_at=request.as_of,
        complete_stock_codes=request.computation_stock_codes,
        facts=tuple(
            DailySecurityFact(
                stock_code=code,
                exchange=code[-2:],
                board="gem" if request.selection == "gem" else "main",
                is_listed=True,
                is_st=code not in members,
            )
            for code in request.computation_stock_codes
        ),
    )
    membership = None
    if request.selection in ("hs300", "zz1000"):
        membership = DailyIndexConstituentBatch(
            selection=request.selection,
            trade_date=day,
            source_id=request.sources.index_source_id,
            source_sha256=request.sources.index_source_sha256,
            source_mode="historical_retrospective",
            source_kind="daily_complete_membership",
            observed_at=request.as_of,
            stock_codes=members,
        )
    points = []
    for stock_index, code in enumerate(request.computation_stock_codes):
        for column in request.definition.dependency_columns:
            key = (code, column)
            value = (values or {}).get(key, float((index + 1) * (stock_index + 1)))
            state = (
                "missing_observation"
                if key in missing
                else "known_null"
                if value is None
                else "value"
            )
            points.append(
                FactorFormulaFeaturePoint(
                    stock_code=code,
                    trade_date=day,
                    column=column,
                    state=state,
                    value=None if state == "missing_observation" else value,
                    first_visible_at=None
                    if state == "missing_observation"
                    else _at(day, 14) + timedelta(minutes=stock_index % 50),
                )
            )
    return FactorFormulaStreamBatch(
        request_sha256=factor_formula_stream_request_sha256(request),
        sources=request.sources,
        universe=FactorUniverseRequest(
            selection=request.selection,
            trade_date=day,
            as_of=request.as_of,
            securities=securities,
            membership=membership,
        ),
        feature_points=tuple(points),
    )


def _legacy(
    request: FactorFormulaStreamRequest, batches: tuple[FactorFormulaStreamBatch, ...]
) -> tuple:
    rows = tuple(
        FeatureObservation(
            stock_code=point.stock_code,
            trade_date=point.trade_date,
            column=point.column,
            value=point.value,
            first_visible_at=point.first_visible_at,
        )
        for batch in batches
        for point in batch.feature_points
        if point.state != "missing_observation"
    )
    return evaluate_factor_time_series(
        FactorTimeSeriesInput(
            definition=request.definition,
            universe=request.computation_stock_codes,
            trading_days=request.trading_days,
            decision_times=request.decision_times,
            observations=rows,
        )
    ).values


@pytest.mark.parametrize(
    "expression",
    [
        "42",
        "+close",
        "-close",
        "close + open",
        "close - open",
        "close * open",
        "close / open",
        "close < open",
        "close <= open",
        "close > open",
        "close >= open",
        "ref(close, 0)",
        "ref(close, 2)",
        "ts_mean(close, 3)",
        "ts_std(close, 3)",
        "ts_delta(close, 2)",
        "ts_rank(close, 3)",
        "ts_corr(close, vol, 3)",
        "ts_std(close, 1)",
        "ts_corr(close, vol, 1)",
        "cs_rank(close)",
        "cs_zscore(close)",
        "cs_winsorize(close, 1)",
        "ref(cs_rank(close), 1)",
        "ts_mean(cs_rank(close), 3)",
        "cs_rank(ref(close, 1))",
        "cs_rank(ts_mean(close, 3))",
        "ts_mean(ref(close, 1), 2) + ts_delta(vol, 1) / 2",
        "ref(ts_mean(cs_zscore(close), 2), 1)",
        "ts_corr(cs_rank(close), ts_mean(open, 2), 2)",
    ],
)
def test_every_runnable_operator_and_nested_formula_matches_existing_evaluator(
    expression: str,
) -> None:
    from rquant.factor.formula_stream import evaluate_factor_formula_stream

    request = _request(expression)
    batches = tuple(
        _batch(request, day, values={(_CODES[1], "close"): 4.0, (_CODES[1], "vol"): 1.0})
        for day in request.trading_days
    )
    stream = evaluate_factor_formula_stream(request, iter(batches))
    assert stream.completion is None
    actual = tuple(point for day in stream for point in day.values)
    assert actual == _legacy(request, batches)
    assert stream.completion is not None
    assert stream.completion.processed_days == len(request.trading_days)


def test_dynamic_pool_uses_each_nodes_own_historical_membership() -> None:
    from rquant.factor.formula_stream import evaluate_factor_formula_stream

    request = _request("ref(cs_rank(close), 1)", days=_DAYS[:2])
    first = _batch(request, request.trading_days[0], members=_CODES[:2])
    second = _batch(request, request.trading_days[1], members=_CODES[1:])
    days = tuple(evaluate_factor_formula_stream(request, (first, second)))
    assert days[1].universe.stock_codes == _CODES[1:]
    assert days[1].values[0].value == 1.0
    assert days[1].values[0].latest_visible_at == _at(_DAYS[0], 14) + timedelta(minutes=1)
    assert days[1].values[1].value is None
    assert days[1].values[1].missing_reason == "missing_observation"


def test_one_shot_source_is_not_materialized_and_completion_requires_tail_validation() -> None:
    from rquant.factor.formula_stream import evaluate_factor_formula_stream

    request = _request("close", days=_DAYS[:2])

    class Source:
        calls = 0

        def __iter__(self) -> Iterator[FactorFormulaStreamBatch]:
            self.calls += 1
            assert self.calls == 1
            for day in request.trading_days:
                yield _batch(request, day)

        def __len__(self) -> int:
            raise AssertionError("materialized source")

    source = Source()
    stream = evaluate_factor_formula_stream(request, source)
    assert stream.completion is None
    next(stream)
    next(stream)
    assert stream.completion is None
    with pytest.raises(StopIteration):
        next(stream)
    assert stream.completion is not None
    assert source.calls == 1


@pytest.mark.parametrize(
    "expression",
    [
        "close",
        "close + open",
        "close / open",
        "ref(close, 1)",
        "ts_delta(close, 2)",
        "ts_mean(close, 3)",
        "ts_std(close, 3)",
        "ts_rank(close, 3)",
        "ts_corr(close, vol, 3)",
        "cs_rank(close)",
        "cs_zscore(close)",
        "cs_winsorize(close, 2)",
        "ts_mean(cs_rank(close), 2)",
        "cs_zscore(ts_mean(close, 2))",
    ],
)
def test_explicit_absence_and_known_null_match_existing_missing_reasons(expression: str) -> None:
    from rquant.factor.formula_stream import evaluate_factor_formula_stream

    request = _request(expression)
    batches = tuple(
        _batch(
            request,
            day,
            missing=frozenset({(_CODES[0], "close")}) if index == 1 else frozenset(),
            values={(_CODES[1], "close"): None} if index == 2 else None,
        )
        for index, day in enumerate(request.trading_days)
    )
    actual = tuple(
        point for day in evaluate_factor_formula_stream(request, batches) for point in day.values
    )
    assert actual == _legacy(request, batches)


@pytest.mark.parametrize(
    ("expression", "values"),
    [
        ("close / open", {"close": 1.0, "open": 0.0}),
        ("close / open", {"close": 1e308, "open": 1e-308}),
        ("close * open", {"close": 1e-300, "open": 1e-300}),
        ("close + open", {"close": 1e20, "open": 1.0}),
        ("close - open", {"close": 1e20, "open": 1.0}),
        ("cs_winsorize(close, 10)", {"close": 1e308}),
        ("cs_zscore(close)", {"close": 1e308}),
        ("ts_std(close, 3)", {"close": 1e-320}),
    ],
)
def test_precision_overflow_zero_variance_and_extreme_values_match_existing_evaluator(
    expression: str,
    values: dict[str, float],
) -> None:
    from rquant.factor.formula_stream import evaluate_factor_formula_stream

    request = _request(expression)
    batches = tuple(
        _batch(
            request,
            day,
            values={
                (code, column): value * (-1 if stock_index == 0 and column == "close" else 1)
                for stock_index, code in enumerate(_CODES)
                for column, value in values.items()
            },
        )
        for day in request.trading_days
    )
    assert tuple(
        point for day in evaluate_factor_formula_stream(request, batches) for point in day.values
    ) == _legacy(request, batches)


def test_raw_history_survives_pool_exit_and_supports_reentry_and_earliest_date() -> None:
    from rquant.factor.formula_stream import evaluate_factor_formula_stream

    request = _request("ts_mean(close, 3)", earliest=_DAYS[2])
    batches = tuple(
        _batch(request, day, members=_CODES[:1] if index < 2 else _CODES)
        for index, day in enumerate(request.trading_days)
    )
    days = tuple(evaluate_factor_formula_stream(request, batches))
    assert (
        days[0].values[0].missing_reason
        == days[1].values[0].missing_reason
        == "before_available_date"
    )
    assert [point.value for point in days[2].values] == pytest.approx((2.0, 4.0, 6.0))
    assert all(
        point.latest_visible_at == _at(_DAYS[2], 14) + timedelta(minutes=index)
        for index, point in enumerate(days[2].values)
    )


def test_rolling_historical_cross_section_is_not_recomputed_with_reentry_members() -> None:
    from rquant.factor.formula_stream import evaluate_factor_formula_stream

    request = _request("ts_mean(cs_rank(close), 2)", days=_DAYS[:3])
    result = tuple(
        evaluate_factor_formula_stream(
            request,
            (
                _batch(request, request.trading_days[0], members=_CODES[:2]),
                _batch(request, request.trading_days[1], members=_CODES[1:]),
                _batch(request, request.trading_days[2], members=_CODES),
            ),
        )
    )
    assert result[1].values[0].value == pytest.approx(0.75)
    assert result[1].values[1].missing_reason == "missing_observation"
    assert result[2].values[0].missing_reason == "missing_observation"
    assert [point.value for point in result[2].values[1:]] == pytest.approx((7 / 12, 1.0))


@pytest.mark.parametrize("expression", ["5", "cs_rank(close)"])
def test_empty_daily_selection_is_a_legal_intermediate_cross_section(expression: str) -> None:
    from rquant.factor.formula_stream import evaluate_factor_formula_stream

    request = _request(expression, days=_DAYS[:1])
    stream = evaluate_factor_formula_stream(request, (_batch(request, _DAYS[0], members=()),))
    day = next(stream)
    assert day.universe.selected_count == 0
    assert day.universe.excluded.st == 3
    assert day.values == ()
    assert stream.completion is None
    with pytest.raises(StopIteration):
        next(stream)
    assert stream.completion is not None


def test_constant_has_no_feature_rows_or_source_visibility() -> None:
    from rquant.factor.formula_stream import evaluate_factor_formula_stream

    request = _request("5", days=_DAYS[:1])
    batch = _batch(request, _DAYS[0])
    assert batch.feature_points == ()
    day = next(evaluate_factor_formula_stream(request, (batch,)))
    assert all(point.value == 5.0 and point.latest_visible_at is None for point in day.values)


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ("missing", "missing_batch"),
        ("extra", "unexpected_batch"),
        ("reversed", "date_order_mismatch"),
        ("duplicate", "date_order_mismatch"),
    ],
)
def test_bad_source_schedule_never_creates_completion(change: str, reason: str) -> None:
    from rquant.factor.formula_stream import evaluate_factor_formula_stream

    request = _request("close", days=_DAYS[:2])
    batches = tuple(_batch(request, day) for day in request.trading_days)
    altered = {
        "missing": batches[:1],
        "extra": batches + batches[:1],
        "reversed": batches[::-1],
        "duplicate": batches[:1] * 2,
    }[change]
    stream = evaluate_factor_formula_stream(request, iter(altered))
    with pytest.raises(ValueError, match=reason):
        tuple(stream)
    assert stream.completion is None
    assert stream._engine.history == {}
    with pytest.raises(StopIteration):
        next(stream)
    assert stream.completion is None


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ("request", "request_binding_mismatch"),
        ("batch_source", "source_binding_mismatch"),
        ("security_id", "source_binding_mismatch"),
        ("security_digest", "source_binding_mismatch"),
        ("index_id", "source_binding_mismatch"),
        ("index_digest", "source_binding_mismatch"),
        ("selection", "selection_mismatch"),
        ("as_of", "as_of_mismatch"),
    ],
)
def test_bindings_compare_real_request_batch_and_security_index_source_fields(
    change: str,
    reason: str,
) -> None:
    from rquant.factor.formula_stream import evaluate_factor_formula_stream

    request = _request("close", days=_DAYS[:1], selection="hs300")
    batch = _batch(request, _DAYS[0])
    if change == "request":
        batch = batch.model_copy(update={"request_sha256": "f" * 64})
    elif change == "batch_source":
        batch = batch.model_copy(
            update={"sources": batch.sources.model_copy(update={"feature_source_id": "other"})}
        )
    elif change in ("selection", "as_of"):
        update = (
            {"selection": "zz1000"}
            if change == "selection"
            else {"as_of": request.as_of + timedelta(seconds=1)}
        )
        batch = batch.model_copy(update={"universe": batch.universe.model_copy(update=update)})
    else:
        field = "membership" if change.startswith("index") else "securities"
        row = getattr(batch.universe, field)
        update = {"source_id": "other"} if change.endswith("id") else {"source_sha256": "f" * 64}
        batch = batch.model_copy(
            update={
                "universe": batch.universe.model_copy(update={field: row.model_copy(update=update)})
            }
        )
    stream = evaluate_factor_formula_stream(request, (batch,))
    with pytest.raises(ValueError, match=reason):
        tuple(stream)
    assert stream.completion is None


@pytest.mark.parametrize(
    "change", ["missing", "extra_column", "extra_stock", "duplicate", "date", "future"]
)
def test_daily_feature_grid_and_visibility_are_checked_before_yield(change: str) -> None:
    from rquant.factor.formula_stream import evaluate_factor_formula_stream

    request = _request("close", days=_DAYS[:1])
    batch = _batch(request, _DAYS[0])
    points = batch.feature_points
    update = {
        "extra_column": {"column": "open"},
        "extra_stock": {"stock_code": "999999.SZ"},
        "date": {"trade_date": _DAYS[1]},
        "future": {"first_visible_at": _at(_DAYS[0]) + timedelta(seconds=1)},
    }
    altered = (
        points[:1]
        if change == "missing"
        else points + points[:1]
        if change == "duplicate"
        else ((points[0].model_copy(update=update[change]),) + points[1:])
    )
    stream = evaluate_factor_formula_stream(
        request, (batch.model_copy(update={"feature_points": altered}),)
    )
    with pytest.raises(ValueError):
        next(stream)
    assert stream.completion is None


def test_universe_selector_refusals_are_propagated_instead_of_accepting_a_manual_pool() -> None:
    from rquant.factor.formula_stream import evaluate_factor_formula_stream

    request = _request("close", days=_DAYS[:1])
    batch = _batch(request, _DAYS[0])
    batch = batch.model_copy(
        update={"universe": batch.universe.model_copy(update={"securities": None})}
    )
    with pytest.raises(ValueError, match="security_source_missing"):
        next(evaluate_factor_formula_stream(request, (batch,)))


def test_security_manifest_may_not_exceed_fixed_computation_scope_even_if_excluded() -> None:
    from rquant.factor.formula_stream import evaluate_factor_formula_stream

    request = _request("close", days=_DAYS[:1], codes=_CODES[:2])
    batch = _batch(request, _DAYS[0])
    security = batch.universe.securities
    extra = DailySecurityFact(
        stock_code=_CODES[2], exchange="SZ", board="main", is_listed=False, is_st=None
    )
    security = security.model_copy(
        update={"complete_stock_codes": _CODES, "facts": security.facts + (extra,)}
    )
    batch = batch.model_copy(
        update={"universe": batch.universe.model_copy(update={"securities": security})}
    )
    with pytest.raises(ValueError, match="security_outside_computation_scope"):
        next(evaluate_factor_formula_stream(request, (batch,)))


@pytest.mark.parametrize(
    "update",
    [
        {"state": "value", "value": None},
        {"state": "known_null"},
        {"state": "known_null", "value": None, "first_visible_at": None},
        {"state": "missing_observation"},
        {"first_visible_at": None},
        {"value": float("inf")},
        {"value": True},
        {"stock_code": "A"},
        {"first_visible_at": datetime(2026, 7, 1, 14)},
        {"column": "volume"},
    ],
)
def test_feature_point_state_and_primitive_fields_revalidate_copied_instances(update: dict) -> None:
    from rquant.factor.formula_stream import FactorFormulaFeaturePoint

    point = _batch(_request("close", days=_DAYS[:1]), _DAYS[0]).feature_points[0]
    with pytest.raises(ValidationError):
        FactorFormulaFeaturePoint.model_validate(point.model_copy(update=update))


@pytest.mark.parametrize(
    "change",
    ["duplicate", "reverse", "decision_date", "future_decision", "too_many_codes", "too_many_days"],
)
def test_request_calendar_cutoffs_codes_and_capacity_are_frozen(change: str) -> None:
    from rquant.factor.formula_stream import FactorFormulaStreamRequest

    request = _request("close")
    updates = {
        "duplicate": {"trading_days": (_DAYS[0],) * 5},
        "reverse": {"trading_days": _DAYS[::-1]},
        "decision_date": {"decision_times": request.decision_times[::-1]},
        "future_decision": {"as_of": _at(_DAYS[0]) - timedelta(seconds=1)},
        "too_many_codes": {
            "computation_stock_codes": tuple(f"{index:06d}.SZ" for index in range(7001))
        },
        "too_many_days": {
            "trading_days": tuple(_FIRST + timedelta(days=index) for index in range(1025))
        },
    }
    with pytest.raises(ValidationError):
        FactorFormulaStreamRequest.model_validate(request.model_copy(update=updates[change]))


@pytest.mark.parametrize("expression", ["industry_neutralize(close)", "size_neutralize(close)"])
def test_unavailable_context_operators_remain_refused(expression: str) -> None:
    with pytest.raises(ValidationError, match="factor_formula_stream_unsupported_definition"):
        _request(expression)


def test_unknown_catalog_fields_remain_refused_even_if_unused() -> None:
    from rquant.factor.expression import FeatureCatalog
    from rquant.factor.formula_stream import FactorFormulaStreamRequest

    request = _request("close")
    definition = build_factor_definition(
        factor_id="unknown",
        name_zh="未知目录",
        category="technical",
        direction="higher_is_better",
        version=1,
        earliest_available_date=None,
        expression="close",
        feature_catalog=FeatureCatalog(columns=("close", "industry")),
    )
    with pytest.raises(ValidationError, match="factor_formula_stream_unsupported_definition"):
        FactorFormulaStreamRequest.model_validate(
            request.model_copy(update={"definition": definition})
        )


def test_cache_budget_refuses_before_iterating_or_fetching_source() -> None:
    from rquant.factor.formula_stream import evaluate_factor_formula_stream

    request = _request(
        "ts_mean(close, 252) + ts_mean(vol, 252)",
        codes=tuple(f"{index:06d}.SZ" for index in range(7000)),
    )

    class Untouched:
        def __iter__(self) -> Iterator:
            raise AssertionError("source touched before cache admission")

    with pytest.raises(ValueError, match="cache_budget_exceeded"):
        evaluate_factor_formula_stream(request, Untouched())


def test_ast_cache_evicts_old_cells_using_actual_parent_windows() -> None:
    from rquant.factor.formula_stream import evaluate_factor_formula_stream

    days = tuple(_FIRST + timedelta(days=index) for index in range(8))
    request = _request("ts_mean(ref(close, 2), 3)", days=days)
    stream = evaluate_factor_formula_stream(request, (_batch(request, day) for day in days))
    assert stream.cache_slots == len(_CODES) * (3 + 3 + 1 + 1 + 1)
    next(stream)
    old_cell = next(iter(stream._engine.history.values()))[0][1][0]
    for _index in range(1, len(days)):
        next(stream)
        assert all(len(rows) <= rows.maxlen for rows in stream._engine.history.values())
    assert all(
        cell is not old_cell
        for rows in stream._engine.history.values()
        for _, cells in rows
        for cell in cells
    )
    with pytest.raises(StopIteration):
        next(stream)
    assert stream._engine.history == {}


def test_order_normalization_preserves_day_chain_and_completion_digests() -> None:
    from rquant.factor.formula_stream import evaluate_factor_formula_stream

    request = _request("cs_rank(ts_mean(close, 2))")
    batches = tuple(_batch(request, day) for day in request.trading_days)
    reversed_batches = tuple(
        batch.model_copy(
            update={
                "feature_points": batch.feature_points[::-1],
                "universe": batch.universe.model_copy(
                    update={
                        "securities": batch.universe.securities.model_copy(
                            update={
                                "complete_stock_codes": request.computation_stock_codes[::-1],
                                "facts": batch.universe.securities.facts[::-1],
                            }
                        )
                    }
                ),
            }
        )
        for batch in batches
    )
    left = evaluate_factor_formula_stream(request, batches)
    right = evaluate_factor_formula_stream(
        request.model_copy(update={"computation_stock_codes": _CODES[::-1]}), reversed_batches
    )
    assert tuple(left) == tuple(right)
    assert left.completion == right.completion


@pytest.mark.parametrize(
    "change", ["value", "visible", "membership", "definition", "cutoff", "source", "window"]
)
def test_fact_identity_changes_digest_and_history_chain_without_rewriting_prior_days(
    change: str,
) -> None:
    from rquant.factor.formula_stream import evaluate_factor_formula_stream

    request = _request("ref(close, 0)")
    batches = tuple(_batch(request, day) for day in request.trading_days)
    original = evaluate_factor_formula_stream(request, batches)
    original_days = tuple(original)
    if change in ("value", "visible", "membership"):
        changed = list(batches)
        batch = batches[1]
        if change == "membership":
            changed[1] = _batch(request, _DAYS[1], members=_CODES[:2])
        else:
            point = batch.feature_points[0].model_copy(
                update={"value": 99.0}
                if change == "value"
                else {"first_visible_at": _at(_DAYS[1], 14) + timedelta(minutes=20)}
            )
            changed[1] = batch.model_copy(
                update={"feature_points": (point,) + batch.feature_points[1:]}
            )
        stream = evaluate_factor_formula_stream(request, iter(changed))
        result = tuple(stream)
        assert result[0] == original_days[0]
        assert result[1].input_sha256 != original_days[1].input_sha256
        assert result[-1].history_sha256 != original_days[-1].history_sha256
    else:
        if change in ("definition", "window"):
            request = _request("close + 1" if change == "definition" else "ref(close, 1)")
        elif change == "cutoff":
            request = request.model_copy(update={"as_of": request.as_of + timedelta(seconds=1)})
        else:
            request = request.model_copy(
                update={
                    "sources": request.sources.model_copy(update={"feature_source_id": "other"})
                }
            )
        stream = evaluate_factor_formula_stream(
            request, (_batch(request, day) for day in request.trading_days)
        )
        result = tuple(stream)
    assert result[-1].sha256 != original_days[-1].sha256
    assert stream.completion.sha256 != original.completion.sha256


def test_source_batches_feature_points_and_security_facts_release_before_next_day() -> None:
    from rquant.factor.formula_stream import evaluate_factor_formula_stream

    request = _request("cs_rank(ts_mean(close, 2))")
    released = []

    def source() -> Iterator[FactorFormulaStreamBatch]:
        for day in request.trading_days:
            batch = _batch(request, day)
            refs = (
                weakref.ref(batch),
                weakref.ref(batch.feature_points[0]),
                weakref.ref(batch.universe.securities.facts[0]),
            )
            yield batch
            del batch
            assert all(ref() is None for ref in refs)
            released.append(True)

    stream = evaluate_factor_formula_stream(request, source())
    for day in stream:
        assert len(day.values) == len(_CODES)
        del day
    assert len(released) == len(request.trading_days)
    assert stream.completion is not None


@pytest.mark.parametrize("failure", ["iterator_exception", "extra_day"])
def test_downstream_statistics_refuse_formula_run_failure_after_intermediate_yields(
    failure: str,
) -> None:
    from rquant.factor.daily_stream import (
        FactorDailyStreamBatch,
        FactorDailyStreamRequest,
        FactorDailyStreamSources,
        evaluate_factor_daily_stream,
        factor_daily_stream_request_sha256,
    )
    from rquant.factor.formula_stream import evaluate_factor_formula_stream
    from rquant.factor.result import FactorForwardReturn

    request = _request("close", days=_DAYS[:2])

    def raw_source() -> Iterator[FactorFormulaStreamBatch]:
        for index, day in enumerate(request.trading_days):
            yield _batch(request, day)
            if index == 0 and failure == "iterator_exception":
                raise RuntimeError("synthetic formula source failed")
        yield _batch(request, _DAYS[0])

    formula = evaluate_factor_formula_stream(request, raw_source())
    statistics = FactorDailyStreamRequest(
        definition=request.definition,
        selection=request.selection,
        evaluation_days=request.trading_days,
        as_of=request.as_of,
        return_price_basis="raw",
        holding_sessions=1,
        sources=FactorDailyStreamSources(
            source_mode="historical_retrospective",
            universe_source_id=request.sources.security_source_id,
            universe_source_sha256=request.sources.security_source_sha256,
            factor_source_id=request.sources.feature_source_id,
            factor_source_sha256=request.sources.feature_source_sha256,
            return_source_id="synthetic-returns",
            return_source_sha256="d" * 64,
        ),
    )
    yielded = []

    def paired() -> Iterator[FactorDailyStreamBatch]:
        for day in formula:
            yielded.append(day.universe.trade_date)
            end_at = day.decision_at + timedelta(days=1)
            yield FactorDailyStreamBatch(
                request_sha256=factor_daily_stream_request_sha256(statistics),
                sources=statistics.sources,
                universe=day.universe,
                decision_at=day.decision_at,
                return_end_at=end_at,
                factor_values=day.values,
                forward_returns=tuple(
                    FactorForwardReturn(
                        stock_code=point.stock_code,
                        decision_date=point.trade_date,
                        decision_at=day.decision_at,
                        return_end_at=end_at,
                        value=0.01,
                        missing_reason=None,
                        first_available_at=end_at,
                    )
                    for point in day.values
                ),
            )

    with pytest.raises(
        ValueError if failure == "extra_day" else RuntimeError,
        match="unexpected_batch" if failure == "extra_day" else "synthetic formula source failed",
    ):
        evaluate_factor_daily_stream(statistics, paired())
    assert len(yielded) == (2 if failure == "extra_day" else 1)
    assert formula.completion is None


def test_all_new_transport_models_are_frozen_strict_extra_forbidden_and_revalidate() -> None:
    from rquant.factor.formula_stream import evaluate_factor_formula_stream

    request = _request("close", days=_DAYS[:1])
    batch = _batch(request, _DAYS[0])
    stream = evaluate_factor_formula_stream(request, (batch,))
    (day,) = tuple(stream)
    for model in (request, request.sources, batch, batch.feature_points[0], day, stream.completion):
        assert (
            model.model_config["strict"] and model.model_config["revalidate_instances"] == "always"
        )
        with pytest.raises(ValidationError) as extra:
            type(model).model_validate({**model.model_dump(), "undeclared": True})
        assert extra.value.errors()[0]["type"] == "extra_forbidden"
        field, value = next(iter(model.model_dump().items()))
        with pytest.raises(ValidationError) as frozen:
            setattr(model, field, value)
        assert frozen.value.errors()[0]["type"] == "frozen_instance"


def test_large_daily_rolling_cross_section_once_records_bounded_allocations_and_release() -> None:
    from rquant.factor.formula_stream import evaluate_factor_formula_stream

    request = _request(
        "cs_rank(ts_mean(close, 3))",
        days=tuple(_FIRST + timedelta(days=index) for index in range(16)),
        codes=tuple(f"{index:06d}.SZ" for index in range(7000)),
    )
    released = []

    def source() -> Iterator[FactorFormulaStreamBatch]:
        for day in request.trading_days:
            batch = _batch(request, day)
            refs = (
                weakref.ref(batch),
                weakref.ref(batch.feature_points[0]),
                weakref.ref(batch.universe.securities.facts[0]),
            )
            yield batch
            del batch
            assert all(ref() is None for ref in refs)
            released.append(True)

    consumed = 0
    tracemalloc.start()
    try:
        stream = evaluate_factor_formula_stream(request, source())
        for day in stream:
            assert len(day.values) == day.universe.selected_count == 7000
            if consumed >= 2:
                assert day.values[0].value == pytest.approx(1 / 7000)
                assert day.values[-1].value == 1.0
            consumed += 1
            del day
        retained, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    print(
        f"synthetic_formula_memory days=16 stocks_per_day=7000 feature_points=112000 "
        f"cache_slots={stream.cache_slots} traced_retained_bytes={retained} "
        f"traced_peak_bytes={peak} released_batches={len(released)}"
    )
    assert consumed == len(released) == stream.completion.processed_days == 16
    assert stream._engine.history == {}
    assert peak < 256 * 1024 * 1024


def _maximum_formula_batch(
    columns: tuple[str, ...], *, codes: tuple[str, ...] | None = None
) -> FactorFormulaStreamBatch:
    from rquant.factor.formula_stream import FactorFormulaStreamBatch, FactorFormulaStreamSources

    day = date(2026, 10, 2)
    at = datetime.combine(day, time(9, 25), _TZ)
    codes = codes if codes is not None else tuple(f"{index:06d}.SZ" for index in range(1, 7001))
    securities = DailySecurityBatch(
        trade_date=day,
        source_id="synthetic-width-securities",
        source_sha256="b" * 64,
        source_mode="historical_retrospective",
        security_scope="china_a_share",
        observed_at=at,
        complete_stock_codes=codes,
        facts=tuple(
            DailySecurityFact(
                stock_code=code, exchange="SZ", board="main", is_listed=True, is_st=False
            )
            for code in codes
        ),
    )
    return FactorFormulaStreamBatch(
        request_sha256="c" * 64,
        sources=FactorFormulaStreamSources(
            source_mode="historical_retrospective",
            feature_source_id="synthetic-width-prices",
            feature_source_sha256="a" * 64,
            security_source_id=securities.source_id,
            security_source_sha256=securities.source_sha256,
        ),
        universe=FactorUniverseRequest(
            selection="all", trade_date=day, as_of=at, securities=securities
        ),
        feature_points=tuple(
            {
                "stock_code": code,
                "trade_date": day,
                "column": column,
                "state": "value",
                "value": 1.0,
                "first_visible_at": at,
            }
            for code in codes
            for column in columns
        ),
    )


def _minute_batch_columns(*, minute: bool, native: bool) -> tuple[str, ...]:
    from rquant.factor.daily_feature_source import (
        DERIVED_DAILY_FIELDS,
        MINUTE_FEATURE_FIELDS,
        STOCK_FEATURE_FIELDS,
    )

    fields = DERIVED_DAILY_FIELDS + STOCK_FEATURE_FIELDS + (MINUTE_FEATURE_FIELDS if minute else ())
    return tuple(field.column for field in fields) + (
        ("open", "high", "low", "close", "vol", "amount") if native else ()
    )


@pytest.mark.parametrize(
    "minute,native,width",
    ((False, True, 45), (True, False, 50), (True, True, 56)),
    ids=("legacy45", "minute50", "minute56"),
)
def test_minute_maximum_formula_batch_grid(minute: bool, native: bool, width: int) -> None:
    columns = _minute_batch_columns(minute=minute, native=native)
    assert len(columns) == len(set(columns)) == width
    batch = _maximum_formula_batch(columns)
    assert len(batch.universe.securities.complete_stock_codes) == 7000
    assert len(batch.feature_points) == 7000 * width
    assert all(point.state == "value" and point.value == 1.0 for point in batch.feature_points)


def test_minute_formula_batch_rejects_one_point_over_finite_limit() -> None:
    from rquant.factor.formula_stream import FactorFormulaStreamBatch

    batch = _maximum_formula_batch(_minute_batch_columns(minute=True, native=True))
    payload = {name: getattr(batch, name) for name in FactorFormulaStreamBatch.model_fields}
    payload["feature_points"] = batch.feature_points + (batch.feature_points[0],)
    with pytest.raises(ValidationError) as caught:
        FactorFormulaStreamBatch.model_validate(payload)
    (error,) = caught.value.errors(include_input=False, include_url=False)
    assert error["loc"] == ("feature_points",) and error["type"] == "too_long"
    assert error["ctx"]["max_length"] == 7000 * 56
    assert error["ctx"]["actual_length"] == 7000 * 56 + 1


def test_original_six_formula_batch_canonical_bytes_unchanged() -> None:
    import hashlib

    from rquant.strict_json import canonical_json_bytes

    batch = _maximum_formula_batch(("open", "high", "low", "close", "vol", "amount"), codes=_CODES)
    data = canonical_json_bytes(batch.model_dump(mode="json"))
    assert (
        hashlib.sha256(data).hexdigest()
        == "e72e94bf8c3c21f5d11fb8cc44c0a17201569486d21df719275307e0e9119fbe"
    )
