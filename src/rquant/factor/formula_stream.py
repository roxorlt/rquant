"""Daily formula facts over trusted retrospective sources and bounded AST history.

Yielded days are intermediate facts. Only natural exhaustion after checking the
source tail creates a completion receipt; source digests are caller bindings.
"""

from __future__ import annotations

import ast
from collections import deque
from collections.abc import Generator, Iterable, Iterator
from dataclasses import dataclass
from datetime import date
from itertools import pairwise
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic_core import PydanticCustomError

from rquant.factor.capability import historical_daily_capabilities
from rquant.factor.daily_feature_source import (
    DailyStoredColumn,
    FactorDailyFeatureInput,
    FactorDailyFeatureSources,
)
from rquant.factor.definition import FactorDefinition
from rquant.factor.neutralization_context import (
    FactorNeutralizationDayBatch,
    FactorNeutralizationSources,
    require_factor_neutralization_binding,
)
from rquant.factor.run_request import NeutralizationMode
from rquant.factor.time_series import (
    MAX_TRADE_DAYS,
    DecisionTime,
    FactorTimeSeriesError,
    FactorTimeSeriesValue,
    FiniteValue,
    _Cell,
    _latest,
    _literal_integer,
    _literal_number,
    _missing,
    _present,
    _SeriesEvaluator,
    neutralize_factor_cells,
)
from rquant.factor.universe import (
    MAX_UNIVERSE_SECURITIES,
    FactorUniverseRequest,
    FactorUniverseResult,
    ObservedTime,
    Sha256,
    SourceId,
    StockCode,
    UniverseSelection,
    select_factor_universe,
)
from rquant.runtime_contracts import canonical_sha256

MAX_FORMULA_CACHE_SLOTS = 2_000_000
_IMMUTABLE = ConfigDict(extra="forbid", frozen=True, strict=True, revalidate_instances="always")
_CS_FUNCTIONS = frozenset(
    {"cs_rank", "cs_zscore", "cs_winsorize", "industry_neutralize", "size_neutralize"}
)
DailyFeatureColumn = Literal["open", "high", "low", "close", "vol", "amount"] | DailyStoredColumn
FormulaStreamErrorReason = Literal[
    "cache_budget_exceeded",
    "missing_batch",
    "unexpected_batch",
    "date_order_mismatch",
    "request_binding_mismatch",
    "source_binding_mismatch",
    "selection_mismatch",
    "as_of_mismatch",
    "security_outside_computation_scope",
    "feature_grid_mismatch",
    "feature_date_mismatch",
    "future_feature",
]


class FactorFormulaStreamError(ValueError):
    def __init__(self, reason: FormulaStreamErrorReason) -> None:
        self.reason = reason
        super().__init__(reason)


class FactorFormulaStreamSources(BaseModel):
    model_config = _IMMUTABLE

    source_mode: Literal["historical_retrospective"]
    feature_source_id: SourceId
    feature_source_sha256: Sha256
    security_source_id: SourceId
    security_source_sha256: Sha256
    index_source_id: SourceId | None = None
    index_source_sha256: Sha256 | None = None
    context: FactorNeutralizationSources | None = Field(
        default=None, exclude_if=lambda v: v is None
    )
    daily_features: FactorDailyFeatureSources | None = Field(
        default=None, exclude_if=lambda v: v is None
    )

    @model_validator(mode="after")
    def _index_binding(self) -> FactorFormulaStreamSources:
        if (self.index_source_id is None) != (self.index_source_sha256 is None):
            raise PydanticCustomError(
                "factor_formula_stream_invalid_index_binding", "index identity requires its digest"
            )
        return self


class FactorFormulaStreamRequest(BaseModel):
    model_config = _IMMUTABLE

    definition: FactorDefinition
    computation_stock_codes: tuple[StockCode, ...] = Field(max_length=MAX_UNIVERSE_SECURITIES)
    trading_days: tuple[date, ...] = Field(min_length=1, max_length=MAX_TRADE_DAYS)
    decision_times: tuple[DecisionTime, ...] = Field(min_length=1, max_length=MAX_TRADE_DAYS)
    as_of: ObservedTime
    selection: UniverseSelection
    sources: FactorFormulaStreamSources
    neutralization: NeutralizationMode = Field(default="none", exclude_if=lambda v: v == "none")
    mad_multiple: float | None = Field(
        default=None, strict=True, gt=0, allow_inf_nan=False, exclude_if=lambda v: v is None
    )

    @field_validator("computation_stock_codes")
    @classmethod
    def _fixed_codes(cls, codes: tuple[str, ...]) -> tuple[str, ...]:
        if len(set(codes)) != len(codes):
            raise PydanticCustomError("factor_formula_stream_duplicate_stock", "duplicate stock")
        return tuple(sorted(codes))

    @model_validator(mode="after")
    def _fixed_schedule(self) -> FactorFormulaStreamRequest:
        if any(left >= right for left, right in pairwise(self.trading_days)):
            raise PydanticCustomError(
                "factor_formula_stream_invalid_calendar", "calendar must ascend"
            )
        if tuple(item.trade_date for item in self.decision_times) != self.trading_days:
            raise PydanticCustomError(
                "factor_formula_stream_decision_mismatch", "decision dates differ from calendar"
            )
        if any(item.decision_at > self.as_of for item in self.decision_times):
            raise PydanticCustomError(
                "factor_formula_stream_future_decision", "decision follows request cutoff"
            )
        if (self.selection in ("hs300", "zz1000")) != (self.sources.index_source_id is not None):
            raise PydanticCustomError(
                "factor_formula_stream_index_selection_mismatch",
                "index binding differs from selection",
            )
        try:
            context = self.sources.context
            industry = context is not None and context.industry is not None
            cap = context is not None and context.market_cap is not None
            require_factor_neutralization_binding(
                context,
                mode=self.neutralization,
                snapshot_id=self.sources.feature_source_id,
                binding_hash=self.sources.feature_source_sha256,
                as_of=self.as_of,
            )
            historical_daily_capabilities(
                industry_available=industry,
                market_cap_available=cap,
                daily_features_available=self.sources.daily_features is not None,
                market_temperature_available=self.sources.daily_features is not None
                and self.sources.daily_features.market_temperature is not None,
                market_temperature_base_daily_available=self.sources.daily_features is not None
                and self.sources.daily_features.market_temperature is not None
                and any(
                    f.table in ("daily_indicator", "daily_basic")
                    for f in self.sources.daily_features.fields
                ),
                minute_features_available=self.sources.daily_features is not None
                and self.sources.daily_features.minute_features is not None,
                minute_base_daily_available=self.sources.daily_features is not None
                and self.sources.daily_features.minute_features is not None
                and any(
                    f.table in ("daily_indicator", "daily_basic")
                    for f in self.sources.daily_features.fields
                ),
                technical_history_available=self.sources.daily_features is not None
                and self.sources.daily_features.technical_history is not None,
                stock_features_available=self.sources.daily_features is not None
                and self.sources.daily_features.stock_features is not None,
                stock_base_daily_available=self.sources.daily_features is not None
                and self.sources.daily_features.stock_features is not None
                and any(
                    f.table in ("daily_indicator", "daily_basic")
                    for f in self.sources.daily_features.fields
                ),
            ).require_runnable_definition(
                self.definition.model_copy(
                    update={
                        "feature_catalog": type(self.definition.feature_catalog)(
                            columns=self.definition.dependency_columns
                        )
                    }
                )
                if self.sources.daily_features is not None
                and (
                    self.sources.daily_features.minute_features is not None
                    or self.sources.daily_features.market_temperature is not None
                )
                else self.definition
            )
            from rquant.factor.capability import HISTORICAL_DAILY_V1

            extra = tuple(
                c
                for c in self.definition.dependency_columns
                if c not in HISTORICAL_DAILY_V1.feature_catalog().columns
            )
            stored = self.sources.daily_features
            if (
                (stored is None) != (not extra)
                or stored is not None
                and (
                    tuple(f.column for f in stored.fields) != extra
                    or stored.prepared_snapshot_id != self.sources.feature_source_id
                    or stored.prepared_binding_hash != self.sources.feature_source_sha256
                )
            ):
                raise ValueError("stored feature dependency subset or raw binding differs")
        except ValueError as error:
            raise PydanticCustomError(
                "factor_formula_stream_unsupported_definition", "definition lacks a daily contract"
            ) from error
        return self


class FactorFormulaFeaturePoint(BaseModel):
    """Known null has an observation time; an absent observation does not."""

    model_config = _IMMUTABLE

    stock_code: StockCode
    trade_date: date
    column: DailyFeatureColumn
    state: Literal["value", "known_null", "missing_observation"]
    value: FiniteValue | None
    first_visible_at: ObservedTime | None

    @model_validator(mode="after")
    def _outcome(self) -> FactorFormulaFeaturePoint:
        if (
            (self.state == "value" and (self.value is None or self.first_visible_at is None))
            or (
                self.state == "known_null"
                and (self.value is not None or self.first_visible_at is None)
            )
            or (
                self.state == "missing_observation"
                and (self.value is not None or self.first_visible_at is not None)
            )
        ):
            raise PydanticCustomError(
                "factor_formula_stream_invalid_feature_state", "feature outcome is inconsistent"
            )
        return self


class FactorFormulaStreamBatch(BaseModel):
    model_config = _IMMUTABLE

    request_sha256: Sha256
    sources: FactorFormulaStreamSources
    universe: FactorUniverseRequest
    feature_points: tuple[FactorFormulaFeaturePoint, ...] = Field(
        max_length=MAX_UNIVERSE_SECURITIES * 58
    )
    context: FactorNeutralizationDayBatch | None = Field(
        default=None, exclude_if=lambda v: v is None
    )
    daily_features: FactorDailyFeatureInput | None = Field(
        default=None, exclude_if=lambda v: v is None
    )

    @field_validator("feature_points")
    @classmethod
    def _unique_points(
        cls, points: tuple[FactorFormulaFeaturePoint, ...]
    ) -> tuple[FactorFormulaFeaturePoint, ...]:
        keys = {(point.stock_code, point.column) for point in points}
        if len(keys) != len(points):
            raise PydanticCustomError(
                "factor_formula_stream_duplicate_feature", "duplicate daily feature key"
            )
        return tuple(sorted(points, key=lambda point: (point.stock_code, point.column)))


class FactorFormulaStreamDay(BaseModel):
    model_config = _IMMUTABLE

    factor_id: str
    version: int = Field(ge=1)
    definition_sha256: Sha256
    request_sha256: Sha256
    sources: FactorFormulaStreamSources
    universe: FactorUniverseResult
    decision_at: ObservedTime
    values: tuple[FactorTimeSeriesValue, ...] = Field(max_length=MAX_UNIVERSE_SECURITIES)
    input_sha256: Sha256
    history_sha256: Sha256
    sha256: Sha256

    @field_validator("values")
    @classmethod
    def _checked_values(
        cls, values: tuple[FactorTimeSeriesValue, ...]
    ) -> tuple[FactorTimeSeriesValue, ...]:
        return tuple(FactorTimeSeriesValue.model_validate(value.model_dump()) for value in values)

    @model_validator(mode="after")
    def _complete_selected_grid(self) -> FactorFormulaStreamDay:
        if tuple(point.stock_code for point in self.values) != self.universe.stock_codes:
            raise PydanticCustomError(
                "factor_formula_stream_invalid_result_grid", "values differ from selected stocks"
            )
        if any(point.trade_date != self.universe.trade_date for point in self.values):
            raise PydanticCustomError(
                "factor_formula_stream_invalid_result_date", "value dates differ from pool"
            )
        return self


class FactorFormulaStreamCompletion(BaseModel):
    """A bounded receipt produced only after successful source exhaustion."""

    model_config = _IMMUTABLE

    definition_sha256: Sha256
    request_sha256: Sha256
    sources: FactorFormulaStreamSources
    processed_days: int = Field(ge=1, le=MAX_TRADE_DAYS)
    history_sha256: Sha256
    sha256: Sha256


@dataclass(frozen=True, slots=True)
class _Plan:
    nodes: tuple[ast.AST, ...]
    retained_days: dict[int, int]
    slots: int


def _children(node: ast.AST) -> tuple[ast.AST, ...]:
    if isinstance(node, ast.UnaryOp):
        return (node.operand,)
    if isinstance(node, ast.BinOp):
        return (node.left, node.right)
    if isinstance(node, ast.Compare):
        return (node.left, node.comparators[0])
    if isinstance(node, ast.Call):
        return tuple(node.args[:2] if node.func.id == "ts_corr" else node.args[:1])
    return ()


def _compile(request: FactorFormulaStreamRequest) -> _Plan:
    root = ast.parse(request.definition.expression, mode="eval").body
    nodes: list[ast.AST] = []
    retained: dict[int, int] = {}

    def visit(node: ast.AST) -> None:
        retained.setdefault(id(node), 1)
        lookback = 1
        if isinstance(node, ast.Call):
            name = node.func.id
            if name in ("ref", "ts_delta"):
                lookback = _literal_integer(node.args[1]) + 1
            elif name in ("ts_mean", "ts_std", "ts_rank", "ts_corr"):
                lookback = _literal_integer(node.args[-1])
        for child in _children(node):
            visit(child)
            retained[id(child)] = max(retained[id(child)], lookback)
        nodes.append(node)

    visit(root)
    # Each node's actual parent window, current feature points, and one working/output vector.
    context = request.sources.context
    context_slots = (
        0
        if context is None
        else 4 * (int(context.industry is not None) + int(context.market_cap is not None)) + 4
    )
    slots = len(request.computation_stock_codes) * (
        sum(retained.values())
        + len(request.definition.dependency_columns)
        + 1
        + context_slots
        + (6 if request.mad_multiple is not None else 0)
    )
    if slots > MAX_FORMULA_CACHE_SLOTS:
        raise FactorFormulaStreamError("cache_budget_exceeded")
    return _Plan(tuple(nodes), retained, slots)


def factor_formula_stream_request_sha256(request: FactorFormulaStreamRequest) -> str:
    return canonical_sha256(FactorFormulaStreamRequest.model_validate(request))


def factor_formula_stream_batch_sha256(batch: FactorFormulaStreamBatch) -> str:
    if batch.context is None:
        return canonical_sha256(batch)
    fields = batch.model_dump()
    fields["context"] = {"sha256": batch.context.sha256}
    return canonical_sha256(fields)


class _CachedSeries(_SeriesEvaluator):
    """The existing pure operators read already computed child cells."""

    def __init__(
        self, stock_index: int, history: dict[int, deque[tuple[int, tuple[_Cell, ...]]]]
    ) -> None:
        self.stock_index = stock_index
        self.history = history

    def evaluate(self, node: ast.AST, day_index: int) -> _Cell:
        rows = self.history[id(node)]
        offset = rows[-1][0] - day_index
        if offset < 0 or offset >= len(rows):
            raise FactorTimeSeriesError("invalid_definition")
        return rows[-1 - offset][1][self.stock_index]


class _DailyFormula:
    def __init__(self, request: FactorFormulaStreamRequest, plan: _Plan) -> None:
        self.request = request
        self.plan = plan
        self.history: dict[int, deque[tuple[int, tuple[_Cell, ...]]]] = {
            key: deque(maxlen=size) for key, size in plan.retained_days.items()
        }

    def evaluate(
        self, batch: FactorFormulaStreamBatch, pool: FactorUniverseResult, day_index: int
    ) -> tuple[FactorTimeSeriesValue, ...]:
        codes = self.request.computation_stock_codes
        industries, caps = ({}, {}) if batch.context is None else batch.context.observations()
        points = {(point.stock_code, point.column): point for point in batch.feature_points}
        adapters = tuple(_CachedSeries(index, self.history) for index in range(len(codes)))
        for node in self.plan.nodes:
            if isinstance(node, ast.Name):
                cells = []
                for code in codes:
                    point = points[(code, node.id)]
                    if point.state == "missing_observation":
                        cells.append(_missing("missing_observation"))
                    elif point.state == "known_null":
                        cells.append(_missing("missing_value"))
                    else:
                        cells.append(_present(point.value, point.first_visible_at))
                current = tuple(cells)
            elif isinstance(node, ast.Call) and node.func.id in _CS_FUNCTIONS:
                child = self.history[id(node.args[0])][-1][1]
                inputs = {
                    code: cell
                    for code, cell in zip(codes, child, strict=True)
                    if code in self.selected
                }
                valid = [(code, cell) for code, cell in inputs.items() if cell.value is not None]
                latest = _latest([cell for _, cell in valid])
                results = inputs.copy()
                if node.func.id == "cs_rank":
                    _SeriesEvaluator._cross_rank(valid, latest, results)
                elif node.func.id == "cs_zscore":
                    _SeriesEvaluator._cross_zscore(valid, latest, results)
                elif node.func.id == "cs_winsorize":
                    _SeriesEvaluator._cross_winsorize(
                        valid, latest, results, _literal_number(node.args[1])
                    )
                else:
                    results = neutralize_factor_cells(
                        "industry" if node.func.id == "industry_neutralize" else "size",
                        inputs,
                        industries=industries,
                        market_caps=caps,
                    )
                current = tuple(
                    results.get(code, _missing("missing_observation")) for code in codes
                )
            else:
                current = tuple(adapter._evaluate_uncached(node, day_index) for adapter in adapters)
            self.history[id(node)].append((day_index, current))
        root_cells = self.history[id(self.plan.nodes[-1])][-1][1]
        if self.request.mad_multiple is not None:
            selected = {
                code: cell
                for code, cell in zip(codes, root_cells, strict=True)
                if code in self.selected
            }
            valid = [(code, cell) for code, cell in selected.items() if cell.value is not None]
            results = selected.copy()
            _SeriesEvaluator._cross_winsorize(
                valid, _latest([cell for _, cell in valid]), results, self.request.mad_multiple
            )
            root_cells = tuple(
                results.get(code, cell) for code, cell in zip(codes, root_cells, strict=True)
            )
        if self.request.neutralization != "none":
            selected = {
                code: cell
                for code, cell in zip(codes, root_cells, strict=True)
                if code in self.selected
            }
            results = neutralize_factor_cells(
                self.request.neutralization, selected, industries=industries, market_caps=caps
            )
            root_cells = tuple(
                results.get(code, cell) for code, cell in zip(codes, root_cells, strict=True)
            )
        earliest = self.request.definition.earliest_available_date
        before_available = earliest is not None and pool.trade_date < earliest
        values: list[FactorTimeSeriesValue] = []
        for code, cell in zip(codes, root_cells, strict=True):
            if code in self.selected:
                if before_available:
                    cell = _missing("before_available_date")
                values.append(
                    FactorTimeSeriesValue(
                        stock_code=code,
                        trade_date=pool.trade_date,
                        value=cell.value,
                        missing_reason=cell.reason,
                        latest_visible_at=cell.latest_visible_at,
                    )
                )
        return tuple(values)

    @property
    def selected(self) -> frozenset[str]:
        return self._selected


def _checked_day(
    request: FactorFormulaStreamRequest,
    batch: FactorFormulaStreamBatch,
    request_sha: str,
    decision: DecisionTime,
) -> FactorUniverseResult:
    if batch.universe.trade_date != decision.trade_date:
        raise FactorFormulaStreamError("date_order_mismatch")
    if batch.request_sha256 != request_sha:
        raise FactorFormulaStreamError("request_binding_mismatch")
    if batch.sources != request.sources:
        raise FactorFormulaStreamError("source_binding_mismatch")
    context = batch.context
    stored = batch.daily_features
    if (
        (stored is None) != (request.sources.daily_features is None)
        or stored is not None
        and (
            stored.sources != request.sources.daily_features
            or stored.trade_date != decision.trade_date
            or tuple(row.stock_code for row in stored.rows) != request.computation_stock_codes
        )
    ):
        raise FactorFormulaStreamError("source_binding_mismatch")
    if (context is None) != (request.sources.context is None):
        raise FactorFormulaStreamError("source_binding_mismatch")
    if context is not None and (
        context.sources != request.sources.context
        or context.trade_date != decision.trade_date
        or context.stock_codes != request.computation_stock_codes
        or context.assumed_visible_at != decision.decision_at
    ):
        raise FactorFormulaStreamError("source_binding_mismatch")
    if batch.universe.selection != request.selection:
        raise FactorFormulaStreamError("selection_mismatch")
    if batch.universe.as_of != request.as_of:
        raise FactorFormulaStreamError("as_of_mismatch")
    pool = select_factor_universe(batch.universe)
    securities = batch.universe.securities
    assert securities is not None
    if (
        securities.source_id != request.sources.security_source_id
        or securities.source_sha256 != request.sources.security_source_sha256
    ):
        raise FactorFormulaStreamError("source_binding_mismatch")
    members = batch.universe.membership
    if members is not None and (
        members.source_id != request.sources.index_source_id
        or members.source_sha256 != request.sources.index_source_sha256
    ):
        raise FactorFormulaStreamError("source_binding_mismatch")
    if not set(securities.complete_stock_codes) <= set(request.computation_stock_codes):
        raise FactorFormulaStreamError("security_outside_computation_scope")
    expected = {
        (code, column)
        for code in request.computation_stock_codes
        for column in request.definition.dependency_columns
    }
    actual = {(point.stock_code, point.column) for point in batch.feature_points}
    if actual != expected:
        raise FactorFormulaStreamError("feature_grid_mismatch")
    for point in batch.feature_points:
        if point.trade_date != decision.trade_date:
            raise FactorFormulaStreamError("feature_date_mismatch")
        if point.first_visible_at is not None and point.first_visible_at > decision.decision_at:
            raise FactorFormulaStreamError("future_feature")
    if stored is not None:
        stored_points = {
            (row.stock_code, field.column): value
            for row in stored.rows
            for field, value in zip(stored.stock_fields, row.values, strict=True)
        }
        for point in batch.feature_points:
            value = stored_points.get((point.stock_code, point.column)) or stored.market_value(
                point.column
            )
            if value is not None and (
                point.value != value.value
                or point.state
                != (
                    "value"
                    if value.status == "valid"
                    else "missing_observation"
                    if value.status == "missing"
                    else "known_null"
                )
            ):
                raise FactorFormulaStreamError("feature_grid_mismatch")
    return pool


class FactorFormulaStream(Iterator[FactorFormulaStreamDay]):
    def __init__(
        self,
        request: FactorFormulaStreamRequest,
        plan: _Plan,
        batches: Iterable[FactorFormulaStreamBatch],
    ) -> None:
        self.cache_slots = plan.slots
        self._engine = _DailyFormula(request, plan)
        self._completion: FactorFormulaStreamCompletion | None = None
        self._iterator = self._iterate(request, batches)

    @property
    def completion(self) -> FactorFormulaStreamCompletion | None:
        return self._completion

    def __iter__(self) -> FactorFormulaStream:
        return self

    def __next__(self) -> FactorFormulaStreamDay:
        try:
            return next(self._iterator)
        except StopIteration as end:
            if isinstance(end.value, FactorFormulaStreamCompletion):
                self._completion = end.value
            raise

    def close(self) -> None:
        self._iterator.close()

    def _iterate(
        self, request: FactorFormulaStreamRequest, batches: Iterable[FactorFormulaStreamBatch]
    ) -> Generator[FactorFormulaStreamDay, None, FactorFormulaStreamCompletion]:
        request_sha = factor_formula_stream_request_sha256(request)
        definition_sha = canonical_sha256(request.definition)
        history_sha = request_sha
        try:
            iterator = iter(batches)
            for day_index, decision in enumerate(request.decision_times):
                try:
                    raw_batch = next(iterator)
                except StopIteration:
                    raise FactorFormulaStreamError("missing_batch") from None
                batch = FactorFormulaStreamBatch.model_validate(raw_batch)
                pool = _checked_day(request, batch, request_sha, decision)
                self._engine._selected = frozenset(pool.stock_codes)
                values = self._engine.evaluate(batch, pool, day_index)
                input_sha = factor_formula_stream_batch_sha256(batch)
                history_sha = canonical_sha256((history_sha, input_sha))
                fields = {
                    "factor_id": request.definition.factor_id,
                    "version": request.definition.version,
                    "definition_sha256": definition_sha,
                    "request_sha256": request_sha,
                    "sources": request.sources,
                    "universe": pool,
                    "decision_at": decision.decision_at,
                    "values": values,
                    "input_sha256": input_sha,
                    "history_sha256": history_sha,
                }
                day = FactorFormulaStreamDay(**fields, sha256=canonical_sha256(fields))
                del batch, raw_batch, values, fields, pool
                yield day
                del day
            try:
                next(iterator)
            except StopIteration:
                pass
            else:
                raise FactorFormulaStreamError("unexpected_batch")
            fields = {
                "definition_sha256": definition_sha,
                "request_sha256": request_sha,
                "sources": request.sources,
                "processed_days": len(request.trading_days),
                "history_sha256": history_sha,
            }
            return FactorFormulaStreamCompletion(**fields, sha256=canonical_sha256(fields))
        finally:
            self._engine.history.clear()


def evaluate_factor_formula_stream(
    request: FactorFormulaStreamRequest, batches: Iterable[FactorFormulaStreamBatch]
) -> FactorFormulaStream:
    """Compile and admit the bounded cache before touching the source iterator."""
    checked = FactorFormulaStreamRequest.model_validate(request)
    return FactorFormulaStream(checked, _compile(checked), batches)
